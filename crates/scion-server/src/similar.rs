//! Rename detection, as git finds renames by default: which deleted file an
//! added file came from. `Diff` pairs the added and deleted files of a diff,
//! as `git diff` does. `Blame` looks for the source of one added file, as
//! `git blame` does. Both call [`renames`].

use std::cmp::Reverse;
use std::collections::HashMap;

use scion_core::{BlobId, FileState, GitOid, RepoPath};

use crate::error::Result;
use crate::store::Store;

/// git's similarity scale: this score means the same content.
pub const SAME: u32 = 60_000;

/// The least score of a rename: 50%, git's default.
const MIN_SCORE: u32 = SAME / 2;

/// The least score of a rename found by name ([`renames`], step 2): halfway
/// from [`MIN_SCORE`] to [`SAME`], so 75%, as in git.
const MIN_NAME_SCORE: u32 = MIN_SCORE + (SAME - MIN_SCORE) / 2;

/// git's default rename limit (`diff.renameLimit`, 1,000), squared. Step 3
/// of [`renames`] runs only when the added files left times the deleted
/// files left are at most this many pairs.
pub const MAX_PAIRS: usize = 1000 * 1000;

/// Most deleted files with the same content that step 1 looks at for one
/// added file, as in git.
const MAX_SAME_CONTENT: usize = 100;

/// Most similar files that step 3 keeps for one added file, as in git.
const CANDIDATES: usize = 4;

/// git hashes each chunk to one of this many values.
const HASHBASE: u32 = 107_927;

/// A file cut as git's rename detection cuts it. A chunk ends after a
/// newline or after 64 bytes. In text (no NUL byte in the first 8,000 bytes,
/// git's test for binary), a CR before a newline does not count, so a line
/// scores the same with either line end. Each chunk is hashed to one of
/// [`HASHBASE`] values, as in git, so two different chunks can count as the
/// same. So a table has at most [`HASHBASE`] rows, whatever the file size.
pub struct Chunks {
    /// For each hash, the bytes of the chunks with that hash, by hash.
    table: Vec<(u32, u32)>,
    /// How many chunks the file has.
    count: usize,
}

impl Chunks {
    /// Cuts `text` into chunks, with git's hash (`diffcore-delta.c`).
    #[must_use]
    pub fn of(text: &[u8]) -> Self {
        let is_text = !text[..text.len().min(8000)].contains(&0);
        let mut table: HashMap<u32, u32> = HashMap::new();
        let mut count = 0;
        let mut add = |accum1: u32, accum2: u32, bytes: u32| {
            let hash = accum1.wrapping_add(accum2.wrapping_mul(0x61)) % HASHBASE;
            *table.entry(hash).or_default() += bytes;
            count += 1;
        };
        let (mut accum1, mut accum2, mut bytes) = (0u32, 0u32, 0u32);
        for (at, &byte) in text.iter().enumerate() {
            if is_text && byte == b'\r' && text.get(at + 1) == Some(&b'\n') {
                continue;
            }
            (accum1, accum2) = (
                (accum1 << 7) ^ (accum2 >> 25),
                (accum2 << 7) ^ (accum1 >> 25),
            );
            accum1 = accum1.wrapping_add(u32::from(byte));
            bytes += 1;
            if bytes == 64 || byte == b'\n' {
                add(accum1, accum2, bytes);
                (accum1, accum2, bytes) = (0, 0, 0);
            }
        }
        if bytes > 0 {
            add(accum1, accum2, bytes);
        }
        let mut table: Vec<(u32, u32)> = table.into_iter().collect();
        table.sort_unstable();
        Self { table, count }
    }

    /// The bytes that both files have: for each hash, the fewer bytes of
    /// the two.
    fn shared(&self, other: &Self) -> u64 {
        let (mut i, mut j, mut shared) = (0, 0, 0);
        while let (Some(&(a, a_bytes)), Some(&(b, b_bytes))) =
            (self.table.get(i), other.table.get(j))
        {
            if a == b {
                shared += u64::from(a_bytes.min(b_bytes));
            }
            i += usize::from(a <= b);
            j += usize::from(b <= a);
        }
        shared
    }
}

/// A rename that [`renames`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pair {
    /// The added file, as its index.
    pub added: usize,
    /// The deleted file it came from, as its index.
    pub deleted: usize,
    /// How similar the two are, on git's scale ([`SAME`]).
    pub score: u32,
}

/// The work that one [`renames`] call may do.
pub struct Budget<'a> {
    /// The cost so far, with what the caller counted before. Each chunk of
    /// a file read counts once, and each row of a chunk table that a
    /// comparison reads counts once.
    pub cost: &'a mut usize,
    /// Once `cost` reaches this, the search reads no more files. It checks
    /// before each read, so a read and its comparisons that start below the
    /// cap run to the end.
    pub cap: usize,
}

/// Pairs added files with the deleted files they came from, as git does by
/// default (`diffcore-rename.c`). A deleted file is the source of at most
/// one added file. `added` and `deleted` are in path order: byte order of
/// the whole path, git's order.
///
/// 1. Same content: a deleted file of the same kind and ID. A regular file
///    can change its executable bit. Added files take their turn in path
///    order. Each looks at the first 100 deleted files with its content
///    that are left, and takes the first with the same name (the last part
///    of the path), else the first. This reads no file.
/// 2. Same name: a deleted and an added regular file whose name no other
///    file left on their side has, if they are at least 75% similar.
/// 3. Similar: if at most `max_pairs` pairs of files are left, each added
///    regular file keeps the four deleted regular files most similar to it,
///    of those at least 50% similar. Then those pairs, most similar first,
///    become renames while both files are free. On a tie, a pair with the
///    same name comes first, then by added path, then by deleted path.
///
/// Similarity is the bytes of the chunks that both files have ([`Chunks`]),
/// over the size of the larger file. A file less than half the size of the
/// other (three quarters in step 2) is not compared. `read` reads a regular
/// file's chunks. When it returns `None`, the file is like no other.
///
/// Step 3 reads the added files that some deleted file could be like, in
/// path order, into one index by chunk hash. Then it reads the deleted
/// files, in path order, one at a time, and looks up each of their chunks
/// in the index. So it holds the tables of those added files, and of one
/// deleted file.
///
/// # Errors
///
/// A failure to read a size, or what `read` returns.
pub fn renames(
    store: &Store,
    added: &[(&RepoPath, FileState)],
    deleted: &[(&RepoPath, FileState)],
    read: impl FnMut(&RepoPath, BlobId) -> Result<Option<Chunks>>,
    max_pairs: usize,
    budget: Budget<'_>,
) -> Result<Vec<Pair>> {
    debug_assert!(added.is_sorted_by_key(|(path, _)| *path));
    debug_assert!(deleted.is_sorted_by_key(|(path, _)| *path));
    let mut search = Search {
        store,
        read,
        budget,
        added,
        deleted,
        taken: vec![false; added.len()],
        used: vec![false; deleted.len()],
        pairs: Vec::new(),
        sizes: HashMap::new(),
        tables: HashMap::new(),
        known: HashMap::new(),
    };
    search.same_content();
    search.same_name()?;
    search.similar(max_pairs)?;
    Ok(search.pairs)
}

/// What step 1 of [`renames`] compares: the kind and the ID. The executable
/// bit does not count, as in git.
#[derive(PartialEq, Eq, Hash)]
enum Content {
    Regular(BlobId),
    Symlink(BlobId),
    Gitlink(GitOid),
}

fn content(state: FileState) -> Content {
    match state {
        FileState::Regular { content, .. } => Content::Regular(content),
        FileState::Symlink { target } => Content::Symlink(target),
        FileState::Gitlink { commit } => Content::Gitlink(commit),
    }
}

/// The blob of a regular file. Only regular files can be similar, as in git.
fn regular(state: FileState) -> Option<BlobId> {
    match state {
        FileState::Regular { content, .. } => Some(content),
        _ => None,
    }
}

/// The last part of a path.
#[must_use]
pub fn file_name(path: &RepoPath) -> &[u8] {
    let bytes = path.as_bytes();
    bytes.rsplit(|&b| b == b'/').next().unwrap_or(bytes)
}

/// Whether files of `size` and `other` bytes can be `min` similar: git's
/// test. The bytes they share are at most the smaller size.
fn can_be(size: usize, other: usize, min: u32) -> bool {
    let (large, small) = (size.max(other) as u64, size.min(other) as u64);
    large * u64::from(SAME - min) >= (large - small) * u64::from(SAME)
}

/// git's score of two files of `size` and `other` bytes that share
/// `shared` bytes: over the larger size, on the scale of [`SAME`].
fn git_score(shared: u64, size: usize, other: usize) -> u32 {
    let large = size.max(other).max(1) as u64;
    // The shared bytes are at most the smaller size.
    u32::try_from(shared * u64::from(SAME) / large).unwrap_or(SAME)
}

/// A regular file that step 3 of [`renames`] compares: its index, blob and
/// size.
type File = (usize, BlobId, usize);

/// The state of one [`renames`] call.
struct Search<'a, R> {
    store: &'a Store,
    read: R,
    budget: Budget<'a>,
    added: &'a [(&'a RepoPath, FileState)],
    deleted: &'a [(&'a RepoPath, FileState)],
    /// Which added files are paired.
    taken: Vec<bool>,
    /// Which deleted files are paired.
    used: Vec<bool>,
    pairs: Vec<Pair>,
    /// The size of each file looked at.
    sizes: HashMap<BlobId, usize>,
    /// The chunks of each added file read, or `None` if `read` refused it.
    tables: HashMap<BlobId, Option<Chunks>>,
    /// The scores that step 2 found under 75%, by added and deleted file, so
    /// step 3 does not compare those files again.
    known: HashMap<(usize, usize), u32>,
}

impl<R: FnMut(&RepoPath, BlobId) -> Result<Option<Chunks>>> Search<'_, R> {
    fn pair(&mut self, added: usize, deleted: usize, score: u32) {
        (self.taken[added], self.used[deleted]) = (true, true);
        self.pairs.push(Pair {
            added,
            deleted,
            score,
        });
    }

    /// The files of a side that are not paired yet.
    fn left(done: &[bool]) -> Vec<usize> {
        (0..done.len()).filter(|&at| !done[at]).collect()
    }

    /// Step 1 of [`renames`].
    fn same_content(&mut self) {
        let mut by_content: HashMap<Content, Vec<usize>> = HashMap::new();
        for (d, (_, state)) in self.deleted.iter().enumerate() {
            by_content.entry(content(*state)).or_default().push(d);
        }
        for (a, (path, state)) in self.added.iter().enumerate() {
            let Some(sources) = by_content.get(&content(*state)) else {
                continue;
            };
            let source = sources
                .iter()
                .copied()
                .filter(|&d| !self.used[d])
                .take(MAX_SAME_CONTENT)
                .min_by_key(|&d| file_name(self.deleted[d].0) != file_name(path));
            if let Some(d) = source {
                self.pair(a, d, SAME);
            }
        }
    }

    /// For each name among the files of `files` left, the one file with it,
    /// or `None` if several have it.
    fn unique_names<'p>(
        files: &[(&'p RepoPath, FileState)],
        done: &[bool],
    ) -> HashMap<&'p [u8], Option<usize>> {
        let mut names = HashMap::new();
        for at in Self::left(done) {
            names
                .entry(file_name(files[at].0))
                .and_modify(|one| *one = None)
                .or_insert(Some(at));
        }
        names
    }

    /// Step 2 of [`renames`].
    fn same_name(&mut self) -> Result<()> {
        let added_names = Self::unique_names(self.added, &self.taken);
        let deleted_names = Self::unique_names(self.deleted, &self.used);
        for d in Self::left(&self.used) {
            let name = file_name(self.deleted[d].0);
            let (Some(Some(_)), Some(&Some(a))) = (deleted_names.get(name), added_names.get(name))
            else {
                continue;
            };
            let (Some(from), Some(to)) = (regular(self.deleted[d].1), regular(self.added[a].1))
            else {
                continue;
            };
            let (from_size, to_size) = (self.size(from)?, self.size(to)?);
            if !can_be(from_size, to_size, MIN_NAME_SCORE) || !self.load(self.added[a].0, to)? {
                continue;
            }
            let Some(source) = self.read_source(self.deleted[d].0, from)? else {
                continue;
            };
            let Some(Some(target)) = self.tables.get(&to) else {
                continue;
            };
            let shared = target.shared(&source);
            *self.budget.cost += target.table.len() + source.table.len();
            let value = git_score(shared, from_size, to_size);
            if value >= MIN_NAME_SCORE {
                self.pair(a, d, value);
            } else {
                self.known.insert((a, d), value);
            }
        }
        Ok(())
    }

    /// Whether step 3 compares the added file `to` and the deleted file
    /// `from`: they can be 50% similar, and step 2 did not compare them.
    fn compares(&self, &(a, _, to_size): &File, &(d, _, from_size): &File) -> bool {
        !self.known.contains_key(&(a, d)) && can_be(from_size, to_size, MIN_SCORE)
    }

    /// Step 3 of [`renames`].
    fn similar(&mut self, max_pairs: usize) -> Result<()> {
        let (added_left, deleted_left) = (Self::left(&self.taken), Self::left(&self.used));
        let count = added_left.len().saturating_mul(deleted_left.len());
        if count == 0 || count > max_pairs {
            return Ok(());
        }
        let mut targets = Vec::new();
        for a in added_left {
            if let Some(to) = regular(self.added[a].1) {
                targets.push((a, to, self.size(to)?));
            }
        }
        let mut sources = Vec::new();
        for d in deleted_left {
            if let Some(from) = regular(self.deleted[d].1) {
                sources.push((d, from, self.size(from)?));
            }
        }
        // For each chunk hash, the targets read with it, and their bytes.
        let mut index: HashMap<u32, Vec<(usize, u32)>> = HashMap::new();
        let mut loaded = vec![false; targets.len()];
        for (t, target) in targets.iter().enumerate() {
            let (a, to, _) = *target;
            if !sources.iter().any(|source| self.compares(target, source))
                || !self.load(self.added[a].0, to)?
            {
                continue;
            }
            let Some(Some(chunks)) = self.tables.get(&to) else {
                continue;
            };
            for &(hash, bytes) in &chunks.table {
                index.entry(hash).or_default().push((t, bytes));
            }
            loaded[t] = true;
        }
        // For each target, its best pairs so far: score, same name, and the
        // deleted file.
        let mut best: Vec<Vec<(u32, bool, usize)>> = vec![Vec::new(); targets.len()];
        let mut shared = vec![0; targets.len()];
        for source in &sources {
            let (d, from, from_size) = *source;
            let mut found = Vec::new();
            for (t, &(a, ..)) in targets.iter().enumerate() {
                if let Some(&value) = self.known.get(&(a, d)) {
                    found.push((t, value));
                }
            }
            let wanted: Vec<usize> = (0..targets.len())
                .filter(|&t| loaded[t] && self.compares(&targets[t], source))
                .collect();
            if !wanted.is_empty()
                && let Some(chunks) = self.read_source(self.deleted[d].0, from)?
            {
                shared.fill(0);
                let mut rows = chunks.table.len();
                for (hash, bytes) in &chunks.table {
                    if let Some(hits) = index.get(hash) {
                        rows += hits.len();
                        for &(t, other) in hits {
                            shared[t] += u64::from((*bytes).min(other));
                        }
                    }
                }
                *self.budget.cost += rows;
                found.extend(
                    wanted
                        .iter()
                        .map(|&t| (t, git_score(shared[t], from_size, targets[t].2))),
                );
            }
            for (t, value) in found.into_iter().filter(|&(_, value)| value >= MIN_SCORE) {
                let same_name =
                    file_name(self.added[targets[t].0].0) == file_name(self.deleted[d].0);
                let kept = &mut best[t];
                kept.push((value, same_name, d));
                // A stable sort, so on a tie the first deleted file stays
                // first.
                kept.sort_by_key(|&(value, same_name, _)| Reverse((value, same_name)));
                kept.truncate(CANDIDATES);
            }
        }
        let mut candidates: Vec<(u32, bool, usize, usize)> = Vec::new();
        for (t, kept) in best.into_iter().enumerate() {
            let a = targets[t].0;
            candidates.extend(
                kept.into_iter()
                    .map(|(value, same_name, d)| (value, same_name, a, d)),
            );
        }
        candidates.sort_by_key(|&(value, same_name, a, d)| (Reverse((value, same_name)), a, d));
        for (value, _, a, d) in candidates {
            if !self.taken[a] && !self.used[d] {
                self.pair(a, d, value);
            }
        }
        Ok(())
    }

    fn size(&mut self, blob: BlobId) -> Result<usize> {
        if let Some(&size) = self.sizes.get(&blob) {
            return Ok(size);
        }
        let size = self.store.blobs().size(blob)?;
        self.sizes.insert(blob, size);
        Ok(size)
    }

    fn spent(&self) -> bool {
        *self.budget.cost >= self.budget.cap
    }

    /// Reads the added file at `path` into `tables`, once. Whether its
    /// chunks are there: not if the budget was spent first, or `read`
    /// refused it.
    fn load(&mut self, path: &RepoPath, blob: BlobId) -> Result<bool> {
        if !self.tables.contains_key(&blob) {
            if self.spent() {
                return Ok(false);
            }
            let chunks = (self.read)(path, blob)?;
            *self.budget.cost += chunks.as_ref().map_or(0, |chunks| chunks.count);
            self.tables.insert(blob, chunks);
        }
        Ok(matches!(self.tables.get(&blob), Some(Some(_))))
    }

    /// Reads the deleted file at `path`, unless the budget is spent or
    /// `read` refuses it.
    fn read_source(&mut self, path: &RepoPath, blob: BlobId) -> Result<Option<Chunks>> {
        if self.spent() {
            return Ok(None);
        }
        let chunks = (self.read)(path, blob)?;
        *self.budget.cost += chunks.as_ref().map_or(0, |chunks| chunks.count);
        Ok(chunks)
    }
}
