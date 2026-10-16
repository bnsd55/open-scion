//! Rename detection, as git finds renames by default: which deleted file an
//! added file came from. `Diff` pairs the added and deleted files of a diff,
//! as `git diff` does. `Blame` looks for the source of one added file, as
//! `git blame` does. Both call [`renames`].

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use scion_core::{BlobId, FileState, GitOid, RepoPath};

use crate::error::Result;
use crate::store::{BlobReader, Store};

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

/// The parts in which [`Chunks::read`] reads a blob. More than the 8,000
/// bytes that tell text from binary.
const PART_BYTES: usize = 1 << 20;

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
        let mut chunker = Chunker::default();
        chunker.push(text);
        chunker.finish()
    }

    /// Reads a blob in parts and cuts it into chunks. Holds one part and
    /// the blob reader ([`crate::store::READER_BYTES`]), not the whole blob.
    ///
    /// # Errors
    ///
    /// As [`crate::store::Blobs::reader`] and [`BlobReader::read_part`].
    pub fn read(store: &Store, blob: BlobId) -> Result<Self> {
        let mut reader: BlobReader = store.blobs().reader(blob)?;
        let mut chunker = Chunker::default();
        while let Some(part) = reader.read_part(PART_BYTES)? {
            chunker.push(&part);
        }
        Ok(chunker.finish())
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

/// Cuts a file into [`Chunks`] one part at a time.
#[derive(Default)]
struct Chunker {
    /// For each hash, the bytes of the chunks with that hash.
    table: HashMap<u32, u32>,
    count: usize,
    /// git's two hash accumulators, and the bytes of the chunk so far.
    accum1: u32,
    accum2: u32,
    bytes: u32,
    /// Whether the file is text, from its first part.
    is_text: Option<bool>,
    /// A CR that ended the last part. The next part says whether a newline
    /// follows it.
    cr: bool,
}

impl Chunker {
    /// The next part of the file. The first part holds the first 8,000
    /// bytes, or the whole file.
    fn push(&mut self, part: &[u8]) {
        let is_text = *self
            .is_text
            .get_or_insert_with(|| !part[..part.len().min(8000)].contains(&0));
        if self.cr {
            self.cr = false;
            if part.first() != Some(&b'\n') {
                self.byte(b'\r');
            }
        }
        for (at, &byte) in part.iter().enumerate() {
            if is_text && byte == b'\r' {
                match part.get(at + 1) {
                    Some(b'\n') => continue,
                    None => {
                        self.cr = true;
                        continue;
                    }
                    Some(_) => {}
                }
            }
            self.byte(byte);
        }
    }

    fn byte(&mut self, byte: u8) {
        (self.accum1, self.accum2) = (
            (self.accum1 << 7) ^ (self.accum2 >> 25),
            (self.accum2 << 7) ^ (self.accum1 >> 25),
        );
        self.accum1 = self.accum1.wrapping_add(u32::from(byte));
        self.bytes += 1;
        if self.bytes == 64 || byte == b'\n' {
            self.end();
        }
    }

    /// Ends the chunk so far.
    fn end(&mut self) {
        let hash = self.accum1.wrapping_add(self.accum2.wrapping_mul(0x61)) % HASHBASE;
        *self.table.entry(hash).or_default() += self.bytes;
        self.count += 1;
        (self.accum1, self.accum2, self.bytes) = (0, 0, 0);
    }

    fn finish(mut self) -> Chunks {
        // A CR at the very end has no newline after it, so it counts.
        if self.cr {
            self.byte(b'\r');
        }
        if self.bytes > 0 {
            self.end();
        }
        let mut table: Vec<(u32, u32)> = self.table.into_iter().collect();
        table.sort_unstable();
        Chunks {
            table,
            count: self.count,
        }
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
    /// a file read counts once, each row of a chunk table put in the index
    /// of step 3 or read by a comparison counts once, and each file size
    /// read counts [`SIZE_COST`]. Steps 2 and 3 share it.
    pub cost: &'a mut usize,
    /// Once `cost` reaches this, the search starts no more work. It checks
    /// before each pair of step 2 and each deleted file of step 3, so the
    /// work on one of them runs to the end.
    pub cap: usize,
    /// Whether step 3 looks at one more deleted file than it already had,
    /// even past the cap, so that a search that goes on over calls always
    /// moves.
    pub progress: bool,
}

/// What one file size read costs against a [`Budget`]: about the work of
/// cutting this many chunks, since it opens the file.
pub const SIZE_COST: usize = 64;

/// Where step 3 of a search for one added file stands ([`renames`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    /// How many deleted files, in path order, step 3 has looked at.
    pub searched: usize,
    /// The deleted file most like the added file among those, if one is at
    /// least 50% like it.
    pub best: Option<usize>,
}

/// What [`renames`] found.
#[derive(Debug)]
pub struct Outcome {
    /// The renames.
    pub pairs: Vec<Pair>,
    /// Set when step 3 stopped at the budget: the first deleted file it did
    /// not look at. Then `pairs` holds the best pairs among the files it
    /// looked at.
    pub stopped: Option<usize>,
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
/// file's chunks, given its path, blob and size. When it returns `None`,
/// the file is like no other.
///
/// Step 3 takes the deleted files in path order, one at a time. For each,
/// it reads the added files that it could be like into one index by chunk
/// hash, if not read yet, then reads the deleted file and looks up its
/// chunks in the index. Added files with the same blob share one place in
/// the index, so a blob is read and indexed once, however many paths add
/// it. So it holds the tables of those added blobs, and of one deleted
/// file.
///
/// A search for one added file can stop at the budget and go on later: pass
/// the [`Progress`] made of [`Outcome::stopped`] and the deleted file of the
/// pair found so far. Then steps 1 and 2 do not run again; they found
/// nothing, or the search would not have reached step 3. Step 3 reads the
/// best file again, and goes on from where it stopped. The answer is the
/// same as that of one search with no budget.
///
/// # Errors
///
/// A failure to read a size, or what `read` returns.
pub fn renames(
    store: &Store,
    added: &[(&RepoPath, FileState)],
    deleted: &[(&RepoPath, FileState)],
    read: impl FnMut(&RepoPath, BlobId, usize) -> Result<Option<Chunks>>,
    max_pairs: usize,
    from: Progress,
    budget: Budget<'_>,
) -> Result<Outcome> {
    debug_assert!(added.is_sorted_by_key(|(path, _)| *path));
    debug_assert!(deleted.is_sorted_by_key(|(path, _)| *path));
    debug_assert!(from == Progress::default() || added.len() == 1);
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
        refused: HashSet::new(),
        known: HashMap::new(),
    };
    if from == Progress::default() {
        search.same_content();
        search.same_name()?;
    }
    let stopped = search.similar(max_pairs, from)?;
    Ok(Outcome {
        pairs: search.pairs,
        stopped,
    })
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
fn file_name(path: &RepoPath) -> &[u8] {
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
    /// The files that `read` refused.
    refused: HashSet<BlobId>,
    /// For each deleted file that step 2 compared and found under 75%: the
    /// added file and the score, so step 3 does not compare them again.
    known: HashMap<usize, (usize, u32)>,
}

impl<R: FnMut(&RepoPath, BlobId, usize) -> Result<Option<Chunks>>> Search<'_, R> {
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
            if self.spent() {
                break;
            }
            let (from_size, to_size) = (self.size(from)?, self.size(to)?);
            if !can_be(from_size, to_size, MIN_NAME_SCORE) || !self.load(self.added[a].0, to)? {
                continue;
            }
            let Some(source) = self.read_file(self.deleted[d].0, from)? else {
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
                self.known.insert(d, (a, value));
            }
        }
        Ok(())
    }

    /// Step 3 of [`renames`], from `from`. Returns the first deleted file
    /// that it did not look at, if it stopped at the budget.
    fn similar(&mut self, max_pairs: usize, from: Progress) -> Result<Option<usize>> {
        let (added_left, deleted_left) = (Self::left(&self.taken), Self::left(&self.used));
        let count = added_left.len().saturating_mul(deleted_left.len());
        if count == 0 || count > max_pairs {
            return Ok(None);
        }
        // The added regular files: index, blob and size.
        let mut targets = Vec::new();
        for a in added_left {
            if let Some(to) = regular(self.added[a].1) {
                targets.push((a, to, self.size(to)?));
            }
        }
        // The best file found before, then the files from where the search
        // stopped.
        let seed = from.best.filter(|&d| d < from.searched);
        let sources = seed
            .into_iter()
            .chain(deleted_left.into_iter().filter(|&d| d >= from.searched));
        // For each chunk hash, the target blobs read with it, as their slot,
        // and their bytes. Targets with the same blob share one slot, so the
        // index holds each blob once, however many paths add it.
        let mut index: HashMap<u32, Vec<(usize, u32)>> = HashMap::new();
        let mut slots: HashMap<BlobId, usize> = HashMap::new();
        // For each target, its best pairs so far: score, same name, and the
        // deleted file.
        let mut best: Vec<Vec<(u32, bool, usize)>> = vec![Vec::new(); targets.len()];
        // For one deleted file: the scores found, the targets read that it is
        // compared with and their slots, and the bytes shared with each slot.
        let (mut found, mut wanted, mut shared) = (Vec::new(), Vec::new(), Vec::new());
        let mut moved = false;
        let mut stopped = None;
        for d in sources {
            // The best file found before is read again, whatever the cost.
            let is_seed = Some(d) == seed;
            if !is_seed && self.spent() && (moved || !self.budget.progress) {
                stopped = Some(d);
                break;
            }
            moved |= !is_seed;
            let Some(from) = regular(self.deleted[d].1) else {
                continue;
            };
            let from_size = self.size(from)?;
            // Step 2 has the score of the one added file it compared.
            let known = self.known.get(&d).copied();
            found.clear();
            wanted.clear();
            for (t, &(a, to, to_size)) in targets.iter().enumerate() {
                match known {
                    Some((known_a, value)) if known_a == a => found.push((t, value)),
                    _ if can_be(from_size, to_size, MIN_SCORE) => {
                        if let Some(s) = self.slot(&mut index, &mut slots, a, to)? {
                            wanted.push((t, s));
                        }
                    }
                    _ => {}
                }
            }
            if !wanted.is_empty()
                && let Some(chunks) = self.read_file(self.deleted[d].0, from)?
            {
                shared.clear();
                shared.resize(slots.len(), 0);
                let mut rows = chunks.table.len();
                for (hash, bytes) in &chunks.table {
                    if let Some(hits) = index.get(hash) {
                        rows += hits.len();
                        for &(s, other) in hits {
                            shared[s] += u64::from((*bytes).min(other));
                        }
                    }
                }
                *self.budget.cost += rows;
                // Targets with one blob have one size, so one score.
                found.extend(
                    wanted
                        .iter()
                        .map(|&(t, s)| (t, git_score(shared[s], from_size, targets[t].2))),
                );
            }
            for (t, value) in found.drain(..).filter(|&(_, value)| value >= MIN_SCORE) {
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
        let added = targets.iter().map(|&(a, ..)| a);
        self.pair_best(added.zip(best));
        Ok(stopped)
    }

    /// The end of step 3: for each added file, the best pairs kept (score,
    /// same name, and the deleted file). Pairs them, most similar first,
    /// while both files are free.
    fn pair_best(&mut self, best: impl Iterator<Item = (usize, Vec<(u32, bool, usize)>)>) {
        let mut candidates: Vec<(u32, bool, usize, usize)> = Vec::new();
        for (a, kept) in best {
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
    }

    /// The slot in the index of step 3 of the added file `a`, of blob `to`:
    /// the slot of its blob, which this reads into `index` the first time.
    /// `None` if `read` refuses it.
    fn slot(
        &mut self,
        index: &mut HashMap<u32, Vec<(usize, u32)>>,
        slots: &mut HashMap<BlobId, usize>,
        a: usize,
        to: BlobId,
    ) -> Result<Option<usize>> {
        if let Some(&s) = slots.get(&to) {
            return Ok(Some(s));
        }
        if !self.load(self.added[a].0, to)? {
            return Ok(None);
        }
        let Some(Some(chunks)) = self.tables.get(&to) else {
            return Ok(None);
        };
        let s = slots.len();
        for &(hash, bytes) in &chunks.table {
            index.entry(hash).or_default().push((s, bytes));
        }
        // Each row the index holds counts too.
        *self.budget.cost += chunks.table.len();
        slots.insert(to, s);
        Ok(Some(s))
    }

    /// A file's size. Each one read costs [`SIZE_COST`].
    fn size(&mut self, blob: BlobId) -> Result<usize> {
        if let Some(&size) = self.sizes.get(&blob) {
            return Ok(size);
        }
        let size = self.store.blobs().size(blob)?;
        *self.budget.cost += SIZE_COST;
        self.sizes.insert(blob, size);
        Ok(size)
    }

    fn spent(&self) -> bool {
        *self.budget.cost >= self.budget.cap
    }

    /// Reads the chunks of the file at `path`, unless `read` refuses it. A
    /// file refused once is not read again.
    fn read_file(&mut self, path: &RepoPath, blob: BlobId) -> Result<Option<Chunks>> {
        if self.refused.contains(&blob) {
            return Ok(None);
        }
        let size = self.size(blob)?;
        let chunks = (self.read)(path, blob, size)?;
        match &chunks {
            Some(chunks) => *self.budget.cost += chunks.count,
            None => {
                self.refused.insert(blob);
            }
        }
        Ok(chunks)
    }

    /// Reads the added file at `path` into `tables`, once. Whether its
    /// chunks are there.
    fn load(&mut self, path: &RepoPath, blob: BlobId) -> Result<bool> {
        if !self.tables.contains_key(&blob) {
            let chunks = self.read_file(path, blob)?;
            self.tables.insert(blob, chunks);
        }
        Ok(matches!(self.tables.get(&blob), Some(Some(_))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file cut in two parts anywhere after its first 8,000 bytes has
    /// the same chunks as when it comes whole, also where a part ends
    /// between a CR and its newline, or in a CR with no newline.
    #[test]
    fn chunks_do_not_depend_on_where_parts_end() {
        let mut text: Vec<u8> = (0..900)
            .flat_map(|n| format!("line {n:04}\r\n").into_bytes())
            .collect();
        text.extend_from_slice(b"a lone CR\r here\r");
        let whole = Chunks::of(&text);
        for at in 8000..=text.len() {
            let mut chunker = Chunker::default();
            chunker.push(&text[..at]);
            chunker.push(&text[at..]);
            let parts = chunker.finish();
            assert_eq!(
                (&parts.table, parts.count),
                (&whole.table, whole.count),
                "{at}"
            );
        }
        // Without CRs that do not count, every byte is in some chunk.
        let counted: u32 = whole.table.iter().map(|&(_, bytes)| bytes).sum();
        assert_eq!(counted as usize, text.len() - 900);
    }

    /// Lines of 8 bytes, one for each number of `range`.
    fn lines(range: std::ops::Range<usize>) -> Vec<u8> {
        range
            .flat_map(|n| format!("{n:07}\n").into_bytes())
            .collect()
    }

    fn file(content: BlobId) -> FileState {
        FileState::Regular {
            content,
            executable: false,
        }
    }

    /// A blob added at many paths is read and indexed once: the search
    /// costs what it costs with one path, and finds the same rename.
    #[test]
    fn a_blob_added_at_many_paths_is_indexed_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let new = store.blobs().put(&lines(0..4000)).unwrap();
        // 90% like `new`.
        let old = store
            .blobs()
            .put(&[lines(0..3600), lines(10_000..10_400)].concat())
            .unwrap();
        let paths: Vec<RepoPath> = (0..100)
            .map(|k| RepoPath::new(format!("c/{k:03}")).unwrap())
            .collect();
        let old_path = RepoPath::new("old").unwrap();
        let deleted = [(&old_path, file(old))];
        let search = |copies: usize| {
            let added: Vec<_> = paths[..copies].iter().map(|p| (p, file(new))).collect();
            let read = |_: &RepoPath, blob, _| Ok(Some(Chunks::read(&store, blob)?));
            let mut cost = 0;
            let budget = Budget {
                cost: &mut cost,
                cap: usize::MAX,
                progress: false,
            };
            let from = Progress::default();
            let found = renames(&store, &added, &deleted, read, MAX_PAIRS, from, budget).unwrap();
            (found.pairs, cost)
        };
        let one = search(1);
        assert_eq!(one.0.len(), 1);
        assert_eq!(search(100), one);
    }

    /// A file that `read` refuses is not read again. Here step 2 reads the
    /// deleted file with the name of the added file, and step 3 would.
    #[test]
    fn a_file_refused_is_read_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let new = store.blobs().put(&lines(0..1000)).unwrap();
        let refused = store.blobs().put(&lines(0..900)).unwrap();
        let paths = ["a/new", "b/new"].map(|p| RepoPath::new(p).unwrap());
        let added = [(&paths[0], file(new))];
        let deleted = [(&paths[1], file(refused))];
        let mut reads = Vec::new();
        let read = |_: &RepoPath, blob, _| {
            reads.push(blob);
            if blob == refused {
                return Ok(None);
            }
            Ok(Some(Chunks::read(&store, blob)?))
        };
        let mut cost = 0;
        let budget = Budget {
            cost: &mut cost,
            cap: usize::MAX,
            progress: false,
        };
        let from = Progress::default();
        let found = renames(&store, &added, &deleted, read, MAX_PAIRS, from, budget).unwrap();
        assert_eq!(found.pairs, []);
        assert_eq!(reads, [new, refused]);
    }
}
