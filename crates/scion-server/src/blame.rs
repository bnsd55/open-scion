//! Blame: which changeset last changed each line of a file, on first-parent
//! history.
//!
//! Computed on each call. Walk first parents to each changeset that changed
//! the file, diff its version with the version in its first parent, and give
//! the added lines to that changeset. Where a changeset adds the file, the
//! version before it can be a file that the same changeset deletes: a
//! rename, found as `git blame` finds it. Stop when every line has a
//! changeset, or at a bound.

use std::collections::HashMap;
use std::collections::hash_map::Entry as Slot;

use gix::diff::blob::sources::byte_lines;
use gix::diff::blob::{Algorithm, Diff, InternedInput};
use rusqlite::Connection;
use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileState, IdentityTime, PathElem, RepoPath, Tree,
    TreeId,
};

use crate::db;
use crate::error::{Error, Result};
use crate::read::{self, MAX_SCAN};
use crate::similar::{self, Budget, Chunks};
use crate::store::Store;
use crate::tree;

/// Largest file blame reads, in bytes. Bounds the memory of one version:
/// a call holds at most two, two versions or one and a rename source, and
/// the chunk tables of two files.
// SHORTCUT: a larger file is refused, and an older version over the limit
// counts as absent, so its lines go to the changeset that replaced it. git
// reads any size. Upgrade: blame from derived data, so a call does not read
// whole versions (#90).
pub const MAX_BLAME_BYTES: usize = 16 << 20;

/// Most lines blame reads in one version. Bounds the lines of one diff,
/// and the ranges in a reply: about 46 bytes each, so a reply stays under
/// the 64 MiB message limit.
// SHORTCUT: a file with more lines is refused, and an older version with
// more counts as absent. git reads any number. Upgrade: blame from derived
// data, and a reply that a client asks for in parts (#90).
pub const MAX_BLAME_LINES: usize = 1 << 20;

/// Most versions of a file one call diffs.
// SHORTCUT: a file with many versions costs one call per 100 versions.
// Upgrade: blame as derived data, kept per file version.
pub const MAX_BLAME_VERSIONS: usize = 100;

/// Most lines one call diffs, counting both sides of each diff. A diff of
/// many repeated lines can take seconds, so this bounds the time of a call.
/// A line counts once per 64 bytes ([`diff_cost`]), a rename search counts
/// each chunk it reads and compares ([`similar::Budget`]), and a file read
/// and refused counts
/// once per 64 bytes, so every byte read counts. The first step of a call
/// always runs, so a call always makes progress. So a call reads about
/// 64 MiB in steps that fit, plus two steps past the budget: the first, and
/// the one that stops the call. One step reads one older version (at most
/// [`MAX_BLAME_BYTES`]) or one rename search (about [`MAX_RENAME_BYTES`]
/// and one more file).
// SHORTCUT: the cost of a diff grows faster than its lines when lines
// repeat, so one diff of two files of MAX_BLAME_LINES lines is the worst
// case of a call. Upgrade: blame from derived data, so a call does not diff
// whole versions.
pub const MAX_BLAME_DIFF_LINES: usize = 1 << 20;

/// Most files a changeset can delete for blame to compare each of them with
/// a file the changeset adds, to find a rename with edits.
// SHORTCUT: when a changeset deletes more files, only a file with the same
// content, or the one deleted file with the same name, can be the source.
// git compares them all. This bounds the files whose size one search
// checks. Upgrade: an index of each changeset's deleted files by chunk, as
// derived data, so a search compares only files that share chunks with the
// new file (#87).
pub const MAX_RENAME_SOURCES: usize = 100;

/// Most bytes one rename search reads, about: [`MAX_RENAME_SOURCES`] files of
/// 1 MiB, the old file limit, so a search still reads every file it read
/// under that limit. Past it, the search stops and keeps the best file so
/// far. It is the same for every search, so the source does not depend on
/// where a call stops.
// SHORTCUT: with larger files a search can stop before it reaches the
// source, and pick another file or none. git reads them all.
// Upgrade: an index of each changeset's deleted files by chunk, as derived
// data, so a search reads only files that share chunks with the new file
// (#87).
pub const MAX_RENAME_BYTES: usize = MAX_RENAME_SOURCES << 20;

/// Consecutive lines with the same source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlameRange {
    /// The first line, counting from 1, in the blamed file.
    pub start_line: u32,
    /// How many lines.
    pub line_count: u32,
    /// The changeset that last changed these lines. `None` when the call
    /// stopped first: see [`Blame::next`].
    pub changeset: Option<ChangesetId>,
    /// The first line, counting from 1, in the file at `changeset`, or at
    /// [`Blame::next`] when `changeset` is `None`.
    pub origin_line: u32,
}

/// The blame of one file.
#[derive(Debug)]
pub struct Blame {
    /// Ranges in file order. Together they cover every line once.
    pub ranges: Vec<BlameRange>,
    /// Each changeset that a range names, once each: its author, and the
    /// path of the file there, where `origin_line` counts. The path differs
    /// from the blamed path after a rename.
    pub changesets: Vec<(ChangesetId, IdentityTime, RepoPath)>,
    /// Set when the call stopped at a bound before every line had a
    /// changeset. Blame this path at this changeset to continue. The path
    /// differs from the blamed path after a rename.
    pub next: Option<(ChangesetId, RepoPath)>,
}

/// An older version of the blamed file: its path, blob and text. The text
/// is `None` when the content is the same as the newer version's, so it is
/// not read again.
type Version = (RepoPath, BlobId, Option<Vec<u8>>);

/// Blames the file at `path` in `cs`. Diffs at most [`MAX_BLAME_VERSIONS`]
/// versions and about [`MAX_BLAME_DIFF_LINES`] lines, and checks at most
/// [`MAX_SCAN`] changesets. A binary file is blamed line by line too, as
/// `git blame` does.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo, the changeset or the file does not
/// exist. [`Error::InvalidArgument`] if the path is not a regular file, or
/// has more than [`MAX_BLAME_BYTES`] bytes or [`MAX_BLAME_LINES`] lines.
pub fn blame(store: &Store, repo: &str, cs: ChangesetId, path: &RepoPath) -> Result<Blame> {
    let repo = store.read(|c| db::repo_id(c, repo))?;
    let root = tree::root_tree(store, repo, cs)?;
    let mut blob = match store.read(|c| tree::lookup(c, root, path))? {
        Some(Entry::File(FileState::Regular { content, .. })) => content,
        Some(_) => {
            return Err(Error::InvalidArgument(format!(
                "{path:?} is not a regular file"
            )));
        }
        None => return Err(Error::NotFound(format!("path {path:?}"))),
    };
    let mut text = read_text(store, blob, path, &mut 0)??;
    let mut path = path.clone();
    // For each line of the blamed file: its changeset once known, and until
    // then its line in `text`, the version being explained.
    let mut lines: Vec<(Option<ChangesetId>, usize)> = (0..byte_lines(&text).count())
        .map(|at| (None, at))
        .collect();
    let mut changesets = Vec::new();
    let mut next = Some(cs);
    let mut budget = MAX_SCAN;
    let mut diffed = 0;
    for _ in 0..MAX_BLAME_VERSIONS {
        if lines.iter().all(|(source, _)| source.is_some()) {
            break;
        }
        let Some((id, changeset)) =
            store.read(|c| read::next_change(c, repo, Some(&path), &mut next, &mut budget))?
        else {
            break;
        };
        let puts_text = matches!(
            changeset.changes.get(&path),
            Some(Some(FileState::Regular { content, .. })) if *content == blob
        );
        if !puts_text {
            return Err(Error::Corrupt(format!(
                "changeset {id} does not put the blamed version of {path:?}"
            )));
        }
        // The version before `id`. If nothing is there, or no text blame can
        // diff, `id` added every line.
        let mut cost = 0;
        let older = match next {
            Some(parent) => {
                let root = tree::root_tree(store, repo, parent)?;
                older_version(store, root, &changeset, &path, blob, &text, &mut cost)?
            }
            None => None,
        };
        if let Some((_, _, Some(older_text))) = &older {
            cost += diff_cost(older_text) + diff_cost(&text);
        }
        // A step runs if it fits in the budget left. The first always runs.
        if diffed > 0 && cost > MAX_BLAME_DIFF_LINES.saturating_sub(diffed) {
            // Not diffed, so the next call starts at `id`.
            next = Some(id);
            break;
        }
        diffed += cost;
        let kept = match &older {
            // The same content, as after a mode change or a rename with no
            // edit: every line stays where it is.
            Some((_, _, None)) => None,
            Some((_, _, Some(older_text))) => Some(kept_lines(older_text, &text)),
            None => Some(vec![None; byte_lines(&text).count()]),
        };
        if let Some(kept) = kept {
            let mut named = false;
            for (source, at) in lines.iter_mut().filter(|(source, _)| source.is_none()) {
                if let Some(older_at) = kept[*at] {
                    *at = older_at;
                } else {
                    *source = Some(id);
                    named = true;
                }
            }
            if named {
                changesets.push((id, changeset.author, path.clone()));
            }
        }
        if let Some((older_path, older_blob, older_text)) = older {
            (path, blob) = (older_path, older_blob);
            if let Some(older_text) = older_text {
                text = older_text;
            }
        }
    }
    if lines.iter().all(|(source, _)| source.is_some()) {
        next = None;
    } else if next.is_none() {
        return Err(Error::Corrupt(format!(
            "no changeset on the history of {cs} adds {path:?}"
        )));
    }
    Ok(Blame {
        ranges: ranges(&lines),
        changesets,
        next: next.map(|id| (id, path)),
    })
}

/// The text of `blob`, the file at `path`, if blame can read it. The inner
/// error says why not: it has more than [`MAX_BLAME_BYTES`] bytes or
/// [`MAX_BLAME_LINES`] lines.
///
/// Every read costs one per 64 bytes. The caller adds the cost of a text it
/// uses, when it diffs or compares it ([`diff_cost`], [`chunks`]). A text
/// read and then refused for its lines adds it to `cost` here.
///
/// # Errors
///
/// The outer error is a failure to read.
fn read_text(
    store: &Store,
    blob: BlobId,
    path: &RepoPath,
    cost: &mut usize,
) -> Result<Result<Vec<u8>>> {
    if store.blobs().size(blob)? > MAX_BLAME_BYTES {
        return Ok(Err(Error::InvalidArgument(format!(
            "{path:?} is larger than {MAX_BLAME_BYTES} bytes"
        ))));
    }
    let text = store.blobs().get(blob)?;
    if byte_lines(&text).nth(MAX_BLAME_LINES).is_some() {
        *cost += text.len().div_ceil(64);
        return Ok(Err(Error::InvalidArgument(format!(
            "{path:?} has more than {MAX_BLAME_LINES} lines"
        ))));
    }
    Ok(Ok(text))
}

/// What diffing `text` costs against [`MAX_BLAME_DIFF_LINES`]: one per line,
/// and one more per 64 bytes past the first 64 of a line.
fn diff_cost(text: &[u8]) -> usize {
    byte_lines(text).map(|line| line.len().div_ceil(64)).sum()
}

/// The version of the file before `changeset`, which puts `blob` and `text`
/// at `path`: the file at `path` in `root`, the tree of its first parent, or
/// else the file that `changeset` renamed to `path` ([`renamed_from`]).
/// `None` if there is none, or blame cannot read it. Adds the cost of what
/// it reads and does not return, and of a rename search, to `cost`.
fn older_version(
    store: &Store,
    root: TreeId,
    changeset: &Changeset,
    path: &RepoPath,
    blob: BlobId,
    text: &[u8],
    cost: &mut usize,
) -> Result<Option<Version>> {
    match store.read(|c| tree::lookup(c, root, path))? {
        // Only the mode changed: nothing to read.
        Some(Entry::File(FileState::Regular { content, .. })) if content == blob => {
            Ok(Some((path.clone(), content, None)))
        }
        Some(Entry::File(FileState::Regular { content, .. })) => {
            let text = read_text(store, content, path, cost)?.ok();
            Ok(text.map(|text| (path.clone(), content, Some(text))))
        }
        // As in git, only a path with no file before can be a rename. A
        // symlink or submodule that becomes a regular file is not one.
        None | Some(Entry::Dir(_)) => renamed_from(store, root, changeset, path, blob, text, cost),
        Some(Entry::File(_)) => Ok(None),
    }
}

/// The file that `changeset` renamed to `path`, where it puts `blob` and
/// `text`. `root` is the tree of its first parent. As `git blame` does by
/// default (renames, no copies), it picks among the files that `changeset`
/// deletes, with [`similar::renames`]: a file with the same content, else
/// one with the same name if at least 75% similar, else the most similar,
/// if at least 50% similar. A similar file is looked for only if the
/// changeset deletes at most [`MAX_RENAME_SOURCES`] files, and only until
/// the search has read [`MAX_RENAME_BYTES`].
///
/// Only a regular file can be the source, and only one that blame can read
/// can be similar. Adds the chunks compared, and the files read and
/// refused, to `cost`.
fn renamed_from(
    store: &Store,
    root: TreeId,
    changeset: &Changeset,
    path: &RepoPath,
    blob: BlobId,
    text: &[u8],
    cost: &mut usize,
) -> Result<Option<Version>> {
    // Each deleted file, in path order.
    let deleted: Vec<(&RepoPath, FileState)> = store.read(|c| {
        let mut trees = HashMap::new();
        let mut deleted = Vec::new();
        for (other, _) in changeset
            .changes
            .iter()
            .filter(|(_, state)| state.is_none())
        {
            if let Some(Entry::File(state)) = lookup_cached(c, &mut trees, root, other)? {
                deleted.push((other, state));
            }
        }
        Ok(deleted)
    })?;
    let added = [(
        path,
        FileState::Regular {
            content: blob,
            executable: false,
        },
    )];
    // The bytes of deleted files read, and the cost of files read and
    // refused for their lines.
    let (mut bytes, mut refused) = (0, 0);
    let read = |other: &RepoPath, other_blob: BlobId| {
        if other_blob == blob {
            return Ok(Some(Chunks::of(text)));
        }
        let size = store.blobs().size(other_blob)?;
        if bytes > MAX_RENAME_BYTES || size > MAX_BLAME_BYTES {
            return Ok(None);
        }
        bytes += size;
        let text = read_text(store, other_blob, other, &mut refused)?.ok();
        Ok(text.map(|text| Chunks::of(&text)))
    };
    let budget = Budget {
        cost: &mut *cost,
        cap: usize::MAX,
    };
    // With one added file, at most this many pairs means at most this many
    // deleted files.
    let found = similar::renames(store, &added, &deleted, read, MAX_RENAME_SOURCES, budget)?;
    *cost += refused;
    let Some(pair) = found.first() else {
        return Ok(None);
    };
    let (other, state) = deleted[pair.deleted];
    let FileState::Regular {
        content: other_blob,
        ..
    } = state
    else {
        return Err(Error::Corrupt(format!(
            "rename source {other:?} is not a regular file"
        )));
    };
    if other_blob == blob {
        return Ok(Some((other.clone(), blob, None)));
    }
    // The search read it, and kept only its chunks.
    let text = read_text(store, other_blob, other, cost)?.ok();
    Ok(text.map(|text| (other.clone(), other_blob, Some(text))))
}

/// [`tree::lookup`] that loads each tree at most once across calls.
fn lookup_cached(
    c: &Connection,
    trees: &mut HashMap<TreeId, Tree>,
    root: TreeId,
    path: &RepoPath,
) -> Result<Option<Entry>> {
    let mut entry = Entry::Dir(root);
    for name in path.as_bytes().split(|&b| b == b'/') {
        let Entry::Dir(id) = entry else {
            return Ok(None);
        };
        let tree = match trees.entry(id) {
            Slot::Occupied(slot) => slot.into_mut(),
            Slot::Vacant(slot) => slot.insert(db::get_tree(c, id)?),
        };
        let name = PathElem::new(name).expect("a RepoPath part is a valid name");
        match tree.entries.get(&name) {
            Some(next) => entry = *next,
            None => return Ok(None),
        }
    }
    Ok(Some(entry))
}

/// For each line of `new`: its line in `old` if the diff keeps it, `None` if
/// `new` adds it. A Myers diff with an indent heuristic, as `git blame` uses
/// by default. Where several minimal diffs exist, it can place a line
/// differently from git.
fn kept_lines(old: &[u8], new: &[u8]) -> Vec<Option<usize>> {
    let input = InternedInput::new(old, new);
    let mut diff = Diff::compute(Algorithm::Myers, &input);
    diff.postprocess_lines(&input);
    // Kept lines are in the same order on both sides.
    let mut old_line = 0;
    (0..input.after.len())
        .map(|line| {
            if diff.is_added(line_u32(line)) {
                return None;
            }
            while diff.is_removed(line_u32(old_line)) {
                old_line += 1;
            }
            old_line += 1;
            Some(old_line - 1)
        })
        .collect()
}

/// Groups lines, in file order, into ranges: the same source and
/// consecutive lines there.
fn ranges(lines: &[(Option<ChangesetId>, usize)]) -> Vec<BlameRange> {
    let mut ranges: Vec<BlameRange> = Vec::new();
    for (line, &(changeset, at)) in lines.iter().enumerate() {
        let origin_line = line_u32(at + 1);
        match ranges.last_mut() {
            Some(range)
                if range.changeset == changeset
                    && range.origin_line + range.line_count == origin_line =>
            {
                range.line_count += 1;
            }
            _ => ranges.push(BlameRange {
                start_line: line_u32(line + 1),
                line_count: 1,
                changeset,
                origin_line,
            }),
        }
    }
    ranges
}

/// A line number as a `u32`. A file of at most [`MAX_BLAME_LINES`] lines
/// has far fewer than `u32::MAX`.
fn line_u32(line: usize) -> u32 {
    u32::try_from(line).expect("a blamed file has fewer than 2^32 lines")
}
