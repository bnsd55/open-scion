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
use crate::similar::{self, Budget, Chunks, MAX_PAIRS, Progress};
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
/// the chunks it reads, the rows it indexes and compares, and the file
/// sizes it reads ([`similar::Budget`]), and a file read and refused counts
/// once per 64 bytes, so every byte read counts. The first step of a call
/// always runs, so a call always makes progress. A rename search uses only
/// the budget left; where it stops, the next call goes on with it
/// ([`Blame::next_search`]). So a call reads about 64 MiB in steps that
/// fit, plus a few files of at most [`MAX_BLAME_BYTES`] past it: the first
/// step can pass it by one file of a rename search, and the step that stops
/// the call reads one older version, or one more file of a rename search.
// SHORTCUT: the cost of a diff grows faster than its lines when lines
// repeat, so one diff of two files of MAX_BLAME_LINES lines is the worst
// case of a call. A changeset that deletes many large files costs one call
// per about 64 MiB of them. Upgrade: blame from derived data, so a call does
// not diff whole versions, and an index of deleted files by chunk (#61).
pub const MAX_BLAME_DIFF_LINES: usize = 1 << 20;

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
    /// With `next`: where the rename search stopped, when the call stopped
    /// inside the search for the file that the changeset at `next` renamed
    /// to its path. Pass it to the next call. Empty otherwise.
    pub next_search: RenameSearch,
}

/// Where a rename search stopped, so that the next call goes on with it
/// ([`blame`]). The default is a search not started.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RenameSearch {
    /// How many of the files that the changeset deletes, in path order,
    /// the search has looked at. Only paths that held a file count. When
    /// it is all of them, the search has ended, and `best` is its answer.
    pub searched: usize,
    /// The deleted file that the search picks among those, if any.
    pub best: Option<RepoPath>,
}

/// An older version of the blamed file: its path, blob and text. The text
/// is `None` when the content is the same as the newer version's, so it is
/// not read again.
type Version = (RepoPath, BlobId, Option<Vec<u8>>);

/// The version of the blamed file being explained: its path, blob and text.
#[derive(Clone, Copy)]
struct Newer<'a> {
    path: &'a RepoPath,
    blob: BlobId,
    text: &'a [u8],
}

/// What [`older_version`] found.
enum Older {
    /// The older version, if any. After a rename search, also the search
    /// that ended, so that a call that goes on there does not search again.
    Found(Option<Version>, RenameSearch),
    /// The rename search stopped at the budget, here.
    Stopped(RenameSearch),
}

/// Blames the file at `path` in `cs`. Diffs at most [`MAX_BLAME_VERSIONS`]
/// versions and about [`MAX_BLAME_DIFF_LINES`] lines, and checks at most
/// [`MAX_SCAN`] changesets. A binary file is blamed line by line too, as
/// `git blame` does. `from` goes on with a rename search that an earlier
/// call stopped ([`Blame::next_search`]); its default starts none.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo, the changeset or the file does not
/// exist. [`Error::InvalidArgument`] if the path is not a regular file, or
/// has more than [`MAX_BLAME_BYTES`] bytes or [`MAX_BLAME_LINES`] lines, or
/// if `from` is not where a rename search of `path` at `cs` can stop.
pub fn blame(
    store: &Store,
    repo: &str,
    cs: ChangesetId,
    path: &RepoPath,
    from: &RenameSearch,
) -> Result<Blame> {
    let repo = store.read(|c| db::repo_id(c, repo))?;
    let root = tree::root_tree(store, repo, cs)?;
    let mut blob = regular_blob(store, root, path)?;
    let mut text = read_text(store, blob, path, &mut 0)??;
    let mut path = path.clone();
    // For each line of the blamed file: its changeset once known, and until
    // then its line in `text`, the version being explained.
    let mut lines: Vec<(Option<ChangesetId>, usize)> = (0..byte_lines(&text).count())
        .map(|at| (None, at))
        .collect();
    let mut changesets = Vec::new();
    let mut next = Some(cs);
    let mut next_search = RenameSearch::default();
    let mut budget = MAX_SCAN;
    let mut diffed = 0;
    let mut from = from.clone();
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
        // A search to go on with is the first step's, at `cs`.
        let search = std::mem::take(&mut from);
        if search != RenameSearch::default() && (id != cs || next.is_none()) {
            return Err(Error::InvalidArgument(format!(
                "{cs} does not add {path:?}, so it has no rename search"
            )));
        }
        // The version before `id`. If nothing is there, or no text blame can
        // diff, `id` added every line. A step runs if it fits in the budget
        // left. The first always runs.
        let always = diffed == 0;
        let left = MAX_BLAME_DIFF_LINES.saturating_sub(diffed);
        let newer = Newer {
            path: &path,
            blob,
            text: &text,
        };
        let mut cost = 0;
        let found = match next {
            Some(parent) => {
                let root = tree::root_tree(store, repo, parent)?;
                let budget = Budget {
                    cost: &mut cost,
                    cap: left,
                    progress: always,
                };
                older_version(store, root, &changeset, newer, budget, &search)?
            }
            None => Older::Found(None, RenameSearch::default()),
        };
        let (older, done) = match found {
            Older::Found(older, done) => (older, done),
            Older::Stopped(stopped) => {
                // Not diffed: the next call goes on with the search at `id`.
                (next, next_search) = (Some(id), stopped);
                break;
            }
        };
        if let Some((_, _, Some(older_text))) = &older {
            cost += diff_cost(older_text) + diff_cost(&text);
        }
        if !always && cost > left {
            // Not diffed, so the next call starts at `id`, and finds the
            // file that this search found at once.
            (next, next_search) = (Some(id), done);
            break;
        }
        diffed += cost;
        if give_lines(&mut lines, id, older.as_ref(), &text) {
            changesets.push((id, changeset.author, path.clone()));
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
        next_search,
    })
}

/// The blob of the regular file at `path` in `root`.
///
/// # Errors
///
/// [`Error::NotFound`] if no file is there. [`Error::InvalidArgument`] if
/// it is not a regular file.
fn regular_blob(store: &Store, root: TreeId, path: &RepoPath) -> Result<BlobId> {
    match store.read(|c| tree::lookup(c, root, path))? {
        Some(Entry::File(FileState::Regular { content, .. })) => Ok(content),
        Some(_) => Err(Error::InvalidArgument(format!(
            "{path:?} is not a regular file"
        ))),
        None => Err(Error::NotFound(format!("path {path:?}"))),
    }
}

/// Moves each line that has no changeset yet to its line in `older`, the
/// version of `text` before `id`, or gives it `id` if `older` does not keep
/// it. Returns whether any line got `id`.
fn give_lines(
    lines: &mut [(Option<ChangesetId>, usize)],
    id: ChangesetId,
    older: Option<&Version>,
    text: &[u8],
) -> bool {
    let kept = match older {
        // The same content, as after a mode change or a rename with no
        // edit: every line stays where it is.
        Some((_, _, None)) => return false,
        Some((_, _, Some(older_text))) => kept_lines(older_text, text),
        None => vec![None; byte_lines(text).count()],
    };
    let mut named = false;
    for (source, at) in lines.iter_mut().filter(|(source, _)| source.is_none()) {
        if let Some(older_at) = kept[*at] {
            *at = older_at;
        } else {
            *source = Some(id);
            named = true;
        }
    }
    named
}

/// The text of `blob`, the file at `path`, if blame can read it. The inner
/// error says why not: it has more than [`MAX_BLAME_BYTES`] bytes or
/// [`MAX_BLAME_LINES`] lines.
///
/// Every read costs one per 64 bytes. The caller adds the cost of a text it
/// uses, when it diffs or compares it ([`diff_cost`], [`similar::Budget`]).
/// A text read and then refused for its lines adds it to `cost` here.
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

/// The version of the file before `changeset`, which puts `newer`: the file
/// at its path in `root`, the tree of its first parent, or else the file
/// that `changeset` renamed to that path ([`renamed_from`]). `None` if there
/// is none, or blame cannot read it. Adds the cost of what it reads and does
/// not return, and of a rename search, to the budget. `search` goes on with
/// a rename search; it must be the default unless there is one.
fn older_version(
    store: &Store,
    root: TreeId,
    changeset: &Changeset,
    newer: Newer<'_>,
    budget: Budget<'_>,
    search: &RenameSearch,
) -> Result<Older> {
    let entry = store.read(|c| tree::lookup(c, root, newer.path))?;
    if *search != RenameSearch::default() && matches!(entry, Some(Entry::File(_))) {
        return Err(Error::InvalidArgument(format!(
            "{:?} was there before, so it has no rename search",
            newer.path
        )));
    }
    let older = match entry {
        // Only the mode changed: nothing to read.
        Some(Entry::File(FileState::Regular { content, .. })) if content == newer.blob => {
            Some((newer.path.clone(), content, None))
        }
        Some(Entry::File(FileState::Regular { content, .. })) => {
            let text = read_text(store, content, newer.path, budget.cost)?.ok();
            text.map(|text| (newer.path.clone(), content, Some(text)))
        }
        // As in git, only a path with no file before can be a rename. A
        // symlink or submodule that becomes a regular file is not one.
        None | Some(Entry::Dir(_)) => {
            return renamed_from(store, root, changeset, newer, budget, search);
        }
        Some(Entry::File(_)) => None,
    };
    Ok(Older::Found(older, RenameSearch::default()))
}

/// The file that `changeset` renamed to the path where it puts `newer`.
/// `root` is the tree of its first parent. As `git blame` does by default
/// (renames, no copies), it picks among the files that `changeset` deletes,
/// with [`similar::renames`]: a file with the same content, else one with
/// the same name if at least 75% similar, else the most similar, if at
/// least 50% similar. As in git, a similar file is looked for among at most
/// [`similar::MAX_PAIRS`] deleted files.
///
/// The search stops at the cap of `budget`, and then says where
/// ([`Older::Stopped`]). `search` goes on from there: the result is the
/// same as that of one search with no cap. A `search` that has looked at
/// every deleted file has ended, and its best file is the answer.
///
/// Only a regular file can be the source, and only one that blame can read
/// can be similar. Adds the cost of the search, and of the files read and
/// refused, to the budget.
///
/// # Errors
///
/// [`Error::InvalidArgument`] if `search` is not where this search can
/// stop.
fn renamed_from(
    store: &Store,
    root: TreeId,
    changeset: &Changeset,
    newer: Newer<'_>,
    budget: Budget<'_>,
    search: &RenameSearch,
) -> Result<Older> {
    let Budget {
        cost,
        cap,
        progress,
    } = budget;
    let deleted = deleted_files(store, root, changeset)?;
    let bad = |why: &str| Error::InvalidArgument(format!("bad rename search: {why}"));
    if search.searched > deleted.len() {
        return Err(bad("past the deleted files"));
    }
    // A regular file pairs only with a regular file.
    let best = match &search.best {
        Some(best) => match deleted.binary_search_by(|&(other, _)| other.cmp(best)) {
            Ok(d) if d < search.searched && matches!(deleted[d].1, FileState::Regular { .. }) => {
                Some(d)
            }
            _ => return Err(bad("the best file is not a regular file searched")),
        },
        None => None,
    };
    // The deleted file it came from, and the files looked at so far.
    let (source, searched) = if search.searched == deleted.len() {
        (best, search.searched)
    } else {
        let added = [(
            newer.path,
            FileState::Regular {
                content: newer.blob,
                executable: false,
            },
        )];
        // The cost of files read and refused for their lines.
        let mut refused = 0;
        let read = |other: &RepoPath, other_blob: BlobId, size: usize| {
            if other_blob == newer.blob {
                return Ok(Some(Chunks::of(newer.text)));
            }
            if size > MAX_BLAME_BYTES {
                return Ok(None);
            }
            let text = read_text(store, other_blob, other, &mut refused)?.ok();
            Ok(text.map(|text| Chunks::of(&text)))
        };
        let from = Progress {
            searched: search.searched,
            best,
        };
        let budget = Budget {
            cost: &mut *cost,
            cap,
            progress,
        };
        let outcome = similar::renames(store, &added, &deleted, read, MAX_PAIRS, from, budget)?;
        *cost += refused;
        let source = outcome.pairs.first().map(|pair| pair.deleted);
        (source, outcome.stopped.unwrap_or(deleted.len()))
    };
    let at = RenameSearch {
        searched,
        best: source.map(|d| deleted[d].0.clone()),
    };
    if searched < deleted.len() {
        return Ok(Older::Stopped(at));
    }
    let Some(d) = source else {
        return Ok(Older::Found(None, at));
    };
    let (other, state) = deleted[d];
    let FileState::Regular {
        content: other_blob,
        ..
    } = state
    else {
        return Err(Error::Corrupt(format!(
            "rename source {other:?} is not a regular file"
        )));
    };
    if other_blob == newer.blob {
        return Ok(Older::Found(
            Some((other.clone(), other_blob, None)),
            RenameSearch::default(),
        ));
    }
    // The search kept only its chunks, so read it again to diff it.
    let text = read_text(store, other_blob, other, cost)?.ok();
    Ok(Older::Found(
        text.map(|text| (other.clone(), other_blob, Some(text))),
        at,
    ))
}

/// Each file that `changeset` deletes, in path order, as it was in `root`,
/// the tree of its first parent.
fn deleted_files<'c>(
    store: &Store,
    root: TreeId,
    changeset: &'c Changeset,
) -> Result<Vec<(&'c RepoPath, FileState)>> {
    store.read(|c| {
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
    })
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
