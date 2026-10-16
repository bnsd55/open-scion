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
use std::time::Instant;

use gix::diff::blob::sources::byte_lines;
use gix::diff::blob::{Algorithm, Diff, InternedInput};
use rusqlite::Connection;
use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileState, IdentityTime, PathElem, RepoPath, Tree,
    TreeId,
};

use crate::db;
use crate::error::{Error, Result};
use crate::memory::{Bytes, Held, BYTES_PER_LINE};
use crate::read::{self, MAX_SCAN};
use crate::similar::{self, Budget, Chunks, MAX_PAIRS, Progress};
use crate::store::Store;
use crate::tree;

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
/// fit, plus a few files of at most [`crate::memory::Bytes::cap`] past it:
/// the first
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
    /// from the blamed path after a rename. Only the changesets that the
    /// reply's ranges name.
    pub changesets: Vec<(ChangesetId, IdentityTime, RepoPath)>,
    /// Set when more lines are left: the rest of this window, when the call
    /// stopped at a bound, or the next window's first line, when this
    /// window is complete. Pass it to the next call for the same file.
    /// `None` when the blame is done.
    pub resume: Option<Resume>,
}

/// Where a blame of one file goes on, so that the next call gives the same
/// answer as one call with no bound ([`blame`]). The default is a first
/// blame: the walk starts at the changeset, from line 1.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Resume {
    /// Set when the walk stopped at a bound: the changeset and path to go
    /// on from. `None` starts the walk at the changeset of the request.
    pub next: Option<(ChangesetId, RepoPath)>,
    /// Where the walk's first step goes on with a rename search
    /// ([`RenameSearch`]).
    pub search: RenameSearch,
    /// The first line, counting from 0, of the window the call answers.
    pub after_line: usize,
    /// How many lines the file that the request names has. A walk that
    /// starts at an older version reads a different text, so this comes
    /// with the cursor.
    pub counted: usize,
    /// The state of the window's lines, from `after_line` on: what the
    /// walk has named so far, and where the rest sit in the version at
    /// `next`. Empty for a fresh walk.
    pub window: Vec<Line>,
}

/// One line of the window a [`Resume`] carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Line {
    /// The walk has named the line: its changeset.
    Named(ChangesetId),
    /// The line is still open: its line in the version at `next`.
    Unnamed(usize),
}

/// Most lines of the blamed file one call answers. Bounds the ranges in a
/// reply: about 46 bytes each, so a reply stays under the 64 MiB message
/// limit. A client pages through the file with the reply's cursor.
pub const MAX_BLAME_WINDOW_LINES: usize = 1 << 20;

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
type Version = (RepoPath, BlobId, Option<HeldText>);

/// The version of the blamed file being explained: its path, blob and text.
#[derive(Clone, Copy)]
struct Newer<'a> {
    path: &'a RepoPath,
    blob: BlobId,
    text: &'a [u8],
}

/// What [`older_version`] found.
enum Older {
    /// The older version, if any, and the search that ended, so that a call
    /// that goes on there does not search again. The version's text, when
    /// set, holds its bytes ([`HeldText`]).
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
/// Every version read takes its bytes from `memory`, and the lines of the
/// blamed file take [`BYTES_PER_LINE`] each; the takes wait for the budget
/// to free up, until `until`. A file whose share is more than the whole
/// budget is refused: see [`memory`].
///
/// # Errors
///
/// [`Error::NotFound`] if the repo, the changeset or the file does not
/// exist. [`Error::InvalidArgument`] if the path is not a regular file, or
/// if `from` is not where a rename search of `path` at `cs` can stop.
/// [`Error::ResourceExhausted`] if the share of the file is more than the
/// whole budget, or a take waited past `until`.
pub fn blame(
    store: &Store,
    repo: &str,
    cs: ChangesetId,
    path: &RepoPath,
    resume: &Resume,
    memory: &Bytes,
    until: Option<Instant>,
) -> Result<Blame> {
    // A fresh walk starts at the changeset, at line 0, and names the lines
    // of the file there. A walk that goes on starts at the version the
    // cursor names, and the lines it still has open sit where the cursor
    // says. The lines outside the window are done: they hold a placeholder
    // changeset that no reply reports.
    let (start, start_path) = resume
        .next
        .clone()
        .unwrap_or((cs, path.clone()));
    let repo = store.read(|c| db::repo_id(c, repo))?;
    let root = tree::root_tree(store, repo, start)?;
    let mut blob = regular_blob(store, root, &start_path)?;
    // The text of the version the walk starts at. Its bytes stay taken for
    // the whole call: each step's older text replaces them when it becomes
    // the text being explained.
    let mut text_hold = memory.take(store.blobs().size(blob)?, until)?;
    let mut text = store.blobs().get(blob)?;
    let mut path = start_path;
    // The lines of the file that the request names. A walk that goes on
    // reads the version at `next`, which can be an older text of another
    // path, so its line count comes from the cursor.
    let counted = if resume.next.is_none() {
        byte_lines(&text).count()
    } else {
        resume.counted
    };
    // The rows of the answer, and the state of one diff, stay taken for the
    // whole call.
    let _state_hold = memory.take(counted * BYTES_PER_LINE, until)?;
    if resume.after_line > counted {
        return Err(Error::InvalidArgument(format!(
            "the cursor's line {} is past the {counted} lines of {path:?}",
            resume.after_line
        )));
    }
    // The window this call answers: the lines from `after_line`, up to the
    // window cap or the end of the file.
    let window_end = (resume.after_line + MAX_BLAME_WINDOW_LINES).min(counted);
    let open = window_end - resume.after_line;
    let here_lines = byte_lines(&text).count();
    let mut lines: Vec<(Option<ChangesetId>, usize)> = Vec::with_capacity(counted);
    match &resume.next {
        // A fresh walk: every line of the window is open, at its own place.
        None => {
            if !resume.window.is_empty() {
                return Err(Error::InvalidArgument(
                    "the cursor holds lines for a walk that starts at the changeset".into(),
                ));
            }
            lines.extend(std::iter::repeat_n((Some(OUTSIDE), 0), resume.after_line));
            lines.extend((resume.after_line..window_end).map(|at| (None, at)));
        }
        // A walk that goes on: the window's lines are where the cursor
        // says, in the version at `next`.
        Some(_) => {
            if resume.window.len() != open {
                return Err(Error::InvalidArgument(format!(
                    "the cursor holds {} lines of a window of {open}",
                    resume.window.len()
                )));
            }
            for line in &resume.window {
                match line {
                    Line::Named(id) => lines.push((Some(*id), 0)),
                    Line::Unnamed(origin) => {
                        if *origin >= here_lines {
                            return Err(Error::InvalidArgument(format!(
                                "the cursor's line {} is past the {here_lines} lines of the version at {start}",
                                origin + 1
                            )));
                        }
                        lines.push((None, *origin));
                    }
                }
            }
        }
    }
    lines.extend(std::iter::repeat_n(
        (Some(OUTSIDE), 0),
        counted - window_end,
    ));
    let mut changesets = Vec::new();
    let mut next = Some(start);
    let mut next_search = RenameSearch::default();
    let mut budget = MAX_SCAN;
    let mut diffed = 0;
    let mut from = resume.search.clone();
    let done = |lines: &[(Option<ChangesetId>, usize)]| {
        lines[resume.after_line..window_end]
            .iter()
            .all(|(source, _)| source.is_some())
    };
    for _ in 0..MAX_BLAME_VERSIONS {
        if done(&lines) {
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
        // A search to go on with is the first step's, at the walk's start.
        let search = std::mem::take(&mut from);
        if search != RenameSearch::default() && (id != start || next.is_none()) {
            return Err(Error::InvalidArgument(format!(
                "{start} does not add {path:?}, so it has no rename search"
            )));
        }
        // The version before `id`. If nothing is there, or no text blame
        // can diff, `id` added every line. A step runs if it fits in the
        // budget left, and in `memory`. The first always runs.
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
                older_version(store, root, &changeset, newer, budget, &search, memory, until)?
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
        // The state of this step's diff, past the share that `_state_hold`
        // reserves for the blamed file's lines. A diff that does not fit
        // the whole budget leaves the version absent, so the walk goes on:
        // the read still cost its size, as a refused read did.
        let mut diff_hold = None;
        let mut absent_read = 0;
        let older = match older {
            Some((older_path, older_blob, Some(older_text))) => {
                let both = byte_lines(&older_text).count().max(counted);
                let need = both.saturating_sub(counted) * BYTES_PER_LINE;
                if need > memory.cap() {
                    absent_read = older_text.len().div_ceil(64);
                    None
                } else {
                    diff_hold = Some(memory.take(need, until)?);
                    Some((older_path, older_blob, Some(older_text)))
                }
            }
            other => other,
        };
        if let Some((_, _, Some(older_text))) = &older {
            cost += diff_cost(older_text) + diff_cost(&text);
        }
        cost += absent_read;
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
        drop(diff_hold);
        if let Some((older_path, older_blob, older_text)) = older {
            (path, blob) = (older_path, older_blob);
            if let Some(HeldText { text: new_text, hold: mut new_hold }) = older_text {
                // The older text is the text being explained now: its bytes
                // replace the ones the last version held.
                new_hold.keep(new_text.len());
                Held::release(std::mem::replace(&mut text_hold, new_hold));
                text = new_text;
            }
        }
    }
    // The reply covers the window's lines only. The changesets named
    // outside it are not in the reply.
    let window_ranges = ranges(&lines[resume.after_line..window_end], resume.after_line);
    let named: Vec<ChangesetId> = window_ranges.iter().filter_map(|r| r.changeset).collect();
    changesets.retain(|(id, _, _)| named.contains(id));
    // More to blame: the rest of this window when the walk stopped at a
    // bound, or the next window from a fresh walk when this one is done.
    // The next call takes the window's lines from the cursor, so it does
    // not diff what this call already did.
    let resume = if done(&lines) {
        (window_end < counted).then(|| Resume {
            next: None,
            search: RenameSearch::default(),
            after_line: window_end,
            counted,
            window: Vec::new(),
        })
    } else if next.is_none() {
        return Err(Error::Corrupt(format!(
            "no changeset on the history of {start} adds {path:?}"
        )));
    } else {
        Some(Resume {
            next: next.map(|id| (id, path.clone())),
            search: next_search,
            after_line: resume.after_line,
            counted,
            window: lines[resume.after_line..window_end]
                .iter()
                .map(|&(source, at)| match source {
                    Some(id) => Line::Named(id),
                    None => Line::Unnamed(at),
                })
                .collect(),
        })
    };
    Ok(Blame {
        ranges: window_ranges,
        changesets,
        resume,
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

/// A text that blame read, and the bytes it holds. Dropping it gives the
/// bytes back.
struct HeldText {
    text: Vec<u8>,
    hold: Held,
}

impl std::ops::Deref for HeldText {
    type Target = Vec<u8>;

    fn deref(&self) -> &Vec<u8> {
        &self.text
    }
}

/// Reads the text of `blob`, the file at `path`, with its bytes taken from
/// `memory`. `None` if the bytes do not fit the whole budget: the version
/// counts as absent, so the walk goes on.
///
/// # Errors
///
/// [`Error::ResourceExhausted`] if a take waited past `until`.
fn read_version(
    store: &Store,
    blob: BlobId,
    memory: &Bytes,
    until: Option<Instant>,
) -> Result<Option<HeldText>> {
    let size = store.blobs().size(blob)?;
    if size > memory.cap() {
        return Ok(None);
    }
    let hold = memory.take(size, until)?;
    Ok(Some(HeldText {
        text: store.blobs().get(blob)?,
        hold,
    }))
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
///
/// The bytes of the text it reads stay taken from `memory`: the second
/// return value holds them, and pairs with the text. A version whose bytes
/// do not fit the whole budget counts as absent, so the walk goes on.
fn older_version(
    store: &Store,
    root: TreeId,
    changeset: &Changeset,
    newer: Newer<'_>,
    budget: Budget<'_>,
    search: &RenameSearch,
    memory: &Bytes,
    until: Option<Instant>,
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
            let text = read_version(store, content, memory, until)?;
            text.map(|text| (newer.path.clone(), content, Some(text)))
        }
        // As in git, only a path with no file before can be a rename. A
        // symlink or submodule that becomes a regular file is not one.
        None | Some(Entry::Dir(_)) => {
            return renamed_from(store, root, changeset, newer, budget, search, memory, until);
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
    memory: &Bytes,
    until: Option<Instant>,
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
        let read = |_other: &RepoPath, other_blob: BlobId, size: usize| {
            if other_blob == newer.blob {
                return Ok(Some(Chunks::of(newer.text)));
            }
            if size > memory.cap() {
                return Ok(None);
            }
            // The bytes are taken only while the text is read: the chunks
            // hold the lines, and their cost counts against the diff-line
            // budget above.
            let hold = memory.take(size, until)?;
            let text = store.blobs().get(other_blob)?;
            let chunks = Chunks::of(&text);
            drop(text);
            Held::release(hold);
            Ok(Some(chunks))
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
    // The search kept only its chunks, so read it again to diff it. Its
    // bytes stay taken for the step that diffs it.
    let text = read_version(store, other_blob, memory, until)?;
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
/// Groups consecutive lines with the same changeset into ranges. `first`
/// is the line, counting from 0, that `lines[0]` is in the blamed file.
fn ranges(lines: &[(Option<ChangesetId>, usize)], first: usize) -> Vec<BlameRange> {
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
                start_line: line_u32(first + line + 1),
                line_count: 1,
                changeset,
                origin_line,
            }),
        }
    }
    ranges
}

/// A line number as a `u32`. A file of at most [`scion_core`] import limits
/// has far fewer than `u32::MAX` lines.
fn line_u32(line: usize) -> u32 {
    u32::try_from(line).expect("a blamed file has fewer than 2^32 lines")
}

// ---- the cursor ----
//
// The reply's cursor holds every field a resumed call needs: where the walk
// goes on, where its rename search stopped, and the window's first line. It
// is opaque to the client, which sends it back unchanged with the same
// repo, changeset and path. The server checks all of it.

/// A changeset for the lines a call does not answer: the ones outside its
/// window. They are done, and no reply reports them.
const OUTSIDE: ChangesetId = ChangesetId::from_bytes([0; 32]);

/// The version of the cursor format.
const CURSOR_VERSION: u8 = 1;

/// Binds a cursor to one blame of one file: a hash of the changeset and
/// path of the request that made it.
fn cursor_digest(cs: ChangesetId, path: &RepoPath) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(cs.as_bytes());
    hasher.update(path.as_bytes());
    hasher.finalize().as_bytes()[..16]
        .try_into()
        .expect("16 bytes of a 32-byte hash")
}

/// A stretch of a window's lines that the walk named together, or that are
/// still open together.
struct Run {
    len: u32,
    state: Line,
}

impl Run {
    /// Splits `lines` into runs: consecutive lines with the same changeset,
    /// or consecutive open lines whose places in the version at `next` are
    /// consecutive too. A diff can delete lines between the ones it keeps,
    /// so open lines with a gap in their places are runs of their own.
    fn runs(lines: &[Line]) -> Vec<Run> {
        let mut runs: Vec<Run> = Vec::new();
        for line in lines {
            let merge = match (&mut runs.last_mut(), line) {
                (
                    Some(Run {
                        len,
                        state: Line::Named(last),
                    }),
                    Line::Named(id),
                ) if *last == *id => {
                    *len += 1;
                    true
                }
                (
                    Some(Run {
                        len,
                        state: Line::Unnamed(first),
                    }),
                    Line::Unnamed(at),
                ) if *first + *len as usize == *at => {
                    *len += 1;
                    true
                }
                _ => false,
            };
            if !merge {
                runs.push(Run {
                    len: 1,
                    state: line.clone(),
                });
            }
        }
        runs
    }
}

fn put_bytes(w: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("a cursor field is shorter than 4 GiB");
    w.extend_from_slice(&len.to_be_bytes());
    w.extend_from_slice(bytes);
}

/// Encodes `resume` for a blame of the file at `path` in `cs`.
#[must_use]
pub fn encode_cursor(resume: &Resume, cs: ChangesetId, path: &RepoPath) -> Vec<u8> {
    let mut w = Vec::new();
    w.push(CURSOR_VERSION);
    w.extend_from_slice(&cursor_digest(cs, path));
    match &resume.next {
        None => w.push(0),
        Some((id, at)) => {
            w.push(1);
            w.extend_from_slice(id.as_bytes());
            put_bytes(&mut w, at.as_bytes());
        }
    }
    w.extend_from_slice(&u64::try_from(resume.search.searched)
        .unwrap_or(u64::MAX)
        .to_be_bytes());
    match &resume.search.best {
        None => w.push(0),
        Some(best) => {
            w.push(1);
            put_bytes(&mut w, best.as_bytes());
        }
    }
    w.extend_from_slice(
        &u64::try_from(resume.counted)
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    // The window's lines as runs: a stretch of lines that the walk named,
    // or one that is still open, with each open line's place in the
    // version at `next`.
    let runs = Run::runs(&resume.window);
    let count = u32::try_from(runs.len()).expect("a window holds fewer than 2^32 runs");
    w.extend_from_slice(&count.to_be_bytes());
    for run in &runs {
        w.extend_from_slice(&run.len.to_be_bytes());
        match &run.state {
            Line::Named(id) => {
                w.push(1);
                w.extend_from_slice(id.as_bytes());
            }
            Line::Unnamed(first) => {
                w.push(0);
                for at in *first..first + run.len as usize {
                    w.extend_from_slice(
                        &u32::try_from(at).unwrap_or(u32::MAX).to_be_bytes(),
                    );
                }
            }
        }
    }
    w
}

/// Decodes a cursor that a reply made for a blame of the file at `path` in
/// `cs`. The client sends it back unchanged; anything else is refused.
///
/// # Errors
///
/// [`Error::InvalidArgument`] for an unknown version, a wrong length, a
/// cursor made for another changeset or path, or a field out of range.
pub fn decode_cursor(bytes: &[u8], cs: ChangesetId, path: &RepoPath) -> Result<Resume> {
    let bad = |why: &str| Error::InvalidArgument(format!("bad blame cursor: {why}"));
    let mut r = CursorReader(bytes);
    if r.u8()? != CURSOR_VERSION {
        return Err(bad("unknown version"));
    }
    if r.array::<16>()? != cursor_digest(cs, path) {
        return Err(bad("it is for another changeset or path"));
    }
    let next = match r.u8()? {
        0 => None,
        1 => {
            let id = ChangesetId::from_bytes(r.array::<32>()?);
            let at = RepoPath::new(r.bytes_field()?).map_err(|_| bad("bad path"))?;
            Some((id, at))
        }
        _ => return Err(bad("bad walk flag")),
    };
    let searched = usize::try_from(r.u64()?).map_err(|_| bad("the search is too far"))?;
    let best = match r.u8()? {
        0 => None,
        1 => Some(RepoPath::new(r.bytes_field()?).map_err(|_| bad("bad best path"))?),
        _ => return Err(bad("bad best flag")),
    };
    let after_line = usize::try_from(r.u64()?).map_err(|_| bad("the line is too far"))?;
    let counted = usize::try_from(r.u64()?).map_err(|_| bad("the line count is too large"))?;
    if after_line > counted {
        return Err(bad("the window starts past the file"));
    }
    let open = (after_line + MAX_BLAME_WINDOW_LINES).min(counted) - after_line;
    let run_count = r.u32()?;
    let mut window = Vec::new();
    let mut total = 0usize;
    for _ in 0..run_count {
        let len = r.u32()?;
        let kind = r.u8()?;
        if kind > 1 {
            return Err(bad("bad line kind"));
        }
        let run_len = len as usize;
        total += run_len;
        if total > open {
            return Err(bad("the cursor's lines fill more than its window"));
        }
        match kind {
            0 => {
                for _ in 0..run_len {
                    window.push(Line::Unnamed(r.u32()? as usize));
                }
            }
            _ => {
                let id = ChangesetId::from_bytes(r.array::<32>()?);
                window.extend(std::iter::repeat_n(Line::Named(id), run_len));
            }
        }
    }
    if total != open {
        return Err(bad("the cursor's lines do not fill its window"));
    }
    r.finish()?;
    Ok(Resume {
        next,
        search: RenameSearch { searched, best },
        after_line,
        counted,
        window,
    })
}

/// Reads a cursor from the front of its bytes.
struct CursorReader<'a>(&'a [u8]);

impl CursorReader<'_> {
    fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let (head, rest) = self
            .0
            .split_first_chunk::<N>()
            .ok_or_else(|| Error::InvalidArgument("bad blame cursor: it is too short".into()))?;
        self.0 = rest;
        Ok(*head)
    }

    fn bytes_field(&mut self) -> Result<&[u8], Error> {
        let len = u32::from_be_bytes(self.array::<4>()?) as usize;
        let (head, rest) = self
            .0
            .split_at_checked(len)
            .ok_or_else(|| Error::InvalidArgument("bad blame cursor: it is too short".into()))?;
        self.0 = rest;
        Ok(head)
    }

    fn finish(self) -> Result<(), Error> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Error::InvalidArgument(
                "bad blame cursor: it has bytes past its fields".into(),
            ))
        }
    }
}
