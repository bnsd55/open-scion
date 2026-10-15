//! Reads. Each takes a changeset ID, so each answer is immutable.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::ops::Bound;

use rusqlite::Connection;
use scion_core::{Changeset, ChangesetId, Entry, FileState, PathElem, RepoPath, Tree, TreeId};

use crate::db::{self, RepoId};
use crate::error::{Error, Result};
use crate::store::Store;
use crate::tree;

/// Where a ref points.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or ref does not exist.
pub fn resolve_ref(store: &Store, repo: &str, name: &str) -> Result<ChangesetId> {
    store.read(|c| {
        let repo = db::repo_id(c, repo)?;
        db::get_ref(c, repo, name)?
            .map(|r| r.target)
            .ok_or_else(|| Error::NotFound(format!("ref {name:?}")))
    })
}

/// A changeset the repo may see.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or changeset does not exist.
pub fn get_changeset(store: &Store, repo: &str, id: ChangesetId) -> Result<Changeset> {
    store.read(|c| {
        let repo = db::repo_id(c, repo)?;
        db::get_changeset(c, repo, id)
    })
}

/// The root tree of a changeset the repo may see.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or changeset does not exist.
pub fn root_tree(store: &Store, repo: &str, cs: ChangesetId) -> Result<TreeId> {
    let repo = store.read(|c| db::repo_id(c, repo))?;
    tree::root_tree(store, repo, cs)
}

/// The content of a regular file, or the target of a symlink.
///
/// # Errors
///
/// [`Error::NotFound`] if nothing is at `path`. [`Error::InvalidArgument`] if
/// `path` is a directory or a submodule.
pub fn read_file(store: &Store, repo: &str, cs: ChangesetId, path: &RepoPath) -> Result<Vec<u8>> {
    let root = root_tree(store, repo, cs)?;
    match store.read(|c| tree::lookup(c, root, path))? {
        Some(Entry::File(state)) => match state.blob() {
            Some(blob) => store.blobs().get(blob),
            None => Err(Error::InvalidArgument(format!("{path:?} is a submodule"))),
        },
        Some(Entry::Dir(_)) => Err(Error::InvalidArgument(format!("{path:?} is a directory"))),
        None => Err(Error::NotFound(format!("path {path:?}"))),
    }
}

/// One directory level. `None` is the repo root.
///
/// # Errors
///
/// [`Error::NotFound`] if nothing is at `path`. [`Error::InvalidArgument`] if
/// `path` is a file.
pub fn list_dir(
    store: &Store,
    repo: &str,
    cs: ChangesetId,
    path: Option<&RepoPath>,
) -> Result<Tree> {
    let root = root_tree(store, repo, cs)?;
    store.read(|c| {
        let Some(path) = path else {
            return db::get_tree(c, root);
        };
        match tree::lookup(c, root, path)? {
            Some(Entry::Dir(id)) => db::get_tree(c, id),
            Some(Entry::File(_)) => Err(Error::InvalidArgument(format!("{path:?} is a file"))),
            None => Err(Error::NotFound(format!("path {path:?}"))),
        }
    })
}

/// Most changesets one call checks on a first-parent walk: a `Log` call, or
/// the walk of a `Blame` call. So it is also the most entries a `Log` reply
/// holds. Each check reads one changeset while the call holds the database
/// lock.
// SHORTCUT: path history and blame walk first parents on every call, so a
// path changed long ago costs one call per 1,000 changesets since. Upgrade:
// per-path history as derived data, so a call reads only the changesets that
// changed the path.
pub const MAX_SCAN: usize = 1000;

/// One page of first-parent history.
#[derive(Debug)]
pub struct LogPage {
    /// The changesets found, newest first.
    pub entries: Vec<(ChangesetId, Changeset)>,
    /// Where the next page starts: the first changeset this call did not
    /// check. `None` when the history ends.
    pub next: Option<ChangesetId>,
}

/// Up to `limit` changesets, walking first parents from `cs`, newest first.
/// For tests; the server reads one [`log_page`] per call.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or changeset does not exist.
#[cfg(test)]
pub fn log(
    store: &Store,
    repo: &str,
    cs: ChangesetId,
    limit: usize,
) -> Result<Vec<(ChangesetId, Changeset)>> {
    let mut entries = Vec::new();
    let mut next = Some(cs);
    while let Some(from) = next.filter(|_| entries.len() < limit) {
        let page = log_page(store, repo, from, None, limit - entries.len())?;
        entries.extend(page.entries);
        next = page.next;
    }
    Ok(entries)
}

/// Up to `limit` changesets, walking first parents from `cs`, newest first.
/// With a `path`, only the changesets that change it ([`changes_path`]).
/// Checks at most [`MAX_SCAN`] changesets.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or changeset does not exist.
pub fn log_page(
    store: &Store,
    repo: &str,
    cs: ChangesetId,
    path: Option<&RepoPath>,
    limit: usize,
) -> Result<LogPage> {
    store.read(|c| {
        let repo = db::repo_id(c, repo)?;
        let mut entries = Vec::new();
        let mut next = Some(cs);
        let mut budget = MAX_SCAN;
        while entries.len() < limit {
            match next_change(c, repo, path, &mut next, &mut budget)? {
                Some(entry) => entries.push(entry),
                None => break,
            }
        }
        Ok(LogPage { entries, next })
    })
}

/// Walks first parents from `next` to the newest changeset that changes
/// `path` ([`changes_path`]), or to the next changeset if `path` is `None`.
/// Checks at most `budget` changesets, and leaves `next` at the first one it
/// did not check.
pub(crate) fn next_change(
    c: &Connection,
    repo: RepoId,
    path: Option<&RepoPath>,
    next: &mut Option<ChangesetId>,
    budget: &mut usize,
) -> Result<Option<(ChangesetId, Changeset)>> {
    while let Some(id) = next.filter(|_| *budget > 0) {
        *budget -= 1;
        let changeset = db::get_changeset(c, repo, id)?;
        *next = changeset.parents.first().copied();
        if path.is_none_or(|path| changes_path(&changeset, path)) {
            return Ok(Some((id, changeset)));
        }
    }
    Ok(None)
}

/// Whether a changeset changes `path`: the file itself, or anything below it
/// if it is a directory. A changeset lists every file it changes, so this
/// needs no tree.
#[must_use]
pub fn changes_path(cs: &Changeset, path: &RepoPath) -> bool {
    let path = path.as_bytes();
    cs.changes.keys().any(|changed| {
        let changed = changed.as_bytes();
        changed.starts_with(path) && matches!(changed.get(path.len()), None | Some(b'/'))
    })
}

/// Most changesets one `MergeBase` or `IsAncestor` call walks. Each step
/// reads the parents of one changeset while the call holds the database
/// lock.
// SHORTCUT: graph queries walk the history on every call, and fail past
// this many changesets. Upgrade: a commit-graph index as derived data, such
// as the skip lists the article describes, so a call reads a few rows
// (https://github.com/bnsd55/open-scion/issues/72).
pub const MAX_WALK: usize = 10_000;

/// Whether `ancestor` is `descendant` or one of its ancestors, over all
/// parents. Walks at most [`MAX_WALK`] changesets, and none if `ancestor`
/// has the higher generation.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or a changeset does not exist.
/// [`Error::ResourceExhausted`] if the walk passes [`MAX_WALK`].
pub fn is_ancestor(
    store: &Store,
    repo: &str,
    ancestor: ChangesetId,
    descendant: ChangesetId,
) -> Result<bool> {
    store.read(|c| {
        let repo = db::repo_id(c, repo)?;
        db::is_ancestor(c, repo, ancestor, descendant, MAX_WALK)
    })
}

/// Reached from `a`.
const FROM_A: u8 = 1;
/// Reached from `b`.
const FROM_B: u8 = 2;
/// Reached from a common ancestor already found, so not a best one.
const STALE: u8 = 4;

/// The best common ancestors of `a` and `b`, over all parents: every common
/// ancestor that is not an ancestor of another common ancestor. This is the
/// rule of `git merge-base --all`. Usually there is one. After criss-cross
/// merges there can be several, and all are returned. Highest generation
/// first, then lowest ID. Empty if the histories share no changeset.
///
/// The walk takes changesets from a queue, highest generation first, and
/// marks each with the sides that reach it. A parent's generation is below
/// its child's, so a changeset leaves the queue only after every
/// descendant the walk reached, with all its marks. One marked from both
/// sides and not stale is a best common ancestor, and its ancestors become
/// stale. The walk ends when no queued changeset that is not stale is
/// marked from `a`, or none from `b`: no other best common ancestor can
/// follow. It takes at most [`MAX_WALK`] changesets from the queue.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or a changeset does not exist.
/// [`Error::ResourceExhausted`] if the walk passes [`MAX_WALK`].
pub fn merge_base(
    store: &Store,
    repo: &str,
    a: ChangesetId,
    b: ChangesetId,
) -> Result<Vec<ChangesetId>> {
    store.read(|c| {
        let repo = db::repo_id(c, repo)?;
        let mut marks: HashMap<ChangesetId, u8> = HashMap::new();
        let mut queue = BinaryHeap::new();
        for (id, side) in [(a, FROM_A), (b, FROM_B)] {
            let generation = db::generation(c, repo, id)?;
            let mark = marks.entry(id).or_insert(0);
            if *mark == 0 {
                queue.push((generation, Reverse(id)));
            }
            *mark |= side;
        }
        // Queued changesets that are not stale, marked from `a` and from `b`.
        let mut live = [0, 0];
        for mark in marks.values() {
            tally(&mut live, *mark, true);
        }
        let mut bases = Vec::new();
        let mut taken = 0;
        while let Some((generation, Reverse(id))) = queue.pop() {
            if live.contains(&0) {
                break;
            }
            let mut mark = marks[&id];
            tally(&mut live, mark, false);
            taken += 1;
            if taken > MAX_WALK {
                return Err(db::too_deep(MAX_WALK));
            }
            if mark & STALE == 0 && mark & (FROM_A | FROM_B) == FROM_A | FROM_B {
                bases.push(id);
                mark |= STALE;
            }
            for (parent, parent_generation) in db::parents(c, id)? {
                if parent_generation >= generation {
                    return Err(Error::Corrupt(format!(
                        "changeset {id} has generation {generation}, its parent {parent} \
                         {parent_generation}"
                    )));
                }
                let old = marks.get(&parent).copied();
                let new = old.unwrap_or(0) | mark;
                if old == Some(new) {
                    continue;
                }
                match old {
                    // Still queued: its generation is below the one taken.
                    Some(old) => tally(&mut live, old, false),
                    None => queue.push((parent_generation, Reverse(parent))),
                }
                tally(&mut live, new, true);
                marks.insert(parent, new);
            }
        }
        Ok(bases)
    })
}

/// Adds a queued changeset's marks to `live`, or removes them. A stale
/// changeset counts for neither side.
fn tally(live: &mut [usize; 2], mark: u8, add: bool) {
    if mark & STALE != 0 {
        return;
    }
    for (count, side) in live.iter_mut().zip([FROM_A, FROM_B]) {
        if mark & side != 0 {
            if add {
                *count += 1;
            } else {
                *count -= 1;
            }
        }
    }
}

/// Most paths one `Diff` call returns. Each path costs at most one tree read
/// per part on each side, while the call holds the database lock.
pub const MAX_DIFF_ENTRIES: usize = 1000;

/// A path whose file differs between two snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiffEntry {
    /// The path.
    pub path: RepoPath,
    /// The file at `path` in the old snapshot. `None` when no file is there:
    /// nothing, or a directory.
    pub old: Option<FileState>,
    /// The file at `path` in the new snapshot. `None` as for `old`.
    pub new: Option<FileState>,
}

/// One page of a diff.
#[derive(Debug)]
pub struct DiffPage {
    /// The paths that differ, in tree order ([`diff`]).
    pub entries: Vec<DiffEntry>,
    /// Set when the call stopped at [`MAX_DIFF_ENTRIES`]: the last path in
    /// `entries`. Diff again with `after` set to it to continue.
    pub next_after: Option<RepoPath>,
}

/// The paths whose file differs between the snapshots of `old` and `new`, in
/// tree order: paths compare part by part, and parts by raw bytes. A path
/// that is a file on one side and a directory on the other gives one entry
/// for the file, then one for each file below the directory. With `path`,
/// only that file or the files below that directory. With `after`, only the
/// paths after it. Returns at most [`MAX_DIFF_ENTRIES`] entries.
///
/// Walks both trees at once and skips a subtree with the same ID on both
/// sides. So it reads only the trees on the paths it returns, and on the
/// way down to `path` and `after`.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or a changeset does not exist.
// SHORTCUT: renames are not detected, so a rename is a delete and an add;
// `git diff` detects them by default. Upgrade: pair deletes and adds with
// the same blob first, then by similarity, with a bound per call
// (https://github.com/bnsd55/open-scion/issues/79).
pub fn diff(
    store: &Store,
    repo: &str,
    old: ChangesetId,
    new: ChangesetId,
    path: Option<&RepoPath>,
    after: Option<&RepoPath>,
) -> Result<DiffPage> {
    let repo = store.read(|c| db::repo_id(c, repo))?;
    let old_root = tree::root_tree(store, repo, old)?;
    let new_root = tree::root_tree(store, repo, new)?;
    let under = path.map(parts).unwrap_or_default();
    let after = after.map(parts);
    let mut walk = DiffWalk {
        path: Vec::new(),
        entries: Vec::new(),
    };
    store.read(|c| {
        walk.walk(
            c,
            Some(Entry::Dir(old_root)),
            Some(Entry::Dir(new_root)),
            &under,
            after.as_deref(),
        )
    })?;
    let mut entries = walk.entries;
    // The walk finds one entry more than a page, to know whether more follow.
    let next_after = if entries.len() > MAX_DIFF_ENTRIES {
        entries.truncate(MAX_DIFF_ENTRIES);
        entries.last().map(|entry| entry.path.clone())
    } else {
        None
    };
    Ok(DiffPage {
        entries,
        next_after,
    })
}

fn parts(path: &RepoPath) -> Vec<&[u8]> {
    path.as_bytes().split(|&b| b == b'/').collect()
}

/// The state of one [`diff`] walk.
struct DiffWalk {
    /// The path of the entries being compared, as raw bytes.
    path: Vec<u8>,
    /// What the walk found, up to one more than [`MAX_DIFF_ENTRIES`].
    entries: Vec<DiffEntry>,
}

impl DiffWalk {
    fn full(&self) -> bool {
        self.entries.len() > MAX_DIFF_ENTRIES
    }

    /// Compares `old` and `new`, the entries at `self.path`. `under` holds
    /// the parts of the requested path below here: it is empty once the walk
    /// is at or below that path. `after` holds the parts of the resume path
    /// below here, or is `None` once every path here comes after it.
    ///
    /// The recursion is as deep as a path has parts: at most 256, by the
    /// path rules.
    fn walk(
        &mut self,
        c: &Connection,
        old: Option<Entry>,
        new: Option<Entry>,
        under: &[&[u8]],
        after: Option<&[&[u8]]>,
    ) -> Result<()> {
        if old == new || self.full() {
            return Ok(());
        }
        let (old_file, new_file) = (file(old), file(new));
        // Here is in the requested path, and after the resume path.
        if old_file != new_file && under.is_empty() && after.is_none() {
            let path = RepoPath::new(self.path.clone())
                .map_err(|e| Error::Corrupt(format!("a tree path breaks the path rules: {e}")))?;
            self.entries.push(DiffEntry {
                path,
                old: old_file,
                new: new_file,
            });
        }
        let (old_dir, new_dir) = (children(c, old)?, children(c, new)?);
        // Names before the requested path or the resume path do not matter.
        let start = match under.first().or_else(|| after.and_then(<[_]>::first)) {
            Some(name) => Bound::Included(
                PathElem::new(name.to_vec()).expect("a RepoPath part is a valid name"),
            ),
            None => Bound::Unbounded,
        };
        let names: BTreeSet<&PathElem> = old_dir
            .range((start.clone(), Bound::Unbounded))
            .chain(new_dir.range((start, Bound::Unbounded)))
            .map(|(name, _)| name)
            .collect();
        for name in names {
            let name_under = match under.split_first() {
                Some((first, rest)) if name.as_bytes() == *first => rest,
                // Past the one name on the requested path.
                Some(_) => break,
                None => &[],
            };
            let name_after = match after {
                Some([first, rest @ ..]) => match name.as_bytes().cmp(first) {
                    Ordering::Less => continue,
                    Ordering::Equal => Some(rest),
                    Ordering::Greater => None,
                },
                // Here is the resume path, or after it: every name follows.
                _ => None,
            };
            let len = self.path.len();
            if len > 0 {
                self.path.push(b'/');
            }
            self.path.extend_from_slice(name.as_bytes());
            let (old_entry, new_entry) = (old_dir.get(name).copied(), new_dir.get(name).copied());
            self.walk(c, old_entry, new_entry, name_under, name_after)?;
            self.path.truncate(len);
            if self.full() {
                break;
            }
        }
        Ok(())
    }
}

/// The file state of an entry, if it is a file.
fn file(entry: Option<Entry>) -> Option<FileState> {
    match entry {
        Some(Entry::File(state)) => Some(state),
        _ => None,
    }
}

/// The entries of a directory. Empty for anything else.
fn children(c: &Connection, entry: Option<Entry>) -> Result<BTreeMap<PathElem, Entry>> {
    match entry {
        Some(Entry::Dir(id)) => Ok(db::get_tree(c, id)?.entries),
        _ => Ok(BTreeMap::new()),
    }
}
