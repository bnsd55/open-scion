//! Reads. Each takes a changeset ID, so each answer is immutable.

use rusqlite::Connection;
use scion_core::{Changeset, ChangesetId, Entry, RepoPath, Tree, TreeId};

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
/// Reads one [`log_page`] at a time, so it holds the database lock for at
/// most [`MAX_SCAN`] changesets at once.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo or changeset does not exist.
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
