//! Reads. Each takes a changeset ID, so each answer is immutable.

use scion_core::{Changeset, ChangesetId, Entry, RepoPath, Tree, TreeId};

use crate::db;
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

/// Up to `limit` changesets, walking first parents from `cs`, newest first.
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
    store.read(|c| {
        let repo = db::repo_id(c, repo)?;
        let mut out = Vec::new();
        let mut cur = Some(cs);
        while let Some(id) = cur.filter(|_| out.len() < limit) {
            let changeset = db::get_changeset(c, repo, id)?;
            cur = changeset.parents.first().copied();
            out.push((id, changeset));
        }
        Ok(out)
    })
}
