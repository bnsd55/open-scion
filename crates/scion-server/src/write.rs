//! Writes: store blobs and create changesets. Neither moves a ref.

use scion_core::{BlobId, Changeset, ChangesetId};

use crate::db;
use crate::error::{Error, Result};
use crate::store::{BlobStatus, Store};
use crate::tree;

/// Most path changes in one created changeset.
pub const MAX_CHANGES: usize = 100_000;
/// Longest commit message in a created changeset, in bytes.
pub const MAX_COMMIT_MESSAGE_BYTES: usize = 1 << 20;
/// Longest author name or email in a created changeset, in bytes.
pub const MAX_IDENTITY_BYTES: usize = 1 << 10;

/// Stores one file's content for later use in a changeset. Safe to repeat.
///
/// # Errors
///
/// [`crate::Error::NotFound`] if the repo does not exist.
pub fn put_blob(store: &Store, repo: &str, data: &[u8]) -> Result<BlobId> {
    store.read(|c| db::repo_id(c, repo))?;
    store.blobs().put(data)
}

/// Stores a changeset sent as `ChangesetV1` bytes, with the root tree of its
/// parent plus its changes. Safe to repeat: the ID is the content.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo does not exist. [`Error::InvalidArgument`]
/// unless the bytes decode, the changeset has exactly one parent that the repo
/// has, no committer, fits the request limits, and every blob it names is
/// stored intact. [`Error::Apply`] if the changes do not apply to the parent's
/// tree. [`Error::Io`] if a blob cannot be read.
pub fn create_changeset(store: &Store, repo: &str, bytes: &[u8]) -> Result<ChangesetId> {
    let (cs, id) = Changeset::decode_with_id_max_changes(bytes, MAX_CHANGES)
        .map_err(|e| Error::InvalidArgument(format!("bad changeset: {e}")))?;
    let parent = check_request(&cs)?;
    // The blob check touches only the object folder, so it runs before the
    // database lock is taken.
    // SHORTCUT: blobs are shared by all repos, so a changeset may name a blob
    // that another repo uploaded and then read it. Safe with one principal.
    // Upgrade before multi-tenant auth: a repo_blobs table checked here.
    check_blobs(store, &cs)?;
    store.write(|c| {
        let repo_id = db::repo_id(c, repo)?;
        if db::has_changeset(c, repo_id, id)? {
            // A retry. Its tree was checked and recorded the first time.
            return Ok(id);
        }
        if !db::has_changeset(c, repo_id, parent)? {
            return Err(Error::InvalidArgument(format!(
                "parent {parent} is not in repo {repo:?}"
            )));
        }
        tree::store_changeset(c, repo_id, &cs).map(|(id, _)| id)
    })
}

/// Checks what a client may send, and returns the one parent.
fn check_request(cs: &Changeset) -> Result<ChangesetId> {
    let &[parent] = cs.parents.as_slice() else {
        return Err(Error::InvalidArgument(format!(
            "a changeset needs exactly one parent, got {}",
            cs.parents.len()
        )));
    };
    if cs.committer.is_some() {
        return Err(Error::InvalidArgument(
            "committer must be empty: only git import sets it".into(),
        ));
    }
    if cs.message.len() > MAX_COMMIT_MESSAGE_BYTES {
        return Err(Error::InvalidArgument(format!(
            "message is longer than {MAX_COMMIT_MESSAGE_BYTES} bytes"
        )));
    }
    if cs.author.name.len() > MAX_IDENTITY_BYTES || cs.author.email.len() > MAX_IDENTITY_BYTES {
        return Err(Error::InvalidArgument(format!(
            "author name or email is longer than {MAX_IDENTITY_BYTES} bytes"
        )));
    }
    Ok(parent)
}

/// Fails on the first put, in path order, whose blob is not stored intact.
/// Reads every blob back, so a torn or overwritten file is not accepted.
fn check_blobs(store: &Store, cs: &Changeset) -> Result<()> {
    let puts = cs
        .changes
        .iter()
        .filter_map(|(path, state)| Some((path, state.as_ref()?.blob()?)));
    for (path, blob) in puts {
        let problem = match store.blobs().verify(blob)? {
            BlobStatus::Valid => continue,
            BlobStatus::Missing => "is not stored",
            BlobStatus::Corrupt => "is corrupt",
        };
        return Err(Error::InvalidArgument(format!(
            "blob {blob} for {path:?} {problem}; upload it with PutBlob"
        )));
    }
    Ok(())
}
