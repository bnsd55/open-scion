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
/// Then syncs them (see [`BlobBatch::keep`]), so the commit never names a
/// blob that a crash can lose.
fn check_blobs(store: &Store, cs: &Changeset) -> Result<()> {
    let puts = cs
        .changes
        .iter()
        .filter_map(|(path, state)| Some((path, state.as_ref()?.blob()?)));
    let mut blobs = store.blobs().batch();
    for (path, blob) in puts {
        let problem = match blobs.keep(blob)? {
            BlobStatus::Valid => continue,
            BlobStatus::Missing => "is not stored",
            BlobStatus::Corrupt => "is corrupt",
        };
        return Err(Error::InvalidArgument(format!(
            "blob {blob} for {path:?} {problem}; upload it with PutBlob"
        )));
    }
    blobs.sync()
}

#[cfg(test)]
mod tests {
    use scion_core::{FileState, IdentityTime, RepoPath};

    use super::*;
    use crate::store::FSYNC_LEAVES_DRIVE_CACHE;

    fn changeset(parents: Vec<ChangesetId>, blob: Option<BlobId>) -> Changeset {
        Changeset {
            parents,
            author: IdentityTime {
                name: b"Ada".to_vec(),
                email: b"ada@example.com".to_vec(),
                unix_secs: 1_700_000_000,
                tz_offset_minutes: 0,
            },
            committer: None,
            message: b"change\n".to_vec(),
            changes: blob
                .map(|content| {
                    let file = FileState::Regular {
                        content,
                        executable: false,
                    };
                    (RepoPath::new("f").unwrap(), Some(file))
                })
                .into_iter()
                .collect(),
        }
    }

    #[test]
    fn a_changeset_makes_its_blobs_durable_before_it_commits() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let root = store
            .write(|c| {
                let repo = db::create_repo(c, "main")?;
                db::insert_changeset(c, repo, &changeset(vec![], None))
            })
            .unwrap();
        // Stored by an upload that never synced.
        let mut upload = store.blobs().batch();
        let blob = upload.put(b"new").unwrap();
        drop(upload);

        let before = store.blobs().full_flushes();
        let child = changeset(vec![root], Some(blob)).encode();
        create_changeset(&store, "main", &child).unwrap();
        // One flush for the blob before the commit, and one for the commit
        // where a plain fsync leaves it in the drive cache.
        assert_eq!(
            store.blobs().full_flushes() - before,
            1 + u64::from(FSYNC_LEAVES_DRIVE_CACHE)
        );

        // A blob that is durable already costs no flush: only the commit's.
        let synced = store.blobs().put(b"synced").unwrap();
        let before = store.blobs().full_flushes();
        let child = changeset(vec![root], Some(synced)).encode();
        create_changeset(&store, "main", &child).unwrap();
        assert_eq!(
            store.blobs().full_flushes() - before,
            u64::from(FSYNC_LEAVES_DRIVE_CACHE)
        );
    }
}
