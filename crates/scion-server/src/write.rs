//! Writes: create changesets. Moves no ref. An upload goes straight to the
//! blob folder (`Blobs::upload`).

use std::collections::BTreeSet;

use rusqlite::Connection;
use scion_core::{Changeset, ChangesetId};

use crate::db::{self, RepoId};
use crate::error::{Error, Result};
use crate::store::{BlobStatus, Store};
use crate::tree;

/// Most path changes in one created changeset.
pub const MAX_CHANGES: usize = 100_000;
/// Longest commit message in a created changeset, in bytes.
pub const MAX_COMMIT_MESSAGE_BYTES: usize = 1 << 20;
/// Longest author name or email in a created changeset, in bytes.
pub const MAX_IDENTITY_BYTES: usize = 1 << 10;

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
    // A cheap read first, so a request on a missing repo or parent reads no
    // blob. The transaction checks again.
    let repo_id = store.read(|c| check_parent(c, repo, parent))?;
    // The blob check touches only the object folder, so it runs before the
    // database lock is taken.
    // SHORTCUT: blobs are shared by all repos, so a changeset may name a blob
    // that another repo uploaded and then read it. Safe with one principal.
    // Upgrade before multi-tenant auth: a repo_blobs table checked here.
    check_blobs(store, &cs)?;
    // See `tree::store_changeset`. The write's flush covers these steps.
    tree::fill_root_tree(store, repo_id, parent)?;
    store.write(|c| {
        let repo_id = check_parent(c, repo, parent)?;
        if db::has_changeset(c, repo_id, id)? {
            // A retry. Its tree was checked and recorded the first time.
            return Ok(id);
        }
        tree::store_changeset(c, repo_id, &cs).map(|(id, _)| id)
    })
}

/// Checks that `repo` exists and has `parent`, and returns the repo.
fn check_parent(c: &Connection, repo: &str, parent: ChangesetId) -> Result<RepoId> {
    let repo_id = db::repo_id(c, repo)?;
    if !db::has_changeset(c, repo_id, parent)? {
        return Err(Error::InvalidArgument(format!(
            "parent {parent} is not in repo {repo:?}"
        )));
    }
    Ok(repo_id)
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
/// Reads every blob back, once however many paths name it, so a torn or
/// overwritten file is not accepted. Then syncs them (see
/// [`BlobBatch::keep`]), so the commit never names a blob that a crash can
/// lose.
fn check_blobs(store: &Store, cs: &Changeset) -> Result<()> {
    let puts = cs
        .changes
        .iter()
        .filter_map(|(path, state)| Some((path, state.as_ref()?.blob()?)));
    let mut blobs = store.blobs().batch();
    // A blob seen before was intact, or the loop would have stopped.
    let mut seen = BTreeSet::new();
    for (path, blob) in puts {
        if !seen.insert(blob) {
            continue;
        }
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
    use std::collections::BTreeMap;

    use scion_core::{BlobId, FileState, IdentityTime, RepoPath};

    use super::*;
    use crate::store::FSYNC_LEAVES_DRIVE_CACHE;

    fn changeset(parents: Vec<ChangesetId>) -> Changeset {
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
            changes: BTreeMap::new(),
        }
    }

    /// A store with repo `main` and an empty root changeset in it.
    fn store_with_root() -> (tempfile::TempDir, Store, ChangesetId) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let root = store
            .write(|c| {
                let repo = db::create_repo(c, "main")?;
                db::insert_changeset(c, repo, &changeset(vec![]))
            })
            .unwrap();
        (dir, store, root)
    }

    /// A child of `parent` that puts each `(path, blob)`.
    fn child(parent: ChangesetId, puts: &[(&str, BlobId)]) -> Vec<u8> {
        let mut cs = changeset(vec![parent]);
        for &(path, content) in puts {
            let file = FileState::Regular {
                content,
                executable: false,
            };
            cs.changes.insert(RepoPath::new(path).unwrap(), Some(file));
        }
        cs.encode()
    }

    #[test]
    fn a_changeset_makes_its_blobs_durable_before_it_commits() {
        let (_dir, store, root) = store_with_root();
        // Stored by an upload that never synced.
        let mut upload = store.blobs().batch();
        let blob = upload.put(b"new").unwrap();
        drop(upload);

        let before = store.blobs().full_flushes();
        create_changeset(&store, "main", &child(root, &[("f", blob)])).unwrap();
        // One flush for the blob before the commit, and one for the commit
        // where a plain fsync leaves it in the drive cache.
        assert_eq!(
            store.blobs().full_flushes() - before,
            1 + u64::from(FSYNC_LEAVES_DRIVE_CACHE)
        );
    }

    #[test]
    fn a_parent_without_a_root_tree_is_derived_in_steps_before_the_write() {
        let (_dir, store, root) = store_with_root();
        // Changesets without root trees, as after a version bump of the kind.
        let tip = store
            .write(|c| {
                let repo = db::repo_id(c, "main")?;
                (0..4).try_fold(root, |tip, _| {
                    db::insert_changeset(c, repo, &changeset(vec![tip]))
                })
            })
            .unwrap();
        let commits = store.commits();
        create_changeset(&store, "main", &child(tip, &[])).unwrap();
        // Five root trees in steps, then the write.
        assert_eq!(store.commits() - commits, crate::derived::steps(5) + 1);
    }

    #[test]
    fn a_blob_that_many_paths_name_is_read_once() {
        let (_dir, store, root) = store_with_root();
        let blob = store.blobs().put(b"same").unwrap();
        let paths: Vec<String> = (0..100).map(|i| format!("f{i}")).collect();
        let puts: Vec<(&str, BlobId)> = paths.iter().map(|p| (p.as_str(), blob)).collect();
        let before = store.blobs().verifies();
        create_changeset(&store, "main", &child(root, &puts)).unwrap();
        assert_eq!(store.blobs().verifies() - before, 1);
    }

    #[test]
    fn a_missing_repo_or_parent_fails_before_any_blob_is_read() {
        let (_dir, store, root) = store_with_root();
        let blob = store.blobs().put(b"new").unwrap();
        let before = store.blobs().verifies();
        let ok = child(root, &[("f", blob)]);
        assert!(matches!(
            create_changeset(&store, "absent", &ok),
            Err(Error::NotFound(_))
        ));
        let orphan = child(ChangesetId::from_bytes([7; 32]), &[("f", blob)]);
        assert!(matches!(
            create_changeset(&store, "main", &orphan),
            Err(Error::InvalidArgument(why)) if why.contains("is not in repo")
        ));
        assert_eq!(store.blobs().verifies(), before);
    }

    #[test]
    fn the_error_names_the_first_path_whose_blob_is_missing() {
        let (_dir, store, root) = store_with_root();
        // Path order is the reverse of blob ID order.
        let (x, y) = (BlobId::of(b"x"), BlobId::of(b"y"));
        let (first, second) = (x.max(y), x.min(y));
        let cs = child(root, &[("a", first), ("b", second)]);
        match create_changeset(&store, "main", &cs) {
            Err(Error::InvalidArgument(why)) => {
                assert!(why.contains(&first.to_string()), "{why}");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }
}
