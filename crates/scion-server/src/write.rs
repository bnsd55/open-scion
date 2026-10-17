//! Writes: store uploads and create changesets. Moves no ref. An upload goes
//! to a spool file in the blob folder (`Blobs::upload`) as it arrives.
//!
//! Blob files are shared by all repos, but each repo may name only its own
//! blobs (`repo_blobs`): those uploaded to it, and those that its changesets
//! name. So a changeset cannot name another repo's blob and then read it.

use std::collections::BTreeSet;

use scion_core::{BlobId, Changeset, ChangesetId, RepoPath};

use crate::db;
use crate::error::{Error, Result};
use crate::meta::{MetaTx, RepoId};
use crate::store::{BlobStatus, Store, Upload};
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
/// has, no committer, fits the request limits, and every blob it names is in
/// the repo and stored intact. [`Error::Apply`] if the changes do not apply
/// to the parent's tree. [`Error::Io`] if a blob cannot be read.
pub fn create_changeset(store: &Store, repo: &str, bytes: &[u8]) -> Result<ChangesetId> {
    let (cs, id) = decode(bytes)?;
    let parent = check_request(&cs)?;
    // A cheap read first, so a request on a missing repo or parent, or one
    // that names a blob the repo does not have, reads no blob. The
    // transaction checks the parent again. The member set changes only by
    // `gc`, which removes a row past the grace period that no stored
    // changeset names; the changeset's own transaction re-adds the row of
    // each blob it puts, so a stored changeset never names a blob that the
    // repo may not.
    let repo_id = store.read(|c| {
        let repo_id = check_parent(c, repo, parent)?;
        check_members(c, repo, repo_id, &cs)?;
        Ok(repo_id)
    })?;
    // The blob check touches only the object folder, so it runs before the
    // write transaction.
    check_blobs(store, repo, &cs)?;
    // See `tree::store_changeset`. The write's flush covers these steps.
    tree::fill_root_tree(store, repo_id, parent)?;
    store.write(|c| {
        let repo_id = check_parent(c, repo, parent)?;
        if c.has_changeset(repo_id, id)? {
            // A retry. Its tree was checked and recorded the first time.
            return Ok(id);
        }
        tree::store_changeset(c, repo_id, &cs).map(|(id, _)| id)
    })
}

/// Decodes a changeset that a client sent, with at most [`MAX_CHANGES`]
/// changes. A larger change count is refused before any change is read.
///
/// # Errors
///
/// [`Error::InvalidArgument`] if the bytes do not decode.
pub fn decode(bytes: &[u8]) -> Result<(Changeset, ChangesetId)> {
    Changeset::decode_with_id_max_changes(bytes, MAX_CHANGES)
        .map_err(|e| Error::InvalidArgument(format!("bad changeset: {e}")))
}

/// Stores a finished upload and adds the blob to the blobs of `repo`.
/// Returns once both are durable. Safe to repeat.
///
/// An intact stored copy is kept only if `repo` has the blob already. Else
/// the file is written again, so the time of the reply does not show whether
/// another repo stored the same content.
///
/// # Errors
///
/// Database errors, and as [`Blobs::put_upload`](crate::store::Blobs::put_upload).
pub fn put_blob(store: &Store, repo: RepoId, upload: Upload) -> Result<BlobId> {
    let id = upload.id();
    // The flush below covers what this read saw.
    let member = store.read_unflushed(|c| c.repo_has_blob(repo, id))?;
    store.blobs().put_upload(upload, member)?;
    if !member {
        // One flush covers the file and this row. A row whose blob a crash
        // lost grants nothing: `create_changeset` reads every blob back.
        store.commit(|c| c.add_repo_blob(repo, id))?;
    }
    store.flush()?;
    Ok(id)
}

/// Why a blob that a changeset puts is refused.
fn bad_blob(blob: BlobId, path: &RepoPath, problem: &str, repo: &str) -> Error {
    Error::InvalidArgument(format!(
        "blob {blob} for {path:?} {problem} in repo {repo:?}; upload it with PutBlob"
    ))
}

/// Fails on the first put, in path order, whose blob `repo` may not name.
/// It gets the answer of a blob that is not stored, and no blob is read, so
/// neither the answer nor its time shows whether another repo has the blob.
fn check_members(c: &mut dyn MetaTx, repo: &str, repo_id: RepoId, cs: &Changeset) -> Result<()> {
    let mut seen = BTreeSet::new();
    for (path, blob) in db::puts(cs) {
        if seen.insert(blob) && !c.repo_has_blob(repo_id, blob)? {
            return Err(bad_blob(blob, path, "is not stored", repo));
        }
    }
    Ok(())
}

/// Checks that `repo` exists and has `parent`, and returns the repo.
fn check_parent(c: &mut dyn MetaTx, repo: &str, parent: ChangesetId) -> Result<RepoId> {
    let repo_id = c.repo_id(repo)?;
    if !c.has_changeset(repo_id, parent)? {
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
/// lose. Call it only after [`check_members`]: every blob here is the repo's.
///
/// [`BlobBatch::keep`]: crate::store::BlobBatch::keep
fn check_blobs(store: &Store, repo: &str, cs: &Changeset) -> Result<()> {
    let mut blobs = store.blobs().batch();
    // A blob seen before was intact, or the loop would have stopped.
    let mut seen = BTreeSet::new();
    for (path, blob) in db::puts(cs) {
        if !seen.insert(blob) {
            continue;
        }
        let problem = match blobs.keep(blob)? {
            BlobStatus::Valid => continue,
            BlobStatus::Missing => "is not stored",
            BlobStatus::Corrupt => "is corrupt",
        };
        return Err(bad_blob(blob, path, problem, repo));
    }
    blobs.sync()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use scion_core::{FileState, GitOid, IdentityTime};

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
                let repo = c.create_repo("main")?;
                c.insert_changeset(repo, &changeset(vec![]))
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

    /// Repo `name`, made if missing.
    fn repo(store: &Store, name: &str) -> RepoId {
        store.write(|c| c.create_repo(name)).unwrap()
    }

    /// Uploads `content` to `repo`, as `PutBlob` does.
    fn upload(store: &Store, repo: RepoId, content: &[u8]) -> BlobId {
        let upload = store.blobs().upload().unwrap().append(content).unwrap();
        put_blob(store, repo, upload).unwrap()
    }

    /// Adds `blob` to the blobs of `main`, without storing it.
    fn give_main(store: &Store, blob: BlobId) {
        store
            .write(|c| {
                let repo = c.repo_id("main")?;
                c.add_repo_blob(repo, blob)
            })
            .unwrap();
    }

    /// The `InvalidArgument` text of a `create_changeset` that fails, with
    /// the blob IDs in it replaced.
    fn refusal(store: &Store, bytes: &[u8], blobs: &[BlobId]) -> String {
        match create_changeset(store, "main", bytes) {
            Err(Error::InvalidArgument(why)) => blobs
                .iter()
                .fold(why, |why, blob| why.replace(&blob.to_string(), "<blob>")),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn an_upload_keeps_a_stored_copy_only_for_a_blob_of_its_repo() {
        use std::os::unix::fs::MetadataExt as _;
        let (_dir, store, _root) = store_with_root();
        let (main, other) = (repo(&store, "main"), repo(&store, "other"));
        let inode = |id| {
            let (_, path) = store.blobs().location(id);
            std::fs::metadata(path).unwrap().ino()
        };
        let id = upload(&store, main, b"same");
        let first = inode(id);
        upload(&store, main, b"same");
        assert_eq!(inode(id), first, "main has it, so the stored file stays");
        // Else the reply would come sooner than for content stored nowhere,
        // and show that another repo has it.
        upload(&store, other, b"same");
        assert_ne!(inode(id), first, "other did not have it: a new file");
        assert!(store.read(|c| c.repo_has_blob(other, id)).unwrap());
        assert_eq!(store.blobs().get(id).unwrap(), b"same");
    }

    #[test]
    fn an_upload_shares_one_full_flush_with_its_row() {
        let (_dir, store, _root) = store_with_root();
        let main = repo(&store, "main");
        let before = store.blobs().full_flushes();
        upload(&store, main, b"new");
        // The rename shares one full flush: it covers the temp file before
        // the rename, so a stored blob it replaces is never left with data
        // that a power loss can lose. Where a plain fsync leaves data in the
        // drive cache, the row's flush is one more.
        assert_eq!(
            store.blobs().full_flushes() - before,
            1 + u64::from(FSYNC_LEAVES_DRIVE_CACHE)
        );
    }

    #[test]
    fn a_blob_of_another_repo_is_refused_before_it_is_read() {
        let (_dir, store, root) = store_with_root();
        let secret = upload(&store, repo(&store, "other"), b"secret");
        let never = BlobId::of(b"stored nowhere");
        let before = store.blobs().verifies();
        let names = |blob| child(root, &[("f", blob)]);
        // The same answer as for content that no repo stored, and the file is
        // not read, so neither the answer nor its time shows that `other`
        // has it.
        let answer = refusal(&store, &names(secret), &[secret]);
        assert_eq!(answer, refusal(&store, &names(never), &[never]));
        assert!(
            answer.contains("is not stored in repo \"main\""),
            "{answer}"
        );
        assert_eq!(store.blobs().verifies(), before);
        // Once uploaded to `main` too, it is accepted.
        upload(&store, repo(&store, "main"), b"secret");
        create_changeset(&store, "main", &names(secret)).unwrap();
    }

    #[test]
    fn a_blob_of_another_repo_fails_the_changeset_before_any_blob_is_read() {
        let (_dir, store, root) = store_with_root();
        let theirs = upload(&store, repo(&store, "other"), b"theirs");
        let mine = upload(&store, repo(&store, "main"), b"mine");
        let before = store.blobs().verifies();
        // `main`'s own blob comes first in path order, and is not read either.
        let why = refusal(&store, &child(root, &[("a", mine), ("b", theirs)]), &[]);
        let expected = format!("blob {theirs} for \"b\" is not stored in repo \"main\"");
        assert!(why.contains(&expected), "{why}");
        assert_eq!(store.blobs().verifies(), before);
    }

    #[test]
    fn a_gitlink_names_no_blob_whatever_its_bytes() {
        let (_dir, store, root) = store_with_root();
        let theirs = upload(&store, repo(&store, "other"), b"theirs");
        let module = FileState::Gitlink {
            commit: GitOid::Sha256(*theirs.as_bytes()),
        };
        let mut cs = changeset(vec![root]);
        cs.changes.insert(RepoPath::new("m").unwrap(), Some(module));
        let with_module = create_changeset(&store, "main", &cs.encode()).unwrap();
        // Storing the gitlink did not make the blob `main`'s.
        let named = child(with_module, &[("f", theirs)]);
        let why = refusal(&store, &named, &[theirs]);
        assert!(why.contains("is not stored"), "{why}");
    }

    #[test]
    fn a_changeset_makes_its_blobs_durable_before_it_commits() {
        let (_dir, store, root) = store_with_root();
        // Stored by an upload that never synced.
        let mut upload = store.blobs().batch();
        let blob = upload.put(b"new").unwrap();
        drop(upload);
        give_main(&store, blob);

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
                let repo = c.repo_id("main")?;
                (0..4).try_fold(root, |tip, _| {
                    c.insert_changeset(repo, &changeset(vec![tip]))
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
        let blob = upload(&store, repo(&store, "main"), b"same");
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
