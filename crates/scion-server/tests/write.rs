//! `create_changeset`: validation, idempotency under concurrency, and the
//! root tree it records. Blob uploads over damaged files, and stored data
//! that does not match its ID.

use std::fs;

use rusqlite::{Connection, params};
use scion_core::{BlobId, Changeset, ChangesetId, Entry, FileState, IdentityTime, RepoPath};
use scion_server::error::ApplyError;
use scion_server::store::BlobStatus;
use scion_server::write::{
    MAX_CHANGES, MAX_COMMIT_MESSAGE_BYTES, MAX_IDENTITY_BYTES, create_changeset,
};
use scion_server::{Error, Store, db, tree};
use tempfile::TempDir;

const REPO: &str = "main";

/// A store with repo `main` (a root holding `a`, `dir/b`, `dir/c`, and a
/// `main` ref) and repo `other` (its own root).
struct Fixture {
    dir: TempDir,
    store: Store,
    root: ChangesetId,
    other_root: ChangesetId,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    for content in ["a", "b", "c", "new"] {
        store.blobs().put(content.as_bytes()).unwrap();
    }
    let (root, other_root) = store
        .write(|c| {
            let repo = db::create_repo(c, REPO)?;
            let root = db::insert_changeset(
                c,
                repo,
                &changeset(
                    vec![],
                    &[
                        ("a", Some(regular("a"))),
                        ("dir/b", Some(regular("b"))),
                        ("dir/c", Some(regular("c"))),
                    ],
                ),
            )?;
            assert!(db::cas_ref(c, repo, "main", None, root)?);
            // As if uploaded to `main`.
            for content in ["a", "b", "c", "new"] {
                db::add_repo_blob(c, repo, BlobId::of(content.as_bytes()))?;
            }
            let other = db::create_repo(c, "other")?;
            let other_root =
                db::insert_changeset(c, other, &changeset(vec![], &[("x", Some(regular("a")))]))?;
            Ok((root, other_root))
        })
        .unwrap();
    Fixture {
        dir,
        store,
        root,
        other_root,
    }
}

fn regular(content: &str) -> FileState {
    FileState::Regular {
        content: BlobId::of(content.as_bytes()),
        executable: false,
    }
}

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

fn changeset(parents: Vec<ChangesetId>, changes: &[(&str, Option<FileState>)]) -> Changeset {
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
        changes: changes.iter().map(|(p, s)| (path(p), *s)).collect(),
    }
}

/// The bytes of a changeset on `parent` whose change count is `n`, with no
/// change after it. The count is the last field when there are no changes.
fn change_count(parent: ChangesetId, n: usize) -> Vec<u8> {
    let mut bytes = changeset(vec![parent], &[]).encode();
    let count = bytes.len() - 4;
    bytes[count..].copy_from_slice(&u32::try_from(n).unwrap().to_be_bytes());
    bytes
}

fn count_changesets(c: &Connection) -> scion_server::Result<i64> {
    Ok(c.query_row("SELECT count(*) FROM changesets", [], |row| row.get(0))?)
}

#[test]
fn concurrent_creates_of_one_changeset_store_it_once() {
    let f = fixture();
    let before = f.store.read(count_changesets).unwrap();
    let (bytes, expected) = changeset(
        vec![f.root],
        &[("a", None), ("dir/new", Some(regular("new")))],
    )
    .encode_with_id();
    let ids: Vec<ChangesetId> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..100)
            .map(|_| s.spawn(|| create_changeset(&f.store, REPO, &bytes)))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap().unwrap())
            .collect()
    });
    assert!(ids.iter().all(|id| *id == expected), "{ids:?}");
    assert_eq!(f.store.read(count_changesets).unwrap(), before + 1);
}

#[test]
fn bad_requests_are_rejected_and_store_nothing() {
    let f = fixture();
    let before = f.store.read(count_changesets).unwrap();
    let ok = |edit: fn(&mut Changeset)| {
        let mut cs = changeset(vec![f.root], &[("dir/new", Some(regular("new")))]);
        edit(&mut cs);
        cs.encode()
    };
    let invalid = [
        ("undecodable", b"not a changeset".to_vec(), "bad changeset"),
        (
            "no parents",
            ok(|cs| cs.parents.clear()),
            "exactly one parent",
        ),
        (
            "two parents",
            ok(|cs| cs.parents.push(cs.parents[0])),
            "exactly one parent",
        ),
        (
            "parent in another repo",
            changeset(vec![f.other_root], &[("dir/new", Some(regular("new")))]).encode(),
            "not in repo",
        ),
        (
            "missing blob",
            ok(|cs| {
                cs.changes
                    .insert(path("dir/zzz"), Some(regular("never uploaded")));
            }),
            "is not stored",
        ),
        (
            "committer set",
            ok(|cs| cs.committer = Some(cs.author.clone())),
            "committer",
        ),
        // No change follows these counts. The limit error, not "ends early",
        // shows that the decoder stopped before it read any change.
        (
            "one change too many",
            change_count(f.root, MAX_CHANGES + 1),
            "more than",
        ),
        (
            "exactly the limit, so decoding goes on",
            change_count(f.root, MAX_CHANGES),
            "ends early",
        ),
        (
            "message too long",
            ok(|cs| cs.message = vec![b'm'; MAX_COMMIT_MESSAGE_BYTES + 1]),
            "message",
        ),
        (
            "email too long",
            ok(|cs| cs.author.email = vec![b'e'; MAX_IDENTITY_BYTES + 1]),
            "author",
        ),
    ];
    for (case, bytes, fragment) in invalid {
        match create_changeset(&f.store, REPO, &bytes) {
            Err(Error::InvalidArgument(why)) => {
                assert!(why.contains(fragment), "{case}: {why}");
            }
            other => panic!("{case}: expected InvalidArgument, got {other:?}"),
        }
    }

    let unapplicable = [
        (
            ("a/b", Some(regular("new"))),
            ApplyError::FileInTheWay {
                path: path("a/b"),
                file: path("a"),
            },
        ),
        (("nope", None), ApplyError::DeleteMissing(path("nope"))),
    ];
    for (change, expected) in unapplicable {
        let bytes = changeset(vec![f.root], &[change]).encode();
        match create_changeset(&f.store, REPO, &bytes) {
            Err(Error::Apply(e)) => assert_eq!(e, expected),
            other => panic!("expected {expected:?}, got {other:?}"),
        }
    }

    let valid = changeset(vec![f.root], &[("dir/new", Some(regular("new")))]).encode();
    assert!(matches!(
        create_changeset(&f.store, "nope", &valid),
        Err(Error::NotFound(_))
    ));
    assert_eq!(f.store.read(count_changesets).unwrap(), before);
}

#[test]
fn created_changesets_have_the_expected_root_tree() {
    let f = fixture();
    let exec = FileState::Regular {
        content: BlobId::of(b"new"),
        executable: true,
    };
    let first = create_changeset(
        &f.store,
        REPO,
        &changeset(vec![f.root], &[("a", None), ("dir/new", Some(exec))]).encode(),
    )
    .unwrap();
    // A file becomes a directory, on top of a created changeset.
    let second = create_changeset(
        &f.store,
        REPO,
        &changeset(
            vec![first],
            &[("dir/b", None), ("dir/b/inner", Some(regular("b")))],
        )
        .encode(),
    )
    .unwrap();

    let repo = f.store.read(|c| db::repo_id(c, REPO)).unwrap();
    let lookup = |cs, p: &str| {
        let root = tree::root_tree(&f.store, repo, cs).unwrap();
        f.store.read(|c| tree::lookup(c, root, &path(p))).unwrap()
    };
    let file = |state| Some(Entry::File(state));
    assert_eq!(lookup(first, "a"), None);
    assert_eq!(lookup(first, "dir/b"), file(regular("b")));
    assert_eq!(lookup(first, "dir/c"), file(regular("c")));
    assert_eq!(lookup(first, "dir/new"), file(exec));
    assert_eq!(lookup(second, "dir/b/inner"), file(regular("b")));
    assert_eq!(lookup(second, "dir/new"), file(exec));
    assert_eq!(
        lookup(f.root, "a"),
        file(regular("a")),
        "the parent is unchanged"
    );
}

#[test]
fn a_blob_that_a_changeset_of_the_repo_names_needs_no_upload() {
    let f = fixture();
    // As an import stores it: no upload, but a changeset of the repo names it.
    let blob = f.store.blobs().put(b"imported").unwrap();
    let file = FileState::Regular {
        content: blob,
        executable: false,
    };
    let root = f
        .store
        .write(|c| {
            let repo = db::create_repo(c, "imported")?;
            let root = changeset(vec![], &[("f", Some(file))]);
            Ok(tree::store_changeset(c, repo, &root)?.0)
        })
        .unwrap();
    // A rename names it again.
    let rename = changeset(vec![root], &[("f", None), ("g", Some(file))]).encode();
    create_changeset(&f.store, "imported", &rename).unwrap();
    // Another repo still may not name it.
    let theirs = changeset(vec![f.root], &[("g", Some(file))]).encode();
    match create_changeset(&f.store, REPO, &theirs) {
        Err(Error::InvalidArgument(why)) => assert!(why.contains("is not stored"), "{why}"),
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
}

/// Overwrites the stored file of blob `new` with `bytes`, as a crash, a bad
/// disk or a stray write could.
fn damage_new(f: &Fixture, bytes: &[u8]) -> BlobId {
    let id = BlobId::of(b"new");
    let hex = id.to_string();
    let file = f.dir.path().join("objects/blob").join(&hex[..2]);
    fs::write(file.join(&hex[2..]), bytes).unwrap();
    id
}

#[test]
fn a_damaged_blob_is_rejected_until_put_rewrites_it() {
    let packed = zstd::bulk::compress(b"new", 3).unwrap();
    let damaged = [
        ("truncated", packed[..packed.len() / 2].to_vec()),
        ("not zstd", b"not a zstd frame".to_vec()),
        ("other content", zstd::bulk::compress(b"old", 3).unwrap()),
    ];
    for (case, bytes) in damaged {
        let f = fixture();
        let id = damage_new(&f, &bytes);
        let blobs = f.store.blobs();
        assert_eq!(blobs.verify(id).unwrap(), BlobStatus::Corrupt, "{case}");
        assert!(matches!(blobs.get(id), Err(Error::Corrupt(_))), "{case}");
        let child = changeset(vec![f.root], &[("dir/new", Some(regular("new")))]).encode();
        match create_changeset(&f.store, REPO, &child) {
            Err(Error::InvalidArgument(why)) => {
                assert!(why.contains("is corrupt"), "{case}: {why}");
            }
            other => panic!("{case}: expected InvalidArgument, got {other:?}"),
        }

        assert_eq!(blobs.put(b"new").unwrap(), id, "{case}");
        assert_eq!(blobs.verify(id).unwrap(), BlobStatus::Valid, "{case}");
        assert_eq!(blobs.get(id).unwrap(), b"new", "{case}");
        create_changeset(&f.store, REPO, &child).unwrap();
    }
}

#[test]
fn concurrent_puts_of_one_blob_leave_it_intact() {
    let f = fixture();
    // Start from a damaged file, so the threads race to replace it too.
    let id = damage_new(&f, b"torn");
    let ids: Vec<BlobId> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..16)
            .map(|_| s.spawn(|| f.store.blobs().put(b"new")))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap().unwrap())
            .collect()
    });
    assert!(ids.iter().all(|got| *got == id), "{ids:?}");
    assert_eq!(f.store.blobs().verify(id).unwrap(), BlobStatus::Valid);
    // Each put stages in its process's folder; nothing of the puts is left
    // in it (issue #101).
    let staged: usize = fs::read_dir(f.dir.path().join("tmp"))
        .unwrap()
        .map(|entry| {
            fs::read_dir(entry.unwrap().path())
                .unwrap()
                .filter(|e| e.as_ref().unwrap().file_name() != ".lock")
                .count()
        })
        .sum();
    assert_eq!(staged, 0, "no temp file is left behind");
}

#[test]
fn a_changeset_that_does_not_match_its_id_reads_as_corrupt() {
    let f = fixture();
    // A valid encoding, but of another changeset.
    let other = changeset(vec![], &[("a", Some(regular("b")))]).encode();
    let repo = f
        .store
        .write(|c| {
            c.execute(
                "UPDATE changesets SET body = ?1 WHERE id = ?2",
                params![other, f.root.as_bytes()],
            )?;
            db::repo_id(c, REPO)
        })
        .unwrap();

    match f.store.read(|c| db::get_changeset(c, repo, f.root)) {
        Err(Error::Corrupt(why)) => assert!(why.contains("does not match its ID"), "{why}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}
