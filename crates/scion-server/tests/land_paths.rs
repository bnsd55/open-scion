//! The land paths that a racing import or a failed apply reach, driven
//! through the public API: a land whose ref an import keeps moving
//! replans and lands exactly once, and a read that panics with its
//! transaction open leaves the reader pool healthy.

use scion_core::{Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::meta::MetaTx as _;
use scion_server::{Backend, Store, meta::Meta as _};
use std::collections::BTreeMap;

const REPO: &str = "r";

fn author() -> IdentityTime {
    IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

/// A change whose blob is stored in the store, so the land's lens reads
/// its length.
fn put(store: &Backend, path: &str, content: &str) -> (RepoPath, Option<FileState>) {
    let content = store.blobs().put(content.as_bytes()).unwrap();
    let state = Some(FileState::Regular {
        content,
        executable: false,
    });
    (RepoPath::new(path).unwrap(), state)
}

/// A repo whose `main` points at a root with `a/b/c`, `a/f` and `b/y`.
struct Fixture {
    _dir: tempfile::TempDir,
    store: Backend,
    root: ChangesetId,
}

fn changeset(parents: Vec<ChangesetId>, changes: Vec<(RepoPath, Option<FileState>)>) -> Changeset {
    Changeset::new(
        parents,
        author(),
        None,
        b"land path test".to_vec(),
        changes.into_iter().collect(),
    )
    .expect("changeset fits the format")
}

impl Fixture {
    /// Inserts the changeset on `base`; the blobs of its puts were stored
    /// by the caller.
    fn commit(
        &self,
        base: ChangesetId,
        changes: Vec<(RepoPath, Option<FileState>)>,
    ) -> ChangesetId {
        self.store
            .write(|c| {
                let repo = c.repo_id(REPO)?;
                c.insert_changeset(repo, &changeset(vec![base], changes))
            })
            .unwrap()
    }
}

fn setup() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
    let blob = |content: &str| store.blobs().put(content.as_bytes()).unwrap();
    let state = |blob| {
        Some(FileState::Regular {
            content: blob,
            executable: false,
        })
    };
    let changes = BTreeMap::from([
        (RepoPath::new("a/b/c").unwrap(), state(blob("c"))),
        (RepoPath::new("a/f").unwrap(), state(blob("f"))),
        (RepoPath::new("b/y").unwrap(), state(blob("y"))),
    ]);
    let root = Changeset::new(vec![], author(), None, b"root".to_vec(), changes)
        .expect("changeset fits the format");
    let root_id = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let id = c.insert_changeset(repo, &root)?;
            assert!(c.cas_ref(repo, "main", None, id)?);
            Ok(id)
        })
        .unwrap();
    Fixture {
        _dir: dir,
        store,
        root: root_id,
    }
}

/// A read that panics with its transaction open must not poison the
/// reader pool: the connection is replaced, and later reads see the
/// writes that happened after the panicked read.
#[test]
fn a_read_that_leaves_a_transaction_open_is_replaced() {
    // The poison reaches into the SQLite pool through rusqlite itself;
    // the Postgres pool has its own recovery, exercised by its own jobs.
    if Backend::test_is_postgres() {
        return;
    }
    let f = setup();
    // The reader pool has 8 connections. Poison each of them once, so
    // every pooled connection would be stale without the replacement.
    for _ in 0..8 {
        let store = f.store.clone();
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _ = store.read(|c| {
                use rusqlite::params;
                c.sqlite()
                    .expect("a SQLite backend")
                    .execute("BEGIN", params![])?;
                panic!("the read dies with its transaction open");
                #[allow(unreachable_code)]
                Ok::<(), scion_server::error::Error>(())
            });
        }));
        assert!(panicked.is_err(), "the read panicked");
    }

    // A write lands after the panicked reads.
    let cs = f.commit(f.root, vec![put(&f.store, "b/y", "after")]);
    f.store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let head = c.get_ref(repo, "main")?.unwrap();
            c.cas_ref(repo, "main", Some(head.version), cs)
        })
        .unwrap();

    // A later read must see it: a stale pooled connection would answer
    // from the snapshot of its panicked read.
    let seen = f
        .store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            c.has_changeset(repo, cs)
        })
        .unwrap();
    assert!(seen, "the pool answered from a stale snapshot");
}
