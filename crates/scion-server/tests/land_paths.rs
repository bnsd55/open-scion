//! The land paths that a racing import or a failed apply reach, driven
//! through the public API: a land whose ref an import keeps moving
//! replans and lands exactly once, and a read that panics with its
//! transaction open leaves the reader pool healthy.

use scion_core::{Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::auth::Caller;
use scion_server::Store;
use scion_server::deadline::Deadline;
use scion_server::land::{LandRequest, LandState, Lands};
use scion_server::meta::MetaTx as _;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const REPO: &str = "r";

fn author() -> IdentityTime {
    IdentityTime {
        name: b"Ada".to_vec(),
        email: b"ada@example.com".to_vec(),
        unix_secs: 1_700_000_000,
        tz_offset_minutes: 0,
    }
}

fn caller() -> Caller {
    Caller("t".into())
}

/// A change whose blob is stored in the store, so the land's lens reads
/// its length.
fn put(store: &Store, path: &str, content: &str) -> (RepoPath, Option<FileState>) {
    let content = store.blobs().put(content.as_bytes()).unwrap();
    let state = Some(FileState::Regular {
        content,
        executable: false,
    });
    (RepoPath::new(path).unwrap(), state)
}

fn req(cs: ChangesetId, key: &str) -> LandRequest {
    LandRequest {
        repo: REPO.into(),
        changeset: cs,
        target_ref: "main".into(),
        request_key: key.into(),
        deadline: Deadline::new(Instant::now() + Duration::from_secs(600)),
    }
}

/// A repo whose `main` points at a root with `a/b/c`, `a/f` and `b/y`.
struct Fixture {
    _dir: tempfile::TempDir,
    store: Store,
    root: ChangesetId,
}

fn changeset(parents: Vec<ChangesetId>, changes: Vec<(RepoPath, Option<FileState>)>) -> Changeset {
    Changeset {
        parents,
        author: author(),
        committer: None,
        message: b"land path test".to_vec(),
        changes: changes.into_iter().collect(),
    }
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

    /// Moves `main` to `to` the way an import does: outside the lands'
    /// ref lock.
    fn import_move(&self, to: ChangesetId) -> bool {
        self.store
            .write(|c| {
                let repo = c.repo_id(REPO)?;
                let head = c.get_ref(repo, "main")?.unwrap();
                c.cas_ref(repo, "main", Some(head.version), to)
            })
            .unwrap()
    }

    fn head(&self) -> (ChangesetId, i64) {
        self.store
            .read(|c| {
                let repo = c.repo_id(REPO)?;
                let head = c.get_ref(repo, "main")?.unwrap();
                Ok((head.target, head.version))
            })
            .unwrap()
    }

    fn landed_count(&self) -> i64 {
        self.store
            .read(|c| {
                let sql = "SELECT count(*) FROM lands WHERE state = 'landed'";
                Ok(c.query_row(sql, [], |row| row.get(0))?)
            })
            .unwrap()
    }
}

fn setup() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let blob = |content: &str| store.blobs().put(content.as_bytes()).unwrap();
    let state = |blob| {
        Some(FileState::Regular {
            content: blob,
            executable: false,
        })
    };
    // A wide root: deriving any head's tree from it takes long enough for
    // a racing import to move the ref inside the land's plan.
    let mut changes = BTreeMap::from([
        (RepoPath::new("a/b/c").unwrap(), state(blob("c"))),
        (RepoPath::new("a/f").unwrap(), state(blob("f"))),
        (RepoPath::new("b/y").unwrap(), state(blob("y"))),
    ]);
    let wide = blob("wide");
    for i in 0..300 {
        changes.insert(RepoPath::new(format!("w{i}")).unwrap(), state(wide));
    }
    let root = Changeset {
        parents: vec![],
        author: author(),
        committer: None,
        message: b"root".to_vec(),
        changes,
    };
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

/// A land whose ref an import keeps moving replans and lands once: the
/// retry loop plans again from the new head instead of failing the land.
#[test]
fn a_land_replans_when_imports_move_its_ref() {
    let f = setup();
    let lands = Lands::default();
    // The land's changeset: parent = root, so it must rebase onto
    // whatever `main` is when it lands.
    let cs = f.commit(f.root, vec![put(&f.store, "a/b/c", "landed")]);

    // The import: a writer that moves `main` without the lands' ref lock,
    // as an import in another process does. Each import carries a wide
    // tree, so the land's plan and commit take long enough for an import
    // to move the ref in between. It stops after a few moves, so a later
    // attempt of the loop can land.
    let stop = Arc::new(AtomicBool::new(false));
    let racer = {
        let store = f.store.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut rounds = 0;
            while !stop.load(Ordering::SeqCst) && rounds < 4 {
                std::thread::sleep(Duration::from_millis(2));
                let moved = store
                    .write(|c| {
                        let repo = c.repo_id(REPO)?;
                        let head = c.get_ref(repo, "main")?.unwrap();
                        let cs = Changeset {
                            parents: vec![head.target],
                            author: author(),
                            committer: None,
                            message: format!("import {rounds}").into_bytes(),
                            changes: [].into(),
                        };
                        let id = c.insert_changeset(repo, &cs)?;
                        c.cas_ref(repo, "main", Some(head.version), id)
                    })
                    .unwrap();
                if moved {
                    rounds += 1;
                }
            }
        })
    };
    std::thread::sleep(Duration::from_millis(2));

    // The land lands although imports move the ref while it runs.
    let out = lands.land(&f.store, &caller(), &req(cs, "k")).unwrap();
    stop.store(true, Ordering::SeqCst);
    racer.join().unwrap();

    assert_eq!(out.state, LandState::Landed, "{}", out.detail);
    let landed = out.result.expect("landed");
    // The landed changeset sits on the first-parent history of main: an
    // import that moved the ref after the land sits on top of it.
    let (head, _) = f.head();
    let on_history = f
        .store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            let mut cursor = head;
            for _ in 0..16 {
                if cursor == landed {
                    return Ok(true);
                }
                let parents = c.get_changeset(repo, cursor)?.parents;
                match parents.as_slice() {
                    [parent] => cursor = *parent,
                    _ => return Ok(false),
                }
            }
            Ok(false)
        })
        .unwrap();
    assert!(on_history, "the landed changeset is on the history of main");
    assert_eq!(f.landed_count(), 1, "the land landed exactly once");
    // A retry with the same key returns the same outcome.
    let again = lands.land(&f.store, &caller(), &req(cs, "k")).unwrap();
    assert_eq!(again, out);
}

/// A read that panics with its transaction open must not poison the
/// reader pool: the connection is replaced, and later reads see the
/// writes that happened after the panicked read.
#[test]
fn a_read_that_leaves_a_transaction_open_is_replaced() {
    let f = setup();
    // The reader pool has 8 connections. Poison each of them once, so
    // every pooled connection would be stale without the replacement.
    for _ in 0..8 {
        let store = f.store.clone();
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _ = store.read(|c| {
                use rusqlite::params;
                c.execute("BEGIN", params![])?;
                panic!("the read dies with its transaction open");
                #[allow(unreachable_code)]
                Ok::<(), scion_server::error::Error>(())
            });
        }));
        assert!(panicked.is_err(), "the read panicked");
    }

    // A write lands after the panicked reads.
    let cs = f.commit(f.root, vec![put(&f.store, "b/y", "after")]);
    f.import_move(cs);

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
