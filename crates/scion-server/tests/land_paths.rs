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
use std::sync::atomic::Ordering;
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

/// A land whose ref an import moves under it plans again from the new
/// head and lands once, and the replan shows in the process's counters.
/// One scenario does not always reach the replan: the import can commit
/// between the land's attempts. The test runs the scenario until the
/// counter moves, at most 10 times.
#[test]
fn a_land_replans_when_imports_move_its_ref() {
    let metrics = Arc::new(scion_proto::metrics::Metrics::default());
    let mut replans = 0;
    for _ in 0..10 {
        replans = replan_scenario(Arc::clone(&metrics));
        if replans >= 1 {
            break;
        }
    }
    assert!(replans >= 1, "no land replanned in 10 runs");
}

fn replan_scenario(metrics: Arc<scion_proto::metrics::Metrics>) -> u64 {
    let f = setup();
    let lands = Lands::new(None, Some(Arc::clone(&metrics)));
    // The land's changeset: parent = root, so it must rebase onto
    // whatever `main` is when it lands.
    let cs = f.commit(f.root, vec![put(&f.store, "a/b/c", "landed")]);

    // The import: a writer that moves `main` without the lands' ref lock,
    // as an import in another process does. It waits for the land to
    // record its pending row, then moves main exactly once: the move is
    // certain to happen while the land runs, not timed against it.
    let rounds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let racer = {
        let store = f.store.clone();
        let rounds = Arc::clone(&rounds);
        std::thread::spawn(move || {
            let until = Instant::now() + Duration::from_secs(30);
            loop {
                let pending: i64 = store
                    .read(|c| {
                        let sql = "SELECT count(*) FROM lands WHERE state = 'pending'";
                        Ok(c.query_row(sql, [], |row| row.get(0))?)
                    })
                    .unwrap();
                if pending >= 1 {
                    break;
                }
                assert!(
                    Instant::now() < until,
                    "the land never recorded its pending row"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            // The land has recorded its request and reads the head soon
            // after; a move now lands between its plan and its commit.
            std::thread::sleep(Duration::from_millis(2));
            store
                .write(|c| {
                    let repo = c.repo_id(REPO)?;
                    let head = c.get_ref(repo, "main")?.unwrap();
                    let cs = Changeset {
                        parents: vec![head.target],
                        author: author(),
                        committer: None,
                        message: b"import".to_vec(),
                        changes: [].into(),
                    };
                    let id = c.insert_changeset(repo, &cs)?;
                    c.cas_ref(repo, "main", Some(head.version), id)
                })
                .unwrap();
            rounds.fetch_add(1, Ordering::SeqCst);
        })
    };

    // The land lands although the import moves the ref while it runs.
    let out = lands.land(&f.store, &caller(), &req(cs, "k")).unwrap();
    assert_eq!(out.state, LandState::Landed, "{}", out.detail);
    racer.join().unwrap();
    assert_eq!(
        rounds.load(Ordering::SeqCst),
        1,
        "the import did not move main while the land ran"
    );
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
    metrics.land_replans()
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
