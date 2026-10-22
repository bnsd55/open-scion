//! The path index's catch-up walk reads first parents in pages, not two
//! statements per changeset: on Postgres each statement is a round trip
//! under the ref's lock, so a ref that moved 1,000 changesets past the
//! index tip must not cost 2,000 of them. The guard counts the
//! statements of one land whose catch-up crosses 100 and 1,000
//! changesets: the 1,000-deep land may cost a page or two more, not
//! nine hundred. On SQLite the count is always zero, which is why the
//! test skips there.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::Backend;
use scion_server::test_support::auth::Caller;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::land::{LandRequest, LandState, Lands};
use scion_server::test_support::meta::{Meta as _, MetaTx as _, RepoId};
use scion_server::test_support::tree;

const REPO: &str = "r";

fn author() -> IdentityTime {
    IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 0, 0).expect("identity fits the format")
}

fn put(path: &str, content: &[u8]) -> (RepoPath, Option<FileState>) {
    let content = BlobId::of(content);
    let state = FileState::Regular {
        content,
        executable: false,
    };
    (RepoPath::new(path).unwrap(), Some(state))
}

/// One store of `repo r`: the root changeset on `main`, through the
/// same seams the metadata uses.
fn setup(dir: &std::path::Path) -> (Backend, RepoId, ChangesetId) {
    let store = Backend::open_test(dir).unwrap();
    let (repo, root) = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = Changeset::new(vec![], author(), None, b"root\n".to_vec(), BTreeMap::new())
                .expect("changeset fits the format");
            let id = c.insert_changeset(repo, &root)?;
            assert!(c.cas_ref(repo, "main", None, id)?);
            Ok((repo, id))
        })
        .unwrap();
    (store, repo, root)
}

/// Stores the changeset through the write path the land's own settle
/// uses: its tree and derived rows land with it, so the final land's own
/// fill does not scale with the history.
fn commit(store: &Backend, repo: RepoId, base: ChangesetId, path: &str) -> ChangesetId {
    let cs = Changeset::new(
        vec![base],
        author(),
        None,
        format!("{path}\n").into_bytes(),
        BTreeMap::from([put(path, b"changed\n")]),
    )
    .expect("changeset fits the format");
    store.blobs().put(b"changed\n").unwrap();
    store
        .write(|c| Ok(tree::store_changeset(c, repo, &cs, &mut tree::lens_of(store))?.0))
        .unwrap()
}

/// Moves `main` to `to` the way an import does, outside any land, so the
/// path index stays at its tip while the ref moves on.
fn move_main(store: &Backend, repo: RepoId, to: ChangesetId) {
    store
        .write(|c| {
            let head = c.get_ref(repo, "main")?.expect("main exists");
            assert!(c.cas_ref(repo, "main", Some(head.version), to)?);
            Ok(())
        })
        .unwrap();
}

/// The statements one land costs when the ref moved `n` changesets past
/// the path index's tip: the land's catch-up walks all `n` of them.
fn catch_up_statements(n: usize) -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let (store, repo, root) = setup(dir.path());
    let lands = Lands::default();
    // The index tip: one landed changeset, so the index holds a tip
    // below everything that follows.
    let cs0 = commit(&store, repo, root, "a/x");
    let out = lands
        .land(
            &store,
            &Caller("t".into()),
            &LandRequest {
                repo: REPO.into(),
                changeset: cs0,
                target_ref: "main".into(),
                request_key: "base".into(),
                deadline: Deadline::new(Instant::now() + Duration::from_secs(600)),
            },
        )
        .unwrap();
    assert_eq!(out.state, LandState::Landed, "{}", out.detail);
    // The ref moves `n` times; the index does not follow (only lands
    // advance it).
    let mut head = cs0;
    for i in 0..n {
        let cs = commit(&store, repo, head, &format!("f/{i}"));
        move_main(&store, repo, cs);
        head = cs;
    }
    // One land on the moved head: its plan catches the index up across
    // all `n` changesets, under the ref's lock. The changeset touches
    // `a/x`, which only the tip's own changeset changed, so the verdict
    // is the catch-up's alone.
    let last = commit(&store, repo, head, "a/x");
    Backend::test_reset_statement_count();
    let out = lands
        .land(
            &store,
            &Caller("t".into()),
            &LandRequest {
                repo: REPO.into(),
                changeset: last,
                target_ref: "main".into(),
                request_key: "end".into(),
                deadline: Deadline::new(Instant::now() + Duration::from_secs(600)),
            },
        )
        .unwrap();
    assert_eq!(
        out.state,
        LandState::Landed,
        "{} {:?}",
        out.detail,
        out.conflict_paths
    );
    Backend::test_statement_count()
}

/// The page statement returns the whole page: one call, `limit` rows.
#[test]
fn the_page_statement_returns_the_whole_page() {
    if !Backend::test_is_postgres() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (store, repo, root) = setup(dir.path());
    let mut head = root;
    for i in 0..50 {
        let cs = commit(&store, repo, head, &format!("f/{i}"));
        move_main(&store, repo, cs);
        head = cs;
    }
    let page = store
        .read(|c| c.first_parent_page(repo, head, 1_000))
        .unwrap();
    assert_eq!(page.len(), 51, "the start plus all 50 ancestors");
}

/// A 1,000-deep catch-up costs a page or two more than a 100-deep one,
/// not a statement per changeset.
#[test]
fn the_catch_up_walk_reads_first_parents_in_pages() {
    if !Backend::test_is_postgres() {
        return;
    }
    let hundred = catch_up_statements(100);
    let thousand = catch_up_statements(1_000);
    assert!(
        thousand - hundred <= 20,
        "the 1,000-deep catch-up costs {} statements over the 100-deep one \
         ({} vs {}): the walk reads first parents one changeset at a time",
        thousand - hundred,
        thousand,
        hundred
    );
}
