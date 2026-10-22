//! `CreateChangeset`'s member check: a changeset's distinct puts are
//! checked in one batched read, not one statement per blob. A changeset
//! may carry 100,000 changes, so one round trip per blob would cost
//! 100,000 of them on Postgres.

use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};
use scion_server::Backend;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::write::create_changeset;

fn later() -> Deadline {
    Deadline::new(std::time::Instant::now() + std::time::Duration::from_secs(600))
}
use std::collections::BTreeMap;

const REPO: &str = "main";

fn author() -> IdentityTime {
    IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

fn regular(content: &str) -> FileState {
    FileState::Regular {
        content: BlobId::of(content.as_bytes()),
        executable: false,
    }
}

/// A store whose repo `main` holds one root changeset, and whose
/// `repo_blobs` rows name `n` blobs with no files behind them: the
/// member check accepts their puts, and the file check after it refuses
/// the changeset, so the measured window is the member check's own
/// statements.
fn store_with_root(n_blobs: usize) -> (tempfile::TempDir, Backend, scion_core::ChangesetId) {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    store.blobs().put(b"seed\n").unwrap();
    let root = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            // The file check after the member check refuses these: the
            // rows say the repo has them, no file backs the rows.
            for i in 0..n_blobs {
                c.add_repo_blob(repo, BlobId::of(format!("missing {i}\n").as_bytes()))?;
            }
            let root = Changeset::new(
                vec![],
                author(),
                None,
                b"root\n".to_vec(),
                BTreeMap::from([(RepoPath::new("seed").unwrap(), Some(regular("seed\n")))]),
            )
            .expect("changeset fits the format");
            c.add_repo_blob(repo, BlobId::of(b"seed\n"))?;
            c.insert_changeset(repo, &root)
        })
        .unwrap();
    (dir, store, root)
}

/// A changeset on `parent` with `n` distinct puts, each its own blob.
/// None of the blobs is uploaded, so the member check refuses the
/// changeset once it has read what it needs.
fn refused(n: usize, parent: scion_core::ChangesetId) -> Vec<u8> {
    let changes: BTreeMap<RepoPath, Option<FileState>> = (0..n)
        .map(|i| {
            (
                RepoPath::new(format!("p/{i}")).unwrap(),
                Some(regular(&format!("missing {i}\n"))),
            )
        })
        .collect();
    Changeset::new(vec![parent], author(), None, b"n puts\n".to_vec(), changes)
        .expect("changeset fits the format")
        .encode()
}

/// The statements one refused `create_changeset` of `n` puts costs. The
/// puts name blobs the repo's rows have, so the member check runs its
/// whole list, and the file check after it refuses the changeset.
fn refused_statements(store: &Backend, n: usize, parent: scion_core::ChangesetId) -> u64 {
    let bytes = refused(n, parent);
    Backend::test_reset_statement_count();
    // No file backs the rows, so the file check refuses the changeset.
    let err = create_changeset(store, REPO, &bytes, &later()).unwrap_err();
    assert!(
        err.to_string().contains("is not stored"),
        "the refusal is the member check's: {err}"
    );
    Backend::test_statement_count()
}

/// The member check reads its blob list in batches: 1,000 distinct puts
/// cost no more statements than 100 of them plus the batch's own two.
/// One statement per blob would cost about 900 more for the 1,000.
/// SQLite counts no statements (its check is local), so the assert
/// holds trivially there.
#[test]
fn the_member_check_costs_one_batch_not_one_query_per_blob() {
    let (_dir, store, root) = store_with_root(1000);
    let small = refused_statements(&store, 100, root);
    let large = refused_statements(&store, 1000, root);
    assert!(
        large <= small + 2,
        "1,000 blobs cost {large} statements against 100 blobs' {small}"
    );
}

/// Two missing blobs: the refusal names the first missing put in the
/// changeset's own order, with that put's path, exactly as the
/// one-query-per-blob check named it.
#[test]
fn a_refused_changeset_names_the_first_missing_put() {
    let (_dir, store, root) = store_with_root(0);
    let cs = Changeset::new(
        vec![root],
        author(),
        None,
        b"two missing\n".to_vec(),
        BTreeMap::from([
            (RepoPath::new("a/have").unwrap(), Some(regular("seed\n"))),
            (
                RepoPath::new("m/one").unwrap(),
                Some(regular("missing one\n")),
            ),
            (
                RepoPath::new("n/two").unwrap(),
                Some(regular("missing two\n")),
            ),
        ]),
    )
    .expect("changeset fits the format");
    let err = create_changeset(&store, REPO, &cs.encode(), &later()).unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("\"m/one\""),
        "the refusal names the first missing put's path: {text}"
    );
    assert!(
        !text.contains("\"n/two\""),
        "the refusal names no later put: {text}"
    );
}

/// A blob the repo's rows do not name is refused even though its file
/// is stored: only the membership row makes a blob the repo's.
#[test]
fn a_blob_the_repo_lacks_is_refused_even_when_its_file_is_stored() {
    let (_dir, store, root) = store_with_root(0);
    // Another repo's blob: the file is in the store, `main`'s rows do
    // not name it.
    store.blobs().put(b"other repo's content\n").unwrap();
    let cs = Changeset::new(
        vec![root],
        author(),
        None,
        b"one put\n".to_vec(),
        BTreeMap::from([(
            RepoPath::new("s/x").unwrap(),
            Some(regular("other repo's content\n")),
        )]),
    )
    .expect("changeset fits the format");
    let err = create_changeset(&store, REPO, &cs.encode(), &later()).unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("\"s/x\"") && text.contains("is not stored"),
        "the refusal names the put whose blob the repo lacks: {text}"
    );
}
