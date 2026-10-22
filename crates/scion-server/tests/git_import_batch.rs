//! The restore's git provenance rows are written in batches, so the
//! statements that write them do not grow with the archive: a restore of
//! an archive with 1,000 provenance pairs costs at most
//! ceil(1,000/1,000)+2 provenance statements more than one with 100. The
//! statement count is a process-wide seam, so this guard owns its binary:
//! no other test speaks SQL in its process. The count is what the job
//! seam saw of the restore; only the `git_import` inserts are counted, so
//! the changeset and tree writes of the same restore stay out. On SQLite
//! the count is always zero, which is why the guard skips there.

use std::collections::BTreeMap;

use gix::ObjectId;
use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};

use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::tree;
use scion_server::{Backend, archive};

const REPO: &str = "main";

fn changeset(parents: Vec<scion_core::ChangesetId>, file: &str) -> Changeset {
    Changeset::new(
        parents,
        scion_core::IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"change\n".to_vec(),
        BTreeMap::from([(
            RepoPath::new("f.txt").unwrap(),
            Some(FileState::Regular {
                content: BlobId::of(file.as_bytes()),
                executable: false,
            }),
        )]),
    )
    .expect("changeset fits the format")
}

/// An archive whose one repo holds one changeset on `main` and `n`
/// provenance pairs. Gives the archive directory and the changeset the
/// pairs name.
fn archive_with_provenance(
    n: usize,
) -> (
    tempfile::TempDir,
    std::path::PathBuf,
    scion_core::ChangesetId,
) {
    let data = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let out = out_dir.path().join("archive");
    let store = Backend::open_test_shared(data.path()).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    let cs = changeset(Vec::new(), "one\n");
    let cs_id = store
        .write(|c| Ok(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0))
        .unwrap();
    store
        .write(|c| c.cas_ref(repo, "main", None, cs_id))
        .unwrap();
    for i in 0..n {
        let oid = gix::hash::ObjectId::from_hex(format!("{i:040x}").as_bytes()).unwrap();
        store.write(|c| c.put_git_import(repo, oid, cs_id)).unwrap();
    }
    // The blobs the tree names must be in the store, or the export's
    // verify refuses the section.
    store.blobs().put(b"one\n").unwrap();
    archive::export(&store, data.path(), Some(REPO), false, &out).unwrap();
    (out_dir, out, cs_id)
}

#[test]
fn the_restore_of_a_thousand_pairs_costs_three_provenance_statements() {
    if !Backend::test_is_postgres() {
        eprintln!("skip on sqlite: the statement count is a Postgres seam");
        return;
    }
    let base = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let mut counts = Vec::new();
    for n in [100, 1000] {
        // The restore's database names one store, so the run gets its own:
        // created and dropped here, never the tests' shared one.
        let name = format!("scion_git_import_batch_{}", std::process::id());
        let mut maker = postgres::Client::connect(&base, postgres::NoTls).unwrap();
        maker
            .batch_execute(&format!("DROP DATABASE IF EXISTS {name}"))
            .unwrap();
        maker
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .unwrap();
        let url = format!(
            "{}/{}",
            base.rsplit_once('/')
                .map_or(base.as_str(), |(rest, _)| rest),
            name
        );
        let (_data, out, _cs) = archive_with_provenance(n);
        let target = tempfile::tempdir().unwrap();
        Backend::test_reset_statement_count();
        let restored = archive::restore(&out, target.path(), Some(&url), None).unwrap();
        assert_eq!(restored, vec![REPO.to_owned()]);
        let puts = Backend::test_statement_log()
            .iter()
            .filter(|sql| sql.starts_with("INSERT INTO git_import"))
            .count();
        maker
            .batch_execute(&format!("DROP DATABASE {name}"))
            .unwrap();
        drop(maker);
        counts.push(u64::try_from(puts).unwrap_or(u64::MAX));
    }
    let [small, large] = [counts[0], counts[1]];
    eprintln!(
        "the restore of 100 pairs wrote provenance in {small} statements, of 1,000 in {large}"
    );
    // The provenance is written, in one statement: a per-pair restore
    // fails the growth assert below, a restore that writes none fails
    // here.
    assert!(small >= 1, "the restore wrote no provenance: {small}");
    let ceiling = 2;
    let growth = large - small;
    assert!(
        growth <= ceiling,
        "the restore of 1,000 pairs wrote provenance in {large} statements, \
         of 100 in {small}: the difference {growth} grew with the archive"
    );
}

/// The batch keeps the single row's rule: a repeated pair is skipped, not
/// overwritten, and the repeat is no error.
#[test]
fn a_repeated_provenance_pair_is_a_no_op() {
    if !Backend::test_is_postgres() {
        eprintln!("skip on sqlite: this guard counts Postgres statements");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test_shared(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    let first = changeset(Vec::new(), "one\n");
    let first = store
        .write(|c| Ok(tree::store_changeset(c, repo, &first, &mut |_| Ok(0))?.0))
        .unwrap();
    let second = changeset(Vec::new(), "two\n");
    let second = store
        .write(|c| Ok(tree::store_changeset(c, repo, &second, &mut |_| Ok(0))?.0))
        .unwrap();
    let oid = gix::hash::ObjectId::from_hex(b"0000000000000000000000000000000000000001").unwrap();
    store
        .write(|c| c.put_git_imports(repo, &[(oid, first), (oid, first)]))
        .unwrap();
    store
        .write(|c| c.put_git_imports(repo, &[(oid, second), (oid, first)]))
        .unwrap();
    let got = store
        .read(|c| Ok(c.imported_commits(repo)?.get(&oid).copied()))
        .unwrap();
    assert_eq!(got, Some(first), "a repeated pair overwrote the first cs");
}
