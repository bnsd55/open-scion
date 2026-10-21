//! The archive's bytes, as the machine form of stability.md's promise
//! for `scion-archive-1`: an export of one fixed store produces the
//! same files with the same contents every run (the manifest's clock
//! reading and store UUID excepted), `verify` takes the golden with no
//! store code, and a restore of it resolves the pinned `main`. A diff
//! in the golden is a format change: docs/archive-format.md's version,
//! beside v1, and a changes/ fragment naming the break.
//!
//! Updating on purpose: run this test with
//! `SCION_UPDATE_ARCHIVE_GOLDEN=1`, commit the changed
//! `tests/archive_v1.golden`, and name the break in a changes/
//! fragment. No other edit may touch the golden.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, archive, fsck, test_support::tree};

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/archive_v1.golden");
const REPO: &str = "golden";

fn regular(name: &str) -> FileState {
    FileState::Regular {
        content: BlobId::of(name.as_bytes()),
        executable: false,
    }
}

fn path(name: &str) -> RepoPath {
    RepoPath::new(name.as_bytes().to_vec()).expect("the path fits the format")
}

fn changeset(parents: Vec<ChangesetId>, files: &[(&str, FileState)]) -> Changeset {
    Changeset::new(
        parents,
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"golden\n".to_vec(),
        files.iter().map(|(p, s)| (path(p), Some(*s))).collect(),
    )
    .expect("changeset fits the format")
}

/// The fixed store the golden pins: one repo, three changesets on
/// `main` over three blobs, every identity timestamp fixed. Determin-
/// istic by construction: the same build exports the same archive.
fn source(data: &Path) -> (Backend, ChangesetId) {
    let store = Backend::open_test(data).unwrap();
    for content in ["a", "b", "c"] {
        store.blobs().put(content.as_bytes()).unwrap();
    }
    let main = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut parent = None;
            for name in ["a", "b", "c"] {
                let cs = changeset(parent.into_iter().collect(), &[(name, regular(name))]);
                parent = Some(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0);
            }
            let main = parent.expect("depth > 0");
            assert!(c.cas_ref(repo, "main", None, main)?);
            Ok(main)
        })
        .unwrap();
    (store, main)
}

/// Every file under `root`, as relative paths, sorted.
fn walk(root: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).unwrap() {
        let p = entry.unwrap().path();
        if p.is_dir() {
            walk(&p, out);
        } else {
            out.push(p);
        }
    }
}

/// The golden's rows: `blake3 <hex>  <relative path>` per archive file
/// except the manifest (whose clock reading and store UUID are not the
/// format), plus `main <changeset id>` for the restored ref.
fn rows(archive: &Path) -> Vec<(String, String)> {
    let mut files = Vec::new();
    walk(archive, &mut files);
    let mut rows = Vec::new();
    for p in files {
        let rel = p.strip_prefix(archive).unwrap().to_path_buf();
        if rel == Path::new("manifest.json") {
            continue;
        }
        let hex = blake3::hash(&fs::read(&p).unwrap()).to_hex().to_string();
        rows.push((hex, rel.to_string_lossy().to_string()));
    }
    rows.sort();
    rows
}

/// The export's files hash to the golden's rows, the manifest's stable
/// fields say what the format says, and the restore resolves the
/// golden's `main` - on a fresh store, `fsck` clean.
#[test]
fn the_export_is_the_golden_and_restores_to_it() {
    let data = tempfile::tempdir().unwrap();
    let (store, main) = source(data.path());

    let out = data.path().join("archive");
    let manifest = archive::export(&store, data.path(), None, true, &out).unwrap();
    assert_eq!(manifest["format"], "scion-archive");
    assert_eq!(manifest["version"], 1);
    assert_eq!(manifest["kind"], "store");
    assert_eq!(manifest["counts"]["changesets"], 3);
    assert_eq!(manifest["counts"]["refs"], 1);
    assert_eq!(manifest["counts"]["pending_lands"], 0);

    let rows = rows(&out);
    if std::env::var_os("SCION_UPDATE_ARCHIVE_GOLDEN").is_some() {
        let mut text = String::new();
        text.push_str("# one row per archive file, hash and path; the manifest's clock\n# reading and store UUID are not the format and pin nothing here\n");
        for (hex, path) in &rows {
            writeln!(text, "{hex}  {path}").unwrap();
        }
        writeln!(text, "# the restored store's main resolves to").unwrap();
        writeln!(text, "main  {main}").unwrap();
        fs::write(GOLDEN, text).unwrap();
    }

    let golden = fs::read_to_string(GOLDEN).unwrap();
    let mut golden_rows: Vec<(String, String)> = Vec::new();
    let mut pinned_main = String::new();
    for line in golden.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let (first, second) = line.split_once("  ").expect("a golden row");
        if first == "main" {
            pinned_main = second.to_owned();
        } else {
            golden_rows.push((first.to_owned(), second.to_owned()));
        }
    }
    assert!(
        !golden_rows.is_empty(),
        "the golden must name the files it freezes"
    );
    assert_eq!(
        rows, golden_rows,
        "the archive's files changed: a format change is a new archive \
         version beside v1 (docs/archive-format.md) and a changes/ fragment \
         naming the break; to re-pin on purpose run with \
         SCION_UPDATE_ARCHIVE_GOLDEN=1"
    );

    // The archive is sound, per the format, with no store code.
    let damages = archive::verify(&out).unwrap();
    assert!(damages.is_empty(), "{damages:?}");

    // A restore of the golden into an empty data directory: fsck clean,
    // and `main` resolves to the ID the golden pins.
    let restored_dir = tempfile::tempdir().unwrap();
    let names = archive::restore(&out, restored_dir.path(), None, None).unwrap();
    assert_eq!(names, vec![REPO]);
    let restored = Backend::open(restored_dir.path(), None, None).unwrap();
    let problems = fsck::run(&restored, fsck::BATCH, &mut |_| {}).unwrap();
    assert!(problems.problems.is_empty(), "{problems:?}");
    let repo = restored.read(|c| c.repo_id(REPO)).unwrap();
    let state = restored.read(|c| c.get_ref(repo, "main")).unwrap().unwrap();
    assert_eq!(
        state.target.to_string(),
        pinned_main,
        "the restored main resolves the golden's ID"
    );
    drop(restored);
}
