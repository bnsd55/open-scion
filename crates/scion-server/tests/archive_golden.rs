//! The archive's bytes, as the machine form of stability.md's promise
//! for `scion-archive-1`: an export of one fixed store produces the
//! same files with the same contents every run (the manifest's clock
//! reading and store UUID excepted), `verify` takes the golden with no
//! store code, and a restore of it resolves the pinned `main`. A diff
//! in the golden is a format change: docs/archive-format.md's version,
//! beside v1, and a changes/ fragment naming the break. `open_test` runs
//! this under test (postgres) too, so the golden also proves both
//! backends export the same bytes.
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
    rows.sort_by(|a, b| a.1.cmp(&b.1));
    rows
}

/// The export's files hash to the golden's rows, the manifest's stable
/// fields say what the format says, and the restore resolves the
/// golden's `main` - on a fresh store, `fsck` clean.
/// The manifest is a format surface (docs/archive-format.md names its
/// keys). Its two run-dependent values normalize: the store UUID to
/// `<uuid or null>` - null on SQLite, which keeps the UUID in the
/// database, and 32 lowercase hex on Postgres, which writes the marker
/// file - and the snapshot instant to `<time>` after it shows as an
/// RFC 3339 stamp to the second with a Z. The rest of the manifest
/// pins as text, so a renamed key or a dropped array shows as a
/// readable diff.
fn normalized_manifest(manifest: &serde_json::Value) -> String {
    let uuid = &manifest["store_uuid"];
    if Backend::test_is_postgres() {
        let hex = uuid
            .as_str()
            .unwrap_or_else(|| panic!("Postgres writes the marker file"));
        assert!(
            hex.len() == 32
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "the store UUID is not 32 lowercase hex"
        );
    } else {
        assert!(
            uuid.is_null(),
            "SQLite keeps the UUID in the database; the manifest names one"
        );
    }
    let stamp = manifest["snapshot_at"]
        .as_str()
        .expect("the snapshot instant is a string");
    assert_eq!(
        stamp.len(),
        20,
        "the stamp is not RFC 3339 to the second with a Z"
    );
    assert_eq!(&stamp[..2], "20", "the stamp starts with a year");
    assert!(stamp.ends_with('Z'), "the stamp does not end in Z");
    let mut normalized = serde_json::to_string_pretty(manifest).unwrap();
    normalized = normalized.replace(
        &format!("\"store_uuid\": {}", serde_json::to_string(uuid).unwrap()),
        "\"store_uuid\": \"<uuid or null>\"",
    );
    normalized.replace(
        &format!("\"snapshot_at\": \"{stamp}\""),
        "\"snapshot_at\": \"<time>\"",
    )
}

#[test]
fn the_export_is_the_golden_and_restores_to_it() {
    let data = tempfile::tempdir().unwrap();
    let (store, main) = source(data.path());

    let out = data.path().join("archive");
    let manifest = archive::export(&store, data.path(), None, true, &out).unwrap();
    let normalized = normalized_manifest(&manifest);

    let rows = rows(&out);
    // The manifest's normalized text is the golden's first section: a
    // key change shows as a readable diff.
    let mut text = String::new();
    text.push_str("# The manifest, normalized: the run's UUID and clock pin nothing.\n");
    text.push_str(&normalized);
    text.push('\n');
    text.push_str("# One row per archive file, hash and path, sorted by path.\n");
    for (hex, path) in &rows {
        writeln!(text, "{hex}  {path}").unwrap();
    }
    text.push_str("# The restored store's main resolves to:\n");
    writeln!(text, "main  {main}").unwrap();
    if std::env::var_os("SCION_UPDATE_ARCHIVE_GOLDEN").is_some() {
        fs::write(GOLDEN, &text).unwrap();
    }

    let golden = fs::read_to_string(GOLDEN).unwrap();
    let mut golden_rows: Vec<(String, String)> = Vec::new();
    let mut pinned_main = String::new();
    for line in golden.lines() {
        if line.starts_with('#')
            || line.trim().is_empty()
            || line.starts_with('{')
            || line.starts_with('"')
            || line.starts_with('}')
            || line.starts_with("  ")
        {
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
    // The manifest's stable text pins too, from the golden: the
    // normalized manifest the run built appears in it verbatim.
    let is_manifest_line = |l: &str| {
        l.starts_with('{') || l.starts_with('"') || l.starts_with("  ") || l == "}" || l == "{"
    };
    let manifest_rows = golden.lines().filter(|l| is_manifest_line(l));
    let golden_manifest: String = manifest_rows.collect::<Vec<_>>().join("\n");
    let built_manifest: String = normalized
        .lines()
        .filter(|l| is_manifest_line(l))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !golden_manifest.is_empty(),
        "the golden carries the normalized manifest"
    );
    assert_eq!(
        built_manifest, golden_manifest,
        "the manifest's keys changed: a format change is a new archive \
         version beside v1, and a changes/ fragment naming the break"
    );

    // The archive is sound, per the format, with no store code.
    let damages = archive::verify(&out).unwrap();
    assert!(damages.is_empty(), "{damages:?}");

    // A restore of the golden into an empty data directory: fsck clean,
    // and `main` resolves to the ID the golden pins.
    let restored_dir = tempfile::tempdir().unwrap();
    let report = archive::restore(&out, restored_dir.path(), None, None).unwrap();
    assert_eq!(report.repos, vec![REPO]);
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
