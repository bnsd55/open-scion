//! The versioned logical archive: an export beside a landing writer, a
//! verify against the format, a restore into an empty store that fsck
//! finds clean and that resolves the same `main`, and a verify that
//! names a corrupted blob.

use std::path::Path;
use std::time::Duration;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, archive, fsck, test_support::tree};

const REPO: &str = "main";

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

fn regular(content: &str) -> FileState {
    FileState::Regular {
        content: BlobId::of(content.as_bytes()),
        executable: false,
    }
}

fn changeset(parents: Vec<ChangesetId>, changes: &[(&str, Option<FileState>)]) -> Changeset {
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
        b"change\n".to_vec(),
        changes.iter().map(|(p, s)| (path(p), *s)).collect(),
    )
    .expect("changeset fits the format")
}

/// A store with a repo whose main holds `depth` changesets over three
/// blobs, and a second repo. Returns the store, its data directory, and
/// the main ref of the snapshot the export is about to take.
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
                let cs = changeset(parent.into_iter().collect(), &[(name, Some(regular(name)))]);
                parent = Some(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0);
            }
            let main = parent.expect("depth > 0");
            assert!(c.cas_ref(repo, "main", None, main)?);
            for content in ["a", "b", "c"] {
                c.add_repo_blob(repo, BlobId::of(content.as_bytes()))?;
            }
            Ok(main)
        })
        .unwrap();
    (store, main)
}

#[test]
fn an_export_beside_a_writer_verifies_and_restores_resolving_main() {
    let data = tempfile::tempdir().unwrap();
    let (store, snapshot_main) = source(data.path());

    // A writer lands more changesets on `dev` while the export runs: the
    // export is beside a live server, not on an idle store. Its own read
    // snapshot keeps what it reads whole; whatever generation the export
    // sees, the archive must be internally sound.
    let writes = store.clone();
    let writer_thread = std::thread::spawn(move || {
        for i in 0..20 {
            let content = format!("writer-{i}");
            writes.blobs().put(content.as_bytes()).unwrap();
            writes
                .write(|c| {
                    let repo = c.repo_id(REPO)?;
                    let state = c.get_ref(repo, "dev")?;
                    let parent = match &state {
                        Some(state) => state.target,
                        None => c.get_ref(repo, "main")?.expect("main").target,
                    };
                    let cs = changeset(vec![parent], &[("w", Some(regular(&content)))]);
                    let id = tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0;
                    c.add_repo_blob(repo, BlobId::of(content.as_bytes()))?;
                    assert!(c.cas_ref(repo, "dev", state.map(|s| s.version), id)?);
                    Ok(id)
                })
                .unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }
    });

    let out = tempfile::tempdir().unwrap().path().join("archive");
    let manifest = match archive::export(&store, data.path(), None, true, &out) {
        Ok(manifest) => manifest,
        Err(e) => panic!("export failed: {e:#}"),
    };
    writer_thread.join().unwrap();
    assert_eq!(manifest["format"], "scion-archive");
    assert_eq!(manifest["version"], 1);
    assert_eq!(manifest["kind"], "store");

    // The archive is sound, per the format, with no store code.
    let damages = archive::verify(&out).unwrap();
    assert!(damages.is_empty(), "{damages:?}");

    // A restore into an EMPTY data directory: the same refs, the same
    // generations, a clean fsck, and a main that resolves to what the
    // snapshot read.
    let restored_dir = tempfile::tempdir().unwrap();
    let names = archive::restore(&out, restored_dir.path(), None, None).unwrap();
    assert_eq!(names, vec![REPO]);
    let restored = Backend::open(restored_dir.path(), None, None).unwrap();
    let problems = fsck::run(&restored, fsck::BATCH, &mut |_| {}).unwrap();
    assert!(problems.problems.is_empty(), "{problems:?}");
    let repo = restored.read(|c| c.repo_id(REPO)).unwrap();
    let state = restored.read(|c| c.get_ref(repo, "main")).unwrap().unwrap();
    let (main, version) = (state.target, state.version);
    assert_eq!(main, snapshot_main, "the restored main resolves the same");
    let original_version = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?.expect("main").version)
        })
        .unwrap();
    assert_eq!(version, original_version, "the ref version carries over");
    // The store derives the same generation for the tip changeset.
    let original_generation = {
        let original_repo = store.read(|c| c.repo_id(REPO)).unwrap();
        store
            .read(|c| c.generation(original_repo, snapshot_main))
            .unwrap()
    };
    let restored_generation = restored
        .read(|c| c.generation(repo, snapshot_main))
        .unwrap();
    assert_eq!(restored_generation, original_generation);
}

#[test]
fn a_verify_names_a_blob_whose_bytes_someone_corrupted() {
    let data = tempfile::tempdir().unwrap();
    let (store, _main) = source(data.path());
    let out = tempfile::tempdir().unwrap().path().join("archive");
    archive::export(&store, data.path(), Some(REPO), false, &out).unwrap();
    drop(store);

    // Corrupt one byte of one blob file, in place.
    let victim = data_path_of(&out, BlobId::of(b"b"));
    let mut bytes = std::fs::read(&victim).unwrap();
    let at = bytes.len() - 1;
    bytes[at] ^= 0xff;
    std::fs::write(&victim, &bytes).unwrap();

    // verify names the damage; restore refuses the store the file.
    let damages = archive::verify(&out).unwrap();
    assert_eq!(damages.len(), 1, "{damages:?}");
    assert!(
        damages[0].contains(&BlobId::of(b"b").to_string()),
        "{damages:?}"
    );
    let empty = tempfile::tempdir().unwrap();
    match archive::restore(&out, empty.path(), None, None) {
        Err(scion_server::Error::Corrupt(why)) => {
            assert!(why.contains("not sound"), "{why}");
        }
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[test]
fn a_manifest_name_that_escapes_the_archive_is_refused() {
    // The archive is input from outside the server: a repo name the
    // manifest holds must be one path component before it ever joins one.
    let data = tempfile::tempdir().unwrap();
    let (store, _main) = source(data.path());
    let out = tempfile::tempdir().unwrap().path().join("archive");
    archive::export(&store, data.path(), Some(REPO), false, &out).unwrap();
    drop(store);
    let manifest_path = out.join("manifest.json");
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
    for name in ["../escape", "/abs", "..", "."] {
        let mut evil = manifest.clone();
        evil["repos"] = serde_json::json!([{ "name": name, "changesets": 0 }]);
        std::fs::write(&manifest_path, serde_json::to_string(&evil).unwrap()).unwrap();
        match archive::verify(&out) {
            Err(scion_server::Error::Corrupt(why)) => {
                assert!(why.contains(name), "{why}");
            }
            Err(other) => panic!("expected Corrupt for {name:?}, got {other:?}"),
            Ok(damages) => panic!("expected a refusal for {name:?}, got {damages:?}"),
        }
        let empty = tempfile::tempdir().unwrap();
        match archive::restore(&out, empty.path(), None, None) {
            Err(scion_server::Error::Corrupt(why)) => {
                assert!(why.contains(name), "{why}");
            }
            other => panic!("expected a refusal for {name:?}, got {other:?}"),
        }
    }
}

#[test]
fn a_blob_file_that_expands_past_the_cap_is_refused() {
    // A decompression bomb: a tiny zstd frame whose content is far past
    // the server's cap for one blob. The reader refuses it without
    // holding the expansion.
    let data = tempfile::tempdir().unwrap();
    let (store, _main) = source(data.path());
    let out = tempfile::tempdir().unwrap().path().join("archive");
    archive::export(&store, data.path(), Some(REPO), false, &out).unwrap();
    drop(store);
    // Pick a member, and replace its file with a frame of zeros that is
    // over the cap when it expands. Hash it so the NAME is right: the
    // refusal must come from the cap, not the name check.
    let cap = scion_server::test_support::store::MAX_OBJECT_BYTES;
    let bomb_raw = vec![0_u8; cap + 1];
    let bomb_id = BlobId::of(&bomb_raw);
    let path = data_path_of(&out, bomb_id);
    let frame = zstd::bulk::compress(&bomb_raw, 3).unwrap();
    // The archive names its members; rewrite members.txt to name the bomb.
    let members = out.join("repos").join(REPO).join("members.txt");
    std::fs::write(&members, format!("{bomb_id}\n")).unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, &frame).unwrap();
    let damages = archive::verify(&out).unwrap();
    // The members.txt rewrite also leaves the changesets' own blobs
    // unlisted, and the cross-check that keeps the reader and the
    // replay honest names each of those; the bomb's cap refusal must
    // be among them.
    assert!(
        damages.iter().any(|d| d.contains("expands past the cap")),
        "{damages:?}"
    );
    let empty = tempfile::tempdir().unwrap();
    match archive::restore(&out, empty.path(), None, None) {
        Err(scion_server::Error::Corrupt(why)) => {
            assert!(why.contains("not sound"), "{why}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// The archive's blob file of `id`.
fn data_path_of(archive: &Path, id: BlobId) -> std::path::PathBuf {
    let hex = id.to_string();
    archive.join("blobs").join(&hex[..2]).join(&hex[2..])
}

#[test]
fn a_restore_refuses_a_data_directory_that_already_holds_a_store() {
    let data = tempfile::tempdir().unwrap();
    let (store, _main) = source(data.path());
    let out = tempfile::tempdir().unwrap().path().join("archive");
    archive::export(&store, data.path(), Some(REPO), false, &out).unwrap();
    match archive::restore(&out, data.path(), None, None) {
        Err(scion_server::Error::FailedPrecondition(why)) => {
            assert!(why.contains("already holds a store"), "{why}");
        }
        other => panic!("expected FailedPrecondition, got {other:?}"),
    }
}

#[test]
fn an_export_refuses_an_out_that_exists_and_a_repo_that_is_not_stored() {
    let data = tempfile::tempdir().unwrap();
    let (store, _main) = source(data.path());
    let out_dir = tempfile::tempdir().unwrap();
    let out = out_dir.path().join("archive");
    match archive::export(&store, data.path(), None, false, &out) {
        Err(scion_server::Error::InvalidArgument(why)) => {
            assert!(why.contains("exactly one"), "{why}");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
    std::fs::create_dir(&out).unwrap();
    match archive::export(&store, data.path(), Some(REPO), false, &out) {
        Err(scion_server::Error::FailedPrecondition(why)) => {
            assert!(why.contains("already exists"), "{why}");
        }
        other => panic!("expected FailedPrecondition, got {other:?}"),
    }
    let out_dir = tempfile::tempdir().unwrap();
    match archive::export(
        &store,
        data.path(),
        Some("nope"),
        false,
        &out_dir.path().join("x"),
    ) {
        Err(scion_server::Error::NotFound(_)) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[test]
fn a_verify_names_a_changeset_a_ref_points_past() {
    // A ref whose target the changeset stream does not hold: exactly the
    // torn-snapshot case the one-read rule exists for.
    let data = tempfile::tempdir().unwrap();
    let (store, _main) = source(data.path());
    let out = tempfile::tempdir().unwrap().path().join("archive");
    archive::export(&store, data.path(), Some(REPO), false, &out).unwrap();
    drop(store);
    let path = out.join("repos").join(REPO).join("refs.json");
    let text = std::fs::read_to_string(&path).unwrap();
    let ghost = ChangesetId::try_from(&[7_u8; 32][..]).unwrap();
    let text = text.replace(&snapshot_main_hex(&out), &ghost.to_string());
    std::fs::write(&path, text).unwrap();
    let damages = archive::verify(&out).unwrap();
    assert!(
        damages
            .iter()
            .any(|line| line.contains("which the archive does not hold")),
        "{damages:?}"
    );
}

fn snapshot_main_hex(archive: &Path) -> String {
    // The first target in refs.json is main's: read it back.
    let path = archive.join("repos").join(REPO).join("refs.json");
    let value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    value[0]["target"].as_str().expect("a target").to_owned()
}

#[test]
fn a_changeset_a_parent_points_past_is_named() {
    // Drop a middle changeset's record from the stream: its child names
    // a parent the archive does not hold, and verify names it.
    let data = tempfile::tempdir().unwrap();
    let (store, _main) = source(data.path());
    let out = tempfile::tempdir().unwrap().path().join("archive");
    archive::export(&store, data.path(), Some(REPO), false, &out).unwrap();
    drop(store);
    // Rewrite the stream without the record whose generation is 2.
    let bin = out.join("repos").join(REPO).join("changesets.bin");
    let bytes = std::fs::read(&bin).unwrap();
    let mut rewritten = Vec::new();
    let mut at = 0_usize;
    while at < bytes.len() {
        let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        let generation = i64::from_le_bytes(bytes[at + 36..at + 44].try_into().unwrap());
        if generation != 2 {
            rewritten.extend_from_slice(&bytes[at..at + 44 + len]);
        }
        at += 44 + len;
    }
    std::fs::write(&bin, &rewritten).unwrap();
    let damages = archive::verify(&out).unwrap();
    assert!(
        damages.iter().any(|line| line.contains("parent")),
        "{damages:?}"
    );
}

/// The wire attack, and the disk's answer: on a disk that folds names
/// (case, or Unicode normalization), an archive whose manifest names
/// two repos that fold to one name gives both names one section
/// directory, and a restore that trusted the bytes would replay one
/// repo's data under both. The disk, not a fold table, is the judge:
/// `verify` reads each section's (device, inode) and refuses a pair
/// that is one directory. This test runs where the fold is real; on a
/// case-sensitive disk the two names are two directories and the
/// symlink test below is the proof instead.
#[cfg(target_os = "macos")]
#[test]
fn an_archive_naming_two_repos_that_fold_to_one_is_refused() {
    let data = tempfile::tempdir().unwrap();
    let (store, _main) = source(data.path());
    let out = tempfile::tempdir().unwrap().path().join("archive");
    archive::export(&store, data.path(), None, true, &out).unwrap();
    drop(store);

    // Second the repo under a name that folds into it on this disk:
    // the manifest gains a "MAIN" section, and the section it names is
    // the one directory this disk already holds — an archive crafted
    // (or exported) so that both names read the same bytes.
    let manifest_path = out.join("manifest.json");
    let manifest = std::fs::read_to_string(&manifest_path).unwrap();
    let crafted = manifest.replace(
        r#""repos": ["#,
        r#""repos": [{"name": "MAIN", "changesets": 3, "refs": 1, "pending_lands": 0, "members": 3, "provenance": 0},"#,
    );
    assert_ne!(crafted, manifest, "the manifest's shape moved upstream");
    std::fs::write(&manifest_path, crafted).unwrap();

    let damages = archive::verify(&out).unwrap();
    assert!(
        damages
            .iter()
            .any(|d| d.contains("MAIN") && d.contains("fold")),
        "verify must name the folded pair: {damages:?}"
    );
    let empty = tempfile::tempdir().unwrap();
    let restored = archive::restore(&out, empty.path(), None, None);
    assert!(
        restored.is_err(),
        "a restore that would replay one repo's data under two names must refuse"
    );
    let written = std::fs::read_dir(empty.path()).unwrap().count();
    assert_eq!(written, 0, "a refused restore leaves the target as it was");
}

/// The same fold, the one way two names reach one directory on a
/// case-sensitive disk: a section that is a symlink to another's. The
/// reader refuses a section that is not a plain directory, on every OS.
#[test]
fn a_section_that_is_a_symlink_to_another_is_refused() {
    let data = tempfile::tempdir().unwrap();
    let (store, _main) = source(data.path());
    let out = tempfile::tempdir().unwrap().path().join("archive");
    archive::export(&store, data.path(), None, true, &out).unwrap();
    drop(store);

    // The manifest names a second repo whose section is a symlink to
    // the first's directory.
    let manifest_path = out.join("manifest.json");
    let manifest = std::fs::read_to_string(&manifest_path).unwrap();
    let crafted = manifest.replace(
        r#""repos": ["#,
        r#""repos": [{"name": "linked", "changesets": 3, "refs": 1, "pending_lands": 0, "members": 3, "provenance": 0},"#,
    );
    assert_ne!(crafted, manifest, "the manifest's shape moved upstream");
    std::fs::write(&manifest_path, crafted).unwrap();
    std::os::unix::fs::symlink(
        out.join("repos").join(REPO),
        out.join("repos").join("linked"),
    )
    .unwrap();

    let damages = archive::verify(&out).unwrap();
    assert!(
        damages
            .iter()
            .any(|d| d.contains("linked") && d.contains("not a plain directory")),
        "verify must refuse a section that is a symlink: {damages:?}"
    );
    let empty = tempfile::tempdir().unwrap();
    let restored = archive::restore(&out, empty.path(), None, None);
    assert!(restored.is_err(), "a named damage must refuse the replay");
}

/// The export half of the fold: on a disk that folds names, exporting a
/// store whose repo names fold to one refuses, instead of writing the
/// second repo's files over the first's and calling the corrupt
/// archive sound. Runs where the fold is real.
#[cfg(target_os = "macos")]
#[test]
fn an_export_of_two_repos_that_fold_to_one_is_refused() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(&data.path().join("store")).unwrap();
    for name in ["main", "MAIN"] {
        store
            .write(|c| {
                let repo = c.create_repo(name)?;
                let cs = changeset(vec![], &[("a", Some(regular("a")))]);
                let id = tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0;
                assert!(c.cas_ref(repo, "main", None, id)?);
                c.add_repo_blob(repo, BlobId::of(b"a"))?;
                Ok(())
            })
            .unwrap();
    }
    let out = tempfile::tempdir().unwrap().path().join("archive");
    let error = archive::export(&store, data.path(), None, true, &out).unwrap_err();
    assert!(
        error.to_string().contains("fold"),
        "the export must name the fold: {error:#}"
    );
}
