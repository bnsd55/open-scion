//! The attacks an adversary runs against the archive reader and the
//! restore (#410): a crafted archive whose manifest names one repo
//! twice, a changeset stream that claims a huge length, a changeset
//! recorded twice, a members list with a duplicate line, a ref that
//! names another repo's changeset, and two pending lands that share a
//! request key. Each test states the attack and the answer the reader
//! must give; a test that passes is proof the reader holds, a test
//! that fails names a bug.

use std::path::{Path, PathBuf};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, archive, test_support::tree};

const REPO: &str = "main";
const SECOND: &str = "second";

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

/// A store with two repos: `main` with three changesets over three
/// blobs, and `second` with one changeset over its own blob.
fn source(data: &Path) -> Backend {
    let store = Backend::open_test(data).unwrap();
    for content in ["a", "b", "c", "x"] {
        store.blobs().put(content.as_bytes()).unwrap();
    }
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut parent = None;
            for name in ["a", "b", "c"] {
                let cs = changeset(parent.into_iter().collect(), &[(name, Some(regular(name)))]);
                parent = Some(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0);
            }
            assert!(c.cas_ref(repo, "main", None, parent.expect("depth > 0"))?);
            for content in ["a", "b", "c"] {
                c.add_repo_blob(repo, BlobId::of(content.as_bytes()))?;
            }
            let second = c.create_repo(SECOND)?;
            let cs = changeset(vec![], &[("x", Some(regular("x")))]);
            let id = tree::store_changeset(c, second, &cs, &mut |_| Ok(0))?.0;
            assert!(c.cas_ref(second, "main", None, id)?);
            c.add_repo_blob(second, BlobId::of(b"x"))?;
            Ok(())
        })
        .unwrap();
    store
}

/// Exports the whole store and drops the store handle, so the crafted
/// archive stands alone. Returns the archive directory and its temp
/// parent (which must outlive the test).
fn exported(store: &Backend, data: &Path) -> (tempfile::TempDir, PathBuf) {
    let parent = tempfile::tempdir().unwrap();
    let out = parent.path().join("archive");
    archive::export(store, data, None, true, &out).unwrap();
    (parent, out)
}

/// The repo section directory of `name` in the archive.
fn section<'a>(archive: &Path, name: &str) -> PathBuf {
    archive.join("repos").join(name)
}

/// A sound archive of the two-repo store, and each repo's tip
/// changeset as the hex its JSON section names.
fn sound_archive() -> (tempfile::TempDir, PathBuf, String, String) {
    let data = tempfile::tempdir().unwrap();
    let store = source(data.path());
    let (parent, out) = exported(&store, data.path());
    let tip = |repo: &str| {
        let refs = std::fs::read_to_string(section(&out, repo).join("refs.json")).unwrap();
        let rows: serde_json::Value = serde_json::from_str(&refs).unwrap();
        rows[0]["target"].as_str().unwrap().to_owned()
    };
    let (main_tip, second_tip) = (tip(REPO), tip(SECOND));
    (parent, out, main_tip, second_tip)
}

/// The attack: the manifest names one repo twice. Whatever a reader
/// does with the second entry, `verify` must name the manifest as the
/// damage, and `restore` must refuse before it writes a row: a
/// half-restored store is the attacker's win, because the operator
/// cannot tell restored state from unrestored.
#[test]
fn a_manifest_naming_one_repo_twice_is_refused_before_any_write() {
    let (_keep, out, _main, _second) = sound_archive();
    let manifest_path = out.join("manifest.json");
    let manifest = std::fs::read_to_string(&manifest_path).unwrap();
    let crafted = manifest.replace(
        r#""repos": ["#,
        r#""repos": [{"name": "main", "changesets": 3, "refs": 1, "pending_lands": 0, "members": 3, "provenance": 0},"#,
    );
    assert_ne!(crafted, manifest, "the manifest's shape moved upstream");
    std::fs::write(&manifest_path, crafted).unwrap();

    let damages = archive::verify(&out).unwrap();
    assert!(
        damages
            .iter()
            .any(|d| d.contains("twice") || d.contains("duplicate")),
        "verify must name the duplicate repo in the manifest: {damages:?}"
    );

    let empty = tempfile::tempdir().unwrap();
    let restored = archive::restore(&out, empty.path(), None, None);
    assert!(
        restored.is_err(),
        "a manifest that names one repo twice must not restore"
    );
    let written = std::fs::read_dir(empty.path()).unwrap().count();
    assert_eq!(
        written, 0,
        "a refused restore must not leave a half-restored store"
    );
}

/// The attack: a changeset record whose body length claims 4 GiB. The
/// reader must answer "ends inside a record" and stop — not allocate
/// 4 GiB of zeros because the length said so.
#[test]
fn a_changeset_stream_claiming_a_huge_length_is_torn_not_allocated() {
    let (_keep, out, _main, _second) = sound_archive();
    let stream = section(&out, REPO).join("changesets.bin");
    let mut bytes = std::fs::read(&stream).unwrap();
    assert!(bytes.len() > 44, "the sound archive holds one record");
    bytes[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
    std::fs::write(&stream, &bytes).unwrap();

    let verified = archive::verify(&out);
    match verified {
        // The reader refused the stream outright: the length never
        // became an allocation.
        Err(scion_server::Error::Corrupt(why)) => {
            assert!(why.contains("ends inside a record"), "{why}");
        }
        Ok(damages) => assert!(
            damages.iter().any(|d| d.contains("ends inside a record")),
            "a huge claimed length must be called torn: {damages:?}"
        ),
        other => panic!("expected a torn-stream answer, got {other:?}"),
    }
}

/// The attack: the same changeset recorded twice in one repo's stream.
/// The ID is the hash of the body, so both records re-hash true, and
/// nothing in `verify` says a stream must hold a changeset once. The
/// restore replays the stream in order: the second replay must not
/// make a partial store (a repo created, blobs written, then an
/// error), so `verify` must name the duplicate, or the replay must
/// tolerate it — one of the two, loudly.
#[test]
fn a_changeset_recorded_twice_is_named_or_tolerated_not_half_restored() {
    let (_keep, out, main_tip, _second) = sound_archive();
    let stream = section(&out, REPO).join("changesets.bin");
    let bytes = std::fs::read(&stream).unwrap();
    let _ = main_tip;
    // The stream is one record per changeset in generation order; the
    // whole stream is one record here (the fixture repo's three
    // changesets are three records) — duplicate the LAST record, whose
    // children are none, so the shape stays topological.
    let mut records: Vec<&[u8]> = Vec::new();
    let mut at = 0_usize;
    while at < bytes.len() {
        let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        let end = at + 44 + len;
        records.push(&bytes[at..end]);
        at = end;
    }
    let last = *records.last().expect("records exist");
    let mut crafted = bytes.clone();
    crafted.extend_from_slice(last);
    std::fs::write(&stream, crafted).unwrap();

    let damages = archive::verify(&out).unwrap();
    if !damages.is_empty() {
        // The reader named the duplicate: proof it holds.
        assert!(
            damages.iter().any(|d| d.contains("twice") || d.contains("duplicate")),
            "verify must name the duplicate record, not something else: {damages:?}"
        );
        return;
    }
    // The reader was silent: the replay must still not leave a partial
    // store. It may tolerate the duplicate, or refuse before writing.
    let empty = tempfile::tempdir().unwrap();
    let restored = archive::restore(&out, empty.path(), None, None);
    if restored.is_err() {
        let written = std::fs::read_dir(empty.path()).unwrap().count();
        assert_eq!(
            written, 0,
            "a refused replay must not leave a half-restored store"
        );
    }
}

/// The attack: the same blob ID twice in members.txt. The restore puts
/// the same bytes twice — the ID is the hash of the bytes, so the
/// second put is the same file — and the tree lens read the size twice.
/// The archive must still verify and restore: a duplicate line is
/// noise, not damage. Proof the reader holds.
#[test]
fn a_duplicated_member_line_verifies_and_restores() {
    let (_keep, out, _main, _second) = sound_archive();
    let members_path = section(&out, REPO).join("members.txt");
    let members = std::fs::read_to_string(&members_path).unwrap();
    let first = members.lines().next().expect("a member exists");
    std::fs::write(&members_path, format!("{members}{first}\n")).unwrap();

    let damages = archive::verify(&out).unwrap();
    assert!(
        damages.is_empty(),
        "a duplicated member line is noise, not damage: {damages:?}"
    );
    let empty = tempfile::tempdir().unwrap();
    let names = archive::restore(&out, empty.path(), None, None)
        .expect("a duplicated member line must not break the restore");
    assert_eq!(names.len(), 2);
}

/// The attack: a ref in `second` that names a changeset of `main` —
/// two repos of the same archive, so the target exists in the archive
/// but not in the repo the ref belongs to. `verify` checks refs
/// against the same repo's changesets, so the damage is named and the
/// restore refuses. Proof the reader holds.
#[test]
fn a_ref_to_another_repos_changeset_is_refused() {
    let (_keep, out, main_tip, _second) = sound_archive();
    // The refs file's shape: an array of {name, target, version}; the
    // target is hex. Write it as the reader reads it.
    let crafted = serde_json::json!([
        {"name": "main", "target": main_tip, "version": 1}
    ]);
    std::fs::write(&section(&out, SECOND).join("refs.json"), crafted.to_string()).unwrap();

    let damages = archive::verify(&out).unwrap();
    assert!(
        damages
            .iter()
            .any(|d| d.contains(SECOND) && d.contains("does not hold")),
        "a ref into another repo must be a named damage: {damages:?}"
    );
    let empty = tempfile::tempdir().unwrap();
    assert!(
        archive::restore(&out, empty.path(), None, None).is_err(),
        "a cross-repo ref must not restore"
    );
}

/// The attack: two pending lands in one repo that share a request key.
/// The restore records both; the store keys a pending land by the
/// request key, so the second record either collides (an error
/// mid-replay, with the repo already created — a partial store) or
/// silently replaces the first. `verify` must name the duplicate key,
/// or the restore must tolerate it; a half-restored store is neither.
#[test]
fn two_pending_lands_sharing_a_request_key_are_named_or_tolerated() {
    let (_keep, out, main_tip, _second) = sound_archive();
    let lands_path = section(&out, REPO).join("lands.json");
    let crafted = serde_json::json!([
        {"caller": "attacker", "request_key": "one-key", "target_ref": "main",
         "input_cs": main_tip},
        {"caller": "attacker", "request_key": "one-key", "target_ref": "dev",
         "input_cs": main_tip},
    ]);
    std::fs::write(&lands_path, crafted.to_string()).unwrap();

    let damages = archive::verify(&out).unwrap();
    if !damages.is_empty() {
        assert!(
            damages.iter().any(|d| d.contains("one-key")),
            "verify must name the colliding request key: {damages:?}"
        );
        return;
    }
    let empty = tempfile::tempdir().unwrap();
    let restored = archive::restore(&out, empty.path(), None, None);
    if restored.is_err() {
        let written = std::fs::read_dir(empty.path()).unwrap().count();
        assert_eq!(
            written, 0,
            "a refused replay must not leave a half-restored store"
        );
    }
}
