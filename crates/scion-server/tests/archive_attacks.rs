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
fn section(archive: &Path, name: &str) -> PathBuf {
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
            damages
                .iter()
                .any(|d| d.contains("twice") || d.contains("duplicate")),
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
    std::fs::write(section(&out, SECOND).join("refs.json"), crafted.to_string()).unwrap();

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

/// The attack: a changeset whose change names a blob the members.txt
/// does not list. Nothing else in `verify` looks at the changes'
/// members, so the reader passed what the replay would die on — the
/// reader and the writer disagreed about one archive. The cross-check
/// names it, and the restore refuses with the target as it went in.
#[test]
fn a_changeset_naming_an_unlisted_blob_is_named_and_refused() {
    let (_keep, out, _main, _second) = sound_archive();
    // Drop one member line the last changeset's tree names.
    let members_path = section(&out, REPO).join("members.txt");
    let members = std::fs::read_to_string(&members_path).unwrap();
    let lines: Vec<&str> = members.lines().collect();
    assert_eq!(lines.len(), 3, "the fixture holds three members");
    std::fs::write(&members_path, lines[..2].join("\n") + "\n").unwrap();

    let damages = archive::verify(&out).unwrap();
    assert!(
        damages
            .iter()
            .any(|d| d.contains("members.txt does not list")),
        "verify must name the changeset's unlisted blob: {damages:?}"
    );

    let empty = tempfile::tempdir().unwrap();
    let restored = archive::restore(&out, empty.path(), None, None);
    assert!(restored.is_err(), "a named damage must refuse the replay");
    let written = std::fs::read_dir(empty.path()).unwrap().count();
    assert_eq!(
        written, 0,
        "a refused restore must not leave a half-restored store"
    );
}

/// The attack behind the two above: whatever kills the replay halfway
/// — here the store's own fail seam, armed on the very store the
/// restore opens — the target must come out as it went in: empty. The
/// whole metadata replay is one transaction, so the store rolls back
/// to what it was, and the staging directory is dropped; nothing of
/// the restore remains anywhere.
#[test]
fn a_mid_replay_failure_leaves_the_target_empty() {
    let (_keep, out, _main, _second) = sound_archive();
    let empty = tempfile::tempdir().unwrap();
    let restored = archive::restore_with(&out, empty.path(), None, None, |path, url, ca| {
        let store = scion_server::Backend::open(path, url, ca)?;
        // The seam: the store's next commit — the whole metadata
        // transaction, every repo — fails as busy, the way a disk or a
        // lock would make it.
        store.fail_next_commits(1);
        Ok(store)
    });
    assert!(
        restored.is_err(),
        "a replay whose store refuses its commit must not report success"
    );
    let written = std::fs::read_dir(empty.path()).unwrap().count();
    assert_eq!(
        written, 0,
        "the target must stay empty when the replay dies halfway"
    );
    // This restore's own staging scratch, beside the target, is gone
    // with the failure: nothing of the restore remains anywhere.
    let staging = empty.path().with_file_name(format!(
        "{}.restoring",
        empty.path().file_name().unwrap().to_string_lossy()
    ));
    assert!(
        !staging.exists(),
        "the staging directory must not survive a failed restore: {}",
        staging.display()
    );
}

/// The Postgres half of the mid-replay attack: the whole metadata
/// replay is one transaction, so a replay whose write waits out the
/// store's lock clock and dies as busy rolls back every row, and the
/// failed restore resets the identity its open minted — the database
/// comes out as it went in, empty and nameless, and the operator's
/// retry against the same URL is refused by nothing. Runs only where
/// `SCION_TEST_POSTGRES_URL` names the job's own scratch instance.
#[test]
fn a_pg_replay_that_dies_halfway_leaves_the_database_empty() {
    let Ok(base) = std::env::var("SCION_TEST_POSTGRES_URL") else {
        return;
    };
    // The restore's database names one store, so the attack gets its
    // own: created and dropped here, never the tests' shared one.
    let name = format!("scion_restore_attack_{}", std::process::id());
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
    let (_keep, out, _main, _second) = sound_archive();
    let empty = tempfile::tempdir().unwrap();
    let holder_url = url.clone();
    let (locked, wait_locked) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let mut db = postgres::Client::connect(&holder_url, postgres::NoTls).unwrap();
        // The lock goes down as soon as the restore's open has made
        // its tables, and holds past the writer's 5 s clock: the
        // replay's one write dies as busy. The channel says the lock
        // is down, and the replay waits for that word before it runs.
        loop {
            let there: Option<String> = db
                .query_one("SELECT to_regclass('public.changesets')::text", &[])
                .unwrap()
                .get(0);
            if there.is_some() {
                db.batch_execute("BEGIN; LOCK TABLE changesets IN EXCLUSIVE MODE;")
                    .unwrap();
                let _ = locked.send(());
                std::thread::sleep(std::time::Duration::from_secs(30));
                let _ = db.batch_execute("COMMIT");
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    });
    let restored = archive::restore_with(&out, empty.path(), Some(&url), None, |path, url, ca| {
        let store = scion_server::Backend::open(path, url, ca)?;
        // The lock is on the table before the replay may write: the
        // store's 5 s clock runs out against it, and the write dies.
        wait_locked
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the holder never took its table");
        Ok(store)
    });
    assert!(
        restored.is_err(),
        "a replay that waited out the lock clock must not report success"
    );
    holder.join().unwrap();
    let written = std::fs::read_dir(empty.path()).unwrap().count();
    assert_eq!(
        written, 0,
        "the target must stay empty when the replay dies halfway"
    );
    // The database came out as it went in: no row the replay wrote,
    // and no identity — the next open mints again, so the retry is
    // refused by nothing. The replay may die before its schema
    // migration lands, so a table that does not exist counts as
    // empty; a table that exists must hold no rows, so a partial
    // schema with rows still fails.
    let mut check = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    for table in ["repos", "changesets", "refs", "store_uuid"] {
        let there: Option<String> = check
            .query_one(&format!("SELECT to_regclass('public.{table}')::text"), &[])
            .unwrap()
            .get(0);
        let rows: i64 = match there {
            None => 0,
            Some(_) => check
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .unwrap()
                .get(0),
        };
        assert_eq!(rows, 0, "{table} must be empty after a failed restore");
    }
    drop(check);
    maker
        .batch_execute(&format!("DROP DATABASE {name}"))
        .unwrap();
}
