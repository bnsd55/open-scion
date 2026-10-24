//! The attacks an adversary runs against the archive reader and the
//! restore (#410): a crafted archive whose manifest names one repo
//! twice, a changeset stream that claims a huge length, a changeset
//! recorded twice, a members list with a duplicate line, a ref that
//! names another repo's changeset, and two pending lands that share a
//! request key. Each test states the attack and the answer the reader
//! must give; a test that passes is proof the reader holds, a test
//! that fails names a bug.

use std::path::{Path, PathBuf};
use std::process::Command;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, archive, test_support::tree};

static LOG: std::sync::LazyLock<Captured> = std::sync::LazyLock::new(Captured::default);

#[derive(Clone, Default)]
struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Installs the one global subscriber, once: the restore's failure log
/// lands in the buffer a test reads from its own mark.
fn init_log() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::level_filters::LevelFilter::TRACE)
            .with_writer(Captured::clone(&*LOG))
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
    });
}

fn mark() -> usize {
    LOG.0.lock().unwrap().len()
}

fn since(mark: usize) -> String {
    String::from_utf8(LOG.0.lock().unwrap()[mark..].to_vec()).unwrap()
}

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
    let report = archive::restore(&out, empty.path(), None, None)
        .expect("a duplicated member line must not break the restore");
    assert_eq!(report.repos.len(), 2);
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
    // own: created and dropped here, never the tests' shared one. The
    // name is this test's own, not only this process's: the tests run
    // in parallel, and a shared name would drop a sibling's database.
    let name = format!("scion_restore_attack_replay_{}", std::process::id());
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
        // Bounded: a table that never appears hangs this thread forever
        // and CI dies of a timeout with no name — so 30 s, then panic.
        let started = std::time::Instant::now();
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
            assert!(
                started.elapsed() < std::time::Duration::from_secs(30),
                "the test database never gained its changesets table within 30 s"
            );
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
    assert_pg_database_empty(&mut check);
    drop(check);
    maker
        .batch_execute(&format!("DROP DATABASE {name}"))
        .unwrap();
}

/// The four tables a Scion Postgres store holds, checked against the
/// failed restore's database: a table that does not exist counts as
/// empty, and a table that exists must hold no rows.
fn assert_pg_database_empty(check: &mut postgres::Client) {
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
}

/// The bounded cleanup: a lock held on the staging schema past the
/// reset's clock must not hang the failed restore forever - the
/// restore fails within the bound, one log line names the staging
/// schema left behind, and the next restore, whose start resets the
/// schema, succeeds once the lock is gone. Runs only where
/// `SCION_TEST_POSTGRES_URL` names the job's own scratch instance.
#[test]
fn a_stale_lock_cannot_hang_the_failed_restore_and_the_next_one_lands() {
    let Ok(base) = std::env::var("SCION_TEST_POSTGRES_URL") else {
        return;
    };
    init_log();
    let name = format!("scion_restore_attack_stale_{}", std::process::id());
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
        // The lock goes down as soon as the restore's open has made its
        // tables, and holds PAST the reset's 60 s lock clock: the
        // failure-path drop must give up on the clock, not hang.
        let started = std::time::Instant::now();
        loop {
            let there: Option<String> = db
                .query_one("SELECT to_regclass('public.changesets')::text", &[])
                .unwrap()
                .get(0);
            if there.is_some() {
                db.batch_execute("BEGIN; LOCK TABLE changesets IN EXCLUSIVE MODE;")
                    .unwrap();
                let _ = locked.send(());
                std::thread::sleep(std::time::Duration::from_secs(75));
                let _ = db.batch_execute("COMMIT");
                return;
            }
            assert!(
                started.elapsed() < std::time::Duration::from_secs(30),
                "the test database never gained its changesets table within 30 s"
            );
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    });
    let mark = mark();
    let started = std::time::Instant::now();
    let restored = archive::restore_with(&out, empty.path(), Some(&url), None, |path, url, ca| {
        let store = scion_server::Backend::open(path, url, ca)?;
        wait_locked
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the holder never took its table");
        // The seam: the replay's first write dies as busy, and the
        // cleanup's drop waits behind the holder's table lock.
        store.fail_next_commits(1);
        Ok(store)
    });
    assert!(
        restored.is_err(),
        "a replay whose store refuses its commit must not report success"
    );
    // Within the bound: the store's 5 s clock, then the reset's 60 s,
    // not the forever the unbounded drop waited.
    let took = started.elapsed();
    assert!(
        took < std::time::Duration::from_secs(110),
        "the failed restore took {took:?}; the reset's clock did not bound it"
    );
    let log = since(mark);
    assert!(
        log.contains("is left behind"),
        "the failure must name the staging schema it left behind: {log}"
    );
    holder.join().unwrap();
    // The lock is gone: the next restore's start resets the leftover
    // schema, and the restore lands whole.
    let report = archive::restore(&out, empty.path(), Some(&url), None)
        .unwrap_or_else(|e| panic!("the restore after the stale lock failed: {e:#}"));
    assert_eq!(report.repos.len(), 2);
    maker
        .batch_execute(&format!("DROP DATABASE {name}"))
        .unwrap();
}

/// The data-loss attack (the #575 review's F1): a restore pointed at
/// the URL of a real store whose server is DOWN. A stopped store holds
/// no one-server lock, so the start cleanup's only guard cannot see
/// it: the drop would erase the store's whole schema. A restore may
/// drop only what a restore made - a schema that holds a store's
/// identity and no restore's staging mark is refused, unchanged.
/// Runs only where `SCION_TEST_POSTGRES_URL` names the job's own
/// scratch instance.
#[test]
fn a_restore_at_a_stopped_stores_url_refuses_and_leaves_it_whole() {
    let Ok(base) = std::env::var("SCION_TEST_POSTGRES_URL") else {
        return;
    };
    let name = format!("scion_restore_attack_stopped_{}", std::process::id());
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
    // A real store, with data: one repo, one row the restore has no
    // right to touch.
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open(data.path(), Some(&url), None).unwrap();
    store.write(|c| c.create_repo("real").map(|_| ())).unwrap();
    drop(store);
    // The counts come from the scratch database the store lives in,
    // not the shared one: a query on the wrong database proves nothing.
    let mut check = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    let repos_before: i64 = check
        .query_one("SELECT count(*) FROM repos", &[])
        .unwrap()
        .get(0);
    assert_eq!(repos_before, 1, "the store holds its repo");

    // The restore, at the stopped store's URL, into an empty target.
    let (_keep, out, _main, _second) = sound_archive();
    let target = tempfile::tempdir().unwrap().path().join("restored");
    std::fs::create_dir_all(&target).unwrap();
    let restored = archive::restore(&out, &target, Some(&url), None);
    assert!(
        restored.is_err(),
        "a restore at a stopped store's URL must refuse, not drop it"
    );
    let why = format!("{:#}", restored.unwrap_err());
    assert!(
        why.contains("refuses to drop"),
        "the refusal must say what it protects: {why}"
    );
    let repos_after: i64 = check
        .query_one("SELECT count(*) FROM public.repos", &[])
        .unwrap()
        .get(0);
    drop(check);
    assert_eq!(
        repos_after, repos_before,
        "the refused restore must leave the store's rows unchanged"
    );
    maker
        .batch_execute(&format!("DROP DATABASE {name}"))
        .unwrap();
}

/// F1: the guards read the schema the URL lands in, the same one the
/// lock and the drop act on. A stopped store whose schema is not
/// `public`, reached by a URL whose `search_path` names it, must
/// refuse with every row intact: the guards read its rows, not
/// `public`'s. Red
/// on today's code: the guards read `public` (empty), so the restore
/// dropped the stopped store's real schema. Runs only where
/// `SCION_TEST_POSTGRES_URL` names the job's own scratch instance.
#[test]
fn a_stopped_store_in_a_search_path_schema_is_refused_whole() {
    let Ok(base) = std::env::var("SCION_TEST_POSTGRES_URL") else {
        return;
    };
    let name = format!("scion_restore_attack_schema_{}", std::process::id());
    let mut maker = postgres::Client::connect(&base, postgres::NoTls).unwrap();
    maker
        .batch_execute(&format!("DROP DATABASE IF EXISTS {name}"))
        .unwrap();
    maker
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .unwrap();
    let root = base
        .rsplit_once('/')
        .map_or(base.as_str(), |(rest, _)| rest);
    let plain = format!("{root}/{name}");
    // The store's schema is not `public`: a URL whose search_path
    // names `privateschema` reaches it, and current_schema() follows.
    // The schema rides the scratch database, not the shared one.
    postgres::Client::connect(&plain, postgres::NoTls)
        .unwrap()
        .batch_execute("CREATE SCHEMA privateschema")
        .unwrap();
    // The search_path keeps `public` after the schema: the catalog
    // lookups ride it, as an operator's URL would.
    let url = format!("{root}/{name}?options=--search_path%3Dprivateschema%2Cpublic");
    let (_keep, out, _main, _second) = sound_archive();
    let target = tempfile::tempdir().unwrap().path().join("restored-schema");
    std::fs::create_dir_all(&target).unwrap();
    // Restore A publishes into the private schema: a complete store.
    archive::restore(&out, &target, Some(&url), None)
        .unwrap_or_else(|e| panic!("restore A failed: {e:#}"));
    let mut check = postgres::Client::connect(&plain, postgres::NoTls).unwrap();
    let repos_before: i64 = check
        .query_one("SELECT count(*) FROM privateschema.repos", &[])
        .unwrap()
        .get(0);
    assert!(
        repos_before >= 1,
        "restore A landed its repos in the private schema"
    );
    drop(check);
    // Restore B at the same URL: the stopped store's own schema is
    // reached by the search_path, so its rows decide - REFUSED, whole.
    let target_b = tempfile::tempdir()
        .unwrap()
        .path()
        .join("restored-schema-b");
    std::fs::create_dir_all(&target_b).unwrap();
    let restored = archive::restore(&out, &target_b, Some(&url), None);
    assert!(
        restored.is_err(),
        "a restore at a stopped store's search_path URL must refuse"
    );
    let mut check = postgres::Client::connect(&plain, postgres::NoTls).unwrap();
    let repos_after: i64 = check
        .query_one("SELECT count(*) FROM privateschema.repos", &[])
        .unwrap()
        .get(0);
    let schema_left: i64 = check
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 'privateschema'",
            &[],
        )
        .unwrap()
        .get(0);
    drop(check);
    assert_eq!(
        repos_after, repos_before,
        "the refused restore must leave the store's rows unchanged"
    );
    assert_eq!(schema_left, 1, "the store's schema is intact");
    maker
        .batch_execute(&format!("DROP DATABASE {name}"))
        .unwrap();
}

/// R1: the guards quote the schema as an identifier in the database,
/// so a mixed-case name is the name the guards read and the drop
/// drops - not a case-folded lookup over `store` while the drop tears
/// `Stopped_Store` down. A lowercase twin schema sits beside it, empty,
/// and must stay empty: the guards must read the `search_path`'s schema,
/// never the fold. Red on 1d5b54b7: the guards read the fold.
#[test]
fn a_mixed_case_schema_is_read_and_dropped_as_itself() {
    let Ok(base) = std::env::var("SCION_TEST_POSTGRES_URL") else {
        return;
    };
    let name = format!("scion_restore_attack_case_{}", std::process::id());
    let mut maker = postgres::Client::connect(&base, postgres::NoTls).unwrap();
    maker
        .batch_execute(&format!("DROP DATABASE IF EXISTS {name}"))
        .unwrap();
    maker
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .unwrap();
    let root = base
        .rsplit_once('/')
        .map_or(base.as_str(), |(rest, _)| rest);
    let plain = format!("{root}/{name}");
    // The store's schema is mixed case; the twin is its case fold. The
    // URL's search_path names the mixed-case one, quoted, because a
    // search_path with an upper-case member needs the quotes.
    postgres::Client::connect(&plain, postgres::NoTls)
        .unwrap()
        .batch_execute("CREATE SCHEMA \"Stopped_Store\"; CREATE SCHEMA stopped_store")
        .unwrap();
    let url = format!("{plain}?options=--search_path%3D%22Stopped_Store%22%2Cpublic");
    let (_keep, out, _main, _second) = sound_archive();
    let target = tempfile::tempdir().unwrap().path().join("restored-case");
    std::fs::create_dir_all(&target).unwrap();
    archive::restore(&out, &target, Some(&url), None)
        .unwrap_or_else(|e| panic!("restore A failed: {e:#}"));
    let mut check = postgres::Client::connect(&plain, postgres::NoTls).unwrap();
    let repos_before: i64 = check
        .query_one("SELECT count(*) FROM \"Stopped_Store\".repos", &[])
        .unwrap()
        .get(0);
    let twin_absent: bool = check
        .query_opt("SELECT to_regclass('stopped_store.repos')::text", &[])
        .unwrap()
        .and_then(|row| row.get::<_, Option<String>>(0))
        .is_none();
    drop(check);
    assert!(
        repos_before >= 1,
        "restore A landed its repos in the mixed-case schema"
    );
    // Restore B at the same URL: refused - the stopped store's rows
    // decide - and the twin stays untouched.
    let target_b = tempfile::tempdir().unwrap().path().join("restored-case-b");
    std::fs::create_dir_all(&target_b).unwrap();
    assert!(
        archive::restore(&out, &target_b, Some(&url), None).is_err(),
        "a restore at a stopped store's mixed-case schema must refuse"
    );
    let mut check = postgres::Client::connect(&plain, postgres::NoTls).unwrap();
    let repos_after: i64 = check
        .query_one("SELECT count(*) FROM \"Stopped_Store\".repos", &[])
        .unwrap()
        .get(0);
    let twin_after_absent: bool = check
        .query_opt("SELECT to_regclass('stopped_store.repos')::text", &[])
        .unwrap()
        .and_then(|row| row.get::<_, Option<String>>(0))
        .is_none();
    drop(check);
    assert_eq!(
        repos_after, repos_before,
        "the refused restore must leave the store's rows unchanged"
    );
    assert!(twin_absent, "the twin starts empty");
    assert!(
        twin_after_absent,
        "the twin stays empty: the guards read the search_path's schema, never the fold"
    );
    maker
        .batch_execute(&format!("DROP DATABASE {name}"))
        .unwrap();
}

/// The published store must not carry the mark (Y1): restore A lands,
/// a server serves it and lands one more change, the server stops, and
/// restore B at the same URL must refuse - not drop a real store as a
/// leftover. Red if the mark survives publication. Runs only where
/// `SCION_TEST_POSTGRES_URL` names the job's own scratch instance.
#[test]
fn a_restored_store_does_not_carry_the_mark_and_a_later_restore_refuses() {
    let Ok(base) = std::env::var("SCION_TEST_POSTGRES_URL") else {
        return;
    };
    let name = format!("scion_restore_attack_published_{}", std::process::id());
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
    let target = tempfile::tempdir().unwrap().path().join("restored-a");
    std::fs::create_dir_all(&target).unwrap();
    // Restore A publishes; the mark must be gone when it reports.
    archive::restore(&out, &target, Some(&url), None)
        .unwrap_or_else(|e| panic!("restore A failed: {e:#}"));
    // The checks run against the scratch database the restore used,
    // not the shared one: a query on the wrong database passes
    // trivially and proves nothing.
    let mut check = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    let mark_there: Option<String> = check
        .query_one("SELECT to_regclass('public.restore_staging')::text", &[])
        .unwrap()
        .get(0);
    assert!(
        mark_there.is_none(),
        "a published store carries the restore's mark: {mark_there:?}"
    );
    // The published store's rows are the state a later restore meets:
    // a store's identity, no mark, and rows a stopped server's schema
    // would hold.
    let repos_before: i64 = check
        .query_one("SELECT count(*) FROM repos", &[])
        .unwrap()
        .get(0);
    assert!(repos_before >= 1, "restore A landed its repos");
    // Restore B at the same URL, empty target: refused, A intact.
    let target_b = tempfile::tempdir().unwrap().path().join("restored-b");
    std::fs::create_dir_all(&target_b).unwrap();
    let restored = archive::restore(&out, &target_b, Some(&url), None);
    assert!(
        restored.is_err(),
        "a restore at the published store's URL must refuse"
    );
    let repos_after: i64 = check
        .query_one("SELECT count(*) FROM repos", &[])
        .unwrap()
        .get(0);
    assert_eq!(repos_after, repos_before, "A's rows are intact");
    drop(check);
    maker
        .batch_execute(&format!("DROP DATABASE {name}"))
        .unwrap();
}

/// The earlier half of the promise, made deterministic: the restore
/// dies at its open — the opener fails before any table exists — so
/// the database comes out exactly as it went in, without a single
/// Scion table. The old check (count(*) alone) panicked here with
/// `relation "store_uuid" does not exist` whenever the death came
/// before the schema; this case pins that the new check holds.
#[test]
fn a_pg_restore_that_dies_at_the_open_leaves_the_database_empty() {
    let Ok(base) = std::env::var("SCION_TEST_POSTGRES_URL") else {
        return;
    };
    // The restore's database names one store, so the attack gets its
    // own: created and dropped here, never the tests' shared one. The
    // name is this test's own, not only this process's: the tests run
    // in parallel, and a shared name would drop a sibling's database.
    let name = format!("scion_restore_attack_open_{}", std::process::id());
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
    let restored = archive::restore_with(&out, empty.path(), Some(&url), None, |_, _, _| {
        Err(scion_server::Error::FailedPrecondition(
            "the open itself dies before any table exists".into(),
        ))
    });
    assert!(
        restored.is_err(),
        "a restore whose open dies must not report success"
    );
    let written = std::fs::read_dir(empty.path()).unwrap().count();
    assert_eq!(
        written, 0,
        "the target must stay empty when the restore dies at the open"
    );
    let mut check = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    assert_pg_database_empty(&mut check);
    drop(check);
    maker
        .batch_execute(&format!("DROP DATABASE {name}"))
        .unwrap();
}

/// The doc's version rule (`docs/archive-format.md`): a reader refuses
/// an archive whose format or version it does not know. A version-2
/// manifest and a foreign format string each stop the restore before
/// it writes a row, with the named error.
#[test]
fn an_archive_of_an_unknown_version_or_format_is_refused() {
    let (_keep, out, _main, _second) = sound_archive();
    let manifest_path = out.join("manifest.json");
    let manifest = std::fs::read_to_string(&manifest_path).unwrap();

    let future = manifest.replace(r#""version": 1"#, r#""version": 2"#);
    assert_ne!(future, manifest, "the manifest's shape moved upstream");
    std::fs::write(&manifest_path, future).unwrap();
    let error = archive::restore(&out, tempfile::tempdir().unwrap().path(), None, None)
        .expect_err("a version-2 archive must not restore");
    assert!(
        error.to_string().contains("version 2 is not 1"),
        "the refusal must name the version rule: {error}"
    );
    let damages = archive::verify(&out).unwrap_err();
    assert!(
        damages.to_string().contains("version 2 is not 1"),
        "verify refuses before it checks rows: {damages}"
    );

    let foreign = manifest
        .replace(
            r#""format": "scion-archive""#,
            r#""format": "scion-archive-2""#,
        )
        .replace(r#""version": 1"#, r#""version": 2"#);
    assert_ne!(foreign, manifest, "the manifest's shape moved upstream");
    std::fs::write(&manifest_path, foreign).unwrap();
    let error = archive::restore(&out, tempfile::tempdir().unwrap().path(), None, None)
        .expect_err("a foreign archive must not restore");
    assert!(
        error.to_string().contains("not a scion archive"),
        "the refusal must name the format rule: {error}"
    );
}

/// F2: the mark clears once, in the LAST repo's publication
/// transaction. A kill between repos - here after the first repo's
/// publication, with no failure path - leaves partial data, and the
/// next restore must reset it cleanly: the mark is still on, so the
/// schema reads as this restore's staging and the reset drops it. Red
/// on the old code: each repo's publication cleared the mark, so the
/// kill left a schema with a store's identity and no mark, and the
/// next restore refused it as a complete store. Runs only where
/// `SCION_TEST_POSTGRES_URL` names the job's own scratch instance.
#[test]
fn a_kill_between_repos_resets_cleanly_on_the_next_restore() {
    let Ok(base) = std::env::var("SCION_TEST_POSTGRES_URL") else {
        return;
    };
    // The child half: the restore that dies after the first repo's
    // publication. The parent re-runs this binary with the seam armed.
    if std::env::var("SCION_TEST_RESTORE_EXIT_AFTER").is_ok() {
        let url = std::env::var("SCION_TEST_RESTORE_URL").unwrap();
        let archive = std::env::var("SCION_TEST_RESTORE_ARCHIVE").unwrap();
        let data = std::env::var("SCION_TEST_RESTORE_DATA").unwrap();
        let _ = archive::restore(
            std::path::Path::new(&archive),
            std::path::Path::new(&data),
            Some(&url),
            None,
        );
        return;
    }
    let name = format!("scion_restore_attack_kill_{}", std::process::id());
    let mut maker = postgres::Client::connect(&base, postgres::NoTls).unwrap();
    maker
        .batch_execute(&format!("DROP DATABASE IF EXISTS {name}"))
        .unwrap();
    maker
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .unwrap();
    let root = base
        .rsplit_once('/')
        .map_or(base.as_str(), |(rest, _)| rest);
    let url = format!("{root}/{name}");
    let (_keep, out, _main, _second) = sound_archive();
    let parent = std::env::current_exe().unwrap();
    let data = tempfile::tempdir().unwrap();
    let target = data.path().join("restored");
    std::fs::create_dir_all(&target).unwrap();
    // The child: the same test binary, the restore armed to exit after
    // the first repo's publication.
    let status = Command::new(parent)
        .args([
            "--exact",
            "a_kill_between_repos_resets_cleanly_on_the_next_restore",
            "--nocapture",
        ])
        .env("SCION_TEST_RESTORE_EXIT_AFTER", REPO)
        .env("SCION_TEST_RESTORE_URL", &url)
        .env("SCION_TEST_RESTORE_ARCHIVE", &out)
        .env("SCION_TEST_RESTORE_DATA", &target)
        .env("SCION_TEST_POSTGRES_URL", &base)
        .env("RUST_LOG", "error")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(9), "the child must die at the seam");
    // The killed restore left partial data: the first repo's rows sit
    // in the schema, the second's do not, and the mark is (or is not)
    // whatever the code under test left there.
    let mut check = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    let first: i64 = check
        .query_one("SELECT count(*) FROM repos WHERE name = $1", &[&REPO])
        .unwrap()
        .get(0);
    let second: i64 = check
        .query_one("SELECT count(*) FROM repos WHERE name = $1", &[&SECOND])
        .unwrap()
        .get(0);
    drop(check);
    assert_eq!(first, 1, "the kill landed after the first repo");
    assert_eq!(second, 0, "the kill landed before the second repo");
    // The next restore at the same URL resets the leftover cleanly and
    // lands the whole archive.
    let target_b = data.path().join("restored-again");
    std::fs::create_dir_all(&target_b).unwrap();
    archive::restore(&out, &target_b, Some(&url), None)
        .unwrap_or_else(|e| panic!("the next restore must reset the leftover: {e:#}"));
    let mut check = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    let repos: i64 = check
        .query_one("SELECT count(*) FROM repos", &[])
        .unwrap()
        .get(0);
    drop(check);
    assert_eq!(repos, 2, "the clean restore landed both repos");
    maker
        .batch_execute(&format!("DROP DATABASE {name}"))
        .unwrap();
}

/// R2: a zero-repo archive leaves the staging mark OFF. The replay
/// loop never runs, so with no clear of its own the mark rides into
/// the published store - and after clients write repos, a restore at
/// that URL reads the mark as a leftover and drops a real store. Red
/// on the code without the zero-repo clear. Runs only where
/// `SCION_TEST_POSTGRES_URL` names the job's own scratch instance.
#[test]
fn a_zero_repo_archive_leaves_no_staging_mark() {
    let Ok(base) = std::env::var("SCION_TEST_POSTGRES_URL") else {
        return;
    };
    // The store that makes the archive lives in one scratch database;
    // the restore lands in a second, so the target starts fresh.
    let a = format!("scion_restore_attack_empty_a_{}", std::process::id());
    let b = format!("scion_restore_attack_empty_b_{}", std::process::id());
    let mut maker = postgres::Client::connect(&base, postgres::NoTls).unwrap();
    for name in [&a, &b] {
        maker
            .batch_execute(&format!("DROP DATABASE IF EXISTS {name}"))
            .unwrap();
        maker
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .unwrap();
    }
    let root = base
        .rsplit_once('/')
        .map_or(base.as_str(), |(rest, _)| rest);
    let url_a = format!("{root}/{a}");
    let url_b = format!("{root}/{b}");
    // The archive of an empty store: the manifest carries "repos": [].
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open(data.path(), Some(&url_a), None).unwrap();
    drop(store);
    let (_keep, out) = {
        let parent = tempfile::tempdir().unwrap();
        let out = parent.path().join("archive");
        let store = Backend::open(data.path(), Some(&url_a), None).unwrap();
        archive::export(&store, data.path(), None, true, &out).unwrap();
        drop(store);
        (parent, out)
    };
    let manifest = std::fs::read_to_string(out.join("manifest.json")).unwrap();
    assert!(
        manifest.contains(r#""repos": []"#),
        "the fixture is an empty archive: {manifest}"
    );
    // The restore lands an empty store in the fresh database.
    let target = tempfile::tempdir().unwrap().path().join("restored-empty");
    std::fs::create_dir_all(&target).unwrap();
    archive::restore(&out, &target, Some(&url_b), None)
        .unwrap_or_else(|e| panic!("the empty restore failed: {e:#}"));
    // The published store must not carry the mark: a store with no
    // repos is still a store, and clients will write repos into it.
    let mut check = postgres::Client::connect(&url_b, postgres::NoTls).unwrap();
    let mark: Option<String> = check
        .query_one("SELECT to_regclass('restore_staging')::text", &[])
        .unwrap()
        .get(0);
    drop(check);
    assert!(
        mark.is_none(),
        "a published empty store carries the staging mark: {mark:?}"
    );
    // The end-to-end form: a client writes one repo into the restored
    // store, then a second restore at that URL must refuse - and with
    // the mark gone it does, for the right reason: the rows, not a
    // leftover mark.
    let grown = Backend::open(&target, Some(&url_b), None).unwrap();
    grown.write(|c| c.create_repo("grown").map(|_| ())).unwrap();
    drop(grown);
    // A fresh target: the refusal must come from the rows at the URL,
    // not from the target directory already holding a store.
    let target_b = tempfile::tempdir().unwrap().path().join("restored-empty-b");
    std::fs::create_dir_all(&target_b).unwrap();
    let second = archive::restore(&out, &target_b, Some(&url_b), None);
    assert!(
        second.is_err(),
        "a second restore at a stopped store's URL must refuse"
    );
    let why = format!("{:#}", second.unwrap_err());
    assert!(
        why.contains("refuses to drop"),
        "the refusal must protect the rows, not a leftover mark: {why}"
    );
    for name in [&a, &b] {
        maker
            .batch_execute(&format!("DROP DATABASE {name}"))
            .unwrap();
    }
}

/// V2: the failure path quotes the schema name too. A replay that dies
/// halfway leaves its partial schema behind, and the restore resets it
/// with DROP/CREATE - the last place the schema's name rode raw into
/// SQL text. A schema whose name holds a double quote and a space,
/// reached by a `search_path` URL, is dropped and recreated as itself;
/// a second schema named like the quote's continuation stays. Red on
/// fcd48a32: the raw interpolation is a syntax error, the reset warns
/// and gives up, and the failed replay's schema stays behind with its
/// rows. Runs only where `SCION_TEST_POSTGRES_URL` names the job's own
/// scratch instance.
#[test]
fn a_failed_restore_resets_a_quoted_schema_as_itself() {
    let Ok(base) = std::env::var("SCION_TEST_POSTGRES_URL") else {
        return;
    };
    // The store's schema holds a double quote and a space; `b c` is
    // the name the raw interpolation's quote-continuation would reach.
    let name = format!("scion_restore_attack_quotey_{}", std::process::id());
    let mut maker = postgres::Client::connect(&base, postgres::NoTls).unwrap();
    maker
        .batch_execute(&format!("DROP DATABASE IF EXISTS {name}"))
        .unwrap();
    maker
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .unwrap();
    let plain = format!(
        "{}/{}",
        base.rsplit_once('/')
            .map_or(base.as_str(), |(rest, _)| rest),
        name
    );
    postgres::Client::connect(&plain, postgres::NoTls)
        .unwrap()
        .batch_execute("CREATE SCHEMA \"a\"\"b c\"; CREATE SCHEMA \"b c\"")
        .unwrap();
    // The search_path names the quoted schema: the quote doubled, the
    // space escaped, public after it.
    let url = format!("{plain}?options=--search_path%3D%22a%22%22b%5C%20c%22%2Cpublic");
    let (_keep, out, _main, _second) = sound_archive();
    // The lock the replay dies on: down as soon as the open's tables
    // appear in the quoted schema, held past the writer's clock.
    let holder_url = plain.clone();
    let (locked, wait_locked) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let mut db = postgres::Client::connect(&holder_url, postgres::NoTls).unwrap();
        let started = std::time::Instant::now();
        loop {
            let there: Option<String> = db
                .query_one("SELECT to_regclass('\"a\"\"b c\".changesets')::text", &[])
                .unwrap()
                .get(0);
            if there.is_some() {
                db.batch_execute("BEGIN; LOCK TABLE \"a\"\"b c\".changesets IN EXCLUSIVE MODE;")
                    .unwrap();
                let _ = locked.send(());
                std::thread::sleep(std::time::Duration::from_secs(30));
                let _ = db.batch_execute("COMMIT");
                return;
            }
            assert!(
                started.elapsed() < std::time::Duration::from_secs(30),
                "the test database never gained its changesets table within 30 s"
            );
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    });
    let empty = tempfile::tempdir().unwrap();
    let restored = archive::restore_with(&out, empty.path(), Some(&url), None, |path, url, ca| {
        let store = scion_server::Backend::open(path, url, ca)?;
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
    // The failure path reset the quoted schema: dropped with every row
    // the replay committed, recreated empty. Red on the raw
    // interpolation: the DROP is a syntax error, the schema keeps its
    // tables.
    let mut check = postgres::Client::connect(&plain, postgres::NoTls).unwrap();
    let left: i64 = check
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class \
             WHERE relnamespace = '\"a\"\"b c\"'::regnamespace AND relkind = 'r'",
            &[],
        )
        .unwrap()
        .get(0);
    let twin: i64 = check
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 'b c'",
            &[],
        )
        .unwrap()
        .get(0);
    drop(check);
    assert_eq!(
        left, 0,
        "the failed restore must reset the quoted schema; {left} table(s) stayed"
    );
    assert_eq!(
        twin, 1,
        "the schema the quote's continuation names must stay"
    );
    maker
        .batch_execute(&format!("DROP DATABASE {name}"))
        .unwrap();
}
