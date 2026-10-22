//! Postgres crash rows: the real `scion-server` binary over a Postgres
//! database, killed with SIGKILL while a land runs, and killed while the
//! schema upgrades. What a kill can leave is a whole step: the ref at its
//! old or new target with the metadata whole, and the schema at one
//! version. The next start serves.
//!
//! Postgres only: every test skips itself when `SCION_TEST_BACKEND` does
//! not name `postgres`; the database URL comes from
//! `SCION_TEST_POSTGRES_URL`. The clients here build their own timeouts
//! (a 5 s connect, a 30 s call) until a shared builder lands.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use scion_core::{Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::auth::Bearer;
use scion_proto::v1::{
    LandRequest, LandResponse, LandState, ResolveRefRequest, objects_client::ObjectsClient,
    scion_client::ScionClient,
};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::write;
use scion_server::{Backend, Error};
use tonic::Status;
use tonic::transport::{Channel, Endpoint};

const REPO: &str = "r";
const TOKEN: &str = "crash-pg-token";

/// The width of a land the kill must straddle: wide enough that its
/// planning and settling take many steps of the machine.
const FILES: usize = 300;

/// How many kill reps the land test runs at most. It stops early once
/// the kills have produced both endings, a ref that moved and one that
/// did not.
const REPS: usize = 12;

/// The connect timeout of every client here: a server that died mid-open
/// is unavailable, and a call that hangs on it would eat the budget.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The deadline of every call here: the kill cuts the call, and a server
/// that stays silent past this is a finding, not a wait.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

fn pg_url() -> String {
    std::env::var("SCION_TEST_POSTGRES_URL").expect("SCION_TEST_POSTGRES_URL")
}

/// One per spawn: the `application_name` every connection of that one
/// server carries, known before the process starts (the pid is not:
/// the URL rides the spawn's environment). The kill waits for these
/// sessions to end, so no session of a dead server can appear - and
/// take a lock - after the wait said the database is clear.
static SPAWN_SEQ: AtomicU64 = AtomicU64::new(1);

fn is_pg() -> bool {
    std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres")
}

/// The server's URL on the test's own schema: the same name
/// `Backend::test_schema` derives for this data directory, in the URL's
/// `options`, so the server's one-server lock and its tables sit beside
/// the harness's.
fn server_url(schema: &str, name: &str) -> String {
    format!(
        "{}?options=--search_path%3D{schema}%20-c%20application_name%3D{name}",
        pg_url().trim_end_matches('/')
    )
}

/// The database, reached directly: the test drops the schema of a used
/// rep and reads the schema version a kill left.
fn raw(url: &str) -> postgres::Client {
    postgres::Client::connect(url, postgres::NoTls).expect("the test database is up")
}

fn drop_schema(schema: &str) {
    raw(&pg_url())
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("the test schema drops");
}

fn create_schema(schema: &str) {
    raw(&pg_url())
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .expect("the test schema is created");
}

/// A real server of this database opens on one database-wide
/// one-server key, so two tests of this binary that run real servers
/// at the same time refuse each other. One mutex, held by a test for
/// its whole run, serializes them. Poisoning says only that a test
/// died; the next one still runs.
static ONE_SERVER: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn one_server() -> std::sync::MutexGuard<'static, ()> {
    ONE_SERVER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Names whoever holds a server's one-server key before the test
/// panics on the refusal: every advisory row on the key, joined to its
/// session, and every session named the schema, with the fields that
/// tell the holder. A server of this database opens on the one key
/// `SERVER_LOCK` (the server opens without a schema), so the dump
/// reads that key's two halves, and the schema's sessions beside it.
fn dump_the_holders(schema: &str) {
    const SERVER_LOCK: i64 = 0x5343_494F_4E53_5256;
    let mut client = raw(&pg_url());
    let rows = client.query(
        "SELECT a.pid, coalesce(a.application_name, ''), \
         coalesce(a.client_port::text, ''), coalesce(a.backend_start::text, ''), \
         coalesce(a.state, ''), coalesce(a.wait_event_type, ''), \
         coalesce(a.wait_event, ''), coalesce(left(a.query, 80), '') \
         FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.locktype = 'advisory' \
           AND l.classid = (($1::bigint >> 32) & 4294967295)::bigint \
           AND l.objid = ($1::bigint & 4294967295)::bigint \
         UNION ALL \
         SELECT a.pid, coalesce(a.application_name, ''), \
         coalesce(a.client_port::text, ''), coalesce(a.backend_start::text, ''), \
         coalesce(a.state, ''), coalesce(a.wait_event_type, ''), \
         coalesce(a.wait_event, ''), coalesce(left(a.query, 80), '') \
         FROM pg_stat_activity a WHERE a.application_name = $1::text \
            OR a.application_name = $2",
        &[&SERVER_LOCK, &schema],
    );
    match rows {
        Ok(rows) => {
            eprintln!("--- the one-server key's holders and the schema's sessions ---");
            for row in rows {
                eprintln!(
                    "pid={} name={} port={} start={} state={} wait={}/{} q={}",
                    row.get::<_, i64>(0),
                    row.get::<_, String>(1),
                    row.get::<_, String>(2),
                    row.get::<_, String>(3),
                    row.get::<_, String>(4),
                    row.get::<_, String>(5),
                    row.get::<_, String>(6),
                    row.get::<_, String>(7),
                );
            }
        }
        Err(e) => eprintln!("the holder dump failed: {e}"),
    }
}

/// The `max(schema_migrations.version)` a kill left, or 0.
fn schema_version(schema: &str) -> i64 {
    let mut client = raw(&pg_url());
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .expect("the schema exists");
    client
        .query("SELECT max(version) FROM schema_migrations", &[])
        .ok()
        .and_then(|rows| rows.first().map(|row| row.get::<_, Option<i64>>(0)))
        .flatten()
        .unwrap_or(0)
}

/// The address of the newest `serving` line in the log, as common's does.
fn bound_addr(log: &Path) -> Option<String> {
    let text = fs::read_to_string(log).ok()?;
    let line = text.lines().rev().find(|line| line.contains("serving"))?;
    let listen = line.split("listen=").nth(1)?;
    Some(listen.trim().to_owned())
}

/// One `scion-server` process over the database. `kill` is SIGKILL: the
/// process dies with its sockets open, which is the crash a FIN-less cut
/// leaves.
struct PgServer {
    child: Child,
    url: String,
    log: PathBuf,
    schema: String,
    name: String,
}

impl PgServer {
    /// Spawns the process and returns at once, before the start's own
    /// work - the schema upgrade - has run. The caller that kills
    /// during that work measures from here, not from the serving line.
    fn spawn_raw(data: &Path, schema: &str, log: &Path) -> Self {
        let name = format!(
            "scion-test-{}",
            SPAWN_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        // One start, one log: a fresh file, so the address this spawn
        // waits for is this spawn's, never an earlier run's.
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(log)
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
            .arg("--data")
            .arg(data)
            .args(["serve", "--token", TOKEN, "--listen", "127.0.0.1:0"])
            .env("SCION_POSTGRES_URL", server_url(schema, &name))
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file.try_clone().unwrap()))
            .spawn()
            .unwrap();
        drop(file);
        Self {
            child,
            url: String::new(),
            log: log.to_path_buf(),
            schema: schema.to_owned(),
            name: name.clone(),
        }
    }

    /// Spawns, and returns once the log says `serving`: the start's own
    /// work, the schema upgrade included, has run by then.
    fn spawn(data: &Path, schema: &str, log: &Path) -> Self {
        let mut server = Self::spawn_raw(data, schema, log);
        let start = Instant::now();
        loop {
            if let Some(status) = server.child.try_wait().unwrap() {
                dump_the_holders(schema);
                let log = fs::read_to_string(&server.log).unwrap_or_default();
                panic!("the server exited with {status}:\n{log}");
            }
            if let Some(addr) = bound_addr(&server.log) {
                server.url = format!("http://{addr}");
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "the server did not listen"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        server
    }

    /// Kills the process and waits for the death, then for Postgres to
    /// end every session the dead server opened. The backend that held
    /// the one-server lock died with the process, but Postgres gives
    /// the lock up only while cleaning that session away, which a
    /// loaded runner can stretch past any fixed sleep - and a session
    /// that was mid-start at the kill can take the lock AFTER any
    /// lock-only check said the database was clear. The wait therefore
    /// watches this one server's sessions, and no lock. A session's
    /// name is one of two: the URL's (`scion-test-N`) until its first
    /// statement, and the schema's after (the store's setup renames
    /// every session it opens), so both names hold the wait. The next
    /// start goes out only when no session of either name remains, or
    /// the test names the wait that never ended. (A lock of the
    /// schema's own key that the test's harness holds between rounds
    /// is not this server's, and must not hold the wait.)
    fn kill_and_wait(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait(); // needs &mut: the kill rides it
        let mut client = raw(&pg_url());
        let start = Instant::now();
        loop {
            let sessions = client
                .query_opt(
                    "SELECT 1 FROM pg_stat_activity \
                     WHERE (application_name = $1 OR application_name = $2) \
                       AND pid <> pg_backend_pid()",
                    &[&self.name, &self.schema],
                )
                .expect("the session query answers")
                .is_some();
            if !sessions {
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "the dead server's sessions never ended"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for PgServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // A server that ends here is one a loop round is done with: its
        // sessions must be gone before the next round's spawn takes the
        // one-server lock, or that spawn races a dying session. The
        // poll is the kill's own, with its ceiling; a drop that runs
        // during unwinding must not panic, so a wait that never ends
        // only names itself in the log.
        let mut client = raw(&pg_url());
        let start = Instant::now();
        loop {
            let sessions = client
                .query_opt(
                    "SELECT 1 FROM pg_stat_activity WHERE application_name = $1",
                    &[&self.name],
                )
                .expect("the session query answers")
                .is_some();
            if !sessions {
                return;
            }
            if start.elapsed() >= Duration::from_secs(10) {
                eprintln!("crash_pg: the dropped server's sessions were still live after 10 s");
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// The channel this file builds per call: its background tasks live on
/// the runtime that built it, so every helper connects and calls inside
/// ONE runtime, with this file's timeouts (5 s to connect, 30 s per
/// call) until a shared builder lands.
async fn channel(url: &str) -> Channel {
    Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(CALL_TIMEOUT)
        .connect()
        .await
        .expect("the client connects")
}

/// `Scion.Land` as one call on a fresh connection.
fn land(url: &str, changeset: &[u8], key: &str) -> Result<LandResponse, Status> {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let channel = channel(url).await;
        let bearer = Bearer::new(TOKEN).unwrap();
        let mut scion = ScionClient::with_interceptor(channel, bearer);
        scion
            .land(LandRequest {
                repo: REPO.into(),
                changeset_id: changeset.to_vec(),
                target_ref: "main".into(),
                request_key: key.into(),
            })
            .await
            .map(tonic::Response::into_inner)
    })
}

/// `Objects.ResolveRef` as one call on a fresh connection.
fn resolve_ref(url: &str, name: &str) -> Vec<u8> {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let channel = channel(url).await;
        let bearer = Bearer::new(TOKEN).unwrap();
        let mut objects = ObjectsClient::with_interceptor(channel, bearer);
        objects
            .resolve_ref(ResolveRefRequest {
                repo: REPO.into(),
                name: name.into(),
            })
            .await
            .expect("the ref resolves")
            .into_inner()
            .changeset_id
    })
}

/// Opens the test's own store. After a SIGKILL, Postgres ends the dead
/// server's session a moment later, and the schema's one-server lock
/// dies with that session: an open that comes too soon after the kill
/// is refused, and waits here for the lock, up to 10 s.
fn open_store(data: &Path) -> Backend {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Backend::open_test(data) {
            Ok(store) => return store,
            Err(err)
                if matches!(&err, Error::FailedPrecondition(text)
                    if text.contains("another server holds this store"))
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(err) => panic!("the test backend opens: {err}"),
        }
    }
}

fn author() -> IdentityTime {
    IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

/// Repo `r` with a root changeset and `main` at it, through the same
/// seam the server's metadata uses; the server that opens this database
/// later serves it.
fn seed_repo(data: &Path) -> ChangesetId {
    let store = open_store(data);
    let readme = store.blobs().put(b"crash pg suite\n").unwrap();
    let root = Changeset::new(
        vec![],
        author(),
        None,
        b"root\n".to_vec(),
        [(
            RepoPath::new("README").unwrap(),
            Some(FileState::Regular {
                content: readme,
                executable: false,
            }),
        )]
        .into_iter()
        .collect(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let (id, _) = scion_server::test_support::tree::store_changeset(
                c,
                repo,
                &root,
                &mut scion_server::test_support::tree::lens_of(&store),
            )?;
            assert!(c.cas_ref(repo, "main", None, id)?);
            Ok(id)
        })
        .unwrap()
}

/// A wide changeset of `FILES` files on `main`'s head: its blobs in the
/// data directory in one batch, its metadata through the seam. The land
/// of it is the work the kill must straddle.
fn seed_wide_changeset(data: &Path, name: &str) -> ChangesetId {
    let store = open_store(data);
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let mut puts = Vec::new();
    for i in 0..FILES {
        let raw = format!("{name} file {i}\n{}", "x".repeat(512));
        // The blob goes through the write path's own upload, so the
        // repo's blob rows exist, like a client's PutBlob leaves them.
        let upload = store.blobs().upload().unwrap();
        let upload = upload.append(raw.as_bytes()).unwrap();
        let content = scion_server::test_support::write::put_blob(&store, repo, upload)
            .expect("the blob stores");
        puts.push((
            RepoPath::new(format!("d/{name}/f{i}.txt").as_str()).unwrap(),
            Some(FileState::Regular {
                content,
                executable: false,
            }),
        ));
    }
    let base = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?.expect("main exists").target)
        })
        .unwrap();
    let cs = Changeset::new(
        vec![base],
        author(),
        None,
        format!("{name}\n").into_bytes(),
        puts.into_iter().collect::<BTreeMap<_, _>>(),
    )
    .expect("changeset fits the format");
    write::create_changeset(&store, REPO, &cs.encode()).unwrap()
}

/// The ref's target as the metadata holds it, through the seam.
fn ref_target(data: &Path) -> ChangesetId {
    let store = open_store(data);
    store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?.expect("main exists").target)
        })
        .unwrap()
}

/// The first parent of a changeset, through the seam.
fn first_parent(data: &Path, id: ChangesetId) -> ChangesetId {
    let store = open_store(data);
    store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            let cs = c.get_changeset(repo, id)?;
            Ok(cs.parents()[0])
        })
        .unwrap()
}

/// Whether a changeset is in the database, through the seam.
fn has_changeset(data: &Path, id: ChangesetId) -> bool {
    let store = open_store(data);
    store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_changeset(repo, id).is_ok())
        })
        .unwrap()
}

/// The warm-up land, sent whole: its time sets the step of the kills, so
/// their placement does not depend on the speed of the machine.
fn warm_land(server: &PgServer, changeset: ChangesetId, key: &str) -> Duration {
    let started = Instant::now();
    let reply = land(&server.url, changeset.as_bytes(), key);
    assert_eq!(
        reply.expect("the warm land lands").state(),
        LandState::Landed
    );
    started.elapsed()
}

/// A kill while a wide land runs leaves the ref at its old or its new
/// target, the metadata whole, and a replay of the same request key
/// agreeing with what the ref says.
#[test]
fn a_kill_during_a_land_leaves_the_ref_old_or_new() {
    if !is_pg() {
        println!("skipping: Postgres only");
        return;
    }
    let _one = one_server();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let log = dir.path().join("server.log");
    fs::create_dir_all(&data).unwrap();
    // The schema is the hash of the data directory's canonical path: the
    // directory exists first, so the name the test derives and the name
    // the harness derives are the same one.
    let schema = Backend::test_schema(&data).expect("the test names a schema");
    drop_schema(&schema);
    create_schema(&schema);
    seed_repo(&data);
    // The warm land, whole: its time sets the step of the kills.
    // (The server's log: the tempdir keeps it; a failure prints it.)
    let warm = seed_wide_changeset(&data, "warm");
    let mut server = PgServer::spawn(&data, &schema, &log);
    warm_land(&server, warm, "warm");
    server.kill_and_wait();

    // One wide changeset per rep, seeded before the loop, each on the
    // head its rep will find: the kill of rep k lands inside rep k's own
    // land, and the next rep's changeset rebases onto whatever main is
    // by then. A probe changeset beside each cut gives the rep its
    // measure: the probe's land on the rep's own server and head is the
    // twin of the land the rep kills, so the kill step is measured from
    // what the kill interrupts and not from the first land's time.
    let cuts: Vec<_> = (0..REPS)
        .map(|k| {
            (
                format!("cut{k}"),
                seed_wide_changeset(&data, &format!("cut{k}")),
                format!("probe{k}"),
                seed_wide_changeset(&data, &format!("probe{k}")),
            )
        })
        .collect();

    // Kill mid-land, rep after rep, each with its own changeset and
    // request key, until the kills have produced both endings. Every rep
    // asserts the whole invariant against the seam.
    let mut saw_old = false;
    let mut saw_new = false;
    let mut reps = 0usize;
    for (k, (name, cs, probe_name, probe_cs)) in cuts.iter().enumerate() {
        let mut server = PgServer::spawn(&data, &schema, &log);
        // The measure is the kill's twin: the same server, the same
        // head depth, a land of the same width. The step comes from it,
        // so a batching change of the product moves the kills with it.
        let probe_started = Instant::now();
        land(&server.url, probe_cs.as_bytes(), probe_name).expect("the probe lands");
        let step = probe_started.elapsed() / 8;
        // The head after the probe, read through the server: the test's
        // own open would take the schema's one-server lock beside a
        // running server, and a second writer on one store is refused.
        let head = ChangesetId::try_from(resolve_ref(&server.url, "main").as_slice())
            .expect("the ref names a changeset");
        let call_name = name.clone();
        let call_cs = *cs;
        let url = server.url.clone();
        let handle = std::thread::spawn(move || land(&url, call_cs.as_bytes(), &call_name));
        // The move sits at the very end of a land, so the factors run
        // past the whole measured land (up to 11/8 of it): a kill after
        // the reply still shows the new ending.
        let factor = k % 11 + 1;
        std::thread::sleep(step * u32::try_from(factor).unwrap());
        server.kill_and_wait();
        let _ = handle.join();

        // The ref is at the head it had (the kill came before the
        // move) or at a changeset the move put there. A landed land
        // rebases onto the head it found, so the ref's target names a
        // changeset of its own, whose first parent is the head the
        // land rebased onto.
        let target = ref_target(&data);
        let moved = target != head;
        reps += 1;
        if moved {
            assert!(
                has_changeset(&data, target),
                "rep {k}: the ref names a changeset the database does not hold"
            );
            assert_eq!(
                first_parent(&data, target),
                head,
                "rep {k}: the landed changeset is not on the head"
            );
        }

        // One restarted server answers the replay of the same key. A
        // ref that moved reports the first move's outcome and moves
        // nothing; a ref that did not move takes the retry and lands.
        // Either way the ref must end exactly where the reply says.
        let mut server = PgServer::spawn(&data, &schema, &log);
        let replay = land(&server.url, cs.as_bytes(), name).expect("the replay answers");
        assert_eq!(
            replay.state(),
            LandState::Landed,
            "rep {k}: the retry of the key did not land"
        );
        let landed = ChangesetId::try_from(replay.result_changeset_id.as_slice())
            .expect("the reply names a changeset");
        // The server is gone before the seam reads: its one-server lock
        // is the store's, and the test's own open must not meet it.
        server.kill_and_wait();
        let after = ref_target(&data);
        assert_eq!(after, landed, "rep {k}: the ref disagrees with the reply");
        assert!(
            has_changeset(&data, after),
            "rep {k}: the ref names a changeset the database does not hold"
        );
        if moved {
            assert_eq!(after, target, "rep {k}: the replay moved the ref");
        } else {
            assert_eq!(
                first_parent(&data, after),
                head,
                "rep {k}: the retry did not land on the head"
            );
        }
        if moved {
            saw_new = true;
        } else {
            saw_old = true;
        }
    }
    assert!(
        saw_old && saw_new,
        "the kills never straddled the land (old seen: {saw_old}, new: {saw_new}) \
         over {reps} reps, steps of an eighth of the measured land up to 11/8"
    );
}

/// A kill while the schema upgrades leaves the database at one whole
/// version, and the next start upgrades it and serves.
#[test]
fn a_kill_during_a_schema_upgrade_leaves_one_whole_version() {
    if !is_pg() {
        println!("skipping: Postgres only");
        return;
    }
    let _one = one_server();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let log = dir.path().join("server.log");
    fs::create_dir_all(&data).unwrap();
    // The schema is the hash of the data directory's canonical path: the
    // directory exists first, so the name the test derives and the name
    // the harness derives are the same one.
    let schema = Backend::test_schema(&data).expect("the test names a schema");
    drop_schema(&schema);
    create_schema(&schema);

    // A whole start from empty, measured from the PROCESS to the serving
    // line: its eighth sets the step of the kills, so their placement
    // does not depend on the speed of the machine.
    // eighth sets the step of the kills, so their placement does not
    // depend on the speed of the machine.
    let started = Instant::now();
    let mut first = PgServer::spawn(&data, &schema, &log);
    let step = started.elapsed() / 8;
    let last = schema_version(&schema);
    first.kill_and_wait();
    assert!(last >= 1, "the first start left no schema");

    // Kill 1, 2, 3 steps into the next start's own work, each from a
    // fresh empty schema. The spawn does not wait: the sleep starts
    // when the process starts, so the kill can fall inside the upgrade.
    // A kill whose log already says `serving` came after the upgrade
    // committed; that rep ends the loop, the way the last migration
    // already stands.
    let mut during = 0usize;
    let mut total = 0usize;
    for k in 1..64 {
        drop_schema(&schema);
        create_schema(&schema);
        let mut cut = PgServer::spawn_raw(&data, &schema, &log);
        std::thread::sleep(step * u32::try_from(k).unwrap());
        let served = bound_addr(&log).is_some();
        cut.kill_and_wait();
        total += 1;
        if !served {
            during += 1;
        }
        let version = schema_version(&schema);
        assert!(
            (0..=last).contains(&version),
            "a kill during the upgrade left version {version}"
        );
        // The next start finishes the upgrade and serves: a mixed schema
        // would fail this open, not silently serve. The repo is seeded
        // while no server runs - the test's own open takes the schema's
        // one-server lock, and a second writer on one store is refused -
        // and the server that serves it starts after the seeding.
        let mut verify = PgServer::spawn(&data, &schema, &log);
        assert_eq!(
            schema_version(&schema),
            last,
            "the restart did not finish the upgrade"
        );
        verify.kill_and_wait();
        let root = seed_repo(&data);
        let mut verify = PgServer::spawn(&data, &schema, &log);
        let head_out = resolve_ref(&verify.url, "main");
        assert_eq!(
            head_out,
            root.as_bytes().to_vec(),
            "the ref the restart serves"
        );
        // The round is done: end this server through the wait, not
        // Drop, so the next round's spawn cannot race its sessions.
        verify.kill_and_wait();
        if served {
            break;
        }
    }
    assert!(
        during > 0,
        "no kill came during the upgrade: {during} of {total} kills \
         landed before the log said serving"
    );
}
