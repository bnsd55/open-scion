//! Common to the suites that run the real `scion-server` and check what a
//! crash must never break: the crash suite (`crash.rs`, SIGKILL) and the
//! power-loss suite (`power.rs`, dropped writes). The clients, the ledger of
//! what was sent and replied, the server process, and the checks.
// Every test binary compiles this file and uses its own subset of it.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use scion_core::{BlobId, Changeset, ChangesetId, Entry, FileState, IdentityTime, RepoPath, Tree};
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::object::{self, Assembler, Header};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{
    self, CreateChangesetRequest, DiffRequest, Encoding, FileKind, GetChangesetRequest,
    GetObjectsRequest, LandRequest, LandResponse, LandState, ObjectKind, ObjectRef, PutBlobRequest,
    ResolveRefRequest,
};
use scion_proto::{CHUNK_BYTES, MAX_OBJECT_REFS};
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::store::BlobStatus;
use scion_server::{Backend, Store, test_support::tree};
use tonic::Status;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

pub const TOKEN: &str = "secret";
pub const REPO: &str = "r";
pub const REFS: [&str; 2] = ["main", "dev"];
pub const CLIENTS: usize = 4;
/// A path that some lands put, so that some lands conflict.
pub const SHARED: &str = "shared.txt";
/// Older lands that each check retries again.
pub const OLD_RETRIES: usize = 16;

/// A seeded stream of numbers (xorshift64*), so a run can be replayed.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).unwrap()).unwrap()
    }

    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut bytes: Vec<u8> = (0..len.div_ceil(8))
            .flat_map(|_| self.next().to_le_bytes())
            .collect();
        bytes.truncate(len);
        bytes
    }
}

/// SIGKILL.
pub struct Server {
    child: Child,
    addr: SocketAddr,
    pub url: String,
}

impl Server {
    /// The server process's id, for a test that must tell the server's
    /// files from another process's.
    pub fn pid(&self) -> u32 {
        self.child.id()
    }
}

impl Server {
    /// Starts a server on `data` and waits until it listens. It appends its
    /// log to `log`.
    pub fn start(data: &Path, log: &Path) -> Self {
        let mut server = Self::spawn(data, log);
        let start = Instant::now();
        while !server.listens(log) {
            if let Some(status) = server.child.try_wait().unwrap() {
                let log = fs::read_to_string(log).unwrap_or_default();
                panic!("the server exited with {status}:\n{log}");
            }
            assert!(start.elapsed() < Duration::from_secs(60), "no listen");
            std::thread::sleep(Duration::from_millis(10));
        }
        server
    }

    /// Whether the server listens. The address comes from the newest
    /// `serving` line in the log: the server binds `127.0.0.1:0`, so the OS
    /// picks a free port and no other process can take it in between. It
    /// opens the data directory before, so it has removed the temp files and
    /// upgraded the schema.
    pub fn listens(&mut self, log: &Path) -> bool {
        let Some(addr) = bound_addr(log) else {
            return false;
        };
        self.addr = addr;
        self.url = format!("http://{addr}");
        TcpStream::connect(addr).is_ok()
    }

    /// Starts a server on `data`, and does not wait for it.
    pub fn spawn(data: &Path, log: &Path) -> Self {
        let log_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .unwrap();
        // The child shares a flock until it execs (see `spawn_guard`): the
        // read side keeps this spawn out of a lock-release window of
        // another test.
        let child = spawn_guard::across_spawn(|| {
            Command::new(env!("CARGO_BIN_EXE_scion-server"))
                .arg("--data")
                .arg(data)
                .args(["serve", "--token", TOKEN, "--listen", "127.0.0.1:0"])
                // Plain log lines, so the address parses without escape
                // codes.
                .env("NO_COLOR", "1")
                // The write park of the two-process race test: inert
                // unless a test creates a park file for a blob.
                .env("SCION_TEST_PARK_WRITES", "1")
                .env("RUST_LOG", "info")
                .stdout(log_file.try_clone().unwrap())
                .stderr(log_file)
                .spawn()
        })
        .unwrap();
        Self {
            child,
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            url: String::new(),
        }
    }
}

/// The address of the newest `serving` line in the log, if there is one.
pub fn bound_addr(log: &Path) -> Option<SocketAddr> {
    let text = fs::read_to_string(log).unwrap_or_default();
    let line = text.lines().rev().find(|line| line.contains("serving"))?;
    line.split("listen=").nth(1)?.parse().ok()
}

impl Drop for Server {
    fn drop(&mut self) {
        // `Child::kill` sends SIGKILL.
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

impl Server {
    /// Freezes the server, and waits until the kernel says it stopped:
    /// SIGSTOP takes effect on the child's own schedule, and a cut that
    /// lands before the stop finds the server still writing. Every reply
    /// a client got was sent before the freeze, so a cut that follows can
    /// lose nothing that a reply depends on ([#185]): on real hardware,
    /// nothing of the server runs after a power cut, so no ack can follow
    /// one either. The caller kills the frozen server; nothing of it runs
    /// past the cut.
    pub fn freeze(&mut self) {
        let pid = self.child.id();
        let pid_i32 = pid.cast_signed();
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(pid_i32).unwrap(),
            rustix::process::Signal::STOP,
        )
        .unwrap();
        // /proc/<pid>/stat is the main thread only: a tokio worker inside
        // a FUSE call can stay unstopped while the main thread is `T`, and
        // LazyFS would run it after the cut. Every task's stat must read
        // `T` (stopped) or `Z` (gone, so reaped or waiting to be reaped:
        // either way nothing of it can write past the cut). A vanished
        // /proc entry means the process is gone too.
        let tasks = format!("/proc/{pid}/task");
        let give_up = Instant::now() + Duration::from_secs(5);
        loop {
            let all_stopped = match fs::read_dir(&tasks) {
                Err(_) => true,
                Ok(tasks) => tasks
                    .filter_map(|task| {
                        let stat = fs::read_to_string(task.ok()?.path().join("stat")).ok()?;
                        let after = &stat[stat.rfind(')').unwrap() + 1..];
                        let state = after.split_whitespace().next().unwrap_or("");
                        Some(state == "T" || state == "Z")
                    })
                    .all(|stopped| stopped),
            };
            if all_stopped {
                return;
            }
            assert!(
                Instant::now() < give_up,
                "the server never stopped for the cut"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

pub type Scion = ScionClient<InterceptedService<Channel, Bearer>>;
pub type Objects = ObjectsClient<InterceptedService<Channel, Bearer>>;

pub async fn connect(url: &str) -> Result<(Scion, Objects), tonic::transport::Error> {
    let channel = Channel::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await?;
    let bearer = Bearer::new(TOKEN).unwrap();
    let objects = ObjectsClient::with_interceptor(channel.clone(), bearer.clone());
    Ok((ScionClient::with_interceptor(channel, bearer), objects))
}

/// `Objects.ResolveRef` as one call: the changeset ID that `name` names.
/// The `Objects` calls stay in one place, so a change to the service moves
/// one helper, not every call.
pub async fn resolve_ref(objects: &mut Objects, name: &str) -> Result<ChangesetId, Status> {
    let request = ResolveRefRequest {
        repo: REPO.into(),
        name: name.into(),
    };
    let reply = objects.resolve_ref(request).await?;
    Ok(changeset_id(&reply.into_inner().changeset_id))
}

pub fn author() -> IdentityTime {
    IdentityTime {
        name: b"Ada".to_vec(),
        email: b"ada@example.com".to_vec(),
        unix_secs: 1_700_000_000,
        tz_offset_minutes: 0,
    }
}

pub fn changeset_id(bytes: &[u8]) -> ChangesetId {
    ChangesetId::try_from(bytes).unwrap()
}

/// A land that a client sent, and its reply if one came.
pub struct Sent {
    pub key: String,
    pub target: &'static str,
    pub changeset: Changeset,
    pub id: ChangesetId,
    pub reply: Option<LandResponse>,
}

impl Sent {
    pub fn request(&self) -> LandRequest {
        LandRequest {
            repo: REPO.into(),
            changeset_id: self.id.as_bytes().to_vec(),
            target_ref: self.target.into(),
            request_key: self.key.clone(),
        }
    }
}

/// What the server replied to the clients.
#[derive(Default)]
pub struct Ledger {
    /// Blobs that `PutBlob` replied with.
    pub blobs: Vec<BlobId>,
    /// Every land sent, in order. `CreateChangeset` replied for each one.
    pub lands: Vec<Sent>,
}

/// Repo `r` with one root changeset, and refs `main` and `dev` at it. No RPC
pub fn create_repo(data: &Path) {
    let store = Store::open(data).unwrap();
    let readme = store.blobs().put(b"crash suite\n").unwrap();
    let root = Changeset {
        parents: vec![],
        author: author(),
        committer: None,
        message: b"root\n".to_vec(),
        changes: [(
            RepoPath::new("README").unwrap(),
            Some(FileState::Regular {
                content: readme,
                executable: false,
            }),
        )]
        .into(),
    };
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let (id, _) = tree::store_changeset(
                c,
                repo,
                &root,
                &mut tree::lens_of(&Backend::Sqlite(store.clone())),
            )?;
            for name in REFS {
                assert!(c.cas_ref(repo, name, None, id)?);
            }
            Ok(())
        })
        .unwrap();
}

pub async fn put_blob(scion: &mut Scion, data: &[u8]) -> Result<BlobId, Status> {
    let parts: Vec<PutBlobRequest> = data
        .chunks(CHUNK_BYTES)
        .enumerate()
        .map(|(i, part)| PutBlobRequest {
            repo: if i == 0 { REPO.into() } else { String::new() },
            data: part.to_vec(),
        })
        .collect();
    let reply = scion.put_blob(tokio_stream::iter(parts)).await?;
    Ok(BlobId::try_from(reply.into_inner().blob_id.as_slice()).unwrap())
}

/// What the clients and the test that kills the server share.
#[derive(Default)]
pub struct Signals {
    /// Set just before the kill.
    pub killed: AtomicBool,
    /// `PutBlob` calls of a large file in progress.
    pub large_uploads: AtomicUsize,
}

/// A file of more than this many bytes counts as large. Its upload writes
/// the blob near its end, 10 to 30 ms after it starts.
pub const LARGE: usize = 64 << 10;

/// One client: uploads files, creates a changeset on a ref's head (or on an
/// older base of it), and lands it, until a call fails. Only the kill may
/// fail a call, and it sets `signals.killed` first.
pub async fn client(url: String, name: String, mut rng: Rng, signals: Arc<Signals>) -> Ledger {
    let mut ledger = Ledger::default();
    let cut = |ledger, call: &str, error: &dyn std::fmt::Debug| {
        let killed = signals.killed.load(Ordering::SeqCst);
        assert!(killed, "{name}: {call} failed before the kill: {error:?}");
        ledger
    };
    let (mut scion, mut objects) = match connect(&url).await {
        Ok(clients) => clients,
        Err(error) => return cut(ledger, "connect", &error),
    };
    let mut bases = HashMap::new();
    for op in 0_u64.. {
        let target = REFS[rng.below(REFS.len())];
        // An older base makes the server rebase the land.
        let base = match bases.get(target) {
            Some(&base) if rng.below(4) == 0 => base,
            _ => match resolve_ref(&mut objects, target).await {
                Ok(id) => id,
                Err(status) => return cut(ledger, "ResolveRef", &status),
            },
        };
        bases.insert(target, base);

        // Mostly small files, some up to about 1 MiB, so a kill can stop a
        // blob write in the middle.
        let mut files = Vec::new();
        for i in 0..=rng.below(3) {
            let len = if rng.below(8) == 0 {
                LARGE + rng.below(1 << 20)
            } else {
                1 + rng.below(4096)
            };
            files.push((format!("{name}/{op}/f{i}"), rng.bytes(len)));
        }
        if rng.below(6) == 0 {
            files.push((SHARED.into(), rng.bytes(16)));
        }
        let mut changes = BTreeMap::new();
        for (path, data) in files {
            let large = usize::from(data.len() > LARGE);
            signals.large_uploads.fetch_add(large, Ordering::SeqCst);
            let put = put_blob(&mut scion, &data).await;
            signals.large_uploads.fetch_sub(large, Ordering::SeqCst);
            let id = match put {
                Ok(id) => id,
                Err(status) => return cut(ledger, "PutBlob", &status),
            };
            assert_eq!(id, BlobId::of(&data));
            ledger.blobs.push(id);
            let state = FileState::Regular {
                content: id,
                executable: false,
            };
            changes.insert(RepoPath::new(path).unwrap(), Some(state));
        }

        let key = format!("{name}-{op}");
        let changeset = Changeset {
            parents: vec![base],
            author: author(),
            committer: None,
            message: key.clone().into_bytes(),
            changes,
        };
        let (bytes, id) = changeset.encode_with_id();
        let request = CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        };
        match scion.create_changeset(request).await {
            Ok(reply) => assert_eq!(changeset_id(&reply.into_inner().changeset_id), id),
            Err(status) => return cut(ledger, "CreateChangeset", &status),
        }

        let sent = Sent {
            key,
            target,
            changeset,
            id,
            reply: None,
        };
        let request = sent.request();
        ledger.lands.push(sent);
        match scion.land(request).await {
            Ok(reply) => ledger.lands.last_mut().unwrap().reply = Some(reply.into_inner()),
            Err(status) => return cut(ledger, "Land", &status),
        }
    }
    unreachable!("the loop runs until a call fails")
}

/// What earlier checks verified. Objects never change, so each one is
/// checked once.
#[derive(Default)]
pub struct Checked {
    /// Lands `..lands` were retried after a restart.
    pub lands: usize,
    /// Blobs `..blobs` of the ledger were read back.
    pub blobs: usize,
    /// Changesets whose root tree was checked against their changes.
    pub changesets: HashSet<ChangesetId>,
    /// Changeset and tree rows whose blobs were found.
    pub rows: HashSet<Vec<u8>>,
    /// Blobs found intact on disk.
    pub files: HashSet<BlobId>,
}

/// The changes of `new` against `old`, from `Diff`.
pub async fn diff(
    scion: &mut Scion,
    old: ChangesetId,
    new: ChangesetId,
) -> BTreeMap<RepoPath, Option<FileState>> {
    let mut changes = BTreeMap::new();
    let mut after = Vec::new();
    loop {
        let request = DiffRequest {
            repo: REPO.into(),
            old_changeset_id: old.as_bytes().to_vec(),
            new_changeset_id: new.as_bytes().to_vec(),
            path: vec![],
            after,
        };
        let page = scion.diff(request).await.unwrap().into_inner();
        for entry in page.entries {
            let state = entry.new.map(
                |v1::FileState {
                     kind,
                     id,
                     executable,
                 }| {
                    assert_eq!(kind, FileKind::Regular as i32, "only regular files are put");
                    FileState::Regular {
                        content: BlobId::try_from(id.as_slice()).unwrap(),
                        executable,
                    }
                },
            );
            changes.insert(RepoPath::new(entry.path).unwrap(), state);
        }
        if page.next_after.is_empty() {
            return changes;
        }
        after = page.next_after;
    }
}

/// Reads `blobs` with `GetObjects`, asking again for the rest after a
/// reply that stops early, and checks each against its ID.
// SHORTCUT: dead until C5 verifies tickets and the read-back returns.
#[allow(dead_code)]
pub async fn read_blobs(objects: &mut Objects, blobs: &[BlobId]) {
    let mut rest = blobs;
    while !rest.is_empty() {
        let batch = &rest[..rest.len().min(MAX_OBJECT_REFS)];
        let refs = batch
            .iter()
            .map(|id| ObjectRef {
                kind: ObjectKind::Blob.into(),
                id: id.as_bytes().to_vec(),
                path: Vec::new(),
            })
            .collect();
        let request = GetObjectsRequest {
            refs,
            accept: vec![Encoding::Zstd.into()],
            ticket: Vec::new(),
        };
        let mut reply = objects.get_objects(request).await.unwrap().into_inner();
        let mut got = 0;
        loop {
            let fail = |status| panic!("blob {:?}: {status:?}", batch.get(got));
            let Some(first) = reply.message().await.unwrap_or_else(fail) else {
                break;
            };
            let mut object = Assembler::new(Header::of(&first, true).unwrap(), first);
            while !object.is_done() {
                let part = reply.message().await.unwrap_or_else(fail);
                object.push(&part.expect("an object cut short")).unwrap();
            }
            let (header, data) = object.finish();
            let id = batch[got];
            assert_eq!(header.id, *id.as_bytes(), "blob {id}");
            let data = object::decode(&header, data).unwrap();
            assert_eq!(BlobId::of(&data), id, "blob {id}");
            got += 1;
        }
        assert!(got > 0, "a reply with no object");
        rest = &rest[got..];
    }
}

pub fn pending_lands(c: &rusqlite::Connection) -> i64 {
    let pending = "SELECT count(*) FROM lands WHERE state = 'pending'";
    c.query_row(pending, [], |row| row.get(0)).unwrap()
}

/// The blobs that the changeset and tree rows of the database name, and the
/// number of pending lands. Rows in `seen` are skipped, and new rows added:
/// a row never changes.
pub fn metadata(store: &Backend, seen: &mut HashSet<Vec<u8>>) -> (BTreeSet<BlobId>, i64) {
    // The rows of the data directory's own database: this helper asserts the
    // storage's internals, which only the SQLite backend holds.
    let raw = match store {
        Backend::Sqlite(raw) => raw,
        Backend::Postgres(_) => unreachable!(),
    };
    raw.read(|c| {
        let mut named = BTreeSet::new();
        for table in ["changesets", "trees"] {
            let mut rows = c.prepare(&format!("SELECT id, body FROM {table}"))?;
            let rows = rows.query_map([], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?;
            for row in rows {
                let (id, body) = row?;
                if !seen.insert(id) {
                    continue;
                }
                let states: Vec<FileState> = if table == "changesets" {
                    let changes = Changeset::decode(&body).unwrap().changes;
                    changes.into_values().flatten().collect()
                } else {
                    let entries = Tree::decode(&body).unwrap().entries.into_values();
                    let file = |entry| match entry {
                        Entry::File(file) => Some(file.state()),
                        Entry::Dir(_) => None,
                    };
                    entries.filter_map(file).collect()
                };
                named.extend(states.iter().filter_map(FileState::blob));
            }
        }
        Ok((named, pending_lands(c)))
    })
    .unwrap()
}

/// Every blob file under `<data>/objects/blob`, by its ID.
pub fn blob_files(data: &Path) -> Vec<BlobId> {
    let mut ids = Vec::new();
    for dir in fs::read_dir(data.join("objects/blob")).unwrap() {
        let dir = dir.unwrap();
        for file in fs::read_dir(dir.path()).unwrap() {
            let name = dir.file_name().into_string().unwrap()
                + &file.unwrap().file_name().into_string().unwrap();
            ids.push(name.parse().unwrap());
        }
    }
    ids
}

/// Retries each land sent since the last check, and a few older ones, with
/// its key. A land whose reply was lost is retried twice at once: calls with
/// the same key wait for each other and return the same land. All calls run
pub async fn retry_lands(scion: &Scion, ledger: &mut Ledger, checked: &mut Checked, rng: &mut Rng) {
    let new = checked.lands..ledger.lands.len();
    let lost = new.clone().filter(|&i| ledger.lands[i].reply.is_none());
    let mut retry: Vec<usize> = lost.chain(new).collect();
    if checked.lands > 0 {
        retry.extend((0..OLD_RETRIES).map(|_| rng.below(checked.lands)));
    }
    checked.lands = ledger.lands.len();
    let mut calls = tokio::task::JoinSet::new();
    for i in retry {
        let (mut scion, request) = (scion.clone(), ledger.lands[i].request());
        calls.spawn(async move { (i, scion.land(request).await) });
    }
    while let Some(done) = calls.join_next().await {
        let (i, reply) = done.unwrap();
        let sent = &mut ledger.lands[i];
        let reply = reply
            .unwrap_or_else(|s| panic!("retry of {}: {s:?}", sent.key))
            .into_inner();
        match &sent.reply {
            Some(before) => assert_eq!(&reply, before, "a retry of {} with its key", sent.key),
            None => sent.reply = Some(reply),
        }
    }
}

/// Collects the parts of one `GetChangeset` reply into its bytes.
async fn changeset_bytes(scion: &mut Scion, request: GetChangesetRequest, what: &str) -> Vec<u8> {
    let mut stream = scion
        .get_changeset(request)
        .await
        .unwrap_or_else(|s| panic!("{what}: {s:?}"))
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(part) = stream.message().await.unwrap() {
        bytes.extend_from_slice(&part.changeset);
    }
    bytes
}

/// The changesets on each ref's first-parent history, but the root, with
/// the ref. Checks that each one hashes to its ID, and that its root tree
/// differs from its parent's by exactly its changes.
pub async fn walk_refs(
    scion: &mut Scion,
    objects: &mut Objects,
    checked: &mut Checked,
) -> HashMap<ChangesetId, (&'static str, Changeset)> {
    let mut history = HashMap::new();
    for target in REFS {
        let mut at = resolve_ref(objects, target).await.unwrap();
        loop {
            let request = GetChangesetRequest {
                repo: REPO.into(),
                changeset_id: at.as_bytes().to_vec(),
            };
            let bytes = changeset_bytes(scion, request, &format!("{target}: {at}")).await;
            let (changeset, id) = Changeset::decode_with_id(&bytes).unwrap();
            assert_eq!(id, at, "{target}: changeset {at} does not match its ID");
            let Some(&parent) = changeset.parents.first() else {
                break;
            };
            if checked.changesets.insert(at) {
                let diff = diff(scion, parent, at).await;
                assert_eq!(diff, changeset.changes, "{target}: root tree of {at}");
            }
            let previous = history.insert(at, (target, changeset));
            let twice = "is on two refs, or twice on one";
            assert!(previous.is_none(), "{at} {twice}");
            at = parent;
        }
    }
    history
}

/// Checks that each landed changeset is on its ref once, with the changes
/// sent, and that nothing else is on a ref.
pub fn check_lands(ledger: &Ledger, history: &HashMap<ChangesetId, (&str, Changeset)>) {
    let mut lands_on_refs = 0;
    // A land ID names one request key for ever, so no second reply may
    // carry it.
    let mut land_ids = HashSet::new();
    for sent in &ledger.lands {
        let reply = sent.reply.as_ref().expect("every land was retried");
        assert!(
            land_ids.insert(reply.land_id.as_str()),
            "land ID {} went to two keys",
            reply.land_id
        );
        let key = &sent.key;
        match reply.state() {
            LandState::Landed => {
                let result = changeset_id(&reply.result_changeset_id);
                let (target, landed) = history
                    .get(&result)
                    .unwrap_or_else(|| panic!("{key} landed as {result}, which is not on a ref"));
                assert_eq!(*target, sent.target, "{key}");
                // The server may rebase, so only the parents may differ.
                let mut want = sent.changeset.clone();
                want.parents.clone_from(&landed.parents);
                assert_eq!(landed, &want, "{key}");
                lands_on_refs += 1;
            }
            LandState::Conflict => {
                let shared = RepoPath::new(SHARED).unwrap();
                let changes = &sent.changeset.changes;
                assert!(changes.contains_key(&shared), "{key}: {reply:?}");
                assert_eq!(
                    reply.conflict_paths,
                    vec![shared.as_bytes().to_vec()],
                    "{key}"
                );
            }
            _ => panic!("{key}: {reply:?}"),
        }
    }
    // So no land is on a ref twice, and no change without a land.
    assert_eq!(history.len(), lands_on_refs);
}

/// Checks what a crash must never break (`docs/architecture.md`, section
pub async fn check(
    url: &str,
    data: &Path,
    ledger: &mut Ledger,
    checked: &mut Checked,
    rng: &mut Rng,
) {
    // A restart removes the staging folders of the processes that died, and
    // its own folder is the only one in `tmp/` (issue #101).
    let folders: Vec<PathBuf> = fs::read_dir(data.join("tmp"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(
        folders.len(),
        1,
        "temp folders after the restart: {folders:?}"
    );
    assert!(
        folders.iter().all(|path| path.is_dir()),
        "temp files after the restart: {folders:?}"
    );
    let (mut scion, mut objects) = connect(url).await.unwrap();
    let new_lands = checked.lands;
    retry_lands(&scion, ledger, checked, rng).await;
    let history = walk_refs(&mut scion, &mut objects, checked).await;
    check_lands(ledger, &history);

    // SHORTCUT: GetObjects fails closed until C5 verifies tickets, so the
    // read-back of the new blobs waits for C5. Their files are still
    // verified below. Every changeset that `CreateChangeset` replied with
    // is there.
    checked.blobs = ledger.blobs.len();
    for sent in &ledger.lands[new_lands..] {
        let request = GetChangesetRequest {
            repo: REPO.into(),
            changeset_id: sent.id.as_bytes().to_vec(),
        };
        changeset_bytes(
            &mut scion,
            request,
            &format!("created changeset {}", sent.id),
        )
        .await;
    }

    // Every blob that the metadata names is intact, and so is every blob
    // file: a file reaches its name only whole.
    let store = Store::open_shared(data).unwrap();
    let (named, pending) = metadata(&Backend::Sqlite(store.clone()), &mut checked.rows);
    assert_eq!(pending, 0, "every land sent was retried");
    let files = blob_files(data);
    for id in named.into_iter().chain(files) {
        if checked.files.insert(id) {
            let status = store.blobs().verify(id).unwrap();
            assert_eq!(status, BlobStatus::Valid, "blob {id}");
        }
    }
}

/// Waits until `ready` holds, for at most 10 s.
pub async fn wait_until(ready: impl Fn() -> bool) {
    let give_up = Instant::now() + Duration::from_secs(10);
    while !ready() && Instant::now() < give_up {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// The environment variable `name` as a number, or `default`.
pub fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |v| v.parse().unwrap())
}

/// Keeps child processes out of the window where a test drops a store and
/// checks its lock released. A forked child shares the flock of every open
/// file description until it execs, so a child spawned while another test
/// drops a store can hold the dropped store's lock past that check. Every
/// test that spawns a child takes the read side across the spawn, and a
/// test that asserts a lock is released takes the write side across its
/// drop and reopen: a pending write side also stops new read sides, so the
/// spawners finish before the release window opens. (The same guard, for
/// the unit tests of the server, is `store::spawn_guard`.)
pub mod spawn_guard {
    use std::sync::RwLock;

    static SPAWNS: RwLock<()> = RwLock::new(());

    /// Holds the read side while `f` spawns its child: any number of tests
    /// may spawn at once.
    pub fn across_spawn<T>(f: impl FnOnce() -> T) -> T {
        let _held = SPAWNS.read().expect("the spawn guard is not poisoned");
        f()
    }

    /// Holds the write side while `f` drops a store and checks its lock is
    /// released: no child is in its fork-to-exec window, and no new spawn
    /// starts, until `f` is done.
    pub fn across_release<T>(f: impl FnOnce() -> T) -> T {
        let _held = SPAWNS.write().expect("the spawn guard is not poisoned");
        f()
    }

    /// Primes gix's one-time `git` installation lookup under the read
    /// side. The first `gix::open` in a process forks `git` outside our
    /// control (gix-path), and caches the result afterwards. Every test
    /// that reaches `gix` calls this first, so that fork waits out any
    /// release window like our own spawns do.
    pub fn prime_gix_lookup() {
        across_spawn(|| {
            let _ = gix::path::env::installation_config();
        });
    }

    /// Waits until the server lock of `dir` is free. Call it on the write
    /// side (inside [`across_release`]), after dropping the stores and
    /// before the reopen that must see the lock released.
    ///
    /// The poll takes the real condition — the lock is free — instead of
    /// a timing guess: it opens `<dir>/server.lock` and takes a
    /// non-blocking exclusive flock every 1 ms, up to 2 s, then fails
    /// with the holder message, so a real leak still fails. The delay it
    /// absorbs is the operating system's: macOS tears a reaped child's
    /// inherited `flock` down one tick after `waitpid`, and about 7% of
    /// immediate reopens after a Rust spawn and its wait fail with
    /// `EWOULDBLOCK` there, sub-ms; Linux shows 0 of 2,000 rounds (a
    /// standalone probe). (The same helper, for the unit tests of the
    /// server, is `store::spawn_guard::wait_for_lock_release`.)
    pub fn wait_for_lock_release(dir: &std::path::Path) {
        use std::time::{Duration, Instant};

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(dir.join("server.lock"))
                .unwrap_or_else(|e| panic!("cannot open the lock of {}: {e}", dir.display()));
            // rustix's flock takes the lock or fails with would-block:
            // `Ok` is free, `Err` is still held.
            let free =
                rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
                    .is_ok();
            drop(file);
            if free {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the server lock of {} was never released within 10 s",
                dir.display()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Runs git in `dir` with fixed identities and no user or system config,
/// and reports whether it succeeded with its stdout. The child shares a
/// flock until it execs (see `spawn_guard`), so the read side of one test
/// cannot land inside another test's lock-release window.
pub fn git_status(dir: &Path, args: &[&str]) -> (bool, String) {
    let out = spawn_guard::across_spawn(|| {
        Command::new("git")
            // No auto gc or maintenance: they pack loose objects in the
            // background and delete the loose files, also while an import
            // of the same repository still reads them.
            .args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"])
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Ada")
            .env("GIT_AUTHOR_EMAIL", "ada@example.com")
            .env("GIT_AUTHOR_DATE", "1700000000 +0000")
            .env("GIT_COMMITTER_NAME", "Ada")
            .env("GIT_COMMITTER_EMAIL", "ada@example.com")
            .env("GIT_COMMITTER_DATE", "1700000000 +0000")
            .output()
    })
    .unwrap();
    let stdout = String::from_utf8(out.stdout).unwrap().trim_end().to_owned();
    (out.status.success(), stdout)
}

/// Runs git in `dir` and returns its stdout. Fails the test unless git
/// succeeds.
pub fn git(dir: &Path, args: &[&str]) -> String {
    let (ok, stdout) = git_status(dir, args);
    assert!(ok, "git {args:?} failed");
    stdout
}

/// Awaits `fut`, or panics naming `step`: a server that never answers a
/// step fails the test in seconds, not at the CI cancel.
pub async fn ceiling<T>(step: &str, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(30), fut)
        .await
        .unwrap_or_else(|_| panic!("the step {step} never finished within 30 s"))
}
