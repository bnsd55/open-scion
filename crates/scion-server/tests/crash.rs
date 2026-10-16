//! Crash suite: the real `scion-server` binary, killed with SIGKILL at random
//! moments while several clients upload, create changesets and land, then
//! started again on the same data directory.
//!
//! After each restart the test checks what a crash must never break:
//!
//! - The restarted server removed the temp files that the crash left.
//! - A retry of each land with its request key returns the reply that the
//!   land got before, field for field. A land whose reply was lost finishes
//!   on retry, as landed or conflict.
//! - Each ref points at a changeset that exists, and so does each first
//!   parent down to the root. Each changeset hashes to its ID, and its root
//!   tree differs from its parent's by exactly its changes (`Diff`).
//! - Each landed changeset is on its ref once, with the changes that the
//!   client sent. Nothing else is on a ref, and no land is pending.
//! - Every blob that a reply named, and every blob that the metadata names,
//!   exists and matches its ID. No blob file is torn.
//!
//! What this does not prove: SIGKILL stops the process, but the kernel keeps
//! the writes that reached its page cache, synced or not. So the suite checks
//! the order of writes, renames and transactions, not durability after a
//! power loss. A missing `fsync` or full flush passes it.
//!
//! Each run picks a seed and prints it. `SCION_CRASH_SEED=<n>` runs the same
//! workload and kill times again; thread timing still differs.
//! `SCION_CRASH_KILLS=<n>` sets the number of kills (default 6).
#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use scion_core::{BlobId, Changeset, ChangesetId, Entry, FileState, IdentityTime, RepoPath, Tree};
use scion_proto::auth::Bearer;
use scion_proto::object::{self, Assembler, Header};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{
    self, CreateChangesetRequest, DiffRequest, Encoding, FileKind, GetChangesetRequest,
    GetObjectsRequest, GetRootTreeRequest, LandRequest, LandResponse, LandState, ObjectKind,
    ObjectRef, PutBlobRequest, ResolveRefRequest,
};
use scion_proto::{CHUNK_BYTES, MAX_OBJECT_REFS};
use scion_server::store::BlobStatus;
use scion_server::{Store, db, tree};
use tonic::Status;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const TOKEN: &str = "secret";
const REPO: &str = "r";
const REFS: [&str; 2] = ["main", "dev"];
const CLIENTS: usize = 4;
/// A path that some lands put, so that some lands conflict.
const SHARED: &str = "shared.txt";
/// Older lands that each check retries again.
const OLD_RETRIES: usize = 16;

/// A seeded stream of numbers (xorshift64*), so a run can be replayed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).unwrap()).unwrap()
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut bytes: Vec<u8> = (0..len.div_ceil(8))
            .flat_map(|_| self.next().to_le_bytes())
            .collect();
        bytes.truncate(len);
        bytes
    }
}

/// A `scion-server serve` process. Dropping it kills the process with
/// SIGKILL.
struct Server {
    child: Child,
    addr: SocketAddr,
    url: String,
}

impl Server {
    /// Starts a server on `data` and waits until it listens. It appends its
    /// log to `log`.
    fn start(data: &Path, log: &Path) -> Self {
        let mut server = Self::spawn(data, log);
        let start = Instant::now();
        while !server.listens() {
            if let Some(status) = server.child.try_wait().unwrap() {
                let log = fs::read_to_string(log).unwrap_or_default();
                panic!("the server exited with {status}:\n{log}");
            }
            assert!(start.elapsed() < Duration::from_secs(60), "no listen");
            std::thread::sleep(Duration::from_millis(10));
        }
        server
    }

    /// Whether the server listens. It opens the data directory before, so
    /// it has removed the temp files and upgraded the schema.
    fn listens(&self) -> bool {
        TcpStream::connect(self.addr).is_ok()
    }

    /// Starts a server on `data`, and does not wait for it.
    fn spawn(data: &Path, log: &Path) -> Self {
        // The OS picks a free port, and the server binds it again.
        let port = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        let log_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
            .arg("--data")
            .arg(data)
            .args(["serve", "--token", TOKEN, "--listen", &addr.to_string()])
            .env("RUST_LOG", "info")
            .stdout(log_file.try_clone().unwrap())
            .stderr(log_file)
            .spawn()
            .unwrap();
        Self {
            child,
            addr,
            url: format!("http://{addr}"),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // `Child::kill` sends SIGKILL.
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;
type Objects = ObjectsClient<InterceptedService<Channel, Bearer>>;

async fn connect(url: &str) -> Result<(Scion, Objects), tonic::transport::Error> {
    let channel = Channel::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await?;
    let bearer = Bearer::new(TOKEN).unwrap();
    let objects = ObjectsClient::with_interceptor(channel.clone(), bearer.clone());
    Ok((ScionClient::with_interceptor(channel, bearer), objects))
}

fn author() -> IdentityTime {
    IdentityTime {
        name: b"Ada".to_vec(),
        email: b"ada@example.com".to_vec(),
        unix_secs: 1_700_000_000,
        tz_offset_minutes: 0,
    }
}

fn changeset_id(bytes: &[u8]) -> ChangesetId {
    ChangesetId::try_from(bytes).unwrap()
}

/// A land that a client sent, and its reply if one came.
struct Sent {
    key: String,
    target: &'static str,
    changeset: Changeset,
    id: ChangesetId,
    reply: Option<LandResponse>,
}

impl Sent {
    fn request(&self) -> LandRequest {
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
struct Ledger {
    /// Blobs that `PutBlob` replied with.
    blobs: Vec<BlobId>,
    /// Changesets that `CreateChangeset` replied with.
    created: Vec<ChangesetId>,
    /// Every land sent, in order.
    lands: Vec<Sent>,
}

/// Repo `r` with one root changeset, and refs `main` and `dev` at it. No RPC
/// creates a repo or a ref.
fn create_repo(data: &Path) {
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
            let repo = db::create_repo(c, REPO)?;
            let (id, _) = tree::store_changeset(c, repo, &root)?;
            for name in REFS {
                assert!(db::cas_ref(c, repo, name, None, id)?);
            }
            Ok(())
        })
        .unwrap();
}

async fn put_blob(scion: &mut Scion, data: &[u8]) -> Result<BlobId, Status> {
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
struct Signals {
    /// Set just before the kill.
    killed: AtomicBool,
    /// Calls in progress that a kill can wait for: `PutBlob` of a large
    /// file, and `Land`.
    calls: [AtomicUsize; 2],
}

/// How long after a call of [`Signals::calls`] starts a kill may come, in
/// milliseconds. A large upload writes its blob near its end, after about
/// 10 to 30 ms. A land is pending for its first few ms, then waits for its
/// flush.
const KILL_WINDOW_MS: [usize; 2] = [30, 4];

impl Signals {
    /// Counts a call of kind `kind` in progress while `call` runs.
    async fn track<T>(&self, kind: usize, call: impl Future<Output = T>) -> T {
        self.calls[kind].fetch_add(1, Ordering::SeqCst);
        let done = call.await;
        self.calls[kind].fetch_sub(1, Ordering::SeqCst);
        done
    }
}

/// A file of more than this many bytes counts as large.
const LARGE: usize = 64 << 10;

/// One client: uploads files, creates a changeset on a ref's head (or on an
/// older base of it), and lands it, until a call fails. Only the kill may
/// fail a call, and it sets `signals.killed` first.
async fn client(url: String, name: String, mut rng: Rng, signals: Arc<Signals>) -> Ledger {
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
            _ => {
                let request = ResolveRefRequest {
                    repo: REPO.into(),
                    name: target.into(),
                };
                match objects.resolve_ref(request).await {
                    Ok(reply) => changeset_id(&reply.into_inner().changeset_id),
                    Err(status) => return cut(ledger, "ResolveRef", &status),
                }
            }
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
            let put = put_blob(&mut scion, &data);
            let put = if data.len() > LARGE {
                signals.track(0, put).await
            } else {
                put.await
            };
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
        ledger.created.push(id);

        let sent = Sent {
            key,
            target,
            changeset,
            id,
            reply: None,
        };
        let request = sent.request();
        ledger.lands.push(sent);
        match signals.track(1, scion.land(request)).await {
            Ok(reply) => ledger.lands.last_mut().unwrap().reply = Some(reply.into_inner()),
            Err(status) => return cut(ledger, "Land", &status),
        }
    }
    unreachable!("the loop runs until a call fails")
}

/// What earlier checks verified. Objects never change, so each one is
/// checked once.
#[derive(Default)]
struct Checked {
    /// Lands `..lands` were retried after a restart.
    lands: usize,
    /// Changesets `..created` were read back.
    created: usize,
    /// Changesets whose root tree was checked against their changes.
    changesets: HashSet<ChangesetId>,
    /// Blobs read back through `GetObjects`.
    replied: HashSet<BlobId>,
    /// Blob files found intact on disk.
    files: HashSet<BlobId>,
}

/// The changes of `new` against `old`, from `Diff`.
async fn diff(
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
async fn read_blobs(objects: &mut Objects, blobs: &[BlobId]) {
    let mut rest = blobs;
    while !rest.is_empty() {
        let batch = &rest[..rest.len().min(MAX_OBJECT_REFS)];
        let refs = batch
            .iter()
            .map(|id| ObjectRef {
                kind: ObjectKind::Blob.into(),
                id: id.as_bytes().to_vec(),
            })
            .collect();
        let request = GetObjectsRequest {
            repo: REPO.into(),
            refs,
            accept: vec![Encoding::Zstd.into()],
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

/// The blobs that every changeset and every tree in the database names, and
/// the number of pending lands. Read through a second `Store` that shares
/// the data directory with the server, as an import does.
fn metadata(data: &Path) -> (Store, BTreeSet<BlobId>, i64) {
    let store = Store::open_shared(data).unwrap();
    let (named, pending) = store
        .read(|c| {
            let mut named = BTreeSet::new();
            let mut changesets = c.prepare("SELECT body FROM changesets")?;
            for body in changesets.query_map([], |row| row.get::<_, Vec<u8>>(0))? {
                let changeset = Changeset::decode(&body?).unwrap();
                named.extend(
                    changeset
                        .changes
                        .values()
                        .flatten()
                        .filter_map(FileState::blob),
                );
            }
            let mut trees = c.prepare("SELECT body FROM trees")?;
            for body in trees.query_map([], |row| row.get::<_, Vec<u8>>(0))? {
                let tree = Tree::decode(&body?).unwrap();
                named.extend(tree.entries.values().filter_map(|entry| match entry {
                    Entry::File(state) => state.blob(),
                    Entry::Dir(_) => None,
                }));
            }
            let pending = c.query_row(
                "SELECT count(*) FROM lands WHERE state = 'pending'",
                [],
                |row| row.get(0),
            )?;
            Ok((named, pending))
        })
        .unwrap();
    (store, named, pending)
}

/// Every blob file under `<data>/objects/blob`, by its ID.
fn blob_files(data: &Path) -> Vec<BlobId> {
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
/// at once, so they share the server's flushes.
async fn retry_lands(scion: &Scion, ledger: &mut Ledger, checked: &mut Checked, rng: &mut Rng) {
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

/// The changesets on each ref's first-parent history, but the root, with
/// the ref. Checks that each one hashes to its ID, and that its root tree
/// differs from its parent's by exactly its changes.
async fn walk_refs(
    scion: &mut Scion,
    objects: &mut Objects,
    checked: &mut Checked,
) -> HashMap<ChangesetId, (&'static str, Changeset)> {
    let mut history = HashMap::new();
    for target in REFS {
        let request = ResolveRefRequest {
            repo: REPO.into(),
            name: target.into(),
        };
        let head = objects.resolve_ref(request).await.unwrap().into_inner();
        let mut at = changeset_id(&head.changeset_id);
        loop {
            let request = GetChangesetRequest {
                repo: REPO.into(),
                changeset_id: at.as_bytes().to_vec(),
            };
            let reply = scion.get_changeset(request).await;
            let bytes = reply
                .unwrap_or_else(|s| panic!("{target}: {at}: {s:?}"))
                .into_inner();
            let (changeset, id) = Changeset::decode_with_id(&bytes.changeset).unwrap();
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
fn check_lands(ledger: &Ledger, history: &HashMap<ChangesetId, (&str, Changeset)>) {
    let mut lands_on_refs = 0;
    for sent in &ledger.lands {
        let reply = sent.reply.as_ref().expect("every land was retried");
        let key = &sent.key;
        match reply.state() {
            LandState::Landed => {
                let result = changeset_id(&reply.result_changeset_id);
                let (target, landed) = history
                    .get(&result)
                    .unwrap_or_else(|| panic!("{key} landed as {result}, which is not on a ref"));
                assert_eq!(*target, sent.target, "{key}");
                assert_eq!(landed.changes, sent.changeset.changes, "{key}");
                assert_eq!(landed.message, sent.changeset.message, "{key}");
                lands_on_refs += 1;
            }
            LandState::Conflict => {
                let shared = RepoPath::new(SHARED).unwrap();
                let changes = &sent.changeset.changes;
                assert!(changes.contains_key(&shared), "{key}: {reply:?}");
            }
            _ => panic!("{key}: {reply:?}"),
        }
    }
    // So no land is on a ref twice, and no change without a land.
    assert_eq!(history.len(), lands_on_refs);
}

/// Checks the invariants of the module comment against a server that has
/// just started on `data`.
async fn check(url: &str, data: &Path, ledger: &mut Ledger, checked: &mut Checked, rng: &mut Rng) {
    let tmp: Vec<PathBuf> = fs::read_dir(data.join("tmp"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert!(tmp.is_empty(), "temp files after the restart: {tmp:?}");
    let (mut scion, mut objects) = connect(url).await.unwrap();
    retry_lands(&scion, ledger, checked, rng).await;
    let history = walk_refs(&mut scion, &mut objects, checked).await;
    check_lands(ledger, &history);

    // Every blob that a reply named reads back, and every changeset that
    // `CreateChangeset` replied with.
    let named = ledger.lands.iter().flat_map(|sent| {
        let states = sent.changeset.changes.values().flatten();
        states.filter_map(FileState::blob)
    });
    let mut blobs: Vec<BlobId> = ledger.blobs.iter().copied().chain(named).collect();
    blobs.retain(|id| checked.replied.insert(*id));
    read_blobs(&mut objects, &blobs).await;
    for &id in &ledger.created[checked.created..] {
        let request = GetChangesetRequest {
            repo: REPO.into(),
            changeset_id: id.as_bytes().to_vec(),
        };
        let reply = scion.get_changeset(request).await;
        reply.unwrap_or_else(|s| panic!("created changeset {id}: {s:?}"));
    }
    checked.created = ledger.created.len();

    // Every blob that the metadata names is intact, and so is every blob
    // file: a file reaches its name only whole.
    let (store, named, pending) = metadata(data);
    assert_eq!(pending, 0, "every land sent was retried");
    let files = blob_files(data);
    for id in named.into_iter().chain(files) {
        if !checked.files.contains(&id) {
            let status = store.blobs().verify(id).unwrap();
            assert_eq!(status, BlobStatus::Valid, "blob {id}");
            checked.files.insert(id);
        }
    }
}

/// The environment variable `name` as a number, or `default`.
fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |v| v.parse().unwrap())
}

/// `SCION_CRASH_SEED`, or a new seed. Printed, so a failed run can be run
/// again with it.
fn seed() -> u64 {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
    let now = now.unwrap();
    let seed = env(
        "SCION_CRASH_SEED",
        now.as_secs() ^ u64::from(now.subsec_nanos()),
    );
    println!("seed {seed}; replay with SCION_CRASH_SEED={seed}");
    seed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_reply_survives_a_kill_of_the_server() {
    let mut rng = Rng::new(seed());
    let kills = usize::try_from(env("SCION_CRASH_KILLS", 6)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);

    let mut ledger = Ledger::default();
    let mut checked = Checked::default();
    let (mut left, mut cut) = (0, 0);
    for round in 0..kills {
        let server = Server::start(&data, &log);
        check(&server.url, &data, &mut ledger, &mut checked, &mut rng).await;
        let signals = Arc::new(Signals::default());
        let clients: Vec<_> = (0..CLIENTS)
            .map(|c| {
                let name = format!("k{round}c{c}");
                let rng = Rng::new(rng.next());
                tokio::spawn(client(server.url.clone(), name, rng, Arc::clone(&signals)))
            })
            .collect();
        let run = 100 + rng.below(500);
        tokio::time::sleep(Duration::from_millis(u64::try_from(run).unwrap())).await;
        // Two kills in three, on average, come soon after a large upload or a
        // land starts, so they often stop the server while it writes a blob
        // or holds a pending land.
        if let Some(kind) = rng.below(3).checked_sub(1) {
            let give_up = Instant::now() + Duration::from_secs(2);
            let running = || signals.calls[kind].load(Ordering::SeqCst);
            while running() == 0 && Instant::now() < give_up {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let delay = u64::try_from(rng.below(KILL_WINDOW_MS[kind])).unwrap();
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        signals.killed.store(true, Ordering::SeqCst);
        drop(server);
        for client in clients {
            let mut got = client.await.unwrap();
            ledger.blobs.append(&mut got.blobs);
            ledger.created.append(&mut got.created);
            cut += got.lands.iter().filter(|sent| sent.reply.is_none()).count();
            ledger.lands.append(&mut got.lands);
        }
        left += fs::read_dir(data.join("tmp")).unwrap().count();
    }
    let server = Server::start(&data, &log);
    check(&server.url, &data, &mut ledger, &mut checked, &mut rng).await;

    println!(
        "{} lands, {cut} of them cut by a kill; {} blob files; {left} temp files left by the kills",
        ledger.lands.len(),
        checked.files.len(),
    );
    assert!(ledger.lands.len() > kills, "the clients did little work");
}

/// Derived rows in the schema-upgrade test, so that the upgrade of version 2
/// to 3, which copies every derived row, takes a while.
const FILLER_ROWS: i64 = 50_000;

/// A data directory at schema version 2, as a build of that version left it:
/// repo `r` with a root changeset, ref `main` at it, and [`FILLER_ROWS`]
/// derived rows of a kind that no build has. Returns the root.
fn version_2_data(data: &Path) -> ChangesetId {
    fs::create_dir_all(data).unwrap();
    let c = rusqlite::Connection::open(data.join("scion.db")).unwrap();
    // As the server leaves it. So a reader never blocks the server's open.
    c.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
    c.execute_batch(include_str!("../migrations/0001_init.sql"))
        .unwrap();
    c.execute_batch(include_str!("../migrations/0002_land_retention.sql"))
        .unwrap();
    let root = Changeset {
        parents: vec![],
        author: author(),
        committer: None,
        message: b"root\n".to_vec(),
        changes: BTreeMap::new(),
    };
    let (body, id) = root.encode_with_id();
    let id_bytes = id.as_bytes().as_slice();
    c.execute("INSERT INTO repos (repo_id, name) VALUES (1, ?1)", [REPO])
        .unwrap();
    let row = "INSERT INTO changesets (id, gen, body) VALUES (?1, 1, ?2)";
    c.execute(row, rusqlite::params![id_bytes, body]).unwrap();
    let row = "INSERT INTO repo_changesets (repo_id, cs) VALUES (1, ?1)";
    c.execute(row, [id_bytes]).unwrap();
    let row = "INSERT INTO refs (repo_id, name, target, version) VALUES (1, 'main', ?1, 1)";
    c.execute(row, [id_bytes]).unwrap();
    c.execute_batch(&format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {FILLER_ROWS})
             INSERT INTO changesets (id, gen, body) SELECT randomblob(32), 1, x'' FROM n;
         INSERT INTO derived (kind, version, cs, value)
             SELECT 'filler', 1, id, randomblob(32) FROM changesets WHERE body = x'';
         PRAGMA user_version = 2;"
    ))
    .unwrap();
    id
}

/// The schema version of the database in `data`.
fn version(data: &Path) -> u32 {
    let c = rusqlite::Connection::open(data.join("scion.db")).unwrap();
    c.query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap()
}

/// The schema version of the database in `data`, and its filler rows.
fn schema(data: &Path) -> (u32, i64) {
    let c = rusqlite::Connection::open(data.join("scion.db")).unwrap();
    let filler = "SELECT count(*) FROM derived WHERE kind = 'filler'";
    let filler = c.query_row(filler, [], |row| row.get(0));
    (version(data), filler.unwrap())
}

/// A kill while the server upgrades the schema leaves the old schema or the
/// new one, never a mix: each migration commits in one transaction. The
/// next start upgrades it and serves.
#[test]
fn a_kill_during_a_schema_upgrade_leaves_the_old_schema_or_the_new_one() {
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    let root = version_2_data(&data);
    let old = dir.path().join("version-2.db");
    fs::copy(data.join("scion.db"), &old).unwrap();

    // Kills 0, 10, 20 ms and so on after a start, each from version 2,
    // until one comes after the upgrade committed. The kills after the
    // upgrade began stop it in the middle.
    let mut kills = 0;
    for delay in (0..).map(|i| Duration::from_millis(10 * i)) {
        for log in ["scion.db-wal", "scion.db-shm"] {
            fs::remove_file(data.join(log)).ok();
        }
        fs::copy(&old, data.join("scion.db")).unwrap();
        let server = Server::spawn(&data, &log);
        std::thread::sleep(delay);
        drop(server);
        kills += 1;
        let (version, filler) = schema(&data);
        assert_eq!(filler, FILLER_ROWS, "version {version} after {delay:?}");
        match version {
            2 => assert!(delay < Duration::from_secs(60), "no upgrade"),
            3 => break,
            _ => panic!("version {version} after {delay:?}"),
        }
    }
    println!("{kills} kills; the last one came after the upgrade");

    let server = Server::start(&data, &log);
    assert_eq!(schema(&data), (3, FILLER_ROWS));
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (_, mut objects) = connect(&server.url).await.unwrap();
        let request = ResolveRefRequest {
            repo: REPO.into(),
            name: "main".into(),
        };
        let main = objects.resolve_ref(request).await.unwrap().into_inner();
        assert_eq!(changeset_id(&main.changeset_id), root);
        let request = GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: root.as_bytes().to_vec(),
        };
        let tree = objects.get_root_tree(request).await.unwrap().into_inner();
        assert_eq!(tree.tree_id, Tree::default().id().as_bytes());
    });
}
