//! Crash suite: the real `scion-server` binary, killed with SIGKILL while
//! several clients upload, create changesets and land, then started again on
//! the same data directory. After each restart, [`check`] checks what a crash
//! must never break; `docs/architecture.md` (section 10) lists it. A second
//! test kills the server while it upgrades the schema of a data directory.
//!
//! What this does not prove: SIGKILL stops the process, but the kernel keeps
//! the writes that reached its page cache, synced or not. So the suite checks
//! the order of writes, renames and transactions, not durability after a
//! power loss. A missing `fsync` or full flush passes it.
//!
//! Each run of the first test picks a seed and prints it.
//! `SCION_CRASH_SEED=<n>` runs the same workload and kill times again;
//! thread timing still differs. `SCION_CRASH_KILLS=<n>` sets the number of
//! kills (default 6).
#![cfg(unix)]

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileState, IdentityTime, RepoPath, Tree, TreeId,
};
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
    fn listens(&mut self, log: &Path) -> bool {
        let Some(addr) = bound_addr(log) else {
            return false;
        };
        self.addr = addr;
        self.url = format!("http://{addr}");
        TcpStream::connect(addr).is_ok()
    }

    /// Starts a server on `data`, and does not wait for it.
    fn spawn(data: &Path, log: &Path) -> Self {
        let log_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .unwrap();
        // The child shares a flock until it execs (see `common::spawn_guard`):
        // the read side keeps this spawn out of a lock-release window of
        // another test.
        let child = common::spawn_guard::across_spawn(|| {
            Command::new(env!("CARGO_BIN_EXE_scion-server"))
                .arg("--data")
                .arg(data)
                .args(["serve", "--token", TOKEN, "--listen", "127.0.0.1:0"])
                // Plain log lines, so the address parses without escape
                // codes.
                .env("NO_COLOR", "1")
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
fn bound_addr(log: &Path) -> Option<SocketAddr> {
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

/// `Objects.ResolveRef` as one call: the changeset ID that `name` names.
/// The `Objects` calls stay in one place, so a change to the service moves
/// one helper, not every call.
async fn resolve_ref(objects: &mut Objects, name: &str) -> Result<ChangesetId, Status> {
    let request = ResolveRefRequest {
        repo: REPO.into(),
        name: name.into(),
    };
    let reply = objects.resolve_ref(request).await?;
    Ok(changeset_id(&reply.into_inner().changeset_id))
}

/// `Objects.GetRootTree` as one call: the ID of the root tree of `changeset`.
async fn root_tree(objects: &mut Objects, changeset: ChangesetId) -> TreeId {
    let request = GetRootTreeRequest {
        repo: REPO.into(),
        changeset_id: changeset.as_bytes().to_vec(),
    };
    let reply = objects.get_root_tree(request).await.unwrap();
    TreeId::try_from(reply.into_inner().tree_id.as_slice()).unwrap()
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
    /// Every land sent, in order. `CreateChangeset` replied for each one.
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
    /// `PutBlob` calls of a large file in progress.
    large_uploads: AtomicUsize,
}

/// A file of more than this many bytes counts as large. Its upload writes
/// the blob near its end, 10 to 30 ms after it starts.
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
struct Checked {
    /// Lands `..lands` were retried after a restart.
    lands: usize,
    /// Blobs `..blobs` of the ledger were read back.
    blobs: usize,
    /// Changesets whose root tree was checked against their changes.
    changesets: HashSet<ChangesetId>,
    /// Changeset and tree rows whose blobs were found.
    rows: HashSet<Vec<u8>>,
    /// Blobs found intact on disk.
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

/// Lands that are pending in the database.
fn pending_lands(c: &rusqlite::Connection) -> i64 {
    let pending = "SELECT count(*) FROM lands WHERE state = 'pending'";
    c.query_row(pending, [], |row| row.get(0)).unwrap()
}

/// The blobs that the changeset and tree rows of the database name, and the
/// number of pending lands. Rows in `seen` are skipped, and new rows added:
/// a row never changes.
fn metadata(store: &Store, seen: &mut HashSet<Vec<u8>>) -> (BTreeSet<BlobId>, i64) {
    store
        .read(|c| {
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
                            Entry::File(state) => Some(state),
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
        let mut at = resolve_ref(objects, target).await.unwrap();
        loop {
            let request = GetChangesetRequest {
                repo: REPO.into(),
                changeset_id: at.as_bytes().to_vec(),
            };
            let bytes = collect_changeset(scion, request, target, at).await;
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
fn check_lands(ledger: &Ledger, history: &HashMap<ChangesetId, (&str, Changeset)>) {
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
/// 10) against a server that has just started on `data`.
async fn check(url: &str, data: &Path, ledger: &mut Ledger, checked: &mut Checked, rng: &mut Rng) {
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

    // Every blob that `PutBlob` replied with reads back. It comes as its
    // stored file, so the file is intact too. Every changeset that
    // `CreateChangeset` replied with is there.
    let blobs = &ledger.blobs[checked.blobs..];
    read_blobs(&mut objects, blobs).await;
    checked.files.extend(blobs);
    checked.blobs = ledger.blobs.len();
    for sent in &ledger.lands[new_lands..] {
        let request = GetChangesetRequest {
            repo: REPO.into(),
            changeset_id: sent.id.as_bytes().to_vec(),
        };
        let mut stream = scion
            .get_changeset(request)
            .await
            .unwrap_or_else(|s| panic!("created changeset {}: {s:?}", sent.id))
            .into_inner();
        while let Some(part) = stream.message().await.unwrap() {
            drop(part);
        }
    }

    // Every blob that the metadata names is intact, and so is every blob
    // file: a file reaches its name only whole.
    let store = Store::open_shared(data).unwrap();
    let (named, pending) = metadata(&store, &mut checked.rows);
    assert_eq!(pending, 0, "every land sent was retried");
    let files = blob_files(data);
    for id in named.into_iter().chain(files) {
        if checked.files.insert(id) {
            let status = store.blobs().verify(id).unwrap();
            assert_eq!(status, BlobStatus::Valid, "blob {id}");
        }
    }
}

/// Collects the parts of one `GetChangeset` reply.
async fn collect_changeset(
    scion: &mut Scion,
    request: GetChangesetRequest,
    target: &str,
    at: ChangesetId,
) -> Vec<u8> {
    let mut stream = scion
        .get_changeset(request)
        .await
        .unwrap_or_else(|s| panic!("{target}: {at}: {s:?}"))
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(part) = stream
        .message()
        .await
        .unwrap_or_else(|s| panic!("{target}: {at}: {s:?}"))
    {
        bytes.extend_from_slice(&part.changeset);
    }
    bytes
}

/// Waits until `ready` holds, for at most 2 s.
async fn wait_until(ready: impl Fn() -> bool) {
    let give_up = Instant::now() + Duration::from_secs(2);
    while !ready() && Instant::now() < give_up {
        tokio::time::sleep(Duration::from_millis(1)).await;
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
        // Two kills in three, on average, wait for a moment that a crash
        // must survive: a blob write, soon after a large upload starts, or a
        // land that is pending in the database.
        match rng.below(3) {
            0 => {}
            1 => {
                wait_until(|| signals.large_uploads.load(Ordering::SeqCst) > 0).await;
                let delay = u64::try_from(rng.below(30)).unwrap();
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            _ => {
                let db = rusqlite::Connection::open(data.join("scion.db")).unwrap();
                wait_until(|| pending_lands(&db) > 0).await;
            }
        }
        signals.killed.store(true, Ordering::SeqCst);
        drop(server);
        for client in clients {
            let mut got = client.await.unwrap();
            ledger.blobs.append(&mut got.blobs);
            cut += got.lands.iter().filter(|sent| sent.reply.is_none()).count();
            ledger.lands.append(&mut got.lands);
        }
        // The staging folders of the killed processes are swept by the
        // restart inside `check`; what a kill left is a folder per process.
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
const FILLER_ROWS: i64 = 30_000;

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

/// What a kill can leave of the database in `data`: its schema version, its
/// tables and indexes (name and SQL), and its filler rows.
#[derive(Debug, PartialEq, Eq)]
struct Schema {
    version: u32,
    tables: Vec<(String, Option<String>)>,
    filler: i64,
}

fn schema(data: &Path) -> Schema {
    let c = rusqlite::Connection::open(data.join("scion.db")).unwrap();
    let version = c.query_row("PRAGMA user_version", [], |row| row.get(0));
    let mut tables = c
        .prepare("SELECT name, sql FROM sqlite_master ORDER BY name")
        .unwrap();
    let tables = tables.query_map([], |row| Ok((row.get(0)?, row.get(1)?)));
    let filler = "SELECT count(*) FROM derived WHERE kind = 'filler'";
    Schema {
        version: version.unwrap(),
        tables: tables.unwrap().map(Result::unwrap).collect(),
        filler: c.query_row(filler, [], |row| row.get(0)).unwrap(),
    }
}

/// A kill while the server upgrades the schema leaves the database at one
/// whole migration step, never a mix: each migration commits in one
/// transaction. The next start upgrades it and serves.
#[test]
fn a_kill_during_a_schema_upgrade_leaves_the_old_schema_or_the_new_one() {
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    let root = version_2_data(&data);
    let old = dir.path().join("version-2.db");
    fs::copy(data.join("scion.db"), &old).unwrap();
    let version_2 = schema(&data);
    assert_eq!((version_2.version, version_2.filler), (2, FILLER_ROWS));
    let restore = || {
        for file in ["scion.db-wal", "scion.db-shm"] {
            fs::remove_file(data.join(file)).ok();
        }
        fs::copy(&old, data.join("scion.db")).unwrap();
    };

    // A whole start with the upgrade. Its time sets the step of the kills,
    // so their number does not depend on the speed of the machine. Its
    // schema is the final one, whatever migrations a build ships.
    let start = Instant::now();
    drop(Server::start(&data, &log));
    let step = start.elapsed() / 64;
    let upgraded = schema(&data);
    assert!(upgraded.version > version_2.version, "no migration ran");
    assert_eq!(upgraded.filler, FILLER_ROWS);

    // Kills 0, 1, 2 steps and so on after a start, each from version 2,
    // until one comes after the upgrade committed. The kills after the
    // upgrade began stop it in the middle.
    let mut kills = 0;
    loop {
        restore();
        let server = Server::spawn(&data, &log);
        let after = step * kills;
        std::thread::sleep(after);
        drop(server);
        kills += 1;
        let found = schema(&data);
        if found == upgraded {
            break;
        }
        // Each migration commits in one transaction, so a kill leaves the
        // database at one whole step: the version it started at (its exact
        // tables and rows), the final one, or one between them, which also
        // keeps every filler row.
        let whole = found == version_2
            || (found.version > version_2.version
                && found.version < upgraded.version
                && found.filler == FILLER_ROWS);
        assert!(whole, "a kill after {after:?}: {found:?}");
        assert!(kills < 256, "no upgrade");
    }
    println!("{kills} kills, {step:?} apart; the last one came after the upgrade");

    let server = Server::start(&data, &log);
    assert_eq!(schema(&data), upgraded);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (_, mut objects) = connect(&server.url).await.unwrap();
        assert_eq!(resolve_ref(&mut objects, "main").await.unwrap(), root);
        assert_eq!(root_tree(&mut objects, root).await, Tree::default().id());
    });
}
