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
//! `SCION_CRASH_KILLS=<n>` sets the number of kills (default 4).
#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use scion_core::{BlobId, Changeset, ChangesetId, Entry, FileState, IdentityTime, RepoPath, Tree};
use scion_proto::auth::Bearer;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{
    self, CreateChangesetRequest, DiffRequest, FileKind, GetChangesetRequest, GetObjectsRequest,
    LandRequest, LandResponse, LandState, ObjectKind, ObjectRef, PutBlobRequest, ResolveRefRequest,
};
use scion_proto::{CHUNK_BYTES, MAX_OBJECT_MESSAGE_BYTES, MAX_OBJECT_REFS};
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
    url: String,
}

impl Server {
    /// Starts a server on `data` and waits until it listens. It appends its
    /// log to `log`.
    fn start(data: &Path, log: &Path) -> Self {
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
        let mut child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
            .arg("--data")
            .arg(data)
            .args(["serve", "--token", TOKEN, "--listen", &addr.to_string()])
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(log_file)
            .spawn()
            .unwrap();
        // The server opens the data directory, and so removes the temp
        // files, before it listens.
        let start = Instant::now();
        while TcpStream::connect(addr).is_err() {
            if let Some(status) = child.try_wait().unwrap() {
                let log = fs::read_to_string(log).unwrap_or_default();
                panic!("the server exited with {status}:\n{log}");
            }
            assert!(start.elapsed() < Duration::from_secs(60), "no listen");
            std::thread::sleep(Duration::from_millis(10));
        }
        Self {
            child,
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
    let objects = ObjectsClient::with_interceptor(channel.clone(), bearer.clone())
        .max_decoding_message_size(MAX_OBJECT_MESSAGE_BYTES);
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

/// One client: uploads files, creates a changeset on a ref's head (or on an
/// older base of it), and lands it, until a call fails. Only the kill may
/// fail a call, and it sets `killed` first.
async fn client(url: String, name: String, mut rng: Rng, killed: Arc<AtomicBool>) -> Ledger {
    let mut ledger = Ledger::default();
    let cut = |ledger, call: &str, error: &dyn std::fmt::Debug| {
        let killed = killed.load(Ordering::SeqCst);
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
                (64 << 10) + rng.below(1 << 20)
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
            let id = match put_blob(&mut scion, &data).await {
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
        };
        let mut reply = objects.get_objects(request).await.unwrap().into_inner();
        let mut got = 0;
        while let Some(object) = reply
            .message()
            .await
            .unwrap_or_else(|status| panic!("blob {:?}: {status:?}", batch.get(got)))
        {
            assert_eq!(BlobId::of(&object.data), batch[got], "blob {}", batch[got]);
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

/// Checks the invariants of the module comment against a server that has
/// just started on `data`.
async fn check(url: &str, data: &Path, ledger: &mut Ledger, checked: &mut Checked, rng: &mut Rng) {
    let tmp: Vec<PathBuf> = fs::read_dir(data.join("tmp"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert!(tmp.is_empty(), "temp files after the restart: {tmp:?}");
    let (mut scion, mut objects) = connect(url).await.unwrap();

    // Retry each land sent since the last check, and a few older ones, with
    // its key. Calls run at once, so they share the server's flushes.
    let mut retry: Vec<usize> = (checked.lands..ledger.lands.len()).collect();
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

    // Walk each ref's first parents down to the root.
    let mut history: HashMap<ChangesetId, (&str, Changeset)> = HashMap::new();
    for target in REFS {
        let request = ResolveRefRequest {
            repo: REPO.into(),
            name: target.into(),
        };
        let mut at = changeset_id(
            &objects
                .resolve_ref(request)
                .await
                .unwrap()
                .into_inner()
                .changeset_id,
        );
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
                let diff = diff(&mut scion, parent, at).await;
                assert_eq!(diff, changeset.changes, "{target}: root tree of {at}");
            }
            let previous = history.insert(at, (target, changeset));
            assert!(
                previous.is_none(),
                "{at} is on two refs, or twice on {target}"
            );
            at = parent;
        }
    }

    // Each landed changeset is on its ref once, with the changes sent.
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
                assert!(
                    sent.changeset.changes.contains_key(&shared),
                    "{key}: {reply:?}"
                );
            }
            _ => panic!("{key}: {reply:?}"),
        }
    }
    // So nothing else is on a ref: no land twice, no change without a land.
    assert_eq!(history.len(), lands_on_refs);

    // Every blob that a reply named reads back, and every changeset that
    // `CreateChangeset` replied with.
    let named = ledger.lands.iter().flat_map(|sent| {
        let states = sent.changeset.changes.values().flatten();
        states.filter_map(FileState::blob)
    });
    let mut blobs: Vec<BlobId> = ledger.blobs.iter().copied().chain(named).collect();
    blobs.retain(|id| checked.replied.insert(*id));
    read_blobs(&mut objects, &blobs).await;
    for &id in &ledger.created {
        let request = GetChangesetRequest {
            repo: REPO.into(),
            changeset_id: id.as_bytes().to_vec(),
        };
        let reply = scion.get_changeset(request).await;
        reply.unwrap_or_else(|s| panic!("created changeset {id}: {s:?}"));
    }

    // Every blob that the metadata names is intact, and so is every blob
    // file: a file reaches its name only whole.
    let (store, named, pending) = metadata(data);
    assert_eq!(pending, 0, "every land sent was retried");
    let files = blob_files(data);
    for id in named.into_iter().chain(files) {
        if !checked.files.contains(&id) {
            assert_eq!(
                store.blobs().verify(id).unwrap(),
                BlobStatus::Valid,
                "blob {id}"
            );
            checked.files.insert(id);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_reply_survives_a_kill_of_the_server() {
    let env = |name, default| std::env::var(name).map_or(default, |v| v.parse().unwrap());
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let seed = env(
        "SCION_CRASH_SEED",
        now.as_secs() ^ u64::from(now.subsec_nanos()),
    );
    let kills = usize::try_from(env("SCION_CRASH_KILLS", 4)).unwrap();
    println!("crash suite: seed {seed}, {kills} kills; replay with SCION_CRASH_SEED={seed}");
    let mut rng = Rng::new(seed);
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);

    let mut ledger = Ledger::default();
    let mut checked = Checked::default();
    let (mut left, mut cut) = (0, 0);
    for round in 0..kills {
        let server = Server::start(&data, &log);
        check(&server.url, &data, &mut ledger, &mut checked, &mut rng).await;
        let killed = Arc::new(AtomicBool::new(false));
        let clients: Vec<_> = (0..CLIENTS)
            .map(|c| {
                let name = format!("k{round}c{c}");
                let rng = Rng::new(rng.next());
                tokio::spawn(client(server.url.clone(), name, rng, Arc::clone(&killed)))
            })
            .collect();
        let run = 100 + rng.below(500);
        tokio::time::sleep(Duration::from_millis(u64::try_from(run).unwrap())).await;
        killed.store(true, Ordering::SeqCst);
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
