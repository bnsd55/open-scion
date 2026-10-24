//! Helpers for the tests that run the cache tiers: a server, a cache in front
//! of it and a daemon in front of the cache, all in one process.
// Each test file uses only some of them.
#![allow(dead_code)]

use std::time::Duration;

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath, TreeId};
use scion_proto::CHUNK_BYTES;
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::listen::Limits;
use scion_proto::runtime::listen::bind_unix;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::{ObjectKind, ObjectRef};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{
    Backend,
    test_support::{service, tree},
};
use scion_tier::{CACHE_UPSTREAM_TIMEOUT, DAEMON_UPSTREAM_TIMEOUT, DiskStore, MemoryStore, Tier};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

pub(crate) const REPO: &str = "r";
pub(crate) const TOKEN: &str = "secret";

pub(crate) type Client = ObjectsClient<InterceptedService<Channel, Bearer>>;

/// An `Objects` client of `url` (`http://` or `unix://`) that sends the
/// token.
pub(crate) async fn connect(url: &str) -> Client {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        // A server that died mid-reply must not leave a test client
        // waiting forever: the connect timeout bounds the dial, the
        // per-request timeout bounds every call.
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .unwrap();
    ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap())
}

/// A ref of `id` at `path`, from the root tree of the ticket that carries
/// it. An empty `path` names the root tree itself.
pub(crate) fn tree_ref(id: TreeId, path: &[u8]) -> ObjectRef {
    ObjectRef {
        kind: ObjectKind::Tree.into(),
        id: id.as_bytes().to_vec(),
        path: path.to_vec(),
    }
}

pub(crate) fn blob_ref(id: BlobId, path: &[u8]) -> ObjectRef {
    ObjectRef {
        kind: ObjectKind::Blob.into(),
        id: id.as_bytes().to_vec(),
        path: path.to_vec(),
    }
}

/// A server, a cache in front of it, and a daemon in front of the cache.
/// Dropping it stops all three.
pub(crate) struct Tiers {
    pub store: Backend,
    /// Address of the server.
    pub server: String,
    pub cache: Tier<MemoryStore>,
    /// Address of the cache.
    pub cache_url: String,
    pub daemon: Tier<DiskStore>,
    /// `unix://` address of the daemon.
    pub socket: String,
    pub runtime: Runtime,
    pub dirs: [TempDir; 2],
}

impl Tiers {
    pub(crate) fn start(data: TempDir, store: Backend) -> Self {
        let runtime = Runtime::new().unwrap();
        let _entered = runtime.enter();
        let tcp = || {
            let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            (listener, url)
        };
        let pending = std::future::pending;

        // The server mints the tickets; the cache verifies them; the daemon
        // only forwards them.
        let tickets = scion_server::Keys::generate();
        let verify_keys = tickets.for_verify().to_vec();
        let (listener, server) = tcp();
        runtime.spawn(service::serve(
            store.clone(),
            listener,
            TOKEN,
            service::ServeOptions {
                data: data.path().to_path_buf(),
                audit: scion_server::test_support::audit::Audit::off(),
                limits: Limits::default(),
                quotas: scion_server::Quotas::off(),
                pending_lands: None,
                land_stale_secs: 1 << 40,
                tls: None,
                metrics: Arc::default(),
                reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
            },
            Some(std::sync::Arc::new(tickets)),
            pending(),
        ));

        let memory = MemoryStore::new(64 << 20);
        let cache = Tier::new(
            memory,
            &server,
            TOKEN,
            CACHE_UPSTREAM_TIMEOUT,
            None,
            verify_keys,
        )
        .unwrap();
        let (listener, cache_url) = tcp();
        let incoming = TcpListenerStream::new(listener);
        runtime.spawn(
            cache
                .clone()
                .serve(TOKEN, incoming, Limits::default(), None, pending()),
        );

        let node = tempfile::tempdir().unwrap();
        let store_dir = DiskStore::open(&node.path().join("data"), 64 << 20).unwrap();
        let daemon = Tier::new(
            store_dir,
            &cache_url,
            TOKEN,
            DAEMON_UPSTREAM_TIMEOUT,
            None,
            Vec::new(),
        )
        .unwrap();
        // The socket's directory must be private.
        fs::set_permissions(node.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = node.path().join("daemon.sock");
        let incoming = UnixListenerStream::new(runtime.block_on(bind_unix(&path)).unwrap());
        runtime.spawn(
            daemon
                .clone()
                .serve(TOKEN, incoming, Limits::default(), None, pending()),
        );

        Self {
            store,
            server,
            cache,
            cache_url,
            daemon,
            socket: format!("unix://{}", path.display()),
            runtime,
            dirs: [data, node],
        }
    }

    /// Calls past the daemon, and calls that reached the server.
    pub(crate) fn upstream_calls(&self) -> (u64, u64) {
        (self.daemon.upstream_calls(), self.cache.upstream_calls())
    }

    /// Where the server, and the daemon, keep blob `id`.
    pub(crate) fn blob_paths(&self, id: BlobId) -> [PathBuf; 2] {
        let [server, node] = &self.dirs;
        [server.path().to_path_buf(), node.path().join("data")].map(|dir| blob_path(&dir, id))
    }

    /// The file of blob `id` on the server, and on the daemon's disk.
    pub(crate) fn blob_files(&self, id: BlobId) -> [Vec<u8>; 2] {
        self.blob_paths(id).map(|path| fs::read(path).unwrap())
    }

    /// Where the daemon keeps tree `id`. The server keeps trees in its
    /// database.
    pub(crate) fn daemon_tree_path(&self, id: TreeId) -> PathBuf {
        let node = self.dirs[1].path().join("data");
        object_path(&node, "tree", &id.to_string())
    }
}

/// Where the server, and the daemon, keep blob `id` under `dir`.
pub(crate) fn blob_path(dir: &Path, id: BlobId) -> PathBuf {
    object_path(dir, "blob", &id.to_string())
}

/// Where an object of `kind` (`tree` or `blob`) with ID `hex` is under `dir`.
fn object_path(dir: &Path, kind: &str, hex: &str) -> PathBuf {
    dir.join("objects")
        .join(kind)
        .join(&hex[..2])
        .join(&hex[2..])
}

/// The files of a snapshot with one of each kind: a large file of four
/// parts of 1 MiB, packed or not (noise, then text), an executable, nested
/// directories, two paths with one blob, a symlink and an empty file. Puts
/// their blobs in `store`, and returns them with the large file's content.
pub(crate) fn files(store: &Backend) -> (Vec<(&'static str, FileState)>, Vec<u8>) {
    let mut big = noise(3 * CHUNK_BYTES + 1);
    big.extend((0..5_000).flat_map(|i| format!("line {i}\n").into_bytes()));
    let mut blobs = store.blobs().batch();
    let mut regular = |content: &[u8], executable| FileState::Regular {
        content: blobs.put(content).unwrap(),
        executable,
    };
    let readme = regular(b"hello\n", false);
    let mut files = vec![
        ("README.md", readme),
        ("big.bin", regular(&big, false)),
        ("empty", regular(b"", false)),
        ("run.sh", regular(b"#!/bin/sh\necho hi\n", true)),
        ("src/lib.rs", regular(b"pub fn f() {}\n", false)),
        ("src/nested/deep/file.txt", regular(b"deep\n", false)),
        // The same blob as README.md.
        ("src/nested/copy.txt", readme),
    ];
    let target = blobs.put(b"README.md").unwrap();
    files.push(("link", FileState::Symlink { target }));
    blobs.sync().unwrap();
    (files, big)
}

/// `len` bytes that zstd cannot make smaller.
pub(crate) fn noise(len: usize) -> Vec<u8> {
    let mut state = 1_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[3]
        })
        .collect()
}

/// A changeset on `parent` that puts `files` (raw path and state).
pub(crate) fn changeset(parent: Option<ChangesetId>, files: &[(&str, FileState)]) -> Changeset {
    Changeset::new(
        parent.into_iter().collect(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"tiers\n".to_vec(),
        files
            .iter()
            .map(|(path, state)| (RepoPath::new(*path).unwrap(), Some(*state)))
            .collect(),
    )
    .expect("changeset fits the format")
}

/// Stores `cs` in the repo and points `main` at it.
pub(crate) fn land(store: &Backend, cs: &Changeset) -> ChangesetId {
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let (id, _) = tree::store_changeset(c, repo, cs, &mut tree::lens_of(store))?;
            let version = c.get_ref(repo, "main")?.map(|r| r.version);
            assert!(c.cas_ref(repo, "main", version, id)?);
            Ok(id)
        })
        .unwrap()
}
