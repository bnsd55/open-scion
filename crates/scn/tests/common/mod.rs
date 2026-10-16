//! Helpers for the tests that run the cache tiers: a server, a cache in front
//! of it and a daemon in front of the cache, all in one process.
// Each test file uses only some of them.
#![allow(dead_code)]

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::{Store, db, service, tree};
use scion_tier::{
    CACHE_UPSTREAM_TIMEOUT, DAEMON_UPSTREAM_TIMEOUT, DiskStore, MemoryStore, Tier, bind_unix,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};

pub const REPO: &str = "r";
pub const TOKEN: &str = "secret";

/// A server, a cache in front of it, and a daemon in front of the cache.
/// Dropping it stops all three.
pub struct Tiers {
    pub store: Store,
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
    pub fn start(data: TempDir, store: Store) -> Self {
        let runtime = Runtime::new().unwrap();
        let _entered = runtime.enter();
        let tcp = || {
            let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            (listener, url)
        };
        let pending = std::future::pending;

        let (listener, server) = tcp();
        runtime.spawn(service::serve(store.clone(), listener, TOKEN, pending()));

        let memory = MemoryStore::new(64 << 20);
        let cache = Tier::new(memory, &server, TOKEN, CACHE_UPSTREAM_TIMEOUT).unwrap();
        let (listener, cache_url) = tcp();
        let incoming = TcpListenerStream::new(listener);
        runtime.spawn(cache.clone().serve(TOKEN, incoming, pending()));

        let node = tempfile::tempdir().unwrap();
        let store_dir = DiskStore::open(&node.path().join("data"), 64 << 20).unwrap();
        let daemon = Tier::new(store_dir, &cache_url, TOKEN, DAEMON_UPSTREAM_TIMEOUT).unwrap();
        // The socket's directory must be private.
        fs::set_permissions(node.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = node.path().join("daemon.sock");
        let incoming = UnixListenerStream::new(runtime.block_on(bind_unix(&path)).unwrap());
        runtime.spawn(daemon.clone().serve(TOKEN, incoming, pending()));

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
    pub fn upstream_calls(&self) -> (u64, u64) {
        (self.daemon.upstream_calls(), self.cache.upstream_calls())
    }

    /// Where the server, and the daemon, keep blob `id`.
    pub fn blob_paths(&self, id: BlobId) -> [PathBuf; 2] {
        let [server, node] = &self.dirs;
        [server.path().to_path_buf(), node.path().join("data")].map(|dir| blob_path(&dir, id))
    }

    /// The file of blob `id` on the server, and on the daemon's disk.
    pub fn blob_files(&self, id: BlobId) -> [Vec<u8>; 2] {
        self.blob_paths(id).map(|path| fs::read(path).unwrap())
    }
}

/// Where the server, and the daemon, keep blob `id` under `dir`.
pub fn blob_path(dir: &Path, id: BlobId) -> PathBuf {
    let hex = id.to_string();
    dir.join("objects/blob").join(&hex[..2]).join(&hex[2..])
}

/// `len` bytes that zstd cannot make smaller.
pub fn noise(len: usize) -> Vec<u8> {
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
pub fn changeset(parent: Option<ChangesetId>, files: &[(&str, FileState)]) -> Changeset {
    Changeset {
        parents: parent.into_iter().collect(),
        author: IdentityTime {
            name: b"Ada".to_vec(),
            email: b"ada@example.com".to_vec(),
            unix_secs: 1_700_000_000,
            tz_offset_minutes: 0,
        },
        committer: None,
        message: b"tiers\n".to_vec(),
        changes: files
            .iter()
            .map(|(path, state)| (RepoPath::new(*path).unwrap(), Some(*state)))
            .collect(),
    }
}

/// Stores `cs` in the repo and points `main` at it.
pub fn land(store: &Store, cs: &Changeset) -> ChangesetId {
    store
        .write(|c| {
            let repo = db::create_repo(c, REPO)?;
            let (id, _) = tree::store_changeset(c, repo, cs)?;
            let version = db::get_ref(c, repo, "main")?.map(|r| r.version);
            assert!(db::cas_ref(c, repo, "main", version, id)?);
            Ok(id)
        })
        .unwrap()
}
