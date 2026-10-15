//! `scn checkout` through the cache tiers: scion-server, then a scion-cache,
//! then a scion-daemon on a Unix socket, all in this process.
#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::auth::Bearer;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::{GetObjectsRequest, ObjectKind, ObjectRef};
use scion_server::{Store, db, service, tree};
use scion_tier::{
    CACHE_UPSTREAM_TIMEOUT, DAEMON_UPSTREAM_TIMEOUT, DiskStore, MemoryStore, Tier, bind_unix,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::Code;
use tonic::transport::Endpoint;

const REPO: &str = "r";
const TOKEN: &str = "secret";

/// A server, a cache in front of it, and a daemon in front of the cache.
/// Dropping it stops all three.
struct Tiers {
    store: Store,
    server: String,
    cache: Tier<MemoryStore>,
    daemon: Tier<DiskStore>,
    /// `unix://` address of the daemon.
    socket: String,
    runtime: Runtime,
    _dirs: [TempDir; 2],
}

impl Tiers {
    fn start(data: TempDir, store: Store) -> Self {
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
            daemon,
            socket: format!("unix://{}", path.display()),
            runtime,
            _dirs: [data, node],
        }
    }

    /// Calls past the daemon, and calls that reached the server.
    fn upstream_calls(&self) -> (u64, u64) {
        (self.daemon.upstream_calls(), self.cache.upstream_calls())
    }
}

/// `scn checkout REV --into dest` against the server at `url`.
fn checkout(url: &str, rev: &str, dest: &Path) {
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(["--server", url, "--repo", REPO, "checkout", rev, "--into"])
        .arg(dest)
        .env("SCION_TOKEN", TOKEN)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{url}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// What a checkout holds at one path.
#[derive(Debug, PartialEq, Eq)]
enum Node {
    Dir,
    File { data: Vec<u8>, mode: u32 },
    Symlink(PathBuf),
}

/// Everything under `root`, by path relative to it. Symlinks are not followed.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Node> {
    let mut nodes = BTreeMap::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let meta = fs::symlink_metadata(&path).unwrap();
            let node = if meta.is_symlink() {
                Node::Symlink(fs::read_link(&path).unwrap())
            } else if meta.is_dir() {
                dirs.push(path.clone());
                Node::Dir
            } else {
                Node::File {
                    data: fs::read(&path).unwrap(),
                    mode: meta.permissions().mode() & 0o777,
                }
            };
            nodes.insert(path.strip_prefix(root).unwrap().to_path_buf(), node);
        }
    }
    nodes
}

/// A changeset on `parent` that puts `files` (raw path and state).
fn changeset(parent: Option<ChangesetId>, files: &[(&str, FileState)]) -> Changeset {
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
fn land(store: &Store, cs: &Changeset) -> ChangesetId {
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

#[test]
fn checkout_through_the_daemon_and_the_cache_matches_a_direct_checkout() {
    let data = tempfile::tempdir().unwrap();
    let store = Store::open(data.path()).unwrap();
    let regular = |content: &[u8], executable| FileState::Regular {
        content: store.blobs().put(content).unwrap(),
        executable,
    };
    let readme = regular(b"hello\n", false);
    let first = changeset(
        None,
        &[
            ("README.md", readme),
            ("run.sh", regular(b"#!/bin/sh\necho hi\n", true)),
            ("src/lib.rs", regular(b"pub fn f() {}\n", false)),
            ("src/nested/deep/file.txt", regular(b"deep\n", false)),
            // The same blob as README.md.
            ("src/nested/copy.txt", readme),
            (
                "link",
                FileState::Symlink {
                    target: store.blobs().put(b"README.md").unwrap(),
                },
            ),
        ],
    );
    let first = land(&store, &first);
    // For a later change. A blob is not part of any tree until a changeset
    // names it.
    let new_readme = regular(b"hello, again\n", false);
    let tiers = Tiers::start(data, store);
    let tmp = tempfile::tempdir().unwrap();
    let dest = |name: &str| tmp.path().join(name);

    checkout(&tiers.socket, "main", &dest("through-tiers"));
    checkout(&tiers.server, "main", &dest("direct"));
    let direct = snapshot(&dest("direct"));
    assert_eq!(direct.len(), 9, "{direct:?}");
    assert_eq!(snapshot(&dest("through-tiers")), direct);
    // The first checkout missed at each tier: calls went up to the server.
    let before = tiers.upstream_calls();
    assert!(before.0 > 0 && before.1 > 0, "{before:?}");

    // A second checkout on the node sends nothing past the daemon.
    checkout(&tiers.socket, &first.to_string(), &dest("again"));
    assert_eq!(snapshot(&dest("again")), direct);
    assert_eq!(tiers.upstream_calls(), before);

    // A ref is always asked of the server, so the tiers follow `main` when it
    // moves.
    let second = changeset(Some(first), &[("README.md", new_readme)]);
    land(&tiers.store, &second);
    checkout(&tiers.socket, "main", &dest("moved"));
    let moved = fs::read(dest("moved").join("README.md")).unwrap();
    assert_eq!(moved, b"hello, again\n");
    assert_eq!(
        fs::read(dest("moved").join("run.sh")).unwrap(),
        b"#!/bin/sh\necho hi\n"
    );
}

#[test]
fn a_missing_repo_fails_the_call_once_at_each_tier() {
    let data = tempfile::tempdir().unwrap();
    let store = Store::open(data.path()).unwrap();
    let tiers = Tiers::start(data, store);
    let refs = (0..3u8)
        .map(|i| ObjectRef {
            kind: ObjectKind::Blob.into(),
            id: BlobId::of(&[i]).as_bytes().to_vec(),
        })
        .collect();
    let request = GetObjectsRequest {
        repo: "no-such-repo".into(),
        refs,
    };

    let reply = tiers.runtime.block_on(async {
        let channel = Endpoint::from_shared(tiers.socket.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut daemon = ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap());
        daemon.get_objects(request).await
    });
    // The call itself fails, as it does on the server, not one object of it.
    let Err(status) = reply else {
        panic!("the call started a stream");
    };
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    // One call past the daemon and one to the server: no tier asked again
    // once per object.
    assert_eq!(tiers.upstream_calls(), (1, 1));
}

#[test]
fn scn_refuses_a_unix_address_that_is_not_a_socket() {
    let tmp = tempfile::tempdir().unwrap();
    let socket = tmp.path().join("real.sock");
    // A socket file that nothing listens on, so a connect fails at once
    // instead of hanging if the check lets it through.
    drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
    // A symlink to a real socket, and a plain file.
    let link = tmp.path().join("link.sock");
    std::os::unix::fs::symlink(&socket, &link).unwrap();
    let file = tmp.path().join("file.sock");
    fs::write(&file, b"").unwrap();

    for path in [&link, &file] {
        let url = format!("unix://{}", path.display());
        let out = Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["--server", &url, "--repo", REPO, "log"])
            .env("SCION_TOKEN", TOKEN)
            .output()
            .unwrap();
        assert!(!out.status.success());
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("it is not a socket"), "{err}");
    }
}
