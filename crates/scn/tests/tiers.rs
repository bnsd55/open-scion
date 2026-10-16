//! `scn checkout` through the cache tiers: scion-server, then a scion-cache,
//! then a scion-daemon on a Unix socket, all in this process.
#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use scion_core::{BlobId, FileState};
use scion_proto::v1::GetObjectsRequest;
use scion_server::Store;
use tonic::Code;

mod common;
use common::{REPO, TOKEN, Tiers, blob_path, blob_ref, changeset, connect, land};

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
    /// A file, by the ID of its content, so a failure prints no content.
    File {
        content: BlobId,
        mode: u32,
    },
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
                    content: BlobId::of(&fs::read(&path).unwrap()),
                    mode: meta.permissions().mode() & 0o777,
                }
            };
            nodes.insert(path.strip_prefix(root).unwrap().to_path_buf(), node);
        }
    }
    nodes
}

#[test]
fn checkout_through_the_daemon_and_the_cache_matches_a_direct_checkout() {
    let data = tempfile::tempdir().unwrap();
    let store = Store::open(data.path()).unwrap();
    let (files, big) = common::files(&store);
    let first = land(&store, &changeset(None, &files));
    // For a later change. A blob is not part of any tree until a changeset
    // names it.
    let new_readme = FileState::Regular {
        content: store.blobs().put(b"hello, again\n").unwrap(),
        executable: false,
    };
    // The server's file of `big`, packed at another level than a tier would
    // pack it, so a tier that packs again is seen.
    let packed = zstd::bulk::compress(&big, 1).unwrap();
    fs::write(blob_path(data.path(), BlobId::of(&big)), packed).unwrap();
    let tiers = Tiers::start(data, store);
    let tmp = tempfile::tempdir().unwrap();
    let dest = |name: &str| tmp.path().join(name);

    checkout(&tiers.socket, "main", &dest("through-tiers"));
    checkout(&tiers.server, "main", &dest("direct"));
    let direct = snapshot(&dest("direct"));
    assert_eq!(direct.len(), 11, "{direct:?}");
    assert_eq!(snapshot(&dest("through-tiers")), direct);
    let written = fs::read(dest("through-tiers").join("big.bin")).unwrap();
    assert_eq!(BlobId::of(&written), BlobId::of(&big));
    // The daemon keeps each blob in the server's disk format, byte for byte:
    // the zstd file went through the cache unchanged.
    let [on_server, on_node] = tiers.blob_files(BlobId::of(&big));
    assert!(on_server == on_node, "the daemon packed the blob again");
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
    let refs = (0..3u8).map(|i| blob_ref(BlobId::of(&[i]))).collect();
    let request = GetObjectsRequest {
        refs,
        accept: Vec::new(),
        ticket: Vec::new(),
    };

    let reply = tiers.runtime.block_on(async {
        let mut daemon = connect(&tiers.socket).await;
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
