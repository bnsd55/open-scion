//! `scn commit --from-mount` end to end: a mount's overlay through the VFS
//! API, then the commit and the diff through the `scn` binary, against the
//! in-process server ([#11](https://github.com/bnsd55/open-scion/issues/11)).

#![cfg(unix)]

use std::process::Command;

use common::{REPO, TOKEN, changeset, land};
use nfsserve::nfs::{filename3, sattr3, set_size3};
use open_scion::client::{id_from, test_objects};
use open_scion::cmd::mount::{MountRecord, write_sessions};
use open_scion::vfs::{NFSFileSystem, Vfs};
use scion_core::{ChangesetId, FileState};
use scion_proto::v1::GetRootTreeRequest;
use scion_server::Store;

mod common;

/// Runs `scn ARGS` against `url`, with `home` as the home directory.
fn scn(url: &str, home: &std::path::Path, args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(["--server", url, "--repo", REPO])
        .args(args)
        .env("SCION_TOKEN", TOKEN)
        .env("HOME", home)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// Lands `keep.txt` and `docs/{a.txt,b.txt}` on `store`, and points
/// `main` at the last changeset.
fn start(store: &Store) -> ChangesetId {
    let store = store.clone();
    let store = &store;
    let first = land(
        store,
        &changeset(
            None,
            &[(
                "keep.txt",
                FileState::Regular {
                    content: store.blobs().batch().put(b"same\n").unwrap(),
                    executable: false,
                },
            )],
        ),
    );
    let mut blobs = store.blobs().batch();
    let a = blobs.put(b"old\n").unwrap();
    let b = blobs.put(b"bye\n").unwrap();
    drop(blobs);
    land(
        store,
        &changeset(
            Some(first),
            &[
                (
                    "docs/a.txt",
                    FileState::Regular {
                        content: a,
                        executable: false,
                    },
                ),
                (
                    "docs/b.txt",
                    FileState::Regular {
                        content: b,
                        executable: false,
                    },
                ),
            ],
        ),
    )
}

/// The mounted VFS of `main`, with its overlay and session file under a
/// temp home.
struct Mount {
    vfs: Vfs,
    home: tempfile::TempDir,
    view: std::path::PathBuf,
}

/// Mounts the tree of `base` through the VFS.
fn mount(tiers: &common::Tiers, base: &ChangesetId) -> Mount {
    let objects = tiers
        .runtime
        .block_on(async { test_objects(&tiers.server, TOKEN).await });
    let reply = tiers
        .runtime
        .block_on(async {
            objects
                .clone()
                .get_root_tree(GetRootTreeRequest {
                    repo: REPO.to_owned(),
                    changeset_id: base.as_bytes().to_vec(),
                })
                .await
        })
        .unwrap()
        .into_inner();
    let root = id_from(&reply.tree_id).unwrap();

    let home = tempfile::tempdir().unwrap();
    // The mount's checkout lives in `view`; the changes go to the scratch
    // overlay, under its `tree`.
    let view = home.path().join("view");
    std::fs::create_dir_all(&view).unwrap();
    let overlay = home.path().join("overlay");
    let vfs = Vfs::new(objects, REPO.to_owned(), root, overlay.join("tree"));
    // The session file the commit reads, under the home of the `scn` runs.
    std::fs::create_dir_all(home.path().join(".scion")).unwrap();
    write_sessions(
        &home.path().join(".scion").join("mounts.json"),
        &[MountRecord {
            dir: view.clone(),
            overlay: overlay.clone(),
            rev: "main".to_owned(),
            pid: std::process::id(),
        }],
    )
    .unwrap();
    Mount { vfs, home, view }
}

#[test]
fn a_mounts_overlay_commits_without_a_scan() {
    let data = tempfile::tempdir().unwrap();
    let store = Store::open(data.path()).unwrap();
    let base = start(&store);
    let tiers = common::Tiers::start(data, store);
    let mounted = mount(&tiers, &base);
    let vfs = &mounted.vfs;

    // Through the VFS: edit a file of the ref, add one, delete one, and
    // rename the directory of the ref, with files in it.
    tiers.runtime.block_on(async {
        let root_id = NFSFileSystem::root_dir(vfs);
        let docs = vfs
            .lookup(root_id, &filename3::from(b"docs".to_vec()))
            .await
            .unwrap();
        let a_id = vfs
            .lookup(docs, &filename3::from(b"a.txt".to_vec()))
            .await
            .unwrap();
        vfs.setattr(
            a_id,
            sattr3 {
                size: set_size3::size(0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        vfs.write(a_id, 0, b"edited\n").await.unwrap();
        let (new_id, _) = vfs
            .create(
                docs,
                &filename3::from(b"new.txt".to_vec()),
                sattr3::default(),
            )
            .await
            .unwrap();
        vfs.write(new_id, 0, b"added\n").await.unwrap();
        vfs.remove(docs, &filename3::from(b"b.txt".to_vec()))
            .await
            .unwrap();
        vfs.rename(
            root_id,
            &filename3::from(b"docs".to_vec()),
            root_id,
            &filename3::from(b"docs2".to_vec()),
        )
        .await
        .unwrap();
    });

    // The commit takes the changes from the overlay, with no scan of the
    // ref.
    let cs = scn(
        &tiers.server,
        mounted.home.path(),
        &[
            "commit",
            "-m",
            "from the mount",
            "--from-mount",
            mounted.view.to_str().unwrap(),
        ],
    );
    assert_eq!(cs.len(), 64, "the commit printed no changeset: {cs}");

    // The diff: the old paths are deletions, the new ones additions, and
    // `keep.txt` is not in it.
    let diff = scn(&tiers.server, mounted.home.path(), &["diff", "main", &cs]);
    let mut lines: Vec<String> = diff.lines().map(str::to_owned).collect();
    lines.sort();
    assert_eq!(
        lines,
        [
            "A\tdocs2/a.txt",
            "A\tdocs2/new.txt",
            "D\tdocs/a.txt",
            "D\tdocs/b.txt",
        ],
        "{diff}"
    );
    // The contents came along.
    assert_eq!(
        scn(
            &tiers.server,
            mounted.home.path(),
            &["cat", &format!("{cs}:docs2/a.txt")]
        ),
        "edited"
    );
    assert_eq!(
        scn(
            &tiers.server,
            mounted.home.path(),
            &["cat", &format!("{cs}:docs2/new.txt")]
        ),
        "added"
    );
}
