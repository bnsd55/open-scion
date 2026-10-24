//! `scn commit --from-mount` end to end: a mount's overlay through the VFS
//! API, then the commit and the diff through the `scn` binary, against the
//! in-process server ([#11](https://github.com/bnsd55/openscion/issues/11)).

#![cfg(unix)]

use std::process::Command;

use common::{REPO, TOKEN, changeset, land};
#[cfg(feature = "unstable-writable-mount")]
use nfsserve::nfs::set_size3;
use nfsserve::nfs::{filename3, nfspath3, nfsstat3, sattr3};
use open_scion::client::id_from;
use open_scion::test_support::{
    client::test_objects,
    cmd::mount::{MountRecord, write_sessions},
    vfs::{NFSFileSystem, Vfs},
};
use scion_core::{ChangesetId, FileState};
use scion_proto::v1::GetRootTreeRequest;
use scion_server::Backend;

mod common;

/// Runs `scn ARGS` against `url`, with `home` as the home directory.
#[cfg(feature = "unstable-writable-mount")]
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
fn start(store: &Backend) -> ChangesetId {
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

/// Mounts the tree of `base` through the VFS, with the writes a
/// `--unstable-writable` mount takes. A build without the feature
/// compiles its callers out, so the helper goes with them.
#[cfg(feature = "unstable-writable-mount")]
fn mount(tiers: &common::Tiers, base: &ChangesetId) -> Mount {
    mount_as(tiers, base, true)
}

/// Mounts as `--unstable-writable` would (`writable`) or not.
fn mount_as(tiers: &common::Tiers, base: &ChangesetId, writable: bool) -> Mount {
    let (objects, roots, reply) = tiers.runtime.block_on(async {
        let objects = test_objects(&tiers.server, TOKEN).await;
        // The refresh of the ticket goes to the server, like a mount's.
        let roots = test_objects(&tiers.server, TOKEN).await;
        let reply = objects
            .clone()
            .get_root_tree(GetRootTreeRequest {
                repo: REPO.to_owned(),
                changeset_id: base.as_bytes().to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        (objects, roots, reply)
    });
    let root = id_from(&reply.tree_id).unwrap();

    let home = tempfile::tempdir().unwrap();
    // The mount's checkout lives in `view`; the changes go to the scratch
    // overlay, under its `tree`.
    let view = home.path().join("view");
    std::fs::create_dir_all(&view).unwrap();
    let overlay = home.path().join("overlay");
    let vfs = Vfs::new(
        objects,
        roots,
        REPO.to_owned(),
        *base,
        root,
        &reply.ticket,
        overlay.join("tree"),
    );
    // A build without the feature has no write path: only its read-only
    // tests run, and the record still says so.
    #[cfg(feature = "unstable-writable-mount")]
    let vfs = vfs.writable(writable);
    // The session file the commit reads, under the home of the `scn` runs.
    std::fs::create_dir_all(home.path().join(".scion")).unwrap();
    write_sessions(
        &home.path().join(".scion").join("mounts.json"),
        &[MountRecord {
            dir: view.clone(),
            overlay: overlay.clone(),
            rev: "main".to_owned(),
            pid: std::process::id(),
            writable,
        }],
    )
    .unwrap();
    Mount { vfs, home, view }
}

#[cfg(feature = "unstable-writable-mount")]
#[test]
fn a_mounts_overlay_commits_without_a_scan() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
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

/// A name that is not UTF-8 commits its own content, not that of the
/// file its lossy spelling names. Linux filesystems hold such names;
/// macOS's refuse them at create time, so there is nothing to commit.
#[cfg(target_os = "linux")]
#[cfg(feature = "unstable-writable-mount")]
#[test]
fn a_non_utf8_name_commits_its_own_content() {
    use open_scion::client::Ctx;
    use scion_core::{Changeset, RepoPath};
    use scion_proto::v1::GetChangesetRequest;

    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    let base = start(&store);
    let tiers = common::Tiers::start(data, store.clone());
    let mounted = mount(&tiers, &base);
    let vfs = &mounted.vfs;

    // Through the VFS: a file named with an invalid byte, and one whose
    // name is that byte's lossy spelling (U+FFFD).
    let raw = b"a\xffb";
    let lossy = String::from_utf8_lossy(raw).into_owned();
    assert_ne!(lossy.as_bytes(), &raw[..]);
    tiers.runtime.block_on(async {
        let root = NFSFileSystem::root_dir(vfs);
        let (id, _) = vfs
            .create(root, &filename3::from(raw.to_vec()), sattr3::default())
            .await
            .unwrap();
        vfs.write(id, 0, b"raw\n").await.unwrap();
        let (id, _) = vfs
            .create(
                root,
                &filename3::from(lossy.clone().into_bytes()),
                sattr3::default(),
            )
            .await
            .unwrap();
        vfs.write(id, 0, b"lossy\n").await.unwrap();
    });

    let cs = scn(
        &tiers.server,
        mounted.home.path(),
        &[
            "commit",
            "-m",
            "raw names",
            "--from-mount",
            mounted.view.to_str().unwrap(),
        ],
    );
    assert_eq!(cs.len(), 64, "the commit printed no changeset: {cs}");

    // Each name carries its own content. The CLI's cat cannot take a
    // non-UTF-8 name (clap reads String), so the changeset is read from
    // the server's RPC and its blobs from the store.
    let mut ctx = tiers
        .runtime
        .block_on(async {
            Ctx::connect(&tiers.server, None, TOKEN, None, REPO.to_owned(), false).await
        })
        .unwrap();
    let id: ChangesetId = cs.parse().unwrap();
    let mut reply = tiers
        .runtime
        .block_on(async {
            ctx.client
                .get_changeset(GetChangesetRequest {
                    repo: REPO.to_owned(),
                    changeset_id: id.as_bytes().to_vec(),
                })
                .await
        })
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    tiers.runtime.block_on(async {
        while let Some(part) = reply.message().await.unwrap() {
            bytes.extend_from_slice(&part.changeset);
        }
    });
    let committed = Changeset::decode(&bytes).unwrap();
    let state = |name: &[u8]| {
        committed
            .changes()
            .get(&RepoPath::new(name.to_vec()).unwrap())
            .and_then(|state| state.as_ref())
    };
    let blob = |state: Option<&FileState>| match state {
        Some(FileState::Regular { content, .. }) => store.blobs().get(*content).unwrap(),
        _ => panic!("not a regular file: {state:?}"),
    };
    assert_eq!(blob(state(raw)), b"raw\n");
    assert_eq!(blob(state(lossy.as_bytes())), b"lossy\n");
}

/// A mount without `--unstable-writable` answers `EROFS` to every
/// mutating call the NFS trait has, before it looks at its arguments,
/// and its reads still work.
#[test]
fn a_read_only_mount_answers_rofs_to_every_write() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    let base = start(&store);
    let tiers = common::Tiers::start(data, store.clone());
    let mounted = mount_as(&tiers, &base, false);
    let vfs = &mounted.vfs;

    tiers.runtime.block_on(async {
        let root = NFSFileSystem::root_dir(vfs);
        // A read still works: the mount is read-only, not dark.
        let id = vfs
            .lookup(root, &filename3::from(b"keep.txt".to_vec()))
            .await
            .unwrap();
        let (bytes, _) = vfs.read(id, 0, 5).await.unwrap();
        assert_eq!(bytes, b"same\n");

        // Every mutating call the VFS implements is refused.
        assert!(
            matches!(
                vfs.setattr(id, sattr3::default()).await.unwrap_err(),
                nfsstat3::NFS3ERR_ROFS
            ),
            "setattr"
        );
        assert!(
            matches!(
                vfs.write(id, 0, b"x").await.unwrap_err(),
                nfsstat3::NFS3ERR_ROFS
            ),
            "write"
        );
        assert!(
            matches!(
                vfs.create(root, &filename3::from(b"n".to_vec()), sattr3::default())
                    .await
                    .unwrap_err(),
                nfsstat3::NFS3ERR_ROFS
            ),
            "create"
        );
        assert!(
            matches!(
                vfs.create_exclusive(root, &filename3::from(b"e".to_vec()))
                    .await
                    .unwrap_err(),
                nfsstat3::NFS3ERR_ROFS
            ),
            "create_exclusive"
        );
        assert!(
            matches!(
                vfs.mkdir(root, &filename3::from(b"d".to_vec()))
                    .await
                    .unwrap_err(),
                nfsstat3::NFS3ERR_ROFS
            ),
            "mkdir"
        );
        assert!(
            matches!(
                vfs.remove(root, &filename3::from(b"keep.txt".to_vec()))
                    .await
                    .unwrap_err(),
                nfsstat3::NFS3ERR_ROFS
            ),
            "remove"
        );
        assert!(
            matches!(
                vfs.rename(
                    root,
                    &filename3::from(b"keep.txt".to_vec()),
                    root,
                    &filename3::from(b"moved".to_vec())
                )
                .await
                .unwrap_err(),
                nfsstat3::NFS3ERR_ROFS
            ),
            "rename"
        );
        assert!(
            matches!(
                vfs.symlink(
                    root,
                    &filename3::from(b"lnk".to_vec()),
                    &nfspath3::from(b"x".to_vec()),
                    &sattr3::default()
                )
                .await
                .unwrap_err(),
                nfsstat3::NFS3ERR_ROFS
            ),
            "symlink"
        );
    });
}

/// `commit --from-mount` refuses a read-only mount, with the flag the
/// mount would have needed named in the error.
#[cfg(feature = "unstable-writable-mount")]
#[test]
fn a_commit_from_a_read_only_mount_names_the_flag() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    let base = start(&store);
    let tiers = common::Tiers::start(data, store.clone());
    let mounted = mount_as(&tiers, &base, false);

    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(["--server", &tiers.server, "--repo", REPO])
        .args(["commit", "-m", "no writes", "--from-mount"])
        .arg(&mounted.view)
        .env("SCION_TOKEN", TOKEN)
        .env("HOME", mounted.home.path())
        .output()
        .unwrap();
    assert!(!out.status.success(), "the commit is refused");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("--unstable-writable"),
        "the error names the flag: {err}"
    );
}

/// A build without the writable-mount feature has no write path at all:
/// the flag and the mount argument exist so the refusal names the build,
/// and every mutating NFS call is refused before it looks at its
/// arguments. This test runs only in that default build (a feature build
/// mounts for real), so it is the pin that keeps the write path out.
#[cfg(not(feature = "unstable-writable-mount"))]
mod a_default_build_has_no_writable_mount {
    use super::*;

    #[test]
    fn the_mount_flag_refuses_by_name() {
        let data = tempfile::tempdir().unwrap();
        let store = Backend::open_test(data.path()).unwrap();
        let base = start(&store);
        let tiers = common::Tiers::start(data, store.clone());
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join("view")).unwrap();
        let out = Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["--server", &tiers.server, "--repo", REPO])
            .args(["mount", "--unstable-writable", "--no-mount"])
            .arg(home.path().join("view"))
            .arg(base.to_string())
            .env("SCION_TOKEN", TOKEN)
            .env("HOME", home.path())
            .output()
            .unwrap();
        assert!(!out.status.success(), "the mount is refused");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains("no writable mount") && err.contains("unstable-writable-mount"),
            "the refusal names the build and the feature: {err}"
        );
    }

    #[test]
    fn the_commit_argument_refuses_by_name() {
        let data = tempfile::tempdir().unwrap();
        let store = Backend::open_test(data.path()).unwrap();
        let base = start(&store);
        let tiers = common::Tiers::start(data, store.clone());
        let mounted = mount_as(&tiers, &base, false);
        let out = Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["--server", &tiers.server, "--repo", REPO])
            .args(["commit", "-m", "no writes", "--from-mount"])
            .arg(&mounted.view)
            .env("SCION_TOKEN", TOKEN)
            .env("HOME", mounted.home.path())
            .output()
            .unwrap();
        assert!(!out.status.success(), "the commit is refused");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains("no writable mount") && err.contains("unstable-writable-mount"),
            "the refusal names the build and the feature: {err}"
        );
    }

    #[test]
    fn the_vfs_serves_read_only_and_refuses_every_write() {
        let data = tempfile::tempdir().unwrap();
        let store = Backend::open_test(data.path()).unwrap();
        let base = start(&store);
        let tiers = common::Tiers::start(data, store.clone());
        let mounted = mount_as(&tiers, &base, true);
        let vfs = &mounted.vfs;

        assert!(
            matches!(vfs.capabilities(), nfsserve::vfs::VFSCapabilities::ReadOnly),
            "the default build serves read-only, whatever the mount said"
        );
        tiers.runtime.block_on(async {
            let root = NFSFileSystem::root_dir(vfs);
            let id = vfs
                .lookup(root, &filename3::from(b"keep.txt".to_vec()))
                .await
                .unwrap();
            let (bytes, _) = vfs.read(id, 0, 5).await.unwrap();
            assert_eq!(bytes, b"same\n");
            assert!(
                matches!(
                    vfs.write(id, 0, b"x").await.unwrap_err(),
                    nfsstat3::NFS3ERR_ROFS
                ),
                "write"
            );
        });
    }
}
