//! D25: a missing data directory is refused, not created. Every
//! subcommand that touches a store - serve, fsck, gc, import, export,
//! derive, ticket-key - exits with the refusal and creates nothing,
//! whether the directory came from `--data`, `SCION_DATA` or the
//! default. `restore` is the one command that still creates its
//! target, and with the directory present every command works as
//! today. Red before the rule: fsck and serve created `./scion-data`
//! and proceeded (fsck on a typo'd path reported a clean empty store).

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;

/// Runs the binary with `args` from a working directory that has no
/// data directory in it.
fn run(cwd: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(args)
        .current_dir(cwd)
        .env("NO_COLOR", "1")
        .env_remove("SCION_DATA")
        .output()
        .unwrap()
}

fn refuses(cwd: &Path, args: &[&str]) {
    let out = run(cwd, args);
    assert_ne!(out.status.code(), Some(0), "{args:?} proceeded");
    assert!(
        !cwd.join("scion-data").exists(),
        "{args:?} created ./scion-data"
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("does not exist; create it or pass --data"),
        "{args:?} did not name the refusal"
    );
}

#[test]
fn a_missing_data_directory_is_refused_not_created() {
    // The default `scion-data` in the cwd: every store-touching
    // subcommand refuses and creates nothing.
    let cwd = tempfile::tempdir().unwrap();
    let empty = cwd.path();
    refuses(empty, &["fsck"]);
    refuses(empty, &["gc"]);
    refuses(empty, &["export", "--repo", "r", "--out", "out"]);
    refuses(empty, &["derive", "--kind", "root_tree"]);
    refuses(empty, &["ticket-key", "--out", "k.pub"]);
    // fsck's refusal is its "could not run" exit, 2.
    let out = run(empty, &["fsck"]);
    assert_eq!(out.status.code(), Some(2), "fsck's refusal exits 2");
    // serve refuses before it binds anything: the auth flags pass (a
    // token file that exists), so the data directory is the gate that
    // fires.
    let token = empty.join("t.txt");
    std::fs::write(&token, "tok\n").unwrap();
    // The token file must be 0600: the open checks refuse looser files.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let out = run(empty, &["serve", "--token-file", token.to_str().unwrap()]);
    assert_ne!(out.status.code(), Some(0), "serve must not serve");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("does not exist; create it or pass --data"),
        "serve names the refusal"
    );
    assert!(!empty.join("scion-data").exists(), "serve created nothing");
    // SCION_DATA counts as the path too.
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("fsck")
        .env("NO_COLOR", "1")
        .env("SCION_DATA", empty.join("elsewhere"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("elsewhere does not exist"),
        "the refusal names the SCION_DATA path"
    );
    assert!(!empty.join("elsewhere").exists());
}

#[test]
fn an_existing_data_directory_works_as_today_and_restore_still_creates() {
    let cwd = tempfile::tempdir().unwrap();
    let data = cwd.path().join("scion-data");
    std::fs::create_dir_all(&data).unwrap();
    let bin = env!("CARGO_BIN_EXE_scion-server");
    // With the directory present, every refused command works as today.
    let out = Command::new(bin)
        .args(["--data"])
        .arg(&data)
        .arg("fsck")
        .env("NO_COLOR", "1")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "fsck runs on the existing dir");
    let key = cwd.path().join("k.pub");
    let out = Command::new(bin)
        .args(["--data"])
        .arg(&data)
        .args(["ticket-key", "--out"])
        .arg(&key)
        .env("NO_COLOR", "1")
        .output()
        .unwrap();
    assert!(out.status.success(), "ticket-key runs: {}", out.status);
    assert!(key.exists());
    // `restore` is the one command that still creates its target.
    let archive = make_archive(cwd.path());
    let target = cwd.path().join("made-by-restore");
    let out = Command::new(bin)
        .args(["--data"])
        .arg(&target)
        .args(["restore", "--archive"])
        .arg(&archive)
        .env("NO_COLOR", "1")
        .output()
        .unwrap();
    assert!(out.status.success(), "restore runs: {}", out.status);
    assert!(target.is_dir(), "restore created its target");
}

/// Exports the smallest archive, from a store built beside `into`.
fn make_archive(into: &Path) -> PathBuf {
    use scion_core::{Changeset, FileState, IdentityTime, RepoPath};
    use scion_server::test_support::meta::{Meta as _, MetaTx as _};
    use scion_server::test_support::tree;

    let repo_dir = tempfile::tempdir_in(into).unwrap();
    let store = scion_server::Backend::open_test(repo_dir.path()).unwrap();
    store.blobs().put(b"a\n").unwrap();
    let repo = store.write(|c| c.create_repo("golden")).unwrap();
    let cs = Changeset::new(
        vec![],
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"m\n".to_vec(),
        std::collections::BTreeMap::from([(
            RepoPath::new(b"a".to_vec()).expect("the path fits the format"),
            Some(FileState::Regular {
                content: scion_core::BlobId::of(b"a\n"),
                executable: false,
            }),
        )]),
    )
    .expect("changeset fits the format");
    let tip = store
        .write(|c| tree::store_changeset(c, repo, &cs, &mut |_| Ok(0)).map(|r| r.0))
        .unwrap();
    store.write(|c| c.cas_ref(repo, "main", None, tip)).unwrap();
    let out = into.join("archive");
    scion_server::archive::export(&store, dir_path(repo_dir.path()), None, true, &out).unwrap();
    out
}

/// The data directory the store sits in (the tempdir's own path).
fn dir_path(dir: &Path) -> &Path {
    dir
}
