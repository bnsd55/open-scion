//! The ticket key file: written 0600 and once, and read only when private.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::{Command, Output};

/// Runs `scion-server` with `args`, and gives its output.
fn run(args: &[&std::ffi::OsStr]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(args)
        .env("NO_COLOR", "1")
        .output()
        .unwrap()
}

/// The text of the file, with its mode bits.
fn mode_of(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode()
}

#[test]
fn a_ticket_key_file_is_private_and_made_once() {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("ticket.key");

    let out = run(&["ticket-key".as_ref(), "--out".as_ref(), key.as_os_str()]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // 0600: the seed mints permissions, so only its owner reads it.
    assert_eq!(mode_of(&key) & 0o777, 0o600);
    let seed = fs::read(&key).unwrap();
    assert_eq!(seed.len(), 65, "64 hex characters and a newline");

    // A second run to the same path fails, and keeps the first key: a
    // silent change breaks every tier that was given the old public half.
    let out = run(&["ticket-key".as_ref(), "--out".as_ref(), key.as_os_str()]);
    assert!(!out.status.success(), "the second run replaced the key");
    assert!(fs::read(&key).unwrap() == seed, "the key changed");
}

#[test]
fn serve_refuses_a_seed_that_group_or_others_can_read() {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("ticket.key");
    let out = run(&["ticket-key".as_ref(), "--out".as_ref(), key.as_os_str()]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 0644: everyone on the machine can read the seed, so serve refuses it.
    fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap();
    let args: Vec<std::ffi::OsString> = [
        "--data",
        dir.path().join("data").to_str().unwrap(),
        "serve",
        "--token",
        "t",
        "--listen",
        "127.0.0.1:0",
        "--ticket-key",
    ]
    .iter()
    .map(std::ffi::OsString::from)
    .chain(std::iter::once(key.as_os_str().to_owned()))
    .collect();
    let out = run(&args
        .iter()
        .map(std::ffi::OsString::as_os_str)
        .collect::<Vec<_>>());
    assert!(!out.status.success(), "serve accepted a 0644 seed");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("group or others"), "{err}");

    // Back to 0600, the server starts with the key.
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args([
            "--data",
            dir.path().join("data2").to_str().unwrap(),
            "serve",
            "--token",
            "t",
            "--listen",
            "127.0.0.1:0",
            "--ticket-key",
            key.to_str().unwrap(),
        ])
        .env("NO_COLOR", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    // A refused seed exits at once; a good one stays up. Two seconds is
    // generous for the refusal and short for a hang.
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert!(child.try_wait().unwrap().is_none(), "serve exited");
    let _ = child.kill();
    let _ = child.wait();
}
