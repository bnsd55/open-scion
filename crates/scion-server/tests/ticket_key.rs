//! The ticket key files: the seed is written 0600 and once, and read only
//! when private; a public key is read by anyone, but written by its owner.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::{Command, Output};

/// Runs `scion-server` with `args` after the global `--data`, and gives
/// its output. The data directory is the test's own: the command's
/// audit log lives there, and the working directory's default ACL on CI
/// would refuse the default path.
fn run(data: &Path, args: &[&std::ffi::OsStr]) -> Output {
    // D25: the binary refuses a missing data directory; a test that
    // wants the command to run makes the directory itself.
    fs::create_dir_all(data).unwrap();
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data)
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
fn ticket_key_prints_one_line_of_64_hex() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let out = run(
        &data,
        &[
            "ticket-key".as_ref(),
            "--out".as_ref(),
            dir.path().join("seed.key").as_os_str(),
        ],
    );
    assert!(out.status.success());
    // stdout carries the command's output and nothing else: callers take
    // the key with $(...). The logs, if any, go to stderr.
    let text = String::from_utf8_lossy(&out.stdout);
    let mut lines = text.lines();
    let line = lines.next().expect("a public key line");
    assert_eq!(line.len(), 64, "{line:?}");
    assert!(line.chars().all(|c| c.is_ascii_hexdigit()), "{line:?}");
    assert_eq!(lines.next(), None, "stdout is one line");
    assert!(
        out.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn a_ticket_key_file_is_private_and_made_once() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let key = dir.path().join("ticket.key");

    let out = run(
        &data,
        &["ticket-key".as_ref(), "--out".as_ref(), key.as_os_str()],
    );
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
    let out = run(
        &data,
        &["ticket-key".as_ref(), "--out".as_ref(), key.as_os_str()],
    );
    assert!(!out.status.success(), "the second run replaced the key");
    assert!(fs::read(&key).unwrap() == seed, "the key changed");
}

#[test]
fn serve_refuses_a_seed_that_group_or_others_can_read() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let key = dir.path().join("ticket.key");
    let out = run(
        &data,
        &["ticket-key".as_ref(), "--out".as_ref(), key.as_os_str()],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 0644: everyone on the machine can read the seed, so serve refuses it.
    fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap();
    let args: Vec<std::ffi::OsString> = [
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
    let out = run(
        &data,
        &args
            .iter()
            .map(std::ffi::OsString::as_os_str)
            .collect::<Vec<_>>(),
    );
    assert!(!out.status.success(), "serve accepted a 0644 seed");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("group or others"), "{err}");

    // Back to 0600, the server starts with the key. D25: the operator
    // makes this data directory (0700) before the serve.
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    let data2 = dir.path().join("data2");
    fs::create_dir_all(&data2).unwrap();
    fs::set_permissions(&data2, fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args([
            "--data",
            data2.to_str().unwrap(),
            "serve",
            "--token",
            "t",
            "--listen",
            "127.0.0.1:0",
            "--ticket-key",
            key.to_str().unwrap(),
        ])
        .env("NO_COLOR", "1")
        // The info lines go to stderr, and the test waits for the one that
        // says the server listens.
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(dir.path().join("serve.log")).unwrap())
        .spawn()
        .unwrap();
    wait_serving(&mut child, &dir.path().join("serve.log"));
    assert!(
        child.try_wait().unwrap().is_none(),
        "serve exited once it listened"
    );
    let _ = child.kill();
    let _ = child.wait();
}

/// Waits until the spawned `serve` prints that it listens, with the log to
/// prove it. A refusal exits at once, and the loop answers with its log;
/// a good server passes as soon as it listens, not after a fixed sleep.
fn wait_serving(child: &mut std::process::Child, log: &Path) {
    let started = std::time::Instant::now();
    loop {
        let text = fs::read_to_string(log).unwrap_or_default();
        if text.contains("serving") {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("serve exited before it listened ({status}): {text}");
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "serve never listened: {text}"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// A file of `text`, with its mode set.
fn write_with_mode(path: &Path, text: &str, mode: u32) {
    fs::write(path, text).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

/// The public key of a fresh seed file: what `ticket-key` prints.
fn public_key_of(dir: &Path) -> String {
    let key = dir.join("seed.key");
    let out = run(
        &dir.join("data"),
        &["ticket-key".as_ref(), "--out".as_ref(), key.as_os_str()],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let hex = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    // The seed file is made 0600; the public half rides in the output.
    assert_eq!(mode_of(&key) & 0o777, 0o600);
    hex
}

/// `serve` with the seed and the old public key of `mode`.
fn serve_with_old(dir: &Path, seed: &Path, old: &Path) -> Output {
    let args: Vec<std::ffi::OsString> = [
        "serve",
        "--token",
        "t",
        "--listen",
        "127.0.0.1:0",
        "--ticket-key",
        seed.to_str().unwrap(),
        "--ticket-key-old",
        old.to_str().unwrap(),
    ]
    .iter()
    .map(std::ffi::OsString::from)
    .collect();
    run(
        &dir.join("data"),
        &args
            .iter()
            .map(std::ffi::OsString::as_os_str)
            .collect::<Vec<_>>(),
    )
}

#[test]
fn an_old_public_key_is_readable_by_anyone_but_writable_by_its_owner_alone() {
    let dir = tempfile::tempdir().unwrap();
    // public_key_of makes dir/seed.key (0600) and prints its public half.
    let hex = public_key_of(dir.path());
    let seed = dir.path().join("seed.key");
    let old = dir.path().join("old.pub");

    // 0644: a public key is not a secret. Every reader on the machine may
    // hold it, so serve starts, and stays up.
    write_with_mode(&old, &hex, 0o644);
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args([
            "--data",
            dir.path().join("data").to_str().unwrap(),
            "serve",
            "--token",
            "t",
            "--listen",
            "127.0.0.1:0",
            "--ticket-key",
            seed.to_str().unwrap(),
            "--ticket-key-old",
            old.to_str().unwrap(),
        ])
        .env("NO_COLOR", "1")
        // The info lines go to stderr, and the test waits for the one that
        // says the server listens.
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(dir.path().join("serve.log")).unwrap())
        .spawn()
        .unwrap();
    wait_serving(&mut child, &dir.path().join("serve.log"));
    assert!(
        child.try_wait().unwrap().is_none(),
        "serve exited once it listened"
    );
    let _ = child.kill();
    let _ = child.wait();

    // 0666: anyone on the machine can change the key, so serve refuses it.
    write_with_mode(&old, &hex, 0o666);
    let out = serve_with_old(dir.path(), &seed, &old);
    assert!(!out.status.success(), "serve accepted a 0666 public key");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("written by group or others"), "{err}");

    // The seed stays under its own rule: 0644 on the seed is still refused.
    fs::set_permissions(&seed, fs::Permissions::from_mode(0o644)).unwrap();
    write_with_mode(&old, &hex, 0o600);
    let out = serve_with_old(dir.path(), &seed, &old);
    assert!(!out.status.success(), "serve accepted a 0644 seed");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("read by group or others"), "{err}");
}
