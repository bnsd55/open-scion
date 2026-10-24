//! An empty token is no secret, so nothing mints its digest: an access
//! file whose line carries blake3 of the empty string is refused at
//! parse, `hash-token` refuses empty and whitespace-only input, and a
//! server whose access file names the empty digest does not start.

use std::io::Read as _;
use std::io::Write as _;
use std::process::{Command, Stdio};

use scion_proto::runtime::auth::token_digest;
use scion_server::Access;

/// The digest an operator must never be able to put in an access file.
fn empty_digest() -> String {
    blake3::Hash::from(token_digest("")).to_hex().to_string()
}

/// The digest of `token`, as an access file line wants it.
fn expected_digest(token: &str) -> String {
    blake3::Hash::from(token_digest(token)).to_hex().to_string()
}

#[test]
fn an_access_file_naming_the_empty_digest_is_refused() {
    let text = format!("token\tci\t{}\nread\tci\tmain\n", empty_digest());
    let error = Access::parse(&text).unwrap_err();
    assert!(
        error.contains("an empty token is no secret"),
        "the refusal names the problem: {error}"
    );
}

/// Runs `scion-server hash-token` with `args`, and returns (code, out, err).
fn hash_token(args: &[&str]) -> (bool, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["hash-token"])
        .args(args)
        .env_remove("SCION_TOKEN")
        .stdin(Stdio::null())
        .output()
        .expect("hash-token runs");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

fn write_access(dir: &std::path::Path, digest: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let access = dir.join("access");
    let mut file = std::fs::File::create(&access).unwrap();
    writeln!(file, "token\tci\t{digest}").unwrap();
    writeln!(file, "read\tci\tmain").unwrap();
    drop(file);
    std::fs::set_permissions(&access, std::fs::Permissions::from_mode(0o600)).unwrap();
    access
}

#[test]
fn hash_token_refuses_an_empty_argv_token() {
    let (ok, out, err) = hash_token(&["--token", ""]);
    assert!(!ok, "hash-token printed {out}");
    assert!(
        err.contains("no secret") || err.contains("empty") || err.contains("whitespace"),
        "the refusal names the problem: {err}"
    );
    assert_ne!(out.trim(), empty_digest(), "the empty digest leaked");
}

#[test]
fn hash_token_refuses_a_whitespace_only_argv_token() {
    let (ok, out, err) = hash_token(&["--token", "   \t "]);
    assert!(!ok, "hash-token printed {out}");
    assert!(
        err.contains("whitespace") || err.contains("no secret"),
        "the refusal names the problem: {err}"
    );
}

/// The token bytes a leading-space token keeps: the server started with
/// `SCION_TOKEN`=' abc' answers a client that sends bearer ' abc'.
#[test]
fn a_server_started_with_a_leading_space_token_answers_its_own_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    // A port the OS says is free: grab one and give it to the server,
    // so a stale listener from an earlier run cannot block the start.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args([
            "--data",
            data.to_str().unwrap(),
            "serve",
            "--listen",
            &format!("127.0.0.1:{port}"),
        ])
        .env("SCION_TOKEN", " abc")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the server starts");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let alive = child.try_wait().expect("the wait works").is_none();
    if !alive {
        let mut err = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut err)
            .unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        panic!("the server refused a leading-space token at start: {err}");
    }
    // A real authed call from a client that sends the same bytes. The
    // scn binary sits in the same target tree as this test's own binary,
    // under the profile dir the build made.
    let target = std::env::current_exe()
        .unwrap()
        .ancestors()
        .nth(3)
        .unwrap()
        .to_path_buf();
    let scn = [
        target.join("debug").join("scn"),
        target.join("release").join("scn"),
    ]
    .into_iter()
    .find(|p| p.exists())
    .expect("the scn binary is built");
    let out = Command::new(scn)
        .args([
            "--server",
            &format!("http://127.0.0.1:{port}"),
            "ls",
            "main",
        ])
        .env("SCION_TOKEN", " abc")
        .output()
        .expect("scn runs");
    child.kill().unwrap();
    child.wait().unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        !err.contains("valid authentication credentials"),
        "the server did not answer the client's own bytes: {err}"
    );
}

/// hash-token keeps the bytes it refused to trim: ' abc' hashes as
/// blake3(' abc'), not blake3('abc').
#[test]
fn hash_token_keeps_a_leading_space() {
    let (ok, out, err) = hash_token(&["--token", " abc"]);
    assert!(ok, "hash-token refused a leading-space token: {err}");
    assert_eq!(
        out.trim(),
        expected_digest(" abc"),
        "the digest must be of the bytes the client will send"
    );
    assert_ne!(out.trim(), expected_digest("abc"));
}

/// A server whose access file names the empty digest does not start.
#[test]
fn a_server_whose_access_file_names_the_empty_digest_does_not_start() {
    let dir = tempfile::tempdir().unwrap();
    let access = write_access(dir.path(), &empty_digest());

    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args([
            "--data",
            dir.path().join("data").to_str().unwrap(),
            "serve",
            "--access",
            access.to_str().unwrap(),
        ])
        .env_remove("SCION_TOKEN")
        .output()
        .expect("the server runs");
    assert!(
        !out.status.success(),
        "the server started with the empty-token digest in its access file"
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("an empty token is no secret"),
        "the refusal names the problem: {err}"
    );
}
