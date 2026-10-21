//! The secret flags of the binaries: a token can arrive through a
//! checked file, the environment, stdin (`hash-token`), or the command
//! line, which the process list exposes. The file works end to end, a
//! file the group or others can read is refused, and the command line
//! prints its deprecation warning.

use std::io::{BufRead as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// The digest the `hash-token` command prints for `token`.
fn expected_digest(token: &str) -> String {
    blake3::Hash::from(scion_proto::runtime::auth::token_digest(token))
        .to_hex()
        .to_string()
}

/// Writes `text` to a file of `mode` under `dir` and returns its path.
fn write_secret(dir: &Path, name: &str, mode: u32, text: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let file = dir.join(name);
    std::fs::write(&file, text).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();
    file
}

/// Runs `scion-server hash-token` with `args`, feeds `stdin` in, and
/// returns (exit code, stdout, stderr).
fn hash_token(args: &[&str], stdin: Option<&str>, env: &[(&str, &str)]) -> (bool, String, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    command
        .args(["hash-token"])
        .args(args)
        .env_remove("SCION_TOKEN")
        // spawn() inherits the terminal's stdio by default; the test
        // reads what the child printed, so every stream is piped.
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, value) in env {
        command.env(name, value);
    }
    let mut child = command.spawn().expect("the test binary runs");
    if let Some(text) = stdin {
        let mut pipe = child.stdin.take().expect("stdin is piped");
        pipe.write_all(text.as_bytes()).unwrap();
    }
    let out = child.wait_with_output().expect("the command ends");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn a_token_file_works_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_secret(dir.path(), "token", 0o600, " the-token \n");
    let (ok, stdout, stderr) = hash_token(&["--token-file", file.to_str().unwrap()], None, &[]);
    assert!(ok, "{stderr}");
    assert_eq!(stdout.trim(), expected_digest("the-token"));
    // The file form is the replacement: it says nothing about argv.
    assert!(!stderr.contains("deprecated"), "{stderr}");
}

#[test]
fn stdin_works_end_to_end() {
    let (ok, stdout, stderr) = hash_token(&[], Some(" the-token \n"), &[]);
    assert!(ok, "{stderr}");
    assert_eq!(stdout.trim(), expected_digest("the-token"));
    assert!(!stderr.contains("deprecated"), "{stderr}");
}

#[test]
fn the_env_works_end_to_end() {
    let (ok, stdout, stderr) = hash_token(&[], None, &[("SCION_TOKEN", "the-token")]);
    assert!(ok, "{stderr}");
    assert_eq!(stdout.trim(), expected_digest("the-token"));
    assert!(!stderr.contains("deprecated"), "{stderr}");
}

#[test]
fn the_argv_form_warns_and_still_answers() {
    let (ok, stdout, stderr) = hash_token(&["--token", "the-token"], None, &[]);
    assert!(ok, "{stderr}");
    assert_eq!(stdout.trim(), expected_digest("the-token"));
    // The warning names the exposure (the process list, whose ps and
    // /proc view every local account has) and the replacement, and says
    // the form is deprecated.
    assert!(stderr.contains("/proc"), "{stderr}");
    assert!(stderr.contains("every local account"), "{stderr}");
    assert!(stderr.contains("--token-file"), "{stderr}");
    assert!(stderr.contains("deprecated"), "{stderr}");
}

#[test]
fn a_group_readable_token_file_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_secret(dir.path(), "token", 0o644, "the-token\n");
    let (ok, stdout, stderr) = hash_token(&["--token-file", file.to_str().unwrap()], None, &[]);
    assert!(!ok, "a 0644 token file must be refused: {stdout}{stderr}");
    assert!(stderr.contains("read by group or others"), "{stderr}");
}

#[test]
fn an_empty_token_file_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_secret(dir.path(), "token", 0o600, "  \n");
    let (ok, stdout, stderr) = hash_token(&["--token-file", file.to_str().unwrap()], None, &[]);
    assert!(!ok, "{stdout}{stderr}");
    assert!(stderr.contains("holds no secret"), "{stderr}");
}

#[test]
fn a_group_readable_serve_token_file_is_refused() {
    // The same refusal on the `serve` path: the flag is parsed by the
    // serve command, which checks the file before the data directory
    // opens, so nothing is created.
    let data = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = write_secret(dir.path(), "token", 0o644, "the-token\n");
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data.path())
        .args(["serve", "--token-file", file.to_str().unwrap()])
        .env_remove("SCION_TOKEN")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(stderr.contains("read by group or others"), "{stderr}");
    assert!(
        !data.path().join("scion.db").exists(),
        "a bad flag must not create the store"
    );
}

#[test]
fn a_serve_token_file_starts_the_server() {
    // The file form reaches the same single-principal access the argv
    // form built: the server starts and says so on its log.
    let data = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = write_secret(dir.path(), "token", 0o600, "the-token\n");
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data.path())
        .args([
            "serve",
            "--token-file",
            file.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
        ])
        .env_remove("SCION_TOKEN")
        .stderr(Stdio::piped())
        .spawn()
        .expect("the test binary runs");
    let stderr = child.stderr.take().expect("stderr is piped");
    // The server exits only when killed, so read until its log says it
    // serves, then kill it: the file form loaded the access and the
    // listener bound.
    let mut text = String::new();
    let mut line = String::new();
    let mut reader = std::io::BufReader::new(stderr);
    loop {
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                text.push_str(&line);
                if line.contains("serving") {
                    break;
                }
                line.clear();
            }
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(text.contains("serving"), "{text}");
    assert!(!text.contains("deprecated"), "{text}");
}
