//! Every directory the server makes is private whatever the umask. Ubuntu
//! gives each user umask 002, and a service manager may run with 000: the
//! server must still accept the data directory it made, and a ticket key
//! must still be able to sit in it.

#![cfg(unix)]

use std::ffi::OsStr;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// `scion-server` with `args`, run by a shell that first sets umask `umask`,
/// as a login shell or a service manager does. The process's own umask
/// cannot be changed for one test: it is the whole test process's.
fn server(umask: &str, args: &[&OsStr]) -> Command {
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg(format!("umask {umask} && exec \"$0\" \"$@\""))
        .arg(env!("CARGO_BIN_EXE_scion-server"))
        .args(args)
        .env("NO_COLOR", "1");
    command
}

/// Runs `command`, and fails the test unless it succeeds.
fn run(mut command: Command) {
    let out = command.output().unwrap();
    assert!(
        out.status.success(),
        "{:?}: {}",
        command,
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A temp directory with mode 0700, set rather than left to the umask of
/// the run.
fn private_tempdir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

/// `dir` and every directory below it, with its mode bits.
fn directories(dir: &Path) -> Vec<(PathBuf, u32)> {
    let mut found = vec![(dir.to_owned(), mode_of(dir))];
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if fs::symlink_metadata(&path).unwrap().is_dir() {
            found.extend(directories(&path));
        }
    }
    found
}

fn mode_of(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o7777
}

/// A git repository with one commit of two files in two directories.
fn git_repo(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();
    fs::write(dir.join("README"), "one\n").unwrap();
    fs::write(dir.join("src/lib.rs"), "fn main() {}\n").unwrap();
    let steps: [&[&str]; 3] = [
        &["init", "-q", "-b", "main"],
        &["add", "-A"],
        &["commit", "-q", "-m", "one"],
    ];
    for args in steps {
        let mut git = Command::new("git");
        git.args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"])
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Ada")
            .env("GIT_AUTHOR_EMAIL", "ada@example.com")
            .env("GIT_AUTHOR_DATE", "1700000000 +0000")
            .env("GIT_COMMITTER_NAME", "Ada")
            .env("GIT_COMMITTER_EMAIL", "ada@example.com")
            .env("GIT_COMMITTER_DATE", "1700000000 +0000");
        run(git);
    }
}

#[test]
fn every_directory_of_a_new_data_directory_is_private_whatever_the_umask() {
    let tmp = private_tempdir();
    let git = tmp.path().join("git");
    git_repo(&git);
    // 000 would leave 0777 on a plain mkdir, and 002 (Ubuntu's) 0775.
    for umask in ["000", "002", "022"] {
        let top = tmp.path().join(format!("umask-{umask}"));
        let data = top.join("project/data");
        // An import makes the data directory, the parents it needs, the
        // database, the blob folders and this process's staging folder.
        run(server(
            umask,
            &[
                "--data".as_ref(),
                data.as_os_str(),
                "import".as_ref(),
                "--repo".as_ref(),
                "main".as_ref(),
                git.as_os_str(),
            ],
        ));
        let made = directories(&top.join("project"));
        assert!(
            made.iter()
                .any(|(path, _)| path.starts_with(data.join("objects/blob/"))),
            "the import stored no blob: {made:?}"
        );
        for (path, mode) in made {
            assert_eq!(mode, 0o700, "umask {umask}: {}", path.display());
        }
    }
}

#[test]
fn serve_starts_with_its_ticket_key_in_a_data_directory_that_it_made() {
    let tmp = private_tempdir();
    let data = tmp.path().join("project/data");
    let key = data.join("ticket.key");
    // The most open umask: a plain mkdir would give 0777, which no check of
    // the key's directory accepts.
    run(server(
        "000",
        &["--data".as_ref(), data.as_os_str(), "fsck".as_ref()],
    ));
    run(server(
        "000",
        &[
            "--data".as_ref(),
            data.as_os_str(),
            "ticket-key".as_ref(),
            "--out".as_ref(),
            key.as_os_str(),
        ],
    ));
    assert_eq!(mode_of(&key), 0o600);

    let log = tmp.path().join("serve.log");
    let mut child = server(
        "000",
        &[
            "--data".as_ref(),
            data.as_os_str(),
            "serve".as_ref(),
            "--token".as_ref(),
            "t".as_ref(),
            "--listen".as_ref(),
            "127.0.0.1:0".as_ref(),
            "--ticket-key".as_ref(),
            key.as_os_str(),
        ],
    )
    .stdout(Stdio::null())
    .stderr(fs::File::create(&log).unwrap())
    .spawn()
    .unwrap();
    // A refused key or data directory exits at once; a good start logs
    // "serving" once the server listens. Wait for one of the two.
    let deadline = Instant::now() + Duration::from_secs(30);
    let served = loop {
        if fs::read_to_string(&log).unwrap().contains("serving") {
            break true;
        }
        if child.try_wait().unwrap().is_some() || Instant::now() > deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        served,
        "serve did not start: {}",
        fs::read_to_string(&log).unwrap()
    );
}
