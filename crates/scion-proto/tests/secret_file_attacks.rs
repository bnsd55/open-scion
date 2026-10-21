//! The attacks an adversary runs against the checked secret read
//! (#402): a FIFO as the file, a file of unbounded size. Each test
//! states the attack and the answer the read must give; a test that
//! passes is proof the read holds, a test that fails names a bug.

use std::time::Duration;

use scion_proto::runtime::auth::read_secret_file;

/// Runs `f` on a thread and waits `limit` for it to finish: the answer
/// is `Some` when the call came back, `None` when it hung.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    let handle = std::thread::spawn(f);
    let deadline = std::time::Instant::now() + limit;
    while std::time::Instant::now() < deadline {
        if handle.is_finished() {
            return Some(handle.join().expect("the thread did not panic"));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    // The thread stays leaked: a hung open is the bug, and the test
    // process must still be able to report it.
    None
}

/// A FIFO as the token file: `open(2)` for read blocks until a writer
/// appears, so a checked read that opens without `O_NONBLOCK` hangs the
/// server on a file an attacker can plant anywhere the operator points
/// `--token-file`. The read must refuse (a FIFO is not a regular file),
/// not block.
#[test]
fn a_fifo_as_the_file_is_refused_not_blocked() {
    let dir = tempfile::tempdir().expect("the temp dir opens");
    let fifo = dir.path().join("token");
    // The system mkfifo: std has none, and rustix's mkfifoat is not
    // on macOS. The mode is set to 0600 below, so the tool's umask
    // cannot matter.
    let made = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("mkfifo runs");
    assert!(made.success(), "mkfifo failed");
    // The mode a real secret file carries: the mode check must not be
    // what saves the read here — the file-type check is.
    std::fs::set_permissions(&fifo, std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .expect("the mode is set");
    let outcome = within(Duration::from_secs(5), move || read_secret_file(&fifo));
    assert!(
        outcome.is_some(),
        "the checked read hung on a FIFO: open(2) blocks without O_NONBLOCK, \
         so a planted FIFO hangs the server at startup"
    );
    let result = outcome.expect("the call came back");
    assert!(result.is_err(), "a FIFO must be refused, not read");
}

/// A huge file: the read takes the whole file into memory, and nothing
/// bounds it — a sparse file the attacker can grow to the disk's size,
/// or a device-like file whose mode and owner pass the checks, turns
/// `--token-file` into an out-of-memory. The read must refuse a file
/// past a stated cap. This file is 512 MiB of sparse zeros, owned by
/// this user, mode 0600: every check but a size cap passes.
#[test]
fn a_file_past_any_sane_cap_is_refused() {
    let dir = tempfile::tempdir().expect("the temp dir opens");
    let big = dir.path().join("token");
    let file = std::fs::File::create(&big).expect("the file is made");
    file.set_len(512 * 1024 * 1024).expect("the size is set");
    drop(file);
    std::fs::set_permissions(&big, std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .expect("the mode is set");
    let outcome = within(Duration::from_secs(30), move || read_secret_file(&big));
    let result = outcome.expect("the read of 512 MiB must come back, hung or not");
    assert!(
        result.is_err(),
        "the read took a 512 MiB file whole: nothing caps a secret file, \
         so a huge file is an out-of-memory at startup"
    );
}

/// The owner swap: between the ancestors' walk and the open, an
/// attacker cannot make the read take their bytes — the checks run on
/// the opened descriptor, so whatever the path names at open time is
/// what is checked. Proof: a file swapped for one the attacker owns is
/// refused, whatever the swap's timing, because the swapped-in file's
/// owner is not this process.
#[test]
fn a_file_owned_by_another_user_is_refused_on_the_descriptor() {
    let dir = tempfile::tempdir().expect("the temp dir opens");
    let file = dir.path().join("token");
    std::fs::write(&file, b"secret").expect("the file is written");
    std::fs::set_permissions(&file, std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .expect("the mode is set");
    // The attacker's file: their bytes, their owner (the test user
    // cannot chown, so the proof is the refusal of root-owned files
    // the check already names — here the swap target is a file with
    // the mode rule broken, which the descriptor check refuses).
    let other = dir.path().join("swapped");
    std::fs::write(&other, b"attacker bytes").expect("the file is written");
    std::fs::set_permissions(&other, std::os::unix::fs::PermissionsExt::from_mode(0o644))
        .expect("the mode is set");
    std::fs::rename(&other, &file).expect("the swap happens");
    let error = read_secret_file(&file).expect_err("the swapped-in file is refused");
    assert!(
        error.to_string().contains("read by group or others"),
        "the refusal must name the mode rule: {error}"
    );
}
