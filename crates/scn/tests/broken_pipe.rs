//! `scn` on a closed stdout: the reader of a pipe that stops early
//! (`scn blame f | head -1`) goes away, and scn's next write fails with
//! `BrokenPipe`. The command ends quietly with status 0 and no panic
//! text on stderr — a closed stdout is a normal end for a CLI, not the
//! "failed printing to stdout" panic.

use std::io::Read as _;
use std::process::{Command, Stdio};

use scion_core::{ChangesetId, FileState};
use scion_server::Backend;

mod common;
use common::{REPO, TOKEN, Tiers, changeset, land};

/// Lands `big.txt` twice: 2000 lines of about 100 printed bytes each,
/// so blame's output passes what a pipe holds (64 KiB) and scn is
/// still writing when the reader leaves. A smaller output would let
/// scn finish into the pipe buffer before the close, and the test
/// would pass without ever reaching the broken pipe.
fn start(store: &Backend) -> ChangesetId {
    let lines = |tag: &str| {
        use std::fmt::Write as _;
        let mut content = String::new();
        for i in 0..2000 {
            writeln!(content, "{tag} line {i:04} {}", "x".repeat(48)).unwrap();
        }
        content
    };
    let put = |content: &str| {
        let mut blobs = store.blobs().batch();
        blobs.put(content.as_bytes()).unwrap()
    };
    let base = land(
        store,
        &changeset(
            None,
            &[(
                "big.txt",
                FileState::Regular {
                    content: put(&lines("old")),
                    executable: false,
                },
            )],
        ),
    );
    land(
        store,
        &changeset(
            Some(base),
            &[(
                "big.txt",
                FileState::Regular {
                    content: put(&lines("new")),
                    executable: false,
                },
            )],
        ),
    )
}

#[test]
fn a_closed_stdout_ends_the_command_quietly() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    let _tip = start(&store);
    let tiers = Tiers::start(data, store.clone());
    let home = tempfile::tempdir().unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args([
            "--server",
            &tiers.server,
            "--repo",
            REPO,
            "blame",
            "main:big.txt",
        ])
        .env("SCION_TOKEN", TOKEN)
        .env("HOME", home.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // One byte proves scn is producing. Dropping the reader closes the
    // pipe while scn still has the rest of the 2000 lines to write, so
    // its next write meets the closed pipe.
    let mut stdout = child.stdout.take().unwrap();
    let mut first = [0u8; 1];
    stdout.read_exact(&mut first).unwrap();
    drop(stdout);

    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(0), "status: {}", out.status);
    assert!(
        out.stderr.is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
