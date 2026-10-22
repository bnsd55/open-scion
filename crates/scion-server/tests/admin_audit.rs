//! The admin audit lines of `scion-server gc`: every run records one
//! intent before it starts and exactly one result after it, in the same
//! audit.log the server appends to. The lines carry the run's what,
//! whether it was a dry run, and the report's counts - never a token, a
//! blob ID, or a file path.

use std::fs;
use std::process::{Command, Output};

use scion_core::BlobId;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use std::os::unix::fs::PermissionsExt as _;

/// Creates the data directory of `dir`, seeds it with repo `r` whose
/// rows are all dead (no stored changeset puts them): one row names a
/// stored blob file, one names a blob with no file at all. Returns the
/// stored blob's ID.
fn seed(data: &std::path::Path) -> BlobId {
    fs::create_dir_all(data).unwrap();
    let store = scion_server::Backend::open(data, None, None).unwrap();
    let blob = store.blobs().put(b"a prunable file\n").unwrap();
    let fake = BlobId::from_bytes([7u8; 32]);
    store
        .write(|c| {
            let repo = c.create_repo("r")?;
            c.add_repo_blob(repo, blob)?;
            c.add_repo_blob(repo, fake)
        })
        .unwrap();
    drop(store);
    blob
}

/// Runs `scion-server gc` on `data` with `args`, and returns its output.
fn gc(data: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data)
        .args(["gc"])
        .args(args)
        .env("NO_COLOR", "1")
        .output()
        .unwrap()
}

/// The audit lines of the data directory's audit.log.
fn audit_lines(data: &std::path::Path) -> Vec<serde_json::Value> {
    let text = fs::read_to_string(data.join("audit.log")).unwrap();
    text.lines()
        .map(|line| serde_json::from_str(line).expect("each audit line is one JSON object"))
        .collect()
}

fn hex(id: &BlobId) -> String {
    id.as_bytes().iter().fold(String::new(), |mut out, b| {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// One real run: one admin intent before, one admin ok after, with the
/// report's counts and `dry_run: false`.
#[test]
fn a_gc_run_writes_an_admin_intent_and_ok_line() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    seed(&data);

    let out = gc(&data, &["--grace-secs", "0"]);
    assert!(
        out.status.success(),
        "gc ran: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines = audit_lines(&data);
    assert_eq!(lines.len(), 2, "one intent and one result: {lines:?}");
    assert_eq!(lines[0]["event"], "admin");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[0]["what"], "gc");
    assert_eq!(lines[0]["dry_run"], false);
    assert_eq!(lines[1]["event"], "admin");
    assert_eq!(lines[1]["outcome"], "ok");
    assert_eq!(lines[1]["what"], "gc");
    assert_eq!(lines[1]["repos"], 1);
    assert_eq!(lines[1]["rows_pruned"], 2);
    assert_eq!(lines[1]["files_removed"], 1);
    assert_eq!(lines[1]["dry_run"], false);
}

/// A dry run changes nothing, and its lines say so.
#[test]
fn a_dry_run_labels_itself_dry_run() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    seed(&data);

    let out = gc(&data, &["--grace-secs", "0", "--dry-run"]);
    assert!(
        out.status.success(),
        "gc ran: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines = audit_lines(&data);
    assert_eq!(lines.len(), 2, "one intent and one result: {lines:?}");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[0]["dry_run"], true);
    assert_eq!(lines[1]["outcome"], "ok");
    assert_eq!(lines[1]["dry_run"], true);
    assert_eq!(
        lines[1]["rows_pruned"], 2,
        "the dry run says what it would prune"
    );
}

/// A gc whose audit file cannot open refuses before it changes
/// anything: the prunable rows survive for the next run.
#[test]
fn a_gc_whose_audit_file_refuses_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    seed(&data);
    // An audit file another hand left world-readable: the audit's open
    // refuses it, so the run must refuse with it.
    let guard = dir.path().join("guard.log");
    fs::write(&guard, "").unwrap();
    fs::set_permissions(&guard, std::fs::Permissions::from_mode(0o644)).unwrap();

    let out = gc(
        &data,
        &["--grace-secs", "0", "--audit-file", guard.to_str().unwrap()],
    );
    assert!(!out.status.success(), "the run refused");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("cannot open audit log"),
        "the error names the audit log: {err}"
    );

    // Nothing changed: the same store still holds its prunable rows,
    // and a run whose audit opens prunes them.
    let out = gc(&data, &["--grace-secs", "0"]);
    assert!(out.status.success(), "the retry ran");
    let lines = audit_lines(&data);
    assert_eq!(lines.len(), 2, "the refused run wrote nothing: {lines:?}");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[1]["outcome"], "ok");
    assert_eq!(lines[1]["rows_pruned"], 2, "the rows were still there");
}

/// No field of an audit line holds a string that is a blob ID: 64 hex
/// characters is a content address, and the lines name counts, never
/// content.
fn no_blob_ids(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => {
            !(s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit()))
        }
        serde_json::Value::Array(v) => v.iter().all(no_blob_ids),
        serde_json::Value::Object(m) => m.values().all(no_blob_ids),
        _ => true,
    }
}

/// No blob ID the store holds appears in the audit: the lines name
/// counts, never content.
#[test]
fn a_gc_audit_names_no_blob() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let blob = seed(&data);

    let out = gc(&data, &["--grace-secs", "0"]);
    assert!(out.status.success(), "gc ran");
    let text = fs::read_to_string(data.join("audit.log")).unwrap();
    assert!(
        !text.contains(&hex(&blob)),
        "no blob digest in the audit: {text}"
    );
    for line in text.lines() {
        let json: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(no_blob_ids(&json), "a blob ID in the line: {line}");
    }
}
