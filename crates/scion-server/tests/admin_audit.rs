//! The admin audit lines of `scion-server gc`: every run records one
//! intent before it starts and exactly one result after it, in the same
//! audit.log the server appends to. The lines carry the run's what,
//! whether it was a dry run, and the report's counts - never a token, a
//! blob ID, or a file path.

mod common;

use std::fs;
use std::process::{Command, Output};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
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

/// Runs `scion-server` with `args` after the global `--data`, and returns
/// its output.
fn run(data: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data)
        .args(args)
        .env("NO_COLOR", "1")
        .output()
        .unwrap()
}

/// A world-readable audit file: the audit's open refuses it.
fn guard_log(dir: &std::path::Path) -> std::path::PathBuf {
    let guard = dir.join("guard.log");
    fs::write(&guard, "").unwrap();
    fs::set_permissions(&guard, std::fs::Permissions::from_mode(0o644)).unwrap();
    guard
}

/// One git commit of `message` over the files `dir` currently holds.
fn git(dir: &std::path::Path, args: &[&str]) {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Ada")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_COMMITTER_NAME", "Ada")
        .env("GIT_COMMITTER_EMAIL", "ada@example.com")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A git repository whose main holds two commits over two files.
fn a_git_repo(path: &std::path::Path) {
    fs::create_dir_all(path).unwrap();
    git(path, &["init", "-q", "-b", "main"]);
    fs::write(path.join("a.txt"), "a\n").unwrap();
    git(path, &["add", "."]);
    git(path, &["commit", "-q", "-m", "one"]);
    fs::write(path.join("b.txt"), "b\n").unwrap();
    git(path, &["add", "."]);
    git(path, &["commit", "-q", "-m", "two"]);
}

/// No field of an audit line holds a content address: a git commit's
/// 40 hex characters or a blob or changeset ID's 64. The lines name
/// counts, never content.
fn no_content_ids(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => {
            let hexish = s.len() == 64 || s.len() == 40;
            !(hexish && s.chars().all(|c| c.is_ascii_hexdigit()))
        }
        serde_json::Value::Array(v) => v.iter().all(no_content_ids),
        serde_json::Value::Object(m) => m.values().all(no_content_ids),
        _ => true,
    }
}

fn repo_path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

fn regular(content: &str) -> FileState {
    FileState::Regular {
        content: BlobId::of(content.as_bytes()),
        executable: false,
    }
}

fn one_changeset(parents: Vec<ChangesetId>, changes: &[(&str, Option<FileState>)]) -> Changeset {
    Changeset::new(
        parents,
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"change\n".to_vec(),
        changes.iter().map(|(p, s)| (repo_path(p), *s)).collect(),
    )
    .expect("changeset fits the format")
}

/// A store with a repository `hist` whose main holds one changeset over
/// one blob. When the function returns, the store is closed and its
/// server lock is released, so the CLI a test spawns next opens the
/// directory at once.
fn seed_exportable(data: &std::path::Path) {
    fs::create_dir_all(data).unwrap();
    {
        let store = scion_server::Backend::open(data, None, None).unwrap();
        store.blobs().put(b"a\n").unwrap();
        store
            .write(|c| {
                let repo = c.create_repo("hist")?;
                let cs = one_changeset(Vec::new(), &[("a.txt", Some(regular("a\n")))]);
                let id =
                    scion_server::test_support::tree::store_changeset(c, repo, &cs, &mut |_| {
                        Ok(0)
                    })?
                    .0;
                assert!(c.cas_ref(repo, "main", None, id)?);
                Ok(())
            })
            .unwrap();
    }
    // The drop above releases server.lock, but the release can lag the
    // drop by a scheduler tick (the macOS lesson in wait_for_lock_release's
    // doc): CI saw the spawned `derive` refuse the directory as in use.
    // The wait takes the real condition -- the lock is free -- instead of
    // a timing guess, so a real leak still fails.
    common::spawn_guard::wait_for_lock_release(data);
}

/// One real import: one admin intent before, one admin ok after, with
/// the import's counts.
#[test]
fn an_import_writes_an_admin_intent_and_ok_line() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let hist = dir.path().join("hist");
    a_git_repo(&hist);

    let out = run(&data, &["import", "--repo", "hist", hist.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "import ran: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines = audit_lines(&data);
    assert_eq!(lines.len(), 2, "one intent and one result: {lines:?}");
    assert_eq!(lines[0]["event"], "admin");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[0]["what"], "import");
    assert_eq!(lines[0]["repo"], "hist");
    assert_eq!(lines[1]["event"], "admin");
    assert_eq!(lines[1]["outcome"], "ok");
    assert_eq!(lines[1]["what"], "import");
    assert_eq!(lines[1]["repo"], "hist");
    assert_eq!(lines[1]["commits"], 2);
    assert_eq!(lines[1]["commits_imported"], 2);
    assert_eq!(lines[1]["refs_moved"], 1);
    assert!(
        lines[1]["verified"].is_u64(),
        "the ok line carries the verified count: {lines:?}"
    );
}

/// The import's lines carry counts and a repo name: no commit ID and no
/// blob ID appears in the audit.
#[test]
fn an_import_audit_names_no_commit_or_blob() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let hist = dir.path().join("hist");
    a_git_repo(&hist);

    let out = run(&data, &["import", "--repo", "hist", hist.to_str().unwrap()]);
    assert!(out.status.success(), "import ran");

    let text = fs::read_to_string(data.join("audit.log")).unwrap();
    for line in text.lines() {
        let json: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(no_content_ids(&json), "a content ID in the line: {line}");
    }
}

/// An import whose audit file cannot open refuses before it changes
/// anything: the repository was not made, and a rerun with a working
/// audit imports the whole history.
#[test]
fn an_import_whose_audit_file_refuses_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let hist = dir.path().join("hist");
    a_git_repo(&hist);
    let guard = guard_log(dir.path());

    let out = run(
        &data,
        &[
            "import",
            "--repo",
            "hist",
            "--audit-file",
            guard.to_str().unwrap(),
            hist.to_str().unwrap(),
        ],
    );
    assert!(!out.status.success(), "the run refused");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("cannot open audit log"),
        "the error names the audit log: {err}"
    );
    assert!(!data.join("audit.log").exists(), "the run wrote no audit");

    let out = run(&data, &["import", "--repo", "hist", hist.to_str().unwrap()]);
    assert!(out.status.success(), "the rerun ran");
    let lines = audit_lines(&data);
    assert_eq!(lines.len(), 2, "the refused run wrote nothing: {lines:?}");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[1]["outcome"], "ok");
    assert_eq!(
        lines[1]["commits_imported"], 2,
        "nothing was imported before"
    );
}

/// One real restore: one admin intent before the replay and one admin ok
/// after it, in the restored store's own audit file, with the restore's
/// counts.
#[test]
fn a_restore_writes_an_admin_intent_and_ok_line() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let restored = dir.path().join("restored");
    let archive = dir.path().join("archive");
    seed_exportable(&data);

    let out = run(
        &data,
        &[
            "export",
            "--repo",
            "hist",
            "--out",
            archive.to_str().unwrap(),
        ],
    );
    assert!(
        out.status.success(),
        "export ran: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let out = run(
        &restored,
        &["restore", "--archive", archive.to_str().unwrap()],
    );
    assert!(
        out.status.success(),
        "restore ran: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines = audit_lines(&restored);
    assert_eq!(lines.len(), 2, "one intent and one result: {lines:?}");
    assert_eq!(lines[0]["event"], "admin");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[0]["what"], "restore");
    assert_eq!(lines[1]["event"], "admin");
    assert_eq!(lines[1]["outcome"], "ok");
    assert_eq!(lines[1]["what"], "restore");
    assert_eq!(lines[1]["repos"], 1);
    assert_eq!(lines[1]["refs"], 1);
    assert_eq!(lines[1]["lands"], 0);
}

/// A restore whose audit file cannot open refuses before it changes
/// anything: the target directory holds no store.
#[test]
fn a_restore_whose_audit_file_refuses_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let restored = dir.path().join("restored");
    seed_exportable(&data);
    let archive = dir.path().join("archive");
    let out = run(
        &data,
        &[
            "export",
            "--repo",
            "hist",
            "--out",
            archive.to_str().unwrap(),
        ],
    );
    assert!(out.status.success(), "export ran");
    let guard = guard_log(dir.path());

    let out = run(
        &restored,
        &[
            "restore",
            "--archive",
            archive.to_str().unwrap(),
            "--audit-file",
            guard.to_str().unwrap(),
        ],
    );
    assert!(!out.status.success(), "the run refused");
    assert!(
        !restored.join("scion.db").exists(),
        "the target holds no store"
    );
}

/// The verify subcommand runs without the store-open match that hid an
/// `unreachable!` behind verify and restore since the archive landed: a
/// good archive exits 0, a damaged one names its damage and does not.
#[test]
fn a_verify_subcommand_reports_a_good_archive_and_names_a_damaged_one() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let archive = dir.path().join("archive");
    seed_exportable(&data);

    let out = run(
        &data,
        &[
            "export",
            "--repo",
            "hist",
            "--out",
            archive.to_str().unwrap(),
        ],
    );
    assert!(
        out.status.success(),
        "export ran: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["verify", "--archive", archive.to_str().unwrap()])
        .env("NO_COLOR", "1")
        .output()
        .unwrap();
    assert!(out.status.success(), "a good archive verifies");
    let out_text = String::from_utf8_lossy(&out.stdout);
    assert!(
        out_text.contains("verify: 0 damages"),
        "the report says what it saw: {out_text}"
    );

    // A blob file whose bytes another hand changed: the verify names the
    // blob, and the exit is not 0. The blobs live in a fan-out of two
    // hex levels under blobs/.
    let mut blob = None;
    for first in fs::read_dir(archive.join("blobs")).unwrap().flatten() {
        for second in fs::read_dir(first.path()).unwrap().flatten() {
            if second.path().is_file() {
                blob = Some(second.path());
                break;
            }
        }
        if blob.is_some() {
            break;
        }
    }
    let blob = blob.expect("the archive holds a blob file");
    fs::write(&blob, b"someone changed these bytes\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["verify", "--archive", archive.to_str().unwrap()])
        .env("NO_COLOR", "1")
        .output()
        .unwrap();
    assert!(!out.status.success(), "a damaged archive does not verify");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("verify: 1 damages"),
        "the report names the damage count: {text}"
    );
}

/// A restore that fails mid-replay still leaves its audit record: the
/// intent goes back to the target with the failed line after it, and
/// the target holds nothing else — the evidence an auditor needs is
/// exactly what a failed run must not delete.
#[test]
fn a_restore_that_fails_mid_replay_leaves_its_audit_record() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let restored = dir.path().join("restored");
    seed_exportable(&data);
    let archive = dir.path().join("archive");
    let out = run(
        &data,
        &[
            "export",
            "--repo",
            "hist",
            "--out",
            archive.to_str().unwrap(),
        ],
    );
    assert!(out.status.success(), "export ran");
    // A blob file whose bytes another hand changed: the replay's check
    // refuses it.
    let mut blob = None;
    for first in fs::read_dir(archive.join("blobs")).unwrap().flatten() {
        for second in fs::read_dir(first.path()).unwrap().flatten() {
            if second.path().is_file() {
                blob = Some(second.path());
                break;
            }
        }
        if blob.is_some() {
            break;
        }
    }
    fs::write(blob.expect("the archive holds a blob"), b"damaged\n").unwrap();

    let out = run(
        &restored,
        &["restore", "--archive", archive.to_str().unwrap()],
    );
    assert!(!out.status.success(), "the run refused");

    let lines = audit_lines(&restored);
    assert_eq!(lines.len(), 2, "one intent and one result: {lines:?}");
    assert_eq!(lines[0]["event"], "admin");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[0]["what"], "restore");
    assert_eq!(lines[1]["event"], "admin");
    assert_eq!(lines[1]["outcome"], "failed");
    assert_eq!(lines[1]["why"], "failed");
    let held: Vec<_> = fs::read_dir(&restored).unwrap().flatten().collect();
    assert_eq!(held.len(), 1, "the target holds only the audit: {held:?}");
    assert_eq!(held[0].file_name(), "audit.log");
}

/// The derive subcommand's admin pair: one intent before the backfill
/// and one ok after it, with the rows the run wrote and removed.
#[test]
fn a_derive_writes_an_admin_intent_and_ok_line() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    seed_exportable(&data);

    let out = run(&data, &["derive", "--kind", "root_tree", "--repo", "hist"]);
    assert!(
        out.status.success(),
        "derive ran: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines = audit_lines(&data);
    assert_eq!(lines.len(), 2, "one intent and one result: {lines:?}");
    assert_eq!(lines[0]["event"], "admin");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[0]["what"], "derive");
    assert_eq!(lines[0]["kind"], "root_tree");
    assert_eq!(lines[1]["event"], "admin");
    assert_eq!(lines[1]["outcome"], "ok");
    assert_eq!(lines[1]["what"], "derive");
    assert_eq!(lines[1]["kind"], "root_tree");
    assert!(
        lines[1]["written"].is_u64(),
        "the ok line carries the rows the run wrote: {lines:?}"
    );
    assert_eq!(lines[1]["removed"], 0, "no older version's rows to remove");
}

/// A derive that fails writes its failed line: the intent's pair is
/// complete, with the error's class and never its message.
#[test]
fn a_derive_that_fails_writes_its_failed_line() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    seed_exportable(&data);

    let out = run(
        &data,
        &["derive", "--kind", "root_tree", "--repo", "no-such-repo"],
    );
    assert!(!out.status.success(), "a missing repo fails the run");
    let lines = audit_lines(&data);
    assert_eq!(lines.len(), 2, "one intent and one result: {lines:?}");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[0]["what"], "derive");
    assert_eq!(lines[1]["event"], "admin");
    assert_eq!(lines[1]["outcome"], "failed");
    assert_eq!(lines[1]["what"], "derive");
    assert_eq!(lines[1]["why"], "failed");
}

/// The ticket-key subcommand's admin pair: one intent before the key
/// exists and one ok after it is on the disk, and no key material in the
/// audit in any encoding.
#[test]
fn a_ticket_key_writes_its_pair_and_no_key_material() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let key_file = dir.path().join("tickets.key");

    let out = run(&data, &["ticket-key", "--out", key_file.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "ticket-key ran: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines = audit_lines(&data);
    assert_eq!(lines.len(), 2, "one intent and one result: {lines:?}");
    assert_eq!(lines[0]["event"], "admin");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[0]["what"], "ticket-key");
    assert_eq!(lines[1]["event"], "admin");
    assert_eq!(lines[1]["outcome"], "ok");
    assert_eq!(lines[1]["what"], "ticket-key");

    // No seed or key bytes in the audit, in hex or in base64: the file
    // holds the seed as hex, and the audit never names it.
    let seed_hex = fs::read_to_string(&key_file).unwrap();
    let seed_hex = seed_hex.trim();
    assert_eq!(seed_hex.len(), 64, "the seed file holds 64 hex characters");
    let seed = (0..seed_hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&seed_hex[i..i + 2], 16).unwrap())
        .collect::<Vec<u8>>();
    let audit = fs::read_to_string(data.join("audit.log")).unwrap();
    assert!(
        !audit.contains(seed_hex),
        "the seed's hex is not in the audit"
    );
    assert!(
        !audit.contains(&base64_of(&seed)),
        "the seed's base64 is not in the audit"
    );
}

/// Base64 of `bytes`, in the standard alphabet with padding, so a test
/// can search an audit for an encoding of a secret without a dependency.
fn base64_of(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// An open that migrates an older store writes its line once: the
/// migration ran inside the store's open, before the audit could open,
/// so its record goes in before the command's own pair. A second open
/// migrates nothing, and writes no line.
#[test]
fn an_open_that_migrates_writes_its_line_once() {
    if scion_server::Backend::test_is_postgres() {
        // The Postgres line rides the same code path this test pins (the
        // open's migration reports its versions, and the record goes in
        // before the command's pair); a test of it needs a schema at an
        // older version, and the Postgres migrations are not idempotent,
        // so no test seam makes one.
        println!("skipping: the migrate line's path is pinned on SQLite");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    // The metadata at version 1, as the build's first migration makes it.
    let db = rusqlite::Connection::open(data.join("scion.db")).unwrap();
    scion_server::test_support::store::migrate(&db, 1).unwrap();
    drop(db);

    let out = gc(&data, &["--grace-secs", "0"]);
    assert!(
        out.status.success(),
        "gc ran: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines = audit_lines(&data);
    assert_eq!(lines.len(), 3, "migrate, then the gc's pair: {lines:?}");
    assert_eq!(lines[0]["event"], "admin");
    assert_eq!(lines[0]["outcome"], "ok");
    assert_eq!(lines[0]["what"], "migrate");
    assert_eq!(lines[0]["from"], 1);
    assert!(
        lines[0]["to"].as_u64().unwrap_or(0) > 1,
        "the open moved the schema up: {lines:?}"
    );
    assert_eq!(lines[1]["what"], "gc");
    assert_eq!(lines[1]["outcome"], "intent");
    assert_eq!(lines[2]["outcome"], "ok");

    // The second open finds the schema at this build's version, and
    // writes no migration line.
    let out = gc(&data, &["--grace-secs", "0"]);
    assert!(out.status.success(), "the second gc ran");
    let lines = audit_lines(&data);
    assert_eq!(
        lines.len(),
        5,
        "the second run adds its own pair: {lines:?}"
    );
    assert_eq!(lines[3]["what"], "gc");
    assert_eq!(lines[3]["outcome"], "intent");
    assert_eq!(lines[4]["outcome"], "ok");
}
