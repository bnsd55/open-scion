//! The admin audit lines of `scion-server gc`: every run records one
//! intent before it starts and exactly one result after it, in the same
//! audit.log the server appends to. The lines carry the run's what,
//! whether it was a dry run, and the report's counts - never a token, a
//! blob ID, or a file path.

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
/// one blob. The caller must drop the store before the CLI opens it.
fn seed_exportable(data: &std::path::Path) {
    fs::create_dir_all(data).unwrap();
    let store = scion_server::Backend::open(data, None, None).unwrap();
    store.blobs().put(b"a\n").unwrap();
    store
        .write(|c| {
            let repo = c.create_repo("hist")?;
            let cs = one_changeset(Vec::new(), &[("a.txt", Some(regular("a\n")))]);
            let id =
                scion_server::test_support::tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0;
            assert!(c.cas_ref(repo, "main", None, id)?);
            Ok(())
        })
        .unwrap();
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
