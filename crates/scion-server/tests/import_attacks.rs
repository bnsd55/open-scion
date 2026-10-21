//! Git import against crafted raw objects: a malicious repository is one
//! an operator imports from someone else, and its tree and commit objects
//! need not be what `git` itself would write. Every test names what the
//! import must do with bytes a crafted repository holds.

#![cfg(unix)]

mod common;

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use scion_server::{Backend, Error, ImportOptions, import_git};
use tempfile::TempDir;

/// A git repository in a temp dir, with fixed identities and no user or
/// system config.
struct Git(TempDir);

impl Git {
    fn init() -> Self {
        let git = Self(tempfile::tempdir().unwrap());
        git.run(&["init", "-q", "-b", "main"]);
        git
    }

    fn path(&self) -> &Path {
        self.0.path()
    }

    /// Runs git with `stdin`, fails the test unless it succeeds, and returns
    /// raw stdout.
    fn raw(&self, args: &[&str], stdin: &[u8]) -> String {
        let mut child = common::spawn_guard::across_spawn(|| {
            Command::new("git")
                .args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"])
                .args(args)
                .current_dir(self.path())
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
        })
        .unwrap();
        child.stdin.take().unwrap().write_all(stdin).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    }

    fn run(&self, args: &[&str]) -> String {
        self.raw(args, b"")
    }

    /// Writes a raw object and returns its name.
    fn object(&self, kind: &str, bytes: &[u8]) -> String {
        self.raw(&["hash-object", "-w", "-t", kind, "--stdin", "--literal"], bytes)
    }

    /// Points `refs/heads/main` at `name` without any check git could make.
    fn force_main(&self, name: &str) {
        self.run(&["update-ref", "refs/heads/main", name]);
    }
}

fn unhex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

/// One raw tree entry: `<mode> <name>\0<20-byte object name>`.
fn entry(mode: &str, name: &[u8], object: &str) -> Vec<u8> {
    let mut bytes = mode.as_bytes().to_vec();
    bytes.push(b' ');
    bytes.extend_from_slice(name);
    bytes.push(0);
    bytes.extend_from_slice(&unhex(object));
    bytes
}

/// A commit object that names `tree`, with the given raw header lines and
/// message.
fn commit(tree: &str, headers: &[String], message: &str) -> Vec<u8> {
    let mut bytes = format!("tree {tree}\n").into_bytes();
    for header in headers {
        bytes.extend_from_slice(header.as_bytes());
        bytes.push(b'\n');
    }
    bytes.push(b'\n');
    bytes.extend_from_slice(message.as_bytes());
    bytes
}

fn author_line(offset: &str) -> String {
    format!("author Ada <ada@example.com> 1700000000 {offset}")
}

fn opts() -> ImportOptions {
    ImportOptions {
        rev: "main".into(),
        target_ref: "main".into(),
        verify_sample: 1000,
    }
}

fn open_store() -> (TempDir, Backend) {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    (dir, store)
}

/// The import's verdict for `git`'s `main`: refused, or accepted with a
/// report. A panic or a hang fails the test on its own.
fn verdict(git: &Git) -> Result<usize, Error> {
    common::spawn_guard::prime_gix_lookup();
    let (_dir, store) = open_store();
    import_git(&store, "r", git.path(), &opts()).map(|report| report.commits)
}

/// A blob the crafted entries can name; the import must refuse on the
/// path before it reads any blob.
fn blob(git: &Git) -> String {
    git.object("blob", b"payload\n")
}

/// A one-file tree of real names, so a crafted parent tree parses and the
/// walk reaches the crafted entry.
fn plain_tree(git: &Git) -> String {
    let blob = blob(git);
    git.object("tree", &entry("100644", b"f", &blob))
}

fn refusal(git: &Git, what: &str) {
    match verdict(git) {
        Ok(commits) => panic!("{what}: the import accepted {commits} commits"),
        Err(Error::Git(_)) | Err(Error::Corrupt(_)) => {}
        Err(other) => panic!("{what}: refused, but as {other:?}"),
    }
}

#[test]
fn a_tree_name_with_a_slash_is_refused() {
    let git = Git::init();
    let blob = blob(&git);
    let tree = git.object(
        "tree",
        &[entry("100644", b"a/b", &blob), entry("100644", b"f", &blob)].concat(),
    );
    let head = commit(&tree, &[author_line("+0000"), format!("committer Ada <ada@example.com> 1700000000 +0000")], "bad name\n");
    let head = git.object("commit", &head);
    git.force_main(&head);
    refusal(&git, "a name with a slash is not one path part");
}

#[test]
fn a_tree_name_that_walks_out_is_refused() {
    let git = Git::init();
    let blob = blob(&git);
    let tree = git.object(
        "tree",
        &[entry("100644", b"..", &blob), entry("100644", b"f", &blob)].concat(),
    );
    let head = commit(&tree, &[author_line("+0000"), format!("committer Ada <ada@example.com> 1700000000 +0000")], "bad name\n");
    let head = git.object("commit", &head);
    git.force_main(&head);
    refusal(&git, "a '..' name does not name one part");
}

#[test]
fn an_empty_tree_name_is_refused() {
    let git = Git::init();
    let blob = blob(&git);
    let tree = git.object(
        "tree",
        &[entry("100644", b"", &blob), entry("100644", b"f", &blob)].concat(),
    );
    let head = commit(&tree, &[author_line("+0000"), format!("committer Ada <ada@example.com> 1700000000 +0000")], "bad name\n");
    let head = git.object("commit", &head);
    git.force_main(&head);
    refusal(&git, "an empty name is not a path");
}

#[test]
fn a_duplicate_tree_entry_is_refused() {
    let git = Git::init();
    let blob = blob(&git);
    let twice = [entry("100644", b"f", &blob), entry("100644", b"f", &blob)].concat();
    let tree = git.object("tree", &twice);
    let head = commit(&tree, &[author_line("+0000"), format!("committer Ada <ada@example.com> 1700000000 +0000")], "duplicate\n");
    let head = git.object("commit", &head);
    git.force_main(&head);
    refusal(&git, "two entries with one name have no Scion tree");
}

#[test]
fn a_directory_entry_pointing_at_a_blob_is_refused() {
    let git = Git::init();
    let blob = blob(&git);
    let tree = git.object(
        "tree",
        &[entry("40000", b"d", &blob), entry("100644", b"f", &blob)].concat(),
    );
    let head = commit(&tree, &[author_line("+0000"), format!("committer Ada <ada@example.com> 1700000000 +0000")], "dir is a blob\n");
    let head = git.object("commit", &head);
    git.force_main(&head);
    refusal(&git, "a directory entry must name a tree");
}

#[test]
fn a_commit_whose_parent_does_not_exist_is_refused() {
    let git = Git::init();
    let tree = plain_tree(&git);
    let missing = "0".repeat(40);
    let head = commit(
        &tree,
        &[
            format!("parent {missing}"),
            author_line("+0000"),
            "committer Ada <ada@example.com> 1700000000 +0000".to_owned(),
        ],
        "parent from nowhere\n",
    );
    let head = git.object("commit", &head);
    git.force_main(&head);
    refusal(&git, "a parent the repository does not hold cannot import");
}

#[test]
fn a_deep_tree_past_the_path_limit_is_refused() {
    let git = Git::init();
    let blob = blob(&git);
    // 300 nested directories: past the 256-part path limit. Innermost
    // first, so every name exists when its parent is written.
    let mut tree = git.object("tree", &entry("100644", b"f", &blob));
    for _ in 0..300 {
        tree = git.object("tree", &entry("40000", b"d", &tree));
    }
    let head = commit(&tree, &[author_line("+0000"), format!("committer Ada <ada@example.com> 1700000000 +0000")], "deep\n");
    let head = git.object("commit", &head);
    git.force_main(&head);
    refusal(&git, "a path past 256 parts has no Scion form");
}

#[test]
fn a_bad_identity_line_is_refused_not_a_panic() {
    let git = Git::init();
    let tree = plain_tree(&git);
    let head = commit(
        &tree,
        &[author_line("+999999999999999999"), "committer Ada <ada@example.com> 1700000000 +0000".to_owned()],
        "huge offset\n",
    );
    let head = git.object("commit", &head);
    git.force_main(&head);
    refusal(&git, "an identity git cannot have is refused");
}

/// Names a checkout keeps off the disk are names the import may store: git
/// trees legitimately hold them, and the checkout is the layer that folds.
/// The import accepts, stores, and verifies them like any other name.
#[test]
fn reserved_names_import_like_any_other_name() {
    for name in [".git".as_bytes(), ".GIT".as_bytes(), b"dot." as &[u8]] {
        let git = Git::init();
        let blob = blob(&git);
        let tree = git.object(
            "tree",
            &[entry("100644", name, &blob), entry("100644", b"f", &blob)].concat(),
        );
        let head = commit(&tree, &[author_line("+0000"), format!("committer Ada <ada@example.com> 1700000000 +0000")], "reserved\n");
        let head = git.object("commit", &head);
        git.force_main(&head);
        match verdict(&git) {
            Ok(commits) => assert_eq!(commits, 1, "{name:?} imports as one commit"),
            Err(e) => panic!("{name:?}: the import refused a name git may hold: {e:?}"),
        }
    }
}

/// A symlink entry stores its target bytes as a blob, like git holds them;
/// the target is never resolved by the import.
#[test]
fn a_symlink_stores_its_target_bytes() {
    let git = Git::init();
    let target = git.object("blob", b"/etc/passwd");
    let tree = git.object(
        "tree",
        &[entry("120000", b"link", &target), entry("100644", b"f", &blob(&git))].concat(),
    );
    let head = commit(&tree, &[author_line("+0000"), format!("committer Ada <ada@example.com> 1700000000 +0000")], "link\n");
    let head = git.object("commit", &head);
    git.force_main(&head);
    match verdict(&git) {
        Ok(commits) => assert_eq!(commits, 1),
        Err(e) => panic!("a symlink is a git kind the import stores: {e:?}"),
    }
    // The target bytes are only a blob: no file left the repository.
    assert!(!git.path().join("etc").exists());
}

/// A gitlink stores the submodule's commit name as an opaque pointer; the
/// commit it names is never fetched, so a name from nowhere imports.
#[test]
fn a_gitlink_never_follows_its_commit() {
    let git = Git::init();
    let from_nowhere = "0".repeat(40);
    let tree = git.object(
        "tree",
        &[
            entry("160000", b"sub", &from_nowhere),
            entry("100644", b"f", &blob(&git)),
        ]
        .concat(),
    );
    let head = commit(&tree, &[author_line("+0000"), format!("committer Ada <ada@example.com> 1700000000 +0000")], "submodule\n");
    let head = git.object("commit", &head);
    git.force_main(&head);
    match verdict(&git) {
        Ok(commits) => assert_eq!(commits, 1),
        Err(e) => panic!("a gitlink is opaque, its commit is not fetched: {e:?}"),
    }
    assert!(fs::read_dir(git.path()).is_ok());
}
