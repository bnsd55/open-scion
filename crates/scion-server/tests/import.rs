//! Git import against fixture repositories built with the `git` CLI.
#![cfg(unix)]

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use gix::ObjectId;
use rusqlite::{Connection, params};
use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileState, GitOid, IdentityTime, PathElem, RepoPath,
};
use scion_server::auth::Caller;
use scion_server::deadline::Deadline;
use scion_server::import::{ImportOptions, ImportReport, import_git};
use scion_server::land::{LandRequest, LandState, Lands};
use scion_server::{Error, Store, db, read, tree, write};
use tempfile::TempDir;

const SUBMODULE: &str = "0123456789abcdef0123456789abcdef01234567";
const NON_UTF8_MESSAGE: &[u8] = b"caf\xe9 is not UTF-8\n\nbody \xff\n";

/// A git repository in a temp dir, with fixed identities and dates and no
/// user or system config.
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
    fn raw(&self, args: &[&str], stdin: &[u8]) -> Vec<u8> {
        // The child shares a flock until it execs (see `common::spawn_guard`):
        // the read side keeps this spawn out of a lock-release window of
        // another test.
        let mut child = common::spawn_guard::across_spawn(|| {
            Command::new("git")
                // No auto gc or maintenance: they pack loose objects in the
                // background and delete the loose files, also while the
                // import of the same repository still reads them.
                .args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"])
                .args(args)
                .current_dir(self.path())
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_AUTHOR_NAME", "Ada")
                .env("GIT_AUTHOR_EMAIL", "ada@example.com")
                .env("GIT_AUTHOR_DATE", "1700000000 +0530")
                .env("GIT_COMMITTER_NAME", "Bob")
                .env("GIT_COMMITTER_EMAIL", "bob@example.com")
                .env("GIT_COMMITTER_DATE", "1700000100 -0700")
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
        out.stdout
    }

    /// Runs git with `stdin` and returns stdout as trimmed text.
    fn run_with(&self, args: &[&str], stdin: &[u8]) -> String {
        String::from_utf8(self.raw(args, stdin))
            .unwrap()
            .trim_end()
            .to_owned()
    }

    /// Runs git and returns stdout as trimmed text.
    fn run(&self, args: &[&str]) -> String {
        self.run_with(args, b"")
    }

    fn head(&self) -> String {
        self.run(&["rev-parse", "HEAD"])
    }

    fn write(&self, path: &str, content: &str) {
        let path = self.path().join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    fn chmod(&self, path: &str, mode: u32) {
        fs::set_permissions(self.path().join(path), fs::Permissions::from_mode(mode)).unwrap();
    }

    fn remove(&self, path: &str) {
        let path = self.path().join(path);
        if path.is_dir() {
            fs::remove_dir_all(path).unwrap();
        } else {
            fs::remove_file(path).unwrap();
        }
    }

    /// Commits the whole work tree. Returns the commit name.
    fn commit(&self, message: &str) -> String {
        self.run(&["add", "-A"]);
        self.run(&["commit", "-q", "-m", message]);
        self.head()
    }

    /// Adds commits `from..to` to `main` with `git fast-import`, which is
    /// fast for long histories. Commit `i` puts `g/f{i % 50}`. Returns the
    /// new head.
    fn fast_commits(&self, from: usize, to: usize) -> String {
        let mut stream = Vec::new();
        for i in from..to {
            let (message, content) = (format!("c{i}\n"), format!("{i}\n"));
            let when = 1_700_000_000 + i;
            writeln!(stream, "commit refs/heads/main").unwrap();
            writeln!(stream, "committer Bob <bob@example.com> {when} +0000").unwrap();
            write!(stream, "data {}\n{message}", message.len()).unwrap();
            if i == from && from > 0 {
                writeln!(stream, "from refs/heads/main^0").unwrap();
            }
            writeln!(stream, "M 100644 inline g/f{}", i % 50).unwrap();
            writeln!(stream, "data {}\n{content}", content.len()).unwrap();
        }
        self.run_with(&["fast-import", "--quiet"], &stream);
        self.run(&["rev-parse", "main"])
    }
}

/// Commits of the main fixture, by role.
struct History {
    root: String,
    swap: String,
    side: String,
    main: String,
    merge: String,
    non_utf8: String,
    submodule: String,
}

/// Builds a history that covers every entry kind, file and directory swaps,
/// deletes, a merge, a submodule and a non-UTF-8 message.
fn fixture() -> (Git, History) {
    let git = Git::init();
    git.write("README.md", "hello\n");
    git.write("run.sh", "#!/bin/sh\necho hi\n");
    git.chmod("run.sh", 0o755);
    std::os::unix::fs::symlink("README.md", git.path().join("link")).unwrap();
    git.write("empty.txt", "");
    git.write("swap", "a file for now\n");
    git.write("dir2/x", "x\n");
    git.write("src/lib.rs", "pub fn f() {}\n");
    git.write("src/nested/deep/file.txt", "deep\n");
    let root = git.commit("root");

    git.write("README.md", "hello again\n");
    git.remove("swap");
    git.write("swap/inner.txt", "a directory now\n");
    git.remove("dir2");
    git.write("dir2", "a file now\n");
    git.remove("src/lib.rs");
    git.chmod("run.sh", 0o644);
    let swap = git.commit("swap files and directories");

    git.run(&["checkout", "-q", "-b", "side"]);
    git.write("side.txt", "side\n");
    git.write("src/nested/deep/file.txt", "deeper\n");
    let side = git.commit("side work");

    git.run(&["checkout", "-q", "main"]);
    git.write("main.txt", "main\n");
    git.remove("link");
    let main = git.commit("main work");

    git.run(&["merge", "-q", "--no-ff", "side", "-m", "merge side"]);
    let merge = git.head();

    git.remove("src/nested");
    git.run(&["add", "-A"]);
    // A non-UTF-8 commit encoding stops git from re-encoding the message
    // as Latin-1, and adds an `encoding` header that the import drops.
    git.run_with(
        &[
            "-c",
            "i18n.commitEncoding=ISO-8859-1",
            "commit",
            "-q",
            "-F",
            "-",
        ],
        NON_UTF8_MESSAGE,
    );
    let non_utf8 = git.head();

    git.run(&[
        "update-index",
        "--add",
        "--cacheinfo",
        &format!("160000,{SUBMODULE},sub"),
    ]);
    git.run(&["commit", "-q", "-m", "add submodule"]);
    let submodule = git.head();

    let history = History {
        root,
        swap,
        side,
        main,
        merge,
        non_utf8,
        submodule,
    };
    (git, history)
}

fn opts(rev: &str) -> ImportOptions {
    ImportOptions {
        rev: rev.to_owned(),
        target_ref: "main".to_owned(),
        verify_sample: 1000,
    }
}

fn open_store() -> (TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}

fn try_import(store: &Store, git: &Git, rev: &str) -> Result<ImportReport, Error> {
    common::spawn_guard::prime_gix_lookup();
    import_git(store, "r", git.path(), &opts(rev))
}

fn import(store: &Store, git: &Git, rev: &str) -> ImportReport {
    try_import(store, git, rev).unwrap()
}

/// The changeset that a git commit was imported as.
fn changeset_of(store: &Store, commit: &str) -> ChangesetId {
    store
        .read(|c| {
            let cs: Vec<u8> = c.query_row(
                "SELECT cs FROM git_import WHERE git_oid = ?1",
                [oid(commit).as_slice()],
                |row| row.get(0),
            )?;
            Ok(ChangesetId::try_from(cs.as_slice()).unwrap())
        })
        .unwrap()
}

fn oid(hex: &str) -> ObjectId {
    ObjectId::from_hex(hex.as_bytes()).unwrap()
}

fn row_counts(c: &Connection) -> Vec<i64> {
    [
        "changesets",
        "changeset_parents",
        "repo_changesets",
        "trees",
        "derived",
        "git_import",
        "refs",
    ]
    .iter()
    .map(|table| {
        c.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    })
    .collect()
}

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

/// The root directory entry at `name`.
fn root_entry(store: &Store, cs: ChangesetId, name: &str) -> Option<Entry> {
    read::list_dir(store, "r", cs, None)
        .unwrap()
        .entries
        .remove(&PathElem::new(name).unwrap())
}

#[test]
fn import_matches_git_and_repeats_as_a_no_op() {
    let (git, h) = fixture();
    let (_dir, store) = open_store();

    let report = import(&store, &git, "HEAD");
    assert_eq!(
        (report.commits, report.imported, report.verified),
        (7, 7, 7),
        "every commit is a root, a merge or sampled, so every one is verified"
    );
    assert_eq!(report.tip, changeset_of(&store, &h.submodule));
    assert_eq!(read::resolve_ref(&store, "r", "main").unwrap(), report.tip);

    let before = store.read(|c| Ok(row_counts(c))).unwrap();
    let again = import(&store, &git, "HEAD");
    assert_eq!((again.commits, again.imported, again.verified), (7, 0, 0));
    assert_eq!(again.tip, report.tip);
    assert_eq!(store.read(|c| Ok(row_counts(c))).unwrap(), before);
    let main = store
        .read(|c| db::get_ref(c, db::repo_id(c, "r")?, "main"))
        .unwrap()
        .unwrap();
    assert_eq!(main.version, 1, "a repeated import does not move the ref");
}

#[test]
fn files_read_back_with_git_content() {
    let (git, h) = fixture();
    let (_dir, store) = open_store();
    import(&store, &git, "HEAD");

    for commit in [&h.root, &h.merge, &h.submodule] {
        let cs = changeset_of(&store, commit);
        let listing = git.run(&["ls-tree", "-r", commit]);
        let mut checked = 0;
        for line in listing.lines() {
            let (meta, name) = line.split_once('\t').unwrap();
            let fields: Vec<&str> = meta.split(' ').collect();
            let content = read::read_file(&store, "r", cs, &path(name));
            if fields[1] == "commit" {
                assert!(matches!(content, Err(Error::InvalidArgument(_))));
                continue;
            }
            let expected = git.raw(&["cat-file", "blob", fields[2]], b"");
            assert_eq!(content.unwrap(), expected, "{name} at {commit}");
            checked += 1;
        }
        assert!(checked >= 5, "{commit} has files to compare");
    }

    // Kinds and flags, which the content alone does not show.
    let root = changeset_of(&store, &h.root);
    let tip = changeset_of(&store, &h.submodule);
    let exec = |cs, name| match root_entry(&store, cs, name) {
        Some(Entry::File(FileState::Regular { executable, .. })) => executable,
        other => panic!("{name}: {other:?}"),
    };
    assert!(exec(root, "run.sh"));
    assert!(!exec(tip, "run.sh"));
    assert!(matches!(
        root_entry(&store, root, "link"),
        Some(Entry::File(FileState::Symlink { .. }))
    ));
    let submodule = GitOid::Sha1(oid(SUBMODULE).as_slice().try_into().unwrap());
    assert_eq!(
        root_entry(&store, tip, "sub"),
        Some(Entry::File(FileState::Gitlink { commit: submodule }))
    );
    assert_eq!(root_entry(&store, tip, "link"), None, "deleted on main");
    assert!(matches!(
        root_entry(&store, tip, "swap"),
        Some(Entry::Dir(_))
    ));
}

#[test]
fn changesets_carry_git_metadata_and_first_parent_diffs() {
    let (git, h) = fixture();
    let (_dir, store) = open_store();
    import(&store, &git, "HEAD");
    let changeset =
        |commit: &str| read::get_changeset(&store, "r", changeset_of(&store, commit)).unwrap();

    let swap = changeset(&h.swap);
    let changes: BTreeMap<&[u8], bool> = swap
        .changes
        .iter()
        .map(|(p, state)| (p.as_bytes(), state.is_some()))
        .collect();
    let expected: BTreeMap<&[u8], bool> = [
        (&b"README.md"[..], true),
        (b"dir2", true),
        (b"dir2/x", false),
        (b"run.sh", true),
        (b"src/lib.rs", false),
        (b"swap", false),
        (b"swap/inner.txt", true),
    ]
    .into();
    assert_eq!(changes, expected, "true is a put, false a delete");
    assert_eq!(swap.parents, [changeset_of(&store, &h.root)]);
    assert_eq!(
        swap.author,
        IdentityTime {
            name: b"Ada".to_vec(),
            email: b"ada@example.com".to_vec(),
            unix_secs: 1_700_000_000,
            tz_offset_minutes: 330,
        }
    );
    assert_eq!(
        swap.committer,
        Some(IdentityTime {
            name: b"Bob".to_vec(),
            email: b"bob@example.com".to_vec(),
            unix_secs: 1_700_000_100,
            tz_offset_minutes: -420,
        })
    );
    assert_eq!(swap.message, b"swap files and directories\n");

    let merge = changeset(&h.merge);
    assert_eq!(
        merge.parents,
        [changeset_of(&store, &h.main), changeset_of(&store, &h.side)]
    );
    let merged: Vec<&[u8]> = merge.changes.keys().map(RepoPath::as_bytes).collect();
    assert_eq!(
        merged,
        [&b"side.txt"[..], b"src/nested/deep/file.txt"],
        "a merge stores its diff against the first parent"
    );

    assert_eq!(changeset(&h.non_utf8).message, NON_UTF8_MESSAGE);
    assert_eq!(changeset(&h.root).parents, []);
}

#[test]
fn log_follows_git_first_parents() {
    let (git, _) = fixture();
    let (_dir, store) = open_store();
    let report = import(&store, &git, "HEAD");

    let expected: Vec<ChangesetId> = git
        .run(&["log", "--first-parent", "--format=%H"])
        .lines()
        .map(|commit| changeset_of(&store, commit))
        .collect();
    let later = Deadline::new(Instant::now() + Duration::from_secs(600));
    let log: Vec<ChangesetId> = read::log_page(&store, "r", report.tip, None, 100, &later)
        .unwrap()
        .entries
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(log, expected);
}

#[test]
fn incremental_import_adds_only_new_commits() {
    let (git, h) = fixture();
    let (_dir, store) = open_store();
    let first = import(&store, &git, &h.swap);
    assert_eq!((first.commits, first.imported), (2, 2));

    let rest = import(&store, &git, "HEAD");
    assert_eq!((rest.commits, rest.imported, rest.verified), (7, 5, 5));
    assert_eq!(read::resolve_ref(&store, "r", "main").unwrap(), rest.tip);

    let (_fresh_dir, fresh) = open_store();
    assert_eq!(
        import(&fresh, &git, "HEAD").tip,
        rest.tip,
        "IDs do not depend on how the import was split"
    );
}

#[test]
fn modes_100644_and_100664_are_the_same_file() {
    let git = Git::init();
    let blob = oid(&git.run_with(&["hash-object", "-w", "--stdin"], b"x\n"));
    // `git mktree` rewrites 100664 to 100644, so write the tree bytes directly.
    let mktree = |mode: &str| {
        let body = [format!("{mode} a\0").as_bytes(), blob.as_slice()].concat();
        let args = ["hash-object", "-t", "tree", "-w", "--literally", "--stdin"];
        git.run_with(&args, &body)
    };
    let (t1, t2) = (mktree("100644"), mktree("100664"));
    assert!(
        git.raw(&["cat-file", "tree", &t2], b"")
            .starts_with(b"100664 ")
    );
    let c1 = git.run(&["commit-tree", &t1, "-m", "one"]);
    let c2 = git.run(&["commit-tree", &t2, "-p", &c1, "-m", "two"]);

    let (_dir, store) = open_store();
    let report = import(&store, &git, &c2);
    assert_eq!(report.verified, 2);
    let cs = read::get_changeset(&store, "r", report.tip).unwrap();
    assert!(cs.changes.is_empty(), "{:?}", cs.changes);
}

#[test]
fn a_path_scion_cannot_store_fails_the_import() {
    let git = Git::init();
    let deep = "d/".repeat(256);
    git.write(&format!("{deep}f"), "too deep\n");
    let commit = git.commit("257 parts");

    let (_dir, store) = open_store();
    match try_import(&store, &git, "HEAD") {
        Err(Error::Git(msg)) => {
            assert!(msg.contains(&commit), "{msg}");
            assert!(msg.contains("d/d/d/"), "{msg}");
        }
        other => panic!("expected a git error, got {other:?}"),
    }
}

#[test]
fn verification_catches_a_tree_that_differs_from_git() {
    let (git, h) = fixture();
    let (_dir, store) = open_store();
    let first = import(&store, &git, &h.root);

    // Corrupt the root commit's derived tree: add a file git does not have.
    // The next commit applies its changes on top, so it inherits the file.
    store
        .write(|c| {
            let root = tree::derive_root_tree(c, db::repo_id(c, "r")?, first.tip)?;
            let extra = BTreeMap::from([(
                path("extra.txt"),
                Some(FileState::Regular {
                    content: scion_core::BlobId::of(b""),
                    executable: false,
                }),
            )]);
            let bad = tree::apply(c, Some(root), &extra)?;
            c.execute(
                "UPDATE derived SET value = ?1 WHERE kind = 'root_tree' AND cs = ?2",
                params![bad.as_bytes(), first.tip.as_bytes()],
            )?;
            Ok(())
        })
        .unwrap();

    match try_import(&store, &git, &h.swap) {
        Err(Error::Corrupt(msg)) => {
            assert!(msg.contains("extra.txt") && msg.contains(&h.swap), "{msg}");
        }
        other => panic!("expected a verification error, got {other:?}"),
    }
    assert_eq!(
        read::resolve_ref(&store, "r", "main").unwrap(),
        first.tip,
        "a failed import does not move the ref"
    );
    let rows: i64 = store
        .read(|c| Ok(c.query_row("SELECT count(*) FROM git_import", [], |r| r.get(0))?))
        .unwrap();
    assert_eq!(rows, 1, "the failed batch rolled back");
}

#[test]
fn verification_catches_missing_content() {
    let (git, h) = fixture();
    let (dir, store) = open_store();
    import(&store, &git, &h.root);
    fs::remove_dir_all(dir.path().join("objects")).unwrap();
    fs::create_dir_all(dir.path().join("objects").join("blob")).unwrap();

    match try_import(&store, &git, &h.swap) {
        Err(Error::Corrupt(msg)) => assert!(msg.contains("is not stored"), "{msg}"),
        other => panic!("expected a verification error, got {other:?}"),
    }
}

#[test]
fn verification_catches_a_corrupt_blob() {
    let (git, h) = fixture();
    let (dir, store) = open_store();
    import(&store, &git, &h.root);
    // The next commit does not touch this file, so its import does not
    // rewrite the blob, and its check reads the damaged file back.
    let hex = BlobId::of(b"deep\n").to_string();
    let file = dir
        .path()
        .join("objects/blob")
        .join(&hex[..2])
        .join(&hex[2..]);
    fs::write(file, zstd::bulk::compress(b"other content\n", 3).unwrap()).unwrap();

    match try_import(&store, &git, &h.swap) {
        Err(Error::Corrupt(msg)) => {
            assert!(msg.contains("src/nested/deep/file.txt"), "{msg}");
            assert!(msg.contains("is corrupt"), "{msg}");
        }
        other => panic!("expected a verification error, got {other:?}"),
    }
}

fn main_ref(store: &Store) -> db::RefState {
    store
        .read(|c| db::get_ref(c, db::repo_id(c, "r")?, "main"))
        .unwrap()
        .unwrap()
}

/// Asserts that an import failed because `main` is not an ancestor of the
/// imported tip, and that the error names both.
fn assert_diverged(result: Result<ImportReport, Error>, main: ChangesetId, tip: ChangesetId) {
    match result {
        Err(Error::FailedPrecondition(msg)) => {
            assert!(msg.contains("diverged from git"), "{msg}");
            assert!(msg.contains(&main.to_string()), "{msg}");
            assert!(msg.contains(&tip.to_string()), "{msg}");
        }
        other => panic!("expected a diverged ref, got {other:?}"),
    }
}

#[test]
fn import_never_moves_a_ref_off_a_land() {
    let (git, h) = fixture();
    let (_dir, store) = open_store();
    let imported = import(&store, &git, "HEAD").tip;
    assert_eq!(imported, changeset_of(&store, &h.submodule));

    // A changeset created and landed on `main` after the import.
    let created = create(&store, imported, "landed.txt");
    let outcome = land(&store, &Lands::default(), created);
    assert_eq!(outcome.state, LandState::Landed);
    let landed = main_ref(&store);
    assert_eq!(landed.target, created);

    // The same git tip again: `main` already contains it, so nothing moves.
    let again = try_import(&store, &git, "HEAD").unwrap();
    assert_eq!((again.imported, again.tip), (0, imported));
    assert_eq!(main_ref(&store), landed);

    // A git descendant of that tip does not have the land either.
    git.write("after.txt", "git moved on\n");
    let next = git.commit("git moves on");
    assert_diverged(
        try_import(&store, &git, &next),
        created,
        changeset_of(&store, &next),
    );
    assert_eq!(main_ref(&store), landed, "the land is not discarded");

    // The commits are imported, so another ref can take them.
    let mut other = opts(&next);
    other.target_ref = "git/main".to_owned();
    common::spawn_guard::prime_gix_lookup();
    let report = import_git(&store, "r", git.path(), &other).unwrap();
    assert_eq!(report.imported, 0);
    assert_eq!(
        read::resolve_ref(&store, "r", "git/main").unwrap(),
        report.tip
    );
}

#[test]
fn criss_cross_merges_import_and_the_ref_moves_only_to_descendants() {
    let git = Git::init();
    git.write("base.txt", "base\n");
    git.commit("base");
    git.run(&["checkout", "-q", "-b", "b"]);
    git.write("b.txt", "b\n");
    git.commit("b1");
    git.run(&["checkout", "-q", "main"]);
    git.write("a.txt", "a\n");
    let a1 = git.commit("a1");
    // Two merges of a1 and b1 with the parents swapped, each extended.
    git.run(&["merge", "-q", "--no-ff", "b", "-m", "m1 = a1 + b1"]);
    let m1 = git.head();
    git.write("a.txt", "a2\n");
    let a2 = git.commit("a2");
    git.run(&["checkout", "-q", "b"]);
    git.run(&["merge", "-q", "--no-ff", &a1, "-m", "m2 = b1 + a1"]);
    let m2 = git.head();
    git.write("c.txt", "c\n");
    let b2 = git.commit("b2");
    // A merge with two merge bases, a1 and b1.
    git.run(&["checkout", "-q", "main"]);
    git.run(&["merge", "-q", "--no-ff", "b", "-m", "criss-cross"]);
    let tip = git.head();

    let (_dir, store) = open_store();
    import(&store, &git, &a1);
    // a1 is only the second parent of m2.
    import(&store, &git, &m2);
    assert_eq!(main_ref(&store).target, changeset_of(&store, &m2));
    // m1 and m2 merge the same commits, but neither descends from the other.
    assert_diverged(
        try_import(&store, &git, &m1),
        changeset_of(&store, &m2),
        changeset_of(&store, &m1),
    );
    assert_eq!(main_ref(&store).target, changeset_of(&store, &m2));

    let report = import(&store, &git, &tip);
    assert_eq!(
        (report.commits, report.imported, report.verified),
        (8, 3, 3),
        "a2, b2 and the criss-cross merge are new; m1 was imported above"
    );
    assert_eq!(main_ref(&store).target, report.tip);
    let merge = read::get_changeset(&store, "r", report.tip).unwrap();
    assert_eq!(
        merge.parents,
        [changeset_of(&store, &a2), changeset_of(&store, &b2)]
    );
    for name in ["a.txt", "b.txt", "c.txt", "base.txt"] {
        assert!(root_entry(&store, report.tip, name).is_some(), "{name}");
    }
}

/// `scion-server --data <data> <args>`, to run as another process.
fn server_process(data: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    command.arg("--data").arg(data).args(args);
    command.env("RUST_LOG", "warn");
    command
}

/// `scion-server import` of `rev` into ref `name` of repo `r`.
fn import_process(data: &Path, git: &Git, rev: &str, name: &str) -> Command {
    let mut command = server_process(data, &["import", "--repo", "r", "--rev", rev]);
    command
        .args(["--ref", name, "--verify", "10"])
        .arg(git.path());
    command
}

fn assert_success(out: &Output) {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn identity() -> IdentityTime {
    IdentityTime {
        name: b"Ada".to_vec(),
        email: b"ada@example.com".to_vec(),
        unix_secs: 1_700_000_200,
        tz_offset_minutes: 0,
    }
}

/// Creates a changeset on `base` that puts `path`, through the write path.
fn create(store: &Store, base: ChangesetId, path: &str) -> ChangesetId {
    let repo = store.read(|c| db::repo_id(c, "r")).unwrap();
    let upload = store.blobs().upload().unwrap();
    let upload = upload.append(path.as_bytes()).unwrap();
    let file = FileState::Regular {
        content: write::put_blob(store, repo, upload).unwrap(),
        executable: false,
    };
    let cs = Changeset {
        parents: vec![base],
        author: identity(),
        committer: None,
        message: path.as_bytes().to_vec(),
        changes: BTreeMap::from([(self::path(path), Some(file))]),
    };
    write::create_changeset(store, "r", &cs.encode()).unwrap()
}

/// Lands `cs` on `main`, and retries with the same key while the ref moves.
fn land(store: &Store, lands: &Lands, cs: ChangesetId) -> scion_server::land::LandOutcome {
    let request = LandRequest {
        repo: "r".into(),
        changeset: cs,
        target_ref: "main".into(),
        request_key: cs.to_string(),
        deadline: Deadline::new(Instant::now() + Duration::from_secs(600)),
    };
    loop {
        match lands.land(store, &Caller("t".into()), &request) {
            Err(Error::Aborted(_)) => {}
            outcome => return outcome.unwrap(),
        }
    }
}

#[test]
fn an_import_process_shares_the_data_directory_with_lands() {
    let git = Git::init();
    let first = git.fast_commits(0, 1);
    let tip = git.fast_commits(1, 60);
    // The server's store and lands; the imports run as other processes.
    let (dir, store) = open_store();
    let lands = Lands::default();
    let data = dir.path();
    assert_success(&import_process(data, &git, &first, "main").output().unwrap());
    let start = main_ref(&store).target;

    // A land whose base is not on main builds main's path index at the
    // start, and leaves main alone.
    let repo = store.read(|c| db::repo_id(c, "r")).unwrap();
    let side = Changeset {
        parents: vec![],
        author: identity(),
        committer: None,
        message: b"side".to_vec(),
        changes: BTreeMap::new(),
    };
    let side = store
        .write(|c| db::insert_changeset(c, repo, &side))
        .unwrap();
    let out = land(&store, &lands, create(&store, side, "s.txt"));
    assert_eq!(out.state, LandState::Rejected, "{}", out.detail);
    assert_eq!(main_ref(&store).target, start);

    // The other process moves main forward. The next plan reads the new
    // head, so the index sees the imported changes.
    assert_success(&import_process(data, &git, &tip, "main").output().unwrap());
    let imported = changeset_of(&store, &tip);
    assert_eq!(main_ref(&store).target, imported);
    let out = land(&store, &lands, create(&store, start, "g/f5"));
    assert_eq!(out.state, LandState::Conflict, "{}", out.detail);
    assert_eq!(out.conflict_paths, [path("g/f5")]);
    let out = land(&store, &lands, create(&store, start, "mine.txt"));
    assert_eq!(out.state, LandState::Landed, "{}", out.detail);
    let mine = out.result.unwrap();
    let parents = read::get_changeset(&store, "r", mine).unwrap().parents;
    assert_eq!(parents, [imported]);

    // Agents land on main while another process imports more commits into
    // another ref.
    let more = git.fast_commits(60, 400);
    let import = common::spawn_guard::across_spawn(|| {
        import_process(data, &git, &more, "git/main")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
    })
    .unwrap();
    let done = AtomicBool::new(false);
    let landed: Vec<ChangesetId> = thread::scope(|scope| {
        let agents: Vec<_> = (0..4)
            .map(|agent| {
                let (store, lands, done) = (&store, &lands, &done);
                scope.spawn(move || {
                    let mut landed = Vec::new();
                    while !done.load(Ordering::Relaxed) || landed.is_empty() {
                        let head = main_ref(store).target;
                        let cs = create(store, head, &format!("agent{agent}/{}", landed.len()));
                        let out = land(store, lands, cs);
                        assert_eq!(out.state, LandState::Landed, "{}", out.detail);
                        landed.push(out.result.unwrap());
                    }
                    landed
                })
            })
            .collect();
        let out = import.wait_with_output().unwrap();
        done.store(true, Ordering::Relaxed);
        assert_success(&out);
        agents.into_iter().flat_map(|a| a.join().unwrap()).collect()
    });

    // Every commit is imported, and the other ref points at the tip.
    let rows: i64 = store
        .read(|c| Ok(c.query_row("SELECT count(*) FROM git_import", [], |row| row.get(0))?))
        .unwrap();
    assert_eq!(rows, 400);
    assert_eq!(
        read::resolve_ref(&store, "r", "git/main").unwrap(),
        changeset_of(&store, &more)
    );
    // Main's first-parent history above `mine` is the lands, each once. A
    // fast machine lands more than one log page (`read::MAX_SCAN`), so walk
    // the parents.
    let mut history = Vec::new();
    let mut at = main_ref(&store).target;
    while at != mine {
        assert!(history.len() < landed.len(), "main does not reach {mine}");
        history.push(at);
        at = read::get_changeset(&store, "r", at).unwrap().parents[0];
    }
    let mut want = landed;
    want.sort();
    history.sort();
    assert_eq!(history, want);
}

#[test]
fn a_second_server_process_is_refused() {
    let (dir, _server) = open_store();
    let out = common::spawn_guard::across_spawn(|| {
        server_process(
            dir.path(),
            &["serve", "--token", "t", "--listen", "127.0.0.1:0"],
        )
        .output()
    })
    .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("in use by another server"), "{stderr}");
}
