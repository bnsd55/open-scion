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
use std::sync::mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use gix::ObjectId;
use rusqlite::{Connection, params};
use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileEntry, FileState, GitOid, IdentityTime, PathElem,
    RepoPath,
};
use scion_server::test_support::auth::Caller;
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::import::{BUSY_TRIES, CHECKS};
use scion_server::test_support::land::{LandRequest, LandState, Lands};
use scion_server::test_support::meta::{Meta as _, MetaTx as _, RefState};
use scion_server::test_support::{read, tree, write};
use scion_server::{Backend, Error};

/// A deadline a test does not reach.
fn later() -> Deadline {
    Deadline::new(std::time::Instant::now() + std::time::Duration::from_secs(600))
}
use scion_server::{ImportOptions, ImportReport, import_git};
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
/// Ends the test with one printed line when it runs on Postgres: it
/// asserts the data directory's own mechanics, which the Postgres backend
/// does not have. The PR body lists these tests.
fn skip_on_postgres(why: &str) -> bool {
    if Backend::test_is_postgres() {
        eprintln!("skip on postgres: {why}");
        return true;
    }
    false
}

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

/// A git commit whose date is git's written form of an unknown offset
/// (`-0000`) cannot read back as itself: zero parses to `+0000`. The
/// import must refuse it, not store history that differs from its
/// source without saying so. `git commit-tree` normalizes `-0000` to
/// `+0000`; fast-import writes the date header verbatim.
#[test]
fn an_unknown_timezone_date_is_refused() {
    let git = Git::init();
    let mut stream = Vec::new();
    writeln!(
        stream,
        "commit refs/heads/main\ncommitter B <b@x> 1717000000 -0000\ndata 2\nt\nM 100644 inline f\ndata 2\n1\n"
    )
    .unwrap();
    git.run_with(&["fast-import", "--quiet"], &stream);
    let bad = git.run(&["rev-parse", "main"]);
    // git really wrote the unknown offset.
    assert!(
        git.raw(&["cat-file", "commit", &bad], b"")
            .windows(5)
            .any(|w| w == b"-0000"),
        "the fixture's commit carries -0000"
    );

    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test_shared(dir.path()).unwrap();
    let err = try_import(&store, &git, &bad).unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("is not a date git can write"),
        "the refusal names the date: {text}"
    );
}

/// A negative zero on the author line takes the same refusal: both
/// identities ride `identity()`.
#[test]
fn an_unknown_timezone_on_the_author_line_is_refused() {
    let git = Git::init();
    let mut stream = Vec::new();
    writeln!(
        stream,
        "commit refs/heads/main\nauthor A <a@x> 1717000000 -0000\ncommitter B <b@x> 1717000000 +0000\ndata 2\nt\nM 100644 inline f\ndata 2\n1\n"
    )
    .unwrap();
    git.run_with(&["fast-import", "--quiet"], &stream);
    let bad = git.run(&["rev-parse", "main"]);

    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test_shared(dir.path()).unwrap();
    let err = try_import(&store, &git, &bad).unwrap_err();
    assert!(
        err.to_string().contains("is not a date git can write"),
        "the refusal names the date: {err}"
    );
}

/// A real zone with a sign imports: `+0000` and `-0530` both read back
/// as themselves.
#[test]
fn a_signed_zone_imports() {
    let git = Git::init();
    let mut stream = Vec::new();
    writeln!(
        stream,
        "commit refs/heads/main\nauthor A <a@x> 1717000000 -0530\ncommitter B <b@x> 1717000000 +0000\ndata 2\nt\nM 100644 inline f\ndata 2\n1\n"
    )
    .unwrap();
    git.run_with(&["fast-import", "--quiet"], &stream);
    let head = git.run(&["rev-parse", "main"]);

    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test_shared(dir.path()).unwrap();
    let report = import(&store, &git, &head);
    let cs = changeset_of(&store, &head);
    let repo = store.read(|c| c.repo_id("r")).unwrap();
    let got = store.read(|c| c.get_changeset(repo, cs)).unwrap();
    // -0530 is -5 h 30 m.
    assert_eq!(got.author().tz_offset_minutes(), -330);
    assert_eq!(report.commits, 1);
}

fn opts(rev: &str) -> ImportOptions {
    ImportOptions {
        rev: rev.to_owned(),
        target_ref: "main".to_owned(),
        verify_sample: 1000,
    }
}

fn open_store() -> (TempDir, Backend) {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    (dir, store)
}

fn try_import(store: &Backend, git: &Git, rev: &str) -> Result<ImportReport, Error> {
    common::spawn_guard::prime_gix_lookup();
    import_git(store, "r", git.path(), &opts(rev))
}

fn import(store: &Backend, git: &Git, rev: &str) -> ImportReport {
    try_import(store, git, rev).unwrap()
}

/// The changeset that a git commit was imported as.
fn changeset_of(store: &Backend, commit: &str) -> ChangesetId {
    let oid = gix::ObjectId::from_hex(commit.as_bytes()).unwrap();
    store
        .read(|c| {
            let repo = c.repo_id("r")?;
            Ok(c.imported_commits(repo)?
                .get(&oid)
                .copied()
                .unwrap_or_else(|| panic!("{commit} is not imported")))
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
fn root_entry(store: &Backend, cs: ChangesetId, name: &str) -> Option<Entry> {
    let root = read::root_tree(
        store,
        "r",
        cs,
        &Deadline::new(Instant::now() + Duration::from_secs(300)),
    )
    .unwrap();
    store
        .read(|c| c.get_tree(root))
        .unwrap()
        .entries()
        .get(&PathElem::new(name).unwrap())
        .copied()
}

#[test]
fn import_matches_git_and_repeats_as_a_no_op() {
    // Asserts the rows of the data directory's database.
    if skip_on_postgres("asserts the data directory's mechanics") {
        return;
    }

    let (git, h) = fixture();
    let (_dir, store) = open_store();

    let report = import(&store, &git, "HEAD");
    assert_eq!(
        (report.commits, report.imported, report.verified),
        (7, 7, 7),
        "every commit is a root, a merge or sampled, so every one is verified"
    );
    assert_eq!(report.tip, changeset_of(&store, &h.submodule));
    assert_eq!(store.resolve_ref("r", "main").unwrap(), report.tip);

    let before = store.read(|c| Ok(row_counts(c.sqlite().unwrap()))).unwrap();
    let again = import(&store, &git, "HEAD");
    assert_eq!((again.commits, again.imported, again.verified), (7, 0, 0));
    assert_eq!(again.tip, report.tip);
    assert_eq!(
        store.read(|c| Ok(row_counts(c.sqlite().unwrap()))).unwrap(),
        before
    );
    let main = store
        .read(|c| {
            let repo = c.repo_id("r")?;
            c.get_ref(repo, "main")
        })
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
            let deadline = Deadline::new(Instant::now() + Duration::from_secs(300));
            let content = read::read_file(&store, "r", cs, &path(name), &deadline);
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
        Some(Entry::File(file)) => match file.state() {
            FileState::Regular { executable, .. } => executable,
            _ => panic!("{name}: not a regular file"),
        },
        other => panic!("{name}: {other:?}"),
    };
    assert!(exec(root, "run.sh"));
    assert!(!exec(tip, "run.sh"));
    assert!(matches!(
        root_entry(&store, root, "link"),
        Some(Entry::File(file)) if matches!(file.state(), FileState::Symlink { .. })
    ));
    let submodule = GitOid::Sha1(oid(SUBMODULE).as_slice().try_into().unwrap());
    assert_eq!(
        root_entry(&store, tip, "sub"),
        Some(Entry::File(FileEntry::new(
            FileState::Gitlink { commit: submodule },
            0
        )))
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
        .changes()
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
    assert_eq!(swap.parents(), [changeset_of(&store, &h.root)]);
    assert_eq!(
        swap.author(),
        &IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            330
        )
        .expect("identity fits the format")
    );
    assert_eq!(
        swap.committer(),
        Some(
            &IdentityTime::new(
                b"Bob".to_vec(),
                b"bob@example.com".to_vec(),
                1_700_000_100,
                -420
            )
            .expect("identity fits the format")
        )
    );
    assert_eq!(swap.message(), b"swap files and directories\n");

    let merge = changeset(&h.merge);
    assert_eq!(
        merge.parents(),
        [changeset_of(&store, &h.main), changeset_of(&store, &h.side)]
    );
    let merged: Vec<&[u8]> = merge.changes().keys().map(RepoPath::as_bytes).collect();
    assert_eq!(
        merged,
        [&b"side.txt"[..], b"src/nested/deep/file.txt"],
        "a merge stores its diff against the first parent"
    );

    assert_eq!(changeset(&h.non_utf8).message(), NON_UTF8_MESSAGE);
    assert_eq!(changeset(&h.root).parents(), []);
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
    assert_eq!(store.resolve_ref("r", "main").unwrap(), rest.tip);

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
    assert!(cs.changes().is_empty(), "{:?}", cs.changes());
}

#[test]
fn a_path_scion_cannot_store_fails_the_import() {
    let git = Git::init();
    let deep = "d/".repeat(256);
    git.write(&format!("{deep}f"), "too deep\n");
    let commit = git.commit("257 parts");

    let (_dir, store) = open_store();
    match try_import(&store, &git, "HEAD") {
        Err(e @ Error::Git(_)) => {
            let msg = e.to_string();
            assert!(msg.contains(&commit), "{msg}");
            assert!(msg.contains("d/d/d/"), "{msg}");
        }
        other => panic!("expected a git error, got {other:?}"),
    }
}

#[test]
fn verification_catches_a_tree_that_differs_from_git() {
    // Asserts the rows of the data directory's database.
    if skip_on_postgres("asserts the data directory's mechanics") {
        return;
    }

    let (git, h) = fixture();
    let (_dir, store) = open_store();
    let first = import(&store, &git, &h.root);

    // Corrupt the root commit's derived tree: add a file git does not have.
    // The next commit applies its changes on top, so it inherits the file.
    store
        .write(|c| {
            let repo = c.repo_id("r")?;
            let root = tree::derive_root_tree(c, repo, first.tip, &mut |_| Ok(0))?;
            let extra = BTreeMap::from([(
                path("extra.txt"),
                Some(FileState::Regular {
                    content: scion_core::BlobId::of(b""),
                    executable: false,
                }),
            )]);
            let bad = tree::apply(c, Some(root), &extra, &mut |_| Ok(0))?;
            c.sqlite()
                .unwrap()
                .execute(
                    "UPDATE derived SET value = ?1 WHERE kind = 'root_tree' AND cs = ?2",
                    params![bad.as_bytes(), first.tip.as_bytes()],
                )
                .map_err(scion_server::test_support::error::from_sqlite)?;
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
        store.resolve_ref("r", "main").unwrap(),
        first.tip,
        "a failed import does not move the ref"
    );
    let rows: i64 = store
        .read(|c| {
            c.sqlite()
                .unwrap()
                .query_row("SELECT count(*) FROM git_import", [], |r| r.get(0))
                .map_err(scion_server::test_support::error::from_sqlite)
        })
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

fn main_ref(store: &Backend) -> RefState {
    store
        .read(|c| {
            let repo = c.repo_id("r")?;
            c.get_ref(repo, "main")
        })
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
fn a_changeset_with_a_repeated_parent_stores_and_reads_back_the_same() {
    // `Changeset::new` accepts a repeated parent, and git can produce
    // one (`commit-tree -p X -p X`); whichever backend stores it, it
    // reads back with the same parents, one row per parent place.
    let (git, _h) = fixture();
    let (_dir, store) = open_store();
    let imported = import(&store, &git, "HEAD").tip;

    let cs = Changeset::new(
        vec![imported, imported],
        identity(),
        None,
        b"repeated parent\n".to_vec(),
        BTreeMap::new(),
    )
    .expect("changeset fits the format");
    let id = store
        .write(|c| {
            let repo = c.repo_id("r")?;
            c.insert_changeset(repo, &cs)
        })
        .unwrap();
    let stored = store
        .read(|c| {
            let repo = c.repo_id("r")?;
            c.get_changeset(repo, id)
        })
        .unwrap();
    assert_eq!(stored.parents(), &[imported, imported]);
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
    assert_eq!(store.resolve_ref("r", "git/main").unwrap(), report.tip);
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
        merge.parents(),
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

/// `scion-server import` that opens the test's Postgres schema: the test
/// URL plus a `search_path` startup option, since the binary names no
/// schema and the tests of one data directory share one. The data
/// directory still holds the blobs and the backend marker.
fn import_process_on_postgres(data: &Path, git: &Git, rev: &str, name: &str) -> Command {
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let schema = Backend::test_schema(data).unwrap();
    let sep = if url.contains('?') { '&' } else { '?' };
    // `--postgres-url` is a flag of the root command, so it goes before
    // the subcommand.
    let mut command = server_process(
        data,
        &[
            "--postgres-url",
            &format!("{url}{sep}options=--search_path%3D{schema}"),
            "import",
            "--repo",
            "r",
            "--rev",
            rev,
        ],
    );
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
    IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_200,
        0,
    )
    .expect("identity fits the format")
}

/// Creates a changeset on `base` that puts `path`, through the write path.
fn create(store: &Backend, base: ChangesetId, path: &str) -> ChangesetId {
    let repo = store.read(|c| c.repo_id("r")).unwrap();
    let upload = store.blobs().upload().unwrap();
    let upload = upload.append(path.as_bytes()).unwrap();
    let file = FileState::Regular {
        content: write::put_blob(store, repo, upload).unwrap(),
        executable: false,
    };
    let cs = Changeset::new(
        vec![base],
        identity(),
        None,
        path.as_bytes().to_vec(),
        BTreeMap::from([(self::path(path), Some(file))]),
    )
    .expect("changeset fits the format");
    write::create_changeset(store, "r", &cs.encode(), &later()).unwrap()
}

/// Lands `cs` on `main`, and retries with the same key while the ref moves.
fn land(
    store: &Backend,
    lands: &Lands,
    cs: ChangesetId,
) -> scion_server::test_support::land::LandOutcome {
    let request = LandRequest {
        repo: "r".into(),
        changeset: cs,
        target_ref: "main".into(),
        request_key: cs.to_string(),
        deadline: Deadline::new(Instant::now() + Duration::from_secs(600)),
    };
    // A ceiling on the retries: a land that aborts forever must fail the
    // test, not hang it.
    for tries in 0..100 {
        match lands.land(store, &Caller("t".into()), &request) {
            Err(Error::Aborted(_)) => {}
            outcome => return outcome.unwrap(),
        }
        assert!(
            tries < 99,
            "the land aborted {tries} times and never landed"
        );
    }
    unreachable!("the land loop exits by returning");
}

#[test]
fn an_import_process_shares_the_data_directory_with_lands() {
    // Asserts the rows of the data directory's database.
    if skip_on_postgres("asserts the data directory's mechanics") {
        return;
    }

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
    let repo = store.read(|c| c.repo_id("r")).unwrap();
    let side = Changeset::new(vec![], identity(), None, b"side".to_vec(), BTreeMap::new())
        .expect("changeset fits the format");
    let side = store.write(|c| c.insert_changeset(repo, &side)).unwrap();
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
    let parents = read::get_changeset(&store, "r", mine)
        .unwrap()
        .parents()
        .to_vec();
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
                    let started = std::time::Instant::now();
                    while !done.load(Ordering::Relaxed) || landed.is_empty() {
                        assert!(
                            started.elapsed() < Duration::from_secs(120),
                            "an agent's lands never landed"
                        );
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
        .read(|c| {
            c.sqlite()
                .unwrap()
                .query_row("SELECT count(*) FROM git_import", [], |row| row.get(0))
                .map_err(scion_server::test_support::error::from_sqlite)
        })
        .unwrap();
    assert_eq!(rows, 400);
    assert_eq!(
        store.resolve_ref("r", "git/main").unwrap(),
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
        at = read::get_changeset(&store, "r", at).unwrap().parents()[0];
    }
    let mut want = landed;
    want.sort();
    history.sort();
    assert_eq!(history, want);
}

#[test]
fn an_import_process_shares_the_postgres_database_with_lands() {
    // The SQLite twin above asserts the data directory's own mechanics, so
    // it skips here. This one runs only on Postgres, where the database
    // sorts its writers out and an import takes no server lock: another
    // process imports while this test's lands write the same schema.
    if !Backend::test_is_postgres() {
        return;
    }
    let git = Git::init();
    let first = git.fast_commits(0, 1);
    let tip = git.fast_commits(1, 60);
    // The server's store and lands; the imports run as other processes.
    let (dir, store) = open_store();
    let lands = Lands::default();
    let data = dir.path();
    let import = |rev: &str, name: &str| {
        assert_success(
            &import_process_on_postgres(data, &git, rev, name)
                .output()
                .unwrap(),
        );
    };
    import(&first, "main");
    let start = main_ref(&store).target;

    // A land whose base is not on main builds main's path index at the
    // start, and leaves main alone.
    let repo = store.read(|c| c.repo_id("r")).unwrap();
    let side = Changeset::new(vec![], identity(), None, b"side".to_vec(), BTreeMap::new())
        .expect("changeset fits the format");
    let side = store.write(|c| c.insert_changeset(repo, &side)).unwrap();
    let out = land(&store, &lands, create(&store, side, "s.txt"));
    assert_eq!(out.state, LandState::Rejected, "{}", out.detail);
    assert_eq!(main_ref(&store).target, start);

    // The other process moves main forward. The next plan reads the new
    // head, so the index sees the imported changes.
    import(&tip, "main");
    let imported = changeset_of(&store, &tip);
    assert_eq!(main_ref(&store).target, imported);
    let out = land(&store, &lands, create(&store, start, "g/f5"));
    assert_eq!(out.state, LandState::Conflict, "{}", out.detail);
    assert_eq!(out.conflict_paths, [path("g/f5")]);
    let out = land(&store, &lands, create(&store, start, "mine.txt"));
    assert_eq!(out.state, LandState::Landed, "{}", out.detail);
    let mine = out.result.unwrap();
    let parents = read::get_changeset(&store, "r", mine)
        .unwrap()
        .parents()
        .to_vec();
    assert_eq!(parents, [imported]);

    // Agents land on main while another process imports more commits into
    // another ref.
    let more = git.fast_commits(60, 400);
    let import = common::spawn_guard::across_spawn(|| {
        import_process_on_postgres(data, &git, &more, "git/main")
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
                    let started = std::time::Instant::now();
                    while !done.load(Ordering::Relaxed) || landed.is_empty() {
                        assert!(
                            started.elapsed() < Duration::from_secs(120),
                            "an agent's lands never landed"
                        );
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

    // Every commit is imported, and the other ref points at the tip. The
    // imported commits come from the store, not from the database's rows.
    let imported_count = store
        .read(|c| {
            let repo = c.repo_id("r")?;
            Ok(c.imported_commits(repo)?.len())
        })
        .unwrap();
    assert_eq!(imported_count, 400);
    assert_eq!(
        store.resolve_ref("r", "git/main").unwrap(),
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
        at = read::get_changeset(&store, "r", at).unwrap().parents()[0];
    }
    let mut want = landed;
    want.sort();
    history.sort();
    assert_eq!(history, want);
}

#[test]
fn a_second_server_process_is_refused() {
    if skip_on_postgres("runs the data directory's server, which locks its database") {
        return;
    }
    let (dir, _server) = open_store();
    // On Postgres the second server carries the URL itself: the database
    // is what is taken, and the refusal comes at its open.
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap_or_default();
    let extra: &[&str] = if std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres") {
        &["--postgres-url", url.as_str()]
    } else {
        &[]
    };
    let mut args = vec!["serve", "--token", "t", "--listen", "127.0.0.1:0"];
    args.extend_from_slice(extra);
    let out =
        common::spawn_guard::across_spawn(|| server_process(dir.path(), &args).output()).unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    let said = if extra.is_empty() {
        stderr.contains("in use by another server")
    } else {
        stderr.contains("another server holds this store")
    };
    assert!(said, "{stderr}");
}

/// Creates the import's repo up front, so the import's first write is a
/// batch, not the repo's create.
fn make_repo(store: &Backend) {
    store.write(|c| c.create_repo("r")).unwrap();
}

/// A store that shares its data directory, so an import keeps its write
/// transactions short and yields between them.
fn open_shared_store() -> (tempfile::TempDir, Backend) {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test_shared(dir.path()).unwrap();
    (dir, store)
}

/// Takes the store's write lock with another connection and keeps it until
/// the import's batch has hit the busy database once: the hold starts at a
/// counted batch boundary, and releases only when a batch retried, so the
/// batch that began inside the hold provably waited out the lock. Its
/// writes are idempotent, so the retry is safe. The thread ends on its own;
/// it commits when the hold is over.
fn hold_until_a_retry(store: &Backend, dir: &Path, before: u64) {
    let (held_tx, held_rx) = mpsc::channel();
    let db = Connection::open(dir.join("scion.db")).unwrap();
    let watcher = store.clone();
    thread::spawn(move || {
        // The lock may be held by a batch of the import; wait for it, then
        // hold it.
        db.busy_timeout(Duration::from_secs(30)).unwrap();
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        held_tx.send(()).unwrap();
        // The import's first write under this hold fails as busy and the
        // batch retries; the hold ends when that retry is counted.
        let deadline = Instant::now() + Duration::from_secs(30);
        while watcher.busy_retries() <= before {
            assert!(
                Instant::now() < deadline,
                "the import never hit the busy lock"
            );
            thread::sleep(Duration::from_millis(5));
        }
        db.execute_batch("COMMIT").unwrap();
    });
    held_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the hold never took the write lock");
}

#[test]
fn an_import_batch_waits_out_a_busy_write_lock() {
    if skip_on_postgres("simulates the data directory's busy write lock") {
        return;
    }
    let git = Git::init();
    let rev = git.fast_commits(0, 3000);
    let (dir, store) = open_shared_store();
    // A busy lock fails fast, so the retry is deterministic: the holder
    // keeps the lock until a batch retried, and the batch runs again whole.
    store.set_busy_timeout(Duration::from_millis(100));
    let store = Arc::new(store);
    let before = store.busy_retries();
    // The hold starts at a counted batch boundary: the hook runs after the
    // first batch's commit has ended and returns only once the hold has the
    // write lock, so the next batch begins inside the hold. A batch is safe
    // to run again whole.
    let holds = dir.path().to_path_buf();
    let hooked = Arc::clone(&store);
    store.set_after_batch(Some(Box::new(move |batch_no| {
        if batch_no == 0 {
            hold_until_a_retry(&hooked, &holds, before);
        }
    })));
    let report = try_import(&store, &git, &rev).unwrap();
    store.set_after_batch(None);
    assert_eq!((report.commits, report.imported), (3000, 3000));
    // The batch that began inside the hold waited out the lock: its try hit
    // the busy database, and a retry ran it again whole.
    assert!(
        store.busy_retries() > before,
        "no batch ever hit the busy lock"
    );
}

#[test]
fn an_import_that_stays_busy_fails_with_the_busy_error() {
    // Asserts the rows of the data directory's database.
    if skip_on_postgres("simulates the data directory's busy write lock") {
        return;
    }

    let git = Git::init();
    let rev = git.fast_commits(0, 60);
    let (_dir, store) = open_store();
    make_repo(&store);
    // Every batch commit fails: the retry bound runs out, and the import
    // gives up with the busy error instead of retrying forever.
    store.fail_next_commits(BUSY_TRIES + 1);
    let error = try_import(&store, &git, &rev).unwrap_err();
    assert!(error.is_busy(), "{error:?}");
    // Nothing was written: the ref is not there, and no changeset is.
    let refs: i64 = store
        .read(|c| {
            c.sqlite()
                .unwrap()
                .query_row("SELECT count(*) FROM refs", [], |row| row.get(0))
                .map_err(scion_server::test_support::error::from_sqlite)
        })
        .unwrap();
    assert_eq!(refs, 0);
    let cs: i64 = store
        .read(|c| {
            c.sqlite()
                .unwrap()
                .query_row("SELECT count(*) FROM changesets", [], |row| row.get(0))
                .map_err(scion_server::test_support::error::from_sqlite)
        })
        .unwrap();
    assert_eq!(cs, 0);
}

/// The Postgres hold of a batch boundary: a client of the test schema
/// takes the changesets table and keeps it until the import's batch has
/// hit the busy database once, as `hold_until_a_retry` holds the data
/// directory's write lock on SQLite. The hold starts at a counted batch
/// boundary, and releases only when a batch retried, so the batch that
/// began inside the hold provably waited out the lock. Its writes are
/// idempotent, so the retry is safe. The thread ends on its own; it
/// commits when the hold is over.
fn hold_until_a_retry_pg(
    store: &Arc<Backend>,
    dir: &Path,
    before: u64,
    failures: &Arc<Mutex<Vec<String>>>,
) {
    let (held_tx, held_rx) = mpsc::channel();
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let Some(schema) = Backend::test_schema(dir) else {
        failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push("no test schema for the hold".to_owned());
        return;
    };
    let watcher = Arc::clone(store);
    let failures = Arc::clone(failures);
    thread::spawn(move || {
        // A failure is reported, not panicked: a panic in this detached
        // thread would be lost, and the test would fail much later with a
        // message that says nothing.
        let fail = |why: String| {
            failures
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(why);
        };
        let mut db = match postgres::Client::connect(&url, postgres::NoTls) {
            Ok(db) => db,
            Err(e) => return fail(format!("the hold never connected: {e}")),
        };
        if let Err(e) = db.batch_execute(&format!("SET search_path TO {schema}")) {
            return fail(format!("the hold never reached the schema: {e}"));
        }
        // The batch's writes are inserts into `changesets`; the lock holds
        // them while reads go past.
        if let Err(e) = db.batch_execute("BEGIN; LOCK TABLE changesets IN EXCLUSIVE MODE;") {
            return fail(format!("the hold never took the table: {e}"));
        }
        if held_tx.send(()).is_err() {
            return; // the test gave up waiting for the hold
        }
        // The import's first write under this hold waits out the 5 s lock
        // clock, answers busy, and the batch retries; the hold ends when
        // that retry is counted.
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        while watcher.busy_retries() <= before {
            if std::time::Instant::now() >= deadline {
                return fail("the import never hit the busy lock".to_owned());
            }
            thread::sleep(Duration::from_millis(5));
        }
        if let Err(e) = db.batch_execute("COMMIT") {
            fail(format!("the hold never let go: {e}"));
        }
    });
    held_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the hold never took the write lock");
}

#[test]
fn an_import_batch_waits_out_a_busy_write_lock_on_postgres() {
    if !Backend::test_is_postgres() {
        return;
    }
    let git = Git::init();
    let rev = git.fast_commits(0, 3000);
    let (dir, store) = open_store();
    let store = Arc::new(store);
    let before = store.busy_retries();
    // The hold starts at a counted batch boundary: the hook runs after the
    // first batch's commit has ended and returns only once the hold has
    // the table, so the next batch begins inside the hold. A batch is safe
    // to run again whole.
    let holds = dir.path().to_path_buf();
    let hooked = Arc::clone(&store);
    // The hold reports what went wrong here, and the test reads the log
    // as soon as the import is over, before the busy assert, whose
    // failure would only say the import answered busy.
    let failures = Arc::new(Mutex::new(Vec::new()));
    let logged = Arc::clone(&failures);
    store.set_after_batch(Some(Box::new(move |batch_no| {
        if batch_no == 0 {
            hold_until_a_retry_pg(&hooked, &holds, before, &logged);
        }
    })));
    let report = try_import(&store, &git, &rev);
    store.set_after_batch(None);
    if let Some(why) = failures
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .first()
    {
        panic!("{why}");
    }
    let report = report.unwrap();
    assert_eq!((report.commits, report.imported), (3000, 3000));
    // The batch that began inside the hold waited out the lock: its try
    // hit the 5 s lock clock, and a retry ran it again whole.
    assert!(
        store.busy_retries() > before,
        "no batch ever hit the busy lock"
    );
}

#[test]
fn an_import_that_loses_disk_space_fails_and_is_not_retried() {
    // The blob files are local in every backend, so the hook below works
    // on Postgres too; both jobs run this test.
    let git = Git::init();
    let rev = git.fast_commits(0, 60);
    let (_dir, store) = open_store();
    let before = store.busy_retries();
    // The import's first blob write is the one the disk refuses.
    store.blobs().fail_writes_after(0);
    // A read works before the import starts.
    assert!(store.read(|c| c.repo_id("r")).is_err());
    let error = try_import(&store, &git, &rev).unwrap_err();
    // The import did not answer busy: the out-of-space answer is not a
    // lock the database holds, and the batch's retry loop does not run.
    assert!(
        matches!(
            &error,
            Error::Io(e) if e.kind() == std::io::ErrorKind::StorageFull
        ),
        "{error:?}"
    );
    assert_eq!(
        store.busy_retries(),
        before,
        "the import retried a full disk"
    );
    // Nothing was written: the ref is not there, and a read works right
    // after the failure.
    let repo_id = store.read(|c| c.repo_id("r")).unwrap();
    assert!(
        store
            .read(|c| c.get_ref(repo_id, "main"))
            .unwrap()
            .is_none()
    );
    // The space is back: the same import, again from the top, works.
    try_import(&store, &git, &rev).unwrap();
}

#[test]
fn a_rolled_back_import_batch_verifies_again_when_it_reruns() {
    if skip_on_postgres("counts the import's verification passes, a SQLite test hook") {
        return;
    }
    let git = Git::init();
    let rev = git.fast_commits(0, 40);
    // A clean import of the same history checks every commit once; its walk
    // count is what one pass over the plan costs.
    let (_clean_dir, clean) = open_store();
    make_repo(&clean);
    let clean_report = try_import(&clean, &git, &rev).unwrap();
    let one_pass = CHECKS.get();
    CHECKS.with(|checks| checks.set(0));
    // The retried import's batch runs whole, checks every commit, and its
    // commit fails as busy. The retry runs the batch again, and the checker
    // reset makes it verify from scratch: every committed changeset was
    // checked by the transaction that stored it.
    let (_dir, store) = open_store();
    make_repo(&store);
    store.fail_next_commits(1);
    let report = try_import(&store, &git, &rev).unwrap();
    assert_eq!((report.commits, report.imported), (40, 40));
    assert_eq!(report.verified, clean_report.verified);
    let walks = CHECKS.get();
    assert_eq!(
        walks,
        2 * one_pass,
        "the checks of the rolled-back batch ran again"
    );
}

#[test]
fn an_import_on_a_read_only_disk_fails_and_lands_after_the_remount() {
    // The blob files are local in every backend, so the hook below works
    // on Postgres too; both jobs run this test.
    let git = Git::init();
    let rev = git.fast_commits(0, 60);
    let (_dir, store) = open_store();
    let before = store.busy_retries();
    // The import's first blob write is the one the read-only disk
    // refuses: 30 is EROFS, what a read-only remount answers.
    store.blobs().fail_writes_after_errno(0, 30);
    assert!(store.read(|c| c.repo_id("r")).is_err());
    let error = try_import(&store, &git, &rev).unwrap_err();
    // The import did not answer busy: a read-only disk is not a lock the
    // database holds, and the batch's retry loop does not run.
    assert!(
        matches!(
            &error,
            Error::Io(e) if e.kind() == std::io::ErrorKind::ReadOnlyFilesystem
        ),
        "{error:?}"
    );
    assert_eq!(
        store.busy_retries(),
        before,
        "the import retried a read-only disk"
    );
    // Nothing was written: the ref is not there, and a read works right
    // after the failure.
    let repo_id = store.read(|c| c.repo_id("r")).unwrap();
    assert!(
        store
            .read(|c| c.get_ref(repo_id, "main"))
            .unwrap()
            .is_none()
    );
    // The remount is back: the same import, again from the top, works.
    try_import(&store, &git, &rev).unwrap();
}

#[test]
fn a_reimport_of_blobs_with_no_rows_still_verifies_and_fsyncs_them() {
    // The import's third file write is the one the disk refuses: two blob
    // files are renamed into place, and the batch's rows never commit.
    let git = Git::init();
    git.write("g/a", "alpha\n");
    git.write("g/b", "beta\n");
    git.write("g/c", "gamma\n");
    let rev = git.commit("three files");
    let (_dir, store) = open_store();
    store.blobs().fail_writes_after(2);
    try_import(&store, &git, &rev).unwrap_err();
    // The two files are on disk, but no row names them and the ref is not
    // there: nothing was acknowledged.
    assert!(store.read(|c| c.repo_id("r")).is_ok());
    let repo_id = store.read(|c| c.repo_id("r")).unwrap();
    assert!(
        store
            .read(|c| c.get_ref(repo_id, "main"))
            .unwrap()
            .is_none()
    );
    // The re-import must not trust the files: without rows they may be
    // the leftovers of a write whose fsync never succeeded, so each goes
    // through put's keep path, which verifies and fsyncs before it
    // answers.
    let verifies = store.blobs().verifies();
    let fsyncs = store.blobs().fsync_count();
    import(&store, &git, &rev);
    // At least the two keeps read back: a wrong skip would leave the
    // counter flat. The checker may read one more.
    assert!(
        store.blobs().verifies() - verifies >= 2,
        "the two stored files went through keep's read-back"
    );
    assert!(
        store.blobs().fsync_count() - fsyncs >= 4,
        "the kept files were fsynced, file and directory both"
    );
}

#[test]
fn a_second_import_of_row_named_blobs_neither_verifies_nor_fsyncs() {
    let git = Git::init();
    git.write("g/a", "alpha\n");
    git.write("g/b", "beta\n");
    let rev = git.commit("first");
    let (_dir, store) = open_store();
    import(&store, &git, &rev);
    // A later commit takes the file away, and the commit after that puts
    // the same bytes back: the re-added blob is named by a committed row,
    // so the import skips its put: no read-back, no fsync.
    git.remove("g/b");
    let _middle = git.commit("take beta away");
    git.write("g/b", "beta\n");
    let next = git.commit("put beta back");
    let fsyncs = store.blobs().fsync_count();
    import(&store, &git, &next);
    // The skip's proof is the fsyncs: the put of the row-named blob never
    // reaches the keep path, so no keep fsync fires. The checker's own
    // read-backs of the tree are a separate mechanism and stay.
    assert_eq!(
        store.blobs().fsync_count(),
        fsyncs,
        "a row-named blob skips the keep fsyncs"
    );
}
