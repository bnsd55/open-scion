//! `scn checkout` against an in-process server, compared with `git checkout`
//! of the same commit.
#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::import::{ImportOptions, import_git};
use scion_server::{Store, db, service, tree};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

const REPO: &str = "r";
const TOKEN: &str = "secret";
const SUBMODULE: &str = "0123456789abcdef0123456789abcdef01234567";

/// Runs git in `dir` with fixed identities and dates and no user or system
/// config. Fails the test unless git succeeds.
fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Ada")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_NAME", "Ada")
        .env("GIT_COMMITTER_EMAIL", "ada@example.com")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A git repository with one commit that has every entry kind: regular and
/// executable files, an empty file, binary content, nested directories, and
/// symlinks to a file, to a directory, out of the repo, to an absolute path,
/// and to nothing; plus a submodule.
fn fixture() -> TempDir {
    let repo = tempfile::tempdir().unwrap();
    let root = repo.path();
    let write = |path: &str, content: &[u8]| {
        let path = root.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    };
    write("README.md", b"hello\n");
    write("run.sh", b"#!/bin/sh\necho hi\n");
    fs::set_permissions(root.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    write("empty.txt", b"");
    write("with space.txt", b"spaces\n");
    write("bin/all-bytes", &(0..=255).collect::<Vec<u8>>());
    write("src/lib.rs", b"pub fn f() {}\n");
    write("src/nested/deep/file.txt", b"deep\n");
    write("src/nested/copy.txt", b"hello\n");
    write("docs/guide.md", b"# guide\n");
    for (link, target) in [
        ("link", "README.md"),
        ("dirlink", "src"),
        ("src/up", "../docs/guide.md"),
        ("escape", "../../outside"),
        ("absolute", "/etc/passwd"),
        ("dangling", "does/not/exist"),
    ] {
        std::os::unix::fs::symlink(target, root.join(link)).unwrap();
    }
    git(root, &["init", "-q", "-b", "main"]);
    git(root, &["add", "-A"]);
    let gitlink = format!("160000,{SUBMODULE},sub");
    git(root, &["update-index", "--add", "--cacheinfo", &gitlink]);
    git(root, &["commit", "-q", "-m", "fixture"]);
    repo
}

/// A server for one test. Dropping it stops the server.
struct Server {
    url: String,
    _runtime: Runtime,
    _data: TempDir,
}

impl Server {
    fn start(data: TempDir, store: Store) -> Self {
        let runtime = Runtime::new().unwrap();
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        runtime.spawn(service::serve(
            store,
            listener,
            TOKEN,
            std::future::pending(),
        ));
        Self {
            url,
            _runtime: runtime,
            _data: data,
        }
    }

    /// Imports `git`'s HEAD as ref `main` and serves it.
    fn with_import(git: &Path) -> Self {
        let (data, store) = open_store();
        let opts = ImportOptions {
            rev: "HEAD".into(),
            target_ref: "main".into(),
            verify_sample: 0,
        };
        import_git(&store, REPO, git, &opts).unwrap();
        Self::start(data, store)
    }

    /// Runs `scn` against this server.
    fn scn(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["--server", &self.url, "--repo", REPO])
            .args(args)
            .env("SCION_TOKEN", TOKEN)
            .output()
            .unwrap()
    }
}

fn open_store() -> (TempDir, Store) {
    let data = tempfile::tempdir().unwrap();
    let store = Store::open(data.path()).unwrap();
    (data, store)
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn text(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// What a checkout holds at one path.
#[derive(Debug, PartialEq, Eq)]
enum Node {
    Dir,
    File { data: Vec<u8>, mode: u32 },
    Symlink(PathBuf),
}

/// Everything under `root` except `.git`, by path relative to `root`.
/// Symlinks are read as links, never followed.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Node> {
    let mut nodes = BTreeMap::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let rel = path.strip_prefix(root).unwrap().to_path_buf();
            if rel == Path::new(".git") {
                continue;
            }
            let meta = fs::symlink_metadata(&path).unwrap();
            let node = if meta.is_symlink() {
                Node::Symlink(fs::read_link(&path).unwrap())
            } else if meta.is_dir() {
                dirs.push(path);
                Node::Dir
            } else {
                Node::File {
                    data: fs::read(&path).unwrap(),
                    mode: meta.permissions().mode() & 0o777,
                }
            };
            nodes.insert(rel, node);
        }
    }
    nodes
}

/// `git checkout` of HEAD into `dest`, without the empty directory that git
/// leaves for the submodule (Scion skips submodules).
fn git_checkout(git_dir: &Path, dest: &Path) -> BTreeMap<PathBuf, Node> {
    git(
        git_dir,
        &["worktree", "add", "-q", "--detach", text(dest), "HEAD"],
    );
    let mut nodes = snapshot(dest);
    assert_eq!(nodes.remove(Path::new("sub")), Some(Node::Dir));
    nodes
}

#[test]
fn checkout_matches_git_checkout_byte_for_byte() {
    let repo = fixture();
    let server = Server::with_import(repo.path());
    let tmp = tempfile::tempdir().unwrap();
    let ours = tmp.path().join("ours");

    let out = server.scn(&["checkout", "main", "--into", text(&ours)]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        stderr(&out).contains(&format!("skipping submodule \"sub\" at {SUBMODULE}")),
        "{}",
        stderr(&out)
    );

    let expected = git_checkout(repo.path(), &tmp.path().join("theirs"));
    assert_eq!(snapshot(&ours), expected);
    // The comparison covers what the fixture claims to have.
    let mode = |path: &str| match &expected[Path::new(path)] {
        Node::File { mode, .. } => *mode,
        other => panic!("{path}: {other:?}"),
    };
    assert_ne!(mode("run.sh") & 0o111, 0);
    assert_eq!(mode("README.md") & 0o111, 0);
    assert_eq!(
        expected[Path::new("escape")],
        Node::Symlink("../../outside".into())
    );
    assert_eq!(
        expected[Path::new("src/nested/deep/file.txt")],
        Node::File {
            data: b"deep\n".to_vec(),
            mode: mode("README.md")
        }
    );

    // The object reads need the token too.
    let out = server.scn(&[
        "--token",
        "wrong",
        "checkout",
        "main",
        "--into",
        text(&tmp.path().join("denied")),
    ]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("wrong token"), "{}", stderr(&out));
}

#[test]
fn sparse_checkout_writes_only_the_requested_directories() {
    let repo = fixture();
    let server = Server::with_import(repo.path());
    let tmp = tempfile::tempdir().unwrap();
    let ours = tmp.path().join("ours");

    let out = server.scn(&[
        "checkout",
        "main",
        "--sparse",
        "src/nested",
        "--sparse",
        "docs/",
        "--into",
        text(&ours),
    ]);
    assert!(out.status.success(), "{}", stderr(&out));

    let mut expected = git_checkout(repo.path(), &tmp.path().join("theirs"));
    expected.retain(|path, _| {
        path == Path::new("src") || path.starts_with("src/nested") || path.starts_with("docs")
    });
    let got = snapshot(&ours);
    assert_eq!(
        got.keys().collect::<BTreeSet<_>>(),
        expected.keys().collect::<BTreeSet<_>>()
    );
    assert_eq!(got, expected);

    // A sparse path that is not in the tree is an error, not an empty checkout.
    let missing = tmp.path().join("missing");
    let out = server.scn(&[
        "checkout",
        "main",
        "--sparse",
        "src/nope",
        "--into",
        text(&missing),
    ]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("\"src/nope\" is not in the tree"),
        "{}",
        stderr(&out)
    );
    assert!(!missing.exists());
}

/// Files of a changeset: raw path and state.
type Files<'a> = &'a [(&'a [u8], FileState)];

/// A root changeset with `files`.
fn root_changeset(files: Files) -> Changeset {
    Changeset {
        parents: vec![],
        author: IdentityTime {
            name: b"Ada".to_vec(),
            email: b"ada@example.com".to_vec(),
            unix_secs: 1_700_000_000,
            tz_offset_minutes: 0,
        },
        committer: None,
        message: b"hostile\n".to_vec(),
        changes: files
            .iter()
            .map(|(path, state)| (RepoPath::new(*path).unwrap(), Some(*state)))
            .collect(),
    }
}

/// Stores each file list as a root changeset in a new repo.
fn store_changesets(store: &Store, cases: &[Files]) -> Vec<ChangesetId> {
    store
        .write(|c| {
            let repo = db::create_repo(c, REPO)?;
            cases
                .iter()
                .map(|files| Ok(tree::store_changeset(c, repo, &root_changeset(files))?.0))
                .collect()
        })
        .unwrap()
}

#[test]
fn unsafe_names_fail_before_anything_is_written() {
    let (data, store) = open_store();
    let file = FileState::Regular {
        content: store.blobs().put(b"x\n").unwrap(),
        executable: false,
    };
    let link = FileState::Symlink {
        target: store.blobs().put(b"../../outside").unwrap(),
    };
    // Each tree is valid in the object format, and each error names the path.
    let cases: [(Files, &str); 5] = [
        (&[(b"A", file), (b"a", file)], r#""A" and "a""#),
        // On a case-insensitive filesystem `link/x` would go through `Link`.
        (
            &[(b"Link", link), (b"link/x", file)],
            r#""Link" and "link""#,
        ),
        (&[(b"ok", file), (b"ok\x1b[2J", file)], r#""ok\x1b[2J""#),
        (&[(b"src/.git/config", file)], r#""src/.git""#),
        (&[(b".Scion", file)], r#"".Scion""#),
    ];
    let files: Vec<_> = cases.iter().map(|(files, _)| *files).collect();
    let ids = store_changesets(&store, &files);
    let server = Server::start(data, store);
    let tmp = tempfile::tempdir().unwrap();

    for ((_, expected), id) in cases.iter().zip(ids) {
        let dest = tmp.path().join(id.to_string());
        let out = server.scn(&["checkout", &id.to_string(), "--into", text(&dest)]);
        assert!(!out.status.success(), "{expected}");
        assert!(stderr(&out).contains(expected), "{}", stderr(&out));
        assert!(!dest.exists(), "{expected}: wrote {}", dest.display());
    }
}

#[test]
fn objects_larger_than_one_reply_span_several_calls() {
    // Each blob is larger than both the 64 MiB gRPC message limit and the
    // server's reply budget, so each one travels alone in its own call.
    let size = 65 << 20;
    let (big_a, big_b) = (vec![b'a'; size], vec![b'b'; size]);
    let (data, store) = open_store();
    let regular = |content| FileState::Regular {
        content,
        executable: false,
    };
    let a = regular(store.blobs().put(&big_a).unwrap());
    let b = regular(store.blobs().put(&big_b).unwrap());
    let ids = store_changesets(&store, &[&[(b"a.bin", a), (b"b.bin", b)]]);
    let server = Server::start(data, store);
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("big");

    let out = server.scn(&["checkout", &ids[0].to_string(), "--into", text(&dest)]);
    assert!(out.status.success(), "{}", stderr(&out));
    // Compared by ID, so a failure prints two IDs, not 65 MiB.
    let written = |name| BlobId::of(&fs::read(dest.join(name)).unwrap());
    assert_eq!(written("a.bin"), BlobId::of(&big_a));
    assert_eq!(written("b.bin"), BlobId::of(&big_b));
}
