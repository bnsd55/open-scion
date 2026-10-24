//! `scn checkout` against an in-process server, compared with `git checkout`
//! of the same commit.
#![cfg(unix)]

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Command, Output, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rustix::process::{Pid, Signal, kill_process};
use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileEntry, FileState, IdentityTime, PathElem, RepoPath,
    Tree, TreeId,
};
use scion_proto::CHUNK_BYTES;
use scion_proto::runtime::listen::Limits;
use scion_proto::runtime::object::{self, Header};
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind, ObjectPart,
    ResolveRefRequest, ResolveRefResponse,
};
use scion_server::Backend;
use scion_server::test_support::error::from_sqlite;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::{service, store::Store, tree};
use scion_server::{ImportOptions, import_git};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt as _};
use tonic::{Request, Response, Status};

const REPO: &str = "r";
const TOKEN: &str = "secret";
const SUBMODULE: &str = "0123456789abcdef0123456789abcdef01234567";
/// A changeset ID for the `Liar`, which serves the same tree for any ID.
const ANY_CHANGESET: &str = "0000000000000000000000000000000000000000000000000000000000000000";

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

/// A scalar row from the store's SQLite file, through the store's error
/// type: the helper the upgrade tests read schema versions and counts
/// with.
fn sqlite_scalar<T: rusqlite::types::FromSql>(
    c: &rusqlite::Connection,
    sql: &str,
) -> scion_server::Result<T> {
    c.query_row(sql, [], |row| row.get(0)).map_err(from_sqlite)
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
    fn start(data: TempDir, store: Backend) -> Self {
        let runtime = Runtime::new().unwrap();
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        // The server mints the checkout's ticket, which scn then spends.
        let tickets = Some(std::sync::Arc::new(scion_server::Keys::generate()));
        runtime.spawn(service::serve(
            store,
            listener,
            TOKEN,
            service::ServeOptions {
                data: data.path().to_path_buf(),
                audit: scion_server::test_support::audit::Audit::off(),
                limits: Limits::default(),
                quotas: scion_server::Quotas::off(),
                pending_lands: None,
                land_stale_secs: 1 << 40,
                tls: None,
                metrics: Arc::default(),
                reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
            },
            tickets,
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
        scn(&self.url, REPO, args)
    }
}

/// Runs `scn` against the server at `url`.
fn scn(url: &str, repo: &str, args: &[&str]) -> Output {
    scn_command(url, repo, args).output().unwrap()
}

/// An `scn` command against the server at `url`.
fn scn_command(url: &str, repo: &str, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scn"));
    command
        .args(["--server", url, "--repo", repo])
        .args(args)
        .env("SCION_TOKEN", TOKEN);
    command
}

/// Asserts that a failed checkout left `dest` the empty directory it was,
/// with no temp directory next to it.
fn assert_untouched(dest: &Path) {
    assert_eq!(fs::read_dir(dest).unwrap().count(), 0, "{}", dest.display());
    let temps = temp_dirs(dest.parent().unwrap());
    assert!(temps.is_empty(), "left behind: {temps:?}");
}

/// The checkout temp directories in `dir`.
fn temp_dirs(dir: &Path) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            name.starts_with(".scn-checkout-")
        })
        .collect()
}

fn open_store() -> (TempDir, Backend) {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
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

    // A destination that is a symlink, even to an empty directory, is
    // refused, and nothing is written to the link's target.
    let target = tmp.path().join("target");
    fs::create_dir(&target).unwrap();
    let link = tmp.path().join("link");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let out = server.scn(&["checkout", "main", "--into", text(&link)]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("symlink"), "{}", stderr(&out));
    assert_untouched(&target);
    assert!(fs::symlink_metadata(&link).unwrap().is_symlink());
}

#[test]
fn checkout_into_an_existing_empty_directory_keeps_it() {
    let repo = fixture();
    let server = Server::with_import(repo.path());
    let tmp = tempfile::tempdir().unwrap();
    let parent = tmp.path().join("parent");
    let ours = parent.join("ours");
    fs::create_dir_all(&ours).unwrap();
    let inode = fs::metadata(&ours).unwrap().ino();

    // Nothing can be made next to it, as next to a mount point.
    fs::set_permissions(&parent, fs::Permissions::from_mode(0o555)).unwrap();
    let out = server.scn(&["checkout", "main", "--into", text(&ours)]);
    fs::set_permissions(&parent, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(out.status.success(), "{}", stderr(&out));

    let expected = git_checkout(repo.path(), &tmp.path().join("theirs"));
    assert_eq!(snapshot(&ours), expected);
    assert_eq!(fs::metadata(&ours).unwrap().ino(), inode);
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
    assert_eq!(snapshot(&ours), expected);

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
    Changeset::new(
        vec![],
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"hostile\n".to_vec(),
        files
            .iter()
            .map(|(path, state)| (RepoPath::new(*path).unwrap(), Some(*state)))
            .collect(),
    )
    .expect("changeset fits the format")
}

/// Stores each file list as a root changeset in a new repo.
fn store_changesets(store: &Backend, cases: &[Files]) -> Vec<ChangesetId> {
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            cases
                .iter()
                .map(|files| {
                    Ok(tree::store_changeset(
                        c,
                        repo,
                        &root_changeset(files),
                        &mut tree::lens_of(store),
                    )?
                    .0)
                })
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
    // Each tree is valid in the object format, and each error names the
    // escaped path.
    let cases: [(Files, &str); 4] = [
        (&[(b"ok", file), (b"ok\x1b[2J", file)], r#""ok\x1b[2J""#),
        // U+009B, a C1 control (CSI on an 8-bit terminal).
        (&[(b"ok/\xc2\x9b", file)], r#""ok/\xc2\x9b""#),
        // U+202E, right-to-left override.
        (
            &[(b"\xe2\x80\xaecod.exe", file)],
            r#""\xe2\x80\xaecod.exe""#,
        ),
        (&[(b"src/.git/config", file)], r#""src/.git""#),
    ];
    let files: Vec<_> = cases.iter().map(|(files, _)| *files).collect();
    let ids = store_changesets(&store, &files);
    let server = Server::start(data, store);
    let tmp = tempfile::tempdir().unwrap();

    for ((_, expected), id) in cases.iter().zip(ids) {
        let dest = tmp.path().join(id.to_string());
        fs::create_dir(&dest).unwrap();
        let out = server.scn(&["checkout", &id.to_string(), "--into", text(&dest)]);
        assert!(!out.status.success(), "{expected}");
        assert!(stderr(&out).contains(expected), "{}", stderr(&out));
        assert_untouched(&dest);
    }
}

/// Whether `dir` is on a filesystem that folds `name` and `other` into one.
fn folds(dir: &Path, name: &str, other: &str) -> bool {
    fs::write(dir.join(name), "").unwrap();
    let folds = dir.join(other).exists();
    fs::remove_file(dir.join(name)).unwrap();
    folds
}

#[test]
fn names_that_fold_together_fail_cleanly_where_the_filesystem_folds_them() {
    let (data, store) = open_store();
    let regular = |content: &[u8]| FileState::Regular {
        content: store.blobs().put(content).unwrap(),
        executable: false,
    };
    let (one, two) = (regular(b"one\n"), regular(b"two\n"));
    // From the temp directory, `../../outside` is `parent/outside` below:
    // the temp directory is in `parent/existing` or in `parent/next-to`.
    let link = FileState::Symlink {
        target: store.blobs().put(b"../../outside").unwrap(),
    };
    let (empty, to_t) = (
        regular(b""),
        FileState::Symlink {
            target: store.blobs().put(b"t").unwrap(),
        },
    );
    let tmp = tempfile::tempdir().unwrap();
    let case = folds(tmp.path(), "probe", "PROBE");
    // U+00E9 in NFC, and in NFD as `e` and U+0301.
    let unicode = folds(tmp.path(), "caf\u{e9}", "cafe\u{301}");
    let cases: [(Files, bool); 6] = [
        (&[(b"A", one), (b"a", two)], case),
        (&[(b"X/f", one), (b"x/g", two)], case),
        // Folded together, `link/x` would be written through `Link`.
        (&[(b"Link", link), (b"link/x", one)], case),
        // Empty files, and symlinks with one target: as the call sent again
        // after a lost NFS reply finds, but at another recorded name.
        (&[(b"E", empty), (b"e", empty)], case),
        (&[(b"S", to_t), (b"s", to_t)], case),
        (&[(b"caf\xc3\xa9", one), (b"cafe\xcc\x81", two)], unicode),
    ];
    let files: Vec<_> = cases.iter().map(|(files, _)| *files).collect();
    let ids = store_changesets(&store, &files);
    let server = Server::start(data, store);

    for ((files, folded), id) in cases.iter().zip(ids) {
        // Into an existing empty directory, and into a new one.
        for existing in [true, false] {
            let parent = tmp.path().join(format!("{id}-{existing}"));
            let outside = parent.join("outside");
            fs::create_dir_all(&outside).unwrap();
            // The empty directory that holds the temp directory.
            let made = parent.join(if existing { "existing" } else { "next-to" });
            fs::create_dir(&made).unwrap();
            let dest = if existing {
                made.clone()
            } else {
                made.join("new")
            };
            let out = server.scn(&["checkout", &id.to_string(), "--into", text(&dest)]);
            if *folded {
                assert!(!out.status.success());
                let err = stderr(&out);
                assert!(
                    err.contains("collides with another path on this filesystem"),
                    "{err}"
                );
                assert_untouched(&made);
            } else {
                // As git does: every name is written as it is.
                assert!(out.status.success(), "{}", stderr(&out));
                let written = |(path, _): &(&[u8], FileState)| {
                    fs::symlink_metadata(dest.join(text_of(path))).is_ok()
                };
                assert!(files.iter().all(written));
            }
            assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
        }
    }
}

fn text_of(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap()
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

/// An `Objects` server that answers trees honestly, then misbehaves on
/// blobs in the way the repo name says.
struct Liar {
    root: TreeId,
    objects: HashMap<Vec<u8>, Object>,
    /// The blob IDs, in the order of their paths.
    in_path_order: Vec<Vec<u8>>,
    /// The repo of the last `GetRootTree`, which carries the mode of the
    /// misbehaviors that the replies fake.
    repo: Mutex<String>,
}

impl Liar {
    /// Two files, `a` and `b`, `depth` directories named `d` below the root,
    /// with their trees and blobs.
    fn new(depth: usize) -> Self {
        let blob = |data: &[u8]| Object {
            kind: ObjectKind::Blob.into(),
            id: BlobId::of(data).as_bytes().to_vec(),
            data: data.to_vec(),
        };
        let (one, two) = (blob(b"one\n"), blob(b"two\n"));
        // Path order is the opposite of ID order, so a fetch in ID order is
        // caught.
        let (low, high) = if one.id < two.id {
            (one, two)
        } else {
            (two, one)
        };
        let file = |object: &Object| {
            Entry::File(FileEntry::new(
                FileState::Regular {
                    content: BlobId::try_from(object.id.as_slice()).unwrap(),
                    executable: false,
                },
                object.data.len() as u64,
            ))
        };
        let tree = Tree::new(
            [
                (PathElem::new("a").unwrap(), file(&high)),
                (PathElem::new("b").unwrap(), file(&low)),
            ]
            .into(),
        )
        .expect("tree fits the format");
        let in_path_order = vec![high.id.clone(), low.id.clone()];
        let mut objects = vec![high, low];
        let mut push = |tree: Tree| {
            let (data, id) = tree.encode_with_id();
            objects.push(Object {
                kind: ObjectKind::Tree.into(),
                id: id.as_bytes().to_vec(),
                data,
            });
            id
        };
        let mut root = push(tree);
        for _ in 0..depth {
            root = push(
                Tree::new([(PathElem::new("d").unwrap(), Entry::Dir(root))].into())
                    .expect("tree fits the format"),
            );
        }
        let objects = objects
            .into_iter()
            .map(|object| (object.id.clone(), object))
            .collect();
        Self {
            root,
            objects,
            in_path_order,
            repo: Mutex::new(String::new()),
        }
    }
}

type Replies = Pin<Box<dyn Stream<Item = Result<ObjectPart, Status>> + Send>>;

/// An object as the [`Liar`] sends it RAW, before it misbehaves.
#[derive(Clone)]
struct Object {
    kind: i32,
    id: Vec<u8>,
    data: Vec<u8>,
}

impl Object {
    /// Its header, for `encoded_size` bytes in `encoding`.
    fn header(&self, encoding: Encoding, encoded_size: usize) -> Header {
        Header {
            kind: ObjectKind::try_from(self.kind).unwrap(),
            id: self.id.clone().try_into().unwrap(),
            encoding,
            encoded_size,
            size: self.data.len(),
        }
    }
}

/// The parts of `objects`, each sent RAW, or the error that ends the reply.
fn raw_parts(objects: Vec<Result<Object, Status>>) -> Vec<Result<ObjectPart, Status>> {
    objects
        .into_iter()
        .flat_map(|object| match object {
            Ok(object) => {
                let header = object.header(Encoding::Raw, object.data.len());
                header
                    .parts(object::read_chunks(&object.data[..], object.data.len()).unwrap())
                    .into_iter()
                    .map(Ok)
                    .collect()
            }
            Err(status) => vec![Err(status)],
        })
        .collect()
}

#[tonic::async_trait]
impl Objects for Liar {
    async fn resolve_ref(
        &self,
        _: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        Err(Status::unimplemented("checkout by ID"))
    }

    async fn get_root_tree(
        &self,
        request: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        *self.repo.lock().unwrap() = request.into_inner().repo;
        Ok(Response::new(GetRootTreeResponse {
            tree_id: self.root.as_bytes().to_vec(),
            ticket: Vec::new(),
        }))
    }

    type GetObjectsStream = Replies;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Replies>, Status> {
        let GetObjectsRequest { refs, .. } = request.into_inner();
        let mut honest: Vec<Object> = refs.iter().map(|r| self.objects[&r.id].clone()).collect();
        let replies: Vec<Result<Object, Status>> = if refs[0].kind == i32::from(ObjectKind::Tree) {
            // A tree reply that ends in the middle of its second part. A
            // blob cannot play this: its first part's size is checked
            // against the tree, so an oversized object is refused first.
            if self.repo.lock().unwrap().as_str() == "truncated" {
                let mut header = honest[0].header(Encoding::Raw, CHUNK_BYTES + 4);
                header.size = CHUNK_BYTES + 4;
                let mut parts = header.parts(vec![vec![0; CHUNK_BYTES].into(), vec![0; 4].into()]);
                parts.truncate(1);
                return Ok(Response::new(Box::pin(tokio_stream::iter(
                    parts.into_iter().map(Ok),
                ))));
            }
            honest.into_iter().map(Ok).collect()
        } else {
            match self.repo.lock().unwrap().as_str() {
                "in-order" => {
                    let asked: Vec<_> = refs.iter().map(|r| r.id.clone()).collect();
                    if asked != self.in_path_order {
                        return Err(Status::invalid_argument("blobs not asked in path order"));
                    }
                    honest.into_iter().map(Ok).collect()
                }
                "extra" => {
                    let id = BlobId::of(b"extra").as_bytes().to_vec();
                    honest.push(Object {
                        kind: ObjectKind::Blob.into(),
                        id,
                        data: b"extra".to_vec(),
                    });
                    honest.into_iter().map(Ok).collect()
                }
                "duplicate" => {
                    honest.insert(0, honest[0].clone());
                    honest.into_iter().map(Ok).collect()
                }
                "wrong-kind" => {
                    for object in &mut honest {
                        object.kind = ObjectKind::Tree.into();
                    }
                    honest.into_iter().map(Ok).collect()
                }
                "none" => Vec::new(),
                "stall" => {
                    let first = tokio_stream::iter(raw_parts(vec![Ok(honest[0].clone())]));
                    let stalled: Replies = Box::pin(first.chain(tokio_stream::pending()));
                    return Ok(Response::new(stalled));
                }
                "error" => vec![
                    Ok(honest[0].clone()),
                    Err(Status::internal("lost the disk")),
                ],
                // A first part whose size differs from what the tree says:
                // the client must refuse at once, before the body reads.
                "size-lies" => {
                    let frame = object::pack(&honest[0].data).unwrap();
                    let mut header = honest[0].header(Encoding::Zstd, frame.len());
                    header.size += 999;
                    let parts = header.parts(vec![frame.into()]).into_iter().map(Ok);
                    return Ok(Response::new(Box::pin(tokio_stream::iter(parts))));
                }
                // The first blob as a good zstd frame of other bytes.
                "zstd-other" => {
                    let frame = object::pack(b"evil\n").unwrap();
                    let mut header = honest[0].header(Encoding::Zstd, frame.len());
                    header.size = 5;
                    let parts = header.parts(vec![frame.into()]).into_iter().map(Ok);
                    return Ok(Response::new(Box::pin(tokio_stream::iter(parts))));
                }
                // The first blob as a frame whose header gives one byte more
                // than it holds, as the object's size does. Byte 5 is the
                // content size of a small frame.
                "zstd-lying" => {
                    let mut frame = object::pack(&honest[0].data).unwrap();
                    frame[5] += 1;
                    let mut header = honest[0].header(Encoding::Zstd, frame.len());
                    header.size += 1;
                    let parts = header.parts(vec![frame.into()]).into_iter().map(Ok);
                    return Ok(Response::new(Box::pin(tokio_stream::iter(parts))));
                }
                other => panic!("no such misbehavior: {other}"),
            }
        };
        Ok(Response::new(Box::pin(tokio_stream::iter(raw_parts(
            replies,
        )))))
    }
}

/// Serves a [`Liar`] with its files `depth` directories deep until the
/// returned runtime is dropped, and returns its URL.
fn start_liar(depth: usize) -> (Runtime, String) {
    let runtime = Runtime::new().unwrap();
    let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    runtime.spawn(
        tonic::transport::Server::builder()
            .add_service(ObjectsServer::new(Liar::new(depth)))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    (runtime, url)
}

#[test]
fn blobs_are_asked_for_in_path_order() {
    let (_runtime, url) = start_liar(0);
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("dest");
    let out = scn(
        &url,
        "in-order",
        &["checkout", ANY_CHANGESET, "--into", text(&dest)],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    let mut written: Vec<_> = ["a", "b"]
        .map(|name| fs::read(dest.join(name)).unwrap())
        .into();
    written.sort();
    assert_eq!(written, [b"one\n", b"two\n"]);
}

#[test]
fn a_misbehaving_server_is_caught_and_nothing_is_written() {
    let (_runtime, url) = start_liar(0);
    let tmp = tempfile::tempdir().unwrap();

    for (misbehavior, expected) in [
        ("extra", "not asked for"),
        ("duplicate", "not asked for"),
        ("wrong-kind", "not asked for"),
        ("none", "none of the requested objects"),
        ("error", "lost the disk"),
        ("size-lies", "the tree says"),
        ("zstd-other", "server sent blob"),
        // A frame whose header gives one byte more than it holds: the size
        // lie of the first part, refused before the body reads.
        ("zstd-lying", "the tree says"),
        ("truncated", "in the middle of an object"),
    ] {
        let dest = tmp.path().join(misbehavior);
        fs::create_dir(&dest).unwrap();
        let out = scn(
            &url,
            misbehavior,
            &["checkout", ANY_CHANGESET, "--into", text(&dest)],
        );
        assert!(!out.status.success(), "{misbehavior}");
        assert!(
            stderr(&out).contains(expected),
            "{misbehavior}: {}",
            stderr(&out)
        );
        assert_untouched(&dest);
    }
}

#[test]
fn a_signal_removes_what_the_checkout_wrote_and_then_kills_it() {
    let (_runtime, url) = start_liar(0);
    let tmp = tempfile::tempdir().unwrap();
    for (signal, name) in [
        (Signal::INT, "SIGINT"),
        (Signal::TERM, "SIGTERM"),
        (Signal::HUP, "SIGHUP"),
    ] {
        let dest = tmp.path().join(name);
        fs::create_dir(&dest).unwrap();
        let child = scn_command(
            &url,
            "stall",
            &["checkout", ANY_CHANGESET, "--into", text(&dest)],
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

        // The server sends `a`, then never sends `b`.
        let deadline = Instant::now() + Duration::from_secs(30);
        while !temp_dirs(&dest).iter().any(|temp| temp.join("a").exists()) {
            assert!(
                Instant::now() < deadline,
                "{name}: the checkout never wrote a"
            );
            thread::sleep(Duration::from_millis(10));
        }
        kill_process(Pid::from_child(&child), signal).unwrap();
        let out = child.wait_with_output().unwrap();

        let err = stderr(&out);
        // Killed by the signal, so a shell reports 128 plus its number.
        assert_eq!(out.status.signal(), Some(signal.as_raw()), "{name}: {err}");
        assert!(err.contains(&format!("interrupted by {name}")), "{err}");
        assert_untouched(&dest);
    }
}

#[test]
fn a_deep_failed_checkout_is_removed_with_few_open_files() {
    // 255 directories, the most a path can have above a file.
    let (_runtime, url) = start_liar(255);
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("dest");
    fs::create_dir(&dest).unwrap();
    // Fewer open files than the tree has levels. The server sends `a`, then
    // fails.
    let scn = scn_command(
        &url,
        "error",
        &["checkout", ANY_CHANGESET, "--into", text(&dest)],
    );
    let out = Command::new("sh")
        .args(["-c", "ulimit -n 128 && exec \"$@\"", "sh"])
        .arg(scn.get_program())
        .args(scn.get_args())
        .env("SCION_TOKEN", TOKEN)
        .output()
        .unwrap();

    assert!(!out.status.success());
    let err = stderr(&out);
    assert!(err.contains("lost the disk"), "{err}");
    assert!(!err.contains("warning"), "{err}");
    assert_untouched(&dest);
}

/// An `Objects` server whose first ticket has expired: a `GetObjects` that
/// carries it ends permission-denied, and every ticket minted after works.
/// It counts its mints, so the test can see the refresh.
struct Expiring {
    root: TreeId,
    objects: HashMap<Vec<u8>, Object>,
    /// Shared with the test, which reads it while the server runs.
    mints: std::sync::Arc<AtomicUsize>,
}

impl Expiring {
    /// One file `a` of one byte, under the root tree.
    fn new() -> Self {
        let blob = Object {
            kind: ObjectKind::Blob.into(),
            id: BlobId::of(b"x\n").as_bytes().to_vec(),
            data: b"x\n".to_vec(),
        };
        let tree = Tree::new(
            [(
                PathElem::new("a").unwrap(),
                Entry::File(FileEntry::new(
                    FileState::Regular {
                        content: BlobId::try_from(blob.id.as_slice()).unwrap(),
                        executable: false,
                    },
                    blob.data.len() as u64,
                )),
            )]
            .into(),
        )
        .expect("tree fits the format");
        let (data, root) = tree.encode_with_id();
        let tree = Object {
            kind: ObjectKind::Tree.into(),
            id: root.as_bytes().to_vec(),
            data,
        };
        Self {
            root,
            objects: [(blob.id.clone(), blob), (tree.id.clone(), tree)]
                .into_iter()
                .collect(),
            mints: std::sync::Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[tonic::async_trait]
impl Objects for Expiring {
    async fn resolve_ref(
        &self,
        _: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        Err(Status::unimplemented("checkout by ID"))
    }

    async fn get_root_tree(
        &self,
        _: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        let mint = self.mints.fetch_add(1, Ordering::SeqCst) + 1;
        let ticket = if mint == 1 {
            b"expired".to_vec()
        } else {
            b"fresh".to_vec()
        };
        Ok(Response::new(GetRootTreeResponse {
            tree_id: self.root.as_bytes().to_vec(),
            ticket,
        }))
    }

    type GetObjectsStream = Replies;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Replies>, Status> {
        let GetObjectsRequest { refs, ticket, .. } = request.into_inner();
        if ticket == b"expired" {
            return Err(Status::permission_denied("bad ticket: ticket expired"));
        }
        assert_eq!(ticket, b"fresh", "the ticket scn was minted last");
        let replies = refs
            .iter()
            .map(|r| Ok(self.objects[&r.id].clone()))
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(raw_parts(
            replies,
        )))))
    }
}

/// A checkout that outlives its first ticket mints a new one, once, and
/// goes on: the walk fetches again with the fresh ticket.
#[test]
fn an_expired_ticket_is_minted_again_once() {
    let runtime = Runtime::new().unwrap();
    let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let fake = Expiring::new();
    let mints = fake.mints.clone();
    runtime.spawn(
        tonic::transport::Server::builder()
            .add_service(ObjectsServer::new(fake))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("dest");
    let out = scn(
        &url,
        "r",
        &["checkout", ANY_CHANGESET, "--into", text(&dest)],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(fs::read(dest.join("a")).unwrap(), b"x\n");
    // One mint for the checkout, one for the refresh: no more.
    assert_eq!(mints.load(Ordering::SeqCst), 2);
}

/// The body of a schema-6 tree: entries carry no byte lengths, so this
/// build cannot decode it.
fn old_tree(entries: &[(&str, BlobId, bool)]) -> Vec<u8> {
    const TAG_REGULAR: u8 = 1;
    let mut w = vec![1u8];
    w.extend_from_slice(&u32::try_from(entries.len()).unwrap().to_be_bytes());
    for (name, blob, exec) in entries {
        w.extend_from_slice(&u32::try_from(name.len()).unwrap().to_be_bytes());
        w.extend_from_slice(name.as_bytes());
        w.push(TAG_REGULAR);
        w.extend_from_slice(blob.as_bytes());
        w.push(u8::from(*exec));
    }
    w
}

/// A data directory that the previous release (schema 6) wrote: its trees
/// carry no byte lengths. Opening it on this build upgrades the database,
/// drops the old trees, and re-derives them from the blob store, so the
/// checkout works and fsck is clean.
#[test]
fn a_schema_6_data_dir_upgrades_and_checks_out() {
    let data = tempfile::tempdir().unwrap();
    // The changeset and blob bytes are the bytes the old release wrote
    // too: #172 changed trees only.
    let store = Store::open(data.path()).unwrap();
    let hello = store.blobs().put(b"hello").unwrap();
    let world = store.blobs().put(b"hello world").unwrap();
    let author = IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format");
    let changeset = |parent: Option<ChangesetId>, content: BlobId| {
        Changeset::new(
            parent.into_iter().collect(),
            author.clone(),
            None,
            b"upgrade\n".to_vec(),
            [(
                RepoPath::new("a").unwrap(),
                Some(FileState::Regular {
                    content,
                    executable: false,
                }),
            )]
            .into_iter()
            .collect(),
        )
        .expect("changeset fits the format")
    };
    let mut lens: std::collections::HashMap<BlobId, u64> =
        [(hello, 5u64), (world, 11)].into_iter().collect();
    let (root, child) = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut missing = |b| {
                lens.remove(&b)
                    .ok_or_else(|| scion_server::Error::NotFound(format!("blob {b}")))
            };
            let root = tree::store_changeset(c, repo, &changeset(None, hello), &mut missing)?.0;
            let child =
                tree::store_changeset(c, repo, &changeset(Some(root), world), &mut missing)?.0;
            assert!(c.cas_ref(repo, "main", None, child)?);
            Ok((root, child))
        })
        .unwrap();
    drop(store);

    // Back to the disk state of the release before #177: schema 6, and
    // tree bodies whose entries carry no byte lengths.
    let olds = [
        (root, old_tree(&[("a", hello, false)])),
        (child, old_tree(&[("a", world, false)])),
    ];
    {
        let c = rusqlite::Connection::open(data.path().join("scion.db")).unwrap();
        for (cs, body) in olds {
            let id: Vec<u8> = c
                .query_row(
                    "SELECT value FROM derived WHERE kind = 'root_tree' AND cs = ?1",
                    [cs.as_bytes().to_vec()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(id.len(), 32, "the changeset has a root tree row");
            c.execute("UPDATE trees SET body = ?1 WHERE id = ?2", [body, id])
                .unwrap();
        }
        // The view memo of #121 did not exist in schema 6: drop the table
        // that this build's first open made, so the upgrade replays 8 and 9.
        c.execute_batch(
            "DELETE FROM derived; DROP TABLE IF EXISTS view_trees; PRAGMA user_version = 6;",
        )
        .unwrap();
    }

    // This build upgrades the directory on open.
    let store = Store::open(data.path()).unwrap();
    let version: u32 = store
        .read(|c| sqlite_scalar(c, "PRAGMA user_version"))
        .unwrap();
    assert_eq!(version, 9);
    let trees: i64 = store
        .read(|c| sqlite_scalar(c, "SELECT count() FROM trees"))
        .unwrap();
    assert_eq!(trees, 0, "the old trees are gone");
    let report = scion_server::fsck::run(
        &scion_server::test_support::backend::from_store(store.clone()),
        1,
        &mut |_| {},
    )
    .unwrap();
    assert!(report.problems.is_empty(), "{:?}", report.problems);

    // A checkout of the upgraded directory re-derives the trees. The db
    // path outlives `data`, which `Server::start` takes.
    let db_path = data.path().join("scion.db");
    let server = Server::start(
        data,
        scion_server::test_support::backend::from_store(store.clone()),
    );
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("dest");
    let out = server.scn(&["checkout", "main", "--into", text(&dest)]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(fs::read(dest.join("a")).unwrap(), b"hello world");
    let c = rusqlite::Connection::open(&db_path).unwrap();
    let trees: i64 = c
        .query_row("SELECT count() FROM trees", [], |row| row.get(0))
        .unwrap();
    assert_eq!(trees, 2, "the checkout re-derived and stored the trees");
}
