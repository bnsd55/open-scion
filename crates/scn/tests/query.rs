//! `scn merge-base`, `scn is-ancestor` and `scn diff` against an in-process
//! server, compared with git on the same history.
#![cfg(unix)]

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::{Command, Output};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, GitOid, IdentityTime, RepoPath};
use scion_proto::runtime::listen::Limits;
use scion_server::Backend;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::read::MAX_DIFF_ENTRIES;
use scion_server::test_support::{service, store::Store, tree};
use scion_server::{ImportOptions, import_git};
use serde_json::Value;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

const REPO: &str = "r";
const TOKEN: &str = "secret";

/// Runs git in `dir` with fixed identities and dates and no user or system
/// config. Returns whether it succeeded, and stdout.
fn git_status(dir: &Path, args: &[&str]) -> (bool, String) {
    let out = Command::new("git")
        // No auto gc or maintenance: they pack loose objects in the
        // background and delete the loose files, also while the import of
        // the same repository still reads them.
        .args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"])
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
    let stdout = String::from_utf8(out.stdout).unwrap().trim_end().to_owned();
    (out.status.success(), stdout)
}

fn git(dir: &Path, args: &[&str]) -> String {
    let (ok, stdout) = git_status(dir, args);
    assert!(ok, "git {args:?} failed");
    stdout
}

/// Writes the files (`None` deletes one) and commits everything.
fn commit(dir: &Path, message: &str, files: &[(&str, Option<&str>)]) {
    for (name, content) in files {
        let file = dir.join(name);
        match content {
            Some(content) => {
                fs::create_dir_all(file.parent().unwrap()).unwrap();
                fs::write(file, content).unwrap();
            }
            None => fs::remove_file(file).unwrap(),
        }
    }
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", message]);
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
            None,
            std::future::pending(),
        ));
        Self {
            url,
            _runtime: runtime,
            _data: data,
        }
    }

    fn scn(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["--server", &self.url, "--repo", REPO])
            .args(args)
            .env("SCION_TOKEN", TOKEN)
            .output()
            .unwrap()
    }

    /// Runs `scn` and returns stdout. Fails the test unless it succeeds.
    fn text(&self, args: &[&str]) -> String {
        let out = self.scn(args);
        assert!(
            out.status.success(),
            "scn {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    /// Runs `scn --json` and parses its output.
    fn json(&self, args: &[&str]) -> Value {
        let all: Vec<&str> = ["--json"].iter().chain(args).copied().collect();
        serde_json::from_str(&self.text(&all)).unwrap()
    }
}

/// Every commit of the git repository, mapped to the changeset it was
/// imported as, in hex.
fn changesets(store: &Backend, dir: &Path) -> HashMap<String, String> {
    git(dir, &["rev-list", "--all"])
        .lines()
        .map(|commit| {
            let oid: Vec<u8> = (0..commit.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&commit[i..i + 2], 16).unwrap())
                .collect();
            let id = store
                .read(|c| {
                    let cs: Vec<u8> = c
                        .sqlite()
                        .unwrap()
                        .query_row(
                            "SELECT cs FROM git_import WHERE git_oid = ?1",
                            [&oid],
                            |row| row.get(0),
                        )
                        .map_err(scion_server::test_support::error::from_sqlite)?;
                    Ok(ChangesetId::try_from(cs.as_slice()).unwrap())
                })
                .unwrap();
            (commit.to_owned(), id.to_string())
        })
        .collect()
}

/// `scn diff --json` as git's `--name-status` lines, sorted. A rename is
/// `R<similarity>`, the old path and the new path.
fn name_status(diff: &Value) -> Vec<String> {
    let mut lines: Vec<String> = diff
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            let (status, path) = (e["status"].as_str().unwrap(), e["path"].as_str().unwrap());
            match status {
                "R" => format!(
                    "R{:03}\t{}\t{path}",
                    e["similarity"].as_u64().unwrap(),
                    e["old_path"].as_str().unwrap()
                ),
                _ => format!("{status}\t{path}"),
            }
        })
        .collect();
    lines.sort();
    lines
}

#[test]
fn merge_base_is_ancestor_and_diff_match_git() {
    if Backend::test_is_postgres() {
        // SHORTCUT: this ceiling is the skip itself; Postgres import
        // provenance (the git_import rows) is unread by these tests, so the
        // git side of the comparison has no Postgres run. Upgrade path: a
        // backend-agnostic git_import read on MetaTx, then drop the skip.
        eprintln!("skip on postgres: the test reads the data directory's git_import rows");
        return;
    }
    merge_base_is_ancestor_and_diff_match_git_inner();
}

fn merge_base_is_ancestor_and_diff_match_git_inner() {
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    git(dir, &["init", "-q", "-b", "main"]);
    commit(
        dir,
        "root",
        &[
            ("a.txt", Some("a")),
            ("a/b", Some("b")),
            ("del", Some("d")),
            ("kind", Some("k")),
            ("moved.txt", Some("1\n2\n3\n4\n5\n")),
        ],
    );
    git(dir, &["checkout", "-q", "-b", "side"]);
    // `kind` becomes a symlink: git's `T`.
    fs::remove_file(dir.join("kind")).unwrap();
    symlink("a.txt", dir.join("kind")).unwrap();
    commit(dir, "s1", &[("a/b", Some("b2")), ("side", Some("s"))]);
    git(dir, &["checkout", "-q", "main"]);
    symlink("a.txt", dir.join("link")).unwrap();
    fs::write(dir.join("run.sh"), "x").unwrap();
    fs::set_permissions(dir.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    // A rename with an edit: 80% similar.
    commit(
        dir,
        "m1",
        &[
            ("a.txt", Some("a2")),
            ("del", None),
            ("moved.txt", None),
            ("moved/renamed.txt", Some("1\n2\n3\n4\n6\n")),
        ],
    );
    let m1 = git(dir, &["rev-parse", "HEAD"]);
    // Criss-cross: x1 and x2 have two best common ancestors, m1 and s1.
    git(dir, &["merge", "-q", "--no-ff", "side", "-m", "x1"]);
    git(dir, &["checkout", "-q", "side"]);
    git(dir, &["merge", "-q", "--no-ff", &m1, "-m", "x2"]);
    let x2 = git(dir, &["rev-parse", "HEAD"]);
    git(dir, &["checkout", "-q", "main"]);
    let x1 = git(dir, &["rev-parse", "HEAD"]);
    git(dir, &["merge", "-q", "--no-ff", "side", "-m", "tip"]);

    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    let opts = ImportOptions {
        rev: "HEAD".into(),
        target_ref: "main".into(),
        verify_sample: 0,
    };
    import_git(&store, REPO, dir, &opts).unwrap();
    let ids = changesets(&store, dir);
    let server = Server::start(data, store);

    // The server tests compare every pair; these check the wire and the
    // command line.
    let root = git(dir, &["rev-list", "--max-parents=0", "HEAD"]);
    let tip = git(dir, &["rev-parse", "HEAD"]);
    let pairs = [
        (&x1, &x2),
        (&x2, &x1),
        (&root, &tip),
        (&tip, &root),
        (&m1, &x2),
        (&x1, &x1),
    ];
    for (x, y) in pairs {
        let (cx, cy) = (&ids[x], &ids[y]);
        let (_, out) = git_status(dir, &["merge-base", "--all", x, y]);
        let mut expected: Vec<&str> = out.lines().map(|c| ids[c].as_str()).collect();
        expected.sort_unstable();
        let bases = server.json(&["merge-base", cx, cy]);
        let mut bases: Vec<&str> = bases
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap())
            .collect();
        bases.sort_unstable();
        assert_eq!(bases, expected, "merge-base {x} {y}");

        let (yes, _) = git_status(dir, &["merge-base", "--is-ancestor", x, y]);
        let code = server.scn(&["is-ancestor", cx, cy]).status.code();
        // 0 is yes, 1 is no.
        assert_eq!(code, Some(i32::from(!yes)), "is-ancestor {x} {y}");

        let git_diff = git(dir, &["diff", "--name-status", x, y]);
        let mut expected: Vec<&str> = git_diff.lines().collect();
        expected.sort_unstable();
        assert_eq!(
            name_status(&server.json(&["diff", cx, cy])),
            expected,
            "diff {x} {y}"
        );
    }

    // Text output, refs, and a path.
    let text = server.text(&["merge-base", &ids[&x1], &ids[&x2]]);
    assert_eq!(text.lines().count(), 2, "{text}");
    assert_eq!(server.text(&["is-ancestor", &ids[&m1], "main"]), "");
    let text = server.text(&["diff", &ids[&root], "main", "--path", "a/"]);
    assert_eq!(text, "M\ta/b\n");
    let text = server.text(&["diff", &ids[&root], &ids[&m1]]);
    assert!(
        text.contains("R\tmoved.txt -> moved/renamed.txt\n"),
        "{text}"
    );
    assert!(!text.contains("\tmoved.txt\n"), "{text}");
    // An error exits 2, so it never reads as the "no" of is-ancestor.
    let out = server.scn(&["is-ancestor", "no-such-ref", "main"]);
    assert_eq!(out.status.code(), Some(2));

    assert_diff_json(&server, &ids[&root], &ids[&m1]);
}

/// Checks the file states of `scn diff --json` from `root` to `m1`, as
/// `scn ls --json` prints them, and its one rename.
fn assert_diff_json(server: &Server, root: &str, m1: &str) {
    let diff = server.json(&["diff", root, m1]);
    let by_path: HashMap<&str, &Value> = diff
        .as_array()
        .unwrap()
        .iter()
        .map(|e| (e["path"].as_str().unwrap(), e))
        .collect();
    let blob = |data: &[u8]| BlobId::of(data).to_string();
    assert_eq!(by_path["link"]["old"], Value::Null);
    assert_eq!(by_path["link"]["new"]["kind"], "symlink");
    assert_eq!(by_path["link"]["new"]["id"], blob(b"a.txt"));
    assert_eq!(by_path["a.txt"]["old"]["id"], blob(b"a"));
    assert_eq!(by_path["a.txt"]["new"]["id"], blob(b"a2"));
    assert_eq!(by_path["run.sh"]["new"]["executable"], true);
    assert_eq!(by_path["del"]["old"]["kind"], "file");
    assert_eq!(by_path["del"]["new"], Value::Null);
    // A rename is one entry, at the new path, with the old file.
    let renamed = by_path["moved/renamed.txt"];
    assert_eq!(renamed["status"], "R");
    assert_eq!(renamed["old_path"], "moved.txt");
    assert_eq!(renamed["similarity"], 80);
    assert_eq!(renamed["old"]["id"], blob(b"1\n2\n3\n4\n5\n"));
    assert_eq!(renamed["new"]["id"], blob(b"1\n2\n3\n4\n6\n"));
    assert!(!by_path.contains_key("moved.txt"));
}

/// Stores a changeset with `base`, then a child with `changes`, in a new repo.
fn two_changesets(
    store: &Backend,
    base: BTreeMap<RepoPath, Option<FileState>>,
    changes: BTreeMap<RepoPath, Option<FileState>>,
) -> (ChangesetId, ChangesetId) {
    two_changesets_lens(store, base, changes, Box::new(|_| Ok(0)))
}

/// [`two_changesets`], with the lens lookup of the tree derivation.
fn two_changesets_lens(
    store: &Backend,
    base: BTreeMap<RepoPath, Option<FileState>>,
    changes: BTreeMap<RepoPath, Option<FileState>>,
    mut lens: Box<dyn FnMut(BlobId) -> Result<u64, scion_server::Error>>,
) -> (ChangesetId, ChangesetId) {
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut parents = Vec::new();
            for changes in [base, changes] {
                let cs = Changeset::new(
                    parents.last().copied().into_iter().collect(),
                    IdentityTime::new(
                        b"Ada".to_vec(),
                        b"ada@example.com".to_vec(),
                        1_700_000_000,
                        0,
                    )
                    .expect("identity fits the format"),
                    None,
                    b"change".to_vec(),
                    changes,
                )
                .expect("changeset fits the format");
                parents.push(tree::store_changeset(c, repo, &cs, &mut *lens)?.0);
            }
            Ok((parents[0], parents[1]))
        })
        .unwrap()
}

#[test]
fn ls_shows_the_size_of_every_entry_that_carries_one() {
    let data = tempfile::tempdir().unwrap();
    let store = scion_server::test_support::backend::from_store(Store::open(data.path()).unwrap());
    let read = b"one two three"; // 13 bytes
    let link = b"a.md"; // 4 bytes
    let empty = b"";
    let gitlink = GitOid::Sha1([0xab; 20]);
    let sizes = |id: BlobId| {
        Ok(if id == BlobId::of(read) {
            read.len() as u64
        } else if id == BlobId::of(link) {
            link.len() as u64
        } else {
            0
        })
    };
    let changes: BTreeMap<RepoPath, Option<FileState>> = BTreeMap::from([
        (
            RepoPath::new(b"read.md").unwrap(),
            Some(FileState::Regular {
                content: BlobId::of(read),
                executable: false,
            }),
        ),
        (
            RepoPath::new(b"link.md").unwrap(),
            Some(FileState::Symlink {
                target: BlobId::of(link),
            }),
        ),
        (
            RepoPath::new(b"empty.md").unwrap(),
            Some(FileState::Regular {
                content: BlobId::of(empty),
                executable: false,
            }),
        ),
        (
            RepoPath::new(b"sub").unwrap(),
            Some(FileState::Gitlink { commit: gitlink }),
        ),
    ]);
    let (_, head) = two_changesets_lens(&store, BTreeMap::new(), changes, Box::new(sizes));
    let server = Server::start(data, store);

    let ls = server.text(&["ls", &format!("{head}:")]);
    assert!(ls.contains("100644 file            13 read.md"), "{ls:?}");
    assert!(ls.contains("120000 symlink          4 link.md"), "{ls:?}");
    // An empty file still shows its size: zero, not blank.
    assert!(ls.contains("100644 file             0 empty.md"), "{ls:?}");
    // A gitlink names a commit, not a blob, so it shows no size.
    assert!(ls.contains("160000 gitlink            sub"), "{ls:?}");

    let entries = server.json(&["ls", &format!("{head}:")]);
    let by_name = |name: &str| {
        entries
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["name"] == name)
            .unwrap()
            .clone()
    };
    assert_eq!(by_name("read.md")["size"], 13);
    assert_eq!(by_name("link.md")["size"], 4);
    assert_eq!(by_name("empty.md")["size"], 0);
    assert!(by_name("sub").get("size").is_none(), "{}", by_name("sub"));
}

#[test]
fn diff_continues_past_one_reply() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    // More paths than one reply holds, with each kind of file. Blobs are
    // never read, so they are not stored.
    let commit = GitOid::Sha1([7; 20]);
    let mut changes = BTreeMap::new();
    for n in 0..=MAX_DIFF_ENTRIES + 10 {
        let state = match n % 3 {
            0 => FileState::Regular {
                content: BlobId::of(&n.to_le_bytes()),
                executable: n % 2 == 0,
            },
            1 => FileState::Symlink {
                target: BlobId::of(&n.to_le_bytes()),
            },
            _ => FileState::Gitlink { commit },
        };
        let path = RepoPath::new(format!("d{}/f{n:05}", n % 7)).unwrap();
        changes.insert(path, Some(state));
    }
    let (old, new) = two_changesets(&store, BTreeMap::new(), changes.clone());
    let server = Server::start(data, store);

    // The paths have two parts of plain names, so tree order is path order.
    let diff = server.json(&["diff", &old.to_string(), &new.to_string()]);
    let entries = diff.as_array().unwrap();
    assert_eq!(entries.len(), changes.len());
    for (entry, (path, state)) in entries.iter().zip(&changes) {
        assert_eq!(
            entry["path"],
            String::from_utf8_lossy(path.as_bytes()).as_ref()
        );
        assert_eq!(entry["status"], "A");
        let (kind, id) = match state.unwrap() {
            FileState::Regular { content, .. } => ("file", content.to_string()),
            FileState::Symlink { target } => ("symlink", target.to_string()),
            FileState::Gitlink { commit } => ("gitlink", commit.to_string()),
        };
        assert_eq!(entry["new"]["kind"], kind, "{path:?}");
        assert_eq!(entry["new"]["id"], id.as_str(), "{path:?}");
    }
    let below = server.json(&["diff", &old.to_string(), &new.to_string(), "--path", "d3"]);
    assert_eq!(
        below.as_array().unwrap().len(),
        changes
            .keys()
            .filter(|p| p.as_bytes().starts_with(b"d3/"))
            .count()
    );
}

#[test]
fn diff_prints_a_rename_split_across_replies() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    // `zzz/old` becomes `a/new`, and the paths between fill the first
    // reply, so the two halves of the rename come in different replies.
    // Blobs are never read: the rename has the same content.
    let state = |n: usize| {
        Some(FileState::Regular {
            content: BlobId::of(&n.to_le_bytes()),
            executable: false,
        })
    };
    let base = BTreeMap::from([(RepoPath::new("zzz/old").unwrap(), state(usize::MAX))]);
    let mut change = BTreeMap::from([
        (RepoPath::new("zzz/old").unwrap(), None),
        (RepoPath::new("a/new").unwrap(), state(usize::MAX)),
    ]);
    for n in 0..MAX_DIFF_ENTRIES {
        change.insert(RepoPath::new(format!("d/f{n:05}")).unwrap(), state(n));
    }
    let (old, new) = two_changesets(&store, base, change);
    let server = Server::start(data, store);
    let (old, new) = (old.to_string(), new.to_string());

    let text = server.text(&["diff", &old, &new]);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), MAX_DIFF_ENTRIES + 1);
    assert_eq!(lines[0], "R\tzzz/old -> a/new");
    assert!(lines[1..].iter().all(|line| line.starts_with("A\td/")));

    let diff = server.json(&["diff", &old, &new]);
    let entries = diff.as_array().unwrap();
    assert_eq!(entries.len(), MAX_DIFF_ENTRIES + 1);
    assert_eq!(entries[0]["path"], "a/new");
    assert_eq!(entries[0]["status"], "R");
    assert_eq!(entries[0]["old_path"], "zzz/old");
    assert_eq!(entries[0]["similarity"], 100);
    let id = BlobId::of(&usize::MAX.to_le_bytes()).to_string();
    assert_eq!(entries[0]["old"]["id"], id.as_str());
    assert_eq!(entries[0]["new"]["id"], id.as_str());
}

#[test]
fn ls_show_and_log_read_whole_replies() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    // A directory whose TreeV1 and a changeset whose ChangesetV1 are both
    // over one part, so `scn ls` and `scn show` read streamed replies; and
    // `scn log` on the same head.
    let state = |n: usize| {
        Some(FileState::Regular {
            content: BlobId::of(&n.to_le_bytes()),
            executable: false,
        })
    };
    let changes: BTreeMap<RepoPath, Option<FileState>> = (0..40_000)
        .map(|n| {
            (
                RepoPath::new(format!("f{n:05}.bin").as_bytes()).unwrap(),
                state(n),
            )
        })
        .collect();
    let (_, head) = two_changesets(&store, BTreeMap::new(), changes);
    let server = Server::start(data, store);

    let ls = server.text(&["ls", &format!("{head}:")]);
    assert_eq!(ls.lines().count(), 40_000, "every entry listed");
    assert!(ls.contains("f00000.bin"), "{ls:?}");
    let show = server.text(&["show", &head.to_string()]);
    assert!(show.contains("f00000.bin"), "{show:?}");
    let log = server.text(&["log", &head.to_string(), "--limit", "1"]);
    assert!(log.contains(&head.to_string()[..12]), "{log:?}");
}
