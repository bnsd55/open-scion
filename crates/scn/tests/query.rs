//! `scn merge-base`, `scn is-ancestor` and `scn diff` against an in-process
//! server, compared with git on the same history.
#![cfg(unix)]

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::{Command, Output};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, GitOid, IdentityTime, RepoPath};
use scion_server::import::{ImportOptions, import_git};
use scion_server::read::MAX_DIFF_ENTRIES;
use scion_server::{Store, db, service, tree};
use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

const REPO: &str = "r";
const TOKEN: &str = "secret";

/// Runs git in `dir` with fixed identities and dates and no user or system
/// config. Returns whether it succeeded, and stdout.
fn git_status(dir: &Path, args: &[&str]) -> (bool, String) {
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
fn changesets(store: &Store, dir: &Path) -> HashMap<String, String> {
    git(dir, &["rev-list", "--all"])
        .lines()
        .map(|commit| {
            let oid: Vec<u8> = (0..commit.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&commit[i..i + 2], 16).unwrap())
                .collect();
            let id = store
                .read(|c| {
                    let cs: Vec<u8> = c.query_row(
                        "SELECT cs FROM git_import WHERE git_oid = ?1",
                        [&oid],
                        |row| row.get(0),
                    )?;
                    Ok(ChangesetId::try_from(cs.as_slice()).unwrap())
                })
                .unwrap();
            (commit.to_owned(), id.to_string())
        })
        .collect()
}

/// `scn diff --json` as git's `--name-status` lines, sorted.
fn name_status(diff: &Value) -> Vec<String> {
    let mut lines: Vec<String> = diff
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            format!(
                "{}\t{}",
                e["status"].as_str().unwrap(),
                e["path"].as_str().unwrap()
            )
        })
        .collect();
    lines.sort();
    lines
}

#[test]
fn merge_base_is_ancestor_and_diff_match_git() {
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
    commit(dir, "m1", &[("a.txt", Some("a2")), ("del", None)]);
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
    let store = Store::open(data.path()).unwrap();
    let opts = ImportOptions {
        rev: "HEAD".into(),
        target_ref: "main".into(),
        verify_sample: 0,
    };
    import_git(&store, REPO, dir, &opts).unwrap();
    let ids = changesets(&store, dir);
    let server = Server::start(data, store);

    for (x, cx) in &ids {
        for (y, cy) in &ids {
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

            let git_diff = git(dir, &["diff", "--name-status", "--no-renames", x, y]);
            let mut expected: Vec<&str> = git_diff.lines().collect();
            expected.sort_unstable();
            assert_eq!(
                name_status(&server.json(&["diff", cx, cy])),
                expected,
                "diff {x} {y}"
            );
        }
    }

    // Text output, refs, and a path.
    let text = server.text(&["merge-base", &ids[&x1], &ids[&x2]]);
    assert_eq!(text.lines().count(), 2, "{text}");
    assert_eq!(server.text(&["is-ancestor", &ids[&m1], "main"]), "");
    let root = git(dir, &["rev-list", "--max-parents=0", "HEAD"]);
    let text = server.text(&["diff", &ids[&root], "main", "--path", "a/"]);
    assert_eq!(text, "M\ta/b\n");
    // An error exits 2, so it never reads as the "no" of is-ancestor.
    let out = server.scn(&["is-ancestor", "no-such-ref", "main"]);
    assert_eq!(out.status.code(), Some(2));

    // The file states, as `scn ls --json` prints them.
    let diff = server.json(&["diff", &ids[&root], &ids[&m1]]);
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
}

#[test]
fn diff_continues_past_one_reply() {
    let data = tempfile::tempdir().unwrap();
    let store = Store::open(data.path()).unwrap();
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
    let (old, new) = store
        .write(|c| {
            let repo = db::create_repo(c, REPO)?;
            let mut parents = Vec::new();
            for changes in [BTreeMap::new(), changes.clone()] {
                let cs = Changeset {
                    parents: parents.last().copied().into_iter().collect(),
                    author: IdentityTime {
                        name: b"Ada".to_vec(),
                        email: b"ada@example.com".to_vec(),
                        unix_secs: 1_700_000_000,
                        tz_offset_minutes: 0,
                    },
                    committer: None,
                    message: b"change".to_vec(),
                    changes,
                };
                parents.push(tree::store_changeset(c, repo, &cs)?.0);
            }
            Ok((parents[0], parents[1]))
        })
        .unwrap();
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
