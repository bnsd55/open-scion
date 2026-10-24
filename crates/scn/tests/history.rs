//! `scn blame` and `scn log --path` against an in-process server. Blame is
//! compared with `git blame --first-parent` of the same history.
#![cfg(unix)]

use std::cmp::Ordering;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::listen::Limits;
use scion_server::Backend;
use scion_server::test_support::blame::MAX_BLAME_VERSIONS;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::read::MAX_SCAN;
use scion_server::test_support::{service, tree};
use scion_server::{ImportOptions, import_git};
use serde_json::Value;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

const REPO: &str = "r";
const TOKEN: &str = "secret";

/// Runs git in `dir` with fixed identities and dates and no user or system
/// config, and returns stdout. Fails the test unless git succeeds.
fn git(dir: &Path, args: &[&str]) -> String {
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
    String::from_utf8(out.stdout).unwrap()
}

/// Writes the files in order (`None` deletes a file or a directory) and
/// commits everything.
fn commit(dir: &Path, message: &str, files: &[(&str, Option<&str>)]) {
    for (name, content) in files {
        let file = dir.join(name);
        match content {
            Some(content) => {
                fs::create_dir_all(file.parent().unwrap()).unwrap();
                fs::write(file, content).unwrap();
            }
            None if file.is_dir() => fs::remove_dir_all(file).unwrap(),
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

    /// Runs `scn` and returns its output.
    fn run(&self, args: &[&str]) -> String {
        let out: Output = Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["--server", &self.url, "--repo", REPO])
            .args(args)
            .env("SCION_TOKEN", TOKEN)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "scn {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    /// Runs `scn --json` and parses its output.
    fn json(&self, args: &[&str]) -> Value {
        serde_json::from_str(&self.run(&[&["--json"], args].concat())).unwrap()
    }
}

/// `scn blame --json` as one `(changeset, origin line, path there)` per
/// line.
fn blame_lines(server: &Server, spec: &str) -> Vec<(String, u64, String)> {
    let mut lines = Vec::new();
    for range in server.json(&["blame", spec]).as_array().unwrap() {
        assert_eq!(
            range["start_line"].as_u64().unwrap(),
            lines.len() as u64 + 1
        );
        let id = range["changeset"].as_str().unwrap().to_owned();
        let path = range["path"].as_str().unwrap().to_owned();
        let origin = range["origin_line"].as_u64().unwrap();
        for n in 0..range["line_count"].as_u64().unwrap() {
            lines.push((id.clone(), origin + n, path.clone()));
        }
    }
    lines
}

/// The changeset that a git commit was imported as, in hex.
fn changeset_of(store: &Backend, commit: &str) -> String {
    let oid: Vec<u8> = (0..commit.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&commit[i..i + 2], 16).unwrap())
        .collect();
    store
        .read(|c| {
            let cs: Vec<u8> = c
                .sqlite()
                .unwrap()
                .query_row(
                    "SELECT cs FROM git_import WHERE git_oid = ?1",
                    [oid],
                    |row| row.get(0),
                )
                .map_err(scion_server::test_support::error::from_sqlite)?;
            Ok(ChangesetId::try_from(cs.as_slice()).unwrap().to_string())
        })
        .unwrap()
}

/// `git blame --first-parent --line-porcelain` as one `(changeset, origin
/// line, path there)` per line, with each commit mapped to its changeset.
fn git_blame_lines(store: &Backend, dir: &Path, file: &str) -> Vec<(String, u64, String)> {
    let mut lines = Vec::new();
    // Each line starts with "<commit> <origin line> <line> [<count>]".
    let mut header = None;
    for line in git(dir, &["blame", "--first-parent", "--line-porcelain", file]).lines() {
        if line.starts_with('\t') {
            header = None;
        } else if header.is_none() {
            let fields: Vec<&str> = line.split(' ').collect();
            header = Some((changeset_of(store, fields[0]), fields[1].parse().unwrap()));
        } else if let Some(name) = line.strip_prefix("filename ") {
            let (id, origin) = header.clone().unwrap();
            lines.push((id, origin, name.to_owned()));
        }
    }
    lines
}

/// A git history of edits that blame must follow: inserts, edits, a merge,
/// a mode change, a delete and re-add, a file that becomes a directory and
/// a file again, CRLF line ends, an empty file, a last line with no newline,
/// and lines that a diff can place in more than one way.
fn blame_fixture(dir: &Path) {
    git(dir, &["init", "-q", "-b", "main"]);
    commit(
        dir,
        "add",
        &[
            (
                "lib.rs",
                Some("fn a() {\n    one();\n}\n\nfn b() {\n    two();\n}\n"),
            ),
            ("b.txt", Some("x\ny\n")),
            (
                "slide.txt",
                Some("top\n\n    y();\n}\n        z();\n        z();\nend\n"),
            ),
            ("crlf.txt", Some("one\r\ntwo\r\nthree\r\n")),
            ("empty.txt", Some("")),
            ("swap", Some("s1\ns2\n")),
        ],
    );
    commit(
        dir,
        "insert a function, swap becomes a directory",
        &[
            (
                "lib.rs",
                Some(
                    "fn a() {\n    one(1);\n}\n\nfn new() {\n    three();\n}\n\nfn b() {\n    two();\n}\n",
                ),
            ),
            ("swap", None),
            ("swap/inner", Some("i\n")),
        ],
    );
    git(dir, &["checkout", "-q", "-b", "side"]);
    commit(
        dir,
        "side",
        &[(
            "lib.rs",
            Some(
                "fn a() {\n    one(1);\n}\n\nfn new() {\n    three();\n}\n\nfn b() {\n    two(2);\n}\n",
            ),
        )],
    );
    git(dir, &["checkout", "-q", "main"]);
    commit(
        dir,
        "header, insert, no newline at the end, delete b.txt, swap is a file again",
        &[
            ("swap", None),
            ("swap", Some("s1\ns2\ns3\n")),
            ("crlf.txt", Some("one\r\nTWO\r\nthree\n")),
            (
                "lib.rs",
                Some(
                    "// header\n\nfn a() {\n    one(1);\n}\n\nfn new() {\n    four();\n    three();\n}\n\nfn b() {\n    two();\n}\n// end",
                ),
            ),
            ("b.txt", None),
        ],
    );
    git(dir, &["merge", "-q", "--no-ff", "side", "-m", "merge side"]);
    fs::set_permissions(dir.join("lib.rs"), fs::Permissions::from_mode(0o755)).unwrap();
    commit(
        dir,
        "executable only, re-add b.txt",
        &[("b.txt", Some("x\ny\nz\n"))],
    );
    commit(
        dir,
        "end with a newline",
        &[(
            "lib.rs",
            Some(
                "// header\n\nfn a() {\n    one(1);\n}\n\nfn new() {\n    four();\n    three();\n}\n\nfn b() {\n    two(2);\n}\n// end\n\nfn c() {}\n",
            ),
        )],
    );
    // Both ends change, so the diff cannot trim them. The two lines added
    // to slide.txt fit in more than one place. Here a Myers diff with the
    // indent heuristic places them where git does; Histogram, or no
    // heuristic, does not.
    commit(
        dir,
        "both ends, and lines that can slide",
        &[
            (
                "lib.rs",
                Some(
                    "// header 2\n\nfn a() {\n    one(1);\n}\n\nfn m() {\n    one(1);\n}\n\nfn new() {\n    four();\n    three();\n}\n\nfn b() {\n    two(2);\n}\n// end\n\nfn c() { }\n",
                ),
            ),
            (
                "slide.txt",
                Some("top 2\n\n    y();\n}\n        z();\n\n        z();\n        z();\nend 2\n"),
            ),
        ],
    );
}

#[test]
fn blame_matches_git_blame_first_parent() {
    if Backend::test_is_postgres() {
        // SHORTCUT: this ceiling is the skip itself; Postgres import
        // provenance (the git_import rows) is unread by these tests, so the
        // git side of the comparison has no Postgres run. Upgrade path: a
        // backend-agnostic git_import read on MetaTx, then drop the skip.
        // The scion blame lines the other rows of this file check are
        // backend-agnostic.
        eprintln!("skip on postgres: the test reads the data directory's git_import rows");
        return;
    }
    blame_matches_git_blame_first_parent_inner();
}

fn blame_matches_git_blame_first_parent_inner() {
    const FILES: [&str; 6] = [
        "src/lib.rs",
        "b.txt",
        "slide.txt",
        "crlf.txt",
        "empty.txt",
        "swap",
    ];
    let git_dir = tempfile::tempdir().unwrap();
    let dir = git_dir.path();
    blame_fixture(dir);
    // Then a rename into a new directory, with an edit.
    let lib = fs::read_to_string(dir.join("lib.rs")).unwrap();
    commit(
        dir,
        "rename lib.rs into a new directory, with an edit",
        &[
            ("lib.rs", None),
            ("src/lib.rs", Some(&lib.replace("header 2", "header 3"))),
        ],
    );
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    let opts = ImportOptions {
        rev: "HEAD".into(),
        target_ref: "main".into(),
        verify_sample: 0,
    };
    import_git(&store, REPO, dir, &opts).unwrap();
    let expected: Vec<_> = FILES
        .iter()
        .map(|file| git_blame_lines(&store, dir, file))
        .collect();
    let server = Server::start(data, store);
    for (file, expected) in FILES.iter().zip(&expected) {
        assert_eq!(
            &blame_lines(&server, &format!("main:{file}")),
            expected,
            "{file}"
        );
    }
    // As in git, the text output shows each line's path when some line
    // came from another path, after the changeset.
    let paths = |file: &str| -> Vec<String> {
        let out = server.run(&["blame", &format!("main:{file}")]);
        out.lines()
            .map(|line| line.split(' ').nth(1).unwrap().to_owned())
            .collect()
    };
    let renamed: Vec<String> = expected[0].iter().map(|(_, _, p)| p.clone()).collect();
    assert!(renamed.contains(&"lib.rs".into()) && renamed.contains(&"src/lib.rs".into()));
    assert_eq!(paths("src/lib.rs"), renamed);
    assert!(paths("b.txt").iter().all(|field| field.starts_with('(')));
}

fn regular(content: BlobId) -> FileState {
    FileState::Regular {
        content,
        executable: false,
    }
}

/// A changeset's message and the files it puts, or deletes (`None`).
type Puts<'a, S> = (Vec<u8>, Vec<(&'a str, S)>);

/// Stores a first-parent chain in a new repo, one changeset per entry.
/// Returns the IDs, oldest first.
fn chain<S: Into<Option<FileState>>>(
    store: &Backend,
    changesets: Vec<Puts<'_, S>>,
) -> Vec<ChangesetId> {
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut ids: Vec<ChangesetId> = Vec::new();
            for (message, files) in changesets {
                let cs = Changeset::new(
                    ids.last().copied().into_iter().collect(),
                    IdentityTime::new(
                        b"Ada".to_vec(),
                        b"ada@example.com".to_vec(),
                        1_700_000_000,
                        0,
                    )
                    .expect("identity fits the format"),
                    None,
                    message,
                    files
                        .into_iter()
                        .map(|(p, state)| (RepoPath::new(p).unwrap(), state.into()))
                        .collect(),
                )
                .expect("changeset fits the format");
                // The blobs of this chain are never read, only named; their lengths
                // are irrelevant to log and blame.
                ids.push(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0);
            }
            Ok(ids)
        })
        .unwrap()
}

/// The changeset IDs that `scn log --json` prints.
fn log_ids(server: &Server, args: &[&str]) -> Vec<String> {
    server
        .json(args)
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["id"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn log_path_and_blame_continue_past_the_server_bounds() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    // The root adds `f` with two lines. Then 2 * MAX_SCAN changesets change
    // only `g`, more than one `Blame` call walks. Then MAX_BLAME_VERSIONS
    // changesets each add one line at the top of `f`, one more than one
    // `Blame` call diffs.
    let mut text = String::from("line 1\nend\n");
    let mut changes = vec![vec![
        ("f", regular(store.blobs().put(text.as_bytes()).unwrap())),
        ("g", regular(BlobId::of(b""))),
    ]];
    changes.extend((0..2 * MAX_SCAN).map(|i| vec![("g", regular(BlobId::of(&i.to_le_bytes())))]));
    for n in 2..=MAX_BLAME_VERSIONS + 1 {
        text.insert_str(0, &format!("line {n}\n"));
        changes.push(vec![(
            "f",
            regular(store.blobs().put(text.as_bytes()).unwrap()),
        )]);
    }
    let message = b"change\n";
    let ids = chain(
        &store,
        changes
            .into_iter()
            .map(|files| (message.to_vec(), files))
            .collect(),
    );
    let tip = ids.last().unwrap().to_string();
    // The changesets that add `f` and its lines, newest first.
    let mut f_history: Vec<String> = ids[ids.len() - MAX_BLAME_VERSIONS..]
        .iter()
        .rev()
        .map(ToString::to_string)
        .collect();
    f_history.push(ids[0].to_string());
    let server = Server::start(data, store);

    assert_eq!(
        log_ids(&server, &["log", &tip, "--path", "f", "-n", "1000"]),
        f_history
    );

    // Each line came from the changeset that added it, as line 1 there. The
    // last line is line 2 of the root's file.
    let root = ids[0].to_string();
    let mut expected: Vec<_> = f_history
        .into_iter()
        .map(|id| (id, 1, "f".into()))
        .collect();
    expected.push((root, 2, "f".into()));
    assert_eq!(blame_lines(&server, &format!("{tip}:f")), expected);
}

#[test]
fn blame_continues_at_the_old_path_after_a_rename() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    // Each changeset adds one line at the top. The one at `before` also
    // renames `a` to `f`. One `Blame` call diffs MAX_BLAME_VERSIONS
    // versions, so it passes the rename and stops on the history of `a`.
    let before = MAX_BLAME_VERSIONS / 2 + 10;
    let mut text = String::new();
    let changes = (0..=before + MAX_BLAME_VERSIONS / 2)
        .map(|n| {
            text.insert_str(0, &format!("line {n}\n"));
            let put = Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
            let files = match n.cmp(&before) {
                Ordering::Less => vec![("a", put)],
                Ordering::Equal => vec![("a", None), ("f", put)],
                Ordering::Greater => vec![("f", put)],
            };
            (b"change\n".to_vec(), files)
        })
        .collect();
    let ids = chain(&store, changes);
    let tip = ids.last().unwrap().to_string();
    let server = Server::start(data, store);
    // Line k came from changeset `len - k`, where it was line 1.
    let len = ids.len();
    let expected: Vec<_> = (1..=len)
        .map(|k| {
            let at = if len - k < before { "a" } else { "f" };
            (ids[len - k].to_string(), 1, at.to_owned())
        })
        .collect();
    assert_eq!(blame_lines(&server, &format!("{tip}:f")), expected);
}

#[test]
fn blame_goes_on_with_a_rename_search_across_replies() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    // `new` replaces 24 deleted files of 65,536 lines, one chunk each, so
    // one call's budget reads about 15 of them, and the search goes on in a
    // second reply. All are 99% like `new`, so the first by path wins.
    let empty = "\n".repeat((1 << 16) - 1);
    let put = |text: String| Some(regular(store.blobs().put(text.as_bytes()).unwrap()));
    let names: Vec<String> = (0..24).map(|k| format!("f/{k:02}")).collect();
    let root = names
        .iter()
        .enumerate()
        .map(|(k, p)| (p.as_str(), put(format!("{empty}{k}\n"))))
        .collect();
    let mut rename: Vec<_> = names.iter().map(|p| (p.as_str(), None)).collect();
    rename.push(("new", put(format!("x\n{empty}"))));
    let message = b"change\n".to_vec();
    let ids = chain(&store, vec![(message.clone(), root), (message, rename)]);
    let server = Server::start(data, store);
    let lines = blame_lines(&server, &format!("{}:new", ids[1]));
    assert_eq!(lines.len(), 1 << 16);
    assert_eq!(lines[0], (ids[1].to_string(), 1, "new".to_owned()));
    let root = ids[0].to_string();
    assert!(
        lines[1..]
            .iter()
            .all(|(id, _, p)| *id == root && p == "f/00")
    );
}

#[test]
fn log_continues_after_a_reply_cut_by_its_byte_budget() {
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    // A reply holds about 32 MiB of changesets, so it stops after three of
    // these and must say where to continue.
    let ids = chain(
        &store,
        (0..5_u8)
            .map(|n| {
                let size = if n == 0 { 1 } else { 9 << 20 };
                (vec![b'a' + n; size], vec![("f", regular(BlobId::of(&[n])))])
            })
            .collect(),
    );
    let server = Server::start(data, store);
    let tip = ids.last().unwrap().to_string();
    let expected: Vec<String> = ids.iter().rev().map(ToString::to_string).collect();
    for args in [vec!["log", &tip], vec!["log", &tip, "--path", "f"]] {
        assert_eq!(log_ids(&server, &args), expected, "{args:?}");
    }
}
