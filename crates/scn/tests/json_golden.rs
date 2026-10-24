//! The JSON fields `scn` promises in 1.x, as one golden: for every
//! command this test runs, the field names and types of its `--json`
//! output are additive-only in 1.x — a minor may add a field, never
//! change or remove one. The golden sits beside this test, and the
//! check runs the binary, so no doc edit can make a removed field pass
//! (docs/stability.md, 1.x).
//!
//! Coverage: the read commands (`log`, `ls`, `show`, `blame`, `diff`,
//! `merge-base`, `is-ancestor`) and the write commands a script calls
//! (`commit`, `land`). `cat` prints file bytes, not JSON; `mount` needs
//! a FUSE mount and `bench` is a measurement, so none of the three
//! carries a golden row until it does.

#![cfg(unix)]

use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use scion_proto::runtime::listen::Limits;
use scion_server::test_support::service;
use scion_server::{Backend, ImportOptions, Quotas, import_git};
use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;

const TOKEN: &str = "secret";
const GOLDEN: &str = include_str!("json_fields_v1.golden");

#[test]
fn the_json_fields_the_golden_names_still_exist() {
    let fixture = Fixture::start();

    // The shapes the binary answers today, one set per command.
    let mut runs: Vec<(String, Vec<String>)> = Vec::new();

    let log = fixture.json(&["log", "--json", "main"]);
    runs.push(("log".into(), leaves(&log, "log")));
    // The history's two ends, as changeset IDs: newest first.
    let tip = log.as_array().unwrap()[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let root = log.as_array().unwrap().last().unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let ls = fixture.json(&["ls", "--json", "main:sub"]);
    runs.push(("ls".into(), leaves(&ls, "ls")));

    let show = fixture.json(&["show", "--json", "main"]);
    runs.push(("show".into(), leaves(&show, "show")));

    let blame = fixture.json(&["blame", "--json", "main:README.txt"]);
    runs.push(("blame".into(), leaves(&blame, "blame")));

    let diff = fixture.json(&["diff", "--json", &root, &tip]);
    runs.push(("diff".into(), leaves(&diff, "diff")));

    let merge_base = fixture.json(&["merge-base", "--json", "main", "main"]);
    runs.push(("merge-base".into(), leaves(&merge_base, "merge-base")));

    // A commit: one put file, its bytes from a local file.
    let put = fixture.write_local("golden.txt", b"the golden put\n");
    let commit = fixture.json(&[
        "commit",
        "--json",
        "--message",
        "the golden commit",
        "--put",
        &format!("golden.txt={}", put.display()),
    ]);
    runs.push(("commit".into(), leaves(&commit, "commit")));

    // Land the committed changeset onto main.
    let committed = commit["changeset"].as_str().unwrap().to_owned();
    let land = fixture.json(&["land", "--json", &committed]);
    runs.push(("land".into(), leaves(&land, "land")));

    let is_ancestor = fixture.json(&["is-ancestor", "--json", "--", &root, "main"]);
    runs.push(("is-ancestor".into(), leaves(&is_ancestor, "is-ancestor")));

    // The floor: everything the golden names must be there, with the
    // type the golden names. What the binary adds passes; what it
    // drops or retypes fails.
    let mut missing = Vec::new();
    let actual: Vec<(String, String)> = runs
        .iter()
        .flat_map(|(c, l)| l.iter().map(move |leaf| (c.clone(), leaf.clone())))
        .collect();
    for line in GOLDEN.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (command, leaf) = line
            .split_once(' ')
            .unwrap_or_else(|| panic!("the golden line must be `command path:type`: {line}"));
        let wanted = format!("{command} {leaf}");
        if !actual.iter().any(|(c, l)| l == leaf && c == command) {
            missing.push(wanted);
        }
    }
    assert!(
        missing.is_empty(),
        "the 1.x JSON fields shrank or changed type; a script that reads \
         them would break. Restore these, or drop them in a major release \
         with an upgrade note: {missing:?}"
    );
}

/// The leaf types of a JSON value, as `path:type` lines: `[0]` steps
/// into an array's first element, `.field` into an object. An empty
/// array is `path:array`; an empty object is `path:object`.
fn leaves(value: &Value, path: &str) -> Vec<String> {
    let mut out = Vec::new();
    leaf_types(value, path, &mut out);
    out
}

fn leaf_types(value: &Value, path: &str, out: &mut Vec<String>) {
    match value {
        Value::Null => out.push(format!("{path}:null")),
        Value::Bool(_) => out.push(format!("{path}:boolean")),
        Value::Number(_) => out.push(format!("{path}:number")),
        Value::String(_) => out.push(format!("{path}:string")),
        Value::Array(items) => match items.first() {
            Some(first) => leaf_types(first, &format!("{path}[0]"), out),
            None => out.push(format!("{path}:array")),
        },
        Value::Object(map) => {
            if map.is_empty() {
                out.push(format!("{path}:object"));
                return;
            }
            for (key, value) in map {
                leaf_types(value, &format!("{path}.{key}"), out);
            }
        }
    }
}

/// One git history, imported, served in-process.
struct Fixture {
    git_dir: TempDir,
    url: String,
    /// Keeps the server's tasks alive for the length of the test.
    _runtime: tokio::runtime::Runtime,
}

impl Fixture {
    fn start() -> Self {
        let git_dir = tempfile::tempdir().unwrap();
        let dir = git_dir.path();
        git(dir, &["init", "-q", "-b", "main"]);
        write_files(
            dir,
            &[("README.txt", Some("one\n")), ("sub/f.txt", Some("f\n"))],
        );
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "root"]);
        write_files(
            dir,
            &[("README.txt", Some("two\n")), ("gone.txt", Some("x\n"))],
        );
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "next"]);

        let data = tempfile::tempdir().unwrap();
        let store = Backend::open_test(data.path()).unwrap();
        let opts = ImportOptions {
            rev: "HEAD".into(),
            target_ref: "main".into(),
            verify_sample: 0,
        };
        import_git(&store, "r", dir, &opts).unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
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
                quotas: Quotas::off(),
                pending_lands: None,
                land_stale_secs: 1 << 40,
                tls: None,
                metrics: Arc::default(),
                reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
            },
            None,
            std::future::pending(),
        ));
        // The data directory must outlive the server: the temp dir leaks
        // for the length of the test process, and the runtime's drop at
        // the test's end stops the server first.
        std::mem::forget(data);
        Self {
            git_dir,
            url,
            _runtime: runtime,
        }
    }

    /// Runs `scn --json` against the fixture's server, repo `r`.
    fn json(&self, args: &[&str]) -> Value {
        let out = self.scn(args);
        assert!(
            out.status.success(),
            "scn {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_str(&String::from_utf8(out.stdout).unwrap()).unwrap()
    }

    fn scn(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["--server", &self.url, "--repo", "r"])
            .args(args)
            .env("SCION_TOKEN", TOKEN)
            .env("SCION_AUTHOR", "Ada <ada@example.com>")
            .output()
            .unwrap()
    }

    /// A local file for `--put`, and its path.
    fn write_local(&self, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let file = self.git_dir.path().join(name);
        fs::write(&file, bytes).unwrap();
        file
    }
}

/// Writes the files (`None` deletes one).
fn write_files(dir: &Path, files: &[(&str, Option<&str>)]) {
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
}

/// Runs git in `dir` with fixed identities and no user or system
/// config. Returns whether it succeeded, and stdout.
fn git_status(dir: &Path, args: &[&str]) -> (bool, String) {
    let out = Command::new("git")
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
