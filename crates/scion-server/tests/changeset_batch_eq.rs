//! The batch writes the rows the per-commit path writes: the same
//! archive imported twice, once one commit per write transaction and
//! once through the batch, and every table compared row by row. The
//! generations are the ones to watch: the batch computes them in memory
//! from the batch's parents, the per-commit path reads each parent from
//! the database.

use std::io::Write;
use std::process::{Command, Stdio};

use scion_server::{Backend, ImportOptions, import_git};

/// A throwaway git repository. `fast_commits` writes a chain of commits
/// on `main`, one file each.
struct Git(tempfile::TempDir);

impl Git {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let out = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .current_dir(dir.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(out.status.success());
        Self(dir)
    }

    /// Commits `n` commits on `main` and returns the tip.
    fn fast_commits(&self, n: usize) -> String {
        let mut stream = Vec::new();
        for i in 0..n {
            let (message, content) = (format!("c{i}\n"), format!("{i}\n"));
            let when = 1_700_000_000 + i;
            writeln!(stream, "commit refs/heads/main").unwrap();
            writeln!(stream, "committer Bob <bob@example.com> {when} +0000").unwrap();
            write!(stream, "data {}\n{message}", message.len()).unwrap();
            writeln!(stream, "M 100644 inline g/f{i}").unwrap();
            writeln!(stream, "data {}\n{content}", content.len()).unwrap();
        }
        let mut child = Command::new("git")
            .args(["fast-import", "--quiet"])
            .current_dir(self.0.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&stream).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "git fast-import: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(
            Command::new("git")
                .args(["rev-parse", "main"])
                .current_dir(self.0.path())
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim_end()
        .to_owned()
    }

    fn path(&self) -> &std::path::Path {
        self.0.path()
    }
}

fn opts(rev: &str) -> ImportOptions {
    ImportOptions {
        rev: rev.to_owned(),
        target_ref: "main".to_owned(),
        verify_sample: 1000,
    }
}

/// Every table the two paths write, with the columns in the order the
/// comparison reads them.
const TABLES: [&str; 8] = [
    "SELECT id, gen, body FROM changesets ORDER BY id",
    "SELECT cs, n, parent FROM changeset_parents ORDER BY cs, n",
    "SELECT repo_id, cs FROM repo_changesets ORDER BY repo_id, cs",
    "SELECT id, body FROM trees ORDER BY id",
    "SELECT kind, version, cs, value FROM derived ORDER BY kind, version, cs",
    "SELECT id, body FROM history_nodes ORDER BY id",
    "SELECT git_algo, git_oid, cs FROM git_import ORDER BY git_oid",
    "SELECT repo_id, name, target, version FROM refs ORDER BY repo_id, name",
];

#[test]
fn the_batch_writes_the_rows_the_per_commit_path_writes() {
    if !Backend::test_is_postgres() {
        eprintln!("skip on sqlite: this guard compares Postgres rows");
        return;
    }
    let git = Git::new();
    let tip = git.fast_commits(50);
    let mut rows = Vec::new();
    for per_commit in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::open_test(dir.path()).unwrap();
        scion_server::set_import_per_commit_txs(per_commit);
        import_git(&store, "main", git.path(), &opts(&tip)).unwrap();
        scion_server::set_import_per_commit_txs(false);
        let schema = Backend::test_schema(dir.path())
            .expect("the test backend is postgres, so the schema is named");
        let mut client = postgres::Client::connect(
            &std::env::var("SCION_TEST_POSTGRES_URL").unwrap(),
            postgres::NoTls,
        )
        .unwrap();
        let mut got = Vec::new();
        for table in TABLES {
            let qualified = table.replacen(" FROM ", &format!(" FROM {schema}."), 1);
            let rows = client.query(&qualified, &[]).unwrap();
            // The row's columns as text, so two runs compare by value.
            // The row's columns as text or hex, so two runs compare by
            // value.
            let rows = rows
                .iter()
                .map(|row| {
                    let cell = |c: usize| -> String {
                        if let Ok(Some(bytes)) = row.try_get::<_, Option<Vec<u8>>>(c) {
                            let hex = "0123456789abcdef".as_bytes();
                            let mut out = String::with_capacity(bytes.len() * 2);
                            for b in bytes {
                                out.push(hex[usize::from(b >> 4)] as char);
                                out.push(hex[usize::from(b & 15)] as char);
                            }
                            return out;
                        }
                        if let Ok(n) = row.try_get::<_, i64>(c) {
                            return n.to_string();
                        }
                        row.get::<_, String>(c)
                    };
                    (0..row.len()).map(cell).collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            got.push((table, rows));
        }
        rows.push((per_commit, got));
    }
    let batch = rows
        .iter()
        .find(|(per_commit, _)| !*per_commit)
        .expect("the batch run is in the list");
    let per_commit = rows
        .iter()
        .find(|(per_commit, _)| *per_commit)
        .expect("the per-commit run is in the list");
    for ((table, a), (_, b)) in batch.1.iter().zip(per_commit.1.iter()) {
        assert_eq!(
            a.len(),
            b.len(),
            "{table}: the batch and the per-commit path store different row counts"
        );
        for (ra, rb) in a.iter().zip(b.iter()) {
            assert_eq!(ra, rb, "{table}: the batch wrote a different row");
        }
    }
}
