//! The import's derived rows - the root tree, the commit graph and the
//! path history of every commit - are written in batches: the values the
//! transaction inserted answer its own reads, so no derive reads the
//! database for a value the batch holds, the flush writes one statement
//! per 1,000 rows, and the older-version delete runs once per kind for
//! the whole transaction. The statement count is a process-wide seam, so
//! this guard owns its binary: no other test speaks SQL in its process.
//! Only the derived family's statements are counted, so the changeset,
//! tree, history and provenance writes of the same import stay out. On
//! SQLite the count is always zero, which is why the guard skips there.

use std::io::Write;
use std::process::{Command, Stdio};

use scion_server::{Backend, ImportOptions, import_git};

/// A throwaway git repository. `fast_commits` writes a chain of commits
/// on `main` with `git fast-import`, which is fast for long histories.
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

    /// Commits `n` commits on `main`, one file each. Returns the tip.
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

fn hex(bytes: &[u8]) -> String {
    let table = "0123456789abcdef";
    let mut out = String::new();
    for b in bytes {
        out.push(table.chars().nth(usize::from(b >> 4)).unwrap());
        out.push(table.chars().nth(usize::from(b & 15)).unwrap());
    }
    out
}

fn opts(rev: &str) -> ImportOptions {
    ImportOptions {
        rev: rev.to_owned(),
        target_ref: "main".to_owned(),
        verify_sample: 1000,
    }
}

#[test]
fn the_chain_read_walks_the_flushed_rows() {
    if !Backend::test_is_postgres() {
        eprintln!("skip on sqlite: this guard compares Postgres rows");
        return;
    }
    let git = Git::new();
    let tip = git.fast_commits(10);
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    import_git(&store, "main", git.path(), &opts(&tip)).unwrap();
    let schema = Backend::test_schema(dir.path()).unwrap();
    let mut client = postgres::Client::connect(
        &std::env::var("SCION_TEST_POSTGRES_URL").unwrap(),
        postgres::NoTls,
    )
    .unwrap();
    let n: i64 = client
        .query(
            &format!("SELECT count(*) FROM {schema}.changeset_parents"),
            &[],
        )
        .unwrap()[0]
        .get(0);
    eprintln!("changeset_parents rows after a 10-commit import: {n}");
    let rows = client
        .query(
            &format!("SELECT id FROM {schema}.changesets ORDER BY gen DESC LIMIT 1"),
            &[],
        )
        .unwrap();
    let tip_bytes: Vec<u8> = rows[0].get(0);
    let rows = client
        .query(
            &format!("SELECT n, parent FROM {schema}.changeset_parents WHERE cs = $1 ORDER BY n"),
            &[&tip_bytes],
        )
        .unwrap();
    eprintln!(
        "the tip's parents rows: {:?}",
        rows.iter()
            .map(|r| (r.get::<_, i64>(0), hex(&r.get::<_, Vec<u8>>(1))))
            .collect::<Vec<_>>()
    );
    let rows = client
        .query(
            &format!(
                "WITH RECURSIVE walk(id, depth) AS (
                     SELECT $1::bytea, 0::bigint
                     UNION ALL
                     SELECT cp.parent, w.depth + 1
                     FROM walk w
                     JOIN {schema}.changeset_parents cp ON cp.cs = w.id AND cp.n = 0
                     WHERE w.depth < 1000
                 )
                 SELECT depth, count(*) FROM walk GROUP BY depth ORDER BY depth",
            ),
            &[&tip_bytes],
        )
        .unwrap();
    let depth = rows.iter().map(|r| r.get::<_, i64>(0)).max().unwrap_or(0);
    assert_eq!(
        depth, 9,
        "the first-parent chain walk of the tip must reach every imported changeset: \
         the walk follows n = 0, so the parents' row numbers must count from zero"
    );
}

#[test]
fn the_imports_derived_family_costs_no_statement_per_commit() {
    if !Backend::test_is_postgres() {
        eprintln!("skip on sqlite: the statement count is a Postgres seam");
        return;
    }
    for n in [100, 1000] {
        let git = Git::new();
        let tip = git.fast_commits(n);
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::open_test(dir.path()).unwrap();
        Backend::test_reset_statement_count();
        import_git(&store, "main", git.path(), &opts(&tip)).unwrap();
        let log = Backend::test_statement_log();
        let count = u64::try_from(log.iter().filter(|sql| sql.contains("derived")).count())
            .unwrap_or(u64::MAX);
        let flushes = u64::try_from(
            log.iter()
                .filter(|sql| sql.contains("INSERT INTO derived") && sql.contains("unnest"))
                .count(),
        )
        .unwrap_or(u64::MAX);
        eprintln!(
            "the import of {n} commits wrote the derived family in {count} statements, of which {flushes} were the batch's flushes"
        );
        let mut shapes: Vec<(String, usize)> = Vec::new();
        for sql in log.iter().filter(|sql| sql.contains("derived")) {
            let short = sql.split_whitespace().take(4).collect::<Vec<_>>().join(" ");
            match shapes.iter_mut().find(|(s, _)| *s == short) {
                Some((_, c)) => *c += 1,
                None => shapes.push((short, 1)),
            }
        }
        eprintln!("shapes: {shapes:?}");
        // The batch wrote every derived row: the root tree, the commit
        // graph and the path history of each commit. An import that
        // drops the flush leaves the table short and fails here.
        let schema = Backend::test_schema(dir.path())
            .expect("the test backend is postgres, so the schema is named");
        let mut client = postgres::Client::connect(
            &std::env::var("SCION_TEST_POSTGRES_URL").unwrap(),
            postgres::NoTls,
        )
        .unwrap();
        let rows = client
            .query(&format!("SELECT count(*) FROM {schema}.derived"), &[])
            .unwrap();
        let stored: i64 = rows[0].get(0);
        assert_eq!(
            i64::try_from(3 * n).unwrap_or(i64::MAX),
            stored,
            "the import of {n} commits stored {stored} derived rows, not the 3 per commit the import derives"
        );
        // No statement per commit: the values the transaction inserted
        // answer its reads, the flush writes one statement per 1,000
        // rows, and the older-version delete runs once per kind. The
        // residue is a constant per transaction - the flush, three
        // deletes, the preloaded chain, and the handful of reads the
        // transaction's first commits' parents cost, whether the preload
        // held them or a jump outside it fell back to a single read. A
        // per-commit load, parent read, put or delete adds a statement
        // per commit and breaks this bound.
        let residue = count.saturating_sub(flushes);
        assert!(
            residue <= 6 * flushes + 6,
            "the import of {n} commits spent {residue} derived statements past the \\
             flushes, over the 6 per batch transaction ({flushes} transactions) \\
             the batch costs"
        );
    }
}
