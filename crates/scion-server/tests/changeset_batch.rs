//! The import's changeset rows are written in batches: the changeset row
//! lands per commit (the derived rows of the same commit name it by an
//! immediate foreign key, so it cannot wait for the flush), and the
//! parents, the repo membership and the generation reads cost a constant
//! per write transaction, not per commit. The statement count is a
//! process-wide seam, so this guard owns its binary: no other test speaks
//! SQL in its process. The count is what the job seam saw of the whole
//! import; only the changeset family's statements are counted, so the
//! tree, derived and provenance writes of the same import stay out. On
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

fn opts(rev: &str) -> ImportOptions {
    ImportOptions {
        rev: rev.to_owned(),
        target_ref: "main".to_owned(),
        verify_sample: 1000,
    }
}

#[test]
fn the_imports_changeset_family_costs_one_statement_per_commit_plus_the_batchs() {
    if !Backend::test_is_postgres() {
        eprintln!("skip on sqlite: the statement count is a Postgres seam");
        return;
    }
    let mut counts: Vec<u64> = Vec::new();
    let mut flushes: Vec<u64> = Vec::new();
    for n in [100, 1000] {
        let git = Git::new();
        let tip = git.fast_commits(n);
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::open_test(dir.path()).unwrap();
        Backend::test_reset_statement_count();
        import_git(&store, "main", git.path(), &opts(&tip)).unwrap();
        let log = Backend::test_statement_log();
        counts.push(
            u64::try_from(
                log.iter()
                    .filter(|sql| sql.contains("changesets") || sql.contains("changeset_parents"))
                    .count(),
            )
            .unwrap_or(u64::MAX),
        );
        flushes.push(
            u64::try_from(
                log.iter()
                    .filter(|sql| {
                        sql.contains("INSERT INTO changeset_parents") && sql.contains("unnest")
                    })
                    .count(),
            )
            .unwrap_or(u64::MAX),
        );
    }
    for (n, count, flush) in [(100, counts[0], flushes[0]), (1000, counts[1], flushes[1])] {
        eprintln!(
            "the import of {n} commits wrote the changeset family in {count} statements, of which {flush} were the batch's parent flushes"
        );
        // The family is written, and the batch with it: an import that
        // writes no changesets fails here.
        assert!(
            count >= n,
            "the import of {n} commits wrote {count} family statements, fewer than the {n} changeset rows"
        );
        // One statement per commit - the changeset row - plus a constant
        // per batch transaction: the parent flush, the membership flush
        // and the first commits' generation reads. A per-commit parents
        // or membership write adds another statement per commit and
        // breaks this bound; so does a per-commit generation read.
        let per_commit = count.saturating_sub(n);
        assert!(
            per_commit <= 3 * flush + 3,
            "the import of {n} commits spent {per_commit} family statements past the \\
             changeset rows, over the 3 per batch transaction ({flush} transactions) \\
             the batch costs"
        );
    }
}
