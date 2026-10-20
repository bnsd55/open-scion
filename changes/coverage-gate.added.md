The statement-coverage gate: a coverage workflow (push to main and
workflow_dispatch, never pull_request) that measures the workspace with
cargo-llvm-cov v0.9.1, pinned by version and the release tarball's
sha256, and fails when the merged report drops under 80 percent lines -
the roadmap's statement bar, where a Rust statement is an executable
line. Two measures accumulate into one report: the default backend and
the Postgres backend, so pg.rs is in the number; the per-crate summary
prints in the log. The exclusions are checked in (scripts/coverage-exclusions.txt): the
file holds no patterns today, with the reasons - the generated proto
code builds into OUT_DIR under target/ and the measure never counts it,
and the test seam sits behind the test-hooks feature, which the
default-feature measure does not compile. A pattern that matches no
tracked file fails the job, so a vacuous exclusion cannot hide in the
file, and the report step proves its own filter: no excluded file may
appear in its printed list.
