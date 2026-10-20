The statement-coverage gate: a coverage workflow (push to main and
workflow_dispatch, never pull_request) that measures the workspace with
cargo-llvm-cov v0.9.1, pinned by version and the release tarball's
sha256, and fails when the merged report drops under 80 percent lines -
the roadmap's statement bar, where a Rust statement is an executable
line. Two measures accumulate into one report: the default backend and
the Postgres backend, so pg.rs is in the number; the per-crate summary
prints in the log. The exclusions are checked in (scripts/coverage-exclusions.txt): the
file holds no patterns today, with the reasons - the generated proto
code builds into OUT_DIR under target/ and the measure never counts
it. The test seam is in the measure: a test build compiles the
test-hooks feature (the crate's own dev-dependency names it), so the
seam's ~900 lines count, accepted for now as a SHORTCUT whose upgrade
path is in the file - move the seam's items into their own files and
one pattern excludes them. A pattern that matches no tracked file
fails the job, so a vacuous exclusion cannot hide in the file, and the
report step proves its own filter: no excluded file may appear in its
printed list.
