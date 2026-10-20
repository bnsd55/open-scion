The statement-coverage gate: a coverage workflow (push to main and
workflow_dispatch, never pull_request) that measures the workspace with
cargo-llvm-cov v0.9.1, pinned by version and the release tarball's
sha256, and fails when the merged report drops under 80 percent lines -
the roadmap's statement bar, where a Rust statement is an executable
line. Two measures accumulate into one report: the default backend and
the Postgres backend, so pg.rs is in the number; the per-crate summary
prints in the log. The exclusions are checked in
(scripts/coverage-exclusions.txt): the test seam only, with its reason;
the generated proto code is out of the measure by construction (it
builds into OUT_DIR under target/) and the file says so.
