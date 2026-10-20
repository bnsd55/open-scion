The CI's workflow tokens now run least-privileged everywhere, and code
scanning comes to the repository.

- The soak workflow, the one workflow without an explicit token grant,
  now declares `permissions: contents: read` at the top level, like the
  other four already do; no job in any workflow needs more.
- A new CodeQL workflow scans Rust and GitHub Actions workflows on
  pushes to main, on pull requests, and weekly. Rust analyzes in the
  CodeQL build mode `none` (no build), and the analysis job carries the
  single `security-events: write` grant it needs to upload its result.
