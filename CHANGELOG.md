# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project will follow [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

### Added

- `scion-core`: the object model (changesets, trees, blob IDs) and a frozen,
  canonical v1 byte format, with golden tests, property tests and a fuzz
  target.
- `scion-server`: SQLite metadata and trees, a blob folder with batched,
  verified durability, and one write path for changesets and their root
  trees.
- Group commit: concurrent blob syncs and database commits share one full
  drive flush.
- One-way git import. Imported commits are checked against git, and a
  re-import never moves a ref off later lands.
- Server-side commits (`CreateChangeset`) and lands: path-level conflict
  checks through a per-ref index, one transaction per land, retries by
  request key, and bounded retry when the ref moves.
- Reads over the API: resolve a ref, read a file, list a directory, history.
- `scn`, the command-line client: `cat`, `ls`, `log`, `show`, `commit`,
  `land` and `bench`, with `--json` output.
- Path history and blame on first-parent history: `Log` takes an optional
  path and says where to continue, and a new `Blame` RPC gives each line's
  last changeset. `scn log --path` and `scn blame`. Each call has a bound,
  and the client continues where a call stopped.
- Bearer-token auth, loopback-only listening by default, request limits and
  name validation at the RPC boundary.
