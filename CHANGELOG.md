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
- `scn checkout`, into a new directory or an existing empty one. An
  existing directory is kept, with its mode and owner, and its parent need
  not be writable. It must not be writable by all users unless it has the
  sticky bit. Writes go through directory handles and never follow a
  symlink, and each directory is opened about once. On failure or Ctrl-C,
  what was written is removed.
- Path history and blame on first-parent history: `Log` takes an optional
  path and says where to continue, and a new `Blame` RPC gives each line's
  last changeset. `scn log --path` and `scn blame`. Each call has a bound,
  and the client continues where a call stopped. An older server ignores
  the new `path` field, so `scn log --path` against it prints unfiltered
  history.
- Bearer-token auth, loopback-only listening by default, request limits and
  name validation at the RPC boundary.
- `docs/protocol-v1.md`: the contract of the `Scion` and `Objects` services,
  for independent clients, tiers and servers.
- Query API: `MergeBase` (every best common ancestor, as
  `git merge-base --all`), `IsAncestor`, and `Diff` (the paths whose file
  differs between two changesets, optionally below a path). The graph walks
  use generation numbers and stop at 10,000 changesets with
  `RESOURCE_EXHAUSTED`. A `Diff` reply holds at most 1,000 paths and says
  where to continue. `scn merge-base`, `scn is-ancestor` (exit 0 or 1) and
  `scn diff`. Every `scn` error now exits 2, so it never reads as the "no"
  of `scn is-ancestor`.
