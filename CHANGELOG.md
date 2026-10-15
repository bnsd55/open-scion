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
  symlink, and each directory is opened about once. On failure, or on
  SIGINT, SIGTERM or SIGHUP, what was written is removed, and then `scn`
  dies by the signal, so a shell reports 128 plus its number (130 for
  Ctrl-C) and a shell loop stops too. A second signal stops the removal at
  once, and names the temp directory that it leaves.
  Where the filesystem has no rename that never replaces (NFS on Linux),
  a checkout into an existing directory still works: each top-level file
  or symlink moves up by a hard link, and each top-level directory by a
  rename onto a new empty directory that claims its name. This is tested
  by forcing it, not on a real NFS mount.
- Path history and blame on first-parent history: `Log` takes an optional
  path and says where to continue, and a new `Blame` RPC gives each line's
  last changeset. `scn log --path` and `scn blame`. Each call has a bound,
  and the client continues where a call stopped. An older server ignores
  the new `path` field, so `scn log --path` against it prints unfiltered
  history.
- `Blame` follows renames as `git blame` does by default: a deleted file
  with the same content, else a similar one (50% or more). Each changeset in
  a reply names the path of the file there (`BlameChangeset.path`), and a
  reply that stops says where to continue (`next_path`). `scn blame` shows
  each line's path when some line came from another path, as git does, and
  `--json` adds `path` to each range. A client that ignores `next_path`
  continues at the wrong path after a rename.
- `Blame` reads files of up to 16 MiB and 1,048,576 lines (was 1 MiB), and
  blames a binary file line by line, as `git blame` does. Every byte read
  counts against the per-call budget, once per 64 bytes. A rename search now
  stops after it has read about 100 MiB of deleted files, so with large files
  it can miss a source and the answer can change. Every search that the old
  1 MiB limit allowed still reads every file.
- Bearer-token auth, loopback-only listening by default, request limits and
  name validation at the RPC boundary.
- `ReadFile` streams a file in 1 MiB parts from the zstd decoder, under a
  256 MiB budget for all replies. It checks the content against its ID
  before the last part, and ends a stalled reply with `DEADLINE_EXCEEDED`.
- The path indexes of all refs share one budget of 128 MiB. When it is
  full, the indexes of idle refs go first, least recently used first. Else
  the growing index starts over at the tip. The lands still get the same
  answers.
- A land walks at most 100,000 changesets below the path index. A base
  further back is `REJECTED` as too old, and the client must rebase.
- A land stops in its long loops 600 s after its call started, or as soon
  as its call is dropped by a timeout or by the client, because neither
  stops a running job. The land stays pending.
- `scion-tier`: the cache tiers. `scion-cache` keeps objects in memory and
  `scion-daemon` keeps them on disk, each under a byte limit, least recently
  used out first. The daemon listens on a Unix socket with mode 0600. Each
  tier answers the `Objects` calls, checks every object from upstream
  against its ID before it stores or sends it, forwards `ResolveRef`, keeps
  `GetRootTree` answers, fetches a shared miss once, and needs a token.
- `scn` reaches a daemon with `--server unix:///PATH`. It first refuses a
  path that is not a socket owned by its user or by root.
- A `GetObjects` reply never ends `OK` with no object. If the server or a
  tier has no memory for the first object within 120 s, the call fails
  with `RESOURCE_EXHAUSTED`.
- `docs/protocol-v1.md`: the contract of the `Scion` and `Objects` services,
  for independent clients, tiers and servers.
- Query API: `MergeBase` (every best common ancestor, as
  `git merge-base --all`), `IsAncestor`, and `Diff` (the paths whose file
  differs between two changesets, optionally below a path). The graph walks
  use generation numbers. A `Diff` reply holds at most 1,000 paths and says
  where to continue. `scn merge-base`, `scn is-ancestor` (exit 0 or 1) and
  `scn diff`. Every `scn` error now exits 2, so it never reads as the "no"
  of `scn is-ancestor`.
- Derived data is derived in bounded steps, one transaction each, so other
  requests run between steps after a new kind or a version bump. Each step
  removes some rows of older versions. `scion-server derive --kind
  root_tree` fills a ref's history ahead of reads and removes the rest.
- `scripts/demo.sh`, a demo that anyone can rerun on a fixed public
  repository, and `scripts/bench-checkout.sh`, a checkout benchmark against
  git. `docs/benchmarks.md` gives the method.
- A kind of derived data can read the value of every parent, not only the
  first. Its history is derived parents first, in generation order.
- A commit-graph index as derived data (`commit_graph`): skip-list jumps
  over each first-parent history and a link to the newest merge on it.
  `IsAncestor` reads a few dozen entries at any depth, and three rows per
  merge between. `MergeBase` does the same when neither first-parent
  history has a merge newer than where the two meet; otherwise it walks.
  `scion-server derive --kind commit_graph` fills it ahead of the first
  query on an older database.
- `MergeBase` and `IsAncestor` have no step bound, so they answer across
  any number of merges, where they failed with `RESOURCE_EXHAUSTED` past
  10,000 steps. At most 4 run at once, and each stops with
  `DEADLINE_EXCEEDED` at 600 s or once its call is dropped. The merge-base
  walk jumps over runs of first parents that only one part of the walk
  reaches: a few dozen reads for a run, not one for each changeset. A walk
  holds the database lock for about 1,000 reads at a time, then leaves it
  free for other requests.
