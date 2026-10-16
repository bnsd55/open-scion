# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project will follow [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

### Added

- Principals and access rules (`#13`, part 2). `scion-server serve --access
  FILE` reads an access file that maps the BLAKE3 digest of each token to a
  principal, and says which repos each principal may read and which paths it
  may write: a path prefix by path parts (`src` covers `src/a`, not
  `srcx`), no prefix meaning the whole repo, and write implying read. A
  principal with no rule for a repo cannot see the repo: every call answers
  `NOT_FOUND`, as for a repo that does not exist. A change to a path outside
  the principal's write prefixes answers `PERMISSION_DENIED` and names the
  first denied path; `PutBlob` needs a write rule, and `Land` is checked for
  the principal that lands it. `scion-server hash-token` prints the digest
  that a `token` line wants, so the file never holds a token. `--token T`
  stays: one principal, `default`, which may read and write every repo.

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
- `scion-server import` can run while the server runs on the same data
  directory. It moves the ref by compare-and-swap, as a land does, and its
  commits are durable before the server can see them. A second server, or
  a process of another build, is refused. A request that waits for the
  import's write lock for more than 5 s fails with `UNAVAILABLE`.
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
  rename onto a new empty directory that claims its name. Where it has no
  hard links either (some FUSE filesystems), each top-level file is copied
  to a new name made with `O_EXCL`, and each symlink is made again. On NFS
  version 3, a lost reply does not fail the checkout, from the temp
  directory it is written into to the move into place (#106). A name that
  the filesystem folds into one already written still fails it, also when
  both hold an empty file or a symlink to one target. This is tested by
  forcing it, not on a real NFS mount.
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
  counts against the per-call budget, once per 64 bytes.
- A `Blame` rename search looks at every file that the changeset deletes, as
  `git blame` does (was 100 files and about 100 MiB). It uses the budget
  that the call has left. Where the budget runs out, the reply says where
  the search stopped (`next_rename_searched`, `next_rename_best`), and the
  next call goes on from there with `rename_searched` and `rename_best`. A
  client must send both back unchanged. When the search has looked at every
  deleted file, the reply says that too, and the next call takes the best
  file as the answer at once. `scn blame` does this.
- Bearer-token auth, loopback-only listening by default, request limits and
  name validation at the RPC boundary.
- TLS and mTLS: the server and `scion-cache` serve TLS with `--tls-cert` and
  `--tls-key` (a non-loopback address then needs no `--insecure-listen`), and
  require a client certificate with `--tls-client-ca`. The cache and
  `scion-daemon` trust `--tls-ca` for their https upstream and present
  `--tls-client-cert` and `--tls-client-key` for mTLS. `scn` trusts `--tls-ca`
  for an https server. The TLS handshake of a connection ends within 20 s, and
  the HTTP/2 preface is still awaited within 20 s of the place, so a peer that
  stops before or after the handshake holds no connection place. The
  cryptography is ring (the `tls-ring` feature of tonic: 8 new crates, fewer
  than the 12 of `tls-aws-lc`, one of which needs cmake).
- Blobs belong to repos (schema version 4, table `repo_blobs`). A repo has
  the blobs uploaded to it and the blobs that its changesets name.
  `CreateChangeset` refuses a blob of another repo with the error of content
  that no repo stored, before it reads any file, so a changeset can no
  longer name another repo's blob and then read it. `PutBlob` keeps a stored
  copy only for a blob that its repo has, and else writes the content again,
  so its time does not show that another repo has the content. The upgrade
  adds the blobs of every changeset of each repo, in one transaction, and
  logs its progress. A blob uploaded before the upgrade and named by no
  changeset must be uploaded again. `GetObjects` still reads any object by
  ID (#13).
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
  path that is not a socket owned by its user or by root. `scn` and the
  daemon also refuse a socket or data directory under a directory that
  another user can change: each directory up to `/` must be owned by root
  or by their user, and not writable by group or others unless it has the
  sticky bit.
- A `GetObjects` reply never ends `OK` with no object. If the server or a
  tier has no memory for the first object within 30 s, the call fails
  with `RESOURCE_EXHAUSTED`.
- `GetObjects` sends blobs in their stored form: the server's zstd file, as
  it is, to a client that lists `ZSTD` in the new `accept` field. The tiers
  keep and pass on that frame, and `scn checkout` unpacks it, so a blob is
  packed once and unpacked once. Each object comes in parts of 1 MiB
  (`ObjectPart`, which replaces `Object`); the first part gives its encoding
  and sizes, so every receiver reserves memory before the bytes arrive and
  unpacks within that size. A tier now fetches up to 64 objects at once
  from upstream instead of 4, within the same 400 MiB. A reply cut off in
  the middle of an object ends with `DEADLINE_EXCEEDED`.
- The server, `scion-cache` and `scion-daemon` serve at most 128
  connections at once (`--max-connections`), each with at most 256 open
  streams (`--max-streams`). Before, both had no bound: tonic replaced
  hyper's default of 200 streams with none. A client beyond either cap
  waits, and a connection that waits logs a warning. Every connection
  closes in time: 20 s after it gets its place if no HTTP/2 preface came;
  after the preface, 20 s after an unanswered ping, which goes out after
  60 s without a frame; and 10 min after a `GOAWAY` at 30 min of age, so a
  daemon's lifelong connection to its cache gives its place up. So the
  number of stalled replies has a bound, and a connection beyond the cap
  waits at most 40 min. Size `--max-connections` on a cache for the
  daemons behind it, and on the server for the caches and direct clients.
- The server and `scion-cache` turn off Nagle's algorithm on each TCP
  connection. It was on: tonic ignores its `tcp_nodelay` setting for a
  listener that it does not open itself. So a small reply, such as a unary
  answer or a status, could wait for the client's ACK, up to about 40 ms
  where the client delays its ACKs.
- The tier timeouts nest: the server and tiers wait 30 s for reply memory,
  a tier 30 s for a fetch slot and 30 s for fetch memory, the cache 60 s per
  step of the server, the daemon 180 s per step of the cache.
- `scion-daemon` keeps its recency index in `<data>/index` too, appended in
  batches. After a clean stop (Ctrl-C, or SIGTERM, which it now handles
  too), a start reads that one file and opens no object directory, and the
  order of use survives the restart. The stop flushes the file, with a hash
  of its records, past the drive's cache. After a crash, or for a file that
  does not match its hash, a start scans every object file as before, and
  keeps the order of the index for the objects that it names. A crash loses
  only what was used last, never an object. After running a daemon build
  from before the index on the same data directory, remove `<data>/index`.
- Receivers accept only plain Zstandard frames, with no dictionary and a
  window of at most 8 MiB; the zstd library is built without its legacy
  decoders.
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
  holds one read connection (below), so writes and other reads run beside
  it.
- `Diff` finds renames as `git diff` does by default: same content, then the
  one file of a name on each side at 75%, then the most similar files at
  50%. A rename keeps its two entries. The added one has `rename_from`, the
  deleted one `rename_to`, and both `similarity`, so a client that ignores
  them still sees a delete and an add. Each call finds the renames of the
  whole diff below its path, so an entry has the same rename in every reply.
  A diff of more than 10,000 entries has no renames, and the search for
  similar files has a fixed budget per call. At most 4 `Diff` calls run at
  once. `scn diff` prints a rename once, as `R`, a tab and `old -> new`, and
  `--json` gives it `status` `R`, `old_path` and `similarity`.
- `Blame` and `Diff` find renames with one implementation. Chunks now go to
  git's hash buckets, as in git, so a score can change where two different
  chunks share a bucket.
- Reads use a pool of 8 read-only SQLite connections. A read no longer
  waits for a write, for a long read such as a graph walk, or for a server
  write that waits for an import's write lock. Writes still take turns
  on one connection.
- `scion-server serve` logs the address that it bound, so `--listen
  127.0.0.1:0` shows the port that the OS chose.

### Changed

- A caller's deadline passes down through the tiers with `grpc-timeout`.
  Each tier ends every wait at the caller's deadline, less a margin (a
  tenth, at most 1 s), and sends what is left upstream, so the status of
  the deepest process reaches the caller for any deadline: a call through
  a daemon, a cache and a busy server fails with the server's
  `RESOURCE_EXHAUSTED`, never with a timeout of a tier in the middle. The
  server also ends its waits before the caller's deadline: a `GetObjects`
  call with no memory for its first object within 30 s, or before the
  caller's deadline, fails with `RESOURCE_EXHAUSTED`. With no deadline,
  the fixed limits per step still apply and nothing goes upstream. A
  caller whose deadline passes stops waiting; a fetch goes on for the
  callers that still wait, and a caller with a later deadline asks again
  with its own if the fetch failed on a shorter one. `scn` sends a 600 s
  deadline (the server's own limit) and a 10 s connect timeout on every
  call, so a waiting `scn` never hangs silently (#98).
- A process now bounds what stalled replies hold: a reply whose client
  stops reading adds what it leaves in HTTP/2 (under 4 MiB) to a count of
  its connection, and a connection whose stalled replies hold more than
  64 MiB together (`--max-stalled-bytes`) is closed with them. Other
  connections are not touched. hyper cannot reset one stalled stream, so
  the connection is the unit of accounting; the ceiling drops from
  connections x streams x 4 MiB to connections x 64 MiB for streamed
  replies. A unary reply that its client never reads is not counted
  (hyper gives the server no hook that sees it stall) (#113).
