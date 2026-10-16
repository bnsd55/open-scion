# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project will follow [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

New entries go at the top of the `[v0.1.0] - unreleased` section below,
until that section gets its release date.

## [v0.1.0] - unreleased

### Added

#### Objects and byte format

- `scion-core`: the object model (changesets, trees, blob IDs) and a frozen,
  canonical v1 byte format, with golden tests, property tests and a fuzz
  target. Blobs belong to repos (schema version 4, table `repo_blobs`): a
  repo has the blobs uploaded to it and the blobs that its changesets name.
  `CreateChangeset` refuses a blob of another repo with the error of
  content that no repo stored, before it reads any file, so a changeset can
  no longer name another repo's blob and then read it. `PutBlob` keeps a
  stored copy only for a blob that its repo has, and else writes the
  content again, so its time does not show that another repo has the
  content. `GetObjects` still reads any object by ID (#13).

#### The server

- `scion-server`: SQLite metadata and trees, a blob folder with batched,
  verified durability, and one write path for changesets and their root
  trees. Group commit: concurrent blob syncs and database commits share one
  full drive flush, so a successful reply never shows state that a power
  loss can undo. `scion-server fsck <data>` reports the damage in a data
  directory: it is read-only, it can run while the server runs, and one
  line per problem names the kind, the ID and the path; it exits 0 when it
  saw no damage, 1 when it did, and 2 when it could not run.
- Server-side commits (`CreateChangeset`) and lands: path-level conflict
  checks through a per-ref index, one transaction per land, retries by
  request key, and a bounded retry when the ref moves. A land stops in its
  long loops 600 s after its call started, or as soon as its call is
  dropped, and stays pending. The path indexes of all refs share one
  budget of 128 MiB; the indexes of idle refs go out first, least recently
  used first, and an index that cannot fit starts over at the tip.
- One-way git import, checked against git: a re-import never moves a ref
  off later lands. The import can run while the server serves the same
  data directory: it moves the ref by compare-and-swap as a land does, its
  commits are durable before the server can see them, its write
  transactions end after 1 s so a server write waits for at most that plus
  one commit, a request that waits more than 5 s fails with `UNAVAILABLE`,
  and the temp files of a crashed import are removed by the next open of
  the directory while the server keeps running. A second server, or a
  process of another build, is refused.
- Derived data is derived in bounded steps, one transaction each, so other
  requests run between steps after a new kind or a version bump, and each
  step removes rows of older versions. `scion-server derive --kind KIND`
  fills a kind ahead of the first query. A kind can read the value of
  every parent, not only the first, and path history is derived parents
  first, in generation order.
- Reads use a pool of 8 read-only SQLite connections, so a read never
  waits for a write, for a long read such as a graph walk, or for a server
  write that waits for an import's write lock. Writes still take turns on
  one write connection.
- `scion-server serve` logs the address that it bound, so `--listen
  127.0.0.1:0` shows the port that the OS chose.

#### Lands

- `CreateChangeset` stores a changeset with one parent and moves no ref;
  `Land` rebases it onto a ref and moves the ref by compare-and-swap.
  Conflicts are path-level, through a per-ref index. A retry with the same
  request key returns the stored land, so a call that was cut never lands
  the same change twice, and a land that the ref moved under is retried a
  bounded number of times.
- Lands on a base more than 10,000 generations below the head check their
  paths in the head's path history, so no base is too old to check.
  Before, a base more than 100,000 changesets back was `REJECTED` (#38).
- `GetLand` and request keys are scoped by caller.

#### Reads, blame and queries

- Reads over the API: resolve a ref, get a changeset, read a file in 1 MiB
  parts from the zstd decoder under a 256 MiB budget for all replies,
  list a directory, and walk first-parent history. `ReadFile` checks the
  content against its ID before the last part, and ends a reply whose
  client stops reading with `DEADLINE_EXCEEDED` after 120 s.
- Path history as derived data (kind `path_history`, schema version 5).
  `Log` with a path reads only the changesets that change the path, so one
  call returns up to 1,000 entries however far apart they are. A call
  stops after about 64 MiB of path history, which a file in a large
  directory reaches in fewer entries. A level of path history above 64 KiB
  is cut into chunks at content-defined boundaries (a name cuts with
  chance 1 in 128), so a change in a directory of 100,000 names stores
  about 47 KiB of new rows, not the 6.3 MB node of every name the
  directory ever had. `scn log --path` and `scn blame`.
  An older server ignores the new `path` field, so `scn log --path`
  against it prints unfiltered history.
- `Blame`, a new RPC: which changeset last changed each line of a file, on
  first-parent history. It follows renames as `git blame` does by
  default: a deleted file with the same content, else a similar one (50%
  or more). Each changeset in a reply names the path of the file there
  (`BlameChangeset.path`). `scn blame` shows each line's path when some
  line came from another path, as git does, and `--json` adds `path` to
  each range.
- `Blame` blames a binary file line by line, as `git blame` does, and
  files of any size the shared memory budget holds: all `Blame` calls of a
  server share 1 GiB, and a call takes the bytes of every version it reads
  and 128 bytes per line of the file it blames. A call's first take may
  wait for bytes; every later take fails at once if they are not free, so
  two calls can never wait for each other. An older version that does not
  fit counts as absent, so its lines go to the changeset that replaced it.
  A call answers at most 1,048,576 lines of the file; a bigger one is
  blamed over several calls.
- A `Blame` reply that stopped at a bound carries an opaque cursor
  (`next_cursor`): where the walk goes on, where its rename search
  stopped, and which lines of the file the reply answered. The client MUST
  send it back unchanged, with the same repo, changeset and path; the
  server binds it to all three, checks every changeset it names, and
  refuses anything else with `INVALID_ARGUMENT`. Each new window re-walks
  the history from the requested changeset and diffs only the versions of
  its own window. When the search has looked at every deleted file, the
  reply says that too, and the next call takes the best file as the answer
  at once. `scn blame` does all of this.
- The query API: `MergeBase` (every best common ancestor, as
  `git merge-base --all`), `IsAncestor` (exit 0 or 1), and `Diff` (the
  paths whose file differs between two changesets, optionally below a
  path). The graph walks use generation numbers and a commit-graph index
  (derived data): `IsAncestor` reads a few dozen entries at any depth, and
  three rows per merge between, and `MergeBase` jumps over runs of first
  parents that only one part of the walk reaches. They have no step bound:
  each stops with `DEADLINE_EXCEEDED` at 600 s or once its call is
  dropped, and at most 4 run at once. `scn merge-base`, `scn is-ancestor`
  and `scn diff`. Every `scn` error exits 2, so it never reads as the
  "no" of `scn is-ancestor`.
- `Diff` finds renames as `git diff` does by default: same content, then
  the one file of a name on each side at 75%, then the most similar files
  at 50%. A rename keeps its two entries: the added one has `rename_from`,
  the deleted one `rename_to`, and both `similarity`, so a client that
  ignores them still sees a delete and an add. Each call finds the renames
  of the whole diff below its path, so an entry has the same rename in
  every reply. The inexact search (same name, then similar files) has a
  fixed budget per call. At most 4 `Diff`
  calls run at once. `scn diff` prints a rename once, as `R`, a tab and
  `old -> new`, and `--json` gives it `status` `R`, `old_path` and
  `similarity`.

#### Access, transport and limits

- Principals and access rules (#13, part 2). `scion-server serve --access
  FILE` reads an access file that maps the BLAKE3 digest of each token to
  a principal, and says which repos each principal may read and which
  paths it may write: a path prefix by path parts (`src` covers `src/a`,
  not `srcx`), no prefix meaning the whole repo, and write implying read.
  A principal with no rule for a repo cannot see the repo: every call
  answers `NOT_FOUND`, as for a repo that does not exist. A change to a
  path outside the principal's write prefixes answers `PERMISSION_DENIED`
  and names the first denied path; `PutBlob` needs a write rule, and
  `Land` is checked for the principal that lands it.
  `scion-server hash-token` prints the digest that a `token` line wants,
  so the file never holds a token. `--token T` stays: one principal,
  `default`, which may read and write every repo.
- Bearer-token auth, loopback-only listening by default, request limits
  and name validation at the RPC boundary.
- TLS and mTLS: the server and `scion-cache` serve TLS with `--tls-cert`
  and `--tls-key` (a non-loopback address then needs no
  `--insecure-listen`), and require a client certificate with
  `--tls-client-ca`. The cache and `scion-daemon` trust `--tls-ca` for
  their https upstream and present `--tls-client-cert` and
  `--tls-client-key` for mTLS. `scn` trusts `--tls-ca` for an https
  server. The TLS handshake of a connection ends within 20 s, and the
  HTTP/2 preface is still awaited within 20 s of the place, so a peer that
  stops before or after the handshake holds no connection place. The
  cryptography is ring (the `tls-ring` feature of tonic: 8 new crates,
  fewer than the 12 of `tls-aws-lc`, one of which needs cmake).
- `GetObjects` streams objects in parts of 1 MiB (`ObjectPart`): the first
  part names the object, its encoding and its sizes, so every receiver
  reserves memory before the bytes arrive and unpacks within that size. A
  blob travels in its stored zstd file, as it is, to a client that lists
  `ZSTD` in the `accept` field: the tiers keep and pass on that frame, and
  `scn checkout` unpacks it, so a blob is packed once and unpacked once. A
  tier fetches up to 64 objects at once from upstream, within the same
  400 MiB. A `GetObjects` reply never ends `OK` with no object, and one
  with no memory for the first object within 30 s fails with
  `RESOURCE_EXHAUSTED`.
- Receivers accept only plain Zstandard frames, with no dictionary and a
  window of at most 8 MiB; the zstd library is built without its legacy
  decoders.
- The server, `scion-cache` and `scion-daemon` serve at most 128
  connections at once (`--max-connections`), each with at most 256 open
  streams (`--max-streams`). A client beyond either cap waits, and a
  connection that waits logs a warning. Every connection closes in time:
  20 s after it gets its place if no HTTP/2 preface came; after the
  preface, 20 s after an unanswered ping, which goes out after 60 s
  without a frame; and 10 min after a `GOAWAY` at 30 min of age, so a
  daemon's lifelong connection to its cache gives its place up. So the
  number of stalled replies has a bound, and a connection beyond the cap
  waits at most 40 min. Size `--max-connections` on a cache for the
  daemons behind it, and on the server for the caches and direct clients.
- A reply whose client stops reading adds what it leaves in HTTP/2 (under
  4 MiB) to a count of its connection, and a connection whose stalled
  replies hold more than 64 MiB together (`--max-stalled-bytes`) is closed
  with them. Other connections are not touched. hyper cannot reset one
  stalled stream, so the connection is the unit of accounting (#113).
- The server and `scion-cache` turn off Nagle's algorithm on each TCP
  connection. It was on: tonic ignores its `tcp_nodelay` setting for a
  listener that it does not open itself. So a small reply, such as a unary
  answer or a status, could wait for the client's ACK, up to about 40 ms
  where the client delays its ACKs.
- A caller's deadline passes down through the tiers with `grpc-timeout`.
  Each tier ends every wait at the caller's deadline, less a margin (a
  tenth, at most 1 s), and sends what is left upstream, so the status of
  the deepest process reaches the caller for any deadline: a call through
  a daemon, a cache and a busy server fails with the server's
  `RESOURCE_EXHAUSTED`, never with a timeout of a tier in the middle. The
  server also ends its waits before the caller's deadline. With no
  deadline, the fixed limits per step still apply and nothing goes
  upstream. A caller whose deadline passes stops waiting; a fetch goes on
  for the callers that still wait, and a caller with a later deadline asks
  again with its own if the fetch failed on a shorter one (#98).
- `scion-tier`: the cache tiers. `scion-cache` keeps objects in memory and
  `scion-daemon` keeps them on disk, each under a byte limit, least
  recently used out first. The daemon listens on a Unix socket with mode
  0600. Each tier answers the `Objects` calls, checks every object from
  upstream against its ID before it stores or sends it, forwards
  `ResolveRef`, keeps `GetRootTree` answers, fetches a shared miss once,
  and needs a token.
- A `scion-cache --snapshot DIR --snapshot-repo R` writes `DIR/R.scnsnap`
  every `--snapshot-every` seconds: the most recently used objects of that
  repo, up to `--snapshot-bytes`. A cold `scion-daemon --seed FILE`
  imports them before it serves, each checked against its ID; a warm one
  ignores the file (#59).
- `scion-daemon` keeps its recency index in `<data>/index.db`, a SQLite
  database read and changed in place. The index is not held in memory: a
  `size` is one point lookup, and the daemon keeps only the last batch of
  row changes, at most 1,024 (or 64 MiB of new bytes), committed in one
  transaction, plus the database's own page cache of 8 MiB. After a clean
  stop (Ctrl-C, or SIGTERM, which it handles too), a start reads the rows
  and opens no object directory, and the order of use survives the
  restart; the stop commits, checkpoints the write-ahead log, and flushes
  it past the drive's cache. After a crash, a start still opens at once,
  at most one batch behind: every change carries a sequence number, and
  the start reads only the changes past the newest one committed. Files
  that a crash left before their batch are adopted by one low-priority
  walk of the object directories after the start: each file with no row
  comes back as the least recently used, and its bytes count against
  `--max-bytes`, so the limit holds again; the walk stops at a clean stop
  and logs what it adopted. A crash loses only what was used last, never
  an object. A store from a build before the index db has `<data>/index`
  (the append-only index of that format): the first start imports its
  records in file order and removes it.

#### scn

- `scn`, the command-line client: `cat`, `ls`, `log`, `show`, `commit`,
  `land` and `bench`, with `--json` output. `scn merge-base`, `scn
  is-ancestor` (exit 0 or 1) and `scn diff` for the query API, and
  `scn blame`. Every `scn` error exits 2, so it never reads as the "no" of
  `scn is-ancestor`.
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
- `scn` reaches a daemon with `--server unix:///PATH`. It first refuses a
  path that is not a socket owned by its user or by root. `scn` and the
  daemon also refuse a socket or data directory under a directory that
  another user can change: each directory up to `/` must be owned by root
  or by their user, and not writable by group or others unless it has the
  sticky bit.
- `scn` sends a 600 s deadline (the server's own limit) and a 10 s connect
  timeout on every call, so a waiting `scn` never hangs silently (#98).

#### Docs and scripts

- `docs/protocol-v1.md`: the contract of the `Scion` and `Objects`
  services, for independent clients, tiers and servers.- `scripts/demo.sh`, a demo that anyone can rerun on a fixed public
  repository, and `scripts/bench-checkout.sh`, a checkout benchmark
  against git. `docs/benchmarks.md` gives the method.

### Changed

- `Diff` finds renames as git does over diffs of 1,000,000 entries
  (`#107`). Exact renames (same content, by blob ID) run at any size in
  that range, where a diff of more than 10,000 entries before had none; a
  larger diff has no renames, and costs the call no more than about 150 MB
  of memory. The inexact search (same name, then similar files) keeps git's
  limit of 1,000 x 1,000 file pairs and its per-call work budget. When it
  did not run or stopped at the budget, or the diff is over the entry
  bound, the reply sets `renames_incomplete`, and `scn diff` prints git's
  warning about skipped inexact rename detection; the exact renames in the
  reply are still whole. Every call of the same diff reports the flag the
  same way.
