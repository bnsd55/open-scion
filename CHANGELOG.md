# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project will follow [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

New entries go at the top of the `[v0.1.0] - unreleased` section below,
until that section gets its release date.

## [v0.1.0] - unreleased

The first release: the server, its cache tiers, and the `scn` client.

### Added

- A Postgres-backed store knows which database and which data directory
  belong together: the first open mints a store UUID, keeps it in the
  database and the data directory's `backend` marker, and an open that
  finds the two different is refused with both UUIDs named.
- A Postgres server that loses the one-server lock to another process is
  fenced: every read and write fails with the loss named, the health
  service answers `NOT_SERVING`, and the process exits after a short
  grace, so a supervisor restart either takes the lock back or refuses.
- `scion-server`, the server: changesets, trees and blobs in a data
  directory (SQLite metadata, one zstd file per blob), one-way git import
  checked against git, `fsck`, `gc`, `derive` and `hash-token` commands.
  The metadata may live in Postgres instead (`--postgres-url`); the blob
  files stay on disk in both backends (`#178`).
- Server-side writes: `PutBlob`, `CreateChangeset` (at most 100,000
  changes, one parent, no committer) and `Land`, which rebases onto a ref
  and moves it by compare-and-swap. Conflicts are path-level; there is no
  text merge. A retry with the same request key returns the stored
  outcome, so a cut-off call never lands the same change twice; a land
  whose ref moved is planned again, up to 3 times. A finished land is
  kept 7 days; one left `PENDING` longer than `--land-stale-secs` (a day)
  is rejected by a sweep at start and then hourly.
- Server-side commits and lands never show state that a power loss can
  undo: concurrent blob syncs and commits share one full drive flush, so
  a successful reply rests on a flushed drive (`#3`).
- One-way git import that can run beside a live server: it moves the ref
  by compare-and-swap, its commits are durable before the server sees
  them, and a re-import never moves a ref off later lands.
- `fsck` checks a data directory and reports one line per problem,
  read-only, safe beside a live server (exit 0 clean, 1 damage, 2 could
  not run). `gc` collects blobs and rows nothing names any more, past a
  grace period of `--grace-secs` (a week), rows before files, safe to
  repeat; `--dry-run` removes nothing. `derive --kind KIND --ref R`
  backfills derived data before the first query.
- The read API: `ResolveRef`, `GetChangeset`, `ReadFile`, `ListDir`,
  `Log` (with a path), `Blame`, `MergeBase`, `IsAncestor` and `Diff`.
  Streamed replies move in 1 MiB parts; a client that stops reading is
  cut off after 120 s.
- Path history is derived data: `Log` with a path returns the changesets
  that changed it, however far apart, and a level above 64 KiB is cut
  into chunks, so a directory that churns through names stays cheap
  (`#159`). `scn log --path` reads it.
- `Blame`: which changeset last changed each line, on first-parent
  history, across renames, on binary files too. All blame calls share a
  1 GiB budget; a file of more than 1,048,576 lines is blamed over
  several calls with a cursor.
- `Diff` finds renames as `git diff` does (same content, then same name
  at 75%, then the most similar at 50%), and names each rename's old
  path, new path and similarity.
- The query API runs on a derived commit-graph index, so `IsAncestor`
  reads a few dozen entries at any depth and `MergeBase` answers in a
  few dozen steps when the histories meet low. They are bounded only by
  the 600 s call limit, and at most 4 run at once.
- Read tickets (`#13`): an object ID alone is no permission. `GetRootTree`
  signs a 15-minute ticket for the repo, principal and root tree;
  `GetObjects` carries the ticket and each object's path, and the server
  and every tier walk the path, so nothing outside it is served. A tier
  refuses `ResolveRef` and `GetRootTree`: the server alone mints
  permissions.
- The metrics of the server and a cache on `--metrics-listen` (off by
  default, loopback only): request counts by RPC and code, a latency
  histogram, reply bytes by RPC, lands by outcome, land replans, view
  derivations, and a tier's cache hits and misses.
- The gRPC health service (`grpc.health.v1.Check` and `Watch`) on the
  serving port, without a token.
- Transport limits: at most 128 connections (`--max-connections`) of 256
  streams (`--max-streams`) per process, a stalled connection capped at
  64 MiB (`--max-stalled-bytes`), a connection closed after 30 min of age
  with a 10 min grace, and Nagle off.
- A caller's deadline passes down through the tiers with `grpc-timeout`,
  each hop keeping a margin, so a busy server's own status reaches the
  caller for any deadline (`#98`).
- Folder read rules (`#184`): a `read` rule may name path prefixes. A
  principal whose read rules are all prefixes reads the derived view of
  its prefixes, a hidden path answers like a missing one, and a partial
  reader cannot write.
- Principals and access rules (`#13`): `--access FILE` maps the BLAKE3
  digest of each token to a principal and its per-repo read and write
  rules; `scion-server hash-token` prints the digest an access file
  wants, so the file never holds a token.
- Per-principal quotas (`#15`): with `--access`, 64 requests in flight,
  512 MiB of upload bytes and 64 pending lands per principal by default,
  each overridable, a call over a cap failing with `RESOURCE_EXHAUSTED`.
- Read keys (`#13`, `#168`): `scion-server ticket-key --out FILE` makes
  the Ed25519 key that signs read tickets, and prints the public key for
  the tiers (`--ticket-public-key`, at most 2 during a rotation;
  `--ticket-key-old` moves the server to a new key).
- Bearer-token auth on every call, loopback-only listening by default,
  request limits and name validation at the RPC boundary.
- `scn`, the client: `cat`, `ls`, `log`, `blame`, `show`, `diff`,
  `merge-base`, `is-ancestor`, `commit`, `land`, `bench`, `checkout`,
  `mount` and `unmount`, with `--json` output. Every `scn` error exits 2,
  and `is-ancestor` answers with exit 0 or 1, so one never reads as the
  other.
- `scn checkout`, into a new or an existing empty directory: every entry
  is checked against its ID, a failure or signal removes what was
  written, and the checkout dies by the signal it caught (130 for
  Ctrl-C).
- `scn mount` and `scn unmount` (`#11`): one ref served as an NFSv3
  filesystem from `scn` itself, so a file loads when it is first read;
  `scn commit --from-mount DIR` commits exactly what the overlay holds.
  For single-user machines only: any local user who can read the mount
  table can reach the mount through the NFS port.
- `scn` reaches a `scion-daemon` over `unix:///PATH`, and checks the
  socket and every directory above it, as the daemon checks its own.
- `scion-tier`, the cache tiers: `scion-cache` keeps objects in memory
  and `scion-daemon` keeps them on disk, each least-recently-used out
  first under `--max-bytes` (1 GiB in memory, 10 GiB on disk). A cache
  listens on TCP, a daemon on a private Unix socket, one daemon per OS
  user. A tier answers only `GetObjects` and checks every object from
  upstream against its ID.
- Snapshots: `scion-cache --snapshot DIR --snapshot-repo R` writes
  `DIR/R.scnsnap` every `--snapshot-every` seconds, and a cold
  `scion-daemon --seed FILE` imports it, each object checked against its
  ID (`#59`).
- The daemon keeps its recency in `<data>/index.db`, so a clean stop
  reopens without reading every stored object, and a crash loses only
  the recency of the last batch, never an object.
- The gRPC health service (`grpc.health.v1`) on every process's serving
  port, without a token, so a probe needs none.
- Prometheus metrics on the server and the cache
  (`--metrics-listen`, off by default, loopback only): request counts by
  RPC and code, a latency histogram, reply bytes, lands by outcome, land
  replans, view derivations, and a tier's cache hits and misses (`#144`).
- One request ID per command, carried in `x-request-id` through every
  hop and logged as `request_id`, so one operation can be followed
  across scn, daemon, cache and server (`#144`).
- TLS and mTLS: the server and a cache serve TLS (`--tls-cert`,
  `--tls-key`) and can require a client certificate
  (`--tls-client-ca`); the tiers trust `--tls-ca` and can present a
  client certificate; `scn` trusts `--tls-ca`.
- `GetObjects` streams objects in parts of 1 MiB, the first part naming
  the object, its encoding and its sizes; a blob travels as its stored
  zstd frame, so it is packed once and unpacked once. A reply that has
  no memory for its first object within 30 s fails with
  `RESOURCE_EXHAUSTED`.
- One-way git import, safe to repeat and safe beside a live server: a
  re-import never moves a ref off later lands, and every root and merge
  commit is checked against git, plus `--verify` (100) others.
- Docs and scripts: the protocol contract (`docs/protocol-v1.md`), the
  operations guide (`docs/operations.md`), the CLI reference
  (`docs/cli.md`), the benchmark method (`docs/benchmarks.md`), the
  fault matrix (`docs/faults.md`, `#264`), `scripts/demo.sh`,
  `scripts/bench-checkout.sh`, a grpcurl conformance
  check (`#163`), a compose example with a server and a cache with TLS
  (`#164`), and a demo of the server on Postgres, with TLS to the
  database (`#195`).

### Changed

- Releasing is for a source release: `docs/RELEASING.md` cuts a
  release as a git tag plus GitHub release notes, with no crates.io
  publish until the crates' public APIs are narrowed (#21). The
  release PR's checklist merges the `changes/` fragments with
  `scripts/merge-changelog.sh`, builds a clean checkout through the
  README's `Try it` flow, runs the umask-002 CI job once #302 lands
  it, and states the provenance of every benchmark number: the
  commit, OS, filesystem, dataset, command and variance, and never
  the maintainer's machine. It tells how to undo a bad tag: redo the
  tag only while the release had no users, else mark it and cut the
  next patch version.
- A second Ctrl-C or SIGTERM while the server drains ends it at once
  (`#294`): a drain waits for the calls in flight, and one held open by
  a stuck client left the operator with a server that answered nothing
  and ended only under `kill -9`. The first signal's graceful drain is
  as it was; the second cuts the calls in flight, logs that it did,
  and exits 1.
- A git import no longer re-reads and re-fsyncs blobs that a committed
  row of the target repo already names (#238): one row probe per
  distinct blob proves the stored copy durable, so a branch import over
  an existing history skips the read-backs and the drive-cache flushes
  of its unchanged files. Blobs without a row keep the full put path.
- `open-scion-server`'s public API is a small facade: `Backend`,
  `service::{serve, ServeOptions}`, `access::Access`, `tickets::Keys`,
  `quotas::Quotas`, `metrics::Metrics`,
  `import::{import_git, ImportOptions, ImportReport}`, `store::Store`,
  `error::{Error, Result}`, `fsck`, `gc` with `DEFAULT_GRACE_SECS`,
  `graph::{backfill_commit_graph, CommitGraph}`,
  `history::{backfill_path_history, PathHistory}` and
  `tree::{backfill_root_tree, RootTree}`. The modules behind it
  exist for the two binaries and can change without a release; a ref
  read goes through `Backend::resolve_ref`, where code that reached
  past the facade used to go.
- The server's test seams (`open_test`, the import's batch-boundary and
  busy-retry hooks, the commit-failure and busy-timeout seams) sit
  behind a `test-hooks` cargo feature, default off: a build without it
  carries none of them, and nothing behind the feature, and nothing
  `#[doc(hidden)]`, is API.
- `scn mount` now works with the system's NFS client (`#238`): the
  listener serves the export name its printed command mounts, where a
  real kernel client was refused with `MNT3ERR_NOENT`.
- `fsck` and `gc` run against a Postgres-backed store (`#180`), through
  the same metadata interface as on a data directory: both used to
  refuse it. The collector's batches on Postgres hold one advisory key
  per schema and at most 256 rows each, so a writer waits at most one
  batch behind the collector.
- `scn` errors say what to check next: one `check:` line under the error,
  keyed to the failure — a refused address, a certificate the client
  does not trust, a wrong token — and `scn mount` refuses a missing or
  non-directory mount point before it serves anything (#246).
- A partial reader's view is memoized per tree (`#121`): a new changeset
  re-derives only the folders on its changed paths, and rows of older
  projection versions or dead rule sets are swept in bounded steps.
- The server, the cache and the daemon log to stderr (`#179`): stdout
  carries only the output of `ticket-key` and `hash-token`.
- `Diff` finds renames over diffs of up to 1,000,000 entries (`#107`):
  exact renames run at any size in that range, where a diff of more than
  10,000 entries before had none. When the inexact search did not run or
  stopped at its budget, the reply says `renames_incomplete`, and `scn
  diff` prints git's warning about skipped inexact rename detection.
- The git import now documents what it verifies: the blobs it writes are
  read back and checked, and a blob it skips, because a committed row of
  the repo already names it and its file is present, is trusted - damage
  inside an already stored file is `fsck`'s job, and the import no longer
  claims to have caught it. A test pins the trust and fsck's report.
- A git commit with more changed paths than the write path's limit
  (100,000) is refused with an error that names the commit and the count,
  before the import builds the whole changeset in memory.

### Fixed

- A directory that the server, a cache tier or `scn` makes is mode 0700,
  whatever the umask (`#303`). On a default Ubuntu account (umask 002),
  the first command on a new data directory failed: the server refused
  the group-writable directory it had just made. A directory that
  already exists keeps its mode for the existing checks, and a
  group-writable parent that the user made is still refused (#302).
- The mount tests' serve-lock probes no longer race a parallel test's
  fork: a fork copies the process's open lock files to the child until
  the child execs, and macOS is slow to exec, so a parallel test's fork
  could hold a probe's lock for ticks and flip its answer
  (`an_unmount_does_not_signal_an_unrelated_process` failed twice this
  way). The tests that fork or read a serve lock now run one at a time
  under a gate, and each stand-in server they start runs in its own
  process group, killed and reaped whole on drop.
- A blame call whose wait for the memory budget ended as the bytes it
  needed came free is served, where it was refused before: the take
  rechecks the budget after the wait, and refuses only a budget that
  still does not hold the bytes. That refusal now answers
  `DEADLINE_EXCEEDED`, the code every other deadline gives, instead of
  `RESOURCE_EXHAUSTED`; the doc says so.

- docs/architecture.md said a tier forwards `ResolveRef` and caches
  `GetRootTree`; it refuses both with `UNIMPLEMENTED` (`#307`), as its own
  statements elsewhere in the doc already said. The tier tables now
  match the code, the duplicated rename-index sentences are gone, the
  derived-data table names `derived::get_by` and the deadline a cold
  derive now carries, the blame bounds say the walk checks the deadline
  between steps and refuses a file over the diff-line cap, and the fault
  suites name the dm-log-writes suite that the directory-fsync row used
  to list as future work.
- A blame's window doc matched a rule that no longer holds, and the
  code behind it could not run (`#305`): a file of more than
  1,048,576 lines is refused, and a window is that many lines, so one
  call's window holds the whole servable file. The second-window
  branch of the resume, which re-walked the history from the request
  for a file's next window, is gone; the cursor still goes on over the
  walk's versions.

- A derive walk that answers a request holds its frontier of seen and
  pending changesets inside a byte budget the store's walks share:
  before, a walk over every parent held about 40 bytes per cold
  changeset with no bound in repo depth, so 10M commits after a
  derived-version bump held about 3 GB, eight walks at once. Over the
  budget the walk stops at its step boundary and the call answers
  `Unavailable` for a retry; the unbounded forms (fsck, import, the
  derive command) still finish the walk.
- A blame walk checks the call's deadline before each step, the first
  one whose memory share is more than the whole budget does, and a
  version over the cap in the file's history counts as absent, as one
  whose bytes do not fit the budget does. Before, the first step's
  diff ran past every bound: two versions of a file at the blob limit
  that share nothing kept a worker for hours, with no deadline check
  inside the diff.
- A damaged backend marker is refused, in both directions: a marker
  whose first line is neither `postgres` nor absent used to be read as
  absent, so a Postgres-backed directory opened as a fresh, empty
  SQLite store, which looks like a server with no data at all. The
  error says what to check. The fresh-directory claim that preceded
  the durable marker write is gone: `write_marker` writes the same
  first line, durable, so no crash can leave a partial marker behind.
- The `serve` flags are all checked before the data directory opens:
  a bad flag answers with its own error, instead of taking the data
  directory's one-server lock or creating a file in an empty directory.
- `--land-stale-secs` refuses 0 and negative values, and
  `--max-principal-requests`, `--max-principal-upload-bytes` and
  `--max-principal-pending-lands` refuse 0: those values used to mean
  that the sweep rejects every pending land, or that every call and
  land is refused, with no word of it at start.

- A fill of a kind over every parent (the commit graph) no longer holds
  a parent once per child that reached it in its walk: the seen set now
  checks pushes, so the walk's queue is as wide as the graph's frontier
  instead of as wide as its edges.
- A fill step reads the blob lengths its trees carry before its write
  transaction, not under the one write lock: up to a step's changesets
  of frame-header file reads sat between the lock's take and its
  release. Kinds that never read lengths (the commit graph, the path
  history) read no blob files at all.
- A land's check of a base that the path index does not reach kept
  every history node row it read, with no cap on their bytes: a row is
  up to 64 KiB, and one check reads one or more per part of each of the
  land's paths, so only the deadline stopped the read. The check's
  cache now clears its rows once they pass the same byte cap a log
  page's nodes stop at, so the answer never changes and only re-reads
  cost more.
- A silent TLS peer holds its connection's place for at most the
  handshake timeout, not twice that: one deadline from the accept
  bounds the TLS handshake and the HTTP/2 preface that follows it
  together, so the preface timer gets what is left of the deadline
  after the handshake ends.
- A `Diff` call's rename search could index every large added file for
  one deleted file, far past the budget that bounds it: one file's work
  was its index of every added file it could be like, and the budget
  was checked only between files, so a diff of many large added files
  read and indexed all of them for the first one (`#290`). The budget
  is now checked after each added file's slot in the index, so one
  call's cost stays at most the cap, plus one slot, plus one deleted
  file's read and lookup; a file whose indexing crosses the cap is not
  read and adds no scores, and the reply's `renames_incomplete` says
  the search stopped. A resumed search has one target, so its seed
  always completes and the search moves.
- `commit --from-mount` and the mount carry raw file names: a name that
  is not UTF-8 is no longer read from, or written to, the file its
  lossy spelling names.
- `commit --put` refuses anything that is not a regular file: a FIFO
  or a device never sees an end, and the read grew until the OOM
  killer.
- A Postgres read whose connection died inside its transaction left the
  pooled client unusable: the worker that serves the client kept the
  transaction's lost state on every later BEGIN, so every statement of
  that client failed and the read that opened a fresh transaction was
  refused too, with nothing to roll the open one back. The worker now
  follows a BEGIN that ran on the fresh session (the lost transaction
  is gone with the old session, and its caller cannot commit it
  through a new BEGIN), and a read's timeout statements that fail
  after the BEGIN roll the read transaction back before the reader
  returns to the pool.
- The land metrics count lands whose call ended in an error: a land
  that answered `Aborted`, `DeadlineExceeded`, or any other error class
  moved no counter, so an operator could not see failing lands. The
  land counter now moves for the error's class, under the same
  `scion_lands_total{outcome}` series.
- Two reads stop at the caller's deadline and the reply's budget
  (#283): a `Log` call without a path read up to 1,000 changesets, each
  as large as a changeset can be, with the call's deadline never
  checked; a `GetObjects` call's ticket walk decoded every tree on up
  to 1,000 requested paths, however many bytes that is, before the
  reply budget that bounds the reply was even consulted. The log page
  now checks the deadline at each entry, and the walk hands a tree to
  the walk only while the bytes it holds fit the budget's whole size,
  and stops at the deadline, before it decodes one more tree.
- `gc` no longer unlinks the file of a blob that a `PutBlob` in another
  process is writing at that moment: a put of an old orphan kept its
  file with a touch and renamed a replacing one, and a collector whose
  re-stat and unlink ran between those and the put's row commit deleted
  the file, so an acknowledged put named a file that was gone. The put
  and the sweep now hold the fan-out directory's `flock` around their
  file work, which orders them across processes the way the in-process
  stripe orders the writers of one process; a touch or rename that
  lands first changes the mtime the sweep re-stats, and an unlink that
  lands first makes the put's keep see a missing file and write anew.
- `fsck` no longer reports a blob-missing when the file it lists is an
  old orphan the concurrent `gc` collected while the check ran: a file
  that is gone and named by no row is the collector's take, not damage,
  and a file that is back between the check's two looks was rewritten
  by a racing put. A file a row names is still reported missing.

- A git import gives up instead of planning a ref's move again without
  end (#280): a ref that other lands keep moving while the import
  runs made the import check the ref's ancestry again and again, with
  no bound on the tries or on each walk of the history. The import now
  tries the move at most 3 times, answers that the ref kept moving,
  and checks ancestry through the derived commit graph, which reads a
  few nodes and stops at a deadline, instead of walking parents
  without one.

- A disk cache whose index row names more bytes than any object holds
  answers that row as corrupt, naming the object, instead of sizing
  its read buffer for the row.
- A snapshot seeded into a daemon counts an object that the store's
  byte limit refuses as skipped, so the seeded log line says what the
  store really holds.
- Two reads of a Postgres-backed store no longer panic on a column's
  type (`#252`): `GetObjects`' tree-size read and `fsck`'s
  derived-row version read wanted a width Postgres does not give, and
  the database driver panics on that. Both read what the column is
  now, and a derived-row version that overflows answers as corruption,
  not a crash.
- A write that the disk refused no longer looks like a server bug
  (#247): an out-of-space file write, SQLite metadata database or
  Postgres metadata database answers `RESOURCE_EXHAUSTED`, "the server
  is out of disk space; retry later", with no path in the reply; the
  server log carries the path and the operation. A cut-off write leaves
  no torn blob file, and an import that ran out of space is not retried
  while the disk is full.
- The container demo's output folders are git-ignored again (`#164`):
  `.gitignore` named `docker/demo/certs` and `docker/demo/work`, while
  `docker/demo.sh` writes `docker/certs` and `docker/work`, so a demo run
  left the demo CA's private key untracked instead of ignored.
- `Log` no longer repeats a page past the first scan (`#203`): a call
  whose `limit` crossed one internal page of 1,000 entries got those
  entries twice.
- A `Blame` cursor whose run length overflows the line count is refused
  with `INVALID_ARGUMENT` (`#201`), where a release build wrapped the
  count and answered a wrong window.
- `scn` prints each part of an error once, where the printed chain said
  the transport error twice.
- `scion-server hash-token --token` has a help text.
- The mount table does not fail silently (`#240`): an unmount whose kill
  fails keeps the record for a retry; a corrupt `mounts.json` is an
  error, not an empty table, so `scn mount` cannot double-mount a
  directory; the stop path reports on stderr what it could not clean; a
  tier sweep logs a removal it cannot make.

### Security

- The CI workflow pins every third-party action to the commit SHA its
  version tag named, with the version in a comment, runs on fixed
  runner images (`ubuntu-24.04`, `macos-15`) and pins the Postgres
  service to `postgres:17.11`; Dependabot keeps the pins current, and
  a runner or image upgrade is a diff, not a surprise. The MSRV job
  checks everything the CI checks, locked, with every feature, on
  `cargo +1.88`.
- A blame cursor that claims an impossible line count is refused as a
  bad cursor (`#288`): the count is the client's word, and its
  per-line memory cost was multiplied without an overflow check, so a
  count of 2^57 wrapped to zero and the walk went on to size its
  window from the count - one request could stop the server.

- The mount's overlay writes no longer follow a symlink planted in the
  overlay: a write, a truncate, a mode change, and the deletions note
  refuse one. A symlinked parent directory still redirects (a named
  limit: the upgrade is opening every parent through a handle chain,
  as `materialize.rs` does).

- The server refuses a data directory that group or others can write, or
  that sits under a directory another user can change: the blobs and the
  metadata are only as private as their directory (`#197`).
- The ticket-key seed must be readable by its owner alone, a public-key
  file must not be group-writable, both refuse a symlink, and a second
  `ticket-key` run refuses, so a key cannot change under the tiers
  (`#168`).
- `scion-daemon --seed` refuses a snapshot unless `--seed-full-repo` says
  every user of the node may read the repo whole (`#241`): a snapshot is
  a full-repo credential.
