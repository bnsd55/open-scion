# The production audit

The blocking audit for v1.0. Every finding carries exactly one status:

- **fixed** — with the PR that closed it, or **fixed by design** with
  the one-line reason the design closes it.
- **bounded** — with the measured ceiling as a number, and the
  `operations.md` line that states it. A row without both is not
  bounded.
- **outside the production profile** — with the `support.md` line that
  excludes it.
- **open** — no status yet. An open finding is a v1.0 blocker; the
  maintainers cut a card for each one until it carries a status. Nothing
  in this document is merely "known".

Sections follow the production review of 2026-10-23 (Q4). The harness
vacuity table of the same day (C50) feeds section 8. Each finding names
its evidence point `file:line`, checked once against the commit this
document landed on.

## 1. Release surface

| Finding | Evidence | Status |
| --- | --- | --- |
| `scn mount --unstable-writable`: a planted parent symlink can redirect an overlay write outside the mount (final-name-only check). Ben's D24: the writable mount leaves the v1 release binary — the write path becomes a build feature, off in release | `crates/scn/src/vfs.rs:1469`, `docs/cli.md:57`, `changes/313.fixed.md` | open, closed by C54 (the write path out of the release builds) |
| Env aliases `SCION_DATA`, `SCION_TOKEN`, `SCION_POSTGRES_URL`, `SCION_POSTGRES_URL_FILE` exist beside their flags | `crates/scion-server/src/main.rs:29,33,43` | fixed by design (each alias is the documented spelling of a flag in `docs/cli.md`; the flag list is the release surface, and the URL env exists so secrets stay off argv) |
| Test hooks in release binaries: the store fault hooks and the derive test pool build only with `--features test-hooks` | `crates/scion-server/Cargo.toml:61`, `crates/scion-server/src/store.rs:920` | fixed by design (the feature is off in every release build; `release.yml` builds plain `--release`) |

## 2. Durability seams

| Finding | Evidence | Status |
| --- | --- | --- |
| The Postgres store identity lives in two durable places: the `store_uuid` row in the database and the `backend` marker file in the data directory. A crash between the two writes, or a lost marker, refuses the open loudly ("this database names the store ..., but the data directory carries no marker of it") | `crates/scion-server/src/backend.rs:108,143`, `crates/scion-server/src/pg.rs:870` | fixed by design (the refusal is loud and never silent; recovery is manual re-pairing) — the operations.md runbook line for re-pairing is open |
| On Postgres, a server restarted right after a crash can refuse with "another server holds this store" until Postgres ends the dead session's advisory lock | `crates/scion-server/src/pg.rs:1745`, `docs/operations.md:269` | open (C60: the runbook must say the supervisor retries the open with backoff until Postgres reaps the dead session, and the open question is whether `serve` should wait for the lock itself, bounded, instead of failing the start) |
| Migrations run one transaction each under a lock with a 5 s clock; an open that waits longer answers busy; a newer database is refused | `docs/operations.md` "The open" (the 5 s clock and the busy answer), `crates/scion-server/src/db.rs` | bounded — ceiling: 1 transaction per migration, the lock wait capped at 5 s; `operations.md` "The open migrates the database's schema ... under a database lock with the same 5 s clock" |
| Rename durability: the blob file fsyncs before the rename and the directory after it; the power-loss suite proves sync-before-reply | `crates/scion-server/src/store.rs:931,1004`, `crates/scion-server/tests/power.rs` | fixed on Linux (the native cell); the macOS gap (no tool can cut power or drop a barrier there) is outside the production profile — `support.md`'s platform matrix runs the power-loss and dm-log-writes cells on Linux only |
| Archive restore holds a whole repository's changesets in RAM while reading the archive | `crates/scion-server/src/archive.rs:833` | open |
| The import writes changesets, parents, repo rows, derived rows, history nodes and the git import in separate transactions; the order and the batching are proven by the statement-count suites | `crates/scion-server/tests/changeset_batch.rs`, `tests/derived_batch.rs`, `tests/history_batch.rs` | fixed (#545, #548, #551) |

## 3. Unbounded work

| Finding | Evidence | Status |
| --- | --- | --- |
| Protocol reads are length-capped before `read_to_end` | `crates/scion-proto/src/runtime/auth/mod.rs:352`, cap `MAX_CHECKED_FILE = 1 MiB` at `:278` | bounded — ceiling: 1 MiB per checked read, `cap + 1` then refused; `docs/protocol-v1.md` states the message size limits |
| One unary reply can aggregate a whole directory or changeset past the per-object memory budget (`whole_share` charges by encoded size, the aggregate is uncapped) | `crates/scion-server/src/service.rs:485,935` | open |
| Slow clients can pin large unary replies outside the memory accounting | `crates/scion-server/src/service.rs` (`into_server`) | open |
| `gc` prune reads and deletes in batches of 1,000 rows | `crates/scion-server/src/gc.rs` (`prune_rows`) | fixed (#529 batched the reads and the deletes; the guard is `tests/gc_prune_batch.rs`) |
| `scn commit` reads a regular file whole into RAM: the size limit is "whatever fits" | `crates/scn/src/cmd/commit.rs` | open |
| `scn` JSON output silently replaces non-UTF-8 bytes | `crates/scn/src/client.rs:326` | open |
| Import batches are time-based per transaction and count-based for blobs | `crates/scion-server/src/import.rs:52` (`TX_BUDGET = 1 s`), `store.rs` (`BlobBatch`) | bounded — ceiling: 1 s of work or one commit per import transaction; `docs/benchmarks.md` records the measured commit size and rate the batches were tuned at |

## 4. Database loops

| Finding | Evidence | Status |
| --- | --- | --- |
| The Postgres writer pings the database with an autocommit `SELECT 1` while holding the writer mutex | `crates/scion-server/src/pg.rs:905` | bounded — ceiling: 1 statement per writer-idle probe (not per call); the probe's role is stated in `operations.md`'s connection-cap paragraph (the server holds 9 connections) |
| A systematic sweep for SQL inside repository-sized loops, missing `ORDER BY` on paged reads, and isolation assumptions across the pool is not done | `crates/scion-server/src/pg.rs`, `read.rs` | open |
| A server asked for Postgres refuses rather than falling back to SQLite (a mismatched data directory is refused at the open) | `crates/scion-server/src/backend.rs:128,183` | fixed by design (refusal, not fallback) |

## 5. Failure paths

### 5a. The panic inventory (O7)

Every `unwrap`, `expect`, `panic!`, `unreachable!` and `todo!` in
non-test code of `scion-server`, `scn`, `scion-proto` and
`scion-tier` (test-tail modules, `tests/` and the fuzz targets
excluded). Verdicts:

- **SAFE** — an invariant the code proves on the line before (named).
- **REACHABLE** — a client request, a file on disk, the database, or
  the OS can reach it; each one is fixed in the panic-fix PR with a
  red/green test.

| Site | What reaches it | Verdict |
| --- | --- | --- |
| `scion-proto/src/ticket.rs:150,194,195,229` (`try_into().unwrap()`) | a ticket from the wire | SAFE — each `try_into` follows a `take(n)` that returns exactly `n` bytes or errors, so the conversion is uninhabitable |
| `scion-proto/src/ticket.rs:365` (`unreachable!`) | the parse loop's exit | SAFE — the loop returns at every entry (the comment names it) |
| `scion-proto/src/ticket.rs:388` (`trees.get(&id).unwrap()`) | a tree id just written to the same table | SAFE — the id is inserted two lines above |
| `scion-proto/src/runtime/listen.rs:69,73,168` (`NonZero::new().unwrap()`) | process start | SAFE — the constants are literal non-zero values |
| `scion-proto/src/runtime/listen.rs:559,633` (semaphore `.expect("never closed")`) | accept-loop teardown | SAFE — the semaphore is never closed while the listener lives; closing happens only in Drop |
| `scion-proto/src/runtime/reply.rs:176` (stall-slot lock `.expect("not poisoned")`) | a panicked stall holder | SAFE — it fails loud (a panic, not a silent continue): the poisoned slot cannot be repaired, and the process dies naming it |
| `scion-server/src/access.rs:295,308` (`unreachable!`) | the prefix-match loop | SAFE — the loop breaks on the first match, so the `_` arm is dead (the comment names the invariant) |
| `scion-server/src/archive.rs:651` (`to.parent().expect`) | a crafted archive manifest | SAFE — `to` is built from a `BlobId` (`archive/blobs/xx/yy`), never a bare name; the parent is always present |
| `scion-server/src/blame.rs:819` (`PathElem::new().expect`) | a repo path from the database | SAFE — `RepoPath::new` validated every part on the way in (no empty part, no `.`/`..`, no nul), and re-splitting a valid path yields exactly those parts |
| `scion-server/src/blame.rs:881` (`line_u32`) | a blamed file with 2^32 lines | SAFE — `MAX_OBJECT_BYTES` is 100 MiB (`store.rs:84`), so a stored file has far fewer than 2^32 one-byte lines |
| `scion-server/src/blame.rs:967` (16 of 32 hash bytes) | the hasher's output | SAFE — `blake3` always yields 32 bytes |
| `scion-server/src/blame.rs:1033,1111,1112` (cursor/table `u32::try_from`) | a cursor field or window table | SAFE — the fields are built from one path (≤ `MAX_PATH_BYTES`) and one window over a store far below 2^32 changesets |
| `scion-server/src/graph.rs:576` (`.expect("a queued changeset has a mark")`) | the walk's own queue | SAFE — the mark is inserted when the changeset is queued, two lines above |
| `scion-server/src/main.rs:794,858,975,1064,1392,1402` (clap-conflict `unreachable!`/`expect`) | the command line | SAFE — clap refuses the conflicting flags before these arms run |
| `scion-server/src/main.rs:1274` (`.expect("exactly one source is given")`) | the access-file flags | SAFE — clap's `conflicts_with` allows exactly one source |
| `scion-server/src/pg.rs:325` (`.expect("a taken reader has a client")`) | the reader pool's taken flag | SAFE — `take` sets the client before returning the guard, and the guard's Drop clears it |
| `scion-server/src/pg.rs:599` (`.expect("a raw job carries its closure")`) | the job queue | SAFE — the sender sets `raw` exactly for `Kind::Raw` jobs, checked one line above |
| `scion-server/src/pg.rs:621` (`.expect("spawn the postgres worker")`) | the OS: thread-spawn `EAGAIN` under a thread limit | REACHABLE — fixed in the panic-fix PR (the open answers the error instead of crashing) |
| `scion-server/src/pg.rs:645,658,666` (`unreachable!`) | a reply of the wrong shape | SAFE — each worker branch answers exactly the reply kind its job kind asked for |
| `scion-server/src/pg.rs:905` (`writer.lock().unwrap()`) | a poisoned writer lock | SAFE — see 5b: a panic mid-probe leaves nothing behind |
| `scion-server/src/service.rs:1295` (`walk.next.unwrap()`) | the log walk's state | SAFE — the `walk.next.is_none()` case returns 13 lines above |
| `scion-server/src/service.rs:1841` (`.expect("asserted to fit in u32")`) | a reply size | SAFE — the `assert!` above bounds it |
| `scion-server/src/tickets.rs:44` (`.expect("system randomness")`) | the OS entropy source | SAFE — `getrandom::fill` cannot fail on the supported platforms (Linux, macOS; `support.md`'s platform matrix); the doc comment states it |
| `scion-server/src/tickets.rs:103` (`.expect("a key is 32 bytes")`) | the 32-byte secret minted above | SAFE — `getrandom::fill(&mut [0u8; 32])` fills exactly 32 |
| `scion-server/src/views.rs:262` (`Tree::new(kept).expect`) | kept entries of a stored tree | SAFE — a subset of an already-valid tree re-validates (every name passed `PathElem` on the way in) |
| `scion-server/src/pg.rs`, `test_support/*`, the fuzz targets | — | outside the audit (test-only builds; `test-hooks` is off in release) |
| `scn/src/cmd/bench.rs:114,125`, `cmd/blame.rs:64`, `cmd/commit.rs:130,138` (`IdentityTime::new`/`Changeset::new` `.expect`) | argv and git config | SAFE — the fields come from `-m` argv (bounded by `ARG_MAX`) and git's config parser, both far below the format's `u32::MAX` field limit |
| `scn/src/cmd/mount.rs:589,590` (signal `.expect`) | the OS signal masks | SAFE — `signal()` on an already-listened signal fails only on a broken runtime; the platforms guarantee it |
| `scn/src/main.rs:208,216`, `scion-tier/src/bin/*:120,128,157,165,197` (clap-conflict `unreachable!`/`expect`) | the command line | SAFE — clap refuses the conflicting flags first |
| `scn/src/vfs.rs:351,607,658,779,1167,1229,1278` (path/kind `expect`/`unreachable`) | the mount's own node table | SAFE — the guards above each call check the same field; a file node always carries its overlay path (the `Node` invariant the mount maintains) |
| `scn/src/vfs.rs:713` (double `.unwrap()`) | an NFS setattr on a forged file id | SAFE — `get(&id)` returns None before it (answered `NFS3ERR_STALE`), and the kind check above returns for dirs, so the remaining node is a file with a path |
| `scn/src/vfs.rs:720` (`file_mode().try_into().unwrap()`) | the mode constant | SAFE — 0o644 fits both Linux `u32` and macOS `u16`; the `useless_conversion` allow names it |
| `scn/src/vfs.rs:811` (`.get(&id).unwrap()`) | a lookup one line after the same get | SAFE — the match above already answered the `None` case |
| `scn/src/vfs.rs:1320,1384,1392` (`take(4).try_into().expect`) | a truncated index file | SAFE — `take(n)` returns exactly `n` bytes or an error; four bytes always fit `u32` |
| `scn/src/vfs.rs:1510` (`.expect("cannot read /dev/urandom")`) | the OS entropy source | SAFE — `/dev/urandom` always opens on the supported platforms; the mount fails at startup, before it serves |
| `scion-core`, `scion-proto` ticket decode, and every `take`-then-`try_into` in the codecs | the wire | SAFE — the same `take` contract |

### 5b. The lock-poison inventory (O7)

Every `Mutex`/`RwLock` that guards store or derived state. Policy:
`unwrap_or_else(PoisonError::into_inner)` — a poisoned lock is used,
not propagated. The question per lock: what a holder that panics
leaves behind, and whether the next holder checks it.

| Lock | Guards | A panicking holder leaves | Verdict |
| --- | --- | --- | --- |
| `store.rs:131` `db: Mutex<Connection>` (the SQLite write connection) | every commit and unflushed read | an open transaction — `SqliteTx`'s Drop rolls it back on unwind, so the connection is usable and the store keeps every committed byte | SAFE — the invariant is stated at `store.rs:649-651` and holds at every panic point inside `commit` (the txn guard drops before any code sees the poison) |
| `store.rs:172` `writers: Mutex<usize>` + `write_woken` | the writer count for the handoff | a stale count | SAFE — the count is repaired by `WriteWaiter`'s Drop, which runs on unwind |
| `store.rs:2021` the flush-state lock | the flush bookkeeping | a stale marker | SAFE — the holder holds it only across counter reads and `fetch_max`es; no panic point inside leaves an inconsistent value (counters are monotonic) |
| `store.rs:2062,2114` `SPAWNS: RwLock` | the spawn-serialization guard | — | SAFE — it fails loud (`.expect("not poisoned")`), which is fail-closed |
| `derived.rs:178` `flights: Mutex<HashMap<FlightKey, Arc<Flight>>>` | which repo/kind walks are live | a live flight whose walk never retires: every later joiner waits for a walk counter that never moves — a caller with no deadline waits forever. Two exit paths leaked it: the panic unwind past `retire`, and the permit wait's early `?` on the owner's deadline | UNSAFE — fixed in the panic-fix PR: one drop guard (`RetireOnDrop`), created the moment the join makes the caller an owner, retires the flight on every exit; the panic still propagates. Tests: a panicking derive (the next caller owns a fresh walk) and a permit timeout (red before: the next caller hung to its DeadlineExceeded; green: it finishes the chain) |
| `derived.rs:180` `in_flight: Mutex<usize>` | the walk-permit count | a stale permit count | SAFE — `Permit`'s Drop runs on unwind and decrements |
| `derived.rs` `flight.walks` per-flight counter + `woken` | the walk-end signal for waiters | a counter that never advances | covered by the `flights` fix above: with `retire` on every owner exit the waiters wake and take over a fresh walk |
| `service.rs`/`listen.rs` semaphores and slot maps | connection/stream accounting | a leaked slot | SAFE — every holder holds it across `.await`-free counter ops; the panic points sit outside the critical sections, and the slot Drop runs on unwind |

The fix PR (#568) carries: the flight-leak fix — one drop guard over
both leak paths — with two tests (a panicking derive; a permit timeout
whose red run hung the next caller to its DeadlineExceeded), and the
worker-spawn fix with the error path (EAGAIN cannot be forced
in-process; stated in the PR).

| `todo!`/`unimplemented!` in production code: none | grep across the five crates | fixed (none exist) |
| Warn-and-continue: the import records and counts per-commit failures against the ledger; the two `eprintln!` skip lines are inside test-only helpers | `crates/scion-server/src/pg.rs:4437`, `gc.rs:1017` (both test helpers) | fixed by design (no production warn-and-continue on a correctness boundary) |

## 6. Defaults

| Finding | Evidence | Status |
| --- | --- | --- |
| The default data directory is `scion-data` relative to the server's working directory, and the server creates it | `crates/scion-server/src/main.rs:29` | bounded — ceiling: the created directory is the one flag-named (or cwd-relative default) path, 0700 by the process umask; `docs/cli.md` documents the flag and the default. The open question — create vs refuse — is tracked open |
| Secrets on argv: `--postgres-url` is deprecated and warns; `--postgres-url-file` and `SCION_POSTGRES_URL` are the supported forms | `crates/scion-server/src/main.rs:33-43` | fixed (the warning ships; the removal is on the deprecation clock) |
| Ticket and seed files are 0600; the access file refuses group/other writable and symlinks | `crates/scion-server/src/main.rs:1333,1433,1493` | bounded — ceiling: 0600 on every secret file, the access file refuses go-w and symlinks at the open; `operations.md` "The open checks" states the refusal |
| Listeners default to loopback | `crates/scion-server/src/main.rs:59` (`default_value = "127.0.0.1:7878"`) | bounded — ceiling: the default bind is 127.0.0.1:7878; `docs/operations.md`'s serve examples restate it |
| Production state in temp dirs: the server never writes store state outside `--data`; every temp-dir user is a harness script | `crates/scion-server/src/main.rs:29` (the only store path) | fixed by design (the data dir is the only durable state; `TMPDIR` appears in no serve path) |

## 7. Compatibility

| Finding | Evidence | Status |
| --- | --- | --- |
| The store format, the protocol and the archive format are versioned documents; the unknown-field policy is stated per format | `docs/format-v1.md`, `docs/protocol-v1.md`, `docs/archive-format.md`, `docs/compatibility.md` | fixed by design (the policy is the compatibility doc) — the additive contract goldens that enforce it are open |
| Migrations carry no rollback path; the evidence is forward-only: N-1 opens N's store, N refuses a newer database, and the failed-migration runbook restores from backup | `crates/scion-server/tests/schema_drift.rs`, `scripts/version-matrix.sh:10-13`, `docs/operations.md` "A failed migration" | fixed by design (forward-only, with the matrix and the runbook as the evidence) |
| Fixtures of the current build only: the version matrix runs the pinned N-1 binaries against the current store both directions | `scripts/version-matrix.sh` | fixed (#540-era legs; the matrix never passes as a skip) |
| Backups and the failed-migration runbook are tested against a live server, not hand-written fixtures | `crates/scion-server/tests/backup.rs`, `crash.rs:342` | fixed |

## 8. Evidence and CI

The harness vacuity audit of 2026-10-23 (C50) is the base table of this
section; its fixes landed as #561. Highlights, with the C50 verdicts:

| Finding | Evidence | Status |
| --- | --- | --- |
| The soaks and the smoke never named the store they served, and soak-fault never counted its kills | `scripts/soak-fault.sh`, `scripts/soak.sh`, `scripts/release-smoke.sh` | fixed (#561: the backend row, the kills count, the no-sample segment rule) |
| The wall-clock budget in the derived flight tests could lose to a thread start | `crates/scion-server/src/derived.rs` | fixed (#566: the tests expire the deadline on the test's word, after the state proves the order) |
| The batch equality guards run both sides through the same flush, so a shared flush bug passes row-by-row equality | `tests/changeset_batch_eq.rs` | bounded — ceiling: 0 shared-flush coverage in the eq witness; the catching suite for quiet wrong content is the git-referenced `tests/history.rs` (NodeId::of(body) == id); the blind spot is recorded in the C50 notes |
| Main-only jobs without dispatch proof: every harness named in the C50 table now carries a branch dispatch run id | `.github/workflows/soak.yml`, `ci.yml`, `version-matrix.yml` | fixed (dispatches 39891410810, 39923747077, 39926987173, 39949854816, 39923804727) |
| Env-required suites that skip silently | `tests/crash_pg.rs:586`, `power.rs:80`, `dm_log_writes.rs:33`, `disk_full.rs:32` | bounded — ceiling: 0 silent skips; each prints its skip line, and the CI job that owns it sets the REQUIRE variable so a missing build fails the job |
| A randomized land model as an independent oracle | the land model script (#492) | fixed |

## 9. Supply chain

| Finding | Evidence | Status |
| --- | --- | --- |
| The workspace and the three fuzz lockfiles are committed | `Cargo.lock`, `crates/*/fuzz/Cargo.lock` (4 files) | bounded — ceiling: exactly 4 lockfiles, all committed; the release smoke and the CI builds run `--locked` |
| Every third-party action is pinned to a commit SHA with the version in a comment | `.github/workflows/ci.yml:24` and every workflow | fixed (pinned at introduction; the scorecard workflow watches for drift) |
| Service containers carry version tags, not mutable `latest` | `ci.yml:316-317` (the matrix `postgres:16.15/17.11/18.6`), `ci.yml:538,629` and `bench.yml:71` (`postgres:17.11`) | bounded — ceiling: 3 version tags, no `latest`; a digest pin per tag is the upgrade, tracked open |
| Artifacts are built from the protected release tag only | `docs/RELEASING.md`, `docs/verify.md`, `docs/slsa.md` | fixed by design (the release workflow builds on the tag; the provenance doc states the SLSA level) |

## The open list (the v1.0 blockers this audit leaves)

1. The `--unstable-writable` write path out of the release binary —
   open, closed by C54 (§1).
2. The archive restore's repository-sized RAM hold (§2), and the C60
   Postgres lock-restart runbook + whether `serve` should wait (§2).
3. The unary reply caps: `whole_share` aggregates and slow-client
   pinning (§3), `scn commit` whole-file reads (§3), `lossy` JSON (§3).
4. The database-loop sweep: SQL in loops, `ORDER BY`, isolation (§4).
5. The panic fixes the inventory (§5a) leaves: the worker-spawn
   `EAGAIN` path, and the flight-leak fix's PR (§5b).
6. The data-dir default create-vs-refuse question (§6).
7. The additive contract goldens that enforce the format policies (§7).
8. Digest pins for the service-container tags (§9).
