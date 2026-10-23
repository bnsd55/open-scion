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

| Finding | Evidence | Status |
| --- | --- | --- |
| `unwrap`/`expect` outside test code: 19 unwraps and 45 expects across the five production crates (test-tail modules excluded). Most are `try_into` after a length check that makes them uninhabitable; each needs that argument written down or the call replaced | e.g. `crates/scion-proto/src/ticket.rs:150,194,195,229`, `crates/scn/src/vfs.rs` (13), `crates/scion-server/src/pg.rs:325,599,621` | open |
| Poisoned locks: every mutex recovers with `unwrap_or_else(PoisonError::into_inner)`. A panic while a thread holds the write connection hands the store to the next holder without any check that the panicked transaction left nothing behind | `crates/scion-server/src/derived.rs:257` and ~40 sites | open |
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
5. The `unwrap`/`expect` inventory with per-site arguments (§5), and the
   poisoned-lock write-integrity audit (§5).
6. The data-dir default create-vs-refuse question (§6).
7. The additive contract goldens that enforce the format policies (§7).
8. Digest pins for the service-container tags (§9).
