# The production audit

The blocking audit for v1.0. Every finding carries exactly one status:

- **fixed** — with the PR that closed it.
- **bounded** — with the measured ceiling, and where `operations.md`
  states it.
- **outside the production profile** — with the `support.md` line that
  excludes it.
- **open** — no status yet. An open finding is a v1.0 blocker; the
  maintainers cut a card for each one until it carries a status. Nothing
  in this document is merely "known".

Sections follow the production review of 2026-10-23 (Q4). The harness
vacuity table of the same day (C50) feeds section 8. Each finding names
its evidence point `file:line` at the commit this document landed on.

## 1. Release surface

| Finding | Evidence | Status |
| --- | --- | --- |
| `scn mount --unstable-writable`: a planted parent symlink can redirect an overlay write outside the mount (final-name-only check). The fence exists — the mount is read-only by default and the writes live behind the unstable flag alone (#313) — but v1.0 must either walk every parent handle or drop the write path from the v1 binary | `crates/scn/src/vfs.rs:1469`, `docs/cli.md:57`, `changes/313.fixed.md` | open |
| Hidden env vars in the release binaries: `SCION_DATA`, `SCION_TOKEN`, `SCION_POSTGRES_URL`, `SCION_POSTGRES_URL_FILE` | `crates/scion-server/src/main.rs:29,33,43` | bounded (each is a documented flag alias; `docs/cli.md` names the data dir and the token env; the URL env exists to keep secrets off argv) |
| Test hooks in release binaries: the fault hooks on the SQLite store and the derive pool build only with `--features test-hooks`, which release builds do not enable | `crates/scion-server/Cargo.toml:61,64`, `crates/scion-server/src/store.rs:920` | fixed (feature-gated; `release.yml` builds without the feature) |
| `scion-server export` prints its error to stderr and exits 0-path unchanged | `crates/scion-server/src/main.rs:337` | open |

## 2. Durability seams

| Finding | Evidence | Status |
| --- | --- | --- |
| The Postgres store identity lives in two durable places: the `store_uuid` row in the database and the `backend` marker file in the data directory. A crash between the two writes, or a lost marker, refuses the open loudly ("this database names the store ..., but the data directory carries no marker of it") | `crates/scion-server/src/backend.rs:108,143`, `crates/scion-server/src/pg.rs:870` | bounded (the refusal is loud, never silent; recovery is the documented manual pairing; `operations.md` section "The open" — a runbook line for re-pairing is open) |
| On Postgres, a server restarted right after a crash can refuse with "another server holds this store" until Postgres ends the dead session's advisory lock | `crates/scion-server/src/pg.rs:1745`, `docs/operations.md:269` | open (C60: say what a supervisor must do — retry the open with backoff until Postgres reaps the dead session — and decide whether `serve` should wait for the lock itself, bounded, instead of failing the start) |
| Migrations run one transaction each under a lock with a 5 s clock; a newer database is refused | `docs/operations.md` "The open", `crates/scion-server/src/db.rs` | bounded (per-migration transactions + the busy answer; the rollback evidence is the schema-drift suite `crates/scion-server/tests/schema_drift.rs` and the version-matrix legs) |
| Rename durability: the blob file fsyncs before the rename and the directory after it | `crates/scion-server/src/store.rs:931,1004` | bounded (the power-loss suite `tests/power.rs` proves sync-before-reply; the directory-fsync gap on macOS is documented in the suite header and is `support.md`'s Linux-native cell) |
| Archive restore holds a whole repository's changesets in RAM while reading the archive | `crates/scion-server/src/archive.rs:833` | open |
| The import writes changesets, parents, repo rows, derived rows, history nodes and the git import in separate transactions; the order and the batching are proven by the statement-count suites | `crates/scion-server/tests/changeset_batch.rs`, `tests/derived_batch.rs`, `tests/history_batch.rs` | fixed (#545, #548, #551) |

## 3. Unbounded work

| Finding | Evidence | Status |
| --- | --- | --- |
| Protocol reads are length-capped before `read_to_end` (`take(cap + 1)` and a refused overflow) | `crates/scion-proto/src/runtime/auth/mod.rs:352` | bounded (the caps are the protocol limits in `docs/protocol-v1.md`) |
| `service.rs` whole-object reply: one large directory or changeset bypasses the memory budget (`whole_share` charges by encoded size, but the aggregate of a unary reply is not capped) | `crates/scion-server/src/service.rs:485,935` | open |
| Slow clients can pin large unary replies outside the accounting | `crates/scion-server/src/service.rs` (`into_server`) | open |
| `gc` prune reads and deletes in batches of 1,000 rows | `crates/scion-server/src/gc.rs` (`prune_rows`) | fixed (#529 batched the reads and the deletes; the guard is `tests/gc_prune_batch.rs`) |
| `scn commit` reads a regular file whole into RAM: the size limit is "whatever fits" | `crates/scn/src/cmd/commit.rs` | open |
| `scn` JSON output silently replaces non-UTF-8 bytes (`lossy`) | `crates/scn/src/client.rs` | open |
| Import transaction and blob batches are count-based (`TX_BUDGET`, `BlobBatch::keep`) | `crates/scion-server/src/import.rs` | bounded (the ceilings are the measured commit sizes in `docs/benchmarks.md` and the capacity table in `docs/operations.md`) |

## 4. Database loops

| Finding | Evidence | Status |
| --- | --- | --- |
| The Postgres worker pings the database with an autocommit `SELECT 1` while holding the writer mutex | `crates/scion-server/src/pg.rs:905` | bounded (one statement per keepalive tick; the tick is the writer's idle probe, not per call) |
| A systematic sweep for SQL inside repository-sized loops, missing `ORDER BY` on paged reads, and isolation assumptions across the pool is not done | `crates/scion-server/src/pg.rs`, `read.rs` | open |
| SQLite is the only fallback: a server asked for Postgres refuses rather than falling back (the open refuses a mismatched data directory, and `serve` never silently switches) | `crates/scion-server/src/backend.rs:128,183` | fixed (refusal, not fallback) |

## 5. Failure paths

| Finding | Evidence | Status |
| --- | --- | --- |
| `unwrap`/`expect` outside test code: 19 unwraps and 45 expects across the five production crates (test-tail modules excluded). Most are `try_into` after a length check that makes them uninhabitable; each needs that argument written down or the call replaced | e.g. `crates/scion-proto/src/ticket.rs:150,194,195,229`, `crates/scn/src/vfs.rs` (13), `crates/scion-server/src/pg.rs:325,599,621` | open |
| Poisoned locks: every mutex recovers with `unwrap_or_else(PoisonError::into_inner)`. A panic while a thread holds the write connection leaves the store to the next holder without any check that the panicked transaction left nothing behind | `crates/scion-server/src/derived.rs:1212` and ~40 sites | open |
| `todo!`/`unimplemented!` in production code: none | grep across the five crates | fixed (none exist) |
| Warn-and-continue on correctness boundaries: the import's per-commit failures are recorded and counted, not swallowed; `eprintln!` skips are sqlite/postgres capability notes in test-only paths | `crates/scion-server/src/pg.rs:4437`, `gc.rs:1017` | bounded (both sites are test-only skip notes) |

## 6. Defaults

| Finding | Evidence | Status |
| --- | --- | --- |
| The default data directory is `scion-data` relative to the server's working directory | `crates/scion-server/src/main.rs:29` | bounded (documented in `docs/cli.md`; every shipped unit and the smoke scripts pass `--data` explicitly) — the open question is whether the default should refuse instead of create; tracked open |
| Secrets on argv: `--postgres-url` is deprecated and warns; `--postgres-url-file` and `SCION_POSTGRES_URL` are the supported forms | `crates/scion-server/src/main.rs:33-43` | fixed (the warning ships; the removal is on the deprecation clock) |
| Ticket and seed files are 0600; the access file refuses group/other writable and symlinks | `crates/scion-server/src/main.rs:1333,1433,1493` | bounded (documented in `operations.md`; the release smoke runs both umask stories 022 and 002) |
| Listeners default to loopback | `docs/operations.md` (the serve examples) | bounded |
| Production state in temp dirs: none — every script that uses a temp dir is a harness, and the server refuses a data dir under the system temp when asked to serve it? | — | open (verify and state the rule) |

## 7. Compatibility

| Finding | Evidence | Status |
| --- | --- | --- |
| The store format, the protocol and the archive format are versioned documents; unknown-field policy is stated per format | `docs/format-v1.md`, `docs/protocol-v1.md`, `docs/archive-format.md`, `docs/compatibility.md` | bounded (the compatibility doc is the policy) |
| Migrations carry no rollback path; the evidence is the forward-only design plus the schema-drift and version-matrix suites | `tests/schema_drift.rs`, `scripts/version-matrix.sh` | bounded (N-1 opens N's store and N refuses a newer database; rollback is restore-from-backup, documented in `operations.md` section "A failed migration") |
| Fixtures of the current build only: the version matrix runs the pinned N-1 binaries against the current store both directions | `scripts/version-matrix.sh:10-13` | fixed |
| Backups and the failed-migration runbook are tested against a server, not fixtures | `crates/scion-server/tests/backup.rs`, `crash.rs:342` | fixed |

## 8. Evidence and CI

The harness vacuity audit of 2026-10-23 (C50) is the base table of this
section; its fixes landed as #561. Highlights, with the C50 verdicts:

| Finding | Evidence | Status |
| --- | --- | --- |
| The soaks and the smoke never named the store they served, and soak-fault never counted its kills | `scripts/soak-fault.sh`, `scripts/soak.sh`, `scripts/release-smoke.sh` | fixed (#561: the backend row, the kills count, the no-sample segment rule) |
| The wall-clock budget in the derived flight tests could lose to a thread start | `crates/scion-server/src/derived.rs` | fixed (#566: the tests expire the deadline on the test's word, after the state proves the order) |
| The batch equality guards run both sides through the same flush, so a shared flush bug passes row-by-row equality | `tests/changeset_batch_eq.rs` | bounded (the guard is documented; the git-referenced content tests `tests/history.rs` are the catching suite for quiet wrong content) |
| Main-only jobs without dispatch proof: every harness named in the C50 table now carries a branch dispatch run id | `.github/workflows/soak.yml`, `ci.yml`, `version-matrix.yml` | fixed (dispatches 39891410810, 39923747077, 39926987173, 39949854816, 39923804727) |
| Env-required suites that skip silently | `tests/crash_pg.rs:585`, `power.rs:80`, `dm_log_writes.rs:33`, `disk_full.rs:32` | bounded (each prints its skip line and the CI job that owns it sets the REQUIRE variable, so a missing build fails the job, not a silent skip) |
| A randomized land model as an independent oracle | `tests/land model script` (`scripts/`), #492 | fixed |

## 9. Supply chain

| Finding | Evidence | Status |
| --- | --- | --- |
| The workspace and the three fuzz lockfiles are committed | `Cargo.lock`, `crates/*/fuzz/Cargo.lock` | bounded (all four are committed and `--locked` builds are enforced by the release smoke and the CI builds) |
| Every third-party action is pinned to a commit SHA with the version in a comment | `.github/workflows/ci.yml:24` and every workflow | fixed (pinned at introduction; the scorecard workflow watches for drift) |
| Service containers carry version tags, not mutable `latest` | `ci.yml:315-317` (`postgres:16.15/17.11/18.6`), `bench.yml:71` | bounded (the matrix versions are minor-pinned; the single-version jobs pin 17.11 — a digest pin is the upgrade, tracked open) |
| Artifacts are built from the protected release tag only | `docs/RELEASING.md`, `docs/verify.md`, `docs/slsa.md` | bounded (the release workflow builds on the tag; the provenance doc states the SLSA level) |

## The open list (the v1.0 blockers this audit leaves)

1. The `--unstable-writable` overlay symlink write escape (§1).
2. `scion-server export` exit path (§1).
3. The archive restore's repository-sized RAM hold (§2), and the C60
   Postgres lock-restart runbook + whether `serve` should wait (§2).
4. The unary reply caps: `whole_share` aggregates and slow-client pinning
   (§3), `scn commit` whole-file reads (§3), `lossy` JSON (§3).
5. The database-loop sweep: SQL in loops, `ORDER BY`, isolation (§4).
6. The `unwrap`/`expect` inventory with per-site arguments (§5), and the
   poisoned-lock write-integrity audit (§5).
7. Whether the default data directory should refuse instead of create,
   and the temp-dir rule (§6).
8. Digest pins for the single-version service containers (§9).
