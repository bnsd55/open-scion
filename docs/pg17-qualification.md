# Postgres 17 qualification gap list (D23)

D23 (`docs/ROADMAP.md` v0.6) promotes Postgres 17 to production when
four things hold: (a) the contract test passes on Postgres 17, (b) the
real Postgres fault rows pass, (c) backup and restore pass on Postgres
17, and (d) the release-candidate trial runs on Postgres 17 as well as
on SQLite. Today every Postgres cell is **experimental**
(`docs/support.md`).

This document is the gap list: one row per requirement and per fault
row of `docs/faults.md`, what proves it on Postgres 17 today, and the
verdict:

- **EVIDENCE** — the test or drill really runs against Postgres 17 in
  CI, asserts the backend it names, and can fail (a red is visible).
- **WEAK** — it runs but could pass vacuously, or runs only when a
  human arms it; the reason is stated.
- **MISSING** — nothing runs it on Postgres 17.

Every row was checked against the code and the workflows on
2026-10-23, at the merge of #571. The last fully green `main` CI run
that proves the EVIDENCE rows is
[39985773698](https://github.com/bnsd55/openscion/actions/runs/39985773698)
(#577): `test (postgres 17.11)`, `power-loss` and `mount (NFS)` all
green. Where a claim could not be verified it says so.

The Postgres 17 evidence rides on one job family:

- `test (postgres ${{ matrix.version }})` (`.github/workflows/ci.yml`,
  job `test-postgres`, line 327): the whole `open-scion-server` suite
  with `SCION_TEST_BACKEND=postgres` (line 368), then the real-fault
  steps — tmpfs disk full (line 385), block-level power cuts
  (line 391), and the takeover (the database stops and restarts,
  line 419). Postgres 17.11 runs per push when the change touches
  Postgres or metadata code, and always on `main`, the nightly and a
  dispatch; 16.15 and 18.6 are nightly and dispatch only.
- `power-loss` (`ci.yml:563`): the
  LazyFS suite runs SQLite and Postgres (line 631). Main, nightly and
  dispatch only — never on a pull request.
- `mount (NFS)` (`ci.yml:654-655`): the flow runs twice, SQLite and
  Postgres (`ci.yml:694`). Also main, nightly and dispatch only.

## 1. The four D23 requirements

| Requirement | What it says | Evidence today | Verdict |
| --- | --- | --- | --- |
| (a) the contract test on Postgres 17 | The end-to-end contract flow (import, serve, land, read, export) answers the same on Postgres 17 | `crates/scion-server/tests/contract.rs` branches on the backend (`is_pg`, `contract.rs:277`; the git side gets `SCION_POSTGRES_URL` at `contract.rs:352`) and runs inside `test (postgres 17.11)`'s whole-suite step (`ci.yml:368`) on every qualifying push, `main`, nightly and dispatch. The contract goldens of #571 (`crates/scion-server/tests/archive_golden.rs`, `audit_golden.rs`, the proto wire goldens) ride the same suite | EVIDENCE |
| (b) the real Postgres fault rows | The fault matrix's Postgres rows are proven by tests that run on Postgres and can fail | See section 2 | EVIDENCE with three WEAK rows (F-race, soak-fault PG arm, drills) |
| (c) backup and restore on Postgres 17 | A Postgres-backed store backs up and restores whole | `crates/scion-server/tests/backup.rs:42` `a_backup_taken_while_serving_restores_whole` — a live server, a backup taken mid-write (`pg_dump` through `SCION_TEST_PG_CONTAINER`, `backup.rs:403`), the restore into a fresh store, then the equality check. Runs in `test (postgres 17.11)` per push. The operations runbook is `docs/operations.md` section 3 (`pg_dump` the database, restore the data directory, reopen) | EVIDENCE for the test; the runbook has never been walked on a real Postgres 17 install — that is requirement (d)'s trial, not a CI gap |
| (d) the release-candidate trial on Postgres 17 | One RC serves real work for 30 days per `docs/production-trial.md`, with the trial file named before day 1 | `docs/production-trial.md` is the template only; `docs/release-notes/` holds no trial file; v0.6 has no RC yet. The drill templates (`docs/drills/operator-drill.md`, `release-drill.md`, `vulnerability-drill.md`) name no Postgres arm | MISSING — by schedule, not by neglect: the trial runs when the RC is cut. The card is in section 4 |

## 2. The fault rows, one per `docs/faults.md` row

The matrix's two-half rule holds throughout: on Postgres the data
directory holds the marker and the blob files, the metadata lives in
the database's own WAL, and each fault row cuts the half it names.

| Fault row (faults.md) | Postgres 17 evidence | Verdict |
| --- | --- | --- |
| Wrong data directory for the database (UUID refusal) | `backend_refusals.rs` in the whole-suite step on Postgres; the takeover step (`ci.yml:419`) runs its `--ignored` tests | EVIDENCE |
| SQLite open of a Postgres-backed directory | Same | EVIDENCE |
| Another server took the store (fence) | `backend_refusals.rs::a_lost_server_lock_fences_the_backend`, Postgres-only by design | EVIDENCE |
| Database unreachable (not a takeover) | `backend_refusals.rs::an_unreachable_database_is_not_a_takeover` | EVIDENCE |
| Another writer holds the write lock | `backend_refusals.rs::a_write_blocked_by_another_writer_answers_busy_in_seconds`, both backends in one run | EVIDENCE |
| Server killed mid-write (SIGKILL) | `crates/scion-server/tests/crash_pg.rs` — the real binary over Postgres, SIGKILL during a land (`crash_pg.rs:594`) and during a schema upgrade (`crash_pg.rs:734`) | EVIDENCE — but the faults.md row is stale: it names only `crash.rs` (SQLite) and the `backend_refusals.rs` rows, not `crash_pg.rs`. Doc fix, card 1 |
| Kill during a schema upgrade | `crash_pg.rs:734` (the row above repeats here because faults.md splits them) | EVIDENCE (same stale-row note) |
| Tier daemon killed mid-fill | No Postgres row by design: the tier keeps its own object cache and takes none of the server's metadata | not a gap (documented exclusion) |
| Blob write hits a full disk (ENOSPC) | `storage.rs` runs on Postgres in the whole-suite step; the blob half is the data directory either way | EVIDENCE |
| SQLite metadata disk full | SQLite-only by definition; the Postgres analogue is the 53100 row below | not a gap |
| A really full Postgres database (53100) | `disk_full.rs::a_full_postgres_database_answers_out_of_space_and_needs_no_repair` in the tmpfs step on Postgres (`ci.yml:385`), per push on the pg tier | EVIDENCE — but the faults.md row names "the dispatch-only job `test (postgres disk full)`", which no longer exists under that name; the step moved into `test-postgres`. Doc fix, card 1 |
| One file write of a batch fails | `storage.rs::a_batch_that_loses_a_file_keeps_its_earlier_files`, backend-independent blob half | EVIDENCE |
| Blob file deleted under the server | `backend_refusals.rs::a_fsck_of_a_postgres_backed_store_finds_missing_blob_files` | EVIDENCE |
| NFS mount | `mount (NFS)` job, Postgres step (`ci.yml:694`); the script proves the marker names Postgres before the flow | EVIDENCE |
| Power cut while clients write | `power-loss` job, Postgres step (`ci.yml:631`); the two race rows stay SQLite-only (their write park is a SQLite hook) | EVIDENCE for the reply rows |
| Racing puts of one blob + power cut | The power job's Postgres step skips these rows: the race is made certain by a SQLite-only park hook. The stripe-lock serialization itself (`store.rs`) runs everywhere. No Postgres row proves the cut under racing puts | WEAK — the fault is real on Postgres (the blob path is the same file tree); only the test's certainty hook is SQLite-shaped. Card 3 |
| Block-level replay (dm-log-writes) | The Postgres job's dm step (`ci.yml:391`), per push on the pg tier | EVIDENCE |
| EIO / EROFS failing disk | `storage.rs` rows on Postgres in the whole-suite step; the metadata half rides the database's WAL | EVIDENCE |
| Whole server on a real full disk (tmpfs) | `disk_full.rs` on Postgres in the tmpfs step (blob path and inodes) | EVIDENCE |
| SIGTERM drain under load | `health.rs` runs on Postgres in the whole-suite step | EVIDENCE |
| Soak-fault: kill -9 the server under load, on Postgres | `scripts/soak-fault.sh` has a full Postgres arm (`soak-fault.sh:25,89-100` and the `pg_restart` fault at `soak-fault.sh:338-344`) — but no CI workflow sets `SCION_FAULT_PG`, `SCION_PG_URL` or `SCION_FAULT_PG_CMD` (`.github/workflows/soak.yml` sets none of them). The arm exists; nothing arms it | WEAK — the script supports it, CI never runs it. Card 2 |
| The C60 restart-refusal row (a server restarting right after a crash refuses until Postgres reaps the dead session's lock) | Open in `docs/production-audit.md` section 2; the runbook must say the supervisor retries the open with backoff | open row of the audit, not a CI gap — feeds the trial's runbook walk |
| Operator / release / vulnerability drills | `docs/drills/*.md` name no Postgres arm | MISSING as drills — covered by (d)'s trial if the trial runs the drills on Postgres 17. Card 4 |

## 3. The WEAK and MISSING rows, and what closes them

| Gap | Smallest closer | Cost |
| --- | --- | --- |
| faults.md staleness (two rows name evidence that moved: `crash_pg.rs` and the tmpfs step's real home) | Edit the two rows of `docs/faults.md` to name `crates/scion-server/tests/crash_pg.rs:594,734` and the `test-postgres` tmpfs step (`ci.yml:385`) | Doc-only; minutes; no new CI |
| The soak-fault Postgres arm never runs | Add `SCION_FAULT_PG: "1"`, a `SCION_PG_URL` for a job-side Postgres container and `SCION_FAULT_PG_CMD: docker restart …` to a dispatch-only soak-fault job in `.github/workflows/soak.yml` (dispatch-only keeps D10's hosting question open) | One new job; ~10 minutes per dispatch run; the script and its checks already exist |
| The racing-puts power row skips on Postgres | Either a Postgres-shaped park hook behind `test-hooks` (an advisory-lock or statement-delay park in the blob write path) plus the row in the power job's Postgres step — or, if the park is judged too invasive, an explicit exclusion row in `docs/support.md` saying the racing-puts cut row is SQLite-proven only | Hook + row: a day of work; exclusion row: minutes. The power job's Postgres step already runs, so the run cost is zero either way |
| The drills have no Postgres arm | The trial (below) runs the three drill templates against the Postgres 17 RC install and records the outputs in the trial file | Part of (d); no CI change |
| The RC trial on Postgres 17 (d) | Cut the RC, name `docs/release-notes/production-trial-<version>-pg17.md` before day 1 per the template, run 30 days with the drill walk-throughs | 30 days wall clock plus one host; not CI-automatable; starts when v0.6's RC is cut |

## 4. The cards, cheapest first

1. **faults.md row repair** (doc-only): the SIGKILL rows name
   `crash_pg.rs`; the 53100 row names the `test-postgres` tmpfs step,
   not the retired `test (postgres disk full)` job.
2. **Arm the Postgres soak-fault arm** in a dispatch-only soak job:
   the script is ready (`soak-fault.sh:338-344`), the workflow never
   sets its three variables.
3. **The racing-puts power row on Postgres**: a PG-shaped park hook
   and the row in the existing Postgres power step, or the explicit
   `support.md` exclusion. Decide before the RC, so (b) is closed by
   evidence or by a stated exclusion, never by silence.
4. **The RC trial (d)**: schedule with the v0.6 RC; run the drills on
   Postgres 17 as part of it; the trial file lands in
   `docs/release-notes/`.

The C60 runbook row (§2 of the production audit) and the #575 restore
finding stay where they are; neither is a Postgres 17 qualification
gap beyond what the trial's runbook walk already covers.
