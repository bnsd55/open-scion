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

- The supply-chain job now runs cargo-deny in every fuzz
    sub-workspace, each of which holds its own Cargo.lock the root check
    never read — the gap that let RUSTSEC-2026-0382 sit in three fuzz
    lockfiles while the root stayed clean. Each fuzz workspace carries
    its own `deny.toml`: the same advisories floor, the license list the
    harness alone needs, and looser bans where a fuzz workspace differs
    from the product by design. A new script,
    `scripts/check-deny-coverage.sh`, fails the job when a Cargo.lock
    sits where no deny step reads it, so a new sub-workspace cannot slip
    past either.
- The land model check becomes a randomized script: a proptest strategy
  generates a sequence of operations against the real `scion-server` —
  lands from four callers on two refs, retries with the same request key,
  twins of one key in flight together, calls dropped in flight, a SIGKILL
  (plain, or at a pending land), the collector beside the live server,
  and bursts of up to eight lands on one ref at once — and a plain
  in-memory model checks the result: each ref's first-parent history is
  one chain, every acknowledged land is in it exactly once, no land is
  applied twice, no key's state regresses, a kill kept every land that
  had answered, and `fsck` sees no damage. On a failure, proptest shrinks
  the script to the smallest one that still fails. A checked-in seed
  corpus of seven scripts runs first and reads like sentences; CI runs
  one random case per pull request, and the nightly `land-model` job
  raises it to eight.
  
  A conflict reply is judged against the head the server saw when it
  decided, which each call records beside its reply (a batch's reply
  records the head after the batch — an upper bound, so a pass there is
  weaker, never a false accusation) — a bogus conflict whose path a
  later land moved cannot slip through. A CONFLICT with no paths is
  allowed when the detail says why, as the protocol provides.
- The scripts own what they start: common.sh's `start` runs every
    server in a process group of its own, records the pid, the group and
    the command line in the run's work directory, and the exit traps stop
    the group — TERM, then KILL — so a script that dies by error,
    interrupt or timeout leaves no server behind it. `stop` kills only
    after the job is still the one the pidfile recorded. The background
    client loops in soak.sh, soak-fault.sh and capacity.sh are recorded
    the same way and stopped through it; version-matrix.sh's own
    start/stop gains the group and the same check. The group, not the
    program name, is what the stop checks: a job started through a
    wrapper that execed another program is still ours, and a pid the
    system gave away since is skipped with a line saying so. A CI test
    starts a stand-in server, dies by error, proves the process is gone,
    and proves a wrapped job is stopped too (`scripts/test-owned-run.sh`).
- # gc writes the audit's first admin lines
  
  The collector subcommand records one `admin`/`intent` line before its
  run and one result after it: `admin`/`ok` with the report's counts, or
  `admin`/`failed` with the storage class of the error. `--audit-file`
  puts the CLI's lines in the same file the server appends to, so one
  audit log holds the whole history of a data directory. The golden pins
  the new `admin`/`intent` kind and the `admin`/`ok` line's fields; a
  source run that fails after its intent is declared undriven for now,
  with its reason in the golden's census.
- # import and restore write the audit's next admin lines
  
  The import subcommand records one `admin`/`intent` line before its run
  and one `admin`/`ok` after it, with the import's counts: the commits it
  saw, the ones it stored, the ones it verified, and whether it moved the
  target ref. The restore subcommand records the same pair around its
  replay, in the restored store's own `audit.log`, with the counts the
  restore reports: repos, refs, and pending lands. Both take
  `--audit-file`, with the same default as `gc`. The golden's
  `admin`/`intent` and `admin`/`ok` sections grow the fields of the new
  lines. The fix that made this possible: `verify` and `restore` sat
  behind an `unreachable!` in the store-open match since the archive
  landed, so both subcommands panicked instead of running; they now run
  before the store opens.
- # The admin lines cover every state-changing command
  
  The last admin actions join the audit. The `derive` subcommand records
  one `admin`/`intent` line before its backfill and one `admin`/`ok` after
  it, with the kind and the rows the run wrote and removed; a failed run
  records `admin`/`failed` with the class. Making the counts real,
  `backfill` returns a `DeriveReport` (the kind's name, the rows it wrote,
  the rows of older versions it removed) instead of a bare name.
  
  An open that migrates a store's schema records one
  `admin`/`ok` line `{what: migrate, from, to}` as soon as the audit can
  open, before the command's own pair: the migration ran inside the
  store's open, before the audit could exist, so no intent can precede
  it. A fresh open creates the schema and reports no migration. Both
  engines report their move: a Postgres open runs its own migrations at
  open, and its runner reports the versions the same way SQLite's does.
  
  The `ticket-key` subcommand records one intent before the key exists
  and one ok after it is on the disk. The lines carry the what and
  nothing else: never the seed, the key bytes, or the key file's path,
  in any encoding the test searches for.
  
  The golden's `admin`/`intent` and `admin`/`ok` sections grow the new
  fields; no new (event, outcome) kind appears. With this slice every
  state-changing CLI command writes its admin lines, and #385's admin
  item is complete: gc, import, restore, derive, the schema migration,
  and ticket-key generation all record what they changed.
- The server records an audit log: an append-only JSON-lines file of
  authentication, writes, ref changes, lands and administrative
  changes, with no tokens and no contents.
  
  The file sits in the data directory (mode 0600, `--audit-file`
  relocates it, no disable flag) and one writer thread owns it: callers
  hand lines to a bounded channel, the writer drains the queue, writes
  and fsyncs once per drain, and wakes each caller when its own lines
  are durable. A state change records two lines: an `intent` line
  fsynced before the change, and a `result` line after it, joined by
  request ID; a read records one. Every intent gets its result: a call
  that dies before its job starts is answered `cancelled` by the call's
  drop, and a land the stale-land sweep rejects is answered `stale` by
  the sweep. One case can leave a result missing: a job that dies after
  it claimed the call but before it recorded anything — the server log
  carries what the audit line cannot. If the audit append fails, the
  change is refused and readiness reports not-ready. Refused
  authentication is aggregated per peer per interval into one counted
  line, capped.
  
  The write, land and ref events carry what an operator needs and no
  contents: the RPC, the repo, the ref, the byte count of an upload,
  the land's state and ID, and a ref move's old and new changeset IDs.
  A changeset ID names content the caller already had; a blob or tree
  digest would name contents, so none appears.
  
  On open, a file whose last line has no newline gets one, so a line
  that a full drive cut in the middle stays a fragment of its own and
  cannot swallow the lines that follow.
  
  A land's ref line names the head the move replaced, read where the
  move happens, under the ref's lock, and is written only by the call
  that made the move. A replayed land (the same request key after it
  landed) moved nothing, so its land line names no old and no ref line
  appears for it; an auditor finds the head the move replaced in the
  land line of the call that made the move, under the same land_id.
  The ref lines of a ref carry the ref's true history: each old is the
  head its move's CAS replaced, so linking old to new rebuilds the
  history. The lines of concurrent changes can appear in a different
  order than the changes themselves (each job records its result after
  its change), so the history is linked, not read in line order.
  
  Blob and tree digests are never logged (a digest is a dictionary key
  to contents), and neither are token bytes or token hashes. Rotation
  is by size, keeps a bounded number of old files, and names what it
  dropped in an admin line.
  
  Not covered yet: the ref moves that happen outside the server.
  Import moves a ref to each imported tip (import.rs), and archive
  restore writes refs back (restore_ref); they run as their own
  processes and cannot hand lines to the server's writer, so they must
  append their own lines to the same audit file, intent before the
  move. Until they do, a reader of the log must not take it as
  complete: a ref can move without a line.
- # The metric surface's golden
  
  The `/metrics` registry joins the machine-checked surfaces: a
  checked-in golden (`crates/scion-proto/tests/metrics_v1.golden`) pins
  every `scion_*` name the registry renders, its `# TYPE`, and its
  labels and label values - names, types and labels are the surface,
  the sample values are this run's counts and pin nothing. The test
  exercises the registry end to end first (one request per RPC path,
  one cancelled, one failed, every gauge and histogram), because
  histograms render only once they counted. The stalled gauge hides
  until a reply stalls, so its name, type and label set pin in a unit
  test beside the render. A diff in the golden is a surface change: a
  removed name or a changed type is a break, never a re-pin; re-pin an
  added name on purpose with `SCION_UPDATE_METRICS_GOLDEN=1`.
- The fault matrix runs its disk, power and mount rows on Postgres beside
  SQLite: the `mount (NFS)`, `power-loss` and `test (postgres)` jobs each
  run the Postgres arm of their suite, and `docs/faults.md` says which half
  of each fault every run proves on Postgres (the blob files in the data
  directory; the metadata in Postgres's own WAL). The tier row is marked
  N/A: the tier keeps its own object cache and takes no metadata backend.
  
  The busy row now runs on both backends in one test: Postgres blocks
  through an open transaction of a second connection, SQLite through
  `BEGIN IMMEDIATE` on the same file, and both answer busy inside the
  same four-to-twenty-second window.
  
  On Postgres, a database that answers SQLSTATE `53100` (out of space)
  maps to `Storage::disk_full` and the fixed `ResourceExhausted` reply, a
  unit test of the mapping in `pg.rs` pins the SQLSTATE, the class and the
  reply text; the stale comments that pointed at error.rs say the same.
- # gc writes the audit's first admin lines
  
  The collector subcommand records one `admin`/`intent` line before its
  run and one result after it: `admin`/`ok` with the report's counts, or
  `admin`/`failed` with the storage class of the error. `--audit-file`
  puts the CLI's lines in the same file the server appends to, so one
  audit log holds the whole history of a data directory. The golden pins
  the new `admin`/`intent` kind and the `admin`/`ok` line's fields; a
  source run that fails after its intent is declared undriven for now,
  with its reason in the golden's census.
- # import and restore write the audit's next admin lines
  
  The import subcommand records one `admin`/`intent` line before its run
  and one `admin`/`ok` after it, with the import's counts: the commits it
  saw, the ones it stored, the ones it verified, and whether it moved the
  target ref. The restore subcommand records the same pair around its
  replay, in the restored store's own `audit.log`, with the counts the
  restore reports: repos, refs, and pending lands. Both take
  `--audit-file`, with the same default as `gc`. The golden's
  `admin`/`intent` and `admin`/`ok` sections grow the fields of the new
  lines. The fix that made this possible: `verify` and `restore` sat
  behind an `unreachable!` in the store-open match since the archive
  landed, so both subcommands panicked instead of running; they now run
  before the store opens.
- # The admin lines cover every state-changing command
  
  The last admin actions join the audit. The `derive` subcommand records
  one `admin`/`intent` line before its backfill and one `admin`/`ok` after
  it, with the kind and the rows the run wrote and removed; a failed run
  records `admin`/`failed` with the class. Making the counts real,
  `backfill` returns a `DeriveReport` (the kind's name, the rows it wrote,
  the rows of older versions it removed) instead of a bare name.
  
  An open that migrates a store's schema records one
  `admin`/`ok` line `{what: migrate, from, to}` as soon as the audit can
  open, before the command's own pair: the migration ran inside the
  store's open, before the audit could exist, so no intent can precede
  it. A fresh open creates the schema and reports no migration. Both
  engines report their move: a Postgres open runs its own migrations at
  open, and its runner reports the versions the same way SQLite's does.
  
  The `ticket-key` subcommand records one intent before the key exists
  and one ok after it is on the disk. The lines carry the what and
  nothing else: never the seed, the key bytes, or the key file's path,
  in any encoding the test searches for.
  
  The golden's `admin`/`intent` and `admin`/`ok` sections grow the new
  fields; no new (event, outcome) kind appears. With this slice every
  state-changing CLI command writes its admin lines, and #385's admin
  item is complete: gc, import, restore, derive, the schema migration,
  and ticket-key generation all record what they changed.
- The server records an audit log: an append-only JSON-lines file of
  authentication, writes, ref changes, lands and administrative
  changes, with no tokens and no contents.
  
  The file sits in the data directory (mode 0600, `--audit-file`
  relocates it, no disable flag) and one writer thread owns it: callers
  hand lines to a bounded channel, the writer drains the queue, writes
  and fsyncs once per drain, and wakes each caller when its own lines
  are durable. A state change records two lines: an `intent` line
  fsynced before the change, and a `result` line after it, joined by
  request ID; a read records one. Every intent gets its result: a call
  that dies before its job starts is answered `cancelled` by the call's
  drop, and a land the stale-land sweep rejects is answered `stale` by
  the sweep. One case can leave a result missing: a job that dies after
  it claimed the call but before it recorded anything — the server log
  carries what the audit line cannot. If the audit append fails, the
  change is refused and readiness reports not-ready. Refused
  authentication is aggregated per peer per interval into one counted
  line, capped.
  
  The write, land and ref events carry what an operator needs and no
  contents: the RPC, the repo, the ref, the byte count of an upload,
  the land's state and ID, and a ref move's old and new changeset IDs.
  A changeset ID names content the caller already had; a blob or tree
  digest would name contents, so none appears.
  
  On open, a file whose last line has no newline gets one, so a line
  that a full drive cut in the middle stays a fragment of its own and
  cannot swallow the lines that follow.
  
  A land's ref line names the head the move replaced, read where the
  move happens, under the ref's lock, and is written only by the call
  that made the move. A replayed land (the same request key after it
  landed) moved nothing, so its land line names no old and no ref line
  appears for it; an auditor finds the head the move replaced in the
  land line of the call that made the move, under the same land_id.
  The ref lines of a ref carry the ref's true history: each old is the
  head its move's CAS replaced, so linking old to new rebuilds the
  history. The lines of concurrent changes can appear in a different
  order than the changes themselves (each job records its result after
  its change), so the history is linked, not read in line order.
  
  Blob and tree digests are never logged (a digest is a dictionary key
  to contents), and neither are token bytes or token hashes. Rotation
  is by size, keeps a bounded number of old files, and names what it
  dropped in an admin line.
  
  Not covered yet: the ref moves that happen outside the server.
  Import moves a ref to each imported tip (import.rs), and archive
  restore writes refs back (restore_ref); they run as their own
  processes and cannot hand lines to the server's writer, so they must
  append their own lines to the same audit file, intent before the
  move. Until they do, a reader of the log must not take it as
  complete: a ref can move without a line.
- # The metric surface's golden
  
  The `/metrics` registry joins the machine-checked surfaces: a
  checked-in golden (`crates/scion-proto/tests/metrics_v1.golden`) pins
  every `scion_*` name the registry renders, its `# TYPE`, and its
  labels and label values - names, types and labels are the surface,
  the sample values are this run's counts and pin nothing. The test
  exercises the registry end to end first (one request per RPC path,
  one cancelled, one failed, every gauge and histogram), because
  histograms render only once they counted. The stalled gauge hides
  until a reply stalls, so its name, type and label set pin in a unit
  test beside the render. A diff in the golden is a surface change: a
  removed name or a changed type is a break, never a re-pin; re-pin an
  added name on purpose with `SCION_UPDATE_METRICS_GOLDEN=1`.
- The fault matrix runs its disk, power and mount rows on Postgres beside
  SQLite: the `mount (NFS)`, `power-loss` and `test (postgres)` jobs each
  run the Postgres arm of their suite, and `docs/faults.md` says which half
  of each fault every run proves on Postgres (the blob files in the data
  directory; the metadata in Postgres's own WAL). The tier row is marked
  N/A: the tier keeps its own object cache and takes no metadata backend.
  
  The busy row now runs on both backends in one test: Postgres blocks
  through an open transaction of a second connection, SQLite through
  `BEGIN IMMEDIATE` on the same file, and both answer busy inside the
  same four-to-twenty-second window.
  
  On Postgres, a database that answers SQLSTATE `53100` (out of space)
  maps to `Storage::disk_full` and the fixed `ResourceExhausted` reply, a
  unit test of the mapping in `pg.rs` pins the SQLSTATE, the class and the
  reply text; the stale comments that pointed at error.rs say the same.
- # The archive's golden
  
  The archive format joins the machine-checked surfaces: a checked-in
  golden (`tests/archive_v1.golden`) pins every file of the export of
  one fixed store by hash and path - the manifest's clock reading and
  store UUID pin nothing - and pins the changeset ID the restored
  store's `main` resolves to. The same test runs `verify` on the golden
  with no store code and a restore into an empty directory, `fsck`
  clean. A diff in the golden is a format change: a new archive version
  beside v1 per docs/archive-format.md, named in a changes/ fragment.
  Re-pin on purpose with `SCION_UPDATE_ARCHIVE_GOLDEN=1`.
- # The audit log's line surface, pinned by a golden
  
  The audit's lines join the machine-checked surfaces: a checked-in
  golden (`crates/scion-server/tests/audit_v1.golden`) pins every
  (event, outcome) kind a served server writes - the exercise drives
  refused auth, the start line, a put, a create, the failed create, a
  land and a land that runs past its deadline - and for each kind, the
  field names and JSON types of its lines. The values (`ts`, the ids,
  the counts) are this run's and pin nothing. A source census pins the
  kind set the code writes, and the exercise must equal it exactly.
  Re-pin on purpose with `SCION_UPDATE_AUDIT_GOLDEN=1` and a changes
  fragment naming the added kind.
- The server records an audit log: an append-only JSON-lines file of
  authentication, writes, ref changes, lands and administrative
  changes, with no tokens and no contents.
  
  The file sits in the data directory (mode 0600, `--audit-file`
  relocates it, no disable flag) and one writer thread owns it: callers
  hand lines to a bounded channel, the writer drains the queue, writes
  and fsyncs once per drain, and wakes each caller when its own lines
  are durable. A state change records two lines: an `intent` line
  fsynced before the change, and a `result` line after it, joined by
  request ID; a read records one. If the audit append fails, the change
  is refused and readiness reports not-ready. Refused authentication is
  aggregated per peer per interval into one counted line, capped.
  
  The write, land and ref events carry what an operator needs and no
  contents: the RPC, the repo, the ref, the byte count of an upload,
  the land's state and ID, and a ref move's old and new changeset IDs.
  A changeset ID names content the caller already had; a blob or tree
  digest would name contents, so none appears.
  
  On open, a file whose last line has no newline gets one, so a line
  that a full drive cut in the middle stays a fragment of its own and
  cannot swallow the lines that follow.
  
  A land's ref line names the head the move replaced, read where the
  move happens, under the ref's lock, and is written only by the call
  that made the move. A replayed land (the same request key after it
  landed) moved nothing, so its land line names no old and no ref line
  appears for it; an auditor finds the head the move replaced in the
  land line of the call that made the move, under the same land_id.
  The ref lines of a ref carry the ref's true history: each old is the
  head its move's CAS replaced, so linking old to new rebuilds the
  history. The lines of concurrent changes can appear in a different
  order than the changes themselves (each job records its result after
  its change), so the history is linked, not read in line order.
  
  Blob and tree digests are never logged (a digest is a dictionary key
  to contents), and neither are token bytes or token hashes. Rotation
  is by size, keeps a bounded number of old files, and names what it
  dropped in an admin line.
  
  Not covered yet: the ref moves that happen outside the server.
  Import moves a ref to each imported tip (import.rs), and archive
  restore writes refs back (restore_ref); they run as their own
  processes and cannot hand lines to the server's writer, so they must
  append their own lines to the same audit file, intent before the
  move. Until they do, a reader of the log must not take it as
  complete: a ref can move without a line.
- The crash suite's Postgres arm, as its own file so it never touches
  crash.rs. The real `scion-server` binary serves a Postgres database,
  and the test kills it with SIGKILL - the crash a cut without a FIN or
  RST leaves - at steps of a measured whole run, the way the SQLite
  suite places its kills.
  
  Two rows:
  
  - A kill while a wide land runs leaves the ref at the head it had or
    at a changeset the move put there - one whose first parent is that
    head, and whole enough to read back. One restarted server answers
    the replay of the same request key, and the ref must end exactly
    where the reply says it did. The kills run until they have seen
    both endings: a land the kill cut before the move, and one it cut
    after.
  - A kill while the schema upgrades leaves the database at one whole
    version, and the next start finishes the upgrade and serves a
    repo the client resolves.
  
  The tests skip themselves unless `SCION_TEST_BACKEND` names
  `postgres`. Their clients carry their own 5 s connect timeout and
  30 s per-call deadline until a shared builder lands. Each try runs
  well under 300 s.
- # Wave 2: the durability point's backup proofs run the guide's commands
  
  The shared helper gains a block selector: `runbook_block_with` and
  `run_guide_block_with` take a needle and pick the first block under the
  heading that contains it, so a section with several blocks (a backup's
  snapshot, its object copy) hands a test the step it performs.
  
  The backup and restore-bench proofs now take their snapshot and copy
  the blob files with the runbook's own lines - `sqlite3 ... "VACUUM
  INTO ..."` and `cp -R ... objects` - with the test's directories in
  place of the documented ones; restore-bench runs its full measurement
  through them. The four physical-failure proofs (power cut, dm-log cut,
  kill timing, injected fsync failure) carry `(behaviour, no command)`
  markers: their subject is the failure's own timing, which no shell
  command reproduces. Six rows leave NOT_YET_DRIVING; six remain.
- # Wave 1: the first boot and one-server proofs run the guide's command
  
  The harness gains `spawn_on`/`start_on`: the serve argv comes from the
  first fenced `sh` block under the named heading of docs/operations.md,
  with the test's data directory, token file, and port substituted for
  the documented ones. The first-boot test starts its server with the
  First boot block's own line and asserts the One server block's command
  is the same command; the kill test starts on the One server block. The
  backend_refusals proofs move to `(behaviour, no command)` markers:
  their subject is the database's own lock and container, not a scion
  command. Five rows leave NOT_YET_DRIVING; twelve remain.
- # The two maintainer drills of #393
  
  docs/drills/vulnerability-drill.md walks a mock private vulnerability
  report end to end, from GitHub's private vulnerability reporting
  through the private fork, the CVE decision, the published advisory,
  and the credit, using only SECURITY.md and the Security tab - and
  lists what SECURITY.md does not say for this (where a fix is
  developed, when a CVE is requested, how a security fix ships, the
  publication timeline, triage ownership), without filling the gaps.
  
  docs/drills/release-drill.md runs RELEASING.md as a second maintainer,
  starting with the dry run, and marks every step that today needs the
  owner: the signed tag (their personal SSH key, D18), the refs/tags/v*
  ruleset, and the immutable-releases setting - the v0.8 list. Both files
  carry the record header, the done-whens, and the notes blanks, and
  keep to what the runbooks already say.
- The nightly is CI itself: `ci.yml` gains a schedule trigger (03:00 UTC,
  after the soak's 02:30), so the scheduled full suite is exactly the CI
  suite - the same jobs, the same cases, nothing to drift. The
  concurrency group carries the event name, so a 03:00 schedule waits
  its turn instead of cancelling a push to main that started minutes
  earlier.
  
  `scripts/nightly-streak.sh` prints the current streak: the scheduled
  runs of `ci.yml` on main, counted from the newest completed one
  backwards until the first non-success, which is the v0.8.0 roadmap
  line's "30 consecutive runs" as a fact anyone re-checks with `gh`. A
  small `nightly streak` job prints it on every run; it reads the run
  history only (`actions: read`).
- One land on Postgres runs its work as serial client-to-database
  statements inside the per-ref critical section, and every land queued
  behind the one that holds the ref waits for all of them. The analysis
  lives in the land performance issue.
  
  The store layer counts the statements a process runs against Postgres,
  behind the test hooks, and a Postgres-only guard measures one land
  through the server's Land RPC - the land already on the head, and one
  that must rebase - and fails the run when either is above its count.
  
  A transaction now reads each changeset's generation once: a
  generation is fixed at insert and never updated, so the derived walks
  and the land that ask for the same few ids again get them from the
  transaction's own cache, and a merge reads every parent's generation
  in one statement instead of one per parent. The guard's limits fell
  from 51 to 48 and from 56 to 55 with this change.
  
  The multi-row writes went the same way: the trees a change builds land
  as one statement per table instead of one per row, and so do the blobs
  a changeset puts and the rows of its parents - the work that scales
  with the change's width. A third guard case pins that: a land of 50
  files ran 106 statements before the batch and 56 after.
  
  A repeated parent now stores the same way on both backends (the
  batched read had diverged from the per-row loop on Postgres), and a
  test holds the two backends to the same answer.
  
  The land's outcome check rides the plan's read transaction: one read
  answers both whether a twin call settled the land and what the land
  must do now, where the loop read them in two steps before. The counts
  do not move (the loop's read ran without transaction boilerplate
  already); what moves is that the plan can no longer run for a land
  that a twin call has settled. On SQLite that read is one snapshot; on
  Postgres it runs in autocommit statements, so the two reads can see
  different states - a settle that lands between them is stopped at
  `set_land_state`'s pending guard ("no longer pending"), which is the
  backstop there.
- The production-trial record exists as a form:
  [docs/production-trial.md](docs/production-trial.md) (#395, the v0.9.0
  gate). Its fields hold what the roadmap names: the commit and the
  binaries with their checksums, the backend and its version, the
  filesystem, the workload with its floors fixed in writing before day
  1, a daily table of lands, reads, latencies, memory, disk and errors,
  and sections for upgrades, restores, key and certificate rotations and
  incidents — each with a time, what happened, and the evidence link —
  ending in the pass-or-fail rule against the floors. Every floor value
  is left blank for the owner to fix, and every field says what a second
  operator fills in.
- The operator's alert rules are versioned with the code they watch
  (deploy/prometheus/openscion-alerts.yml)
  
  Nineteen alerts over the metrics the server, the cache and the gc
  collector export today - the server is down, RPC error rate, internal
  errors, land errors and replans, latency p99, disk free (two levels),
  blob sync saturation, the view memo bypassed, stalled replies, the
  Postgres pool and transaction age and collector key, the tier's miss
  rate, the gc run going stale, and Objects refusals. Severities are
  docs/support.md's levels, and each alert's annotation says the
  operator's first move, linked to the operations.md section that has
  it. The Postgres group is true only for the Postgres shape.
  
  ci.yml gains a promtool job: the pinned Prometheus release tarball
  (v3.13.4, the current LTS line, its sha256 in the job) checks the
  rules and runs openscion-alerts-test.yml, where every alert fires on
  a crafted series and does not fire just below its threshold. The
  daemon exports no metrics; an alert on it needs the gap closed first.
- # The guide's own commands become an executable rule
  
  A Proof of a runbook used to mean a reviewer reading the test and the
  guide side by side. Now the check runs itself:
  
  - The block-extraction the alert-runbook tests carried moves into the
    shared test support (`test_support::docs`): `runbook_block` reads the
    first fenced `sh` block under a heading of `docs/operations.md`, with
    named substitutions (an address, a data directory, a token file) in
    place of the documented literals, and `run_guide_block` runs it
    through `sh`. The alert tests keep their blocking-pool wrapper and
    call the shared helper.
  - `docs_ops` gains `every_proof_runs_the_guides_command_or_says_why`:
    for every `Proof:` line, the named test must call the shared helper
    with its section's heading, or the line must say `(drill)` or
    `(behaviour, no command)` with a reason, or the proof must sit in
    `NOT_YET_DRIVING` with every other converting proof. The list must
    match the guide exactly, so no proof is exempt silently and no
    finished conversion keeps its row.
  - The certificate-rotation and ticket-key Proof lines now carry the
    `(behaviour, no command)` marker with their reasons, as do the
    disk-full, failing-disk, and Postgres-restart runbooks, whose steps
    are actions on the machine or the database rather than one command.
  
  ## Proofs that do not yet run their block
  
  `NOT_YET_DRIVING`, in `docs_ops.rs`, names the tests that still run the
  guide's commands their own way; each row is work to delete. Today:
  
  - First boot, One server: the harness spawns the guide's serve command
    itself; conversion is a refactor of the shared spawn.
  - The durability point: power, fsync, and backup tests drive the
    storage through the test seams, not through the guide's shell.
  - A failed migration and 5. Routines: the refusal and gc tests run the
    commands, but not yet through the helper.
  - Stalled replies high: the log-stream test reproduces the stall, not
    the guide's curl.
- # Wave 3: the collector's proof runs the routine's own line
  
  The block selector now finds a heading at either level, so `5. Routines`
  - a numbered part, not a subsection - hands its collector block over.
  The gc-metrics proof builds its collector command from the routine's own
  line, with the test's data directory and metrics file substituted, and
  its dry run is the documented `--dry-run` flag.
  
  The four proofs whose subject is behaviour no command reproduces - the
  refusal and restore of a failed migration, the kill inside a migration,
  the fsck-restore routine, and the stalled-reply close on a paused clock
  - carry `(behaviour, no command)` markers with their reasons.
  
  NOT_YET_DRIVING is empty: every Proof in the guide runs its section's
  command block through the shared helper, or says on its line why it
  cannot. A new proof must do one of the two, and a stale row or a silent
  exemption fails the check.
- The disk-full runbook (docs/operations.md, "Disk full"): the two alerts'
  first move is the gc routine - a dry run, then a real run - then fsck,
  then grow the volume if gc collected too little; a write that failed
  out-of-space leaves nothing to repair, and writes resume with no restart.
  The Proof test runs those steps in order on a real full disk (tmpfs):
  fill, refusal, the gc dry run and real run through the actual command,
  fsck clean, then a write that lands again with no restart and a clean
  reopen.
- The failed-migration runbook (docs/operations.md, "A failed migration"):
  the two failures an operator can see, each with the rule that names it.
  A newer schema refuses the open with both versions named, and the fix is
  the old binary or the restore of section 3 - never a hand-edited
  user_version. A start that waits on other processes says which; stop
  them. The Proof test forges the newer-schema case: the server refuses
  the open with the error, the restore of the backup per section 3 brings
  the store back, and the server serves. The fsck-failure runbook is its
  own subsection of the routines, pointing at the failing-disk runbook's
  Proof test, which runs the restore of section 3 end to end.
- The failing-disk runbook (docs/operations.md, "A failing disk"): a read
  the disk refuses answers internal and the rest keeps serving; fsck runs
  beside the live server and its exit 1 names the damage; the store has no
  repair, so the fix is the restore of section 3, and fsck runs on the
  restored copy before the server starts. The Proof test runs those steps:
  it takes a backup per section 3's order, fails a read with EIO, corrupts
  a blob file behind the store's back, runs the fsck command to its exit 1
  with the blob named, restores into a fresh directory, and reads back
  through a fresh open.
- The first-boot runbook (docs/operations.md, "First boot"): a token file
  and the three commands of a first start, each with its promise - the
  token goes in a 0600 file (umask 077, then printf), not in argv, the
  binary creates the data directory and serves, the health service
  answers for the service name and for liveness, and the first import
  makes a repo the client reads. The Proof test runs the commands in
  order against a fresh data directory - the harness's server now takes
  its token from the file too - and reads the imported ref back.
  
  The certificate-rotation runbook (docs/operations.md, "Rotating the
  server's certificate"): a new certificate of the same CA needs no client
  change - the rotation is write the pair, restart, and the client's
  unmoved CA file still connects. The Proof test rotates the server's
  certificate under a connected client and reads back over https. A new
  CA is the drill record of docs/drills/, since no test can hold a
  running fleet's client CA files.
- The Postgres-restart runbook (docs/operations.md, "A Postgres restart"):
  a database that restarts under the server needs no server restart - the
  calls answer the busy error a client retries against, the store does not
  fence itself, and the next write after the database is back reconnects,
  takes the advisory lock again, and runs. The Proof test is the existing
  takeover test, which stops the job's database, sees the busy answers and
  the fence stay down, brings the database back, and recovers the store
  with no process restart.
- - A packet for a hired reviewer: [docs/security-review.md](docs/
    security-review.md) states the review's scope and its out-of-scope
    boundaries, links the trust model instead of copying it, and lists
    the internal attack passes by surface — what each tried, what it
    found, and the pull request that closed it — so a reviewer starts
    past them. It walks a ten-minute local build-and-attack setup, links
    the fuzz target map, and names where the project itself wants the
    most eyes, strongest first. No exploit recipes: every named weakness
    is closed with a test or pinned as documented behavior.
- # The snapshot file's golden
  
  The snapshot file joins the machine-checked surfaces: a checked-in
  golden (`tests/snapshot_v1.golden`) pins the bytes today's writer
  produces for a fixed object set, and the same golden seeds back into
  an empty store. A diff in the golden is a format change: stability.md
  says a new magic beside `scnsnap1\n`, read beside v1, and a changes/
  fragment naming the break. The test header says how to re-pin on
  purpose (`SCION_UPDATE_SNAPSHOT_GOLDEN=1`). The codec and proto
  goldens get their on-purpose update paths written in their headers
  too - the codec's is "a grammar change is a new format, never a
  re-pin", the proto's is "re-pin additions only; a removal is v2".
- # The docs' named tests are checked
  
  Every test the docs and README name by file and function is now
  machine-checked (docs_links.rs): the file is in the repo, it holds
  the fn, and the fn is a test - `#[test]`, `#[tokio::test]` or one a
  `proptest!` generates - not a helper. A failure names the doc, its
  line and the missing name. Eight references in docs/threat-model.md
  named helpers, not tests; they now name the covering test or keep the
  helper in prose, so no claim reads as proven when its test is gone.
- - A release page for users: [docs/verify.md](docs/verify.md) walks the
    four checks a download needs before it runs — the SSH-signed tag
    (`git tag -v` with the owner's allowed-signers line), the checksums,
    the GitHub build attestation (`gh attestation verify --repo
    bnsd55/openscion`, the exact flag the pipeline runs), and the SBOM.
    The README's platform note links it.
- `scripts/lint-workflows.sh` runs actionlint (pinned by version and
  sha256, like the grpcurl pin) over `.github/workflows/` in the `fmt +
  clippy` job: a workflow that cannot parse fails the PR that carries it,
  not main's next push-triggered run (#416 broke bench.yml with an
  unquoted colon, #419 shipped a coverage.yml its pinned tool refused,
  and no run caught either). The check proves it on the #416 case: the
  unquoted `CAPACITY_MACHINE` value fails actionlint's syntax check, the
  quoted one passes.
- [docs/ROADMAP.md](docs/ROADMAP.md), the owner's order of the work: the
  v0.1.1 distribution and portability gate, the v0.2.0 compatibility and
  recovery gate, the v0.3.0 single-node operational beta, and the work that
  stays out until those pass. The README links it.
- Two documents that say what the project stands behind:
  [docs/stability.md](docs/stability.md), one row per surface (the
  crates' APIs, the CLI and its `--json` output, protocol v1, object
  format v1, the SQLite and Postgres schemas, snapshot files, ticket
  bytes, and the `test-hooks` feature) saying what 0.x promises, what a
  patch or a minor release may change, and how to upgrade; and a
  "Supported platforms" section in the README naming the runner images
  and architectures CI tests (Ubuntu 24.04 x86-64, macOS 15 arm64),
  Postgres 17.11, and what is untested or unsupported (Windows,
  FreeBSD, a data directory on a network filesystem) (#312).
- A pull request adds a changelog row as a fragment,
  changes/<slug>.<section>.md, instead of editing CHANGELOG.md: the
  section is one of added, changed, fixed, security, the slug is the
  pull request's number or a short kebab name, and the file is plain
  text, wrapped at 76 columns. At a release,
  scripts/merge-changelog.sh <release> assembles the fragments into
  the changelog and deletes them, and CI refuses a pull request that
  edits CHANGELOG.md itself; only a release/ branch does (#314).
- A release smoke test for the release checklist (#315):
  `scripts/release-smoke.sh` builds the release binaries and walks the
  README quick start on a small git repository it makes — import,
  ticket-key, serve, `scn` reads (log, ls, cat, blame) and writes (commit,
  land, read back), `fsck` clean, and a shutdown by one signal that must
  exit 0. It runs the story once under umask 022 and once under 002, names
  the step that failed, and removes everything it made. A CI job runs it
  on Linux.
- The release smoke joins CI on macOS (#317): the release-smoke job
  runs on macos-15 as well as ubuntu-24.04, so the README quick start
  story — import, ticket-key, serve, `scn` reads and writes, `fsck`, and
  a one-signal shutdown — proves itself on every supported platform on
  every push and pull request, once under umask 022 and once under 002.
- A versioned logical archive: `scion-server export`, `verify`, and
  `restore`. An export writes a whole repository, or the whole store, to a
  `scion-archive-1` directory: one read snapshot holds every metadata
  section (changesets with their bodies and generations, refs with their
  versions, pending lands with their request keys, git provenance), and
  the blob files are copied byte for byte, which is safe beside a live
  server because a blob file is immutable and gc keeps a blob past its
  grace. `verify` checks an archive against docs/archive-format.md with
  the core codec, blake3, and zstd only: it re-hashes every blob file and
  changeset body, and checks that every ref, parent, and land input is
  backed. `restore` replays an archive into an EMPTY store: the blobs go
  through the store's own write path, so every object ID carries over;
  the changesets replay in generation order, so the store derives the
  same generations and rebuilds the trees; the refs go back with their
  versions; the pending lands are recorded again, so a retry with the
  same request key resumes. The e2e exports beside a landing writer,
  verifies, restores, finds fsck clean, and resolves the same main; and
  it proves verify fails: a corrupted blob byte and a ref past the
  changeset stream are both named.
- Readiness, liveness and a bounded drain, tested end to end. The health
  service splits: the service names (scion.v1.Scion, scion.v1.Objects) are
  readiness and answer NOT_SERVING from the start of a drain, while the
  empty name is liveness and stays SERVING while the process answers, so a
  supervisor does not kill a draining server. A stop drains with
  --drain-timeout-secs (default 600): the in-flight calls finish and the
  process exits 0; past the bound they are cut, the log names the count,
  and the process exits 1. The e2e stops the real binary under a parked
  upload and a land: an answered call is durable after the restart, a cut
  land stays pending and a retry with the same request key resumes it, and
  fsck is clean.
- A backup and restore runbook for a server that keeps serving:
  the metadata snapshot first (VACUUM INTO, or pg_dump on Postgres), the
  blob files second, the store's UUID marker with the metadata; what the
  backup promises, why gc must stay out of it, and a test that restores
  a backup taken while writes land.
- The checkout benchmark runs in a workflow_dispatch-only workflow on a shared
  ubuntu-24.04 runner, and docs/benchmarks.md states the runner, the dataset
  and the commands, with the noise in the min and max.
- The OpenSSF Best Practices badge audit: docs/best-practices.md maps the
  passing and silver criteria (read live from bestpractices.dev) to the
  repository's evidence, one table per level, so the badge form can be
  filled from it. Every row says met, gap, or N/A with the file, workflow
  or doc section behind it; the gaps are listed smallest first, split into
  doc changes we can make and project facts only the owner can supply
  (governance and the second access path, the issue-history answers, the
  signed-release channels, the person claims). Nothing is submitted
  anywhere: the badge entry and the README badge are the owner's actions.
- The CI's workflow tokens now run least-privileged everywhere, and code
  scanning comes to the repository.
  
  - The soak workflow, the one workflow without an explicit token grant,
    now declares `permissions: contents: read` at the top level, like the
    other four already do; no job in any workflow needs more.
  - A new CodeQL workflow scans Rust and GitHub Actions workflows on
    pushes to main, on pull requests, and weekly. Rust analyzes in the
    CodeQL build mode `none` (no build), and the analysis job carries the
    single `security-events: write` grant it needs to upload its result.
- CI fails if the test suite leaves the work tree dirty: after every
  `cargo test` in the three test jobs, `git diff --exit-code` runs, and its
  failure names the files a test wrote outside its sandbox.
- A written compatibility policy: which component pairs work at versions
  N and N-1 (the previous release, including the previous minor), what
  "works" means per pair, and what a break needs in 0.x — a minor that
  changes a wire, ticket or snapshot contract serves or reads the old
  version beside the new one and drops it no earlier than the next
  minor, while the store's migrations stay forward-only.
- The statement-coverage gate: a coverage workflow (push to main and
  workflow_dispatch, never pull_request) that measures the workspace with
  cargo-llvm-cov v0.9.1, pinned by version and the release tarball's
  sha256, and fails when the merged report drops under 80 percent lines -
  the roadmap's statement bar, where a Rust statement is an executable
  line. Two measures accumulate into one report: the default backend and
  the Postgres backend, so pg.rs is in the number; the per-crate summary
  prints in the log. The exclusions are checked in (scripts/coverage-exclusions.txt): the
  file holds no patterns today, with the reasons - the generated proto
  code builds into OUT_DIR under target/ and the measure never counts
  it. The test seam is in the measure: a test build compiles the
  test-hooks feature (the crate's own dev-dependency names it), so the
  seam's ~900 lines count, accepted for now as a SHORTCUT whose upgrade
  path is in the file - move the seam's items into their own files and
  one pattern excludes them. A pattern that matches no tracked file
  fails the job, so a vacuous exclusion cannot hide in the file, and the
  report step proves its own filter: no excluded file may appear in its
  printed list.
- docs/operations.md's shell blocks cannot drift from --help: two docs_ops
  tests parse every command the guide writes for scion-server (split at its
  subcommand) and for scion-cache and scion-daemon, and fail when a flag the
  command names is not in that binary's help - the direction docs_flags.rs
  does not cover. A runbook section whose steps a test runs ends with a
  Proof: line naming the test; the test fails when a proof's file or
  function is gone, so renaming or deleting a proof breaks the guide loudly.
  The first five proofs name the tests that already run the key-rotation,
  backup-and-restore, upgrade, and gc steps. docs/drills/ gains the template
  a recorded drill follows, and RELEASING.md gains the line that runs the
  release's drills on the release candidate.
- The collector's operating metrics (#239): `scion-server gc
  --metrics-file PATH` writes the last finished run as a Prometheus
  textfile — the end time, the duration, and the rows and files it
  collected, each with a `dry_run` label — atomically, and only when the
  run succeeds, so a failed run leaves the old file whose timestamp goes
  stale. A long run logs its progress over the repos at most every 10 s.
  The architecture metric table and the operations guide name the file,
  the textfile collector's folder, and the stale-timestamp alert.
- ci.yml's macOS test job runs the same two clippy passes as the Linux
  lint job (lib+bins and all-targets, -D warnings) before the tests.
  The Linux job never compiles macOS-only code - the workspace's one
  unsafe FFI module among it - so those lines were never linted; the
  job's warm cache makes the added cost a couple of minutes, and no
  second macOS runner is booked. The one macOS-only warning this
  exposed (an unused mut in the ACL FFI walk, also fixed by #402) is
  fixed here so the new gate is green.
- Add a mount-reps workflow: on demand only, on macos-15, it builds the
  scn lib test binary once and runs the fork-gated mount tests the
  number of times the reps input gives (100 by default), stopping at
  the first failure and uploading that run's log. This is the repeated
  evidence that the fork gate (#309) fixed the macOS mount flake.
- A store of the previous schema version opens on a new build: the open
  migrates it forward and serves what it held. A test builds a version-1
  store of each backend in place (the SQLite schema of migration 0001, a
  Postgres schema of migrations/postgres/0001, each with a repo, a root
  changeset, its ref and two blob files this build wrote), opens it the
  normal way, and checks the migration level after the open, the ref,
  the changeset, both blobs byte for byte, a file read through the tree
  the upgrade rebuilt, and a clean fsck (#318).
- The toolchain floor is on the record: docs/stability.md states when it
  may rise (only in a minor release, named in the release notes, never
  above the stable toolchain of six months ago, and checked by CI);
  docs/RELEASING.md checks the pushed tag against the release commit
  before the notes are built; CONTRIBUTING.md says who decides and that a
  squash keeps the outside contributor as the commit's author, with the
  maintainer as the committer.
- The crates publish only after their public APIs are on the record:
  scripts/public-api.sh writes, for each of the five publishable crates,
  what cargo-public-api sees as the crate's public API, one file per
  crate under the folder the script takes as its argument. The step for
  the crates.io publish of #21 is to run it on the tagged commit and
  read the diff against the previous release's listings before anything
  publishes. The script installs cargo-public-api and the nightly
  toolchain it needs when they are missing, and prints one line per
  crate with the line count of its listing.
- The release pipeline ([.github/workflows/release.yml](.github/workflows/
  release.yml)): on a pushed `v*` tag it verifies the tag's SSH signature
  through GitHub's API (the workflow holds no key), builds every artifact
  — the four binaries, the source tarball, an SBOM per shipped binary's
  crate — twice per cell from `.github/release-cells.json`, fails on one
  differing byte between the two builds, attests each artifact and SBOM
  with GitHub's build-provenance and SBOM attestations, verifies every
  attestation, and publishes one immutable release whose notes come from
  the tagged `CHANGELOG.md`. A `workflow_dispatch` dry-run runs the whole
  pipeline but publishes nothing, so it is provable before the first tag.
  [docs/slsa.md](docs/slsa.md) is the honest Build L3 assessment;
  [docs/RELEASING.md](docs/RELEASING.md) lists the repository settings
  only the owner can turn on.
- The Scorecard run now enforces a pinned policy: floors are per check,
  not an aggregate score, in .github/scorecard-policy.yml. Ten checks
  carry a floor of 10 (Binary-Artifacts, CI-Tests, Dangerous-Workflow,
  Dependency-Update-Tool, License, Maintained, Pinned-Dependencies,
  Security-Policy, Token-Permissions, Vulnerabilities) and
  Branch-Protection a floor of 8. Every check the project does not meet
  yet - Signed-Releases, SBOM, Packaging, SAST, Fuzzing, Code-Review,
  Contributors, CII-Best-Practices - is a dated exception, each with an
  owner, a reason, and an expiry date; an expired exception fails the
  job like a floor breach.
  
  The job enforces the policy after each scan with
  scripts/check-scorecard-policy.py, against a second Scorecard pass in
  its JSON results format: the JSON lists every check with its score,
  so a check that Scorecard skipped or errored on fails the policy
  instead of hiding behind a missing SARIF entry, and a score of -1
  (Scorecard's "could not run") fails unless an unexpired exception
  covers it.
- The repository now runs the OpenSSF Scorecard itself, weekly and on
  every push to main, and publishes the result: the public Scorecard REST
  API (and any later badge) reads the score from these runs, and the
  SARIF lands in code scanning next to the CodeQL results.
  
  The analysis job carries the four grants the action's docs name —
  security-events write, id-token write, actions read, contents read —
  and nothing else; it checks out with no persisted credentials.
  
  Dependabot already covers the other half of the gap: cargo and
  github-actions updates are weekly and grouped, so the pinned action
  SHAs stay current.
- The support policy is written down in docs/support.md: the four
  severity levels (critical, high, medium, low, with one example from
  this project each), what each level gets (every fix on the current
  minor; critical and high security and data-loss fixes on the previous
  minor while its N-1 compatibility window is open, from v0.4), how a
  release announces the end of its support (from v0.4), and that 0.x has
  no long-term-support minor. SECURITY.md and README link to it.
- A threat model, docs/threat-model.md, replaces the one-line "one
  trusted machine" with the profile the code holds: one active server,
  its host and data directories trusted, the network and the token
  holders not. It names the assets, the actors, the six trust boundaries,
  and a security invariant for each promise the code makes, with the
  test that proves it and the claims that have no test yet.
- The version matrix (#239): a nightly job runs the pairs of the
  compatibility policy at versions N and N-1, building the tagged release
  behind HEAD, so a release that breaks a listed pair is caught by CI.
  The pairs: scn against the server, the cache tier against the server,
  scn against the daemon over its socket, each in both directions, and
  the store, which a newer server opens and migrates and an older one
  refuses. A dispatch can name any commit or tag as N-1, so a pair can be
  proven before the first release exists. Until a release is tagged the
  job says it skipped, in the run summary.
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

- A `Log` page checks its deadline before its first entry and then once
  every 16 entries, not before each one. A check reads the clock, a system
  call where the clocksource has no user-space read (HPET), and on such a
  host the check per entry made `scn log --path` of a 674-entry history in
  tokio about 5% slower: 88.1 ms against 83.3 ms, with 777 clock reads in
  the server against 149. A call that expires or goes still stops within
  16 entries.
- - The graph queries read their merges' parents and their side branches
    from caches that a chain page fills: one `parents_many` statement for
    the page's parents and one node batch for those parents, instead of
    one parents statement per merge. The walk order is unchanged.
- a pull request runs only the fast tier: fmt + clippy, the SQLite
  tests with 500 proptest cases and the docs tests. A pull request
  that touched only docs, markdown or changelog fragments runs the
  docs tests alone; a draft pull request skips every job. Main, the
  nightly and a dispatch run the full suite as before.
- - The graph queries read their merges' parents and their side branches
    from caches that a chain page fills: one `parents_many` statement for
    the page's parents and one node batch for those parents, instead of
    one parents statement per merge. The walk order is unchanged.
- a pull request runs only the fast tier: fmt + clippy, the SQLite
  tests with 500 proptest cases and the docs tests. A pull request
  that touched only docs, markdown or changelog fragments runs the
  docs tests alone; a draft pull request skips every job. Main, the
  nightly and a dispatch run the full suite as before.
- # The fuzz campaign: a wider target, a dispatchable budget
  
  ## The archive section readers gain their own target
  
  The archive readers behind `verify` and `restore` are the door bytes
  from an untrusted source enter by. `archive-verify` already fuzzed the
  whole archive; `archive-read` feeds each section reader directly
  (manifest, changesets, refs, lands, members, provenance), one
  first-byte selector, the rest the file's bytes. An archive accepted
  for verification keeps its verdict-or-refusal promise section by
  section.
  
  ## A manual run can give every target an hour
  
  `fuzz.yml` gains a `budget_scale` dispatch input (default 1) that
  multiplies every target's budget; the nightly run is unchanged, and a
  dispatched run can scale it.
  
  ## The seeds path is the crate's, and a missing seeds dir is not a failure
  
  The first dispatched run failed on every target whose seeds directory
  exists: the path was written from the repository root, while
  `cargo fuzz run` works from the crate. The step now resolves the
  seeds path from the crate, and a target without a seeds directory
  starts with an empty corpus instead of failing.
  
  ## The seed target counts the records the file frames
  
  The snapshot-seed target asserted every accepted seed report names at
  least one object. A valid snapshot may hold zero records (magic, a
  repo name, nothing more), and the first campaign run's crash was
  exactly such a file: the report is honestly all zero, and the
  assertion, not the reader, was wrong. The invariant is now exact: the
  target parses the framing independently and asserts the report covers
  exactly the records the file holds. The 12-byte empty snapshot joins
  the target's seeds as the regression case.
- - A path lookup read each level's whole tree node: decode it into the
    entry map, clone the map, remove the one name. Now the level's
    stored `TreeV1` bytes are scanned for the one name — linear with an
    early exit, because variable-length entries give no random access
    and a binary search would need an offset table the format does not
    have. The decoder's checks keep their place: the body is checked
    against its ID, and the scan refuses what the decoder refuses. In
    the lookup example's own measurement, finding one entry in a
    100,000-entry folder's peak allocation drops from 331 MB to 26 MB,
    and the scan answers what the full decode answers, name for name.
- Releasing is for a source release: `docs/RELEASING.md` cuts a release as
  a git tag plus GitHub release notes, with no crates.io publish until the
  crates' public APIs are narrowed (#21). The release PR's checklist
  merges the `changes/` fragments with `scripts/merge-changelog.sh`,
  builds a clean checkout through the README's `Try it` flow, runs the
  umask-002 CI job once #302 lands it, and states the provenance of every
  benchmark number: the commit, OS, filesystem, dataset, command and
  variance, and never the maintainer's machine. It tells how to undo a
  bad tag: redo the tag only while the release had no users, else mark it
  and cut the next patch version.
- scion-proto names its supported surface: `v1`, the wire constants, and
  `ticket` are the contract; everything else the five crates share —
  auth, check, deadline, listen, metrics, object, reply, request_id and
  tls — moves into one `scion_proto::runtime` module that is documented
  as unstable and not the published contract. Constants and helpers with
  no user outside the crate drop to `pub(crate)`, so the crates.io
  publish of #21 carries only what the wire needs.
- The server's Backend no longer says which database it holds: the
  public Sqlite and Postgres variants are gone, and the backend is an
  opaque struct with one private engine behind it, opened by the same
  engine-agnostic constructors as before. A caller matches behavior
  through the methods, never the engine, so a new engine (or a major
  bump of one) is a private change of the server crate, not a break of
  its API. The tests that build a backend from a store they prepared,
  or read the data directory's own store out of one, find both in
  test_support::backend (from_store, and BackendExt::sqlite), where the
  test seams live since the facade became the API.
- The checkout benchmark publishes its first results: one bench.yml run
  on main, the medians with min and max for the five states, and the
  ratios between the tools. A cold checkout through the daemon is 9.3 to
  22.1 times faster than git's clones and moves hundreds of times less;
  git's mirror clone stays 1.7 times faster than the warm daemon, and the
  numbers say why: the server side is idle, the gap is 50 fresh client
  processes.
- The unix-socket serving helper moved to scion_proto::runtime::listen,
  next to the TCP listener it shares the bounds with: scion-daemon and
  the tests that serve a daemon over its socket bind through the proto
  runtime, and scion-tier no longer carries serving plumbing in its
  public surface. The check the bind makes is unchanged: a real
  directory, owned by this user, closed to others, under parents no
  other user can change, and a dead socket file replaced.
- The capacity envelope of docs/operations.md (#387): a grid of writers,
  readers and backends run by scripts/capacity.sh on a GitHub-hosted
  runner, a dispatch job beside the checkout benchmark, and a doc test
  that holds the published table to the envelope's shape - the grid's
  every cell present in order, numbers that parse, no half-filled row,
  a footer that names the run's commit, and the edge in the caption.
- CI stops re-saving a rust-cache for every pull-request branch, which
  had grown to 66 caches and 10.6 GB - past GitHub's 10 GB ceiling, so
  caches were evicting each other and hits were shrinking. Caches save
  from main only now; a pull request restores main's. The three cargo
  test jobs also cache the workspace crates' own artifacts
  (cache-workspace-crates), so the postgres job stops rebuilding the
  bins the SQLite job just built: about five minutes off the job that
  finishes last in 13 of 20 runs. The fuzz smoke runs its ten targets
  four at a time (20 s each) instead of serially (15 s each), which cuts
  the job from ~11 minutes to ~7 on the runs where it sets the tail.
- CONTRIBUTING.md states the test policy the silver level asks for:
  major new functionality lands with tests in the automated suite, and a
  pull request that cannot test its change says why not in the
  description. The README states the interface's accessibility scope: a
  command-line tool and a service whose interface is text - commands,
  plain files, and log lines. best-practices.md closes the rows these
  close (test_policy_mandated, tests_documented_added,
  accessibility_best_practices) and the gap list shrinks to six.
- Waits bound by a call's deadline read what is left from one method,
  Deadline::remaining, instead of each call site computing it by hand;
  after the deadline it is zero, never a panic.
- Every server error that wraps another error returns it from
  `source()`. `Error::Git` stops discarding its cause: it carries the
  import's context and keeps the git library's own error as the source,
  which type is no longer part of the API; `Error::Apply` forwards to the
  apply error again, which `#[error(transparent)]` had dropped. The
  internal-error log line walks the whole source chain, and a test fails
  if a wrapping error's source goes missing.
- Store leaves the scion-server facade: only the test surfaces used it,
  and they read it from test_support now.
- README's Supported platforms section carries the macOS evidence: the
  mount and commit tests that fork passed 100 runs in a row on
  macos-15, run by the mount-reps workflow, after PR #309's fork gate.
- The scion-server modules are private except the fsck and gc
  operations: the crate's API is the facade in lib.rs, and the test
  seams sit in one test_support module behind the test-hooks feature.
- No production secret needs to appear in argv. The tokens and the
  Postgres URL take a checked-file form (`--token-file`,
  `--upstream-token-file`, `--postgres-url-file`, read the way the access
  file is: the owner alone may read it, a symlink is refused), the
  environment stays documented, `hash-token` reads stdin, and every argv
  form prints a deprecation warning naming its replacement. The scripts'
  throwaway tokens move to the environment.
- The storage engines come out of the server's public API. Error::Db,
  Error::Pg and Error::Busy fold into one Error::Storage, which carries a
  class (busy, out of space, or a failure of the engine's own) and keeps
  the engine's own error as the source, where logs and tests read it. A
  major bump of an embedded engine is then a private change, and callers
  match classes, not engine internals. The gRPC codes and reply texts of
  every class are unchanged.
- The runbook states what an acknowledged write has survived - the
  durability point: a blob put is answered after the file fsync, the
  rename, the directory fsyncs and the drive-cache flush; a land is
  answered after its blobs synced and its metadata commit was made
  durable (SQLite's `synchronous = FULL` WAL commit, Postgres's
  `synchronous_commit = on`, which the server checks at every open).
  
  Section 3 also states the recovery point of a backup (its metadata
  snapshot; a backup every N minutes loses about N minutes of
  acknowledged writes) and the restore time, measured: the new
  `restore time` CI job runs `restore_bench.rs` - a store of 20 repos,
  1000 changesets, about 32 MiB of raw blob content, restored and fscked
  - on a fixed `ubuntu-24.04` runner and prints the seconds, past a
  600 s bound.
  
  Section 1 says plainly that one active server has no automatic
  failover: a second server is refused (the lock is the anti-split-brain
  guard, not a failover), the operator restarts the server, and nothing
  takes over by itself.
- The project is now openscion, at github.com/bnsd55/openscion; the old
  URL redirects. The package names stay open-scion-*.
- The roadmap carries the whole line to v1.0: the production boundary at
  v0.4, durability at v0.5, the 1.x contracts at v0.6, operations at
  v0.7, releases and governance at v0.8, the release candidate at v0.9,
  and single-server production at v1.0. v1.0 supports one active server
  and no automatic failover; scale-out stays out. No crate publishes to
  crates.io before a real outside consumer asks for one. v0.1.0 is cut
  as one SSH-signed annotated tag and one immutable GitHub release.
- The scn library is the binary's pieces, not an API to build on: the
  facade in lib.rs is what the scn binary names (the client's Ctx and its
  repo-path parser, each command's run function and arguments), and
  everything else is crate-internal. The materialize and vfs modules are
  private, the mount session file's record moved out of the public
  surface into a crate-internal module, and the one test seam is a
  test_support module behind the test-hooks feature (default off), which
  holds the client preset, the session-file writer and the NFS
  filesystem that the integration tests drive. The encoded bytes, the
  commands and their output are unchanged.
- The three stale SHORTCUT markers now match main. The store's
  directory-flush marker says its upgrade shipped (the dm-log-writes
  suite runs in CI) and names the next one; the power suite's macOS
  marker says plainly that no macOS power-cut tool exists and what
  would lift it; the dead blob read-back helper is gone with its
  undefined-C5 marker, and the crash walk's marker states today's
  reason and trigger.
- The fault half of the soak: scripts/soak-fault.sh runs the soak's
  mixed load against one server and kills it on schedule - kill -9 under
  load, then a restart on the same data directory, one restart of the
  run taking the whole chain (cache and daemon with it). After every
  restart the run checks that the server lock is free, the server
  serves, a client read succeeds, fsck is clean beside the server, and
  every land the server acknowledged is still in the ref's history; the
  resource ceilings apply per restart segment. An optional Postgres
  restart ships behind SCION_FAULT_PG. The nightly gains a soak-fault
  job beside the soak, and the 72-hour run (#239, part 3) stands on it.
- The nightly soak runs 120 minutes instead of 10, so the latency trend's
  thirds mean something and a leak that a short run hides has two hours to
  show; the run stays bounded by the job's own timeout, and the soak's
  150-minute cap keeps a hung client inside it. The three resource
  ceilings are named in the workflow — 150 percent of the warm RSS, 100
  open files over the start, a late p99 under twice the early one — and
  soak.sh takes each from its environment, so a ceiling change is a diff
  in one file. The workflow no longer runs on pull requests: the nightly
  and a manual `workflow_dispatch` carry it, and its actions carry the
  same commit-SHA pins the main workflow got.
- The project's status label is alpha, not proof of concept: the README
  badge and note, SECURITY.md, and the architecture document's security
  limit now say alpha (0.x). The limit itself is unchanged: one trusted
  machine, never an untrusted network.
- The threat model tracks the code: #369 made the directory checks refuse
  every ACL beyond the mode (the sticky bit excuses no group write), so
  that invariant now names its tests instead of sitting in the no-test
  list. Two more claims got tests: a narrowed access file does not cut
  outstanding tickets until the ticket key rotates, and the partial
  reader's view keeps answering real changeset IDs, so a landing on
  hidden paths shows through the ref's ID over an unchanged view. The
  remaining no-test claims say why no test can exist.
- The workspace now denies unreachable `pub` items: `unreachable_pub = "warn"`
  lands in `[workspace.lints.rust]`, and every crate's `[lints]` inherits it.
  
  - Internal items that only the crate and its binaries use drop from `pub` to
    `pub(crate)`: the similar-object limits and helpers, the object/name/graph
    internals, the derived changeset helpers, the access constants, the metrics
    re-exports, and the test common modules' shared helpers.
  - Items the test seam re-exports (`test_support`) keep `pub` with an
    `#[allow(unreachable_pub)]` and a reason: the lint does not see through a
    re-export, and their external reachability is the seam itself.
  - `Metrics` is re-exported from `scion_proto` directly in lib.rs, since the
    `metrics` module is now crate-private.
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

- The import's history nodes are written in batches: the nodes a
  transaction inserted answer its own reads, and the flush writes one
  statement per 1,000 rows after the derived rows and before the git
  import rows. An import of 1,000 commits costs about two history
  statements per transaction instead of four per commit.
- An error reply of a land now waits for the store's flush before it goes
  out, the same durability barrier a successful reply already waited for.
  A reply that says the land stays pending rests on the row the land
  recorded, and that row is durable before the reply is sent, so a crash
  that follows the reply cannot lose it.
- The import's changeset rows are written in batches: the changesets row
  lands per commit (the derived rows of the same commit name it by an
  immediate foreign key), and the parents and the repo membership of a
  transaction's changesets wait for the flush at the transaction's end,
  one statement per 1,000 rows per table. The parents' generations chain
  through the generation cache, so the batch reads the database only for
  the parents it did not insert, usually the first commit's. An import of
  1,000 commits costs about one changeset statement per commit plus a
  handful per transaction, instead of four per commit. When the last
  per-commit writer of a commit moves into the batch, the changesets row
  moves with it, and every table writes at the transaction's end in the
  order the foreign keys read: changesets, changeset_parents,
  repo_changesets, derived, history, git_import.
- - The `CreateChangeset` write stops its blob checks at the call's
    deadline: the check re-hashes every distinct blob the changeset puts,
    so a big changeset would otherwise keep hashing after the client gave
    up. Nothing is written when the call is gone or its deadline passed,
    and the write's audit gets its one failed line.
- The import's derived rows are written in batches: the values a
  transaction inserted answer its own reads (the batch's derives chain
  through them, and a jump outside the batch costs one preloaded
  first-parent chain read per transaction, never one per commit), the
  flush writes one statement per 1,000 rows, and the older-version delete
  runs once per kind. The per-commit stored-value check waits for the
  batch: the transaction's changesets are new, so nothing stored can
  disagree. The flush's parents rows follow #540's numbering from zero,
  which the squashed #545 still numbered from one.
- The write path re-checks each blob's file inside its commit, so the
  collector's sweep cannot leave a stored changeset naming a file it took.
  PutBlob adds the repo's row on every store, not only when an earlier read
  said the row was there, so a reply of ok leaves the blob named.
- The collector's prune reads the bodies of a batch's unseen changesets
  in one statement per 1,000 ids, and deletes a batch's dead rows in one
  statement, where the per-item code read one body and deleted one row
  per statement: a repo of 100,000 changesets paid over 200,000
  statements a run on Postgres. The measured prune of 1,000 rows and
  1,000 prunable rows costs the same 17 statements as a repo of 100,
  where the per-item code cost 156 more (#520's batch shape, = ANY).
- The root-tree derivation reads a commit's directory trees in one
  statement per level of depth, where the per-directory code read one
  tree per statement: a commit that touches 100 stored directories paid
  101 tree reads on Postgres, one in 10 paid 11. The trees hand their
  entries over whole (Tree::into_entries), so a prefetched tree is no
  longer decoded and then cloned; the bytes are checked against their
  ids, as the single read does.
- The import and the restore write their git provenance in batches: one
  statement per 1,000 pairs on Postgres (= ANY over bytea arrays), where
  the per-commit code wrote one git_import row per statement - a
  100,000-commit import or restore paid 100,000 statements. The pairs of
  one import transaction leave in one call at the transaction's end, so
  a crash still loses whole commits, never a changeset without its
  provenance row, and a repeated pair is still skipped, not overwritten.
- A land's verdict is final only against the head it was planned on: the
  settle now re-reads the ref version inside its own transaction, and an
  import that moved the ref between the plan and the settle makes the land
  replan instead of recording a stale answer.
- The one-server lock of a Postgres store was one key for the whole
  database: a second server against the same database was refused, even
  when its URL named a different schema through `options`. The lock now
  keys on what the connection actually lands in — the open asks the
  database `current_schema()` — so a plain URL and one that rides
  `--search_path=public` fence each other (same schema, one lock), and
  two URLs that ride different schemas each fence their own. A URL that
  names a schema can now serve beside another server of the same
  database; that is a production behavior change, and the reason the
  tests of one database can hold one server each.
- - A data directory whose marker names a store refuses a database
    schema that holds no store, so a `--postgres-url` whose search_path
    names the wrong or a new schema fails at the start, naming the
    directory's store, instead of adopting its identity into an empty
    store that every later call reads as "repo not found". A schema that
    holds a store's rows but no store UUID row - a dump taken before the
    UUID existed - is still adopted.
- - The fault scripts hand `--postgres-url` to the server root command
    before the subcommand again: `serve`, `import`, `gc` and `fsck` take
    no such flag of their own, so the NFS mount job's Postgres run failed
    to start since the fault-matrix change.
- Postgres numbered a changeset's parents from one (WITH ORDINALITY
  starts at one) where SQLite numbers them from zero, so every
  first-parent read (`n = 0`) matched nothing on real Postgres data: a
  chain fetch walked one changeset and stopped, and the savings the
  first-parent chain fetch promises never happened on Postgres. The
  insert now shifts the ordinality down, so both backends put the first
  parent at n = 0, and a migration renumbers the one-based rows of an
  existing store (only the rows of a changeset whose smallest n is 1
  move, in two steps that never hold a duplicate of the primary key, and
  the migration changes nothing on a store that is already zero-based).
  The guard tests pin the numbering on both backends, chain a 300-deep
  ladder through the ordinary path, migrate a one-based store, and hold
  the bulk seed to the rows the ordinary path writes.
  
  Proof: crates/scion-server/tests/pg_parent_numbering.rs
- The tier job that decides what a pull request runs now starts the
  Postgres tests for any change under the server's `src/`, not only for
  the Postgres, metadata, backend and db files: the server's correctness
  depends on the Postgres transaction order. A test under
  `crates/scion-server/tests/` opts in when its text names the shared
  backend (`open_test_shared`) or the env var (`SCION_TEST_BACKEND`),
  instead of a hand list.
- The check that reads an extended ACL on macOS treated any
  `acl_get_entry` failure as the end of the walk, so a walk that failed
  partway answered "deny only" and the ACL passed.
  
  Found in the review of #345. The walk now decides each call with a
  pure function: -1 with `EINVAL` on a `ACL_NEXT_ENTRY` call is the
  walk's end (the macOS contract); any other failure refuses the path
  ("an ACL whose walk failed").
  
  macOS only; every other target keeps refusing group-writable paths.
- # Archive: a section's reads hold one snapshot
  
  An export's metadata sections claim one read snapshot, and on SQLite
  the claim was false: `Store::read` runs each statement autocommit, so
  a land that commits between a section's changesets query and its refs
  query left a ref in the archive whose target the archive does not
  hold, and the export failed its own verify. A section now reads inside
  one explicit read transaction on the reader connection
  (`Backend::read_snapshot`); Postgres already gave its reads one
  REPEATABLE READ transaction. The cost is the one the section's
  snapshot always named: the log cannot restart while the section reads.
- # A client's connection notices a dead peer
  
  ## The product clients run the server's keepalive, on their side
  
  `scn`'s client and a tier's upstream client now run HTTP/2 keepalive
  pings (every 30 s, an answer within 10 s, while the connection is busy
  or idle) and TCP keepalive probes (every 15 s), the mirror of the
  server's own settings. A peer that died without a FIN or RST — a
  frozen process, a host gone — ends every call of the client in about
  40 s, where a call's own deadline was the only bound (`scn`'s is
  600 s), and the tier's whole upstream window.
  
  ## Why
  
  A kill that the kernel does not turn into a FIN or RST leaves the
  connection half-dead: the socket is open and nothing answers. The
  server already closed its side of such connections; the clients now
  see the same thing from their side, and the values are named
  constants beside the server's own, so the pair stays a pair.
  
  A test that proves the dead-peer failure needs no 40 s of its own:
  the `test-hooks` feature of the protocol crate shortens the keepalive
  for the test's connection, the same failure is proved in seconds, and
  compile-time asserts pin the product constants at their documented
  values.
- # The git import refuses what git cannot write
  
  ## A date the source does not hold is not imported
  
  The parser behind a git signature reads a timezone it cannot
  understand as zero, so a commit with a broken timezone imported as a
  changeset whose date differs from its source without a word. The
  import now reads the raw date back through git's own grammar
  (`<seconds> <sign><hhmm>`) and refuses a commit whose date does not
  round-trip, with the commit and the raw line in the error. git's
  written form of an unknown offset, `-0000`, still imports.
  
  ## A commit or tree object past the cap is refused on its header
  
  Files were capped; the commit and tree objects the import decodes
  whole were not, so a crafted repository could hand the import a
  gigabyte message or a tree of millions of entries. Both are now
  refused from the object's header, before any decode, with the object
  and its size in the error: one commit or tree takes at most the byte
  cap a file takes, and one tree takes at most a million entries.
  
  ## Why
  
  The git import is a trust boundary: an operator imports someone
  else's repository, and its objects need not be what git itself would
  write. A field that does not read back as its source must be refused,
  and an object the import decodes in one piece must be bounded before
  the decode, not after.
- A metadata write the database refuses names its SQLSTATE, and the
  store answers by the code. A full disk is one of them and only one:
  53100, which errcode_for_file_access gives every ENOSPC the database
  hits, temp files included (elog.c). The mapping pins it to the fixed
  `ResourceExhausted` shortage reply the blob path already promises.
  
  The codes beside it stay internal on purpose, each with its reason
  and each pinned by a unit test so a later widening cannot widen
  silently: 53200 is out of memory, where a disk reply lies; 53000 is
  the class's catch-all, too vague to promise a disk answer; 53400 is
  a configured cap - a temp_file_limit or the replication slots - the
  same refusal every time, so a retry-later reply would tell the
  client to retry what cannot succeed; and 58030 is an io_error, a
  failing device, which is the read-only-disk row's internal.
- A Postgres operator met three numbers the manual never wrote: how
  many connections a server opens, what the advisory rows in `pg_locks`
  are, and how long the collector's key can hold a writer. The Postgres
  section now says all three, beside the one-server rule they enforce:
  
  - A server opens one write connection and 8 read connections, plus
    the short-lived connections of a migration or an import; the
    database's `max_connections` is sized for the server's 9, the
    operator's tooling beside it, and a margin, and a database at its
    cap answers `unavailable` until a connection is free.
  - The one-server lock, the migration lock and the collector's key
    (`hashtext(current_schema() || ':gc')`, a 60 s bound on that key
    alone) are named as the `advisory` rows an operator sees in
    `pg_locks` - held by this server by design, not a stuck client.
  
  The watch list's `scion_pg_pool_wait_seconds` was listed twice, the
  first copy without the 300 s/30 s bounds; the two are one entry now.
  
  The per-land round-trip budgets and the out-of-space SQLSTATEs are
  deliberately absent: their source PRs (#467, #480) are unmerged, and
  the numbers land with them.
- A first start after an unclean kill can exit with "another server
  holds this database": Postgres keeps the dead server's session, and
  with it the one-server advisory lock, until the session is ended. The
  Postgres section now says the recovery and both clocks that decide
  how long the lock stands: on the same host the kernel closes the dead
  process's sockets at once, so a service manager's restart
  (`Restart=on-failure`) is the whole recovery; a lost host is seen
  only through TCP keepalives, whose clock is the database's
  `tcp_keepalives_idle` (unset, the system default, often 2 hours), so
  the section names `pg_terminate_backend` and that clock for an
  operator who cannot wait it out.
- The release pipeline's dry run takes a commit SHA instead of a tag
  name, so no tag is ever pushed for one: the dispatch refuses a commit
  that is not an ancestor of main, the tag-identity verdicts moved to
  `scripts/release-verify-tag.sh` (fed canned tag objects by
  `scripts/test-release-verify-tag.sh`, since no CI run holds a signed
  release tag), and everything after the checkout is the same code path a
  real tag runs.
- A checkout now refuses a tree entry whose name turns into `.git` or
  `.scion` once the parts a disk folds are gone: trailing ASCII spaces and
  dots (NTFS drops them) and the code points HFS+ ignores anywhere
  (U+200C..U+200F, U+202A..U+202E, U+206A..U+206F, U+FEFF). Before, a
  malicious server's tree could smuggle a `.git ` directory past the
  reserved-name check into a checkout.
- This fixes the Scorecard job's four failing checks. Three fixes and
  two exceptions:
  
  - The Dockerfile pins both base images by their multi-arch index
    digest, with the tag in a comment: rust:1.88-bookworm and
    debian:bookworm-slim, the same way the workflows pin actions.
  - The three fuzz sub-workspace lockfiles (scion-proto, scion-server,
    scion-tier) pick up rustls-webpki 0.103.16, the fixed version for
    RUSTSEC-2026-0382; the root lockfile had it since #409, but
    Scorecard walks every lockfile and `cargo deny check` only reads the
    root workspace, so the fuzz locks kept the job red.
  - The policy's header no longer claims every floor is met today:
    Maintained (the repo is younger than 90 days) and Branch-Protection
    (the check needs a token with admin read) were never met, and stand
    as dated exceptions now, with reasons and expiries.
- The operations guide now shows every secret's deployment form first:
  the Postgres example gives the URL through --postgres-url-file (or
  SCION_POSTGRES_URL), not on the command line, and the flags and
  environment section states the rule the flags follow - a secret has a
  file form the owner alone may read, an environment form visible to
  the same user and root, and an argv form that
  is deprecated: it warns now, goes away in a later release, and ps
  shows its value to every local account. No flag changes.
- The snapshot file name of a repo whose name holds ASCII capitals
  changes: `MAIN` gave `MAIN.scnsnap`, and now gives
  `%4D%41%49%4E.scnsnap`. On a case-insensitive disk (the macOS
  default), two repos whose names differ only in case wrote their
  snapshots to one file, and the second write silently replaced the
  first; with the capitals escaped, the output holds lowercase literal
  letters only, and no fold can put two names on one file. Nothing reads a snapshot file
  by its generated name — `scion-daemon` seeds only from an explicit
  `--seed FILE`, and the cache does not reload its own snapshot at
  start — so an upgrade makes a repo's next snapshot appear under the
  new name; the old file goes cold once and can be deleted.
- The version matrix always has an N-1, from the first nightly: `PREV_REF`
  beats the release tags, the newest tag that is an ancestor of HEAD and
  not HEAD itself wins next, and with neither, the newest of the stand-in
  ladder - the two pinned commits that
  [version-matrix.sh](scripts/version-matrix.sh) holds, chosen where
  the store schema changed - stands in until the first release tag
  replaces them. A run that tests no pair fails with the reason; the
  matrix never passes as a skip. Two windows of the N and N-1 pair are
  proved, both
  directions: the ticket one version's server signs verifies on the other
  version's tier, and a ticket of another key is refused with the ticket
  reason; the snapshot one version's cache writes opens as the other
  version's daemon seed, and a checkout through that daemon reads the
  repo whole. The tier pair's read is a checkout, not a `cat`: `cat`
  reads at the server and never reaches the tier, so only a checkout
  exercises the GetObjects the ticket gates.
- The mount is read-only by default, and `scn commit --from-mount` refuses
  a read-only mount: a symlinked parent directory in the overlay can still
  redirect a write outside it, so the writes and the commit live behind
  `scn mount --unstable-writable` alone. The read-only mount answers every
  NFS write with `EROFS`, and the refusal names the flag.
- scion-core's IdentityTime, Changeset and Tree hide their fields
    behind getters and build only through checked constructors, so a
    caller cannot build a value the encoder cannot encode: a field or
    count above u32::MAX is refused at the boundary with a new
    EncodeError::FieldTooLong instead of panicking inside the encoder.
    The encoded bytes and the IDs are unchanged (#324).
- # The macOS deny-only ACL is read through, and the refusal names its entry (#399)
  
  ## Fixed
  
  The check for extended ACLs on macOS (PR #369) read only the *first* ACL
  entry: a descriptor carrying the ambient home-folder ACL —
  `group:everyone deny delete`, one entry — was refused as if it granted.
  Every macOS home folder carries that shape, so the server refused any
  data directory under `$HOME` (issue #399, a real Mac).
  
  The walk now reads **every** entry (`acl_get_entry`, first to last) and
  types each one (`acl_get_tag_type`):
  
  - every entry deny-only → **accepted**: a deny entry grants nobody
    anything, so the mode bits stay the whole story;
  - any allow entry, unknown tag, unreadable entry, or ACL that cannot be
    read or released → **refused**.
  
  The refusal names the entry that granted ("an allow entry for account
  501 grants access") and the platform's fix tool: macOS `chmod -N`,
  Linux `setfacl -b` (unchanged).
  
  Tests (macOS, each able to fail, proven red against the previous code
  first): a file under a `chmod +a "everyone deny delete"` directory is
  read; a file under a parent with a deny-only ACL passes
  `check_ancestors`; an `everyone allow read` entry is refused and the
  refusal names `chmod -N`.
  
  The claim of PR #369 — "deny-only ACLs pass" — is now true.
  Closes #399. Exposed by #345.
- Job starts are staggered 10 ms apart so a burst cannot overflow a server's
  accept queue, a run with SCION_KEEP_LOGS set keeps every server and batch
  log plus the nstat listen-overflow counters around every batch instead of
  losing them with the temp directory, and the stagger carries a SHORTCUT note
  naming its ceiling and its upgrade path.
- A call whose client went now answers cancelled, not deadline_exceeded.
  A job that outlives its call checks whether the call has gone: it stopped
  because the caller went, so the reply maps to Status::cancelled, and only
  a call that ran past its deadline reads deadline_exceeded. A land whose
  call went stays pending, and its counter says cancelled, not internal.
- The docs that walk an operator through making the files the server reads
  said nothing about the file modes the reads now check: the README's
  access-file section showed the format but not the mode, the README's and
  operations' TLS sections told the operator to make certificates with a
  tool and started the server on them, and the Postgres section named the
  CA file without its rule. An operator following them under umask 002 —
  a default Ubuntu account — wrote an access file or a certificate the
  server then refused at start. Each place now states the rule where the
  file is first made, with the fix: a secret (the access file, the TLS
  private keys) is read when its owner alone can read it, chmod 600; a
  file that must stay whole (the TLS certificate, the CAs of --tls-ca,
  --tls-client-ca and --postgres-ca) must not be group- or others-
  writable, chmod go-w, and a copy tool or a umask-002 session can leave
  0664 behind.
- The changelog merge script refused every section that held more than
  one fragment: its file-name check read the newlines between the paths
  of the fragment list as whitespace, so a release whose fragments share
  a section stopped at the merge. The check now runs on each fragment's
  own path, and the merge script's fixture test holds two fragments in
  one section, so the refusal cannot come back. Two changelog rows read
  wrong: the release-smoke rows carried backticks around their issue
  numbers and the older row said macOS stays out of CI for the job,
  which the newer row made untrue; the stability row and the policies
  row both introduced docs/stability.md, so the policies row now leads
  with the toolchain floor; the backup row dropped its commit-style
  prefix.
- A quota test no longer depends on the runner's TCP buffer sizes: its
  unread reply is 64 MiB, about 4x the pipeline a probe must lose to, so
  the slot is still held when the test asserts it.
- The README's claims now match the repo. The fuzz command makes the
  corpus directory that cargo-fuzz refuses to start without. The import
  and lands numbers link docs/benchmarks.md instead of quoting one
  machine's run. The CI list carries the two release smoke jobs and calls
  the disk-full and dm-log-writes runs what they are, steps of `test` (15
  checks, not 13). The contributing section points a private security
  report at SECURITY.md.
- A plain cargo build --release printed about 15 warnings in scion-server:
  unused imports, an unused batch index, and dead test helpers. The users
  of every one of them sit behind cfg(any(test, feature = "test-hooks"))
  while the items themselves did not, and CI never saw it, because the
  workspace clippy with --all-targets unifies the dev-dependency that
  turns the feature on. Each item now carries the same cfg as its users,
  with no allow attribute; the release build of the SyncStep payloads
  rides a unit-typed field instead. The fmt + clippy job gains
  cargo clippy --workspace --lib --bins -- -D warnings, which builds the
  libraries and binaries without dev-dependencies, so the next ungated
  test-only item fails CI instead of a release build's stderr.
- A dry run of the release walk fixed the guide where it read wrong: the
  milestone check passes while the release's own tracking issue is open,
  the unreleased note's fate on release day is stated, the version step
  says the first release checks the five versions equal instead of
  bumping them, the release notes name the command that extracts the
  version's changelog section alone, and the tag check runs locally
  before the push, not only on the remote after it.
- scn no longer panics when the reader of its stdout leaves: a pipeline
  like `scn blame main:README.md | head -1` closed the pipe at the first
  line, and the next print panicked with "failed printing to stdout".
  Every stdout write of scn now goes through one writer, and a broken
  pipe ends the command with status 0 and no message — a closed stdout
  is a normal end for a CLI, the reader chose to stop, and with
  `set -o pipefail` a 141 exit would fail every benign `| head`. Any
  other stdout error still fails the command. A test pipes `scn blame`
  into a reader that leaves after one line and asserts the quiet exit.
  The release smoke reads scn's output from files instead of pipes, so
  its checks no longer race against a reader that leaves mid-write.
- The soak scripts ran on a default Mac and failed at once: `timeout`
  does not exist there (Homebrew installs it as gtimeout) and nothing
  checked, so every client, import, gc and fsck call died with "timeout:
  command not found" and the writers landed 0 (issue #399, Run A). The
  scripts now resolve TIMEOUT once in common.sh - timeout, else
  gtimeout, else a named error - and call it everywhere, including the
  NFS mount script.
  
  The slow client never held a stream on any shell: its drain used
  `read -N`, which exists in neither dash (Ubuntu's /bin/sh) nor bash
  3.2 (macOS's /bin/sh), and the error vanished behind `|| true`. The
  drain is now dd, one 4 KiB block at a time with a pause between -
  portable across dash, bash 3.2 and Linux bash - shared by both soak
  scripts in common.sh, and each drain logs bytes read and seconds
  held. A drain that reads fewer bytes than the file has and does not
  end inside a kill window fails the run: the reader quit while the
  server was still serving. The old reader makes that check red, which
  is the proof it works.
- The nightly soak failed its own RSS bound on 2026-10-19 (run
  39219458903, the schedule event on 2bc9099): 34892 KB -> 54140 KB, over
  the 150 percent ceiling. At that commit the schedule branch of the
  workflow still fell back to the 10-minute default, so the nightly ran
  its planned 10 minutes and failed the check at the end, against a
  baseline taken after 60 seconds; the 120-minute schedule came later on
  main. There was no early abort: the only RSS check in the script runs
  after the sampling loop, and the fixed script's only RSS check needs
  the warm baseline, which exists only after SOAK_WARM_SECONDS. A 35-minute local run of the same load saw RSS climb for about
  30 minutes (32288 -> 59184 KB) and then hold flat for the last 16
  minutes (the last 20 samples: max 59184, average 58137, no trend), and
  the failed CI curve itself rose, stepped once at about 4 minutes, and
  then oscillated 49-57 KB for its last 5 minutes with no climb. That is
  warm-up - sqlite page caches of the 8 read connections and the write
  connection, allocator arenas, prepared statements - not a leak: a leak
  does not plateau on two runs.
  
  The bound's baseline is now the first sample after SOAK_WARM_SECONDS
  (1800 s, named in soak.yml): the nightly still checks 90 minutes of RSS
  after the warm-up. A run shorter than the window checks no RSS bound -
  it cannot tell warm-up from a leak (a 10-minute run, as the nightly of
  that commit was, sees only the climb). The other bounds are
  untouched.
  
  The samples now include the store's size on disk: every 10 s the loop
  appends epoch and `du -sk` of the server data directory to a
  metrics-store.csv, next to metrics.csv in the artifacts, so a run can
  be read for disk growth and for Ben's 72-hour soak decision, not only
  for the RSS it shares a cause with.
- A streamed reply that stalled on a client that stopped reading now gives
  its byte estimate back when its stream ends, instead of holding it until
  the connection closes. A connection that serves many stalled replies no
  longer ratchets toward its stalled-bytes close limit for bytes it no
  longer holds. The new scion_stalled_reply_bytes gauge shows what the
  stalled replies of the process hold now.
- The cache's serving line now names the address the OS bound, not the
  one the flag asked for: with `--listen 127.0.0.1:0` the log shows the
  real port, so a starter can read the line and connect, the way the
  release smoke parses the server's serving line.
- The cache and the daemon log the call's request ID on their GetObjects
  line, the way the server's call lines do: one command is followable
  through every hop's log, which the operations guide and the README
  promise. The reply runs outside the call, so it now stays in the call's
  span, as the server's replies do. A test sends one ID through a daemon
  and a cache and finds it in both logs and on the fetch upstream.
- The version matrix's dispatch could pass green without testing a pair:
  the workflow resolved the N-1 ref in one step, and the step that ran
  the matrix never received it, so the script resolved again, found no
  release tag, and printed its skip. The run step gets the resolved ref
  now, and the script fails when the resolve step left its ref file but
  the run received no ref, so a wiring slip can never be green again.
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

- The access file of `--access` holds bearer tokens and rules, and was
  read by plain path: no check of the directories above it, no symlink
  refusal, no owner or mode check, so a file another user holds - and can
  replace at will - was taken as the access policy. The TLS private keys
  of `--tls-key` (scion-server) and `--tls-client-key` (scion-cache,
  scion-daemon) were read by plain path too. All of them are read through
  one checked descriptor now, the read the ticket keys gained in #354,
  moved into `scion_proto::runtime::auth` so every binary shares it: the
  directories above are checked, the file is opened once with
  `O_NOFOLLOW`, and kind, owner (the account that runs the process or
  root), and mode run on the descriptor the bytes are read from - a
  secret is read by its owner alone. The ticket keys keep their
  public-key rule (no group or others write) through the same core. A
  symlinked data directory is refused by kind now, not by the link's
  mode: on macOS lchmod can give a symlink any mode, so the mode check
  alone did not catch one. The ELOOP message names a link at the file
  precisely and a loop on the way as what it is.
  
  Files the binaries read at start that were checked and left as they
  are, with why: the TLS certificate and client-CA files (public
  material, but not nothing: a swapped client CA would change whose
  certificates the server accepts, so they stay a listed gap for a
  follow-up decision), the `scn --put` content file (the caller's own
  content, kind-checked already), and the `scn` mount session file (not
  a secret).
- The check that keeps another user from swapping a path on its way had
  two gaps. The sticky bit excused a group-writable directory as it
  excuses the /tmp shape, so 01770 and 02770 passed: a sticky bit bounds
  who may remove a name, not who may add one, so a group that can write
  the directory can still plant and swap its own entries. A
  group-writable ancestor is refused now whatever the sticky bit does;
  the sticky bit keeps its one excuse, the shape where others can write
  too (01777), where the others-write rule is the real wall. And the
  checks read only the mode bits, not POSIX ACLs, so an access ACL that
  grants another account what the mode does not passed everywhere: now
  the checked read of a secret or a shared file refuses one whose access
  ACL is not the exact minimal form (version 2, one USER_OBJ, one
  GROUP_OBJ, one OTHER, undefined ids, perms equal to the mode's bits),
  an ancestor directory with such an ACL or with any default ACL is
  refused on the walk, and an ACL that cannot be read is refused with the
  rest. On macOS the descriptor's extended ACL is read through one private
  ffi module (acl_get_fd_np, the one place the workspace's unsafe-code
  lint lets hold it): a descriptor with no ACL passes, one or more entries
  refuses, a read that fails refuses. The workspace's unsafe_code lint
  drops from forbid to deny, with the lint forbid at every crate root but
  this one, and a test walks the crate's sources and refuses unsafe code
  outside the ffi module. On this Mac the temp directories carry no ACLs
  and the ambient deny-only ACLs of the system pass, as they grant
  nothing; an ACL that grants anything is refused.
- A ticket key was checked by path and then read through the same
  unresolved path again: a directory on the checked path that another
  user could change let them swap the file between the two steps, and a
  key file owned by another user passed the checks at all, so a public
  key they hold could be fed in as the old key of a rotation. The key
  now opens once, through the path the ancestor check resolved and with
  a symlink at the end refused, and the kind, owner (the user that runs
  the server or root), and mode checks run on the open descriptor that
  the key is read from. A symlinked data directory is refused by kind
  now: a symlink's mode is not a rule (on macOS lchmod can set any mode
  on one), so the mode check alone did not catch it.
- The files a tier loads that must stay whole — the TLS certificate and
  the client CA of a server, the CA, client certificate, and seed
  snapshots of a cache or daemon — were read by path with no check: a
  symlink, a file another user owns, or a mode that lets another user
  rewrite the file all passed. A swapped certificate lets someone else
  be the server, a swapped CA changes whose client certificates the
  server accepts, and a swapped seed can seed what the daemon serves.
  These reads go through one checked descriptor now (read_shared_file:
  ancestors checked, O_NOFOLLOW, kind, owner, and a mode no other
  account can write, checked on the open descriptor the bytes are read
  from), as the secrets already did. The checked open is its own step
  now, so a seed the size of a disk streams through a buffered reader
  instead of being held whole in memory.
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
