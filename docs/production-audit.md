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
| One unary reply can aggregate a whole directory or changeset past the per-object memory budget (`whole_share` charged by encoded size, the aggregate uncapped) | `crates/scion-server/src/service.rs:485` (`whole_share_of`), `:1822` (the changeset's length-only query), `:1986` (`tree_size`) | fixed (#586): the share is taken from a length-only query before the object is built, an object over `MAX_OBJECT_BYTES` is refused before any byte is read, and the share is held for the whole reply (`stream_whole_holding`); the guard is `tests/reply_budget_h2.rs` (`a_client_that_resets_its_unread_replies_frees_the_budget`, which asserts the budget binds at `bodies.len() < 40` and the served reply is whole) |
| Slow clients can pin large unary replies outside the memory accounting | `crates/scion-server/src/reply.rs` (`REPLY_MEMORY_BUDGET`), `crates/scion-server/src/metrics.rs` (`CountingBody`) | fixed (#586): the reply-memory layer counts every in-flight reply's bytes and refuses new unary replies past the 128 MiB budget; a client that resets its unread replies frees the budget (the guard is the same `tests/reply_budget_h2.rs` test) |
| `gc` prune reads and deletes in batches of 1,000 rows | `crates/scion-server/src/gc.rs` (`prune_rows`) | fixed (#529 batched the reads and the deletes; the guard is `tests/gc_prune_batch.rs`) |
| `scn commit` reads a regular file whole into RAM: the size limit is "whatever fits" | `crates/scn/src/cmd/commit.rs` | open |
| `scn` JSON output silently replaces non-UTF-8 bytes | `crates/scn/src/client.rs:328` (`lossy`), `:336` (`put_json_bytes`) | fixed (#576): the lossy text stays, and every non-UTF-8 name or message carries the exact bytes in a sibling `<field>_b64` (standard base64) so scripts can round-trip them; the guard is `crates/scn/tests/json_raw_bytes.rs` (`assert_b64` and the seed suites) |
| Import batches are time-based per transaction and count-based for blobs | `crates/scion-server/src/import.rs:52` (`TX_BUDGET = 1 s`), `store.rs` (`BlobBatch`) | bounded — ceiling: 1 s of work or one commit per import transaction; `docs/benchmarks.md` records the measured commit size and rate the batches were tuned at |

## 4. Database loops

| Finding | Evidence | Status |
| --- | --- | --- |
| The Postgres writer pings the database with an autocommit `SELECT 1` while holding the writer mutex | `crates/scion-server/src/pg.rs:905` | bounded — ceiling: 1 statement per writer-idle probe (not per call); the probe's role is stated in `operations.md`'s connection-cap paragraph (the server holds 9 connections) |
| The sweep below ran over both backends. No production site pays a statement per repository item over the wire: Postgres chunks every multi-row write (1,000 rows per `unnest` insert) or answers one `ANY($1)` statement per list, every paged read is a keyset page with a matching `ORDER BY`, and SQLite's per-item loops are local prepared statements on its one connection (no round trips), which the code names as its batch at `db.rs:276,876,1181,1372` | `db.rs:228,278,875,1193,1346,1384,1518`; `pg.rs:1330,2401,2573,2634,2979,3313,3599,3710-3910`; `fsck.rs:240,300`; `land.rs:1041,1170`; `import.rs:236` | fixed by design (chunked or local; no REPO-SIZED site) |

### The sweep, per site

| Site | The loop grows with | Statements per iteration | Verdict |
| --- | --- | --- | --- |
| `db.rs:228` `insert_changeset`: per-parent generation read, then the per-parent `changeset_parents` inserts | the parents of one changeset | 2 statements per parent, local | BATCHED — a client commit has exactly one parent (`write.rs:278` refuses otherwise); only git import carries merges, and it runs ≤ 1,000 commits per transaction (`import.rs:44,236`), all local |
| `db.rs:278` `repo_missing_blobs`: per-blob existence read | the blob list of one upload request | 1 local SELECT per blob | BATCHED — local (the comment at `db.rs:276` names it); Postgres answers one `ANY($1)` statement per 1,000 (`pg.rs:2573`) |
| `db.rs:875` `get_trees`: per-id body read | the ids of one request | 1 local SELECT per id | BATCHED — local; Postgres chunks at `meta::DERIVED_BATCH` (`pg.rs:3313`) |
| `db.rs:1193` `put_git_imports`: per-pair insert | the pairs of one import batch | 1 local INSERT per pair | BATCHED — local; Postgres inserts one `unnest` per batch (`pg.rs:3599`) |
| `db.rs:1346` `delete_repo_blobs`: per-blob delete | the blobs gc removes | 1 local DELETE per blob | BATCHED — local; Postgres answers one `ANY($2)` statement (`pg.rs:3809`) |
| `db.rs:1384` `changeset_bodies_after`/`tree_bodies_after`, `derived_rows_after`; `pg.rs:3710-3910` the same readers | the whole table | 1 statement per ≤ 1,000-row keyset page | BATCHED — keyset pagination, each page ordered by its key |
| `db.rs:1518` `lands_with_missing_changesets` | two fixed columns | 2 statements | BOUNDED (2) |
| `fsck.rs:240,300` the check page loops | every changeset and tree | 1 statement per page | BATCHED |
| `land.rs:1041,1170` the path index walks (`first_parent_page`) | the repo's history length | 1 statement per 1,000-change page | BATCHED — the page is one recursive CTE with `ORDER BY walk.depth` |
| `pg.rs:1330,2401` the change flushes (`flush_history_rows`, `flush_changeset_rows`) | the transaction's pending rows | 1 `unnest` insert per 1,000 rows | BATCHED |
| `pg.rs:2283` the per-changeset parent-generation read | the parents the transaction did not insert | 1 statement with `= ANY($2)` | BATCHED |
| `pg.rs:2634,2979` `first_parent_page`/`get_derived_chain` recursive CTEs | the history the walk covers | 1 statement per page | BATCHED |

### Queries a caller's order depends on

Every paged read and every recursive CTE carries the `ORDER BY` its
keyset or walk needs (`db.rs:354,649,1362,1415,1432,1553`;
`pg.rs:2646,2994,3710,3734,3742,3757,3768,3800,3844,3861,3874,3890,3951`).
Three queries lack `ORDER BY`, and no caller depends on their order —
each caller answers per row and the reply is a set:

- `dangling_refs` (`pg.rs:3912`, `db.rs:1510`) — fsck reports each row.
- `stale_lands` (`pg.rs:3462`) — the janitor rejects each land by id.
- `lands_with_missing_changesets` (`db.rs:1518`, `pg.rs:3929`) — fsck
  reports each row.

### Postgres isolation, per transaction

| Transaction | Level | Read-then-write paths at that level | Lost update? |
| --- | --- | --- | --- |
| Writes (`pg.rs:1355`, plain `BEGIN`) | READ COMMITTED (Postgres's default) — one in-process writer mutex, and every write transaction of the schema holds the schema's one advisory key until it commits, so two writers serialize even across processes | `cas_ref` keeps its version predicate inside the UPDATE; `record_land`'s count-then-insert runs under the advisory key (`pg.rs:3407`); `settle` re-reads the ref and writes only under the version it read (`land.rs:810`); `put_derived`'s agree-check reads and writes in one writer-serialized transaction | No — `two_lands_that_record_together_respect_the_pending_cap` (`pg.rs:4735`) proves the count, `a_conflict_recorded_while_an_import_moved_the_ref_is_not_final` (`land.rs:1982`, run on both backends) proves the verdict's CAS window |
| Reads (`pg.rs:1389`) | REPEATABLE READ, READ ONLY — one snapshot per read, with `SET LOCAL` statement and idle timeouts | none (read-only) | No |
| SQLite | one writer; plain reads run per-statement autocommit on pooled readers, `read_snapshot` holds one explicit read transaction (`store.rs:494`) | the same CAS and quota shapes, one writer | No |
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
| The default data directory is `scion-data` relative to the server's working directory | `crates/scion-server/src/main.rs:29` | fixed (D25): a missing data directory is refused, not created — every subcommand except restore answers "<path> does not exist; create it or pass --data" (`main.rs`, `refuse_missing_data_dir`); fsck's refusal exits 2, its "could not run" code; `tests/refuse_data_dir.rs` holds every subcommand to the refusal and asserts nothing is created |
| Secrets on argv: `--postgres-url` is deprecated and warns; `--postgres-url-file` and `SCION_POSTGRES_URL` are the supported forms | `crates/scion-server/src/main.rs:33-43` | fixed (the warning ships; the removal is on the deprecation clock) |
| Ticket and seed files are 0600; the access file refuses group/other writable and symlinks | `crates/scion-server/src/main.rs:1333,1433,1493` | bounded — ceiling: 0600 on every secret file, the access file refuses go-w and symlinks at the open; `operations.md` "The open checks" states the refusal |
| Listeners default to loopback | `crates/scion-server/src/main.rs:59` (`default_value = "127.0.0.1:7878"`) | bounded — ceiling: the default bind is 127.0.0.1:7878; `docs/operations.md`'s serve examples restate it |
| Production state in temp dirs: the server never writes store state outside `--data`; every temp-dir user is a harness script | `crates/scion-server/src/main.rs:29` (the only store path) | fixed by design (the data dir is the only durable state; `TMPDIR` appears in no serve path) |

## 7. Compatibility

| Finding | Evidence | Status |
| --- | --- | --- |
| The store format, the protocol and the archive format are versioned documents; the unknown-field policy is stated per format, and each format's goldens enforce it | `docs/format-v1.md`, `docs/protocol-v1.md`, `docs/archive-format.md`, `docs/compatibility.md` | fixed — the table below |

### The contract goldens

| Format | The golden | The additive rule the test enforces | The doc pairing |
| --- | --- | --- | --- |
| Store (`docs/format-v1.md`) | `crates/scion-core/tests/golden.rs`: expected bytes hand-assembled from the grammar, independent of the encoder | an unknown tag, version, flag or git-algo byte is refused with the named error (`rejects_bad_tags_flags_and_algos`); red-proven: a decoder that accepts unknown tags fails it | the lint watches `golden.rs`: a change without a version-line move fails |
| Protocol (`docs/protocol-v1.md`) | `crates/scion-proto/tests/wire_golden.rs` + `protocol_wire_v1.golden`: the wire bytes of three fixed messages, hand-assembled from the proto3 grammar; the encoder must write exactly them and the golden must read back to the same values | unknown fields (varint 999, length-delimited 998) appended to every golden message decode with the known values unchanged (the doc's MUST-ignore); red-proven: one flipped golden byte fails the encoder test | the lint watches `protocol_wire_v1.golden`, `protocol_surface_v1.golden` and `proto/scion.proto` |
| Archive (`docs/archive-format.md`) | `crates/scion-server/tests/archive_golden.rs` + `archive_v1.golden`: one export's files and normalized manifest, byte-for-byte, on both backends | a manifest with an unknown version or format is refused with the named error before any write (`an_archive_of_an_unknown_version_or_format_is_refused`); red-proven: a reader that accepts version 2 restores the archive and fails the test | the lint watches `archive_v1.golden` and `archive_golden.rs` |

The pairing is enforced on the diff: `scripts/check-golden-pairing.sh`
(the CI lint job, on the pull request's own `HEAD^1..HEAD`). Pinned
bytes (the wire golden, the store pins, the archive golden) fail
without their format document's version line in the same pull request.
`proto/scion.proto` and the surface golden are additive - they need no
new version (the doc's additive table) - so they need only the same
pull request changing `docs/protocol-v1.md`, any line: the contract
must describe the new surface. A golden file the pull request adds is
its initial pin: the in-tree tests hold that first pin against the
doc, and the lint holds every later change.
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
| Service containers carry version tags, not mutable `latest` | `ci.yml` (the matrix `postgres:16.15/17.11/18.6`, each `@sha256:`-pinned in its include entry), `ci.yml` and `bench.yml` (`postgres:17.11@sha256:...`) | fixed — every services image pins its tag's multi-arch index; `scripts/lint-workflows.sh` fails a services `image:` line without `@sha256:` |
| Artifacts are built from the protected release tag only | `docs/RELEASING.md`, `docs/verify.md`, `docs/slsa.md` | fixed by design (the release workflow builds on the tag; the provenance doc states the SLSA level) |

## The open list (the v1.0 blockers this audit leaves)

1. The `--unstable-writable` write path out of the release binary —
   open, closed by C54 (§1).
2. The archive restore's repository-sized RAM hold (§2), and the C60
   Postgres lock-restart runbook + whether `serve` should wait (§2).
3. `scn commit` whole-file reads (§3), open until #582; the unary
   reply caps and `lossy` JSON closed (#586, #576).
4. The database-loop sweep: SQL in loops, `ORDER BY`, isolation (§4).
6. The data-dir default create-vs-refuse question (§6).
7. The additive contract goldens that enforce the format policies (§7).
8. Digest pins for the service-container tags (§9).
