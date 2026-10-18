# Faults and what the server does about them

One row per fault: what the client sees, what the operator sees, what
survives a restart, and the test on `main` that proves it. A row states
only what a test proves; rows whose proof is still in review say
`open` and name the PR.

The matrix:

| Fault | Client sees | Operator sees | Survives a restart | Proven by |
|---|---|---|---|---|
| Wrong data directory for the database (a dump of store A restored beside directory B) | open fails before serving; the error names `UUID` and both stores' UUIDs | nothing written by the refusal; the matching directory still opens | yes, nothing changed | `backend_refusals.rs::a_database_refuses_the_directory_of_another_store` |
| SQLite open of a Postgres-backed directory | open refused; the message names `--postgres-url` | the marker file named Postgres first, so an empty SQLite store can never look like a lost server | yes | `backend_refusals.rs::a_sqlite_open_of_a_postgres_backed_directory_is_refused`, `backend_refusals.rs::a_postgres_open_makes_the_data_directory_it_names` |
| Another server process took the database (advisory lock lost) | every read and write answers `FailedPrecondition`; no row of the other process is served | the fence trips, the server logs and exits (code 70); a supervisor restart retakes the lock or is refused at open | yes | `backend_refusals.rs::a_lost_server_lock_fences_the_backend` |
| Database unreachable (stopped, not taken over) | writes answer `Busy` | no fence: an unreachable database is no proof of a takeover | yes; the next write reconnects, retakes the lock, and runs without a process restart | `backend_refusals.rs::an_unreachable_database_is_not_a_takeover` |
| Another writer holds the write lock | `Busy` in seconds (a lock timeout, not the minute-long statement clock) | nothing; the client retries | yes | `backend_refusals.rs::a_write_blocked_by_another_writer_answers_busy_in_seconds` |
| Server killed mid-write (SIGKILL) | every in-flight reply either landed or failed; no client sees a half reply | the log holds the kills; the index and the files agree after the restart | yes; a restart serves the landed replies and the ledger holds | `crash.rs::every_reply_survives_a_kill_of_the_server` |
| Kill during a schema upgrade | - | the store is on the old schema or the new one, never half-migrated | yes | `crash.rs::a_kill_during_a_schema_upgrade_leaves_the_old_schema_or_the_new_one` |
| Tier daemon killed while it fills its store | no torn object is ever served | the index and the files agree after the restart; the byte limit still holds; reads go on | yes | `scion-tier/tests/tier.rs::a_daemon_killed_mid_fill_serves_no_torn_object_after_its_restart` |
| Blob write hits a full disk (`ENOSPC`) | `ResourceExhausted` with the fixed reply "the server is out of disk space; retry later" (no path in it) | no torn blob file and no temp file left; reads and later writes work once space returns | yes | `storage.rs::a_put_the_disk_refuses_answers_out_of_space_and_leaves_no_torn_file`, `error.rs::an_out_of_space_write_is_resource_exhausted_with_a_fixed_reply` |
| Metadata write must grow the database file; the disk is full (SQLite) | `ResourceExhausted`, same fixed reply | the transaction rolled back; reads work; the same write works once space is back | yes | `storage.rs::a_metadata_write_that_must_grow_the_file_answers_out_of_space` |
| One file write of a batch fails | the refused blob answers the error | the earlier files of the batch stay intact (no row names them yet, so gc may reclaim them later) | yes | `storage.rs::a_batch_that_loses_a_file_keeps_its_earlier_files` |
| A blob file was deleted under the server (corruption) | reads of it fail | `fsck` finds the missing files and lists them | yes, after repair | `backend_refusals.rs::a_fsck_of_a_postgres_backed_store_finds_missing_blob_files` |
| Serve through a real kernel NFS mount | reads, writes, a rename and a delete through the mount work; a commit from the mount diffs; a tampered mount path is refused | the CI job `mount (NFS)` runs the whole flow under `timeout` | yes | CI job `mount (NFS)`, `scripts/mount-nfs.sh` (#243) |
| Power cut while clients write | every reply lands or fails; the server restarts clean | the cut is a real lazyfs cache drop; runs in the CI job `power-loss` (needs `SCION_LAZYFS`) | yes | `power.rs::every_reply_survives_a_power_cut`; the durability-order refinements are open: #225, #261 |
| A disk that fails mid-write or went read-only (`EIO`, `EROFS`) | a write on a read-only disk answers `Unavailable`, "the disk is read-only; retry later"; a read or fsync that the device refuses (`EIO`) answers `Internal` | no torn blob file and no temp file left; the log line carries the path and the operation; a retry that finds the stored file intact fsyncs it, file and directory, before it answers | yes | `storage.rs::a_write_on_a_read_only_disk_answers_unavailable_and_leaves_no_torn_file`, `storage.rs::a_read_on_a_failing_disk_answers_unavailable_and_keeps_serving_the_rest`, `storage.rs::a_file_fsync_the_disk_refuses_answers_internal_and_leaves_no_torn_file`, `storage.rs::a_retry_after_a_failed_dir_fsync_makes_the_kept_file_durable` |
| The whole server on a real full disk (tmpfs, not injected faults) | open - #263 | | | #263 |

Notes:

- The fence rows are Postgres-only. SQLite has no cross-process lock to
  lose in this way; its crash story is the kill rows.
- The out-of-space rows inject the fault through the test hooks
  (`fail_writes_after`, a capped metadata file). #263 adds a real tmpfs
  test beside them; the injected tests stay. #262 adds the faults a
  disk fails with that are not "no space".
- The power-cut row needs LazyFS, so it skips where `SCION_LAZYFS` is
  unset; the CI job sets it.
