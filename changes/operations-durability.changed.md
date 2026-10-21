The runbook states what an acknowledged write has survived - the
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
