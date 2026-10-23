The fault matrix runs its disk, power and mount rows on Postgres beside
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
