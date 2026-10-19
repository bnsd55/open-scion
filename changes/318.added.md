A store of the previous schema version opens on a new build: the open
migrates it forward and serves what it held. A test builds a version-1
store of each backend in place (the SQLite schema of migration 0001, a
Postgres schema of migrations/postgres/0001, each with a repo, a root
changeset, its ref and two blob files this build wrote), opens it the
normal way, and checks the migration level after the open, the ref,
the changeset, both blobs byte for byte, a file read through the tree
the upgrade rebuilt, and a clean fsck (#318).
