The failing-disk runbook (docs/operations.md, "A failing disk"): a read
the disk refuses answers internal and the rest keeps serving; fsck runs
beside the live server and its exit 1 names the damage; the store has no
repair, so the fix is the restore of section 3, and fsck runs on the
restored copy before the server starts. The Proof test runs those steps:
it takes a backup per section 3's order, fails a read with EIO, corrupts
a blob file behind the store's back, runs the fsck command to its exit 1
with the blob named, restores into a fresh directory, and reads back
through a fresh open.
