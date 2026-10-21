# Wave 2: the durability point's backup proofs run the guide's commands

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
