The collector's prune reads the bodies of a batch's unseen changesets
in one statement per 1,000 ids, and deletes a batch's dead rows in one
statement, where the per-item code read one body and deleted one row
per statement: a repo of 100,000 changesets paid over 200,000
statements a run on Postgres. The measured prune of 1,000 rows and
1,000 prunable rows costs the same 17 statements as a repo of 100,
where the per-item code cost 156 more (#520's batch shape, = ANY).
