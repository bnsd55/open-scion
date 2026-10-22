The import's derived rows are written in batches: the values a
transaction inserted answer its own reads (the batch's derives chain
through them, and a jump outside the batch costs one preloaded
first-parent chain read per transaction, never one per commit), the
flush writes one statement per 1,000 rows, and the older-version delete
runs once per kind. The per-commit stored-value check waits for the
batch: the transaction's changesets are new, so nothing stored can
disagree. An import of 1,000 commits costs about six derived statements
per transaction instead of ten per commit. The parents' row numbers in
`changeset_parents` now count from zero, as the first-parent readers
count; the write path counted from one, so the first-parent chain walk
of #533 found nothing on imported data.
