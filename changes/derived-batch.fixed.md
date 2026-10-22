The import's derived rows are written in batches: the values a
transaction inserted answer its own reads (the batch's derives chain
through them, and a jump outside the batch costs one preloaded
first-parent chain read per transaction, never one per commit), the
flush writes one statement per 1,000 rows, and the older-version delete
runs once per kind. The per-commit stored-value check waits for the
batch: the transaction's changesets are new, so nothing stored can
disagree. The flush's parents rows follow #540's numbering from zero,
which the squashed #545 still numbered from one.
