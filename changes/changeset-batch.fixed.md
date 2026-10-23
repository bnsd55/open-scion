The import's changeset rows are written in batches: the changesets row
lands per commit (the derived rows of the same commit name it by an
immediate foreign key), and the parents and the repo membership of a
transaction's changesets wait for the flush at the transaction's end,
one statement per 1,000 rows per table. The parents' generations chain
through the generation cache, so the batch reads the database only for
the parents it did not insert, usually the first commit's. An import of
1,000 commits costs about one changeset statement per commit plus a
handful per transaction, instead of four per commit. When the last
per-commit writer of a commit moves into the batch, the changesets row
moves with it, and every table writes at the transaction's end in the
order the foreign keys read: changesets, changeset_parents,
repo_changesets, derived, history, git_import.
