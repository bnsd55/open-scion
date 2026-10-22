The import and the restore write their git provenance in batches: one
statement per 1,000 pairs on Postgres (= ANY over bytea arrays), where
the per-commit code wrote one git_import row per statement - a
100,000-commit import or restore paid 100,000 statements. The pairs of
one import transaction leave in one call at the transaction's end, so
a crash still loses whole commits, never a changeset without its
provenance row, and a repeated pair is still skipped, not overwritten.
