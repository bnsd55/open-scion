# Archive: a section's reads hold one snapshot

An export's metadata sections claim one read snapshot, and on SQLite
the claim was false: `Store::read` runs each statement autocommit, so
a land that commits between a section's changesets query and its refs
query left a ref in the archive whose target the archive does not
hold, and the export failed its own verify. A section now reads inside
one explicit read transaction on the reader connection
(`Backend::read_snapshot`); Postgres already gave its reads one
REPEATABLE READ transaction. The cost is the one the section's
snapshot always named: the log cannot restart while the section reads.
