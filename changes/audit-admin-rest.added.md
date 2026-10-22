# The admin lines cover every state-changing command

The last admin actions join the audit. The `derive` subcommand records
one `admin`/`intent` line before its backfill and one `admin`/`ok` after
it, with the kind and the rows the run wrote and removed; a failed run
records `admin`/`failed` with the class. Making the counts real,
`backfill` returns a `DeriveReport` (the kind's name, the rows it wrote,
the rows of older versions it removed) instead of a bare name.

An open that migrates a SQLite store's schema records one
`admin`/`ok` line `{what: migrate, from, to}` as soon as the audit can
open, before the command's own pair: the migration ran inside the
store's open, before the audit could exist, so no intent can precede
it. A fresh open creates the schema and reports no migration; a
Postgres database's schema is the fleet's, and no single open moves it.

The `ticket-key` subcommand records one intent before the key exists
and one ok after it is on the disk. The lines carry the what and
nothing else: never the seed, the key bytes, or the key file's path,
in any encoding the test searches for.

The golden's `admin`/`intent` and `admin`/`ok` sections grow the new
fields; no new (event, outcome) kind appears. With this slice every
state-changing CLI command writes its admin lines, and #385's admin
item is complete: gc, import, restore, derive, the schema migration,
and ticket-key generation all record what they changed.
