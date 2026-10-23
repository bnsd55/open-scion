A Postgres operator met three numbers the manual never wrote: how
many connections a server opens, what the advisory rows in `pg_locks`
are, and how long the collector's key can hold a writer. The Postgres
section now says all three, beside the one-server rule they enforce:

- A server opens one write connection and 8 read connections, plus
  the short-lived connections of a migration or an import; the
  database's `max_connections` is sized for the server's 9, the
  operator's tooling beside it, and a margin, and a database at its
  cap answers `unavailable` until a connection is free.
- The one-server lock, the migration lock and the collector's key
  (`hashtext(current_schema() || ':gc')`, a 60 s bound on that key
  alone) are named as the `advisory` rows an operator sees in
  `pg_locks` - held by this server by design, not a stuck client.

The watch list's `scion_pg_pool_wait_seconds` was listed twice, the
first copy without the 300 s/30 s bounds; the two are one entry now.

The per-land round-trip budgets and the out-of-space SQLSTATEs are
deliberately absent: their source PRs (#467, #480) are unmerged, and
the numbers land with them.
