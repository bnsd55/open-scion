The one-server lock of a Postgres store was one key for the whole
database: a second server against the same database was refused, even
when its URL named a different schema through `options`. The lock now
keys on what the connection actually lands in — the open asks the
database `current_schema()` — so a plain URL and one that rides
`--search_path=public` fence each other (same schema, one lock), and
two URLs that ride different schemas each fence their own. A URL that
names a schema can now serve beside another server of the same
database; that is a production behavior change, and the reason the
tests of one database can hold one server each.
