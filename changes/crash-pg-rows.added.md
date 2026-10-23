The crash suite's Postgres arm, as its own file so it never touches
crash.rs. The real `scion-server` binary serves a Postgres database,
and the test kills it with SIGKILL - the crash a cut without a FIN or
RST leaves - at steps of a measured whole run, the way the SQLite
suite places its kills.

Two rows:

- A kill while a wide land runs leaves the ref at the head it had or
  at a changeset the move put there - one whose first parent is that
  head, and whole enough to read back. One restarted server answers
  the replay of the same request key, and the ref must end exactly
  where the reply says it did. The kills run until they have seen
  both endings: a land the kill cut before the move, and one it cut
  after.
- A kill while the schema upgrades leaves the database at one whole
  version, and the next start finishes the upgrade and serves a
  repo the client resolves.

The tests skip themselves unless `SCION_TEST_BACKEND` names
`postgres`. Their clients carry their own 5 s connect timeout and
30 s per-call deadline until a shared builder lands. Each try runs
well under 300 s.
