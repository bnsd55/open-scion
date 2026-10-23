-- A changeset's parents are numbered from 0, as on SQLite. The inserts
-- before this migration numbered them from 1: WITH ORDINALITY starts at
-- one, and nothing else on Postgres ever wrote the rows. Every
-- first-parent read (`n = 0`) then matched nothing, so a chain fetch
-- walked one changeset and stopped.
--
-- Only the rows of a changeset whose smallest n is 1 move: a changeset
-- written by the ordinary path has every row one-based, and a changeset
-- written by the bulk seed already has them from zero. The two steps
-- never hold a duplicate of the primary key (cs, n): the first lifts the
-- moved rows above the CHECK (n >= 0) and far above every other row, the
-- second lands them at n - 1. Running it again changes nothing: after a
-- run, every changeset's smallest n is 0, so no changeset qualifies.
UPDATE changeset_parents SET n = n + 4294967296
 WHERE cs IN (SELECT cs FROM changeset_parents GROUP BY cs HAVING min(n) = 1);
UPDATE changeset_parents SET n = n - 4294967297 WHERE n > 4294967295;
