The Postgres-restart runbook (docs/operations.md, "A Postgres restart"):
a database that restarts under the server needs no server restart - the
calls answer the busy error a client retries against, the store does not
fence itself, and the next write after the database is back reconnects,
takes the advisory lock again, and runs. The Proof test is the existing
takeover test, which stops the job's database, sees the busy answers and
the fence stay down, brings the database back, and recovers the store
with no process restart.
