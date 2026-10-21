ms: "the Postgres section says what a start after an unclean kill sees"
---

A first start after an unclean kill can exit with "another server
holds this database": Postgres keeps the dead server's session, and
with it the one-server advisory lock, until the session is ended. The
Postgres section now says the recovery and both clocks that decide
how long the lock stands: on the same host the kernel closes the dead
process's sockets at once, so a service manager's restart
(`Restart=on-failure`) is the whole recovery; a lost host is seen
only through TCP keepalives, whose clock is the database's
`tcp_keepalives_idle` (unset, the system default, often 2 hours), so
the section names `pg_terminate_backend` and that clock for an
operator who cannot wait it out.
