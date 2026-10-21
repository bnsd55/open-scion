# Wave 1: the first boot and one-server proofs run the guide's command

The harness gains `spawn_on`/`start_on`: the serve argv comes from the
first fenced `sh` block under the named heading of docs/operations.md,
with the test's data directory, token file, and port substituted for
the documented ones. The first-boot test starts its server with the
First boot block's own line and asserts the One server block's command
is the same command; the kill test starts on the One server block. The
backend_refusals proofs move to `(behaviour, no command)` markers:
their subject is the database's own lock and container, not a scion
command. Five rows leave NOT_YET_DRIVING; twelve remain.
