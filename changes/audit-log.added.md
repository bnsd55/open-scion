The server records an audit log: an append-only JSON-lines file of
authentication, writes, ref changes, lands and administrative
changes, with no tokens and no contents.

The file sits in the data directory (mode 0600, `--audit-file`
relocates it, no disable flag) and one writer thread owns it: callers
hand lines to a bounded channel, the writer drains the queue, writes
and fsyncs once per drain, and wakes each caller when its own lines
are durable. A state change records two lines: an `intent` line
fsynced before the change, and a `result` line after it, joined by
request ID; a read records one. If the audit append fails, the change
is refused and readiness reports not-ready. Refused authentication is
aggregated per peer per interval into one counted line, capped.

Blob and tree digests are never logged (a digest is a dictionary key
to contents), and neither are token bytes or token hashes. Rotation
is by size, keeps a bounded number of old files, and names what it
dropped in an admin line.

Not covered yet: nothing - the events above are the whole of the
issue's list.
