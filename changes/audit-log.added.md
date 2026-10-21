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

The write, land and ref events carry what an operator needs and no
contents: the RPC, the repo, the ref, the byte count of an upload,
the land's state and ID, and a ref move's old and new changeset IDs.
A changeset ID names content the caller already had; a blob or tree
digest would name contents, so none appears.

On open, a file whose last line has no newline gets one, so a line
that a full drive cut in the middle stays a fragment of its own and
cannot swallow the lines that follow.

Blob and tree digests are never logged (a digest is a dictionary key
to contents), and neither are token bytes or token hashes. Rotation
is by size, keeps a bounded number of old files, and names what it
dropped in an admin line.

Not covered yet: nothing - the events above are the whole of the
issue's list.
