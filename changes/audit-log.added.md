The server records an audit log: an append-only JSON-lines file of
authentication, writes, ref changes, lands and administrative
changes, with no tokens and no contents.

The file sits in the data directory (mode 0600, `--audit-file`
relocates it, no disable flag) and one writer thread owns it: callers
hand lines to a bounded channel, the writer drains the queue, writes
and fsyncs once per drain, and wakes each caller when its own lines
are durable. A state change records two lines: an `intent` line
fsynced before the change, and a `result` line after it, joined by
request ID; a read records one. Every intent gets its result: a call
that dies before its job starts is answered `cancelled` by the call's
drop, and a land the stale-land sweep rejects is answered `stale` by
the sweep. One case can leave a result missing: a job that dies after
it claimed the call but before it recorded anything — the server log
carries what the audit line cannot. If the audit append fails, the
change is refused and readiness reports not-ready. Refused
authentication is aggregated per peer per interval into one counted
line, capped.

The write, land and ref events carry what an operator needs and no
contents: the RPC, the repo, the ref, the byte count of an upload,
the land's state and ID, and a ref move's old and new changeset IDs.
A changeset ID names content the caller already had; a blob or tree
digest would name contents, so none appears.

On open, a file whose last line has no newline gets one, so a line
that a full drive cut in the middle stays a fragment of its own and
cannot swallow the lines that follow.

A land's ref line names the head the move replaced, read where the
move happens, under the ref's lock, and is written only by the call
that made the move. A replayed land (the same request key after it
landed) moved nothing, so its land line names no old and no ref line
appears for it; an auditor finds the head the move replaced in the
land line of the call that made the move, under the same land_id.
The ref lines of a ref carry the ref's true history: each old is the
head its move's CAS replaced, so linking old to new rebuilds the
history. The lines of concurrent changes can appear in a different
order than the changes themselves (each job records its result after
its change), so the history is linked, not read in line order.

Blob and tree digests are never logged (a digest is a dictionary key
to contents), and neither are token bytes or token hashes. Rotation
is by size, keeps a bounded number of old files, and names what it
dropped in an admin line.

Not covered yet: the ref moves that happen outside the server.
Import moves a ref to each imported tip (import.rs), and archive
restore writes refs back (restore_ref); they run as their own
processes and cannot hand lines to the server's writer, so they must
append their own lines to the same audit file, intent before the
move. Until they do, a reader of the log must not take it as
complete: a ref can move without a line.
