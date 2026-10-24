The production audit's evidence now holds as tests: a tampered history
node reads as corrupt through the server path (`get_history_node`
answers "does not match its ID"), and fsck reports the row
("history-node-bad"), on both backends (`tests/audit_evidence.rs`). The
audit's evidence cells name the lines and dispatch runs that exist, and
the writer-idle probe's role and its untested cadence are stated in the
runbook and the audit.
