# The audit log's line surface, pinned by a golden

The audit's lines join the machine-checked surfaces: a checked-in
golden (`crates/scion-server/tests/audit_v1.golden`) pins every
(event, outcome) kind a served server writes - the exercise drives
refused auth, the start line, a put, a create, the failed create, a
land and a land that runs past its deadline - and for each kind, the
field names and JSON types of its lines. The values (`ts`, the ids,
the counts) are this run's and pin nothing. A source census pins the
kind set the code writes, and the exercise must equal it exactly.
Re-pin on purpose with `SCION_UPDATE_AUDIT_GOLDEN=1` and a changes
fragment naming the added kind.
