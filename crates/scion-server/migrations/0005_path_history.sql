-- Scion metadata schema, version 4: nodes of path history trees.

-- One directory level of a path history tree (history.rs), keyed by the
-- BLAKE3 hash of its body. Derived data: rebuildable from changesets.
CREATE TABLE history_nodes (
    id   BLOB PRIMARY KEY CHECK (length(id) = 32),
    body BLOB NOT NULL
) STRICT, WITHOUT ROWID;
