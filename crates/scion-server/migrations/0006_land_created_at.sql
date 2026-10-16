-- Scion metadata schema, version 5: when each land was recorded.

-- When the land was recorded, in Unix seconds. The stale-land sweep
-- (#15) rejects a pending land that started before its cutoff, so the
-- age of a pending row must be known. Lands that existed before this
-- migration get the time the migration runs, so they become stale 24 h
-- after the upgrade, not at once.
ALTER TABLE lands ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0;
UPDATE lands SET created_at = unixepoch() WHERE created_at = 0;

-- Finds the oldest pending lands without a scan of the table.
CREATE INDEX lands_pending_by_created_at ON lands (created_at) WHERE state = 'pending';
