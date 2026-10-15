-- Scion metadata schema, version 2: finished land rows expire.

-- When a land left the pending state, in Unix seconds. NULL while pending,
-- so a pending land never expires. Lands that finished before this
-- migration get the time it runs, so their retention period starts at the
-- upgrade.
ALTER TABLE lands ADD COLUMN finished_at INTEGER;
UPDATE lands SET finished_at = unixepoch() WHERE state != 'pending';

-- Finds the oldest finished lands without a scan of the table.
CREATE INDEX lands_by_finished_at ON lands (finished_at) WHERE finished_at IS NOT NULL;
