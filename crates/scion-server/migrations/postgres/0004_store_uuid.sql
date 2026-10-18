-- The identity of this database: one row, written at the first open. The
-- marker file of the data directory carries the same UUID, so a database
-- and a data directory that do not belong together are refused at open
-- instead of serving metadata that names blobs of another disk.
CREATE TABLE IF NOT EXISTS store_uuid (
    one  BIGINT PRIMARY KEY CHECK (one = 1),
    uuid BYTEA NOT NULL CHECK (octet_length(uuid) = 16)
);
