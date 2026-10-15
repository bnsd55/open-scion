-- Scion metadata schema, version 3: a derived value can have any length.

-- Version 1 allowed only 32-byte values, the size of a root tree ID. Each
-- kind now encodes its own value. SQLite cannot drop a CHECK in place, so
-- the table is made again and every row is copied into it.
CREATE TABLE derived_v3 (
    kind    TEXT NOT NULL,
    version INTEGER NOT NULL,
    cs      BLOB NOT NULL REFERENCES changesets (id),
    value   BLOB NOT NULL,
    PRIMARY KEY (kind, version, cs)
) STRICT, WITHOUT ROWID;

INSERT INTO derived_v3 (kind, version, cs, value)
    SELECT kind, version, cs, value FROM derived;
DROP TABLE derived;
ALTER TABLE derived_v3 RENAME TO derived;
