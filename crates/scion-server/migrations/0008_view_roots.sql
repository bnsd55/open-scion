-- The root of the view tree that one canonical read rule set derives for
-- one changeset (#121). Rebuildable: a cache of a pure function of the rule
-- set and the real tree. The view trees themselves are ordinary tree rows.
CREATE TABLE view_roots (
    repo_id INTEGER NOT NULL REFERENCES repos (repo_id),
    digest  BLOB NOT NULL CHECK (length(digest) = 32),
    cs      BLOB NOT NULL REFERENCES changesets (id),
    root    BLOB NOT NULL CHECK (length(root) = 32),
    PRIMARY KEY (repo_id, digest, cs)
) STRICT, WITHOUT ROWID;
