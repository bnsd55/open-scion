-- The per-tree view memo (#184): one row per (repo, rule-set digest,
-- projection version, real tree, path-context digest). A derivation asks
-- this table at every directory, so a new changeset re-derives only the
-- folders on its changed paths. The changeset's root row is the row of
-- its real root tree under the rule set's full context. Rebuildable: a
-- cache of a pure function of the rule set and the real trees, so it
-- holds no truth; rows of old versions or digests no principal uses are
-- swept in bounded steps.
DROP TABLE view_roots;
CREATE TABLE view_trees (
    repo_id INTEGER NOT NULL REFERENCES repos (repo_id),
    digest  BLOB NOT NULL CHECK (length(digest) = 32),
    version INTEGER NOT NULL,
    tree    BLOB NOT NULL CHECK (length(tree) = 32),
    ctx     BLOB NOT NULL CHECK (length(ctx) = 32),
    view    BLOB NOT NULL CHECK (length(view) = 32),
    PRIMARY KEY (repo_id, digest, version, tree, ctx)
) STRICT, WITHOUT ROWID;
