-- The root of the view tree that one canonical read rule set derives for
-- one changeset (#121). Rebuildable: a cache of a pure function of the rule
-- set and the real tree. The view trees themselves are ordinary tree rows.
CREATE TABLE view_roots (
    repo_id BIGINT NOT NULL REFERENCES repos (repo_id),
    digest  BYTEA NOT NULL CHECK (octet_length(digest) = 32),
    cs      BYTEA NOT NULL REFERENCES changesets (id),
    root    BYTEA NOT NULL CHECK (octet_length(root) = 32),
    PRIMARY KEY (repo_id, digest, cs)
);
