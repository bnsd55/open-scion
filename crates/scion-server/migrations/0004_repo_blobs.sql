-- Scion metadata schema, version 4: blobs belong to repos.

-- The blobs that a repo may name: those uploaded to it, and those that its
-- changesets name. Blob files are shared by all repos, so without this a
-- changeset could name another repo's blob by ID and then read it. A row is
-- not a promise that the blob is stored: CreateChangeset still reads every
-- blob back. Rows are never removed.
--
-- The upgrade fills the table from the changesets of each repo, in the same
-- transaction (`db::fill_repo_blobs`).
CREATE TABLE repo_blobs (
    repo_id INTEGER NOT NULL REFERENCES repos (repo_id),
    blob    BLOB NOT NULL CHECK (length(blob) = 32),
    PRIMARY KEY (repo_id, blob)
) STRICT, WITHOUT ROWID;
