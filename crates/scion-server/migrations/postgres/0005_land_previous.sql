-- Scion metadata schema, version 5: what a land moved the ref from.

-- The ref's target that the land's move replaced. NULL while the land is
-- pending, and for the lands that finished before this migration: their
-- moves already happened, and nothing can read what they replaced now.
-- A replay of a land reads this back, so its audit lines carry the move
-- of the first run, not the ref's later head.
ALTER TABLE lands ADD COLUMN previous_cs BYTEA REFERENCES changesets (id);
