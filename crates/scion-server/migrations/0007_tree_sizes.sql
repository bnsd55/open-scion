-- Scion metadata schema, version 7: tree entries carry their blob's size.

-- The format change re-hashes every tree, so trees stored before it are in
-- bytes that no longer decode: their IDs were hashes of the old bytes. The
-- derived rows that name a tree ID name the old IDs too. Both rebuild from
-- the stored changesets on demand (`derived::fill`), so the migration just
-- drops them. The blobs and changesets stay: they did not change.
DELETE FROM trees;
DELETE FROM derived WHERE kind = 'root_tree';
