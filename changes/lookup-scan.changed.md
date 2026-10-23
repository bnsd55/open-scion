A path lookup read each level's whole tree node: decode it into the
  entry map, clone the map, remove the one name. Now the level's
  stored `TreeV1` bytes are scanned for the one name — linear with an
  early exit, because variable-length entries give no random access
  and a binary search would need an offset table the format does not
  have. The decoder's checks keep their place: the body is checked
  against its ID, and the scan refuses what the decoder refuses. In
  the lookup example's own measurement, finding one entry in a
  100,000-entry folder's peak allocation drops from 331 MB to 26 MB,
  and the scan answers what the full decode answers, name for name.
