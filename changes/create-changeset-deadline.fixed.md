The `CreateChangeset` write stops its blob checks at the call's
  deadline: the check re-hashes every distinct blob the changeset puts,
  so a big changeset would otherwise keep hashing after the client gave
  up. Nothing is written when the call is gone or its deadline passed,
  and the write's audit gets its one failed line.
