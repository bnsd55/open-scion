The threat model tracks the code: #369 made the directory checks refuse
every ACL beyond the mode (the sticky bit excuses no group write), so
that invariant now names its tests instead of sitting in the no-test
list. Two more claims got tests: a narrowed access file does not cut
outstanding tickets until the ticket key rotates, and the partial
reader's view keeps answering real changeset IDs, so a landing on
hidden paths shows through the ref's ID over an unchanged view. The
remaining no-test claims say why no test can exist.
