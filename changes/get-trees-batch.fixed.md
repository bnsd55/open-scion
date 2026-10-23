The root-tree derivation reads a commit's directory trees in one
statement per level of depth, where the per-directory code read one
tree per statement: a commit that touches 100 stored directories paid
101 tree reads on Postgres, one in 10 paid 11. The trees hand their
entries over whole (Tree::into_entries), so a prefetched tree is no
longer decoded and then cloned; the bytes are checked against their
ids, as the single read does.
