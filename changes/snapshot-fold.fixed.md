The snapshot file name of a repo whose name holds ASCII capitals
changes: `MAIN` gave `MAIN.scnsnap`, and now gives
`%4D%41%49%4E.scnsnap`. On a case-insensitive disk (the macOS
default), two repos whose names differ only in case wrote their
snapshots to one file, and the second write silently replaced the
first; with the capitals escaped, the output holds lowercase literal
letters only, and no fold can put two names on one file. Nothing reads a snapshot file
by its generated name — `scion-daemon` seeds only from an explicit
`--seed FILE`, and the cache does not reload its own snapshot at
start — so an upgrade makes a repo's next snapshot appear under the
new name; the old file goes cold once and can be deleted.
