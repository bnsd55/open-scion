The daemon refuses a data directory that does not exist.

scion-daemon's `--data` defaults to a working-directory-relative name
and the daemon created it when it was missing, so a typo in `--data`,
or a run from the wrong folder, made a new empty store and served from
it. The daemon now refuses a missing data directory before it opens
the store, with the server's message shape ("does not exist; create
it or pass --data"), and creates nothing. The cache keeps no data
directory (memory only), so it has no path to refuse. Scripts and
docs that start the daemon on a fresh path create it first.
