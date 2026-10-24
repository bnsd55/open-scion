The server no longer creates a missing data directory. Every
scion-server subcommand except restore now refuses a directory that
does not exist -- given by --data, SCION_DATA, or the default
scion-data in the working directory -- with one message: "<path> does
not exist; create it or pass --data". restore still creates its
target. Update scripts that relied on auto-create: create the
directory first, or keep passing an existing --data (operations.md's
examples already do).
