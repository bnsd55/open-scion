The backup-N-1 and store-upgrade drills kill every server they
backgrounded on any exit path, TERM then KILL, via the pid files the
starter wrote. Before, a drill that died between starting and stopping
a server left it running unparented. A drill run without a caller-named
run directory now writes under `.scn-drills/` in the repository, not in
the shared temp directory.
