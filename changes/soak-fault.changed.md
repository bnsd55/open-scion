The fault half of the soak: scripts/soak-fault.sh runs the soak's
mixed load against one server and kills it on schedule - kill -9 under
load, then a restart on the same data directory, one restart of the
run taking the whole chain (cache and daemon with it). After every
restart the run checks that the server lock is free, the server
serves, a client read succeeds, fsck is clean beside the server, and
every land the server acknowledged is still in the ref's history; the
resource ceilings apply per restart segment. An optional Postgres
restart ships behind SCION_FAULT_PG. The nightly gains a soak-fault
job beside the soak, and the 72-hour run (#239, part 3) stands on it.
