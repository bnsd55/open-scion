- The scripts own what they start: common.sh's `start` runs every
  server in a process group of its own, records the pid, the group and
  the command line in the run's work directory, and the exit traps stop
  the group — TERM, then KILL — so a script that dies by error,
  interrupt or timeout leaves no server behind it. `stop` kills only
  after the job is still the one the pidfile recorded. The background
  client loops in soak.sh, soak-fault.sh and capacity.sh are recorded
  the same way and stopped through it; version-matrix.sh's own
  start/stop gains the group and the same check. A CI test starts one
  server and dies by error, then proves the process is gone
  (`scripts/test-owned-run.sh`).
