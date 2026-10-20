The nightly is CI itself: `ci.yml` gains a schedule trigger (03:00 UTC,
after the soak's 02:30), so the scheduled full suite is exactly the CI
suite - the same jobs, the same cases, nothing to drift. The
concurrency group carries the event name, so a 03:00 schedule waits
its turn instead of cancelling a push to main that started minutes
earlier.

`scripts/nightly-streak.sh` prints the current streak: the scheduled
runs of `ci.yml` on main, counted from the newest completed one
backwards until the first non-success, which is the v0.8.0 roadmap
line's "30 consecutive runs" as a fact anyone re-checks with `gh`. A
small `nightly streak` job prints it on every run; it reads the run
history only (`actions: read`).
