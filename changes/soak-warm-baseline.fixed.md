The nightly soak failed its own RSS bound on 2026-10-19 (run
39219458903, the schedule event on 2bc9099): 34892 KB -> 54140 KB, over
the 150 percent ceiling. At that commit the schedule branch of the
workflow still fell back to the 10-minute default, so the nightly ran
its planned 10 minutes and failed the check at the end, against a
baseline taken after 60 seconds; the 120-minute schedule came later on
main. There was no early abort: the only RSS check in the script runs
after the sampling loop, and the fixed script's only RSS check needs
the warm baseline, which exists only after SOAK_WARM_SECONDS. A 35-minute local run of the same load saw RSS climb for about
30 minutes (32288 -> 59184 KB) and then hold flat for the last 16
minutes (the last 20 samples: max 59184, average 58137, no trend), and
the failed CI curve itself rose, stepped once at about 4 minutes, and
then oscillated 49-57 KB for its last 5 minutes with no climb. That is
warm-up - sqlite page caches of the 8 read connections and the write
connection, allocator arenas, prepared statements - not a leak: a leak
does not plateau on two runs.

The bound's baseline is now the first sample after SOAK_WARM_SECONDS
(1800 s, named in soak.yml): the nightly still checks 90 minutes of RSS
after the warm-up. A run shorter than the window checks no RSS bound -
it cannot tell warm-up from a leak (a 10-minute run, as the nightly of
that commit was, sees only the climb). The other bounds are
untouched.

The samples now include the store's size on disk: every 10 s the loop
appends epoch and `du -sk` of the server data directory to a
metrics-store.csv, next to metrics.csv in the artifacts, so a run can
be read for disk growth and for Ben's 72-hour soak decision, not only
for the RSS it shares a cause with.
