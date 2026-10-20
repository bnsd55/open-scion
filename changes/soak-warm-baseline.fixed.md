The nightly soak failed its own RSS bound on 2026-10-19 (run
39219458903): 34892 KB -> 54140 KB in about 13 minutes, over the 150
percent ceiling. The bound's baseline was the first sample, taken after
60 seconds - and the server's memory is still climbing then, for a
while. A 35-minute local run of the same load saw RSS climb for about
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
it cannot tell warm-up from a leak (the failed run was a 10-minute
dispatch; the nightly is the check that matters). The other bounds are
untouched.

The samples now include the store's size on disk: every 10 s the loop
appends epoch and `du -sk` of the server data directory to a
metrics-store.csv, next to metrics.csv in the artifacts, so a run can
be read for disk growth and for Ben's 72-hour soak decision, not only
for the RSS it shares a cause with.
