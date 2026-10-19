The collector's operating metrics (#239): `scion-server gc
--metrics-file PATH` writes the last finished run as a Prometheus
textfile — the end time, the duration, and the rows and files it
collected, each with a `dry_run` label — atomically, and only when the
run succeeds, so a failed run leaves the old file whose timestamp goes
stale. A long run logs its progress over the repos at most every 10 s.
The architecture metric table and the operations guide name the file,
the textfile collector's folder, and the stale-timestamp alert.
