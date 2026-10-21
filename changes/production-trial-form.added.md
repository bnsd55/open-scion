The production-trial record exists as a form:
[docs/production-trial.md](docs/production-trial.md) (#395, the v0.9.0
gate). Its fields hold what the roadmap names: the commit and the
binaries with their checksums, the backend and its version, the
filesystem, the workload with its floors fixed in writing before day
1, a daily table of lands, reads, latencies, memory, disk and errors,
and sections for upgrades, restores, key and certificate rotations and
incidents — each with a time, what happened, and the evidence link —
ending in the pass-or-fail rule against the floors. Every floor value
is left blank for the owner to fix, and every field says what a second
operator fills in.
