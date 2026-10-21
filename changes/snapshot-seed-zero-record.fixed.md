# A zero-record snapshot seeds an all-zero report, calmly

The fuzzer found a snapshot whose header and repo name parse but that
holds no records: `snapshot::seed` took it and reported
`stored: 0, skipped: 0, damaged: false` - a calm take of an empty
snapshot - and the fuzz target then failed its own `>= 1` assertion.
The reader was right; the target's invariant was not. The target now
allows the all-zero report exactly when the file holds no records, and
a unit test pins the zero-record file class in snapshot.rs. Main's own
fuzz job fails a different way today: `fuzz/seeds/snapshot-seed` does
not exist, so the target exits before it fuzzes - that one is a
workflow gap, not a reader bug.
