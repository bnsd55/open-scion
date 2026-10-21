# Wave 3: the collector's proof runs the routine's own line

The block selector now finds a heading at either level, so `5. Routines`
- a numbered part, not a subsection - hands its collector block over.
The gc-metrics proof builds its collector command from the routine's own
line, with the test's data directory and metrics file substituted, and
its dry run is the documented `--dry-run` flag.

The four proofs whose subject is behaviour no command reproduces - the
refusal and restore of a failed migration, the kill inside a migration,
the fsck-restore routine, and the stalled-reply close on a paused clock
- carry `(behaviour, no command)` markers with their reasons.

NOT_YET_DRIVING is empty: every Proof in the guide runs its section's
command block through the shared helper, or says on its line why it
cannot. A new proof must do one of the two, and a stale row or a silent
exemption fails the check.
