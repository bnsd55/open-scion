# The guide's own commands become an executable rule

A Proof of a runbook used to mean a reviewer reading the test and the
guide side by side. Now the check runs itself:

- The block-extraction the alert-runbook tests carried moves into the
  shared test support (`test_support::docs`): `runbook_block` reads the
  first fenced `sh` block under a heading of `docs/operations.md`, with
  named substitutions (an address, a data directory, a token file) in
  place of the documented literals, and `run_guide_block` runs it
  through `sh`. The alert tests keep their blocking-pool wrapper and
  call the shared helper.
- `docs_ops` gains `every_proof_runs_the_guides_command_or_says_why`:
  for every `Proof:` line, the named test must call the shared helper
  with its section's heading, or the line must say `(drill)` or
  `(behaviour, no command)` with a reason, or the proof must sit in
  `NOT_YET_DRIVING` with every other converting proof. The list must
  match the guide exactly, so no proof is exempt silently and no
  finished conversion keeps its row.
- The certificate-rotation and ticket-key Proof lines now carry the
  `(behaviour, no command)` marker with their reasons, as do the
  disk-full, failing-disk, and Postgres-restart runbooks, whose steps
  are actions on the machine or the database rather than one command.

## Proofs that do not yet run their block

`NOT_YET_DRIVING`, in `docs_ops.rs`, names the tests that still run the
guide's commands their own way; each row is work to delete. Today:

- First boot, One server: the harness spawns the guide's serve command
  itself; conversion is a refactor of the shared spawn.
- The durability point: power, fsync, and backup tests drive the
  storage through the test seams, not through the guide's shell.
- A failed migration and 5. Routines: the refusal and gc tests run the
  commands, but not yet through the helper.
- Stalled replies high: the log-stream test reproduces the stall, not
  the guide's curl.
