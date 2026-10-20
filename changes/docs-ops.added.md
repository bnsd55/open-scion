docs/operations.md's shell blocks cannot drift from --help: two docs_ops
tests parse every command the guide writes for scion-server (split at its
subcommand) and for scion-cache and scion-daemon, and fail when a flag the
command names is not in that binary's help - the direction docs_flags.rs
does not cover. A runbook section whose steps a test runs ends with a
Proof: line naming the test; the test fails when a proof's file or
function is gone, so renaming or deleting a proof breaks the guide loudly.
The first five proofs name the tests that already run the key-rotation,
backup-and-restore, upgrade, and gc steps. docs/drills/ gains the template
a recorded drill follows, and RELEASING.md gains the line that runs the
release's drills on the release candidate.
