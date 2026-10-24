The 72-hour soak of issue #239's run-length decision is ready for the
day a VM is rented for it, and runs nothing until then. A
dispatch-only `soak-72h` workflow waits for one self-hosted runner
with the `scion-soak` label (a label no other job names), gives it
4400 minutes, and runs one uninterrupted 72-hour soak (RUN_MINUTES
4320, the same ceilings and warm-up window as the nightly). It is
never a pull_request, push or scheduled run, and the fault job stays a
nightly of its own.

The job's `GITHUB_TOKEN` is dead after 24 hours, so the long job calls
nothing with it after its checkout: the samples stream to files on the
runner's disk while it runs (`metrics.csv`, `metrics-store.csv`, the
logs), and a separate short job - `needs: soak-72h`, `if: always()`, a
fresh token - uploads them. With one `scion-soak` runner, the upload
job lands on the same machine and reads the same disk.

`scripts/soak-runner-setup.md` registers the runner on a fresh Ubuntu
VM as a normal runner (NOT `--ephemeral`: the workflow's two jobs both
need it, and an ephemeral runner de-registers after the first, leaving
the upload waiting forever), sizes the disk at about 300 GB for the
store (one 30-minute release run: 334 MB -> 8.95 GB beside 22,930
lands, slowing as it goes) plus system and margin, and removes the
runner (`config.sh remove`) and the VM after. Because the repository
is public, the doc also names the fork-PR risk - a fork workflow can
target the runner's label while it is up - and the owner's two doors:
the fork-PR approval policy is tightened to all external contributors
before the runner is registered and put back to
`first_time_contributors` after, and the runner exists only for the
rental.
