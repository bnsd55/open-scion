`scripts/lint-workflows.sh` runs actionlint (pinned by version and
sha256, like the grpcurl pin) over `.github/workflows/` in the `fmt +
clippy` job: a workflow that cannot parse fails the PR that carries it,
not main's next push-triggered run (#416 broke bench.yml with an
unquoted colon, #419 shipped a coverage.yml its pinned tool refused,
and no run caught either). The check proves it on the #416 case: the
unquoted `CAPACITY_MACHINE` value fails actionlint's syntax check, the
quoted one passes.
