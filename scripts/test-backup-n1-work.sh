#!/bin/bash
# The backup drill must not delete a run directory the CALLER named.
# H1 (2026-10-24): DRILL_WORK_MADE was set unconditionally, so cleanup()
# ran rm -rf "$WORK" even for WORK=... an operator passed - an operator
# running WORK=~/drills scripts/backup-n1.sh lost ~/drills. The guard:
# the script runs with WORK pointing at a directory that holds a marker
# file, fails fast (STORE_UPGRADE_TAGS names a ref that cannot build),
# and the marker must survive.
#
# Red-proved on the pre-H1 script: the marker was gone (exit 1).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/changes"
printf 'A real fragment, still on the books.\n' >"$work/changes/1.fixed.md"
printf '## [v0.1.0] - unreleased\n' >"$work/CHANGELOG.md"
printf 'the operator kept files here\n' >"$work/marker.txt"

# A ref the builder fails fast on: the empty tree is not a commit, so
# the drill dies at its first git call, well after cleanup's trap is
# armed - exactly the path that used to delete the caller's directory.
status=0
(cd "$work" &&
    WORK="$work" STORE_UPGRADE_TAGS=0000000000000000000000000000000000000000 \
    "$HERE/backup-n1.sh") >/dev/null 2>&1 || status=$?

if [ ! -f "$work/marker.txt" ]; then
    echo "the caller's WORK directory was deleted; only a run directory" >&2
    echo "the script itself made may go" >&2
    exit 1
fi
echo "backup-n1 work guard: a caller's WORK survives the drill (exit $status)"
