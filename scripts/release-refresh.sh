#!/usr/bin/env bash
# Refreshes a release's changelog and release notes, right before its
# tag. Run from the repository root:
#
#   scripts/release-refresh.sh v0.1.0
#
# 1. scripts/merge-changelog.sh <release> folds the changes/ fragments
#    into CHANGELOG.md and deletes them.
# 2. The release notes file docs/release-notes/<release>.md must exist.
# 3. The notes may not carry an unfilled pending slot: a
#    CAPACITY-NUMBERS-PENDING marker means the capacity runs have not
#    printed, and a release does not carry numbers it has not measured
#    — fill the slot or drop the release.
# 4. The changelog's release section must not still say "unreleased"
#    when the tag is cut: the date is set by the release PR (the step
#    is in docs/RELEASING.md), and this script prints a reminder either
#    way.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: scripts/release-refresh.sh <release>   e.g. v0.1.0" >&2
    exit 2
fi
release=$1
notes="docs/release-notes/$release.md"

scripts/merge-changelog.sh "$release"

if [ ! -f "$notes" ]; then
    echo "no release notes at $notes" >&2
    exit 1
fi

if grep -q "PENDING" "$notes"; then
    echo "$notes still carries a pending marker:" >&2
    grep -n "PENDING" "$notes" >&2
    echo "fill it from a green run, or drop the release" >&2
    exit 1
fi

if grep -q "## \[$release\] - unreleased" CHANGELOG.md; then
    echo "reminder: set the release date in CHANGELOG.md before the tag"
    echo "(the step is in docs/RELEASING.md, the release PR)"
else
    echo "$release is dated in CHANGELOG.md; ready for the tag"
fi
