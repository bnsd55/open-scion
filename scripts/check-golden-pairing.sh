#!/usr/bin/env bash
# Fails a pull request that changes a contract golden without the same
# pull request moving its format document's version line. Takes the
# base and head revs; CI passes none and gets HEAD^1..HEAD, the pull
# request's own changes.
#
#   scripts/check-golden-pairing.sh            # as CI runs it
#   scripts/check-golden-pairing.sh main HEAD
set -euo pipefail

base=${1:-HEAD^1}
head=${2:-HEAD}
branch=${GITHUB_HEAD_REF:-$(git rev-parse --abbrev-ref HEAD)}

# The check fails closed: a revision the checkout cannot see is a broken
# setup, never a pass.
for rev in "$base" "$head"; do
    if ! git rev-parse --verify --quiet "$rev^{commit}" >/dev/null; then
        echo "check-golden-pairing: revision '$rev' is missing from this" >&2
        echo "checkout, so the changed files cannot be named; the check" >&2
        echo "refuses to pass. Give the checkout a fetch-depth that has" >&2
        echo "the base side of the pull request's merge commit." >&2
        exit 1
    fi
done

case $branch in
    release/*) exit 0 ;;
esac

# One row per golden: the files whose change pins the format, and the
# document whose version line must move in the same pull request, with
# the pattern that line matches. A golden file the pull request ADDS
# is its initial pin, not a re-pin: the pairing tests in the tree hold
# that first pin against the doc, and this check holds every later
# change of the golden against the doc's diff. A golden that changed
# without its doc's version line means an old reader meets new bytes:
# the break the version rules exist for.
table=(
    "crates/scion-core/tests/golden.rs|docs/format-v1.md|^# Scion object format v"
    "crates/scion-proto/tests/protocol_wire_v1.golden|docs/protocol-v1.md|describes version "
    "crates/scion-proto/tests/protocol_surface_v1.golden|docs/protocol-v1.md|describes version "
    "crates/scion-proto/proto/scion.proto|docs/protocol-v1.md|describes version "
    "crates/scion-server/tests/archive_v1.golden|docs/archive-format.md|^Version [0-9]+ is this document"
    "crates/scion-server/tests/archive_golden.rs|docs/archive-format.md|^Version [0-9]+ is this document"
)

failed=0
for row in "${table[@]}"; do
    golden=${row%%|*}
    rest=${row#*|}
    doc=${rest%%|*}
    pattern=${rest#*|}
    if git diff --name-only --diff-filter=A "$base" "$head" -- "$golden" | grep -q .; then
        continue
    fi
    if ! git diff --name-only "$base" "$head" -- "$golden" | grep -q .; then
        continue
    fi
    if git diff "$base" "$head" -- "$doc" | grep -E "^[+-].*$pattern" | grep -q .; then
        continue
    fi
    echo "golden pairing: '$golden' changed on branch '$branch', but" >&2
    echo "'$doc' version line did not. A golden change is a format" >&2
    echo "change: move the document's version line in the same pull" >&2
    echo "request, and name the break in a changes/ fragment. If this" >&2
    echo "change is not a format break, it must not touch the golden." >&2
    failed=1
done
exit "$failed"
