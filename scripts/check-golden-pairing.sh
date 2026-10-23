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

# Two rules, per the docs they serve.
#
# VERSION (the byte goldens of existing messages): the pinned bytes
# changed, so the document's version line must move in the same pull
# request - the bytes ARE the grammar, and an old reader meets new
# bytes otherwise.
#
# DESCRIBE (proto/scion.proto and the surface golden): an additive
# field needs NO new version (docs/protocol-v1.md's additive table), so
# demanding a version bump would force a v2 for every allowed change.
# The same pull request must change the document, any line: the
# contract must describe the new surface.
#
# A golden file the pull request ADDS is its initial pin, not a re-pin:
# the in-tree tests hold that first pin against the doc, and this
# check holds every later change of the golden against the doc's diff.
table=(
    "version|crates/scion-core/tests/golden.rs|docs/format-v1.md|^# Scion object format v"
    "version|crates/scion-proto/tests/protocol_wire_v1.golden|docs/protocol-v1.md|describes version "
    "describe|crates/scion-proto/tests/protocol_surface_v1.golden|docs/protocol-v1.md|."
    "describe|crates/scion-proto/proto/scion.proto|docs/protocol-v1.md|."
    "describe|proto/scion.proto|docs/protocol-v1.md|."
    "crates/scion-server/tests/archive_v1.golden|docs/archive-format.md|^Version [0-9]+ is this document"
    "crates/scion-server/tests/archive_golden.rs|docs/archive-format.md|^Version [0-9]+ is this document"
)

failed=0
for row in "${table[@]}"; do
    rule=${row%%|*}
    rest=${row#*|}
    golden=${rest%%|*}
    rest=${rest#*|}
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
    if [ "$rule" = version ]; then
        echo "golden pairing: '$golden' changed on branch '$branch', but" >&2
        echo "'$doc' version line did not. A pinned-byte change is a" >&2
        echo "format change: move the document's version line in the same" >&2
        echo "pull request, and name the break in a changes/ fragment. If" >&2
        echo "this change is not a format break, it must not touch the" >&2
        echo "golden." >&2
    else
        echo "contract doc: '$golden' changed on branch '$branch', but" >&2
        echo "'$doc' did not. An additive surface change needs no new" >&2
        echo "version, but the contract must describe it: change" >&2
        echo "'$doc' in the same pull request (any line)." >&2
    fi
    failed=1
done
exit "$failed"
