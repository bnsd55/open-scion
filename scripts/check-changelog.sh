#!/usr/bin/env bash
# Fails a pull request that edits CHANGELOG.md instead of adding a
# changes/ fragment, unless its branch starts with release/. Takes the
# base and head revs; CI passes none and gets HEAD^1..HEAD, the pull
# request's own changes.
#
#   scripts/check-changelog.sh            # as CI runs it
#   scripts/check-changelog.sh main HEAD
set -euo pipefail

base=${1:-HEAD^1}
head=${2:-HEAD}
branch=${GITHUB_HEAD_REF:-$(git rev-parse --abbrev-ref HEAD)}

case $branch in
    release/*) exit 0 ;;
esac

if git diff --name-only "$base" "$head" -- CHANGELOG.md | grep -q .; then
    echo "CHANGELOG.md changed on branch '$branch': add a" >&2
    echo "changes/<slug>.<section>.md fragment instead; only a release/" >&2
    echo "branch edits CHANGELOG.md" >&2
    exit 1
fi
