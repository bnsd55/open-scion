#!/bin/sh
# Feeds scripts/release-verify-tag.sh the two API answers a tag has,
# captured from real API responses, and checks the verdicts: the one
# decision in the release pipeline that no CI run can exercise, because
# no test run holds a signed release tag.
#
# The fixtures live in scripts/testdata/release-verify-tag/: the good
# pair is a signed annotated tag of a public repository, captured with
# `gh api` (git/git at v2.43.0), its tagger email edited to the owner's;
# the lightweight pair is a real lightweight tag's answers (tokio at
# tokio-1.53.0); the other bad cases are the good pair with one field
# edited each. The API calls themselves are the thin part the workflow
# does; the decisions on their answers are what fails loudly here.
set -eu
cd "$(dirname "$0")"
script=./release-verify-tag.sh
data=testdata/release-verify-tag

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# A verified tag by a stranger: the good pair, one field edited.
jq '.tagger.email = "stranger@example.com"' \
    "$data/tag-good.json" > "$work/tag-stranger.json"
# An unverified tag: the good pair, the verification edited.
jq '.verification = {verified: false, reason: "unsigned"}' \
    "$data/tag-good.json" > "$work/tag-unsigned.json"
# The good pair, with the owner's tagger email: the captured answer
# came from git/git, whose tagger signs the release, so one field —
# the email — moves it into this project's shape.
jq '.tagger.email = "bnsd55@gmail.com"' \
    "$data/tag-good.json" > "$work/tag-owned.json"

expect_ok() {
    got=$($script check "$data/$1" "$2") || {
        echo "FAIL: $1 must pass, refused with: $got" >&2
        exit 1
    }
    [ "$got" = "$3" ] || {
        echo "FAIL: $1/$2 peeled to $got, not $3" >&2
        exit 1
    }
}

expect_refused() {
    if out=$($script check "$1" "$2" 2>&1); then
        echo "FAIL: $1/$2 must be refused, accepted as $out" >&2
        exit 1
    fi
}
# The release case: a signed annotated tag owned by the owner.
expect_ok ref-good.json "$work/tag-owned.json" \
    564d0252ca632e0264ed670534a51d18a689ef5d
# A lightweight tag: no tag object, no signature to verify.
expect_refused "$data/ref-lightweight.json" "$data/tag-good.json"
# An annotated tag whose signature GitHub could not verify.
expect_refused "$data/ref-good.json" "$work/tag-unsigned.json"
# Verified by the wrong account's key.
expect_refused "$data/ref-good.json" "$work/tag-stranger.json"

echo "release-verify-tag: all four verdicts hold"
