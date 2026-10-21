#!/bin/sh
# The tag-identity decision of the release pipeline, where a test can
# reach it. The workflow (release.yml) reads the tag object through
# GitHub's API and hands the JSON here; this script holds the verdicts.
#
#   scripts/release-verify-tag.sh check <tag-object.json>
#
# Prints the peeled commit SHA and exits 0 when the tag object is a
# valid release tag: an annotated tag (its object is a tag, whose target
# is a commit), verified by GitHub (`verification.verified`, reason
# `valid` — the SSH signature checked at push time against the key on
# the owner's account), tagged by the owner's email. Otherwise it prints
# the reason to stderr and exits 1. The API call itself cannot be
# tested without a real tag; the decisions on its answer are what this
# script pins, and scripts/test-release-verify-tag.sh feeds it canned
# answers.
set -eu

owner_email=bnsd55@gmail.com

if [ "$#" -ne 2 ] || [ "$1" != check ]; then
    echo "usage: release-verify-tag.sh check <tag-object.json>" >&2
    exit 2
fi
json=$2

[ -f "$json" ] || { echo "no tag object at $json" >&2; exit 2; }

type=$(jq -r .object.type "$json")
[ "$type" = tag ] || {
    echo "a release needs an annotated tag, not a lightweight one" >&2
    exit 1
}

verified=$(jq -r '.verification.verified | tostring' "$json")
reason=$(jq -r '.verification.reason' "$json")
[ "$verified" = true ] && [ "$reason" = valid ] || {
    echo "the tag is not verified: reason=$reason (D18: sign it)" >&2
    exit 1
}

email=$(jq -r .tagger.email "$json")
[ "$email" = "$owner_email" ] || {
    echo "the tagger is not the owner" >&2
    exit 1
}

jq -r .object.sha "$json"
