#!/bin/sh
# The tag-identity decision of the release pipeline, where a test can
# reach it. The workflow (release.yml) reads the two API answers a tag
# has — the ref's answer (GET git/ref/tags/T: what the name points at)
# and the tag object's answer (GET git/tags/T: the tagger, the
# verification, and the peeled commit) — and hands both here; this
# script holds the verdicts.
#
#   scripts/release-verify-tag.sh check <ref.json> <tag.json>
#
# Prints the peeled commit SHA and exits 0 when the pair is a valid
# release tag: the ref names a tag object (an annotated tag — a
# lightweight tag's object is the commit itself, so it cannot carry a
# signature, D18), the tag object names a commit, GitHub verified the
# signature (`verification.verified`, reason `valid` — checked at push
# time against the key on the owner's account), and the tagger is the
# owner's email. Otherwise it prints the reason to stderr and exits 1.
# The API calls themselves cannot be tested without a real signed tag;
# the decisions on their answers are what this script pins, and
# scripts/test-release-verify-tag.sh feeds it answers captured from
# real API responses (scripts/testdata/release-verify-tag/), the bad
# cases derived by editing one field each.
set -eu

owner_email=bnsd55@gmail.com

if [ "$#" -ne 3 ] || [ "$1" != check ]; then
    echo "usage: release-verify-tag.sh check <ref.json> <tag.json>" >&2
    exit 2
fi
ref=$2
tag=$3

[ -f "$ref" ] || { echo "no ref answer at $ref" >&2; exit 2; }
[ -f "$tag" ] || { echo "no tag answer at $tag" >&2; exit 2; }

# The ref names a tag object: an annotated tag. A lightweight tag's
# object is the commit itself, so there is no tag object to verify.
type=$(jq -r .object.type "$ref")
[ "$type" = tag ] || {
    echo "a release needs an annotated tag, not a lightweight one" >&2
    exit 1
}

# The tag object names the commit it peels to, carries the tagger, and
# carries the verification GitHub made at push time.
type=$(jq -r .object.type "$tag")
[ "$type" = commit ] || {
    echo "the tag object does not name a commit" >&2
    exit 1
}

verified=$(jq -r '.verification.verified | tostring' "$tag")
reason=$(jq -r '.verification.reason' "$tag")
[ "$verified" = true ] && [ "$reason" = valid ] || {
    echo "the tag is not verified: reason=$reason (D18: sign it)" >&2
    exit 1
}

email=$(jq -r .tagger.email "$tag")
[ "$email" = "$owner_email" ] || {
    echo "the tagger is not the owner" >&2
    exit 1
}

jq -r .object.sha "$tag"
