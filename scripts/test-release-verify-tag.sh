#!/bin/sh
# Feeds scripts/release-verify-tag.sh canned tag objects, the answers
# GitHub's API would give, and checks the verdicts: the one decision in
# the release pipeline that no CI run can exercise, because no test run
# holds a signed release tag. The API call itself is the thin part the
# workflow does; the decisions on its answer are what fails loudly
# here.
set -eu
cd "$(dirname "$0")"
script=./release-verify-tag.sh

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

write_object() {
    cat > "$work/$1.json"
}

# An annotated tag, verified, tagged by the owner: the release case.
write_object good <<'EOF'
{
  "object": {"type": "tag", "sha": "1111111111111111111111111111111111111111"},
  "tagger": {"email": "bnsd55@gmail.com"},
  "verification": {"verified": true, "reason": "valid"}
}
EOF

# A lightweight tag: its object is the commit itself, so no signature.
write_object lightweight <<'EOF'
{
  "object": {"type": "commit", "sha": "2222222222222222222222222222222222222222"},
  "verification": {"verified": true, "reason": "valid"}
}
EOF

# An annotated tag whose signature GitHub could not verify.
write_object unsigned <<'EOF'
{
  "object": {"type": "tag", "sha": "3333333333333333333333333333333333333333"},
  "tagger": {"email": "bnsd55@gmail.com"},
  "verification": {"verified": false, "reason": "unsigned"}
}
EOF

# Verified by the wrong account's key.
write_object stranger <<'EOF'
{
  "object": {"type": "tag", "sha": "4444444444444444444444444444444444444444"},
  "tagger": {"email": "stranger@example.com"},
  "verification": {"verified": true, "reason": "valid"}
}
EOF

expect_ok() {
    got=$($script check "$work/$1.json") || {
        echo "FAIL: $1 must pass, refused with: $got" >&2
        exit 1
    }
    [ "$got" = "$2" ] || {
        echo "FAIL: $1 peeled to $got, not $2" >&2
        exit 1
    }
}

expect_refused() {
    if out=$($script check "$work/$1.json" 2>&1); then
        echo "FAIL: $1 must be refused, accepted as $out" >&2
        exit 1
    fi
}

expect_ok good 1111111111111111111111111111111111111111
expect_refused lightweight
expect_refused unsigned
expect_refused stranger

echo "release-verify-tag: all four verdicts hold"
