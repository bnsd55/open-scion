#!/usr/bin/env bash
# The current streak of the nightly: the scheduled runs of ci.yml on main,
# counted from the newest completed one backwards until the first
# non-success. A report, not a check: it prints and exits 0, so the
# nightly cannot go red on its own way to 30. Anyone with `gh` runs it:
# scripts/nightly-streak.sh [target]
set -euo pipefail

target=${1:-30}

streak=0
broke_at=""
# Newest first; only completed runs count, so the run of tonight (still
# in progress while the workflow prints this) is not in the way, and a
# pending or cancelled run does not break the count of finished nights.
while IFS=$'\t' read -r conclusion url; do
    if [ "$conclusion" = "success" ]; then
        streak=$((streak + 1))
    else
        broke_at="$url"
        break
    fi
done < <(
    gh run list \
        --workflow ci.yml \
        --branch main \
        --event schedule \
        --limit 40 \
        --json conclusion,url,status \
        --jq '.[] | select(.status == "completed") | [.conclusion, .url] | @tsv'
)

if [ "$streak" -eq 0 ] && [ -z "$broke_at" ]; then
    echo "nightly streak: 0/$target (no completed scheduled run yet)"
    exit 0
fi

echo "nightly streak: $streak/$target"
if [ -n "$broke_at" ]; then
    echo "the streak broke at $broke_at"
fi
