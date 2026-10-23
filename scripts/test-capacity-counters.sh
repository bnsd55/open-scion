#!/usr/bin/env bash
# Tests the capacity reader log's counters against a fixture: lines in
# the shapes the reader loop writes, counted by the functions
# scripts/capacity.sh uses. The bug this catches: a grep anchored to the
# line's start counted zero of every event, because every line starts
# with its attempt number (a review of #461, 2026-10-21). Run from the
# repository root; CI runs it in the lint job.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$here/common.sh"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cat >"$work/reader.log" <<'EOF'
attempt 1 start 10:00:00.000
attempt 1 end ms=107 10:00:05.321
attempt 2 start 10:00:05.400
attempt 2 timeout after 60s at 10:01:05.410
attempt 3 start 10:01:05.400
attempt 3 checkout failed rc=1 at 10:01:05.900
attempt 4 start 10:01:06.000
EOF

count=$(count_reader_timeouts "$work/reader.log")
[ "$count" -eq 1 ] || { echo "timeouts: wanted 1, got $count" >&2; exit 1; }
count=$(count_reader_failures "$work/reader.log")
[ "$count" -eq 1 ] || { echo "failures: wanted 1, got $count" >&2; exit 1; }

: >"$work/empty.log"
count=$(count_reader_timeouts "$work/empty.log")
[ "$count" -eq 0 ] || { echo "an empty log must count 0, got $count" >&2; exit 1; }
count=$(count_reader_failures "$work/empty.log")
[ "$count" -eq 0 ] || { echo "an empty log must count 0, got $count" >&2; exit 1; }

# A missing log counts 0 too: the functions never fail the cell.
count=$(count_reader_timeouts "$work/absent.log")
[ "$count" -eq 0 ] || { echo "an absent log must count 0, got $count" >&2; exit 1; }

echo "counter test: ok"
