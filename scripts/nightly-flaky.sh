# One flaky-or-real record per red nightly: the open issue titled
# "nightly: flaky or real?" carries every red nightly as a comment, so
# the trail of a flaky test is one thread, and the issue's closure is
# what lets the streak reach 30. Ben approved the automation before it
# shipped (a public post by a bot is Ben's call, not the agents').
# Usage: scripts/nightly-flaky.sh <run-id>
set -euo pipefail

run_id=${1:?the workflow run id}
title="nightly: flaky or real?"

url=$(gh run view "$run_id" --json url --jq .url)
# The names of the tests the run failed, one per line; empty when no
# test failed - a setup step or the runner died, which is infra, not a
# flaky test, and the record says so instead of guessing.
failed=$(gh run view "$run_id" --log-failed 2>/dev/null |
    grep -oE '^test [A-Za-z0-9_]+ ?::?[A-Za-z0-9_:]* ?\.\.\. FAILED' |
    sort -u || true)

body="A scheduled full-suite run failed.

- run: $url"
if [ -n "$failed" ]; then
    body="$body

Failed tests:

\`\`\`
$failed
\`\`\`"
else
    body="$body

The log names no failed test: a setup step or the runner itself died.
A human should look at the run before anyone calls it flaky."
fi

number=$(gh issue list --search "$title in:title" --state open \
    --json number --jq '.[0].number // empty')
if [ -n "$number" ]; then
    gh issue comment "$number" --body "$body"
else
    gh issue create --title "$title" --body "$body"
fi
