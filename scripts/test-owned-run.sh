#!/bin/bash
# The parent half of the owned-process test: run the dying child, then
# check what it left behind — nothing running, and a record that says
# what it should. Then the wrapper case: a job started through env, so
# its program name at stop time is not what was recorded; the group is
# what the stop checks, so it stops anyway.
set -u
here=$(cd "$(dirname "$0")" && pwd)

fail() {
  echo "test-owned: $*" >&2
  exit 1
}

record=$(mktemp)
child_out=$(mktemp)

# Case 1: the plain job, then the dying script.
bash "$here/test-owned.sh" "$record" > "$child_out" 2>&1
rc=$?
[ "$rc" -eq 1 ] || { echo "the child ended $rc, not 1" >&2; cat "$child_out"; exit 1; }
pid=$(sed -n 's/^pid=//p' "$record")
[ -n "$pid" ] || fail "the record is incomplete"
for _ in 1 2 3 4 5 6 7 8 9 10; do
  kill -0 "$pid" 2> /dev/null || break
  sleep 0.5
done
if kill -0 "$pid" 2> /dev/null; then
  fail "the server outlived the script that died (pid $pid)"
fi
echo "test-owned: a script that dies by error leaves no process (pid $pid gone; cmd was: $(sed -n 's/^cmd=//p' "$record"))"

# Case 2: the same job through a wrapper. env execs the stand-in, so
# the program name at stop time is 'env', not the stand-in; the group
# is the identity, so the job is stopped all the same.
bash -c '
  here="$1"
  . "$here/common.sh"
  make_work test-owned
  start wrapped serving env OWNED_TEST=1 "$here/test-owned-standin"
  pgid=$(sed -n "s/^pgid=//p" "$WORK/owned/wrapped")
  sleep 0.2
  stop wrapped
  sleep 0.5
  kill -0 "-$pgid" 2> /dev/null && { echo "WRAPPED JOB SURVIVED" >&2; exit 1; }
  exit 0
' _ "$here" > "$child_out" 2>&1
rc=$?
[ "$rc" -eq 0 ] || { echo "the wrapped job case failed ($rc)" >&2; cat "$child_out"; exit 1; }
echo "test-owned: a wrapped job (env) is stopped through its group"
rm -f "$record" "$child_out"
