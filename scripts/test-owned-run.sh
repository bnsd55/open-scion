#!/bin/bash
# The parent half: run the child, then check what it left behind.
set -u
here=$(cd "$(dirname "$0")" && pwd)
record=$(mktemp)
child_out=$(mktemp)
bash "$here/test-owned.sh" "$record" > "$child_out" 2>&1
rc=$?
[ "$rc" -eq 1 ] || { echo "the child ended $rc, not 1" >&2; cat "$child_out"; exit 1; }
pid=$(sed -n 's/^pid=//p' "$record")
cmd=$(sed -n 's/^cmd=//p' "$record")
[ -n "$pid" ] && [ -n "$cmd" ] || { echo "the record is incomplete: $(cat "$record")" >&2; exit 1; }
for _ in 1 2 3 4 5 6 7 8 9 10; do
  kill -0 "$pid" 2> /dev/null || break
  sleep 0.5
done
if kill -0 "$pid" 2> /dev/null; then
  echo "FAIL: the server outlived the script that died (pid $pid)" >&2
  kill -9 "$pid" 2> /dev/null || true
  exit 1
fi
echo "test-owned: a script that dies by error leaves no process (pid $pid gone; cmd was: $cmd)"
rm -f "$record" "$child_out"
