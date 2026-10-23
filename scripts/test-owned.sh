#!/bin/bash
# The proof of the owned-process rules (scripts/common.sh), against a
# stand-in server that needs no build: the recorded job runs in a group
# of its own; then the script dies by error, the way a red CI step
# does, and the parent checks what it left behind.
set -u
here=$(cd "$(dirname "$0")" && pwd)
. "$here/common.sh"

fail() {
  echo "test-owned: $*" >&2
  exit 1
}

make_work test-owned
standin="$here/test-owned-standin"

# Case 1: the plain job. Its record goes to the file the parent names;
# then the script dies by error, the way a red CI step does.
start server serving "$standin"
pid=$(sed -n 's/^pid=//p' "$WORK/owned/server")
pgid=$(sed -n 's/^pgid=//p' "$WORK/owned/server")
[ -n "$pid" ] && [ -n "$pgid" ] || fail "the record is incomplete"
[ "$pgid" != "$(ps -o pgid= -p $$ | tr -d ' ')" ] \
  || fail "the server shares this script's process group"
cp "$WORK/owned/server" "$1"
exit 1
