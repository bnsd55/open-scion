#!/bin/bash
# The proof of the owned-process rules (scripts/common.sh): a script
# that starts a server and then dies by error leaves no process behind,
# and the process runs in a group of its own, recorded with the command
# that started it. The child here starts one server, prints what it
# recorded, waits for the word, and dies by error; this script then
# checks that nothing is left and that the record said what it should.
set -u
here=$(cd "$(dirname "$0")" && pwd)
. "$here/common.sh"

fail() {
  echo "test-owned: $*" >&2
  exit 1
}

make_work test-owned

BIN="$here/../target/release"
[ -x "$BIN/scion-server" ] || BIN="$here/../target/debug"
[ -x "$BIN/scion-server" ] || {
  note "building a debug scion-server for the owned-process test"
  cargo build --quiet -p open-scion-server --bin scion-server || fail "the build failed"
  BIN="$here/../target/debug"
}

# One server, started the owned way. Its record goes to the parent's
# file; then the script dies by error, the way a red CI step does.
record=$1
start server serving "$BIN/scion-server" --data "$WORK/server" serve \
  --listen "127.0.0.1:$((21000 + $$ % 20000))"
cp "$WORK/owned/server" "$record"
pgid=$(sed -n 's/^pgid=//p' "$WORK/owned/server")
[ "$pgid" != "$(ps -o pgid= -p $$ | tr -d ' ')" ] \
  || fail "the server shares this script's process group"
exit 1
