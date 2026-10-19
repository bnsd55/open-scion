#!/usr/bin/env bash
# The release smoke test (roadmap item for the release checklist): the
# README quick start, end to end, on the release binaries, from a clean
# checkout. It builds the binaries, makes a small git repository, imports
# it, makes a ticket key, serves, reads with `scn` (log, ls, cat, blame),
# writes with `scn` (commit, land, then reads back), runs `fsck`, and
# stops the server with one signal, which must drain and exit 0. It runs
# the whole story once under umask 022 and once under 002, because the
# release checklist says the binaries must work under both. Every failure
# names the step that failed, and everything it made is removed when it
# exits, for any reason.
#
# The story is the one a reader of the README walks through, so a release
# that fails here fails the front door, not an internal test seam.
#
# CI runs this on Linux. macOS does not run it in CI: a cold release build
# there nearly doubles the job for a story that Linux already tells. Run
# it by hand on a Mac before a release.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
case ${CARGO_TARGET_DIR:-} in
  "") BIN=$ROOT/target/release ;;
  /*) BIN=$CARGO_TARGET_DIR/release ;;
  *) BIN=$ROOT/$CARGO_TARGET_DIR/release ;;
esac

# The script passes every address, name and path as a flag or its own
# export, so the caller's environment must not change the story. Git runs
# without the user's and the system's config, and scn commits as its own
# author, so local identities do not leak into the story. NO_COLOR gives
# plain log lines, so the `serving listen=` line parses without escape
# codes (the same reason the server's tests set it).
unset SCION_SERVER SCION_REPO SCION_DATA SCION_DAEMON_DATA SCION_AUTHOR
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export SCION_TOKEN=smoke
export NO_COLOR=1

WORK=
SERVER_PID=

die() {
  echo "release-smoke: $*" >&2
  exit 1
}

note() { echo "-- $*" >&2; }

# step NAME CMD...: runs CMD; if it fails, the message names the step.
step() {
  local name=$1
  shift
  "$@" || die "step '$name' failed (umask $UMASK_RUN)"
}

cleanup() {
  [ -n "$WORK" ] || return 0
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# make_repo: the small repository the story imports. Three commits on
# main, so log, ls, cat and blame all have something to say, and the
# commit and land at the end land on top of the third.
make_repo() {
  git init -q -b main "$WORK/src"
  git -C "$WORK/src" config user.name smoke
  git -C "$WORK/src" config user.email smoke@localhost
  printf 'pub fn main() {}\n' >"$WORK/src/hello.rs"
  printf '# smoke\n\nA small repository for the release smoke test.\n' >"$WORK/src/README.md"
  git -C "$WORK/src" add -A
  git -C "$WORK/src" commit -q -m first
  printf '// one more line\n' >>"$WORK/src/hello.rs"
  git -C "$WORK/src" commit -qam second
  printf 'One more line of prose.\n' >>"$WORK/src/README.md"
  git -C "$WORK/src" commit -qam third
}

run_story() {
  UMASK_RUN=$1
  umask "$UMASK_RUN"

  # The data directory's parent must not be writable by group or others:
  # the server refuses to open a data directory whose parent another user
  # can change. mktemp -d already makes 0700 on Linux and macOS; the chmod
  # says so and keeps a umask-002 run from a mktemp that does not. The
  # server's own directories are 0700 whatever the umask (#302), so this
  # parent is the only one the story has to get right itself.
  WORK=$(mktemp -d "${TMPDIR:-/tmp}/scion-smoke.XXXXXX")
  chmod 700 "$WORK"
  note "run under umask $UMASK_RUN in $WORK"

  step repo make_repo

  step import "$BIN/scion-server" --data "$WORK/server" import "$WORK/src"

  # The ticket key, as the README prints it: the server signs with the
  # private half, and every object read carries a ticket it minted.
  "$BIN/scion-server" --data "$WORK/server" ticket-key --out "$WORK/ticket.key" \
    >"$WORK/ticket.pub" || die "step 'ticket-key' failed (umask $UMASK_RUN)"

  # The server on a port the OS chooses: `--listen 127.0.0.1:0`, and the
  # log's `serving listen=` line names the port, so the story never races
  # another test for a fixed port.
  "$BIN/scion-server" --data "$WORK/server" serve --listen 127.0.0.1:0 \
    --token smoke --ticket-key "$WORK/ticket.key" \
    >"$WORK/server.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 300); do
    grep -q " serving listen=" "$WORK/server.log" 2>/dev/null && break
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      tail -n 20 "$WORK/server.log" >&2
      die "step 'serve' failed (umask $UMASK_RUN)"
    fi
    sleep 0.1
  done
  if ! grep -q " serving listen=" "$WORK/server.log"; then
    tail -n 20 "$WORK/server.log" >&2
    die "step 'serve' did not come up within 30 s (umask $UMASK_RUN)"
  fi
  PORT=$(sed -n 's/.* serving listen=127\.0\.0\.1:\([0-9]*\).*/\1/p' "$WORK/server.log" | tail -1)
  [ -n "$PORT" ] || die "step 'serve' logged no port (umask $UMASK_RUN)"
  export SCION_SERVER=http://127.0.0.1:$PORT

  # Read the history the import made.
  "$BIN/scn" log -n 5 | grep -q third || die "step 'log' failed (umask $UMASK_RUN)"
  "$BIN/scn" ls main | grep -q README.md || die "step 'ls' failed (umask $UMASK_RUN)"
  "$BIN/scn" cat main:README.md | grep -q 'A small repository' \
    || die "step 'cat' failed (umask $UMASK_RUN)"
  "$BIN/scn" blame main:README.md | grep -q . || die "step 'blame' failed (umask $UMASK_RUN)"

  # Write, as an agent does: commit, land, and read back through main.
  printf 'hello from the release smoke test\n' >"$WORK/hello.txt"
  CS=$("$BIN/scn" commit -m "add hello" --put hello.txt="$WORK/hello.txt") ||
    die "step 'commit' failed (umask $UMASK_RUN)"
  [ -n "$CS" ] || die "step 'commit' printed no changeset (umask $UMASK_RUN)"
  "$BIN/scn" land "$CS" | grep -q '^landed ' \
    || die "step 'land' failed (umask $UMASK_RUN)"
  "$BIN/scn" cat main:hello.txt | grep -q '^hello from the release smoke test$' \
    || die "step 'land' did not move main (umask $UMASK_RUN)"

  # fsck while the server runs, as the README says is safe: it must be
  # clean. Its exit code says so (0 clean, 1 damage, 2 cannot run).
  step fsck "$BIN/scion-server" --data "$WORK/server" fsck

  # Clean shutdown: the first signal starts the drain, and the process
  # exits 0 when the drain is done. TERM, not INT: a script's background
  # child may have INT ignored, and the story must reach the handler.
  kill -TERM "$SERVER_PID"
  if ! wait "$SERVER_PID"; then
    die "step 'shutdown' failed: the server did not exit 0 after the first signal (umask $UMASK_RUN)"
  fi
  SERVER_PID=

  rm -rf "$WORK"
  WORK=
  note "umask $UMASK_RUN passed"
}

command -v cargo >/dev/null || die "needs cargo"
command -v git >/dev/null || die "needs git"

# The build, once: the two umask runs share it. `--locked`, because a
# release smoke test runs the checkout's own Cargo.lock.
UMASK_RUN=-
(cd "$ROOT" && cargo build --release --locked --quiet) ||
  die "step 'build' failed"

run_story 022
run_story 002
note "release smoke passed: both umask runs, every step"
