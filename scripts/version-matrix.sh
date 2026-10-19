#!/usr/bin/env bash
# The version matrix (issue #239, part 1): the pairs of
# docs/compatibility.md, at versions N (this tree) and N-1, so a release
# that breaks a listed pair is caught by CI, not by an operator.
#
# N-1 is PREV_REF when set (any commit or tag), else the newest release
# tag that is an ancestor of HEAD and not HEAD itself: when HEAD carries
# the release tag, N-1 is the one before it. With no tag and no PREV_REF
# the script prints `skipped: no release tag yet` in the step summary
# (GITHUB_STEP_SUMMARY when set) and exits 0: the matrix starts at
# v0.1.0 and says so, it never passes silently.
#
# The pairs, and what each proves:
#
#   1. scn <-> server, both directions: the release-smoke story (import,
#      ticket key, log, ls, cat, blame, commit, land, read back) answers
#      inside the v1 contract with the client and the server at opposite
#      versions.
#   2. tier <-> server, both directions: a read through the cache
#      matches a direct read byte for byte, the ticket verifies (the
#      cache holds its public half), and the second read is served by
#      the cache: the server's GetObjects counter does not grow.
#   3. scn <-> daemon over the unix:// path, both directions: the
#      checkout reads what cli.md documents, and --json keeps its shape.
#      The server stays at N: the pair under test is the socket pair.
#   4. the store, one direction only (docs/compatibility.md): a server
#      of N opens a store of N-1, the migration runs forward, and reads
#      and writes then work on the migrated store. A store of N on a
#      server of N-1 is refused with the version reason -- but only when
#      the two builds write different schema versions at all; when they
#      share one, the script says so instead of failing.
#
# Left out on purpose, per docs/compatibility.md:
#   - tier <-> tier (a chain of caches): the policy's chain rule follows
#     from every hop, and each hop is pair 2 here.
#   - scn <-> repositories of any 0.x: the golden object tests pin the
#     object bytes and IDs, so this pair needs no built binaries.
#
# A minor that adds a call must leave the client able to handle
# UNIMPLEMENTED from an N-1 server, but the matrix cannot force that
# from two arbitrary commits: which call is new is not known until the
# pair exists. The conformance job covers the status codes; a release
# that adds a call proves the fallback in its own PR.
#
# Versions:
#   PREV_REF   the commit or tag to test as N-1 (the workflow's prev_ref
#              input goes here); beats tag resolution.
#   SCION_BIN  a directory with the N binaries, to skip that build (a
#              local debug run); PREV_BIN the same for N-1. PREV_SRC
#              must name the matching source checkout, pair 4 reads its
#              schema version. In CI both are unset and the release
#              builds run.
#   PREV_SRC   where the N-1 checkout and its build live. CI names it a
#              stable path and never cleans it: rust-cache reads the
#              target dir after this script exits, keyed by the commit.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
case ${CARGO_TARGET_DIR:-} in
  "") BIN=$ROOT/target/release ;;
  /*) BIN=$CARGO_TARGET_DIR/release ;;
  *) BIN=$ROOT/$CARGO_TARGET_DIR/release ;;
esac

# The scripts give every address, name and path as a flag or their own
# export, so the caller's environment must not change the story. Git runs
# without the user's and the system's config, and NO_COLOR keeps the log
# lines plain, so the `serving listen=` lines parse.
unset SCION_SERVER SCION_REPO SCION_DATA SCION_DAEMON_DATA SCION_AUTHOR
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export SCION_TOKEN=matrix SCION_UPSTREAM_TOKEN=matrix NO_COLOR=1

die() {
  echo "version-matrix: $*" >&2
  exit 1
}

note() { echo "-- $*" >&2; }

# summary LINE: what a CI run reads. Into the step summary when there is
# one; to stderr always, so a local run sees it too.
summary() {
  echo "$*" >&2
  [ -z "${GITHUB_STEP_SUMMARY:-}" ] || echo "$*" >> "$GITHUB_STEP_SUMMARY"
}

# prev_ref: the ref to test as N-1, per the header. Empty when there is
# none: PREV_REF unset, and no release tag behind HEAD.
prev_ref() {
  if [ -n "${PREV_REF:-}" ]; then
    printf '%s\n' "$PREV_REF"
    return 0
  fi
  local tag head
  head=$(git -C "$ROOT" rev-parse HEAD)
  git -C "$ROOT" tag --list 'v[0-9]*' --sort=-version:refname --merged=HEAD |
    while read -r tag; do
      if [ "$(git -C "$ROOT" rev-parse "$tag^{commit}")" != "$head" ]; then
        printf '%s\n' "$tag"
        break
      fi
    done
}

# --resolve-ref: print the N-1 ref and exit, so a workflow can key its
# cache on the commit before any build runs. No ref: say why, exit 0.
if [ "${1:-}" = "--resolve-ref" ]; then
  ref=$(prev_ref)
  if [ -z "$ref" ]; then
    summary "skipped: no release tag yet"
    exit 0
  fi
  printf '%s\n' "$ref"
  exit 0
fi

# The N-1 source checkout. One the caller named is theirs: the script
# never cleans it, so a CI cache can still read the target dir it built.
if [ -n "${PREV_SRC:-}" ]; then
  PREV_OWNED=
else
  PREV_SRC=$(mktemp -d "${TMPDIR:-/tmp}/scion-matrix-prev.XXXXXX")
  PREV_OWNED=1
fi
PREV_SRC=$(cd "$PREV_SRC" && pwd -P)

WORK=
# The daemon's socket sits outside WORK: the pair's temp path is longer
# than the Unix socket limit of 104 bytes. It sits in the temp root
# itself, which is the user's: the daemon refuses a socket whose parent
# another user owns, so macOS's /tmp (a root-owned link) is out. One
# path per run and per direction, named by the pid, cannot collide: a
# slow daemon that unlinks its socket on exit must not touch the next
# daemon's live one.
SOCK=$(cd "${TMPDIR:-/tmp}" && pwd -P)/scion-matrix-$$.sock

cleanup() {
  local rc=$1 f
  rm -f "$SOCK" "$SOCK-cur" "$SOCK-prev"
  [ -n "$WORK" ] || return 0
  for f in "$WORK"/*/*.pid; do
    if [ -e "$f" ]; then stop "$(dirname "$f")"; fi
  done
  if [ "$rc" -ne 0 ]; then
    # A red run keeps its logs, whichever pair failed: the CI job
    # uploads the directory, and the next run replaces it.
    rm -rf "${TMPDIR:-/tmp}/scion-matrix-failed"
    mv "$WORK" "${TMPDIR:-/tmp}/scion-matrix-failed"
    note "the logs of the failed run are in ${TMPDIR:-/tmp}/scion-matrix-failed"
  else
    rm -rf "$WORK"
  fi
  if [ -n "$PREV_OWNED" ]; then
    git -C "$ROOT" worktree remove --force "$PREV_SRC" 2> /dev/null || true
    rm -rf "$PREV_SRC"
  fi
}
trap 'rc=$?; cleanup "$rc"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# make_work: the run's temp root; each pair gets a directory under it,
# and each process's pid file sits beside its log.
make_work() {
  WORK=$(mktemp -d "${TMPDIR:-/tmp}/scion-matrix.XXXXXX")
  # Resolve symlinks (macOS: /var is /private/var), as git daemon does.
  WORK=$(cd "$WORK" && pwd -P)
}

command -v cargo > /dev/null || die "needs cargo"
command -v git > /dev/null || die "needs git"
command -v curl > /dev/null || die "needs curl"

# build_prev REF: the N-1 binaries and their source checkout. PREV_BIN
# skips the build (a local debug run); the source checkout is made
# either way, because pair 4 reads the schema version out of its source.
build_prev() {
  local ref=$1
  if [ ! -f "$PREV_SRC/Cargo.toml" ]; then
    note "checking out $ref into $PREV_SRC"
    git -C "$ROOT" worktree add --detach "$PREV_SRC" "$ref" > /dev/null ||
      die "step 'checkout $ref' failed"
  fi
  if [ -n "${PREV_BIN:-}" ]; then
    [ -x "$PREV_BIN/scion-server" ] || die "PREV_BIN=$PREV_BIN has no scion-server"
    return 0
  fi
  PREV_BIN=$PREV_SRC/target/release
  [ -x "$PREV_BIN/scion-server" ] && return 0
  note "building the N-1 release binaries ($ref)"
  (cd "$PREV_SRC" && CARGO_TARGET_DIR="$PREV_SRC/target" cargo build --release --locked --quiet) ||
    die "step 'build N-1 ($ref)' failed"
}

# schema_of SRC: the store schema version a checkout writes, from its
# source. Pair 4's refusal can only fire when the two builds differ.
schema_of() {
  sed -n 's/.*SCHEMA_VERSION: u32 = \([0-9]*\).*/\1/p' "$1/crates/scion-server/src/store.rs" | head -1
}

# make_repo DIR: the small repository of release-smoke.sh: three commits
# on main, so log, ls, cat and blame all have something to say, and the
# commit and land at the end land on top of the third.
make_repo() {
  git init -q -b main "$1/src"
  git -C "$1/src" config user.name matrix
  git -C "$1/src" config user.email matrix@localhost
  printf 'pub fn main() {}\n' >"$1/src/hello.rs"
  printf '# matrix\n\nA small repository for the version matrix.\n' >"$1/src/README.md"
  git -C "$1/src" add -A
  git -C "$1/src" commit -q -m first
  printf '// one more line\n' >>"$1/src/hello.rs"
  git -C "$1/src" commit -qam second
  printf 'One more line of prose.\n' >>"$1/src/README.md"
  git -C "$1/src" commit -qam third
}

# import DIR SERVER_BIN: a fresh store of SERVER_BIN's version, with the
# repo of make_repo in it, and its ticket key made. The store's version
# is the server's, so every pair starts from what its own server wrote.
import() {
  local dir=$1 srv=$2
  make_repo "$dir"
  "$srv/scion-server" --data "$dir/server" import "$dir/src" \
    >"$dir/import.out" 2>"$dir/import.log" || {
    tail -n 5 "$dir/import.log" >&2
    die "pair '$PAIR' ($SIDE): the import failed"
  }
  "$srv/scion-server" --data "$dir/server" ticket-key --out "$dir/ticket.key" \
    >"$dir/ticket.pub" || die "pair '$PAIR' ($SIDE): ticket-key failed"
}

# start NAME DIR BIN READY FLAGS...: run the binary BIN (a full path,
# scion-server or scion-cache or scion-daemon) in the background, its
# log and pid file in DIR as NAME, and wait for its serving line. READY
# is `port` (echo the `serving listen=` port) or `serving` (echo
# nothing; the daemon serves a socket, not a port).
start() {
  local name=$1 dir=$2 bin=$3 ready=$4 log pid port tries=300
  shift 4
  log="$dir/$name.log"
  "$bin" "$@" >"$log" 2>&1 &
  pid=$!
  echo "$pid" >"$log.pid"
  while :; do
    port=$(sed -n 's/.* serving listen=127\.0\.0\.1:\([0-9]*\).*/\1/p' "$log" | tail -1)
    if [ -n "$port" ] || [ "$ready" = serving ] && grep -q ' serving' "$log"; then
      break
    fi
    if ! kill -0 "$pid" 2> /dev/null; then
      tail -n 10 "$log" >&2
      die "pair '$PAIR' ($SIDE): $name did not start"
    fi
    tries=$((tries - 1))
    [ "$tries" -gt 0 ] || {
      tail -n 10 "$log" >&2
      die "pair '$PAIR' ($SIDE): $name did not come up within 30 s"
    }
    sleep 0.1
  done
  [ "$ready" = port ] && printf '%s\n' "$port"
  return 0
}

# stop_one DIR NAME: stop the one process `start DIR NAME` started. The
# pid is not a child of this shell (the server started inside a command
# substitution that has since exited), so wait cannot confirm a death:
# poll kill -0 for ten seconds, then KILL, and say which process needed
# it. A wedged server here is exactly what the matrix hunts; it must
# fail loud, not hang cleanup.
stop_one() {
  local dir=$1 name=$2 f="$1/$2.log.pid" pid n
  [ -e "$f" ] || return 0
  pid=$(cat "$f")
  rm -f "$f"
  kill -TERM "$pid" 2> /dev/null || true
  n=0
  while kill -0 "$pid" 2> /dev/null && [ "$n" -lt 100 ]; do
    sleep 0.1
    n=$((n + 1))
  done
  if kill -0 "$pid" 2> /dev/null; then
    note "$name (pid $pid) ignored TERM; sent KILL"
    kill -KILL "$pid" 2> /dev/null || true
  fi
}

# stop DIR: stop every process `start DIR` started.
stop() {
  local dir=$1 f
  for f in "$dir"/*.log.pid; do
    [ -e "$f" ] || return 0
    stop_one "$dir" "$(basename "$f" .log.pid)"
  done
}

# story DIR URL: the reads and the write of the smoke story, run by
# $SCN/scn against URL. Every command must succeed and print what
# protocol-v1 and cli.md say it prints. Each output goes to a file and
# the file is grepped: a `cmd | grep -q` closes the pipe at the first
# match, and a client still printing dies on the broken pipe.
story() {
  local dir=$1 url=$2 cs
  SCION_SERVER=$url "$SCN/scn" log -n 5 >"$dir/log.out" 2>"$dir/log.err" ||
    die "pair '$PAIR' ($SIDE): scn log failed"
  grep -q third "$dir/log.out" ||
    die "pair '$PAIR' ($SIDE): scn log did not name the third commit"
  SCION_SERVER=$url "$SCN/scn" ls main >"$dir/ls.out" 2>"$dir/ls.err" ||
    die "pair '$PAIR' ($SIDE): scn ls failed"
  grep -q README.md "$dir/ls.out" ||
    die "pair '$PAIR' ($SIDE): scn ls did not name README.md"
  SCION_SERVER=$url "$SCN/scn" cat main:README.md >"$dir/cat.out" 2>"$dir/cat.err" ||
    die "pair '$PAIR' ($SIDE): scn cat failed"
  grep -q 'A small repository' "$dir/cat.out" ||
    die "pair '$PAIR' ($SIDE): scn cat read the wrong bytes"
  SCION_SERVER=$url "$SCN/scn" blame main:README.md >"$dir/blame.out" 2>"$dir/blame.err" ||
    die "pair '$PAIR' ($SIDE): scn blame failed"
  grep -q . "$dir/blame.out" ||
    die "pair '$PAIR' ($SIDE): scn blame printed nothing"
  printf 'hello from the version matrix\n' >"$dir/hello.txt"
  cs=$(SCION_SERVER=$url "$SCN/scn" commit -m "add hello" --put hello.txt="$dir/hello.txt" 2>"$dir/commit.err") ||
    die "pair '$PAIR' ($SIDE): scn commit failed"
  [ -n "$cs" ] || die "pair '$PAIR' ($SIDE): scn commit printed no changeset"
  SCION_SERVER=$url "$SCN/scn" land "$cs" >"$dir/land.out" 2>"$dir/land.err" ||
    die "pair '$PAIR' ($SIDE): scn land failed"
  grep -q '^landed ' "$dir/land.out" ||
    die "pair '$PAIR' ($SIDE): scn land did not say it landed"
  SCION_SERVER=$url "$SCN/scn" cat main:hello.txt >"$dir/back.out" 2>"$dir/back.err" ||
    die "pair '$PAIR' ($SIDE): the read back failed"
  grep -q '^hello from the version matrix$' "$dir/back.out" ||
    die "pair '$PAIR' ($SIDE): the landed write did not read back"
}

# getobjects URL: the server's total of GetObjects requests, over every
# code. A counter that never incremented does not render, hence the +0.
getobjects() {
  command curl -fsS "$1/metrics" | awk '
    /scion_requests_total\{rpc="Objects\/GetObjects"/ { s += $NF }
    END { print s + 0 }'
}

# metrics_port LOG: the port of the server's `serving metrics` line.
metrics_port() {
  sed -n 's/.* serving metrics listen=127\.0\.0\.1:\([0-9]*\).*/\1/p' "$1" | tail -1
}

# pair_1 DIRECTION SCN_BIN SRV_BIN: the client of one version against
# the server of the other, on a store the server's version wrote.
pair_1() {
  local side=$1 scn=$2 srv=$3 dir port
  PAIR='scn<->server' SIDE=$side SCN=$scn
  dir=$(mktemp -d "$WORK/pair1-$side.XXXXXX")
  note "pair 1 ($side)"
  import "$dir" "$srv"
  port=$(start server "$dir" "$srv/scion-server" port --data "$dir/server" serve \
    --listen 127.0.0.1:0 --token matrix --ticket-key "$dir/ticket.key")
  story "$dir" "http://127.0.0.1:$port"
  stop "$dir"
}

# pair_2 DIRECTION CACHE_BIN SRV_BIN: the tier of one version before the
# server of the other. The client is always N: the pair under test is
# the tier against the server.
pair_2() {
  local side=$1 cache=$2 srv=$3 dir sport cport mport before
  PAIR='tier<->server' SIDE=$side SCN=$BIN
  dir=$(mktemp -d "$WORK/pair2-$side.XXXXXX")
  note "pair 2 ($side)"
  import "$dir" "$srv"
  sport=$(start server "$dir" "$srv/scion-server" port --data "$dir/server" serve \
    --listen 127.0.0.1:0 --token matrix --ticket-key "$dir/ticket.key" \
    --metrics-listen 127.0.0.1:0)
  mport=$(metrics_port "$dir/server.log")
  [ -n "$mport" ] || die "pair '$PAIR' ($SIDE): the server logged no metrics port"
  # The cache binds port 0 and its serving line names the address it
  # bound, as the server's does. An N-1 cache older than a826c71 logged
  # the port it asked for instead, 0, which nothing can connect to; for
  # such a pair, take the port beside the server's, which the kernel
  # never reserved for it.
  cport=$(start cache "$dir" "$cache/scion-cache" port --listen 127.0.0.1:0 \
    --upstream "http://127.0.0.1:$sport" \
    --ticket-public-key "$(cat "$dir/ticket.pub")")
  if [ "$cport" = 0 ]; then
    note "the N-1 cache predates a826c71: it cannot name its port"
    stop_one "$dir" cache
    cport=$((sport + 1))
    start cache "$dir" "$cache/scion-cache" port --listen "127.0.0.1:$cport" \
      --upstream "http://127.0.0.1:$sport" \
      --ticket-public-key "$(cat "$dir/ticket.pub")" > /dev/null
  fi
  # The read through the tier must match the direct read byte for byte.
  # The ref and its ticket come from the server; the objects ride the
  # cache, as cli.md documents the split.
  "$SCN/scn" --server "http://127.0.0.1:$sport" \
    --objects "http://127.0.0.1:$cport" cat main:README.md \
    >"$dir/through.txt" ||
    die "pair '$PAIR' ($SIDE): the read through the tier failed"
  "$SCN/scn" --server "http://127.0.0.1:$sport" cat main:README.md \
    >"$dir/direct.txt" ||
    die "pair '$PAIR' ($SIDE): the direct read failed"
  cmp -s "$dir/through.txt" "$dir/direct.txt" ||
    die "pair '$PAIR' ($SIDE): the read through the tier differs from the direct read"
  # The second read is the cache's: the server's GetObjects total stays.
  before=$(getobjects "http://127.0.0.1:$mport") ||
    die "pair '$PAIR' ($SIDE): the metrics scrape failed"
  "$SCN/scn" --server "http://127.0.0.1:$sport" \
    --objects "http://127.0.0.1:$cport" cat main:README.md > /dev/null ||
    die "pair '$PAIR' ($SIDE): the second read through the tier failed"
  [ "$(getobjects "http://127.0.0.1:$mport")" = "$before" ] ||
    die "pair '$PAIR' ($SIDE): the second read reached the server, the cache did not serve it"
  stop "$dir"
}

# pair_3 DIRECTION SCN_BIN DAEMON_BIN: the client of one version against
# the daemon of the other, over the unix:// path. The server stays at N:
# the pair under test is the socket pair, not the tier behind it.
pair_3() {
  local side=$1 scn=$2 daemon=$3 dir sport
  PAIR='scn<->daemon' SIDE=$side SCN=$scn
  dir=$(mktemp -d "$WORK/pair3-$side.XXXXXX")
  note "pair 3 ($side)"
  import "$dir" "$BIN"
  # A socket of this run's own: one per direction, so a slow daemon's
  # exit unlink cannot remove the next daemon's live socket.
  sock="$SOCK-$side"
  rm -f "$sock"
  sport=$(start server "$dir" "$BIN/scion-server" port --data "$dir/server" serve \
    --listen 127.0.0.1:0 --token matrix --ticket-key "$dir/ticket.key")
  start daemon "$dir" "$daemon/scion-daemon" serving --data "$dir/daemon-data" \
    --socket "$sock" --upstream "http://127.0.0.1:$sport"
  mkdir -p "$dir/out"
  "$scn/scn" --server "http://127.0.0.1:$sport" --objects "unix://$sock" \
    checkout main --into "$dir/out" > /dev/null ||
    die "pair '$PAIR' ($SIDE): the checkout over the socket failed"
  grep -q 'A small repository' "$dir/out/README.md" ||
    die "pair '$PAIR' ($SIDE): the checkout over the socket read the wrong bytes"
  # The --json rules of stability.md: the shape the CLI documents. The
  # output goes to a file first: a pipe closed at the first match kills
  # a client still printing.
  "$scn/scn" --server "http://127.0.0.1:$sport" --objects "unix://$sock" \
    log --json -n 1 >"$dir/json.out" 2>"$dir/json.err" ||
    die "pair '$PAIR' ($SIDE): the --json read failed"
  grep -q '"author"' "$dir/json.out" ||
    die "pair '$PAIR' ($SIDE): the --json shape changed"
  stop "$dir"
}

# pair_4: the store pair, one direction by design. A server of N opens a
# store of N-1, the migration runs forward, and reads and writes work on
# the migrated store. A store of N on a server of N-1 is refused with
# the version reason -- but only when the two builds write different
# schema versions at all.
pair_4() {
  local dir port cur_schema prev_schema
  PAIR=store SIDE='N opens N-1' SCN=$BIN
  dir=$(mktemp -d "$WORK/pair4.XXXXXX")
  note "pair 4 (N opens N-1)"
  import "$dir" "$PREV_BIN"
  "$BIN/scion-server" --data "$dir/server" fsck >"$dir/fsck.log" 2>&1 ||
    die "pair '$PAIR' ($SIDE): the N server cannot open the N-1 store"
  port=$(start server "$dir" "$BIN/scion-server" port --data "$dir/server" serve \
    --listen 127.0.0.1:0 --token matrix --ticket-key "$dir/ticket.key")
  story "$dir" "http://127.0.0.1:$port"
  stop "$dir"

  cur_schema=$(schema_of "$ROOT")
  prev_schema=$(schema_of "$PREV_SRC")
  if [ -z "$cur_schema" ] || [ -z "$prev_schema" ]; then
    die "pair 'store': cannot read SCHEMA_VERSION from one of the two builds"
  fi
  if [ "$cur_schema" = "$prev_schema" ]; then
    summary "pair 4: both builds write schema $cur_schema, so the N-1 refusal cannot fire on this pair"
    return 0
  fi
  SIDE='N-1 refuses N'
  note "pair 4 ($SIDE)"
  "$BIN/scion-server" --data "$dir/server2" import "$dir/src" >/dev/null 2>&1 ||
    die "pair '$PAIR' ($SIDE): the import of the N store failed"
  if "$PREV_BIN/scion-server" --data "$dir/server2" fsck >"$dir/refuse.log" 2>&1; then
    die "pair '$PAIR' ($SIDE): the N-1 server accepted a store of N"
  fi
  grep -q 'newer than this build' "$dir/refuse.log" ||
    die "pair '$PAIR' ($SIDE): the N-1 server refused the store of N, but not with the version reason"
}

command -v cargo > /dev/null || die "needs cargo"
command -v git > /dev/null || die "needs git"
command -v curl > /dev/null || die "needs curl"

# The ref first: a run with nothing to compare against says so before it
# builds anything.
ref=$(prev_ref)
if [ -z "$ref" ]; then
  # A workflow that resolved an N-1 ref hands it to the run through
  # PREV_REF. The resolve step also leaves it in this file, which no env
  # wiring can lose: a run that received no ref while the file is here
  # tested nothing, and it must fail loud, never pass as a skip.
  if [ -s version-matrix.prev-ref ]; then
    die "the workflow resolved N-1 $(cat version-matrix.prev-ref), but this run never received it (PREV_REF was empty)"
  fi
  summary "skipped: no release tag yet"
  exit 0
fi
note "N-1 is $ref"

if [ -n "${SCION_BIN:-}" ]; then
  BIN=$SCION_BIN
else
  note "building the N release binaries"
  (cd "$ROOT" && cargo build --release --locked --quiet) || die "step 'build N' failed"
fi
build_prev "$ref"

make_work
PAIR='' SIDE='' SCN=''

pair_1 cur "$BIN" "$PREV_BIN"
pair_1 prev "$PREV_BIN" "$BIN"
pair_2 cur "$BIN" "$PREV_BIN"
pair_2 prev "$PREV_BIN" "$BIN"
pair_3 cur "$BIN" "$PREV_BIN"
pair_3 prev "$PREV_BIN" "$BIN"
pair_4

summary "version matrix passed: N-1 $ref against N $(git -C "$ROOT" rev-parse --short HEAD), all four pairs"
