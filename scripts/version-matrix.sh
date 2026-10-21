#!/usr/bin/env bash
# The version matrix (issue #239, part 1): the pairs of
# docs/compatibility.md, at versions N (this tree) and N-1, so a release
# that breaks a listed pair is caught by CI, not by an operator.
#
# N-1 is PREV_REF when set (any commit or tag), else the newest release
# tag that is an ancestor of HEAD and not HEAD itself: when HEAD carries
# the release tag, N-1 is the one before it. With no tag and no
# PREV_REF, N-1 is the newest commit of the stand-in ladder below - the
# same two pinned commits scripts/store-upgrade.sh runs, chosen where
# the store schema changed, so every leg has something real to prove
# from the first nightly on. A run with nothing to compare against
# fails; the matrix never passes as a skip.
# SHORTCUT: the pins stand in for release tags; the first release tag
# replaces them, because a release tag is an ancestor of HEAD and the
# pins sit on main.
#
# The pairs, and what each proves:
#
#   1. scn <-> server, both directions: the release-smoke story (import,
#      ticket key, log, ls, cat, blame, commit, land, read back) answers
#      inside the v1 contract with the client and the server at opposite
#      versions.
#   2. tier <-> server, both directions: a checkout through the cache
#      matches a direct checkout byte for byte, the ticket verifies (the
#      cache holds its public half), and the second checkout is served
#      by the cache: the server's GetObjects counter does not grow.
#   3. scn <-> daemon over the unix:// path, both directions: the
#      checkout reads what cli.md documents, and --json keeps its shape.
#      The server stays at N: the pair under test is the socket pair.
#   4. the store, one direction only (docs/compatibility.md): a server
#      of N opens a store of N-1, the migration runs forward, and reads
#      and writes then work on the migrated store. A store of N on a
#      server of N-1 is refused with the version reason -- but only when
#      the two builds write different schema versions at all; when they
#      share one, the script says so instead of failing.
#   5. the ticket window, both directions (ROADMAP v0.6.0: tickets
#      follow the N and N-1 mixed-version window): the ticket one
#      version's server signs verifies on the tier of the other version,
#      and both versions' tiers refuse a ticket of another key with the
#      ticket reason - the claims cross the window, not just the bytes.
#   6. the snapshot window, both directions (ROADMAP v0.6.0: snapshots
#      follow the N and N-1 mixed-version window): the snapshot one
#      version's cache writes opens as the other version's daemon seed,
#      and a checkout through that daemon reads the repo whole.
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

# The stand-in ladder of scripts/store-upgrade.sh (coder3, #428): until
# the first release tag, N-1 is the newest of these two pinned commits,
# chosen where the store schema changed (8: read rules per folder, 9:
# view memo). One resolution, two users: whoever lands second rebases
# onto the other's form.
STANDIN_TAGS=(
  32a6117bf54510450b1bfea10b6c873dcb383207 # view memo: schema 9 (newest)
  afe1c4db878f38fa22bd6adddd57a8c410668db5 # read rules per folder: schema 8
)

note() { echo "-- $*" >&2; }

# summary LINE: what a CI run reads. Into the step summary when there is
# one; to stderr always, so a local run sees it too.
summary() {
  echo "$*" >&2
  [ -z "${GITHUB_STEP_SUMMARY:-}" ] || echo "$*" >> "$GITHUB_STEP_SUMMARY"
}

# prev_ref: the ref to test as N-1, per the header. There is always
# one: PREV_REF, else the newest release tag behind HEAD, else the
# newest stand-in commit.
prev_ref() {
  if [ -n "${PREV_REF:-}" ]; then
    printf '%s\n' "$PREV_REF"
    return 0
  fi
  local tag head
  head=$(git -C "$ROOT" rev-parse HEAD)
  tag=$(git -C "$ROOT" tag --list 'v[0-9]*' --sort=-version:refname --merged=HEAD |
    while read -r t; do
      if [ "$(git -C "$ROOT" rev-parse "$t^{commit}")" != "$head" ]; then
        printf '%s\n' "$t"
        break
      fi
    done)
  if [ -n "$tag" ]; then
    printf '%s\n' "$tag"
  else
    printf '%s\n' "${STANDIN_TAGS[0]}"
  fi
}

# --resolve-ref: print the N-1 ref and exit, so a workflow can key its
# cache on the commit before any build runs. There is always a ref, so
# this never skips.
if [ "${1:-}" = "--resolve-ref" ]; then
  prev_ref
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
# The daemon's socket: in a directory the run owns (the daemon refuses
# a socket whose parent another user owns, and a CI runner's /tmp is
# root's), and short (the pair's temp path on macOS is longer than the
# Unix socket limit of 104 bytes). One socket per direction, named by
# the pid: a slow daemon's exit unlink cannot reach the next one's.
SOCKDIR=$(mktemp -d "${TMPDIR:-/tmp}/vmsock.XXXXXX")
SOCKDIR=$(cd "$SOCKDIR" && pwd -P)
SOCK="$SOCKDIR/matrix-$$.sock"

cleanup() {
  local rc=$1 f dir
  rm -rf "$SOCKDIR"
  [ -n "$WORK" ] || return 0
  for f in "$WORK"/*/owned; do
    [ -d "$f" ] || continue
    stop "$(dirname "$f")"
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
  mkdir -p "$dir/owned"
  # perl puts the job in its own process group, so the group's id is
  # the recorded pid, and the exit traps stop the group. The direct
  # process is the binary itself (no time wrapper here), so the
  # pidfile's comm is the binary's basename.
  perl -e 'use POSIX qw(setpgid); setpgid(0,0); exec @ARGV or die "exec: $!\n"' -- "$bin" "$@" >"$log" 2>&1 &
  pid=$!
  {
    echo "pid=$pid"
    echo "pgid=$pid"
    echo "comm=$(basename "$bin")"
    echo "cmd=$bin $*"
  } >"$dir/owned/$name"
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
  local dir=$1 name=$2 f="$1/owned/$2" pid pgid comm have n
  [ -e "$f" ] || return 0
  pid=$(sed -n 's/^pid=//p' "$f")
  pgid=$(sed -n 's/^pgid=//p' "$f")
  comm=$(sed -n 's/^comm=//p' "$f")
  rm -f "$f" "$1/$2.log.pid"
  [ -n "$pid" ] || return 0
  # The pidfile's job must still be the one it recorded: a pid the
  # system has since given to another program is left alone.
  have=$(ps -o comm= -p "$pid" 2> /dev/null)
  case "$have" in
    "") return 0 ;;
    *"$comm") ;;
    *) return 0 ;;
  esac
  kill -TERM "-$pgid" 2> /dev/null || kill -TERM "$pid" 2> /dev/null || true
  n=0
  while kill -0 "$pid" 2> /dev/null && [ "$n" -lt 100 ]; do
    sleep 0.1
    n=$((n + 1))
  done
  if kill -0 "$pid" 2> /dev/null; then
    note "$name (pid $pid) ignored TERM; sent KILL"
    kill -KILL "-$pgid" 2> /dev/null || kill -KILL "$pid" 2> /dev/null || true
  fi
}

# stop DIR: stop every process `start DIR` started.
stop() {
  local dir=$1 f
  for f in "$dir"/owned/*; do
    [ -e "$f" ] || return 0
    stop_one "$dir" "$(basename "$f")"
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
    --listen 127.0.0.1:0 --ticket-key "$dir/ticket.key")
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
    --listen 127.0.0.1:0 --ticket-key "$dir/ticket.key" \
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
  # The read is a checkout, not a cat: cat reads the file at the server
  # (ReadFile) and never reaches the tier, so a cat would prove nothing
  # about the pair. The ref and its ticket come from the server; the
  # objects ride the cache, as cli.md documents the split.
  "$SCN/scn" --server "http://127.0.0.1:$sport" \
    --objects "http://127.0.0.1:$cport" checkout main --into "$dir/through" > /dev/null ||
    die "pair '$PAIR' ($SIDE): the checkout through the tier failed"
  "$SCN/scn" --server "http://127.0.0.1:$sport" checkout main --into "$dir/direct" > /dev/null ||
    die "pair '$PAIR' ($SIDE): the direct checkout failed"
  cmp -s "$dir/through/README.md" "$dir/direct/README.md" ||
    die "pair '$PAIR' ($SIDE): the checkout through the tier differs from the direct checkout"
  # The second checkout is the cache's: the server's GetObjects total
  # stays.
  before=$(getobjects "http://127.0.0.1:$mport") ||
    die "pair '$PAIR' ($SIDE): the metrics scrape failed"
  "$SCN/scn" --server "http://127.0.0.1:$sport" \
    --objects "http://127.0.0.1:$cport" checkout main --into "$dir/again" > /dev/null ||
    die "pair '$PAIR' ($SIDE): the second checkout through the tier failed"
  [ "$(getobjects "http://127.0.0.1:$mport")" = "$before" ] ||
    die "pair '$PAIR' ($SIDE): the second checkout reached the server, the cache did not serve it"
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
    --listen 127.0.0.1:0 --ticket-key "$dir/ticket.key")
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

# pair_5 DIRECTION CACHE_BIN SRV_BIN: the ticket window, both
# directions. The ticket one version's server signs must verify on the
# tier of the other version; and a ticket of a key that is not the
# server's must be refused by the other version's tier with the ticket
# reason - the claims cross the window, not just the bytes.
pair_5() {
  local side=$1 cache=$2 srv=$3 dir sport cport
  PAIR='ticket window' SIDE=$side SCN=$BIN
  dir=$(mktemp -d "$WORK/pair5-$side.XXXXXX")
  note "pair 5 ($side)"
  import "$dir" "$srv"
  # A key that is not the server's, made by the OTHER build's tool: if
  # the two versions disagreed on the key format, the refusal below
  # would catch it.
  "$cache/scion-server" ticket-key --out "$dir/wrong.key" \
    >"$dir/wrong.pub" || die "pair '$PAIR' ($SIDE): the second ticket-key failed"
  sport=$(start server "$dir" "$srv/scion-server" port --data "$dir/server" serve \
    --listen 127.0.0.1:0 --ticket-key "$dir/ticket.key")
  # The positive: the server's ticket verifies on the other version's
  # tier, and the read matches the source.
  cport=$(start cache "$dir" "$cache/scion-cache" port --listen 127.0.0.1:0 \
    --upstream "http://127.0.0.1:$sport" \
    --ticket-public-key "$(cat "$dir/ticket.pub")")
  if [ "$cport" = 0 ]; then
    stop_one "$dir" cache
    cport=$((sport + 1))
    start cache "$dir" "$cache/scion-cache" port --listen "127.0.0.1:$cport" \
      --upstream "http://127.0.0.1:$sport" \
      --ticket-public-key "$(cat "$dir/ticket.pub")" > /dev/null
  fi
  "$SCN/scn" --server "http://127.0.0.1:$sport" \
    --objects "http://127.0.0.1:$cport" checkout main --into "$dir/through" > /dev/null ||
    die "pair '$PAIR' ($SIDE): the ticket of one version did not verify on the tier of the other"
  cmp -s "$dir/through/README.md" "$dir/src/README.md" ||
    die "pair '$PAIR' ($SIDE): the checkout through the tier read the wrong bytes"
  stop_one "$dir" cache
  # The negative: the wrong key, the other version's tier refuses with
  # the ticket reason - not a crash, not a hang. The read is a checkout:
  # it is GetObjects, the call the ticket gates.
  start cache "$dir" "$cache/scion-cache" serving --listen "127.0.0.1:$((sport + 2))" \
    --upstream "http://127.0.0.1:$sport" \
    --ticket-public-key "$(cat "$dir/wrong.pub")" > /dev/null
  if "$SCN/scn" --server "http://127.0.0.1:$sport" \
    --objects "http://127.0.0.1:$((sport + 2))" checkout main --into "$dir/wrong" \
    >"$dir/wrong.out" 2>"$dir/wrong.err"; then
    die "pair '$PAIR' ($SIDE): the tier of the other version accepted a ticket of another key"
  fi
  grep -q 'a GetObjects ticket was refused' "$dir/cache.log" ||
    die "pair '$PAIR' ($SIDE): the wrong-key refusal named no ticket reason"
  stop "$dir"
}

# pair_6 DIRECTION CACHE_BIN DAEMON_BIN: the snapshot window, both
# directions. The snapshot one version's cache writes opens as the
# other version's daemon seed (operations.md's warm start), and a
# checkout through that daemon reads the repo whole.
pair_6() {
  local side=$1 cache=$2 daemon=$3 dir sock sport tries
  PAIR='snapshot window' SIDE=$side SCN=$BIN
  dir=$(mktemp -d "$WORK/pair6-$side.XXXXXX")
  note "pair 6 ($side)"
  import "$dir" "$BIN"
  sport=$(start server "$dir" "$BIN/scion-server" port --data "$dir/server" serve \
    --listen 127.0.0.1:0 --ticket-key "$dir/ticket.key")
  # The cache of one version writes the snapshot. Its memory store only
  # holds what went through it, so a checkout through the cache first,
  # then the file appears within an interval; one settle beat after it
  # appears, because a half-written seed is not the window under test.
  mkdir -p "$dir/snaps"
  start cache "$dir" "$cache/scion-cache" serving --listen "127.0.0.1:$((sport + 1))" \
    --upstream "http://127.0.0.1:$sport" \
    --ticket-public-key "$(cat "$dir/ticket.pub")" \
    --snapshot "$dir/snaps" --snapshot-repo main --snapshot-every 2 > /dev/null
  "$SCN/scn" --server "http://127.0.0.1:$sport" \
    --objects "http://127.0.0.1:$((sport + 1))" checkout main --into "$dir/warm" > /dev/null ||
    die "pair '$PAIR' ($SIDE): the checkout that warms the cache failed"
  tries=75
  until [ -s "$dir/snaps/main.scnsnap" ]; do
    sleep 0.2
    tries=$((tries - 1))
    [ "$tries" -gt 0 ] || die "pair '$PAIR' ($SIDE): the cache wrote no snapshot file"
  done
  sleep 1
  stop_one "$dir" cache
  # The daemon of the other version starts warm from it, and a checkout
  # through it reads the repo whole. An older daemon seeds without the
  # say-so flag: --seed-full-repo arrived with the refusal that needs
  # it, and the stand-in commits predate it.
  sock="$SOCK-snap-$side"
  rm -f "$sock"
  if "$daemon/scion-daemon" --help 2>&1 | grep -q -- '--seed-full-repo'; then
    start daemon "$dir" "$daemon/scion-daemon" serving --data "$dir/daemon-data" \
      --socket "$sock" --upstream "http://127.0.0.1:$sport" \
      --seed "$dir/snaps/main.scnsnap" --seed-full-repo
  else
    note "the N-1 daemon predates --seed-full-repo: it seeds without the say-so"
    start daemon "$dir" "$daemon/scion-daemon" serving --data "$dir/daemon-data" \
      --socket "$sock" --upstream "http://127.0.0.1:$sport" \
      --seed "$dir/snaps/main.scnsnap"
  fi
  mkdir -p "$dir/out"
  "$SCN/scn" --server "http://127.0.0.1:$sport" --objects "unix://$sock" \
    checkout main --into "$dir/out" > /dev/null ||
    die "pair '$PAIR' ($SIDE): the checkout from the other version's snapshot failed"
  cmp -s "$dir/out/README.md" "$dir/src/README.md" ||
    die "pair '$PAIR' ($SIDE): the checkout from the snapshot read the wrong bytes"
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
    --listen 127.0.0.1:0 --ticket-key "$dir/ticket.key")
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

# The ref first: there is always one (PREV_REF, a release tag, or the
# stand-in ladder), and a run that tests no pair is a failed run, never
# a skip. A workflow that resolved an N-1 ref hands it to the run
# through PREV_REF; the resolve step also leaves it in this file, which
# no env wiring can lose: a run whose file is here but whose PREV_REF
# is empty lost its wiring, and it must fail loud.
ref=$(prev_ref)
if [ -z "${PREV_REF:-}" ] && [ -s version-matrix.prev-ref ]; then
  die "the workflow resolved N-1 $(cat version-matrix.prev-ref), but this run never received it (PREV_REF was empty)"
fi
# The file has said all it can: the run has its ref, and a leftover
# must not reach the next run on a reused runner.
rm -f version-matrix.prev-ref
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
pair_5 cur "$BIN" "$PREV_BIN"
pair_5 prev "$PREV_BIN" "$BIN"
pair_6 cur "$BIN" "$PREV_BIN"
pair_6 prev "$PREV_BIN" "$BIN"

summary "version matrix passed: N-1 $ref against N $(git -C "$ROOT" rev-parse --short HEAD), all six pairs (scn, tier, daemon, store, ticket window, snapshot window)"
