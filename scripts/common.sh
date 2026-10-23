# shellcheck shell=bash
# Shared by demo.sh and bench-checkout.sh. Source it; do not run it.
#
# Everything goes into one private temp directory, $WORK. Every process runs
# under `/usr/bin/time -p`, so its wall and CPU time are at the end of its log
# when it stops. On exit, for any reason, the processes stop and $WORK is
# removed.

# The dataset: a public repository at a fixed commit (tag tokio-1.53.0, MIT).
REPO_URL=${REPO_URL:-https://github.com/tokio-rs/tokio}
REPO_COMMIT=${REPO_COMMIT:-be689a35f5ade5a39e507f79d3ec85cdab27806f}
# What each checkout writes: this directory and everything below it.
SPARSE_DIR=${SPARSE_DIR:-tokio/src}
SERVER_PORT=${SERVER_PORT:-17878}
CACHE_PORT=${CACHE_PORT:-17879}
SERVER_METRICS_PORT=${SERVER_METRICS_PORT:-17880}
CACHE_METRICS_PORT=${CACHE_METRICS_PORT:-17881}

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
case ${CARGO_TARGET_DIR:-} in
  "") BIN=$ROOT/target/release ;;
  /*) BIN=$CARGO_TARGET_DIR/release ;;
  *) BIN=$ROOT/$CARGO_TARGET_DIR/release ;;
esac
export SERVER=http://127.0.0.1:$SERVER_PORT

# The scripts give every address, name and directory as a flag, so the
# caller's environment must not change them. Git runs without the user's and
# the system's config, so local settings do not change the numbers.
unset SCION_SERVER SCION_REPO SCION_DATA SCION_DAEMON_DATA
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
# A new token for each run. The server and the tiers read it from here.
SCION_TOKEN=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
export SCION_TOKEN SCION_UPSTREAM_TOKEN=$SCION_TOKEN

die() {
  echo "$(basename "$0"): $*" >&2
  exit 1
}

# Progress goes to stderr; results go to stdout.
note() { echo "-- $*" >&2; }

# The tools that a plain Linux or macOS may lack. The rest are POSIX.
need() {
  local c
  for c in cargo git pkill seq curl; do
    command -v "$c" > /dev/null || die "needs $c"
  done
  # GNU timeout caps every client and maintenance call. A default Mac
  # has none: Homebrew installs it as gtimeout (issue #399, whose soak
  # machine failed every call at once because nothing checked).
  if command -v timeout > /dev/null 2>&1; then
    TIMEOUT=timeout
  elif command -v gtimeout > /dev/null 2>&1; then
    TIMEOUT=gtimeout
  else
    die "needs GNU timeout (macOS: brew install coreutils)"
  fi
  [ -x /usr/bin/time ] || die "needs /usr/bin/time (Debian, Ubuntu: apt install time)"
}

# du_sample CSV DATA_DIR NOW: append the store's size as `NOW,size`.
# du walks the live store: a temp file the server makes and deletes
# while du reads vanishes, du reports it on stderr and exits 1 (the
# 10-19 and 10-20 nightly failures, #561). A vanished file is churn,
# not a failure: the size stands. Any other du complaint, or a du that
# printed no size at all (a sample that reads nothing), fails the run.
du_sample() {
  local csv=$1 dir=$2 now=$3 err="$WORK/.du.err" size
  size=$(du -sk "$dir" 2> "$err") || {
    grep -v 'No such file or directory' "$err" > "$err.flt" || true
    if [ -s "$err.flt" ]; then
      cat "$err.flt" >&2
      echo "FAIL: du failed reading $dir" >&2
      return 1
    fi
  }
  if [ -z "$size" ]; then
    echo "FAIL: du printed no size for $dir: the sample reads nothing" >&2
    return 1
  fi
  printf '%s,%s\n' "$now" "$(printf '%s' "$size" | cut -f1)" >> "$csv"
}

# make_work NAME: makes $WORK, and arranges for cleanup on exit.
make_work() {
  WORK=$(mktemp -d "${TMPDIR:-/tmp}/scion-$1.XXXXXX")
  # The owned processes go first: on INT and TERM the shell exits, and
  # the EXIT trap stops what the script owns before the work directory
  # goes away.
  trap 'owned_cleanup; cleanup' EXIT
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 143' TERM
  # Resolve symlinks (macOS: /var is /private/var), as git daemon does.
  WORK=$(cd "$WORK" && pwd -P)
  SOCKET=$WORK/daemon.sock
  DAEMON=unix://$SOCKET
  # A Unix socket path has at most 103 bytes on macOS.
  [ "${#SOCKET}" -le 103 ] || die "temp path is too long for a Unix socket; set TMPDIR to a shorter directory"
}

cleanup() {
  local f
  [ -n "${WORK:-}" ] || return 0
  for f in "$WORK"/*.pid; do
    if [ -e "$f" ]; then stop "$(basename "$f" .pid)"; fi
  done
  # A failed run's only evidence is its logs: the servers', the batch's,
  # and the results. Setting SCION_KEEP_LOGS to a directory keeps them
  # there instead of losing them with $WORK. Unset: nothing changes.
  if [ -n "${SCION_KEEP_LOGS:-}" ] && [ -d "$WORK" ]; then
    mkdir -p "$SCION_KEEP_LOGS"
    for f in "$WORK"/*.log "$WORK"/*.nstat.* "$WORK"/results; do
      if [ -e "$f" ]; then cp "$f" "$SCION_KEEP_LOGS/$(basename "$WORK").$(basename "$f")"; fi
    done
  fi
  rm -rf "$WORK"
}

build() {
  note "building the release binaries"
  (cd "$ROOT" && cargo build --release --locked --quiet)
}

# Fetches the history of $REPO_COMMIT into $WORK/src.git, a bare repository
# whose only branch, main, points at it.
fetch_dataset() {
  note "fetching $REPO_URL at $REPO_COMMIT"
  git init -q --bare --initial-branch=main "$WORK/src.git"
  git -C "$WORK/src.git" fetch -q "$REPO_URL" "$REPO_COMMIT"
  git -C "$WORK/src.git" update-ref refs/heads/main "$REPO_COMMIT"
}

# Imports $WORK/src.git into a new server data directory, $WORK/server, and
# sets $COMMITS. Its times are in $WORK/import.log.
import_dataset() {
  note "importing"
  /usr/bin/time -p "$BIN/scion-server" --data "$WORK/server" import --rev main "$WORK/src.git" \
    > "$WORK/import.out" 2> "$WORK/import.log" || {
    tail -n 20 "$WORK/import.log" >&2
    die "import failed"
  }
  # shellcheck disable=SC2034 # used by the scripts that source this file
  COMMITS=$(sed -n 's/^imported [0-9]* of \([0-9]*\) commits.*/\1/p' "$WORK/import.out")
}

# start NAME READY CMD...: runs CMD in its own process group, with its
# output in $WORK/NAME.log, and waits until that log shows READY. The
# group, the pid and the command line go into $WORK/owned/NAME, and the
# script's exit traps stop the group: a script that dies by error,
# interrupt or timeout leaves no server behind it. (The orphans that
# ran for hours were servers with no one left to stop them.)
start() {
  local name=$1 ready=$2 tries=300 pid job
  shift 2
  mkdir -p "$WORK/owned"
  # The layout: the job ($!, a child of this shell) runs
  # /usr/bin/time; time forks the wrapper perl, which puts itself in a
  # group of its own — the group id is the wrapper's pid — and execs
  # the program. The group holds the program alone; time stays in this
  # shell's group, which is why stop waits on the job: it is what
  # makes time's report land in the log before stop returns.
  OWNED_INFO="$WORK/.owned.$name" OWNED_CMD="$*" \
    /usr/bin/time -p perl -e '
      use POSIX qw(setpgid);
      setpgid(0, 0) or die "setpgid: $!\n";
      open(my $fh, ">", $ENV{OWNED_INFO}) or die "record: $!\n";
      print $fh "pid=$$\npgid=", getpgrp(0), "\n";
      close $fh;
      exec @ARGV or die "exec: $!\n";
    ' -- "$@" > "$WORK/$name.log" 2>&1 &
  job=$!
  tries=0
  while [ ! -s "$WORK/.owned.$name" ] && [ "$tries" -lt 50 ]; do
    kill -0 "$job" 2> /dev/null || break
    sleep 0.1
    tries=$((tries + 1))
  done
  # The record: pid and pgid from perl (the group itself), the program
  # name and the command line from the shell. A job that died before it
  # recorded is recorded as the shell saw it; the readiness loop below
  # fails the start either way.
  if [ -s "$WORK/.owned.$name" ]; then
    { cat "$WORK/.owned.$name"; echo "job=$job"; echo "cmd=$*"; } \
      > "$WORK/owned/$name"
  else
    { echo "pid=$job"; echo "pgid="; echo "job=$job"; echo "cmd=$*"; } \
      > "$WORK/owned/$name"
  fi
  rm -f "$WORK/.owned.$name"
  pid=$job
  # Its own counter: the record loop above runs down its own budget.
  # SHORTCUT: nothing sweeps a pidfile left by a script that died by
  # SIGKILL — the trap cannot run then. The janitor sweep is the named
  # upgrade path if stale servers show up.
  tries_ready=300
  while ! grep -q "$ready" "$WORK/$name.log"; do
    if ! kill -0 "$pid" 2> /dev/null; then
      tail -n 20 "$WORK/$name.log" >&2
      rm -f "$WORK/owned/$name"
      die "$name did not start"
    fi
    tries_ready=$((tries_ready - 1))
    [ "$tries_ready" -gt 0 ] || die "$name did not start within 30 s"
    sleep 0.1
  done
}

# Out of scope, said where a reader would look for it: a job that calls
# setsid leaves its group and survives every stop here. Nothing in this
# repository starts one.

# owned_kill PID PGID: stop one owned job. With a group, TERM the group
# and KILL it after five seconds; without one, TERM the pid and its
# children. Either way the caller has already checked the pidfile.
owned_kill() {
  local pid=$1 pgid=$2
  if [ -n "$pgid" ]; then
    kill -TERM "-$pgid" 2> /dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "$pid" 2> /dev/null || return 0
      sleep 0.5
    done
    kill -KILL "-$pgid" 2> /dev/null || true
  else
    kill -TERM "$pid" 2> /dev/null || true
    pkill -TERM -P "$pid" 2> /dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "$pid" 2> /dev/null || return 0
      sleep 0.5
    done
    kill -KILL "$pid" 2> /dev/null || true
    pkill -KILL -P "$pid" 2> /dev/null || true
  fi
}

# owned_cleanup: stop every owned process the script leaves behind.
# Runs from the exit traps; safe to call twice.
OWNED_CLEANUP_DONE=
owned_cleanup() {
  [ -n "$OWNED_CLEANUP_DONE" ] && return 0
  OWNED_CLEANUP_DONE=1
  local f
  [ -n "${WORK:-}" ] && [ -d "$WORK/owned" ] || return 0
  for f in "$WORK"/owned/*; do
    [ -e "$f" ] || continue
    owned_stop_one "$f"
  done
}

# owned_stop_one PIDFILE: the one process one pidfile names. The group
# is the identity, not a name: a live group holds the recorded id, and
# the system does not hand out a pid that is still a live group's id,
# so a recorded group with members is ours whatever program it runs —
# a wrapper that execed another program is still ours to stop. A
# record with no group (a function loop) falls back to the pid, and a
# live pid whose program is not the recorded one is skipped, with a
# line saying so: silence here is how a skipped cleanup hides.
#
# Either way, the kill bounds the leader, not the members: a member
# that missed the TERM (one only starting as the TERM landed) can
# still be writing into $WORK while the caller's rm removes it —
# #561's "rm: Directory not empty". So after the kill, wait, bounded,
# until the leader, its group and its direct children are all gone,
# KILL whatever is still live on each round, and only then release
# the pidfile.
owned_stop_one() {
  local f=$1 pid pgid job have n stopped
  pid=$(sed -n 's/^pid=//p' "$f")
  pgid=$(sed -n 's/^pgid=//p' "$f")
  job=$(sed -n 's/^job=//p' "$f")
  [ -n "$pid" ] || { rm -f "$f"; return 0; }
  stopped=
  if [ -n "$pgid" ] && kill -0 "-$pgid" 2> /dev/null; then
    owned_kill "$pid" "$pgid"
    stopped=1
  elif kill -0 "$pid" 2> /dev/null; then
    have=$(ps -o comm= -p "$pid" 2> /dev/null)
    case "$have" in
      *bash)
        owned_kill "$pid" ""
        stopped=1
        ;;
      *)
        echo "owned: skipped $f: pid $pid runs '$have', not what the record names" >&2
        ;;
    esac
  fi
  # The member wait, only for a record the kill ran on: a skipped
  # record is not ours, and its pid must not be touched. The kill
  # bounds the leader, and a member it missed would race the rm that
  # follows the cleanup. Wait them out, KILL on each live round, then
  # release the record.
  [ -n "$stopped" ] || { rm -f "$f"; return 0; }
  n=0
  while [ "$n" -lt 10 ]; do
    if ! kill -0 "$pid" 2> /dev/null \
      && ! { [ -n "$pgid" ] && kill -0 "-$pgid" 2> /dev/null; } \
      && ! pgrep -P "$pid" > /dev/null 2>&1; then
      break
    fi
    [ -n "$pgid" ] && kill -KILL "-$pgid" 2> /dev/null
    kill -KILL "$pid" 2> /dev/null
    pkill -KILL -P "$pid" 2> /dev/null
    sleep 0.5
    n=$((n + 1))
  done
  rm -f "$f"
  wait "$job" 2> /dev/null || true
}

# loop_bg NAME FUNC ARGS...: runs FUNC in the background as one the
# script owns, recorded like a `start`ed server. A loop is the script's
# own shell, so it has no process group of its own; the pidfile still
# survives a script that was killed outright, where a janitor sweep can
# find it. # SHORTCUT: no process group for function loops — bash only
# grants one to an exec'd program; the upgrade path is a dispatch
# subcommand the script re-invokes for the loop.
loop_bg() {
  local name=$1 pid
  shift
  mkdir -p "$WORK/owned"
  "$@" &
  pid=$!
  {
    echo "pid=$pid"
    echo "pgid="
    echo "comm=bash"
    echo "cmd=loop $name: $*"
  } > "$WORK/owned/$name"
}

# stop NAME: stops what `start NAME` started, through its pidfile. Its
# times are then in its log.
stop() {
  local name=$1 f="$WORK/owned/$1"
  [ -e "$f" ] || { rm -f "$WORK/$1.pid" 2> /dev/null; return 0; }
  owned_stop_one "$f"
}

# The server, then a cache in front of it, then a daemon in front of the
# cache. The daemon keeps its objects in $WORK/daemon, from run to run.
# Object reads need a ticket: the server signs with one key, which it makes
# once in $WORK and keeps from run to run, and the cache verifies with the
# key's public half. The daemon only forwards the tickets.
start_scion() {
  if [ ! -f "$WORK/ticket.key" ]; then
    "$BIN/scion-server" --data "$WORK/server" ticket-key --out "$WORK/ticket.key" > "$WORK/ticket.pub"
  fi
  start server serving "$BIN/scion-server" --data "$WORK/server" serve \
    --listen "127.0.0.1:$SERVER_PORT" \
    --ticket-key "$WORK/ticket.key" \
    --metrics-listen "127.0.0.1:$SERVER_METRICS_PORT"
  start cache serving "$BIN/scion-cache" --listen "127.0.0.1:$CACHE_PORT" \
    --upstream "$SERVER" \
    --ticket-public-key "$(cat "$WORK/ticket.pub")" \
    --metrics-listen "127.0.0.1:$CACHE_METRICS_PORT"
  start daemon serving "$BIN/scion-daemon" --data "$WORK/daemon" --socket "$SOCKET" \
    --upstream "http://127.0.0.1:$CACHE_PORT"
}

# scrape_metrics URL: the total of the out-bytes counters of the registry at
# URL. A counter that never incremented does not render, hence the +0.
scrape_metrics() {
  command curl -fsS "$1/metrics" | awk '
    /^scion_rpc_bytes_total\{.*direction="out"/ { s += $NF }
    END { print s + 0 }'
}

# getobjects URL: the total of the GetObjects request counters at URL,
# over every code. A counter that never incremented does not render,
# hence the +0.
getobjects() {
  command curl -fsS "$1/metrics" | awk '
    /scion_requests_total\{rpc="Objects\/GetObjects"/ { s += $NF }
    END { print s + 0 }'
}

stop_scion() {
  stop daemon
  stop cache
  stop server
}

# One sparse checkout through the daemon, into "$1". The ref and the ticket
# come from the server, the objects ride the daemon. For `batch`, in the
# scripts that source this file.
# shellcheck disable=SC2016,SC2034 # expanded by the sh that batch starts
CHECKOUT='"$BIN/scn" --server "$SERVER" --objects "$DAEMON" checkout main --sparse "$SPARSE_DIR" --into "$1" > /dev/null'

# batch NAME JOBS CMD: runs the sh command CMD in JOBS processes at once, under
# /usr/bin/time -p. In each, $1 is its own new directory, $WORK/NAME/<job>.
# All their output goes to $WORK/NAME.log. Fails if any job fails.
batch() {
  local name=$1 jobs=$2 cmd=$3
  export BIN DAEMON SPARSE_DIR
  mkdir "$WORK/$name"
  # Each job starts 10 ms after the one before it, round-robin over 20
  # steps. All jobs starting at once can overflow a server's accept
  # queue: git daemon listens with a backlog of 5 (daemon.c:
  # listen(sockfd, 5)), and a connection that completes its handshake
  # onto a full queue is refused - with tcp_abort_on_overflow=0 the
  # kernel drops the final ACK and the client's reset comes when it
  # sends its first bytes, which is why the failure landed mid-run as
  # "read error: Connection reset by peer". A start every 10 ms keeps
  # the queue near empty; the half second it adds to a 50-job batch is
  # inside the noise, and both tools pay it alike. nstat's
  # TcpExtListenOverflows and TcpExtListenDrops, taken around every
  # batch, say whether the queue ever overflowed.
  # SHORTCUT: stagger the starts because git daemon's backlog is 5 and
  # cannot be set; the ceiling is a lazy-fetch burst during checkout;
  # the upgrade is a real git server (http-backend) in the rig.
  nstat -az TcpExtListenOverflows TcpExtListenDrops > "$WORK/$name.nstat.0" 2>/dev/null || \
    netstat -s > "$WORK/$name.nstat.0" 2>/dev/null || true
  # The job's number drives the stagger, and its path is the checkout
  # target, so sh gets both: $3 is the number, $2 the command, and $1 the
  # path that the command's own "$1" reads.
  local rc=0
  seq 1 "$jobs" | /usr/bin/time -p xargs -P "$jobs" -I{} sh -c \
    'sleep "$(printf "0.%02ds" $(( ($3 - 1) % 20 )))"; eval "$2"' \
    sh "$WORK/$name/{}" "$cmd" {} \
    > "$WORK/$name.log" 2>&1 || rc=$?
  # The counters land even when the batch failed: an overflow during a
  # failed batch is exactly the evidence the run exists for.
  nstat -az TcpExtListenOverflows TcpExtListenDrops > "$WORK/$name.nstat.1" 2>/dev/null || \
    netstat -s > "$WORK/$name.nstat.1" 2>/dev/null || true
  [ "$rc" -eq 0 ] || {
    tail -n 20 "$WORK/$name.log" >&2
    die "$name: a job failed"
  }
}

# wall FILE: the wall time, in seconds, from the /usr/bin/time -p report in
# FILE. cpu FILE...: the user plus system CPU time of all the reports.

# The slow client: it holds a ReadFile stream open and drains it at a
# crawl, so back pressure reaches the server (issue #239). The crawl is
# dd, one 4 KiB block at a time with a pause between: `read -N` exists
# in neither dash (Ubuntu's /bin/sh) nor bash 3.2 (macOS's /bin/sh and
# /bin/bash), so a reader built on it exited at once - hidden by `||
# true` - and no nightly ever held a stream (issue #399). Each drain
# logs epoch,bytes,held into $WORK/slow-held.csv, and
# check_slow_stream fails the run on a drain that read fewer bytes
# than the file has and did not end inside a kill window: it ended
# early while the server was still serving, which is what the old
# reader did on every pass.
SLOW_FILE=${SLOW_FILE:-main:tokio/CHANGELOG.md}
SLOW_DRAIN_CAP=${SLOW_DRAIN_CAP:-180}

slow_reader() {
  local t0 now total
  while [ "$(date +%s)" -lt "$END" ]; do
    t0=$(date +%s)
    # shellcheck disable=SC2016 # the drain reads its own $1 and $2
    total=$("$TIMEOUT" "$SLOW_DRAIN_CAP" sh -c '
      eval "$1" | {
        total=0
        while :; do
          dd bs=4096 count=1 of="$2" 2> /dev/null || break
          sz=$(wc -c < "$2")
          [ "$sz" -gt 0 ] || break
          total=$((total + sz))
          sleep 0.25
        done
        echo "$total"
      }' drain "$BIN/scn --server $SERVER cat $SLOW_FILE" "$WORK/slow.block" \
      2>> "$WORK/slow.log")
    [ -n "$total" ] || total=0
    now=$(date +%s)
    echo "$(date +%s),$total,$((now - t0))" >> "$WORK/slow-held.csv"
    sleep 1
  done
}

# count_fds PID: the server's open files. macOS has no /proc, where a
# find of /proc/PID/fd failed into /dev/null and every count was 0, so
# the open-files bound passed vacuously (issue #399); there it is
# lsof, which costs a few ms once every 10 s. Linux reads /proc.
count_fds() {
  case $(uname -s) in
    Darwin) lsof -p "$1" 2> /dev/null | tail -n +2 | wc -l | tr -d ' ' ;;
    *) find "/proc/$1/fd" -maxdepth 1 2> /dev/null | wc -l | tr -d ' ' ;;
  esac
}

# check_slow_stream EXPECTED: EXPECTED is the file's byte size, taken
# once while the server was fresh. A drain that read fewer bytes and
# did not overlap a kill - soak-fault's restarts.csv names them; the
# plain soak has none - ended early while the server was still
# serving, and fails the run (issue #399). A run whose slow reader
# held no stream at all fails the same way: a measurement that reads
# nothing is a broken measurement, not a passing one.
check_slow_stream() {
  local expected=$1 red=0 any=0 start bytes held end kills
  kills=""
  if [ -f "$WORK/restarts.csv" ]; then
    kills=$(cut -d' ' -f3 "$WORK/restarts.csv" 2> /dev/null)
  fi
  while IFS=, read -r start bytes held; do
    [ -n "$start" ] || continue
    if [ "$bytes" -ge "$expected" ]; then
      # This drain delivered the whole file: the reader held a stream.
      any=1
      continue
    fi
    end=$((start + held))
    for k in $kills; do
      [ "$start" -le $((k + 5)) ] && [ "$end" -ge $((k - 5)) ] && continue 2
    done
    echo "FAIL: the slow reader ended early: $bytes of $expected bytes, held ${held}s" >&2
    red=1
    break
  done < "$WORK/slow-held.csv"
  if [ "$any" -eq 0 ]; then
    echo "FAIL: the slow reader held no stream: no drain delivered the file" >&2
    return 1
  fi
  return "$red"
}
wall() { awk '$1 == "real" && NF == 2 { s = $2 } END { printf "%.2f\n", s }' "$1"; }
cpu() { awk '($1 == "user" || $1 == "sys") && NF == 2 { s += $2 } END { printf "%.2f\n", s }' "$@"; }

# file_bytes DIR [TEST...]: the total size of the files below DIR that pass
# the find(1) tests.
file_bytes() {
  find "$@" -type f -exec wc -c {} + | awk '$2 != "total" { s += $1 } END { print s + 0 }'
}
