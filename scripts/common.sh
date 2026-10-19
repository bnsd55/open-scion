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
  [ -x /usr/bin/time ] || die "needs /usr/bin/time (Debian, Ubuntu: apt install time)"
}

# make_work NAME: makes $WORK, and arranges for cleanup on exit.
make_work() {
  WORK=$(mktemp -d "${TMPDIR:-/tmp}/scion-$1.XXXXXX")
  trap cleanup EXIT
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

# start NAME READY CMD...: runs CMD under /usr/bin/time -p, with its output in
# $WORK/NAME.log, and waits until that log shows READY.
start() {
  local name=$1 ready=$2 tries=300 pid
  shift 2
  /usr/bin/time -p "$@" > "$WORK/$name.log" 2>&1 &
  pid=$!
  echo "$pid" > "$WORK/$name.pid"
  while ! grep -q "$ready" "$WORK/$name.log"; do
    if ! kill -0 "$pid" 2> /dev/null; then
      tail -n 20 "$WORK/$name.log" >&2
      rm -f "$WORK/$name.pid"
      die "$name did not start"
    fi
    tries=$((tries - 1))
    [ "$tries" -gt 0 ] || die "$name did not start within 30 s"
    sleep 0.1
  done
}

# stop NAME: stops what `start NAME` started. Its times are then in its log.
stop() {
  local pid
  pid=$(cat "$WORK/$1.pid")
  rm -f "$WORK/$1.pid"
  # /usr/bin/time ignores signals while it waits, so signal its child.
  pkill -TERM -P "$pid" 2> /dev/null || true
  wait "$pid" 2> /dev/null || true
}

# The server, then a cache in front of it, then a daemon in front of the
# cache. The daemon keeps its objects in $WORK/daemon, from run to run.
# Object reads need a ticket: the server signs with one key, which it makes
# once in $WORK and keeps from run to run, and the cache verifies with the
# key's public half. The daemon only forwards the tickets.
start_scion() {
  if [ ! -f "$WORK/ticket.key" ]; then
    "$BIN/scion-server" ticket-key --out "$WORK/ticket.key" > "$WORK/ticket.pub"
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
  seq 1 "$jobs" | /usr/bin/time -p xargs -P "$jobs" -I{} sh -c \
    'sleep "$(printf "0.%02ds" $(( ($1 - 1) % 20 )))"; eval "$2"' \
    sh "$WORK/$name/{}" "$cmd" \
    > "$WORK/$name.log" 2>&1 || {
    tail -n 20 "$WORK/$name.log" >&2
    die "$name: a job failed"
  }
  nstat -az TcpExtListenOverflows TcpExtListenDrops > "$WORK/$name.nstat.1" 2>/dev/null || \
    netstat -s > "$WORK/$name.nstat.1" 2>/dev/null || true
}

# wall FILE: the wall time, in seconds, from the /usr/bin/time -p report in
# FILE. cpu FILE...: the user plus system CPU time of all the reports.
wall() { awk '$1 == "real" && NF == 2 { s = $2 } END { printf "%.2f\n", s }' "$1"; }
cpu() { awk '($1 == "user" || $1 == "sys") && NF == 2 { s += $2 } END { printf "%.2f\n", s }' "$@"; }

# file_bytes DIR [TEST...]: the total size of the files below DIR that pass
# the find(1) tests.
file_bytes() {
  find "$@" -type f -exec wc -c {} + | awk '$2 != "total" { s += $1 } END { print s + 0 }'
}
