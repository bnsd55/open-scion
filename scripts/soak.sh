#!/usr/bin/env bash
# A soak: one server under a mixed load for RUN_MINUTES (issue #239, part
# 2). Writers land with scn bench, partial readers check out through the
# cache and the daemon, an import runs beside the server, gc collects, a
# loop cancels calls by killing their processes, and one reader reads
# slowly, holding a server stream open. Every 10 s the script samples the
# server's RSS, its open files, and one end-to-end read timed in ms, into
# a CSV. At the end it fails if memory or files grew past their bound, if
# late calls got slower, or if a client failed with anything but the
# errors this load is expected to make (each listed in `classifies`, with
# its reason). One clock everywhere: epoch seconds from date +%s.
#
# Set SCION_BIN to a directory with the binaries to skip the build (a
# local debug run); in CI it is unset and the release build runs.
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"

RUN_MINUTES=${RUN_MINUTES:-10}
AGENTS=${AGENTS:-2}
# The resource ceilings, each named in soak.yml: a server may grow to
# SOAK_RSS_PCT percent of its warm RSS, hold at most SOAK_FD_GROWTH more
# open files than it started with, and its late p99 must stay under
# SOAK_P99_FACTOR times its early p99.
SOAK_RSS_PCT=${SOAK_RSS_PCT:-150}
SOAK_FD_GROWTH=${SOAK_FD_GROWTH:-100}
SOAK_P99_FACTOR=${SOAK_P99_FACTOR:-2}
# The warm-up window: the RSS baseline is the first sample after it, not
# the first sample of the run. The server's memory climbs while its
# caches fill - sqlite page caches (8 readers plus the write connection),
# allocator arenas, prepared statements - and on one local 35-minute run
# it kept climbing for about 30 minutes before it went flat. A baseline
# at 60 s made every longer warm-up a false red. A run shorter than the
# window plus two samples checks no RSS bound: it cannot tell warm-up
# from a leak (the soak on a PR checks the mechanics, not the trend).
SOAK_WARM_SECONDS=${SOAK_WARM_SECONDS:-1800}
# Enough lands that the writers would need more than the run to finish;
# bench is stopped at the end, and the server's own Land counter says what
# it landed.
LANDS=$((RUN_MINUTES * 60 * 100))

# classifies LOG: 0 if every failure LINE in LOG is one this load expects.
# The list, each with why:
#   exit 130, 137, 143: the cancel loop's TERM, KILL and INT, by design.
#   unavailable / busy: a write that waited out the 5 s lock timeout; the
#     writers, the import and gc contend, and the client retries.
#   aborted: a land that lost its ref race; a retry with the same request
#     key wins, which is what the soak checks.
#   expired / permission denied: a read ticket that aged out mid-walk.
classifies() {
  local log=$1 line
  while IFS= read -r line; do
    case $line in
      *"exit 130"* | *"exit 137"* | *"exit 143"*) ;;
      *unavailable* | *busy* | *aborted* | *expired* | *"permission denied"*) ;;
      *) return 1 ;;
    esac
  done < "$log"
  return 0
}

need
make_work soak
if [ -n "${SCION_BIN:-}" ]; then
  BIN=$SCION_BIN
else
  build
fi
fetch_dataset
import_dataset
start_scion
# common.sh's start(1) runs the server under /usr/bin/time, so the PID
# file names time(1), not the server — and time(1) may exec or fork
# depending on the system. The process that has the server's port is the
# server, on every system. Measure it, and only if it really is the
# server.
SERVER_PID=$(lsof -t -iTCP:"$SERVER_PORT" -sTCP:LISTEN 2> /dev/null | head -n 1)
[ -n "$SERVER_PID" ] || die "no process listens on the server port"
# macOS ps prints the full path as the command name; take the last part.
case $(ps -o comm= -p "$SERVER_PID") in
  */scion-server | scion-server) ;;
  *) die "the server port is held by $(ps -o comm= -p "$SERVER_PID"), not scion-server" ;;
esac
CACHE_URL="http://127.0.0.1:$CACHE_PORT"

# The loaders, in background with PID files, so cleanup stops them. END is
# epoch seconds, the same clock every loop reads.
END=$(($(date +%s) + RUN_MINUTES * 60))

writers() {
  # A hard cap on the writers: bench would otherwise outlive the run.
  "$TIMEOUT" $((RUN_MINUTES * 60 + 120)) "$BIN/scn" --server "$SERVER" bench \
    --agents "$AGENTS" --lands "$LANDS" \
    > "$WORK/bench.json" 2> "$WORK/bench.log"
}
readers() {
  local n=0
  while [ "$(date +%s)" -lt "$END" ]; do
    n=$((n + 1))
    if ! "$TIMEOUT" 120 "$BIN/scn" --server "$SERVER" --objects "$DAEMON" checkout main \
      --sparse "$SPARSE_DIR" --into "$WORK/read/$n" \
      >> "$WORK/readers.log" 2>&1; then
      echo "checkout $n failed" >> "$WORK/read-fail.log"
      tail -n 3 "$WORK/readers.log" >> "$WORK/read-fail.log"
    fi
    rm -rf "$WORK/read/$n"
  done
}
# The import takes no server lock, so it runs beside the live server
# (docs/operations.md, "An import does not take that lock").
importer() {
  local n=0
  while [ "$(date +%s)" -lt "$END" ]; do
    n=$((n + 1))
    "$TIMEOUT" 300 "$BIN/scion-server" --data "$WORK/server" import \
      --rev main "$WORK/src.git" \
      >> "$WORK/import-soak.log" 2>&1 || echo "import $n failed" >> "$WORK/import-fail.log"
    sleep 60
  done
}
# gc is safe beside a live server and safe to repeat (docs/operations.md:
# rows go before files, so a crash leaves orphan files only).
gcer() {
  local n=0
  while [ "$(date +%s)" -lt "$END" ]; do
    n=$((n + 1))
    "$TIMEOUT" 120 "$BIN/scion-server" --data "$WORK/server" gc --dry-run \
      >> "$WORK/gc.log" 2>&1 || echo "gc-dry $n failed" >> "$WORK/gc-fail.log"
    "$TIMEOUT" 120 "$BIN/scion-server" --data "$WORK/server" gc \
      >> "$WORK/gc.log" 2>&1 || echo "gc $n failed" >> "$WORK/gc-fail.log"
    sleep 120
  done
}
# The cancel loop: a call every ~3 s, killed while it runs.
canceller() {
  local n=0 pid
  while [ "$(date +%s)" -lt "$END" ]; do
    n=$((n + 1))
    "$BIN/scn" --server "$SERVER" log -n 50 > "$WORK/cancel/$n.out" 2>&1 &
    pid=$!
    sleep "0.$((RANDOM % 3))"
    kill -9 "$pid" 2> /dev/null && echo "exit 137" >> "$WORK/cancel.log" || true
    wait "$pid" 2> /dev/null || true
    sleep 3
  done
}
# One slow client: it holds a ReadFile stream of the dataset's largest
# file open and drains it at a crawl, so back pressure reaches the server
# for the whole run. The reader and its check live in common.sh
# (issue #399: the old `read -N` reader exited at once on every shell
# it ran on). SLOW_EXPECTED, the file's byte size, is what the end
# check compares each drain against.
SLOW_EXPECTED=$("$TIMEOUT" 60 "$BIN/scn" --server "$SERVER" cat "$SLOW_FILE" | wc -c | tr -d ' ')
[ -n "$SLOW_EXPECTED" ] && [ "$SLOW_EXPECTED" -gt 0 ] \
  || die "the slow file $SLOW_FILE is missing from the store"

sample() {
  echo "$(ps -o rss= -p "$SERVER_PID" | tr -d ' '),\
$(count_fds "$SERVER_PID")" \
    > "$WORK/last.sample"
}
# The latency probe times one real read: the ticket comes from the server,
# the bytes ride the cache, as a checkout's does. A failed probe is not a
# fast read: it goes to latency-fail.log and fails the run at the end.
latency() {
  local t0 t1
  t0=$(date +%s%N)
  if "$TIMEOUT" 60 "$BIN/scn" --server "$SERVER" --objects "$CACHE_URL" \
    cat main:tokio/Cargo.toml > /dev/null 2>> "$WORK/latency-fail.log"; then
    t1=$(date +%s%N)
    echo $(( (t1 - t0) / 1000000 ))
  else
    echo fail >> "$WORK/latency-fail.log"
    echo 0
  fi
}

note "soaking for $RUN_MINUTES minutes"
# Every check below, from the du samples in the load loop to the
# bounds, ORs into this one flag; it starts at 0 once, before the loop.
fail=0
mkdir -p "$WORK/read" "$WORK/cancel"
loop_bg soak-writers writers
loop_bg soak-readers readers
loop_bg soak-importer importer
loop_bg soak-gcer gcer
loop_bg soak-canceller canceller
loop_bg soak-slow slow_reader

: > "$WORK/metrics.csv"
: > "$WORK/metrics-store.csv"
T0=$(date +%s)
sleep 60 # the first sample still sees the run's first minute
warm_rss=
while [ "$(date +%s)" -lt "$END" ]; do
  sample
  now=$(date +%s)
  rss=$(sed -n 1p "$WORK/last.sample" | cut -d, -f1)
  echo "$now,$rss,\
$(sed -n 1p "$WORK/last.sample" | cut -d, -f2),$(latency)" >> "$WORK/metrics.csv"
  # The store's size on disk, next to the RSS it may share a cause with:
  # files a leak of disk would grow, and what Ben's run-length decision
  # needs (issue #239). du_sample keeps the size through the server's
  # temp-file churn (its vanished-file errors) and fails on anything
  # else, including a sample that reads nothing.
  du_sample "$WORK/metrics-store.csv" "$WORK/server" "$now" || fail=1
  if [ -z "$warm_rss" ] && [ $((now - T0)) -ge "$SOAK_WARM_SECONDS" ]; then
    warm_rss=$rss
  fi
  sleep 10
done
# The writers stop at the end: bench has no time limit of its own.
stop soak-writers
sleep 2

# The artifacts go where the workflow finds them, right after the run and
# before any bound: a failed run needs them more than a passed one. The
# EXIT trap of common.sh's make_work removes $WORK after this.
mkdir -p "${RUNNER_TEMP:-/tmp}/soak-artifacts"
cp "$WORK/metrics.csv" "${RUNNER_TEMP:-/tmp}/soak-artifacts/metrics.csv"
cp "$WORK/slow-held.csv" "${RUNNER_TEMP:-/tmp}/soak-artifacts/slow-held.csv" \
  2> /dev/null || true
cp "$WORK/metrics-store.csv" "${RUNNER_TEMP:-/tmp}/soak-artifacts/metrics-store.csv"
cp "$WORK"/*.log "${RUNNER_TEMP:-/tmp}/soak-artifacts/" 2> /dev/null || true

rows=$(wc -l < "$WORK/metrics.csv" | tr -d ' ')
third=$((rows / 3))
rss_end=$(tail -n 1 "$WORK/metrics.csv" | cut -d, -f2)
fds_start=$(sed -n 1p "$WORK/metrics.csv" | cut -d, -f3)
if [ -n "${warm_rss:-}" ] && [ "$rss_end" -gt $((warm_rss * SOAK_RSS_PCT / 100)) ]; then
  echo "FAIL: server RSS grew past ${SOAK_RSS_PCT}% of its warm RSS (the sample after ${SOAK_WARM_SECONDS}s): ${warm_rss} KB -> ${rss_end} KB" >&2
  fail=1
fi
if awk -F, -v s="${fds_start:-0}" -v g="$SOAK_FD_GROWTH" '$3 > s + g { c++; if (c >= 3) exit 1 }' \
  "$WORK/metrics.csv"; then
  :
else
  echo "FAIL: open files grew for 3 samples in a row (start ${fds_start:-?})" >&2
  fail=1
fi
# A measurement that reads nothing is a broken measurement: a live
# server has open files, so a run whose every sample counted 0 open
# files never measured them (macOS's /proc, issue #399).
if [ "$(cut -d, -f3 "$WORK/metrics.csv" | tail -n +2 | sort -u | grep -cv '^0$')" -eq 0 ]; then
  echo "FAIL: the open-file count never read anything" >&2
  fail=1
fi

p99() {
  sed -n "$1,$2p" "$WORK/metrics.csv" | cut -d, -f4 | sort -n |
    awk '{ a[NR] = $1 } END { print a[int(NR * 0.99 + 0.5)] }'
}
# A p99 of a handful of samples is their maximum, noise twice over, so
# the latency bound applies only to a run long enough to compare steady
# thirds (the nightly 10 minutes give 54 samples; a 3-minute PR run gives
# 12 and checks the mechanics, not the trend).
early=""
late=""
if [ "$rows" -ge 30 ]; then
  early=$(p99 1 "$third")
  late=$(p99 "$((rows - third + 1))" "$rows")
  if [ -n "$early" ] && [ -n "$late" ] && [ "$late" -gt $((early * SOAK_P99_FACTOR)) ]; then
    echo "FAIL: p99 grew: $early ms early, $late ms late" >&2
    fail=1
  fi
fi

# The slow client must have held its stream: a drain that read fewer
# bytes than the file has ended early while the server was still
# serving (common.sh, issue #399).
if ! check_slow_stream "$SLOW_EXPECTED"; then
  fail=1
fi

# Every failure a client saw, one line at a time: a line that matches no
# expected pattern fails the run, however many others matched.
for f in read-fail latency-fail import-fail gc-fail; do
  if [ -s "$WORK/$f.log" ] && ! classifies "$WORK/$f.log"; then
    echo "FAIL: unexpected failures in $f.log:" >&2
    head -n 5 "$WORK/$f.log" >&2
    fail=1
  fi
done

# The writers are stopped at the end, so their report never lands in a
# file; the server's own counter says what landed. The RPC is Scion/Land.
landed=$(curl -fsS "http://127.0.0.1:$SERVER_METRICS_PORT/metrics" |
  awk '/^scion_requests_total\{.*rpc="Scion\/Land"/ { s += $NF } END { print s + 0 }')
if [ "$landed" -eq 0 ]; then
  echo "FAIL: the writers landed nothing" >&2
  tail -n 10 "$WORK/bench.log" 2> /dev/null >&2 || true
  fail=1
fi
row() { printf '%-40s %s\n' "$1" "$2"; }
echo "soak: $RUN_MINUTES minutes, $AGENTS writers"
echo
row "step" "result"
row "land requests served" "$landed"
row "samples" "$rows"
if [ -n "${warm_rss:-}" ]; then
  row "server RSS warm (after ${SOAK_WARM_SECONDS}s) -> last" "${warm_rss} KB -> ${rss_end:-?} KB"
else
  row "server RSS bound" "not checked: the run ended before the ${SOAK_WARM_SECONDS}s warm-up"
fi
row "store size first -> last" "$(sed -n 1p "$WORK/metrics-store.csv" | cut -d, -f2) KB -> \
$(tail -n 1 "$WORK/metrics-store.csv" | cut -d, -f2) KB"
row "p99 early -> late" "${early:-?} ms -> ${late:-?} ms"
# The backend the run really served, from the data dir, against the
# one the run asked for (the soak serves the default store): a run
# asked for one store that got another fails, and the summary names
# what ran either way. served_backend comes from common.sh.
asked_backend=sqlite
served=$(served_backend "$WORK/server")
row "backend (asked -> data dir)" "$asked_backend -> $served"
if [ "$served" != "$asked_backend" ]; then
  echo "FAIL: asked for $asked_backend, the data dir says $served" >&2
  fail=1
fi
[ "${fail:-0}" -eq 0 ] || die "the soak failed its bounds"

stop_scion
