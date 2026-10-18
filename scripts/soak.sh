#!/usr/bin/env bash
# A soak: one server under a mixed load for RUN_MINUTES (issue #239, part
# 2). Writers land with scn bench, partial readers check out through the
# daemon, an import runs beside the server, gc collects on a schedule, a
# loop cancels calls by killing their processes, and one reader reads
# slowly. Every 10 s the script samples the server's RSS, its open files
# and one timed read, into a CSV. At the end it fails if memory or files
# grew past their bound, if late calls got slower, or if any client failed
# with anything but the errors this load is expected to make, each with
# its reason in `classifies`. The CSV and the logs upload as artifacts.
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"

RUN_MINUTES=${RUN_MINUTES:-10}
AGENTS=${AGENTS:-2}
# More lands than the run can finish, so writers never idle.
LANDS=$((RUN_MINUTES * 60 * 100))

need
make_work soak
build
fetch_dataset
import_dataset
start_scion
SERVER_PID=$(cat "$WORK/server.pid")

# classifies LOG: 0 if every failure in LOG is one this load expects.
# The list, each with why:
#   exit 130, 137, 143: the cancel loop's TERM, KILL and INT, by design.
#   unavailable / busy: a write that waited out the 5 s lock timeout; the
#     writers, the import and gc contend, and the client retries.
#   aborted: a land that lost its ref race; a retry with the same request
#     key wins, which is what the soak checks.
#   expired / permission denied: a read ticket that aged out mid-walk.
classifies() {
  local log=$1
  if grep -qE "exit (130|137|143)" "$log"; then return 0; fi
  ! grep -qiE "unavailable|busy|aborted|expired|permission denied" "$log" || return 0
  return 1
}

# The loaders, each with its own log. cleanup stops them by PID file.
writers() {
  "$BIN/scn" --server "$SERVER" bench \
    --agents "$AGENTS" --lands "$LANDS" \
    > "$WORK/bench.json" 2> "$WORK/bench.log"
}
readers() {
  local n=0
  while [ "$(date +%s)" -lt "$END" ]; do
    n=$((n + 1))
    rm -rf "$WORK/read/$n"
    if ! "$BIN/scn" --server "$SERVER" --objects "$DAEMON" checkout main \
      --sparse "$SPARSE_DIR" --into "$WORK/read/$n" \
      >> "$WORK/readers.log" 2>&1; then
      echo "checkout $n: $(tail -n 1 "$WORK/readers.log")" >> "$WORK/read-fail.log"
    fi
    rm -rf "$WORK/read/$n"
  done
}
importer() {
  local n=0
  while [ "$(date +%s)" -lt "$END" ]; do
    sleep 120
    n=$((n + 1))
    "$BIN/scion-server" --data "$WORK/server" import --rev main "$WORK/src.git" \
      >> "$WORK/import-soak.log" 2>&1 || echo "import $n failed" >> "$WORK/import-fail.log"
  done
}
# gc is documented safe beside a live server (operations.md: "It is safe
# beside a live server and safe to repeat"; rows go before files, so a
# crash leaves orphan files only).
gcer() {
  local n=0
  while [ "$(date +%s)" -lt "$END" ]; do
    sleep 300
    n=$((n + 1))
    "$BIN/scion-server" --data "$WORK/server" gc --dry-run \
      >> "$WORK/gc.log" 2>&1 || echo "gc-dry $n failed" >> "$WORK/gc-fail.log"
    "$BIN/scion-server" --data "$WORK/server" gc \
      >> "$WORK/gc.log" 2>&1 || echo "gc $n failed" >> "$WORK/gc-fail.log"
  done
}
canceller() {
  local n=0 pid
  while [ "$(date +%s)" -lt "$END" ]; do
    n=$((n + 1))
    "$BIN/scn" --server "$SERVER" log -n 50 > "$WORK/cancel/$n.out" 2>&1 &
    pid=$!
    sleep "0.$((RANDOM % 3))"
    kill -9 "$pid" 2> /dev/null && echo "exit 137" >> "$WORK/cancel.log" || true
    wait "$pid" 2> /dev/null || true
    sleep 2
  done
}
slow_reader() {
  # One client that reads a file every 2 s: a slow reader on the wire,
  # through the daemon, for the whole run.
  while [ "$(date +%s)" -lt "$END" ]; do
    for f in $(find "$WORK/read/1" -name Cargo.toml 2> /dev/null | head -5); do
      cat "$f" > /dev/null
      sleep 2
    done
  done
}

note "soaking for $RUN_MINUTES minutes"
END=$((SECONDS + RUN_MINUTES * 60))
mkdir -p "$WORK/read/1" "$WORK/cancel"
writers & echo $! > "$WORK/soak-writers.pid"
readers & echo $! > "$WORK/soak-readers.pid"
importer & echo $! > "$WORK/soak-importer.pid"
gcer & echo $! > "$WORK/soak-gcer.pid"
canceller & echo $! > "$WORK/soak-canceller.pid"
slow_reader & echo $! > "$WORK/soak-slow.pid"

# The sampler: one CSV line every 10 s, unix_time,rss_kb,open_files,latency_ms.
# The latency column times one small read through the cache, end to end.
sample() {
  local t0 t1
  echo "$(date +%s),$(ps -o rss= -p "$SERVER_PID" | tr -d ' '),\
$(find "/proc/$SERVER_PID/fd" -maxdepth 1 2> /dev/null | wc -l | tr -d ' ')" \
    > "$WORK/last.sample"
}
latency() {
  local t0 t1
  t0=$(date +%s%N)
  "$BIN/scn" --server "http://127.0.0.1:$CACHE_PORT" cat main:tokio/Cargo.toml \
    > /dev/null 2>&1 || true
  t1=$(date +%s%N)
  echo $(( (t1 - t0) / 1000000 ))
}
sleep 60 # the warm start, before the first sample
: > "$WORK/metrics.csv"
while [ "$(date +%s)" -lt "$END" ]; do
  sample
  echo "$(date +%s),$(cut -d, -f2 "$WORK/last.sample"),\
$(cut -d, -f3 "$WORK/last.sample"),$(latency)" >> "$WORK/metrics.csv"
  sleep 10
done

# The bounds. RSS: at most 1.5x the first sample. Files: no 3 samples in a
# row past the first value + 100. Latency: p99 of the last third of the
# samples at most 2x the p99 of the first third.
fail=0
rss_warm=$(sed -n 1p "$WORK/metrics.csv" | cut -d, -f2)
rss_end=$(tail -n 1 "$WORK/metrics.csv" | cut -d, -f2)
if [ -n "$rss_warm" ] && [ -n "$rss_end" ] && [ "$rss_end" -gt $((rss_warm * 3 / 2)) ]; then
  echo "FAIL: server RSS grew: $rss_warm KB -> $rss_end KB" >&2
  fail=1
fi
fds_start=$(sed -n 1p "$WORK/metrics.csv" | cut -d, -f3)
if awk -F, -v s="${fds_start:-0}" 'NR > 1 && $3 > s + 100 { c++; if (c >= 3) exit 1 }' \
  "$WORK/metrics.csv"; then
  :
else
  echo "FAIL: open files grew for 3 samples in a row (start ${fds_start:-?})" >&2
  fail=1
fi
rows=$(wc -l < "$WORK/metrics.csv" | tr -d ' ')
third=$((rows / 3))
p99() {
  sed -n "$1,$2p" "$WORK/metrics.csv" | cut -d, -f4 | sort -n |
    awk '{ a[NR] = $1 } END { print a[int(NR * 0.99 + 0.5)] }'
}
early=$(p99 1 "$third")
late=$(p99 "$((rows - third))" "$third")
if [ -n "$early" ] && [ -n "$late" ] && [ "$late" -gt "$((early * 2))" ]; then
  echo "FAIL: p99 grew: $early ms early, $late ms late" >&2
  fail=1
fi

# Any failure the client saw must be one of the expected ones; anything
# else fails the run.
for f in read-fail import-fail gc-fail; do
  if [ -s "$WORK/$f.log" ]; then
    if ! classifies "$WORK/$f.log"; then
      echo "FAIL: unexpected failures in $f.log:" >&2
      head -n 5 "$WORK/$f.log" >&2
      fail=1
    fi
  fi
done
if [ ! -s "$WORK/bench.json" ]; then
  echo "FAIL: the writers stopped early" >&2
  tail -n 10 "$WORK/bench.log" >&2
  fail=1
fi

landed=$(sed -n 's/.*"landed":\([0-9]*\).*/\1/p' "$WORK/bench.json" | tail -n 1)
row() { printf '%-40s %s\n' "$1" "$2"; }
echo "soak: $RUN_MINUTES minutes, $AGENTS writers"
echo
row "step" "result"
row "lands" "${landed:-0}"
row "samples" "$rows"
row "server RSS first -> last" "${rss_warm:-?} KB -> ${rss_end:-?} KB"
row "p99 early -> late" "${early:-?} ms -> ${late:-?} ms"
[ "$fail" -eq 0 ] || die "the soak failed its bounds"

# The artifacts outlive cleanup: the workflow copies them from here.
mkdir -p "${RUNNER_TEMP:-/tmp}/soak-artifacts"
cp "$WORK/metrics.csv" "${RUNNER_TEMP:-/tmp}/soak-artifacts/metrics.csv"
cp "$WORK"/*.log "${RUNNER_TEMP:-/tmp}/soak-artifacts/" 2> /dev/null || true
stop_scion
