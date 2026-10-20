#!/usr/bin/env bash
# The fault half of the soak (issue #239, part 3). One server under the
# soak's mixed load, killed and restarted on schedule, so a nightly run
# sees what a 72-hour run would: a crash in the middle of real traffic.
#
# The load is soak.sh's, re-implemented here in its shape; the writers
# differ: bench aggregates, and the fault soak needs every acknowledged
# land on record. So each writer commits, lands, and the moment the
# land returns 0 appends `repo,ref,changeset` to its ledger part file.
# After every restart, and at the end, the script checks that every
# ledger entry is in that ref's history (`scn log`): nothing the server
# acknowledged was lost. A land the server accepted but had not applied
# at kill time applies during the restart, so the check retries for up
# to 60 s, as the crash suite's `retry_lands` does in process. The
# server's Land counter is scraped as a resume signal only: Prometheus
# counters start at 0 after a restart, so they say nothing about lost
# writes.
#
# Faults: kill -9 the server under load. Nightly (120 min): every 20
# minutes, six kills; the 10-minute dispatch: at a quarter, a half and
# three quarters of the run, three kills. The third restart takes the
# whole chain down - cache and daemon too - the rest the server alone,
# so most restarts prove the clients resume against a restarted
# upstream through a live tier. Optional: a Postgres restart on the pg
# backend (SCION_FAULT_PG=1 with SCION_PG_URL and SCION_FAULT_PG_CMD,
# the command that restarts Postgres).
#
# After every restart: the server lock is free (a flock probe, the
# shell twin of the crash suite's `wait_for_lock_release`), the server
# reaches `serving`, a client read succeeds within 60 s, fsck is clean
# beside the running server (release-smoke.sh runs it the same way),
# and the ledger holds. Ceilings, from soak.yml's names, apply per
# restart segment - each restart resets the server's memory, so the
# whole-run bound of soak.sh would false-fail. The RSS baseline is
# taken SOAK_WARMUP_MIN minutes into each segment; soak.sh takes it
# from the same window (coder5 is pinning the exact warm-up there -
# SHORTCUT: this script reads SOAK_WARMUP_MIN, default 10, until that
# lands, then the two align). A segment shorter than the warm-up window
# proves the mechanics, not the RSS bound; it is logged and skipped.
#
# What this proves and what it does not: the ledger check is
# system-level - acknowledged lands are in the ref's history after the
# kill. The in-process ledger (every reply's bytes checked against what
# the client sent) stays with the crash suite (crash.rs); the
# sync-before-reply proof stays with the power-loss suite.
#
# SHORTCUT: the loaders are copied from soak.sh, not factored out,
# because soak.sh must not change while its RSS warm-up fix is in
# flight. The upgrade path is one place: lift the loaders into
# common.sh when soak.sh settles, and have both scripts call them. The
# same for the server-only start: common.sh has no server-only starter,
# so start_scion's server line is copied here.
#
# Set SCION_BIN to a directory with the binaries to skip the build (a
# local debug run); in CI it is unset and the release build runs.
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"

RUN_MINUTES=${RUN_MINUTES:-10}
AGENTS=${AGENTS:-2}
# The ceilings, the same names soak.yml passes to soak.sh. Each applies
# per restart segment, from the segment's post-warm-up baseline.
SOAK_RSS_PCT=${SOAK_RSS_PCT:-150}
SOAK_FD_GROWTH=${SOAK_FD_GROWTH:-100}
SOAK_P99_FACTOR=${SOAK_P99_FACTOR:-2}
SOAK_WARMUP_MIN=${SOAK_WARMUP_MIN:-10}
# The kill schedule, in seconds from the start. Nightly (120 min):
# every 20 minutes, six kills, the last 2 minutes before the end.
# Shorter runs kill at a quarter, a half and three quarters of the run.
KILL_AT=()
if [ "$RUN_MINUTES" -ge 60 ]; then
  for m in $(seq 20 20 $((RUN_MINUTES - 2))); do
    KILL_AT+=("$((m * 60))")
  done
else
  q=$((RUN_MINUTES * 60 / 4))
  KILL_AT+=("$q" "$((q * 2))" "$((q * 3))")
fi
# The pg backend and its restart. Off by default; the nightly keeps it
# off until the 72-hour hosting decision (D10) says where Postgres
# lives. SCION_FAULT_PG_CMD is the restart command itself, for example
# `docker restart soak-pg`.
PGFLAGS=()
if [ -n "${SCION_PG_URL:-}" ]; then
  PGFLAGS=(--postgres-url "$SCION_PG_URL")
fi

# classifies LOG: 0 if every failure LINE in LOG is one this load
# expects. soak.sh's list, plus the transport lines a kill makes: a
# client whose stream the kill tore is reporting the fault, and a
# client that cannot reach a live server is caught by the latency
# probe, which is a hard check, not a classified one.
classifies() {
  local log=$1 line
  while IFS= read -r line; do
    case $line in
      *"exit 130"* | *"exit 137"* | *"exit 143"*) ;;
      *unavailable* | *busy* | *aborted* | *expired* | *"permission denied"*) ;;
      *"error trying to connect"* | *"transport error"* | *"Connection refused"*) ;;
      *"connection closed"* | *"connection reset"*) ;;
      # The daemon's socket file, gone for the length of the full-chain
      # restart.
      *"cannot use socket"*) ;;
      # scn's hint under a connect error.
      *"check: is a scion server or tier running at that address?"*) ;;
      # The readers' own marker for a checkout the kill tore, and the
      # partial report the checkout printed before it died: the line
      # after them carries the client's error, which the patterns above
      # classify.
      *checkout*\ *failed*) ;;
      *"checked out"*"from"*) ;;
      *) return 1 ;;
    esac
  done < "$log"
  return 0
}

need
command -v python3 > /dev/null || die "needs python3 (the lock probe)"
# A writer commits with --put, so its file must be on disk.

make_work soak-fault
if [ -n "${SCION_BIN:-}" ]; then
  BIN=$SCION_BIN
else
  build
fi
fetch_dataset
import_dataset
start_scion
# The PID that holds the server's port is the server (soak.sh: the PID
# file names time(1), which may exec or fork). Re-resolved after every
# restart.
server_pid() {
  local pid
  pid=$(lsof -t -iTCP:"$SERVER_PORT" -sTCP:LISTEN 2> /dev/null | head -n 1)
  [ -n "$pid" ] || return 1
  case $(ps -o comm= -p "$pid") in
    */scion-server | scion-server) SERVER_PID=$pid ;;
    *) die "the server port is held by $(ps -o comm= -p "$pid"), not scion-server" ;;
  esac
}
server_pid || die "no process listens on the server port"
CACHE_URL="http://127.0.0.1:$CACHE_PORT"

# start_server: the server line of common.sh's start_scion, alone. The
# cache and daemon keep running across a server-only restart, which is
# the point: the clients must resume against a restarted upstream
# through a live tier.
start_server() {
  start server serving "$BIN/scion-server" --data "$WORK/server" serve \
    --listen "127.0.0.1:$SERVER_PORT" \
    --ticket-key "$WORK/ticket.key" \
    --metrics-listen "127.0.0.1:$SERVER_METRICS_PORT" \
    "${PGFLAGS[@]+"${PGFLAGS[@]}"}"
  server_pid || die "the restarted server does not hold the port"
}

# wait_lock_free: the shell twin of the crash suite's
# `wait_for_lock_release` (tests/common/mod.rs): the lock is taken
# non-blocking, every second, until free. Bounded, so a real leak fails
# instead of hanging. macOS tears a reaped child's inherited flock down
# one tick after the reap, and Linux shows no delay; a second of
# probing absorbs both.
wait_lock_free() {
  local n=0
  until python3 -c "
import fcntl, sys
f = open(sys.argv[1], 'a+')
fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
" "$WORK/server/server.lock" 2> /dev/null; do
    n=$((n + 1))
    [ "$n" -lt 10 ] || die "the server lock was never released within 10 s"
    sleep 1
  done
}

# A client read, timed in ms, through the cache, as a checkout's is.
# 0 means the probe failed; the run's end classifies the log.
latency() {
  local t0 t1
  t0=$(date +%s%N)
  if timeout 60 "$BIN/scn" --server "$SERVER" --objects "$CACHE_URL" \
    cat main:tokio/Cargo.toml > /dev/null 2>> "$WORK/latency-fail.log"; then
    t1=$(date +%s%N)
    echo $(( (t1 - t0) / 1000000 ))
  else
    echo fail >> "$WORK/latency-fail.log"
    echo 0
  fi
}

# One writer agent: commit, land, and the moment the land returns 0,
# record `repo,ref,changeset` in the agent's ledger part. The id
# recorded is the one the server's reply named (`landed <id>`), because
# that is what was acknowledged. A land that lost its ref race exits
# non-zero and is not recorded - it was never acknowledged.
writer() {
  local w=$1 n=0 cs out
  local file="$WORK/seed/w$w.bin"
  head -c 4096 /dev/zero > "$file"
  while [ "$(date +%s)" -lt "$END" ]; do
    n=$((n + 1))
    if cs=$(timeout 60 "$BIN/scn" --server "$SERVER" commit \
      -m "fault w$w r$n" --put "fault/w$w.bin=$file" 2>> "$WORK/writer-$w.log"); then
      if out=$(timeout 60 "$BIN/scn" --server "$SERVER" land "$cs" --onto main \
        2>> "$WORK/writer-$w.log"); then
        case $out in
          landed\ *) printf 'main,main,%s\n' "${out#landed }" >> "$WORK/ledger/$w.csv" ;;
          # A landed state with no result id is a replay of a land the
          # server already applied; nothing new was acknowledged.
          *) echo "land without an id: $out" >> "$WORK/writer-$w.log" ;;
        esac
      fi
    fi
    sleep 1
  done
}
readers() {
  local n=0
  while [ "$(date +%s)" -lt "$END" ]; do
    n=$((n + 1))
    if ! timeout 120 "$BIN/scn" --server "$SERVER" --objects "$DAEMON" checkout main \
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
    timeout 300 "$BIN/scion-server" --data "$WORK/server" import \
      "${PGFLAGS[@]+"${PGFLAGS[@]}"}" --rev main "$WORK/src.git" \
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
    timeout 120 "$BIN/scion-server" --data "$WORK/server" gc --dry-run \
      "${PGFLAGS[@]+"${PGFLAGS[@]}"}" >> "$WORK/gc.log" 2>&1 \
      || echo "gc-dry $n failed" >> "$WORK/gc-fail.log"
    timeout 120 "$BIN/scion-server" --data "$WORK/server" gc \
      "${PGFLAGS[@]+"${PGFLAGS[@]}"}" >> "$WORK/gc.log" 2>&1 \
      || echo "gc $n failed" >> "$WORK/gc-fail.log"
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
slow_reader() {
  while [ "$(date +%s)" -lt "$END" ]; do
    # shellcheck disable=SC2016 # the drain reads its own $1, the cat command
    timeout 120 sh -c '"$1" | while IFS= read -r -N 4096 chunk; do
        printf %s "$chunk" > /dev/null
        sleep 0.2
      done' drain "$BIN/scn --server $SERVER cat main:tokio/Cargo.lock" \
      >> "$WORK/slow.log" 2>&1 || true
    sleep 1
  done
}

# check_ledger [FROM]: every acknowledged land from line FROM+1 is in
# main's history. After a restart, FROM is the count already checked,
# and the new entries sit at the top of the history; at the end, FROM
# is 0 and the walk is the whole history. A land the server accepted
# but had not applied at kill time applies during the restart, so the
# check retries for up to 60 s before it fails - the crash suite's
# `retry_lands` does the same in process. The ids come from `scn log
# --json`, whose entries carry the full ID.
check_ledger() {
  local from=${1:-0} tries=0 count
  cat "$WORK"/ledger/*.csv 2> /dev/null | tail -n +"$((from + 1))" \
    > "$WORK/ledger-new.csv" || true
  [ -s "$WORK/ledger-new.csv" ] || return 0
  count=$(wc -l < "$WORK/ledger-new.csv" | tr -d ' ')
  note "ledger check: $count entries against main's history"
  while :; do
    if timeout 120 "$BIN/scn" --server "$SERVER" log --json -n $((count + 50)) main \
      > "$WORK/history.json" 2>> "$WORK/ledger-check.log"; then
      break
    fi
    tries=$((tries + 1))
    [ "$tries" -lt 3 ] || {
      echo "FAIL: the history walk failed" >&2
      return 1
    }
    sleep 20
  done
  grep -o '"id":"[0-9a-f]*"' "$WORK/history.json" | cut -d'"' -f4 | sort -u \
    > "$WORK/history-ids.txt"
  comm -23 <(cut -d, -f3 "$WORK/ledger-new.csv" | sort -u) "$WORK/history-ids.txt" \
    > "$WORK/missing.csv"
  if [ -s "$WORK/missing.csv" ]; then
    echo "FAIL: lands the server acknowledged are missing from main's history:" >&2
    head -n 5 "$WORK/missing.csv" >&2
    return 1
  fi
}

# fsck_ok: the store and database hold. Safe beside the running server
# (release-smoke.sh runs it the same way).
fsck_ok() {
  timeout 300 "$BIN/scion-server" --data "$WORK/server" fsck \
    "${PGFLAGS[@]+"${PGFLAGS[@]}"}" >> "$WORK/fsck.log" 2>&1
}

# pg_restart: the optional Postgres fault. SCION_FAULT_PG_CMD runs the
# restart; the server must come back with it.
pg_restart() {
  if [ -z "${SCION_FAULT_PG_CMD:-}" ]; then
    die "SCION_FAULT_PG=1 needs SCION_FAULT_PG_CMD"
  fi
  sh -c "$SCION_FAULT_PG_CMD" >> "$WORK/pg.log" 2>&1
  local n=0
  until pg_isready -q 2> /dev/null; do
    n=$((n + 1))
    [ "$n" -lt 60 ] || die "Postgres never came back"
    sleep 1
  done
}

note "fault-soaking for $RUN_MINUTES minutes, ${#KILL_AT[@]} kills"
mkdir -p "$WORK/read" "$WORK/cancel" "$WORK/ledger" "$WORK/seed"
END=$(($(date +%s) + RUN_MINUTES * 60))
T0=$(date +%s)

writers() {
  local w
  for w in $(seq 1 "$AGENTS"); do
    writer "$w" &
  done
  wait
}
writers & echo $! > "$WORK/soak-writers.pid"
readers & echo $! > "$WORK/soak-readers.pid"
importer & echo $! > "$WORK/soak-importer.pid"
gcer & echo $! > "$WORK/soak-gcer.pid"
canceller & echo $! > "$WORK/soak-canceller.pid"
slow_reader & echo $! > "$WORK/soak-slow.pid"

# The fault loop. SEG is the restart segment's number; the metrics CSV
# carries it, and the ceilings are checked per segment. The third
# restart takes the whole chain, the rest the server alone.
SEG=1
: > "$WORK/metrics.csv"
: > "$WORK/restarts.csv"

resume_and_check() {
  # C4, clients resume: a read through the cache within 60 s. A probe
  # that never succeeds fails the run.
  local t0
  t0=$(date +%s)
  until [ "$(latency)" -gt 0 ]; do
    [ "$(($(date +%s) - t0))" -lt 60 ] || {
      echo "FAIL: clients did not resume within 60 s of the restart" >&2
      exit 1
    }
    sleep 5
  done
  if [ "${SCION_FAULT_PG:-0}" = "1" ]; then
    pg_restart
    t0=$(date +%s)
    until [ "$(latency)" -gt 0 ]; do
      [ "$(($(date +%s) - t0))" -lt 60 ] || {
        echo "FAIL: clients did not resume within 60 s of the Postgres restart" >&2
        exit 1
      }
      sleep 5
    done
  fi
  # C2, nothing acknowledged was lost, and C3, the store holds.
  if ! check_ledger; then
    exit 1
  fi
  if ! fsck_ok; then
    echo "FAIL: fsck is not clean after the restart" >&2
    exit 1
  fi
}

kill_once() {
  SEG=$((SEG + 1))
  echo "$SEG kill $(date +%s)" >> "$WORK/restarts.csv"
  kill -9 "$SERVER_PID" 2> /dev/null || true
  # Wait for the port and the lock, then start again. The port frees in
  # a moment; the lock probe bounds a stale holder. Both are bounded, so
  # a stuck port or a leaked lock fails the run instead of hanging it.
  local n=0
  while lsof -t -iTCP:"$SERVER_PORT" -sTCP:LISTEN 2> /dev/null | grep -q .; do
    n=$((n + 1))
    [ "$n" -lt 60 ] || die "the server port never freed within 60 s"
    sleep 1
  done
  wait_lock_free
  if [ "$SEG" -eq 3 ]; then
    stop_scion
    start_scion
    # start_scion does not name the server's PID; re-resolve it, or the
    # next kill would kill a dead PID and the run would hang on the
    # port of the server it meant to kill.
    server_pid || die "no process listens on the server port after the restart"
  else
    start_server
  fi
  resume_and_check
}

KI=0
while [ "$(date +%s)" -lt "$END" ]; do
  if [ "$KI" -lt "${#KILL_AT[@]}" ]; then
    due=$((T0 + ${KILL_AT[$KI]}))
    if [ "$(date +%s)" -ge "$due" ]; then
      KI=$((KI + 1))
      kill_once
    fi
  fi
  echo "$(date +%s),$SEG,$(ps -o rss= -p "$SERVER_PID" | tr -d ' '),\
$(find "/proc/$SERVER_PID/fd" -maxdepth 1 2> /dev/null | wc -l | tr -d ' '),$(latency)" \
    >> "$WORK/metrics.csv"
  sleep 10
done

# The artifacts go where the workflow finds them, right after the run
# and before any check: a failed run needs them more than a passed one.
# The EXIT trap of common.sh's make_work removes $WORK after this. Its
# own directory, so a manual run beside soak.sh does not overwrite
# soak's CSVs.
mkdir -p "${RUNNER_TEMP:-/tmp}/soak-fault-artifacts"
cp "$WORK/metrics.csv" "$WORK/restarts.csv" \
  "${RUNNER_TEMP:-/tmp}/soak-fault-artifacts/" 2> /dev/null || true
cp "$WORK"/*.log "${RUNNER_TEMP:-/tmp}/soak-fault-artifacts/" 2> /dev/null || true

fail=0

# C2, the whole ledger, against one full walk of main's history.
if ! check_ledger 0; then
  fail=1
fi

# C3, fsck over the final state.
if ! fsck_ok; then
  echo "FAIL: fsck is not clean at the end" >&2
  fail=1
fi

# C3, the last checkout reads back whole: one file's bytes through the
# daemon match what the server serves. The checkout is sparse over
# tokio/src, so the file comes from there.
final="$WORK/final-read"
if ! timeout 120 "$BIN/scn" --server "$SERVER" --objects "$DAEMON" checkout main \
  --sparse "$SPARSE_DIR" --into "$final" >> "$WORK/read-fail.log" 2>&1; then
  echo "FAIL: the final checkout failed" >&2
  fail=1
elif ! "$BIN/scn" --server "$SERVER" cat main:tokio/src/lib.rs 2>> "$WORK/read-fail.log" |
  cmp -s - "$final/tokio/src/lib.rs"; then
  echo "FAIL: the final checkout's bytes do not match the server's" >&2
  fail=1
fi

# Every failure a client saw, one line at a time.
for f in read-fail latency-fail import-fail gc-fail; do
  if [ -s "$WORK/$f.log" ] && ! classifies "$WORK/$f.log"; then
    echo "FAIL: unexpected failures in $f.log:" >&2
    head -n 5 "$WORK/$f.log" >&2
    fail=1
  fi
done

# The ceilings, per restart segment, from each segment's post-warm-up
# baseline. A segment with no sample after its warm-up window proves
# the mechanics only; say so and move on.
warm=$((SOAK_WARMUP_MIN * 60))
seg_fail=0
for s in $(seq 1 "$SEG"); do
  awk -F, -v s="$s" -v w="$warm" -v t0="$(head -n 1 "$WORK/metrics.csv" | cut -d, -f1)" \
    '$2 == s && ($1 - t0) >= w' "$WORK/metrics.csv" > "$WORK/seg-$s.csv"
  if [ ! -s "$WORK/seg-$s.csv" ]; then
    echo "segment $s: shorter than the warm-up; mechanics only"
    continue
  fi
  base_rss=$(head -n 1 "$WORK/seg-$s.csv" | cut -d, -f3)
  end_rss=$(tail -n 1 "$WORK/seg-$s.csv" | cut -d, -f3)
  if [ -n "$base_rss" ] && [ -n "$end_rss" ] && \
    [ "$end_rss" -gt $((base_rss * SOAK_RSS_PCT / 100)) ]; then
    echo "FAIL: segment $s RSS grew past ${SOAK_RSS_PCT}%: $base_rss KB -> $end_rss KB" >&2
    seg_fail=1
  fi
  base_fds=$(head -n 1 "$WORK/seg-$s.csv" | cut -d, -f4)
  if ! awk -F, -v s0="$base_fds" -v g="$SOAK_FD_GROWTH" \
    '$4 > s0 + g { c++; if (c >= 3) exit 1 }' "$WORK/seg-$s.csv"; then
    echo "FAIL: segment $s open files grew for 3 samples in a row" >&2
    seg_fail=1
  fi
done
[ "$seg_fail" -eq 0 ] || fail=1

# The p99 bound, over the whole run, as soak.sh takes it: thirds.
rows=$(wc -l < "$WORK/metrics.csv" | tr -d ' ')
third=$((rows / 3))
p99() {
  sed -n "$1,$2p" "$WORK/metrics.csv" | cut -d, -f5 | sort -n |
    awk '{ a[NR] = $1 } END { print a[int(NR * 0.99 + 0.5)] }'
}
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

# The writers are stopped at the end, so the counter says what the
# server served - a resume signal, not the ledger proof.
landed=$(curl -fsS "http://127.0.0.1:$SERVER_METRICS_PORT/metrics" 2> /dev/null |
  awk '/^scion_requests_total\{.*rpc="Scion\/Land"/ { s += $NF } END { print s + 0 }')
acked=$(cat "$WORK"/ledger/*.csv 2> /dev/null | wc -l | tr -d ' ')
if [ "${acked:-0}" -eq 0 ]; then
  echo "FAIL: the writers acknowledged nothing" >&2
  tail -n 10 "$WORK/writer-1.log" 2> /dev/null >&2 || true
  fail=1
fi

row() { printf '%-40s %s\n' "$1" "$2"; }
echo "soak-fault: $RUN_MINUTES minutes, $AGENTS writers, $((SEG - 1)) kills"
echo
row "step" "result"
row "lands acknowledged" "$acked"
row "restarts" "$((SEG - 1))"
row "land requests served (counter)" "$landed"
row "samples" "$rows"
[ -n "$early" ] && row "p99 early -> late" "$early ms -> $late ms"
[ "${fail:-0}" -eq 0 ] || die "the fault soak failed its checks"
stop_scion
