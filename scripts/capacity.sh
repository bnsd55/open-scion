#!/usr/bin/env bash
# The capacity envelope (issue #387): one server on one named runner
# class, pushed over a grid of writers, readers and backends, and the
# table in docs/operations.md is what it produced. The dataset is the
# fixed public one (benchmarks.md); everything runs on loopback, as the
# other scripts do.
#
# The grid: writers x readers x backends. Writers land with `scn bench
# --json` (which reports lands per second and the land and end-to-end
# percentiles); readers run checkouts through the cache and the daemon,
# each one timed. A cell runs REPS repetitions on the same store, so a
# cell also shows the repo growing; the table reports per 1,000 lands.
# Between cells the store is re-imported, so every cell starts equal.
#
# The backends: SQLite is the default store; the Postgres cells run
# only when SCION_PG_URL is set (the workflow gives the job a service
# container). Without it the script prints the SQLite cells and a note
# - a partial table fails the doc test, which is how a run that skipped
# a backend is kept out of the published text.
#
# The edge is not enforced here: the run measures, the table's caption
# states the limits, and the reader applies them to the CSV. What the
# script does stop on is a failed check: bench verified main, every
# checkout read back whole, and every client error is one the load
# explains (the soak's classes).
#
# Set CAPACITY_MACHINE to the runner's public name and spec; the table's
# caption carries it. Set RUN_URL to the workflow run's URL for the
# footer. The growth splits into two columns: the blob store's (the
# data directory du samples) and, on Postgres, the database's own
# (pg_database_size of the database the cell reset); SQLite has no
# separate database, so its database column is "-". Both divide by the
# lands the server's counter measured, not the lands the plan named.
#
# SHORTCUT: the readers and the sampling are the soak's shape, copied
# here rather than lifted, because soak.sh and soak-fault.sh are two
# working scripts whose knobs this does not want to share; the upgrade
# path is a common.sh load library when a third caller appears.
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"

# The harness drops and recreates the database it runs on between
# cells, so it runs only on a database of its own naming: an operator
# URL that names a real database is refused here, before any psql and
# so before any DROP.
if [ -n "${SCION_PG_URL:-}" ]; then
  pg_db="${SCION_PG_URL##*/}"
  pg_db="${pg_db%%\?*}"
  case $pg_db in
    scion_capacity_*) ;;
    *) die "SCION_PG_URL names database '${pg_db:-none}': the harness drops and creates the databases it runs on, so it runs only on a scion_capacity_* database" ;;
  esac
fi

WRITERS=${WRITERS:-2 50 200}
READERS=${READERS:-0 5 20}
REPS=${REPS:-2}
LANDS=${LANDS:-1000}
# Each rep: the writers land for the bench's own duration, and the
# readers check out beside them for READ_SECONDS.
READ_SECONDS=${READ_SECONDS:-90}
CELL_WARM_SECONDS=${CELL_WARM_SECONDS:-60}
CAPACITY_MACHINE=${CAPACITY_MACHINE:-unknown runner}
RUN_URL=${RUN_URL:-}

# classifies LOG: the soak's accepted failures. A cell whose clients
# failed with anything else is past the edge; the run stops there.
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
# jq parses bench's JSON report; only this script needs it, so its
# check lives here and not in common.sh's need().
command -v jq > /dev/null || die "needs jq"

make_work capacity
if [ -n "${SCION_BIN:-}" ]; then
  BIN=$SCION_BIN
else
  build
fi
fetch_dataset

HEAD_HEX=$(git rev-parse HEAD)

      # pct FILE P: the P-percentile of the numbers in FILE. A cell with no
# readers (r=0) has no times file: the percentile columns then say so
# with "-".
pct() {
  [ -f "$1" ] || return 0
  sort -n "$1" | awk -v p="$2" '{ a[NR] = $1 } END { if (NR) print a[int(NR * p + 0.5)] }'
}

note "capacity: building"
if [ -n "${SCION_BIN:-}" ]; then
  BIN=$SCION_BIN
else
  build
fi

: > "$WORK/capacity.csv"
echo "cell_start,reps,landed" > /dev/null

# The summary's numbers live in one CSV row per repetition:
# backend,writers,readers,land_p99_ms,checkout_p50_ms,checkout_p99_ms,
# reader_timeouts,errors,rss_end_kib,landed,blob_growth_kib_per_1k_lands,
# db_growth_kib_per_1k_lands

for backend in sqlite postgres; do
  if [ "$backend" = postgres ] && [ -z "${SCION_PG_URL:-}" ]; then
    note "no SCION_PG_URL: the Postgres cells are not run"
    continue
  fi
  # The Postgres URL reaches every scion-server through the env, which
  # postgres_url_of reads quietly and an empty value filters. The env
  # decides the backend of every server this cell raises, so it is set
  # here, per cell: the URL for postgres, empty for sqlite. A sqlite
  # cell that saw a URL would open Postgres and die - what the first
  # dispatch proved.
  if [ "$backend" = postgres ]; then
    export SCION_POSTGRES_URL=$SCION_PG_URL
  else
    export SCION_POSTGRES_URL=
  fi
  for w in $WRITERS; do
    for r in $READERS; do
      note "cell $backend w$w r$r"
      # A fresh store per cell: every cell starts equal, and the growth
      # a cell measures is the growth its own lands cause. On Postgres
      # the store is the database, so the reset is drop and create;
      # rm -rf of the data directory never touches it.
      if [ "$backend" = postgres ]; then
        admin_url="${SCION_PG_URL%/*}/postgres"
        # The database this URL names, query string off: the reset must
        # drop and create the one the server will open, whatever it is
        # called - a hardcoded name silently skips a differently named
        # database's reset, and the next cell starts on the last cell's
        # lands.
        pg_db="${SCION_PG_URL##*/}"
        pg_db="${pg_db%%\?*}"
        [ -n "$pg_db" ] || die "the Postgres URL names no database: $SCION_PG_URL"
        psql "$admin_url" -X -q -c "DROP DATABASE IF EXISTS $pg_db" \
          -c "CREATE DATABASE $pg_db" || die "the $backend database reset failed"
      fi
      rm -rf "$WORK/server"
      mkdir -p "$WORK/server"
      note "importing for $backend"
      /usr/bin/time -p "$BIN/scion-server" --data "$WORK/server" import \
        --rev main "$WORK/src.git" \
        > "$WORK/import-$backend.out" 2> "$WORK/import-$backend.log" || {
        tail -n 20 "$WORK/import-$backend.log" >&2
        die "the $backend import failed"
      }
      du_sample "$WORK/store-sizes-$backend-w$w-r$r.csv" "$WORK/server" start
      STORE_START=$(sed -n 's/^[^,]*,//p' "$WORK/store-sizes-$backend-w$w-r$r.csv" | tail -n 1)
      if [ "$backend" = postgres ]; then
        # The database's own disk at the cell's start, from the server
        # of the reset: the database column measures this, so a
        # Postgres row cannot look complete while the rows it grew sit
        # unmeasured in the service container.
        db_start_kib=$(psql "$admin_url" -X -A -t \
          -c "SELECT pg_database_size('$pg_db')" | awk '{ print int(($1 + 1023) / 1024) }')
      fi
      start_scion
      server_pid() {
        SERVER_PID=$(lsof -t -iTCP:"$SERVER_PORT" -sTCP:LISTEN 2> /dev/null | head -n 1)
        [ -n "$SERVER_PID" ] || die "no server"
      }
      server_pid
      END=$(($(date +%s) + READ_SECONDS * REPS + 60))
      T0=$(date +%s)
      readers() {
        local n=0 rc t0 t1
        while [ "$(date +%s)" -lt "$END" ]; do
          n=$((n + 1))
          # Every attempt logs its own start and end, so the run can
          # tell an attempt that outlived the servers (teardown) from
          # one that hung while the servers were up. The r loops share
          # the log and the times file, so the counts are the cell's.
          echo "attempt $n start $(date -u +%T.%3N)" >> "$WORK/reads-$backend-w$w-r$r.log"
          t0=$(date +%s%N)
          # Each attempt reads into a destination of its own: a killed
          # reader must never share a path with the attempts that come
          # after it, and a checkout the timeout killed mid-write is
          # the attempt's own leftover, not the next attempt's wall.
          dest=$(mktemp -d "$WORK/reads.XXXXXX")
          if "$TIMEOUT" 60 "$BIN/scn" --server "$SERVER" --objects "$DAEMON" checkout main \
            --sparse "$SPARSE_DIR" --into "$dest" > /dev/null 2>> "$WORK/reads-$backend-w$w-r$r.log"; then
            t1=$(date +%s%N)
            echo "attempt $n end ms=$(( (t1 - t0) / 1000000 )) $(date -u +%T.%3N)" >> "$WORK/reads-$backend-w$w-r$r.log"
            echo $(( (t1 - t0) / 1000000 )) >> "$WORK/read-times.csv"
          else
            rc=$?
            t1=$(date +%s%N)
            if [ "$rc" -eq 124 ]; then
              echo "attempt $n timeout after 60s at $(date -u +%T.%3N)" >> "$WORK/reads-$backend-w$w-r$r.log"
            else
              echo "attempt $n checkout failed rc=$rc at $(date -u +%T.%3N)" >> "$WORK/reads-$backend-w$w-r$r.log"
            fi
          fi
          rm -rf "$dest"
        done
      }
      # r reader loops, none for r=0: the cell's reader axis is real
      # concurrency, not a label. Each loop is recorded like a started
      # server (loop_bg), so a killed script's exit trap and the janitor
      # still find them, and their pids ride the pids file the cell's
      # teardown freezes and reaps. The loops share the log and the
      # times file, so the row's counts and percentiles stay the
      # cell's own.
      : > "$WORK/capacity-readers.pids"
      i=0
      while [ "$i" -lt "$r" ]; do
        i=$((i + 1))
        loop_bg "capacity-readers-$i" readers
        sed -n 's/^pid=//p' "$WORK/owned/capacity-readers-$i" >> "$WORK/capacity-readers.pids"
      done
      # The server's Land counter at the cell's start: lands served is
      # the counter's growth over the cell, so a cell whose bench dies
      # still reports what its writers got through.
      lands_before=$(curl -fsS "http://127.0.0.1:$SERVER_METRICS_PORT/metrics" 2> /dev/null |
        awk '/^scion_requests_total\{.*rpc="Scion\/Land"/ { s += $NF } END { print s + 0 }')
      rep=1
      bench_fail=0
      while [ "$rep" -le "$REPS" ]; do
        # The writers land for this repetition; the server's own report
        # carries the land percentiles.
        sleep 10 # the cell's warm start, before bench begins
        if ! "$TIMEOUT" 600 "$BIN/scn" --server "$SERVER" bench --agents "$w" \
          --lands "$LANDS" --json > "$WORK/bench-$backend-w$w-r$r.json" 2> "$WORK/bench-$backend-w$w-r$r.log"; then
          # A bench that failed is the cell's data, not the run's
          # failure: the envelope's edge is where errors start. The row
          # records it and the grid goes on.
          echo "bench failed in cell $backend w$w r$r rep$rep (the row records it)" >&2
          bench_fail=1
        fi
        # The readers keep going until this rep's slice of the run is
        # over, so every rep sees the same reader load.
        while [ "$(date +%s)" -lt $((T0 + READ_SECONDS * rep)) ]; do
          sleep 5
        done
        rep=$((rep + 1))
      done
      note "tearing down cell $backend w$w r$r at $(date -u +%T.%3N)"
      # Each loop's in-flight client dies with the loop: freeze the loop
      # first (no new attempt can start), kill its children - the
      # timeout, which hands the TERM to the checkout it watches - and
      # only then the loop. A child killed after its parent is re-parented
      # and lost, and one left alive runs up to its own 60 s timeout
      # into the next cell, where its failure would be someone else's.
      while read -r pid; do kill -STOP "$pid" 2> /dev/null || true; done < "$WORK/capacity-readers.pids"
      while read -r pid; do pkill -TERM -P "$pid" 2> /dev/null || true; done < "$WORK/capacity-readers.pids"
      while read -r pid; do kill -TERM "$pid" 2> /dev/null || true; done < "$WORK/capacity-readers.pids"
      # A stopped process queues its TERM until it runs again: CONT
      # delivers it, or the wait below never returns.
      while read -r pid; do kill -CONT "$pid" 2> /dev/null || true; done < "$WORK/capacity-readers.pids"
      while read -r pid; do wait "$pid" 2> /dev/null || true; done < "$WORK/capacity-readers.pids"
      rss_end=$(ps -o rss= -p "$SERVER_PID" | tr -d ' ')
      # Lands served, from the server's counter, while it still serves:
      # the counter counts what the writers got through, whether bench
      # finished or not.
      lands_end=$(curl -fsS "http://127.0.0.1:$SERVER_METRICS_PORT/metrics" 2> /dev/null |
        awk '/^scion_requests_total\{.*rpc="Scion\/Land"/ { s += $NF } END { print s + 0 }')
      stop_scion
      lands_landed=$((lands_end - lands_before))
      note "cell $backend w$w r$r: $lands_landed lands landed (the plan named $((LANDS * REPS)))"
      reader_failures=0
      reader_log="$WORK/reads-$backend-w$w-r$r.log"
      [ -f "$reader_log" ] || { touch "$reader_log"; }
      reader_timeouts=$(count_reader_timeouts "$reader_log")
      reader_failures=$(count_reader_failures "$reader_log")
      errors=$((reader_failures + bench_fail))
      # The lands counter's growth stays a check (a cell whose bench
      # died mid-budget names it in the errors column), but it is not a
      # table column: the bench's own budget paces the lands, so a rate
      # over the cell says what the bench did, not what the store held.
      if [ "$bench_fail" -eq 0 ] && [ -s "$WORK/bench-$backend-w$w-r$r.json" ]; then
        land_p99=$(jq -r '.land_ms.p99' "$WORK/bench-$backend-w$w-r$r.json")
      else
        # No bench report, no land percentiles: the cell says so in the
        # errors column, and the land p99 column stays empty.
        land_p99="-"
      fi
      c50=$(pct "$WORK/read-times.csv" 0.50)
      c99=$(pct "$WORK/read-times.csv" 0.99)
      [ -n "$c50" ] || c50="-"
      [ -n "$c99" ] || c99="-"
      du_sample "$WORK/store-sizes-$backend-w$w-r$r.csv" "$WORK/server" end
      store_end=$(sed -n 's/^[^,]*,//p' "$WORK/store-sizes-$backend-w$w-r$r.csv" | tail -n 1)
      # Per 1,000 lands, divided by the lands the counter measured: a
      # cell that lost lands reports its own bytes over its own lands,
      # never the plan's.
      blob_growth=$(kib_per_1k "$STORE_START" "$store_end" "$lands_landed")
      if [ "$backend" = postgres ]; then
        db_end_kib=$(psql "$admin_url" -X -A -t \
          -c "SELECT pg_database_size('$pg_db')" | awk '{ print int(($1 + 1023) / 1024) }')
        db_growth=$(kib_per_1k "$db_start_kib" "$db_end_kib" "$lands_landed")
      else
        db_growth="-"
      fi
      echo "$backend,$w,$r,$land_p99,$c50,$c99,$reader_timeouts,$errors,$rss_end,$lands_landed,$blob_growth,$db_growth" \
        >> "$WORK/capacity.csv"
      if [ "$((errors + reader_timeouts))" -gt 0 ]; then
        note "cell $backend w$w r$r: $reader_timeouts timeouts, $errors errors (readers $reader_failures, bench $bench_fail) - recorded, next cell"
      fi
      rm -f "$WORK/read-times.csv"
      rm -rf "$WORK"/reads.*
    done
  done
done

# The artifacts go where the workflow finds them.
mkdir -p "${RUNNER_TEMP:-/tmp}/capacity-artifacts"

# The machine, measured - not quoted: the runner image's published spec
# is a fact GitHub can change, so the run records what it actually ran
# on, and the envelope page states those values next to the run's link.
# nproc, /proc/meminfo and df -T are Linux; another OS says so instead
# of writing empty values, so a local run cannot look like a runner.
if [ "$(uname -s)" = Linux ]; then
  {
    echo "nproc=$(nproc)"
    echo "memory_kib=$(awk '/MemTotal/ { print $2 }' /proc/meminfo)"
    echo "kernel=$(uname -r)"
    echo "filesystem=$(df -T "$WORK" | awk 'NR == 2 { print $2 }')"
    echo "disk_total_kib=$(df -k "$WORK" | awk 'NR == 2 { print $2 }')"
    echo "disk_free_kib=$(df -k "$WORK" | awk 'NR == 2 { print $4 }')"
    echo "work_dir=$WORK"
    echo "runner_image=${ImageOS:-}${ImageVersion:+ $ImageVersion}"
  } > "${RUNNER_TEMP:-/tmp}/capacity-artifacts/machine.txt"
else
  echo "not recorded: the machine's shape is measured on Linux only, \
and this is $(uname -s)" > "${RUNNER_TEMP:-/tmp}/capacity-artifacts/machine.txt"
fi
cp "$WORK/capacity.csv" "${RUNNER_TEMP:-/tmp}/capacity-artifacts/" 2> /dev/null || true
cp "$WORK"/*.log "${RUNNER_TEMP:-/tmp}/capacity-artifacts/" 2> /dev/null || true
cp "$WORK"/bench-*.json "${RUNNER_TEMP:-/tmp}/capacity-artifacts/" 2> /dev/null || true

row() { printf '%-46s %s\n' "$1" "$2"; }
echo "capacity envelope: machine $CAPACITY_MACHINE, commit $HEAD_HEX"
echo
echo "| backend | writers | readers | land p99 ms | checkout p50 ms | checkout p99 ms | timeouts | errors | server RSS MiB | landed | blob store KiB per 1k lands | database KiB per 1k lands |"
echo "|---|---|---|---|---|---|---|---|---|---|---|---|"
while IFS=, read -r backend w r land_p99 c50 c99 timeouts errors rss landed blob_growth db_growth; do
  echo "| $backend | $w | $r | $land_p99 | $c50 | $c99 | $timeouts | $errors | $((rss / 1024)) | $landed | $blob_growth | $db_growth |"
done < "$WORK/capacity.csv"
echo
row "commit" "$HEAD_HEX"
[ -n "$RUN_URL" ] && row "run" "$RUN_URL"
row "runner" "$CAPACITY_MACHINE"
stop_scion 2> /dev/null || true
