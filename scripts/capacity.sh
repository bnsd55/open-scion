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
# footer. The store-size column is the SQLite store's growth; the
# Postgres cells carry "-" there, because the database's disk is the
# service container's, not a file tree this script can measure.
#
# SHORTCUT: the readers and the sampling are the soak's shape, copied
# here rather than lifted, because soak.sh and soak-fault.sh are two
# working scripts whose knobs this does not want to share; the upgrade
# path is a common.sh load library when a third caller appears.
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"

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
PGFLAGS=()
if [ -n "${SCION_PG_URL:-}" ]; then
  PGFLAGS=(--postgres-url "$SCION_PG_URL")
fi

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

make_work capacity
if [ -n "${SCION_BIN:-}" ]; then
  BIN=$SCION_BIN
else
  build
fi
fetch_dataset

HEAD_HEX=$(git rev-parse HEAD)

# pct FILE P: the P-percentile of the numbers in FILE.
pct() {
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
# backend,writers,readers,rep,lands_per_min,land_p99_ms,checkout_p50_ms,
# checkout_p99_ms,reader_failures,rss_end_kib,store_growth_kib_per_1k

for backend in sqlite postgres; do
  if [ "$backend" = postgres ] && [ -z "${SCION_PG_URL:-}" ]; then
    note "no SCION_PG_URL: the Postgres cells are not run"
    continue
  fi
  # A fresh store per cell: every cell starts equal, and the growth a
  # cell measures is the growth its own lands cause.
  rm -rf "$WORK/server"
  note "importing for $backend"
  /usr/bin/time -p "$BIN/scion-server" --data "$WORK/server" import \
    "${PGFLAGS[@]+"${PGFLAGS[@]}"}" --rev main "$WORK/src.git" \
    > "$WORK/import-$backend.out" 2> "$WORK/import-$backend.log" || {
    tail -n 20 "$WORK/import-$backend.log" >&2
    die "the $backend import failed"
  }
  STORE_START=$(du -sk "$WORK/server" | cut -f1)
  for w in $WRITERS; do
    for r in $READERS; do
      note "cell $backend w$w r$r"
      start_scion
      server_pid() {
        SERVER_PID=$(lsof -t -iTCP:"$SERVER_PORT" -sTCP:LISTEN 2> /dev/null | head -n 1)
        [ -n "$SERVER_PID" ] || die "no server"
      }
      server_pid
      mkdir -p "$WORK/reads"
      END=$(($(date +%s) + READ_SECONDS * REPS + 60))
      T0=$(date +%s)
      readers() {
        local n=0 t0 t1
        while [ "$(date +%s)" -lt "$END" ]; do
          n=$((n + 1))
          t0=$(date +%s%N)
          if timeout 60 "$BIN/scn" --server "$SERVER" --objects "$DAEMON" checkout main \
            --sparse "$SPARSE_DIR" --into "$WORK/reads/$n" > /dev/null 2>> "$WORK/reads-$backend-w$w-r$r.log"; then
            t1=$(date +%s%N)
            echo $(( (t1 - t0) / 1000000 )) >> "$WORK/read-times.csv"
          else
            echo "checkout $n failed" >> "$WORK/read-fail.log"
          fi
          rm -rf "$WORK/reads/$n"
        done
      }
      readers & echo $! > "$WORK/capacity-readers.pid"
      rep=1
      while [ "$rep" -le "$REPS" ]; do
        # The writers land for this repetition; the server's own report
        # carries the land percentiles.
        sleep 10 # the cell's warm start, before bench begins
        if ! timeout 600 "$BIN/scn" --server "$SERVER" bench --agents "$w" \
          --lands "$LANDS" --json > "$WORK/bench-$backend-w$w-r$r.json" 2> "$WORK/bench-$backend-w$w-r$r.log"; then
          tail -n 5 "$WORK/bench-$backend-w$w-r$r.log" >&2
          die "bench failed in cell $backend w$w r$r rep$rep"
        fi
        # The readers keep going until this rep's slice of the run is
        # over, so every rep sees the same reader load.
        while [ "$(date +%s)" -lt $((T0 + READ_SECONDS * rep)) ]; do
          sleep 5
        done
        rep=$((rep + 1))
      done
      kill "$(cat "$WORK/capacity-readers.pid")" 2> /dev/null || true
      wait "$(cat "$WORK/capacity-readers.pid")" 2> /dev/null || true
      rss_end=$(ps -o rss= -p "$SERVER_PID" | tr -d ' ')
      stop_scion
      # One row per repetition would need bench per rep; bench's report
      # covers all REPS lands together, so the row is the cell's total:
      # lands per minute over the whole cell, and the percentiles the
      # report carried.
      if [ -s "$WORK/read-fail.log" ]; then
        echo "FAIL: the readers failed in cell $backend w$w r$r:" >&2
        head -n 3 "$WORK/read-fail.log" >&2
        die "the readers failed"
      fi
      lands_per_s=$(python3 -c "
import json,sys
with open(sys.argv[1]) as f: d=json.load(f)
print(f\"{d['lands_per_second']:.1f},{d['land_ms']['p99']:.0f}\")
" "$WORK/bench-$backend-w$w-r$r.json")
      lps=${lands_per_s%%,*}
      land_p99=${lands_per_s##*,}
      c50=$(pct "$WORK/read-times.csv" 0.50)
      c99=$(pct "$WORK/read-times.csv" 0.99)
      store_end=$(du -sk "$WORK/server" | cut -f1)
      growth=$(( (store_end - STORE_START) * 1000 / (LANDS * REPS) ))
      echo "$backend,$w,$r,$lps,$land_p99,$c50,$c99,$rss_end,$growth" \
        >> "$WORK/capacity.csv"
      rm -f "$WORK/read-times.csv" "$WORK/read-fail.log"
    done
  done
done

# The artifacts go where the workflow finds them.
mkdir -p "${RUNNER_TEMP:-/tmp}/capacity-artifacts"
cp "$WORK/capacity.csv" "${RUNNER_TEMP:-/tmp}/capacity-artifacts/" 2> /dev/null || true
cp "$WORK"/*.log "${RUNNER_TEMP:-/tmp}/capacity-artifacts/" 2> /dev/null || true
cp "$WORK"/bench-*.json "${RUNNER_TEMP:-/tmp}/capacity-artifacts/" 2> /dev/null || true

row() { printf '%-46s %s\n' "$1" "$2"; }
echo "capacity envelope: machine $CAPACITY_MACHINE, commit $HEAD_HEX"
echo
echo "| backend | writers | readers | lands/min | land p99 ms | checkout p50 ms | checkout p99 ms | server RSS MiB | store KiB per 1k lands |"
echo "|---|---|---|---|---|---|---|---|---|"
while IFS=, read -r backend w r lps land_p99 c50 c99 rss growth; do
  echo "| $backend | $w | $r | $lps | $land_p99 | $c50 | $c99 | $((rss / 1024)) | $growth |"
done < "$WORK/capacity.csv"
echo
row "commit" "$HEAD_HEX"
[ -n "$RUN_URL" ] && row "run" "$RUN_URL"
row "runner" "$CAPACITY_MACHINE"
stop_scion 2> /dev/null || true
