#!/usr/bin/env bash
# A demo that anyone can rerun (issue #55). Builds the release binaries,
# imports a public repository at a fixed commit, and starts the server, a
# cache and a daemon on loopback. Then many agents land changes at once
# (`scn bench`, which checks main afterwards), sparse checkouts run through
# the daemon, and a short table shows the numbers. Everything stops, and
# everything the demo made is removed, when it exits.
#
# Settings, from the environment:
#   AGENTS=100 LANDS=2000    scn bench: agents, and lands in all
#   JOBS=10                  warm checkouts at once
#   SPARSE_DIR=tokio/src     what each checkout writes
#   REPO_URL, REPO_COMMIT    the dataset (scripts/common.sh)
#   SERVER_PORT=17878 CACHE_PORT=17879   loopback ports
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"
AGENTS=${AGENTS:-100}
LANDS=${LANDS:-2000}
JOBS=${JOBS:-10}

# json_num KEY FILE: a number from one line of JSON, as `scn --json` prints.
json_num() { sed -E -n "s/.*\"$1\":([0-9.eE+-]+).*/\1/p" "$2"; }

need
make_work demo
build
fetch_dataset
import_dataset
start_scion
note "landing: $AGENTS agents, $LANDS lands"
# scn bench fails unless main gained each landed change exactly once.
"$BIN/scn" --server "$SERVER" --json bench --agents "$AGENTS" --lands "$LANDS" \
  > "$WORK/bench.json"
note "checking out $SPARSE_DIR through the daemon"
batch checkout-cold 1 "$CHECKOUT"
batch checkout-warm "$JOBS" "$CHECKOUT"
files=$(find "$WORK/checkout-cold/1" -type f | wc -l | tr -d ' ')
stop_scion

landed=$(json_num landed "$WORK/bench.json")
rate=$(json_num lands_per_second "$WORK/bench.json")
p99=$(sed -E -n 's/.*"land_ms":\{[^}]*"p99":([0-9.eE+-]+).*/\1/p' "$WORK/bench.json")
row() { printf '%-40s %s\n' "$1" "$2"; }
echo "open-scion demo: $REPO_URL at ${REPO_COMMIT:0:12}"
echo
row "step" "result"
row "import, $COMMITS commits" "$(wall "$WORK/import.log") s"
row "land, $AGENTS agents" "$landed lands, $(printf '%.0f' "$rate") per s, land p99 $(printf '%.0f' "$p99") ms"
row "" "main gained each land exactly once"
row "checkout $SPARSE_DIR/, cold daemon" "$(wall "$WORK/checkout-cold.log") s, $files files"
row "checkout $SPARSE_DIR/, warm, $JOBS at once" "$(wall "$WORK/checkout-warm.log") s"
