#!/usr/bin/env bash
# Checkout benchmark against git (issue #6). JOBS sparse checkouts run at
# once, through one scion-daemon (cold, then warm), and then through git in
# three ways that write the same files. Prints wall time, CPU time and bytes
# from the server for each run, and the ratios between git and scion.
# docs/benchmarks.md describes the method and what each number means.
#
# Settings, from the environment:
#   JOBS=50                  checkouts at once, in each run
#   SPARSE_DIR=tokio/src     what each checkout writes
#   REPO_URL, REPO_COMMIT    the dataset (scripts/common.sh)
#   SERVER_PORT=17878 CACHE_PORT=17879 GIT_PORT=19418   loopback ports
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"
JOBS=${JOBS:-50}
GIT_PORT=${GIT_PORT:-19418}

need
make_work bench
build
fetch_dataset
import_dataset
# The git server: partial clone needs filters. The warm mirror is made once,
# before any run, as a CI machine keeps one.
git -C "$WORK/src.git" config uploadpack.allowFilter true
git clone -q --mirror "$WORK/src.git" "$WORK/mirror.git"
GIT_URL=git://127.0.0.1:$GIT_PORT/src.git
MIRROR=$WORK/mirror.git
export GIT_URL MIRROR

# record LABEL STATE RUN BYTES SERVER_LOG [LOG...]: checks that the first job
# of RUN wrote the same files as the first scion job, adds a line to the
# results, and removes the output of RUN. CPU time adds up the jobs, the
# server (SERVER_LOG) and the other logs.
record() {
  local label=$1 state=$2 run=$3 bytes=$4
  shift 4
  if [ -d "$WORK/expect" ]; then
    diff -r -x .git "$WORK/expect" "$WORK/$run/1" > /dev/null || die "$run wrote other files"
  else
    mv "$WORK/$run/1" "$WORK/expect"
  fi
  printf '%s|%s|%s|%s|%s|%s\n' "$label" "$state" "$(wall "$WORK/$run.log")" \
    "$(cpu "$WORK/$run.log" "$@")" "$(cpu "$1")" "$bytes" >> "$WORK/results"
  rm -rf "${WORK:?}/$run"
}

# scion_run STATE: one run through the daemon. The server, cache and daemon
# start fresh, so their CPU time is this run's; the daemon keeps its disk
# store. Each tier fetches an object that it lacks once and keeps it, and the
# cache starts empty, so what the daemon's data directory gains in the run is
# what the server sent.
scion_run() {
  local run=scion-$1 before bytes
  note "scion, $1 daemon"
  start_scion
  before=$(file_bytes "$WORK/daemon")
  batch "$run" "$JOBS" "$CHECKOUT"
  bytes=$(($(file_bytes "$WORK/daemon") - before))
  stop_scion
  [ "$1" = warm ] || [ "$bytes" -gt 0 ] || die "the daemon gained no bytes in a cold run"
  record "scion, $1 daemon" "$1" "$run" "$bytes" \
    "$WORK/server.log" "$WORK/cache.log" "$WORK/daemon.log"
}

# git_run LABEL STATE RUN FLAGS: one run of `git clone FLAGS` and a sparse
# checkout, against a fresh git daemon. Its CPU time includes the upload-pack
# and pack-objects processes that it starts. The bytes from the server are
# the pack files that the clones received. Each job writes only SPARSE_DIR,
# as scn checkout --sparse does: no cone, so no files from the directories
# above it, and nothing written before the sparse rules are set.
git_run() {
  note "$1"
  start gitd "Ready to rumble" git daemon --verbose --listen=127.0.0.1 --port="$GIT_PORT" \
    --reuseaddr --export-all --max-connections=0 --base-path="$WORK" "$WORK/src.git"
  batch "$3" "$JOBS" "git clone -q --no-checkout $4 \"\$GIT_URL\" \"\$1\" &&
    git -C \"\$1\" sparse-checkout set --no-cone \"/\$SPARSE_DIR/\" &&
    git -C \"\$1\" checkout -q main"
  stop gitd
  record "$1" "$2" "$3" "$(file_bytes "$WORK/$3" -name '*.pack')" "$WORK/gitd.log"
}

scion_run cold
scion_run warm
git_run "git clone" cold git-full ""
git_run "git clone --filter=blob:none" cold git-partial --filter=blob:none
# shellcheck disable=SC2016 # expanded by the sh that batch starts
git_run "git clone --reference mirror" warm git-mirror '--reference "$MIRROR"'

files=$(find "$WORK/expect" -type f | wc -l | tr -d ' ')
echo "Dataset: $REPO_URL at ${REPO_COMMIT:0:12}, $COMMITS commits."
echo "Each run: $JOBS checkouts at once of $SPARSE_DIR/ ($files files)."
echo
# Each git run is divided by the scion run in the same state, which runs
# first: a plain or partial clone keeps nothing on the machine (cold), and
# the mirror is git's warm state.
awk -F'|' '
  function ratio(a, b) { return b > 0 ? sprintf("%.1fx", a / b) : "-" }
  {
    label[NR] = $1; state[NR] = $2; wall[NR] = $3; cpu[NR] = $4; scpu[NR] = $5
    mib[NR] = $6 / 1048576
    if (!($2 in base)) base[$2] = NR
  }
  END {
    f = "%-36s %8s %8s %13s %11s\n"
    printf f, "run", "wall s", "CPU s", "server CPU s", "server MiB"
    for (i = 1; i <= NR; i++)
      printf "%-36s %8.2f %8.2f %13.2f %11.1f\n", label[i], wall[i], cpu[i], scpu[i], mib[i]
    printf "\n" f, "git / scion", "wall", "CPU", "server CPU", "server MiB"
    for (i = 1; i <= NR; i++) {
      b = base[state[i]]
      if (b == i) continue
      printf f, label[i] " / " state[i], ratio(wall[i], wall[b]), ratio(cpu[i], cpu[b]),
        ratio(scpu[i], scpu[b]), ratio(mib[i], mib[b])
    }
  }' "$WORK/results"
