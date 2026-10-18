#!/usr/bin/env bash
# Checkout benchmark against git (issue #6). REPS times, JOBS sparse
# checkouts run at once, through one scion-daemon (cold, then warm), and
# through git in three ways that write the same files. The runs alternate
# between the tools, so machine drift hits both alike. Prints, per state,
# the median with min and max of wall time, CPU time and bytes from the
# server, and the ratios of the medians between git and scion.
# docs/benchmarks.md describes the method and what each number means.
#
# Settings, from the environment:
#   JOBS=50 REPS=5           checkouts at once, and repetitions per state
#   SPARSE_DIR=tokio/src     what each checkout writes
#   REPO_URL, REPO_COMMIT    the dataset (scripts/common.sh)
#   SERVER_PORT=17878 CACHE_PORT=17879 GIT_PORT=19418   loopback ports
#   SERVER_METRICS_PORT=17880 CACHE_METRICS_PORT=17881  metrics exports
set -euo pipefail
# shellcheck source=common.sh source-path=SCRIPTDIR
. "$(dirname "$0")/common.sh"
JOBS=${JOBS:-50}
REPS=${REPS:-5}
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


# record LABEL RUN BYTES SERVER_LOG [LOG...]: checks that the first job of
# RUN wrote the same files as the first scion job, adds a line to the
# results, and removes the output of RUN. CPU time adds up the jobs, the
# server (SERVER_LOG) and the other logs.
record() {
  local label=$1 run=$2 bytes=$3
  shift 3
  if [ -d "$WORK/expect" ]; then
    diff -r -x .git "$WORK/expect" "$WORK/$run/1" > /dev/null || die "$run wrote other files"
  else
    mv "$WORK/$run/1" "$WORK/expect"
  fi
  printf '%s|%s|%s|%s|%s|%s\n' "$label" "$run" "$(wall "$WORK/$run.log")" \
    "$(cpu "$WORK/$run.log" "$@")" "$(cpu "$1")" "$bytes" >> "$WORK/results"
  rm -rf "${WORK:?}/$run"
}

# scion_run STATE REP: one run through the daemon. RUN is scion-STATE-REP. The server, cache and
# daemon start fresh, so their CPU time is this run's; the daemon keeps its
# disk store within a rep, from the cold run to the warm one. The bytes from
# the server are the out-bytes counters of its metrics export, scraped
# before and after the run.
scion_run() {
  local run=scion-$1-$2 before after bytes
  note "scion, $1 daemon (rep $2)"
  if [ "$1" = cold ]; then
    # A cold rep starts from an empty daemon. The disk store of the last
    # warm rep would serve every object from disk, and the cold rep of
    # rep 2 on would read warm.
    rm -rf "${WORK:?}/daemon"
  fi
  start_scion
  before=$(scrape_metrics "http://127.0.0.1:$SERVER_METRICS_PORT")
  gbefore=$(getobjects "http://127.0.0.1:$SERVER_METRICS_PORT")
  batch "$run" "$JOBS" "$CHECKOUT"
  after=$(scrape_metrics "http://127.0.0.1:$SERVER_METRICS_PORT")
  gafter=$(getobjects "http://127.0.0.1:$SERVER_METRICS_PORT")
  bytes=$((after - before))
  stop_scion
  # A cold rep fetches its objects from the server: its daemon starts
  # with an empty store, so the server answers at least one GetObjects.
  # (The server's bytes are stored zstd frames, so they run well under
  # the checkout's uncompressed size, and no byte floor tells cold from
  # warm. The GetObjects counter can.) A daemon that still holds the
  # objects fetches none, which this fails on.
  [ "$1" = warm ] || [ "$((gafter - gbefore))" -gt 0 ] || \
    die "a cold run fetched no object from the server: the daemon was not cold"
  record "scion, $1 daemon" "$run" "$bytes" \
    "$WORK/server.log" "$WORK/cache.log" "$WORK/daemon.log"
}

# git_run LABEL REP RUN FLAGS: one run of `git clone FLAGS` and a sparse
# checkout, against a fresh git daemon. Its CPU time includes the upload-pack
# and pack-objects processes that it starts. The bytes from the server are
# the pack files that the clones received. Each job writes only SPARSE_DIR,
# as scn checkout --sparse does: no cone, so no files from the directories
# above it, and nothing written before the sparse rules are set.
git_run() {
  local label=$1 rep=$2 run=$3 flags=$4
  note "$label (rep $rep)"
  start gitd "Ready to rumble" git daemon --verbose --listen=127.0.0.1 --port="$GIT_PORT" \
    --reuseaddr --export-all --max-connections=0 --base-path="$WORK" "$WORK/src.git"
  batch "$run" "$JOBS" "git clone -q --no-checkout $flags \"\$GIT_URL\" \"\$1\" &&
    git -C \"\$1\" sparse-checkout set --no-cone \"/\$SPARSE_DIR/\" &&
    git -C \"\$1\" checkout -q main"
  stop gitd
  record "$label" "$run" "$(file_bytes "$WORK/$run" -name '*.pack')" "$WORK/gitd.log"
}

# The runs interleave: in each rep, a scion run is followed by the git runs
# it compares with. A plain or partial clone keeps nothing on the machine
# (cold), and the mirror is git's warm state.
for rep in $(seq 1 "$REPS"); do
  scion_run cold "$rep"
  git_run "git clone" "$rep" "git-full-$rep" ""
  git_run "git clone --filter=blob:none" "$rep" "git-partial-$rep" "--filter=blob:none"
  scion_run warm "$rep"
  git_run "git clone --reference mirror" "$rep" "git-mirror-$rep" '--reference $MIRROR'
done

files=$(find "$WORK/expect" -type f | wc -l | tr -d ' ')
echo "Dataset: $REPO_URL at ${REPO_COMMIT:0:12}, $COMMITS commits."
echo "Each run: $JOBS checkouts at once of $SPARSE_DIR/ ($files files), $REPS repetitions per state."
echo
# Per label (one run kind), the median with min and max of each column; then,
# per git run kind, the ratio of its medians to the scion kind of the same
# temperature (cold: the plain and partial clones, warm: the mirror).
awk -F'|' '
  # median KEY COL: prints "median (min-max)".
  function median(key, col,   n, i, vals, t, j, m) {
    n = count[key]
    for (i = 1; i <= n; i++) vals[i] = v[key, i, col]
    for (i = 2; i <= n; i++)
      for (j = i; j > 1 && vals[j - 1] > vals[j]; j--) {
        t = vals[j]; vals[j] = vals[j - 1]; vals[j - 1] = t
      }
    m = int((n + 1) / 2)
    return sprintf("%.2f (%.2f-%.2f)", vals[m], vals[1], vals[n])
  }
  # mid KEY COL: only the median, as a number, for the ratios.
  function mid(key, col) {
    return median(key, col) + 0
  }
  function ratio(a, b) { return b > 0 ? sprintf("%.1fx", a / b) : "-" }
  {
    if (!($1 in seen)) { order[++kinds] = $1; seen[$1] = 1 }
    count[$1]++
    v[$1, count[$1], "wall"] = $3 + 0
    v[$1, count[$1], "cpu"] = $4 + 0
    v[$1, count[$1], "scpu"] = $5 + 0
    v[$1, count[$1], "mib"] = $6 / 1048576
    # The mirror run and the warm daemon run hold the objects already; every
    # other run starts from nothing.
    temp[$1] = ($1 ~ /mirror|warm/) ? "warm" : "cold"
  }
  END {
    f = "%-52s %20s %20s %20s %20s\n"
    printf f, "run (median, min-max)", "wall s", "CPU s", "server CPU s", "server MiB"
    for (i = 1; i <= kinds; i++)
      printf f, order[i], median(order[i], "wall"), median(order[i], "cpu"),
        median(order[i], "scpu"), median(order[i], "mib")
    printf "\n" f, "git / scion (medians)", "wall", "CPU", "server CPU", "server MiB"
    for (i = 1; i <= kinds; i++) {
      git = order[i]
      if (git !~ /^git/) continue
      for (j = 1; j <= kinds; j++)
        if (order[j] ~ /^scion, / && temp[order[j]] == temp[git]) {
          pair = order[j]
          printf f, git " / " pair,
            ratio(mid(git, "wall"), mid(pair, "wall")),
            ratio(mid(git, "cpu"), mid(pair, "cpu")),
            ratio(mid(git, "scpu"), mid(pair, "scpu")),
            ratio(mid(git, "mib"), mid(pair, "mib"))
        }
    }
  }' "$WORK/results"