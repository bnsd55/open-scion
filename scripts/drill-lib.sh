# The shared drill helpers: sourced by store-upgrade.sh and
# backup-n1.sh. The preamble both want, and the function bodies
# both call; a run section belongs to its caller, never here.

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# The scripts give every address, name and path as a flag or their own
# export, so the caller's environment must not change the story. Git runs
# without the user's and the system's config, and NO_COLOR keeps the log
# lines plain, so the `serving listen=` lines parse.
unset SCION_SERVER SCION_REPO SCION_DATA SCION_AUTHOR
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export SCION_TOKEN=upgrade NO_COLOR=1


die() {
  echo "store-upgrade: $*" >&2
  exit 1
}

note() { echo "-- $*" >&2; }

summary() {
  echo "$*" >&2
  [ -z "${GITHUB_STEP_SUMMARY:-}" ] || echo "$*" >> "$GITHUB_STEP_SUMMARY"
}

schema_of() {
  sed -n 's/.*SCHEMA_VERSION: u32 = \([0-9]*\).*/\1/p' "$1/crates/scion-server/src/store.rs" | head -1
}

make_repo() {
  local dir=$1
  git init -q -b main "$dir/src"
  printf 'pub fn main() {}\n' >"$dir/src/hello.rs"
  printf '# upgrade\n\nA small repository for the tagged-store job.\n' >"$dir/src/README.md"
  git -C "$dir/src" add -A
  GIT_AUTHOR_NAME=upgrade GIT_AUTHOR_EMAIL=upgrade@localhost \
  GIT_COMMITTER_NAME=upgrade GIT_COMMITTER_EMAIL=upgrade@localhost \
    git -C "$dir/src" commit -q -m first
  printf '// one more line\n' >>"$dir/src/hello.rs"
  GIT_AUTHOR_NAME=upgrade GIT_AUTHOR_EMAIL=upgrade@localhost \
  GIT_COMMITTER_NAME=upgrade GIT_COMMITTER_EMAIL=upgrade@localhost \
    git -C "$dir/src" commit -qam second
  printf 'One more line of prose.\n' >>"$dir/src/README.md"
  GIT_AUTHOR_NAME=upgrade GIT_AUTHOR_EMAIL=upgrade@localhost \
  GIT_COMMITTER_NAME=upgrade GIT_COMMITTER_EMAIL=upgrade@localhost \
    git -C "$dir/src" commit -qam third
}

start() {
  local name=$1 dir=$2 bin=$3 ready=$4 log pid port tries=300
  shift 4
  log="$dir/$name.log"
  "$bin" "$@" >"$log" 2>&1 &
  pid=$!
  echo "$pid" >"$log.pid"
  while :; do
    port=$(sed -n 's/.* serving listen=127\.0\.0\.1:\([0-9]*\).*/\1/p' "$log" | tail -1)
    if [ -n "$port" ]; then
      break
    fi
    if ! kill -0 "$pid" 2> /dev/null; then
      tail -n 10 "$log" >&2
      die "$name did not start"
    fi
    tries=$((tries - 1))
    [ "$tries" -gt 0 ] || {
      tail -n 10 "$log" >&2
      die "$name did not come up within 30 s"
    }
    sleep 0.1
  done
  [ "$ready" = port ] && printf '%s\n' "$port"
  return 0
}

stop_one() {
  local dir=$1 name=$2 log pid
  log="$dir/$name.log"
  pid=$(cat "$log.pid")
  if ! kill -0 "$pid" 2> /dev/null; then
    rm -f "$log.pid"
    return 0
  fi
  kill "$pid" 2> /dev/null || true
  for _ in $(seq 1 100); do
    if ! kill -0 "$pid" 2> /dev/null; then
      rm -f "$log.pid"
      return 0
    fi
    sleep 0.1
  done
  kill -9 "$pid" 2> /dev/null || true
  rm -f "$log.pid"
  note "$name needed a KILL to stop"
}

fingerprint() {
  local db=$1
  {
    echo "version $(sqlite3 "$db" 'PRAGMA user_version')"
    sqlite3 "$db" "SELECT type || ' ' || tbl_name || ' ' || sql FROM sqlite_master
                   WHERE sql IS NOT NULL ORDER BY type, tbl_name, sql"
    for t in repos changesets changeset_parents repo_changesets refs trees git_import history_nodes; do
      echo "$t $(sqlite3 "$db" "SELECT count(*) FROM $t" 2> /dev/null || echo absent)"
    done
  } | sort
}

build_ref() {
  local ref=$1 dir=$2
  note "checking out $ref"
  # A previous run's registration of the same path must not block this
  # one: -f overrides a missing-but-registered worktree.
  rm -rf "$dir/src"
  git -C "$ROOT" worktree add -f --detach "$dir/src" "$ref" > /dev/null ||
    die "step 'checkout $ref' failed"
  note "building $ref"
  (cd "$dir/src" && CARGO_TARGET_DIR="$dir/target" cargo build --release --locked --quiet \
    -p open-scion-server -p open-scion) || die "step 'build $ref' failed"
}

make_store() {
  local ref=$1 dir=$2
  make_repo "$dir/fixture"
  "$dir/bin/scion-server" --data "$dir/store" import "$dir/fixture/src" \
    >"$dir/import.out" 2>"$dir/import.log" || {
    tail -n 5 "$dir/import.log" >&2
    die "the import of the $ref store failed"
  }
  "$dir/bin/scion-server" --data "$dir/store" ticket-key --out "$dir/ticket.key" \
    >"$dir/ticket.pub" || die "the ticket key of the $ref store failed"
}
