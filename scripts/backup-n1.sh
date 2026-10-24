#!/usr/bin/env bash
# The N-1 backup, restore and rollback drill (issue #379, item 3): the
# backup an operator takes before an upgrade restores and serves under
# the binary that took it, the upgrade path stays open, and the store
# refuses to go backwards.
#
#   BACKUP: the N-1 pair (built by scripts/store-upgrade.sh's builder —
#   one checkout, one release build, no second builder) imports a repo,
#   serves it, and takes two acknowledged writes through the server.
#   The backup is the runbook order: `VACUUM INTO` of `scion.db` (one
#   committed metadata state, taken while the server runs), then the
#   `objects/` copy (blob-first, so the copy only adds).
#   UPGRADE: HEAD's binary opens the N-1 store — the migration runs
#   forward — and the acknowledged writes read back through HEAD.
#   ROLLBACK: the pre-upgrade backup restores into a fresh directory
#   and serves under the N-1 binary: fsck opens it, the acknowledged
#   writes read back, and a fresh commit and land through N-1 work.
#   REFUSAL: HEAD's migrated store, offered to the N-1 binary, is
#   refused with the version reason — or, when both builds write one
#   schema version, the drill says the refusal cannot fire on this
#   pair instead of failing.
#
# The N-1 ref arrives in STORE_UPGRADE_TAGS (one ref), the same env the
# store-upgrade workflow's dispatch input feeds. Before v0.1.0 it is a
# pinned main SHA; after the tag it is the tag — the input's default
# flips, this script never does.

HERE=$(cd "$(dirname "$0")" && pwd)

# The helpers the drill shares with store-upgrade.sh: one source, so a
# helper written in another style can never be skipped in silence.
. "$HERE/drill-lib.sh"

WORK=${WORK:-$ROOT/.scn-drills/backup-n1}
N1_REF=${STORE_UPGRADE_TAGS:?STORE_UPGRADE_TAGS must name the N-1 ref \
(a pinned main SHA before v0.1.0, the release tag after)}
# Only a run directory this script MADE goes at exit: a caller's WORK -
# CI's $RUNNER_TEMP, an operator's drills directory - is theirs.
DRILL_WORK_MADE=
if [ ! -d "$WORK" ]; then
    mkdir -p "$WORK"
    DRILL_WORK_MADE=1
fi

# kill_drill_servers: every server `start` backgrounded, via its
# $dir/$name.log.pid. TERM first, a pause, then KILL: a server that
# ignores TERM still dies, and the drill never orphans one on any exit
# path. stop_one already handled its file on the happy path, so a pid
# file left here means a die.
kill_drill_servers() {
  local log pid killed=0
  for log in $(find "$WORK" -name '*.log.pid' 2>/dev/null); do
    pid=$(cat "$log" 2>/dev/null) || continue
    kill "$pid" 2>/dev/null || true
    killed=1
  done
  [ "$killed" = 1 ] || return 0
  sleep 2
  for log in $(find "$WORK" -name '*.log.pid' 2>/dev/null); do
    pid=$(cat "$log" 2>/dev/null) || continue
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$log"
  done
}

# The drill's own exit guard: the servers it started die on every path,
# and a run directory this script made does not outlive it. One a caller
# named (CI's $RUNNER_TEMP) is left for the caller.
cleanup() {
  kill_drill_servers
  [ -n "${DRILL_WORK_MADE:-}" ] && rm -rf "$WORK"
  return 0
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# The N-1 ref must be a commit of this repository's history before any
# build: a bad ref dies here, in a second, not after the release build.
git -C "$ROOT" rev-parse --verify --quiet "$N1_REF^{commit}" ||
    die "the N-1 ref $N1_REF is not a commit here"

# HEAD's pair, built the way the ladder's run builds it: this
# repository's source, one target directory.
note "building HEAD"
(cd "$ROOT" && CARGO_TARGET_DIR="$WORK/head-target" cargo build --release --locked --quiet \
  -p open-scion-server -p open-scion) || die "step 'build HEAD' failed"
mkdir -p "$WORK/head/bin"
ln -f "$WORK/head-target/release/scion-server" "$WORK/head/bin/scion-server"
ln -f "$WORK/head-target/release/scn" "$WORK/head/bin/scn"
HEAD_SCHEMA=$(schema_of "$ROOT")
[ -n "$HEAD_SCHEMA" ] || die "cannot read SCHEMA_VERSION from HEAD"

# The N-1 pair: one checkout, one release build, the builder's own
# target directory so one ref's build never sees another's.
mkdir -p "$WORK/n1/bin"
build_ref "$N1_REF" "$WORK/n1"
ln -f "$WORK/n1/target/release/scion-server" "$WORK/n1/bin/scion-server"
ln -f "$WORK/n1/target/release/scn" "$WORK/n1/bin/scn"
N1_SCHEMA=$(schema_of "$WORK/n1/src")
[ -n "$N1_SCHEMA" ] || die "cannot read SCHEMA_VERSION from the N-1 build"

# The N-1 store: the builder's fixture repo, imported under the N-1
# binary, with its ticket key.
make_store "$N1_REF" "$WORK/n1"

# Two acknowledged writes through the N-1 server: commit and land, the
# writes an operator's clients would have made before the upgrade.
port=$(start server "$WORK/n1" "$WORK/n1/bin/scion-server" port --data "$WORK/n1/store" serve \
  --listen 127.0.0.1:0 --token upgrade --ticket-key "$WORK/n1/ticket.key") ||
  die "the N-1 serve failed"
for name in before-one before-two; do
  printf 'written before the upgrade: %s\n' "$name" >"$WORK/n1/$name.txt"
  out=$("$WORK/n1/bin/scn" --server "http://127.0.0.1:$port" --token upgrade \
    commit -m "$name" --put "$name.txt=$WORK/n1/$name.txt" 2>"$WORK/n1/$name.err") ||
    die "the N-1 commit $name failed: $(cat "$WORK/n1/$name.err")"
  cs=$(printf '%s' "$out" | tail -1)
  [ -n "$cs" ] || die "the N-1 commit $name printed no changeset"
  "$WORK/n1/bin/scn" --server "http://127.0.0.1:$port" --token upgrade land "$cs" \
    >"$WORK/n1/$name.land" 2>"$WORK/n1/$name.landerr" ||
    die "the N-1 land of $name failed: $(cat "$WORK/n1/$name.landerr")"
done

# The backup, in the runbook's order, while the server runs: the
# metadata snapshot first (VACUUM INTO reads one committed state), then
# the objects copy (blob-first: the copy only adds, never removes).
mkdir -p "$WORK/backup"
sqlite3 "$WORK/n1/store/scion.db" "VACUUM INTO '$WORK/backup/scion.db'" ||
  die "the metadata snapshot failed"
mkdir -p "$WORK/backup/objects"
cp -R "$WORK/n1/store/objects/." "$WORK/backup/objects/" ||
  die "the objects copy failed"
stop_one "$WORK/n1" server
summary "backup: the N-1 snapshot and objects are in $WORK/backup"

# UPGRADE: HEAD's binary opens the N-1 store; the migration runs
# forward, and the acknowledged writes read back through HEAD.
"$WORK/head/bin/scion-server" --data "$WORK/n1/store" fsck >"$WORK/upgrade-fsck.log" 2>&1 ||
  die "the upgrade open failed: HEAD cannot open the N-1 store"
mkdir -p "$WORK/upgrade"
port=$(start server "$WORK/upgrade" "$WORK/head/bin/scion-server" port --data "$WORK/n1/store" serve \
  --listen 127.0.0.1:0 --token upgrade --ticket-key "$WORK/n1/ticket.key") ||
  die "the upgraded serve failed"
out=$("$WORK/head/bin/scn" --server "http://127.0.0.1:$port" --token upgrade \
  cat main:before-two.txt 2>"$WORK/upgrade/cat.err") ||
  die "the upgraded read of before-two failed: $(cat "$WORK/upgrade/cat.err")"
printf '%s' "$out" | grep -q 'written before the upgrade: before-two' ||
  die "the upgraded store lost the acknowledged write before-two"
stop_one "$WORK/upgrade" server
summary "upgrade: HEAD opened the N-1 store and the acknowledged writes read back"

# ROLLBACK: the pre-upgrade backup restores into a fresh directory and
# serves under the N-1 binary — the same old-schema bytes, so it opens.
mkdir -p "$WORK/rollback/store"
cp "$WORK/backup/scion.db" "$WORK/rollback/store/scion.db" ||
  die "the restore of the metadata snapshot failed"
cp -R "$WORK/backup/objects/." "$WORK/rollback/store/objects/" ||
  die "the restore of the objects failed"
"$WORK/n1/bin/scion-server" --data "$WORK/rollback/store" fsck >"$WORK/rollback-fsck.log" 2>&1 ||
  die "the restored store does not open under the N-1 binary"
port=$(start server "$WORK/rollback" "$WORK/n1/bin/scion-server" port --data "$WORK/rollback/store" serve \
  --listen 127.0.0.1:0 --token upgrade --ticket-key "$WORK/n1/ticket.key") ||
  die "the rolled-back serve failed"
for name in before-one before-two; do
  out=$("$WORK/n1/bin/scn" --server "http://127.0.0.1:$port" --token upgrade \
    cat "main:$name.txt" 2>"$WORK/rollback/$name.err") ||
    die "the rolled-back read of $name failed: $(cat "$WORK/rollback/$name.err")"
  printf '%s' "$out" | grep -q "written before the upgrade: $name" ||
    die "the rolled-back store lost the acknowledged write $name"
done
# The rolled-back store takes writes: the rollback is a working server,
# not a museum.
out=$("$WORK/n1/bin/scn" --server "http://127.0.0.1:$port" --token upgrade \
  commit -m "after the rollback" --put after.txt="$WORK/n1/before-one.txt" \
  2>"$WORK/rollback/after.err") ||
  die "the commit on the rolled-back store failed: $(cat "$WORK/rollback/after.err")"
cs=$(printf '%s' "$out" | tail -1)
[ -n "$cs" ] || die "the commit on the rolled-back store printed no changeset"
"$WORK/n1/bin/scn" --server "http://127.0.0.1:$port" --token upgrade land "$cs" \
  >"$WORK/rollback/after.land" 2>"$WORK/rollback/after.landerr" ||
  die "the land on the rolled-back store failed: $(cat "$WORK/rollback/after.landerr")"
stop_one "$WORK/rollback" server
summary "rollback: the pre-upgrade backup serves under N-1, the writes read back, a fresh commit lands"

# REFUSAL: HEAD's migrated store, offered to the N-1 binary, is refused
# with the version reason. When both builds write one schema version
# the refusal cannot fire, and the drill says so instead of failing.
if [ "$N1_SCHEMA" = "$HEAD_SCHEMA" ]; then
  summary "refusal: both builds write schema $N1_SCHEMA, so the refusal cannot fire on this pair"
else
  if "$WORK/n1/bin/scion-server" --data "$WORK/n1/store" fsck >"$WORK/refuse.log" 2>&1; then
    die "the refusal: the N-1 binary accepted HEAD's migrated store"
  fi
  grep -q "is newer than this build" "$WORK/refuse.log" ||
    die "the refusal named no version reason: $(cat "$WORK/refuse.log")"
  summary "refusal: the N-1 binary refused HEAD's migrated store with the version reason"
fi

summary "backup-n1: ok (N-1 $N1_REF, schema $N1_SCHEMA -> HEAD schema $HEAD_SCHEMA)"
