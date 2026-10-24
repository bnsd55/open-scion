#!/usr/bin/env bash
# The helpers the drill legs share with backup-n1.sh: the preamble and
# the function bodies the legs call. The run section below is this
# script's own.
. "$(cd "$(dirname "$0")" && pwd)/drill-lib.sh"

# The tagged-store upgrade job (issue #382): a store written by each tagged
# release opens under this build and upgrades directly, and the direct
# upgrade is the same store that upgrading stepwise through the tags gives.
#
#   - direct: the store of tag T opens under HEAD; the open migrates it to
#     HEAD's schema, then ls, cat and a commit work, and the schema version
#     is HEAD's.
#   - refusal: HEAD's store on T's binary is refused with the version
#     reason -- or, when the two builds write the same schema version, the
#     script says the refusal cannot fire instead of failing.
#   - direct equals stepwise: the store of T upgraded directly by HEAD is
#     the same store (schema, table set, row counts) as the store of T
#     upgraded by T+1, then T+2, ..., then HEAD. Stepwise is what
#     production does across releases; direct is what an operator who
#     skips versions runs; if they ever disagree, a migration is
#     order-dependent, and that is a bug.
#
# The stores are built in CI from the tags, never checked in: a fixture
# store rots with every tag, and the ancestor-tag resolution of
# version-matrix.sh is already proven. Each built store is first read back
# under its own tag's binary, so a broken fixture is told apart from a
# broken migration.
#
# Tags: every release tag that is an ancestor of HEAD and not HEAD itself,
# oldest first, capped to the newest 8 (the summary says when the cap
# bites; 0.x cadence makes that a later worry). Until the first release
# tag exists, the ladder is the pinned stand-in commits below (marked
# SHORTCUT there): the prove legs run on every trigger, never a skip --
# a job that skips every night tests nothing.
#
#   STORE_UPGRADE_TAGS  tags or refs to use instead of tag resolution,
#                       oldest first (the workflow's tags input goes here;
#                       any commit works, so the job is provable before
#                       the first tag exists).
#   WORK                where the checkouts, builds and stores live. In
#                       CI a run-owned directory; never the work tree.
#                       The git history must carry the tags (a checkout
#                       with fetch-depth: 0 and fetch-tags: true does).



# summary LINE: what a CI run reads. Into the step summary when there is
# one; to stderr always, so a local run sees it too.

# release_tags: every release tag that is an ancestor of HEAD and not HEAD
# itself, oldest first. The ladder caps itself to the newest 8 (each tag is
# a build); tags outside the cap are named in a ::warning:: so "every tag"
# never silently shrinks. Until the first tag:
# SHORTCUT: two pinned commits whose stores differ in schema version, so
# every leg of the job has something real to prove; the first release tag
# replaces them (the resolution never sees them again, because a release
# tag is an ancestor of HEAD and the pins are on main).
STANDIN_TAGS=(
  afe1c4db878f38fa22bd6adddd57a8c410668db5 # read rules per folder: schema 8
  32a6117bf54510450b1bfea10b6c873dcb383207 # view memo: schema 9
)

release_tags() {
  local head tag
  head=$(git -C "$ROOT" rev-parse HEAD)
  git -C "$ROOT" tag --list 'v[0-9]*' --sort=version:refname --merged=HEAD |
    while read -r tag; do
      [ "$(git -C "$ROOT" rev-parse "$tag^{commit}")" != "$head" ] && printf '%s\n' "$tag"
    done
}

# cap_tags: the newest 8 of the release tags, oldest first; tags outside
# the cap are warned about by name, never dropped silently.
CAP=8
cap_tags() {
  local all total skipped
  all=$(release_tags)
  [ -z "$all" ] && return 0
  total=$(printf '%s\n' "$all" | grep -c . || true)
  [ "$total" -le "$CAP" ] && { printf '%s\n' "$all"; return 0; }
  skipped=$(printf '%s\n' "$all" | awk -v n="$total" -v cap="$CAP" 'NR <= n - cap')
  summary "::warning::the ladder keeps the newest $CAP tags; these older tags are not built: $(printf '%s' "$skipped" | tr '\n' ' ')"
  printf '%s\n' "$all" | awk -v n="$total" -v cap="$CAP" 'NR > n - cap'
}

# --resolve-tags: print the tag list and exit, so a workflow can key its
# caches before any build runs. An empty ladder is a pinned-stand-ins
# ladder, never a skip: a run that proves nothing must not pass.
if [ "${1:-}" = "--resolve-tags" ]; then
  tags=$(cap_tags)
  if [ -z "$tags" ]; then
    summary "no release tag yet: the ladder is the pinned stand-in commits"
    printf '%s\n' "${STANDIN_TAGS[@]}"
    exit 0
  fi
  printf '%s\n' "$tags"
  exit 0
fi

command -v cargo > /dev/null || die "needs cargo"
command -v git > /dev/null || die "needs git"
command -v curl > /dev/null || die "needs curl"
command -v sqlite3 > /dev/null || die "needs sqlite3 (the store fingerprint reads the index directly)"

WORK=${WORK:?WORK must name the run directory}
mkdir -p "$WORK"

# The checkouts are worktrees of the real repository: their git commands
# must never write its config. The guard fails the run if the identity
# changed (the section the old make_repo wrote); other agents may add
# branch tracking to the same config while this run goes, and that is
# not this run's doing.
# A runner's config may hold no identity at all: an empty guard file is
# fine, and grep's "nothing matched" must not kill the script (set -e
# with pipefail).
git -C "$ROOT" config --local --list | grep '^user\.' > "$WORK/git-config.before" || true

# The tags of this run: the caller's override, or the resolved ladder.
if [ -n "${STORE_UPGRADE_TAGS:-}" ]; then
  tags=$(printf '%s' "${STORE_UPGRADE_TAGS}" | tr ' ' '\n')
else
  tags=$(cap_tags)
  if [ -z "$tags" ]; then
    summary "no release tag yet: the ladder is the pinned stand-in commits"
    tags=$(printf '%s\n' "${STANDIN_TAGS[@]}")
  fi
fi
count=$(printf '%s\n' "$tags" | grep -c . || true)
note "the ladder holds $count tag(s): $(printf '%s' "$tags" | tr '\n' ' ')"

# schema_of SRCDIR: the store schema version a checkout writes, from its
# source. The refusal can only fire when two builds differ.

# make_repo DIR: the small repository the matrix uses: three commits on
# main, so ls, cat and blame all have something to say. The caller gives
# a directory of its own: never a checkout or a worktree path, whose
# .git/config is the repository's. The identity rides the commit
# environment, so no git config of any checkout is ever written.

# start NAME DIR BIN ready args...: run a server in the background and
# wait for its `serving listen=` line; with `ready` = port, print it.

# stop DIR NAME: stop the one process `start` started, polling for ten
# seconds, then KILL, and say which process needed it.

# fingerprint STORE: the state a migrated store must keep, whatever path
# the migrations took to it: the schema (every table and index as the
# schema says it), the version, and the row counts of the tables that
# hold truth. The cache-like tables are left out: their sweep runs on
# the server's clock, not on this script's.

# build_ref REF DIR: check REF out (a tag or any commit) and build its
# server and client. The build lives in DIR/target, so one ref's build
# never sees another's.

# make_store REF DIR: a fresh store of REF's version, with the repo of
# make_repo in it and its ticket key made.

# readback REF DIR: the built store reads back under its own tag's pair
# (O2): fsck opens the whole store, and a served ls and cat answer with
# the bytes the import took in. A failure here is the fixture, not a
# migration.
readback() {
  local ref=$1 dir=$2 port out
  "$dir/bin/scion-server" --data "$dir/store" fsck >"$dir/fsck.log" 2>&1 ||
    die "the read-back of the $ref store failed: its own binary cannot open it"
  port=$(start server "$dir" "$dir/bin/scion-server" port --data "$dir/store" serve \
    --listen 127.0.0.1:0 --token upgrade --ticket-key "$dir/ticket.key") ||
    die "the read-back serve of the $ref store failed"
  out=$("$dir/bin/scn" --server "http://127.0.0.1:$port" --token upgrade ls main: 2>"$dir/ls.err") ||
    die "the read-back ls of the $ref store failed: $(cat "$dir/ls.err")"
  printf '%s\n' "$out" | grep -q 'README.md' || die "the read-back ls of the $ref store lost README.md"
  out=$("$dir/bin/scn" --server "http://127.0.0.1:$port" --token upgrade cat main:README.md 2>"$dir/cat.err") ||
    die "the read-back cat of the $ref store failed: $(cat "$dir/cat.err")"
  printf '%s\n' "$out" | grep -q 'A small repository' || die "the read-back cat of the $ref store returned other bytes"
  stop_one "$dir" server
}

# direct REF DIR: HEAD opens the store of REF; the migration runs forward,
# then the served story answers, a commit works, and the schema version is
# HEAD's. Prints nothing; fails loud.
direct() {
  local ref=$1 dir=$2 port out schema
  note "direct: HEAD opens the store of $ref"
  "$BIN/scion-server" --data "$dir/store" fsck >"$dir/fsck-head.log" 2>&1 || {
    tail -n 20 "$dir/fsck-head.log" >&2
    die "direct ($ref): HEAD cannot open the store"
  }
  schema=$(sqlite3 "$dir/store/scion.db" 'PRAGMA user_version')
  [ "$schema" = "$HEAD_SCHEMA" ] ||
    die "direct ($ref): after the open the schema is $schema, not HEAD's $HEAD_SCHEMA"
  port=$(start server "$dir" "$BIN/scion-server" port --data "$dir/store" serve \
    --listen 127.0.0.1:0 --token upgrade --ticket-key "$dir/ticket.key") ||
    die "direct ($ref): the serve after the migration failed"
  out=$("$BIN/scn" --server "http://127.0.0.1:$port" --token upgrade ls main: 2>"$dir/ls.err") ||
    die "direct ($ref): ls after the migration failed: $(cat "$dir/ls.err")"
  printf '%s\n' "$out" | grep -q 'README.md' || die "direct ($ref): ls after the migration lost README.md"
  out=$("$BIN/scn" --server "http://127.0.0.1:$port" --token upgrade cat main:README.md 2>"$dir/cat.err") ||
    die "direct ($ref): cat after the migration failed: $(cat "$dir/cat.err")"
  printf '%s\n' "$out" | grep -q 'A small repository' || die "direct ($ref): cat after the migration returned other bytes"
  stop_one "$dir" server
}

# write REF DIR: a commit and land on the migrated store, then the
# read-back: the upgraded store takes writes.
write_leg() {
  local ref=$1 dir=$2 port out cs
  port=$(start server "$dir" "$BIN/scion-server" port --data "$dir/store" serve \
    --listen 127.0.0.1:0 --token upgrade --ticket-key "$dir/ticket.key") ||
    die "write ($ref): the serve failed"
  printf 'written after the upgrade\n' >"$dir/after.txt"
  out=$("$BIN/scn" --server "http://127.0.0.1:$port" --token upgrade \
    commit -m "after the upgrade" --put after.txt="$dir/after.txt" 2>"$dir/commit.err") ||
    die "direct ($ref): a commit on the migrated store failed: $(cat "$dir/commit.err")"
  cs=$(printf '%s' "$out" | tail -1)
  [ -n "$cs" ] || die "direct ($ref): scn commit printed no changeset"
  "$BIN/scn" --server "http://127.0.0.1:$port" --token upgrade land "$cs" \
    >"$dir/land.out" 2>"$dir/land.err" ||
    die "direct ($ref): the land of the commit failed: $(cat "$dir/land.err")"
  out=$("$BIN/scn" --server "http://127.0.0.1:$port" --token upgrade cat main:after.txt 2>"$dir/after.err") ||
    die "direct ($ref): the read-back of the commit failed: $(cat "$dir/after.err")"
  printf '%s\n' "$out" | grep -q 'written after the upgrade' ||
    die "direct ($ref): the committed bytes did not come back"
  stop_one "$dir" server
}

# refusal REF DIR: HEAD's store on REF's binary is refused with the
# version reason -- or the note that both builds write one schema version,
# so the refusal cannot fire on this pair.
refusal() {
  local ref=$1 dir=$2 ref_schema fresh
  ref_schema=$(schema_of "$dir/src")
  if [ -z "$ref_schema" ] || [ -z "$HEAD_SCHEMA" ]; then
    die "cannot read SCHEMA_VERSION from one of the two builds ($ref)"
  fi
  if [ "$ref_schema" = "$HEAD_SCHEMA" ]; then
    summary "refusal $ref: both builds write schema $ref_schema, so the refusal cannot fire on this pair"
    return 0
  fi
  note "refusal: the store of HEAD on $ref's binary"
  fresh="$dir/store-of-head"
  # HEAD's binary refuses a missing data directory (#578), and it is
  # the first to touch this one: the import creates it.
  mkdir -p "$fresh"
  "$BIN/scion-server" --data "$fresh" import "$WORK/head/fixture/src" \
    >"$dir/import-head.log" 2>&1 || die "refusal ($ref): the import of the HEAD store failed"
  if "$dir/bin/scion-server" --data "$fresh" fsck >"$dir/refuse.log" 2>&1; then
    die "refusal ($ref): the $ref binary accepted a store of HEAD"
  fi
  grep -q 'newer than this build' "$dir/refuse.log" ||
    die "refusal ($ref): the store of HEAD was refused, but not with the version reason"
}

# stepwise REF DIR STORE: the store of REF migrates through every tag
# between it and HEAD, one open per tag with that tag's own binary, then
# HEAD. Prints the fingerprint of the result on stdout.
stepwise() {
  local ref=$1 dir=$2 store=$3 pass=0 tag tag_dir
  cp -R "$store-pristine" "$dir/chain"
  for tag in $tags; do
    if [ "$tag" = "$ref" ]; then
      pass=1
      continue
    fi
    [ "$pass" = 1 ] || continue
    tag_dir="$WORK/$(printf '%s' "$tag" | tr -c 'A-Za-z0-9._-' '_')"
    "$tag_dir/bin/scion-server" --data "$dir/chain" fsck >"$dir/step.log" 2>&1 ||
      die "stepwise ($ref): the open at $tag failed"
  done
  "$BIN/scion-server" --data "$dir/chain" fsck >"$dir/step-head.log" 2>&1 ||
    die "stepwise ($ref): HEAD cannot open the chain"
  fingerprint "$dir/chain/scion.db"
}

# cleanup: the checkouts live in WORK, which the caller owns; their
# registrations must not outlive the run, whatever the exit. Then the
# guard has the last word: a changed local config fails the run.
cleanup() {
  local status=$?
  local ref dir log pid
  # The servers `start` backgrounded die on every exit path, TERM then
  # KILL: a die between start and stop_one must not orphan one.
  for log in $(find "$WORK" -name '*.log.pid' 2>/dev/null); do
    pid=$(cat "$log" 2>/dev/null) || continue
    kill "$pid" 2>/dev/null || true
  done
  sleep 2
  for log in $(find "$WORK" -name '*.log.pid' 2>/dev/null); do
    pid=$(cat "$log" 2>/dev/null) || continue
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$log"
  done
  for ref in $tags; do
    dir="$WORK/$(printf '%s' "$ref" | tr -c 'A-Za-z0-9._-' '_')"
    git -C "$ROOT" worktree remove --force "$dir/src" > /dev/null 2>&1 || true
  done
  git -C "$ROOT" worktree prune > /dev/null 2>&1 || true
  git -C "$ROOT" config --local --list | grep '^user\.' > "$WORK/git-config.after" || true
  if ! diff -u "$WORK/git-config.before" "$WORK/git-config.after" > /dev/null; then
    diff -u "$WORK/git-config.before" "$WORK/git-config.after" >&2 || true
    die "the run changed the repository's local git identity"
  fi
  exit $status
}
trap cleanup EXIT

# ---- the run ----

BIN=$WORK/head/bin
note "building HEAD"
(cd "$ROOT" && CARGO_TARGET_DIR="$WORK/head-target" cargo build --release --locked --quiet \
  -p open-scion-server -p open-scion) || die "step 'build HEAD' failed"
mkdir -p "$WORK/head/bin"
ln -f "$WORK/head-target/release/scion-server" "$WORK/head/bin/scion-server"
ln -f "$WORK/head-target/release/scn" "$WORK/head/bin/scn"
ln -sf "$ROOT" "$WORK/head/src"
HEAD_SCHEMA=$(schema_of "$ROOT")
[ -n "$HEAD_SCHEMA" ] || die "cannot read SCHEMA_VERSION from HEAD"
# The fixture repo of the HEAD store, once: the refusal legs import it.
mkdir -p "$WORK/head"
make_repo "$WORK/head/fixture"

# The stores of every tag, oldest first, each read back under its own
# binary (O2) before anything migrates it.
for ref in $tags; do
  dir="$WORK/$(printf '%s' "$ref" | tr -c 'A-Za-z0-9._-' '_')"
  mkdir -p "$dir/bin"
  build_ref "$ref" "$dir"
  ln -f "$dir/target/release/scion-server" "$dir/bin/scion-server"
  ln -f "$dir/target/release/scn" "$dir/bin/scn"
  make_store "$ref" "$dir"
  readback "$ref" "$dir"
  cp -R "$dir/store" "$dir/store-pristine"
  summary "store of $ref built and read back (schema $(schema_of "$dir/src"))"
done

# The direct legs, the refusal legs, and the direct fingerprints.
for ref in $tags; do
  dir="$WORK/$(printf '%s' "$ref" | tr -c 'A-Za-z0-9._-' '_')"
  direct "$ref" "$dir"
  refusal "$ref" "$dir"
  fingerprint "$dir/store/scion.db" > "$dir/fingerprint-direct.txt"
  summary "direct upgrade of $ref: schema $HEAD_SCHEMA, served, written"
done

# The stepwise legs: only a ladder of two or more has an intermediate.
if [ "$count" -ge 2 ]; then
  for ref in $tags; do
    dir="$WORK/$(printf '%s' "$ref" | tr -c 'A-Za-z0-9._-' '_')"
    stepwise "$ref" "$dir" "$dir/store" > "$dir/fingerprint-stepwise.txt"
    if ! diff -u "$dir/fingerprint-direct.txt" "$dir/fingerprint-stepwise.txt" >"$dir/step.diff"; then
      cat "$dir/step.diff" >&2
      die "the direct upgrade of $ref differs from the stepwise one: a migration is order-dependent"
    fi
    summary "stepwise upgrade of $ref: identical to direct"
  done
else
  summary "one tag: the direct-vs-stepwise comparison needs two; it starts with the second tag"
fi

summary "tagged-store upgrade: $count tag(s), all direct and stepwise legs green"
