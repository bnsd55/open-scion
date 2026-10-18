#!/usr/bin/env bash
# `scn mount` driven by a real kernel NFS client (issue #238), the way the
# README tells users: a server, a land, `scn mount` under sudo, then a read
# with its bytes checked, a write, a rename and a delete through the mount,
# `scn commit --from-mount`, the diff, the refusal of a tampered mount
# path, and the unmount. Every external command runs under `timeout`, so a
# hang fails this script, and a trap unmounts and kills the processes on
# any exit, so a failed step does not leave the runner hung.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
case ${CARGO_TARGET_DIR:-} in
  "") BIN=$ROOT/target/debug ;;
  /*) BIN=$CARGO_TARGET_DIR/debug ;;
  *) BIN=$ROOT/$CARGO_TARGET_DIR/debug ;;
esac
SERVER_PORT=${SERVER_PORT:-17900}
SERVER=http://127.0.0.1:$SERVER_PORT

die() {
  echo "$(basename "$0"): $*" >&2
  exit 1
}

note() { echo "-- $*" >&2; }

WORK=$(mktemp -d "${TMPDIR:-/tmp}/scion-mount.XXXXXX")
WORK=$(cd "$WORK" && pwd -P)
VIEW=$WORK/view
HELLO='hello from the mount test'
cleanup() {
  [ -n "${WORK:-}" ] || return 0
  # A mount left behind hangs the runner: force-unmount it, and kill the
  # mount and the server by their saved PIDs.
  sudo umount -f -l "$WORK/view" 2> /dev/null || true
  [ -f "$WORK/mount.pid" ] && sudo kill -9 "$(cat "$WORK/mount.pid")" 2> /dev/null || true
  [ -f "$WORK/server.pid" ] && kill "$(cat "$WORK/server.pid")" 2> /dev/null || true
  sudo rm -rf "$WORK"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
SCION_TOKEN=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
export SCION_TOKEN

for c in sudo mount umount modprobe; do
  command -v "$c" > /dev/null || die "needs $c"
done

note "building the debug binaries"
timeout 300 cargo build --locked --quiet -p scion-server -p open-scion

# The kernel NFS client: the module, and mount.nfs(8).
sudo modprobe nfs
command -v mount.nfs > /dev/null || sudo apt-get install -y -qq nfs-common

note "starting the server"
"$BIN/scion-server" ticket-key --out "$WORK/ticket.key" > "$WORK/ticket.pub"
"$BIN/scion-server" --data "$WORK/server" serve \
  --listen "127.0.0.1:$SERVER_PORT" \
  --ticket-key "$WORK/ticket.key" \
  > "$WORK/server.log" 2>&1 &
echo $! > "$WORK/server.pid"
timeout 30 bash -c 'until grep -q serving '"$WORK"'/server.log 2> /dev/null; do sleep 0.2; done' \
  || die "the server did not start"

# A known changeset: one file of known bytes, landed on main.
printf '%s\n' "$HELLO" > "$WORK/hello.txt"
timeout 60 "$BIN/scn" --server "$SERVER" commit \
  --put README.md="$WORK/hello.txt" -m "seed the repo" \
  > "$WORK/commit.out" 2> "$WORK/commit.log" || {
  tail -n 20 "$WORK/commit.log" >&2
  die "the first commit failed"
}
CS=$(grep -Eo '[0-9a-f]{64}' "$WORK/commit.out" | tail -n 1)
[ -n "$CS" ] || die "no changeset id from the first commit"
timeout 60 "$BIN/scn" --server "$SERVER" land "$CS" > "$WORK/land.log" 2>&1 \
  || die "the land failed"

# The mount, the way the README tells users: `scn mount DIR main` under
# sudo (the kernel client needs root), in its own process. scn mounts it
# itself; the script polls until the mount answers.
note "mounting main on $VIEW"
sudo env SCION_TOKEN="$SCION_TOKEN" \
  "$BIN/scn" --server "$SERVER" mount "$VIEW" main \
  > "$WORK/mount.log" 2>&1 &
echo $! > "$WORK/mount.pid"
timeout 60 bash -c 'until [ -f "'"$VIEW"'/README.md" ]; do sleep 0.2; done' \
  || { tail -n 20 "$WORK/mount.log" >&2; die "the mount did not come up"; }

# (a) A read through the mount, its bytes against the landed blob.
got=$(timeout 30 cat "$VIEW/README.md")
[ "$got" = "$HELLO" ] || die "the mount served '$got', not '$HELLO'"

# (b) A write, a rename and a delete through the mount.
timeout 30 bash -c 'echo "written through the mount" > "'"$VIEW"'/notes.txt"'
timeout 30 mv "$VIEW/notes.txt" "$VIEW/renamed.txt"
timeout 30 rm "$VIEW/README.md"

# (c) The commit from the mount: the session file lives under root's home,
# so scn runs under sudo again.
timeout 60 sudo env SCION_TOKEN="$SCION_TOKEN" \
  "$BIN/scn" --server "$SERVER" commit -m "from the mount" \
  --from-mount "$VIEW" \
  > "$WORK/from-mount.out" 2> "$WORK/from-mount.log" || {
  tail -n 20 "$WORK/from-mount.log" >&2
  die "the commit from the mount failed"
}
CS2=$(grep -Eo '[0-9a-f]{64}' "$WORK/from-mount.out" | tail -n 1)
[ -n "$CS2" ] || die "no changeset id from the commit from the mount"

# (d) The diff says exactly what the mount changed.
diff_lines=$(timeout 60 "$BIN/scn" --server "$SERVER" diff main "$CS2")
printf '%s\n' "$diff_lines" | grep -qx 'A[[:space:]]renamed.txt' \
  || die "the diff lacks 'A renamed.txt': $diff_lines"
printf '%s\n' "$diff_lines" | grep -qx 'D[[:space:]]README.md' \
  || die "the diff lacks 'D README.md': $diff_lines"
[ "$(printf '%s\n' "$diff_lines" | grep -c .)" -eq 2 ] \
  || die "the diff has more than the two changes: $diff_lines"

# (e) The failure case the docs promise (architecture.md): a mount of a
# wrong path is refused. Tamper one byte of the secret in the target that
# the mount table shows, and the kernel client must get nothing; the
# server lives on.
line=$(mount | grep " on $VIEW ")
target=$(echo "$line" | awk '{print $1}')
port=$(echo "$line" | grep -o 'port=[0-9]*' | head -n 1 | cut -d= -f2)
[ -n "$port" ] || die "no port in the mount table line: $line"
note "mount table: $target port $port"
wrong_target="${target%?}x"
mkdir -p "$WORK/wrong"
if timeout 30 sudo mount -t nfs -o "vers=3,tcp,port=$port,mountport=$port,nolock,soft" \
  "$wrong_target" "$WORK/wrong" 2> "$WORK/wrong.log"; then
  sudo umount "$WORK/wrong" 2> /dev/null || true
  die "the tampered mount path $wrong_target was accepted"
fi
grep -q MNT3ERR_NOENT "$WORK/wrong.log" \
  || note "the tampered mount failed without MNT3ERR_NOENT: $(cat "$WORK/wrong.log")"
timeout 30 stat "$VIEW/renamed.txt" > /dev/null || die "the mount died"

# (f) The unmount, and a clean table afterwards.
sudo env SCION_TOKEN="$SCION_TOKEN" \
  "$BIN/scn" --server "$SERVER" unmount "$VIEW" > "$WORK/unmount.log" 2>&1 \
  || die "scn unmount failed"
grep -q "unmounted $VIEW" "$WORK/unmount.log" || die "no unmounted line"
if mount | grep -q " on $VIEW "; then
  die "$VIEW is still mounted"
fi

note "mount-nfs: all steps passed"
