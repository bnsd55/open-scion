#!/usr/bin/env bash
# `scn mount` driven by a real kernel NFS client (issue #238), the way the
# README tells users: `scn mount` runs as the user, and only the mount
# command it prints runs under sudo. The script starts a server, lands a
# known changeset, mounts it, then reads with the bytes checked, writes, a
# rename and a delete through the mount, commits from the mount, checks the
# diff, refuses a tampered mount path, and unmounts. Every external command
# runs under `timeout`, so a hang fails this script, and a trap unmounts
# and kills the processes on any exit, so a failed step does not leave the
# runner hung.
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
MOUNT_PID=
cleanup() {
  [ -n "${WORK:-}" ] || return 0
  # A mount left behind hangs the runner: force-unmount it, and kill the
  # mount and the server by their saved PIDs.
  sudo umount -f -l "$VIEW" 2> /dev/null || true
  [ -f "$WORK/mount.pid" ] && kill -9 "$(cat "$WORK/mount.pid")" 2> /dev/null || true
  [ -f "$WORK/server.pid" ] && kill "$(cat "$WORK/server.pid")" 2> /dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export SCION_SERVER=$SERVER
SCION_TOKEN=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
export SCION_TOKEN

for c in sudo mount umount modprobe; do
  command -v "$c" > /dev/null || die "needs $c"
done

note "building the debug binaries"
timeout 300 cargo build --locked --quiet -p open-scion-server -p open-scion

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

# A known changeset with known bytes on main: a one-file git repository,
# imported (import creates the repo, a commit cannot, so this goes before
# any scn call).
git init -q --initial-branch=main "$WORK/src"
printf 'hello from the mount test\n' > "$WORK/src/README.md"
git -C "$WORK/src" add README.md
git -C "$WORK/src" -c user.name=mount-nfs -c user.email=mount-nfs@localhost \
  commit -qm "seed the repo"
timeout 120 "$BIN/scion-server" --data "$WORK/server" import \
  --repo main --rev main "$WORK/src" \
  > "$WORK/import.out" 2> "$WORK/import.log" || {
  tail -n 20 "$WORK/import.log" >&2
  die "the import failed"
}

# The mount, as the README says: `scn mount DIR main` as the user. Its own
# mount attempt fails (only root may mount), so it prints the sudo command
# and keeps serving; the script runs that printed command under sudo, and
# polls until the mount answers.
note "mounting main on $VIEW"
"$BIN/scn" --server "$SERVER" mount "$VIEW" main \
  > "$WORK/mount.log" 2>&1 &
MOUNT_PID=$!
echo "$MOUNT_PID" > "$WORK/mount.pid"
timeout 30 bash -c 'until grep -q "^sudo mount -t nfs" '"$WORK"'/mount.log 2> /dev/null; do
  kill -0 '"$MOUNT_PID"' 2> /dev/null || { cat '"$WORK"'/mount.log >&2; die "scn mount died"; }
  sleep 0.2
done' || die "scn mount printed no mount command"
MOUNT_CMD=$(grep '^sudo mount -t nfs' "$WORK/mount.log" | tail -n 1)
# The split of $MOUNT_CMD is the point: it is the whole command line that
# scn mount printed.
# shellcheck disable=SC2086
timeout 60 sudo $MOUNT_CMD || die "the printed mount command failed: $MOUNT_CMD"
timeout 60 bash -c 'until [ -f "'"$VIEW"'/README.md" ]; do sleep 0.2; done' \
  || { cat "$WORK/mount.log" >&2; die "the mount did not come up"; }

# (a) A read through the mount, its bytes against the landed blob.
got=$(timeout 30 cat "$VIEW/README.md")
[ "$got" = "hello from the mount test" ] || die "the mount served '$got'"

# (b) A write, a rename and a delete through the mount.
timeout 30 bash -c 'echo "written through the mount" > "'"$VIEW"'/notes.txt"'
timeout 30 mv "$VIEW/notes.txt" "$VIEW/renamed.txt"
timeout 30 rm "$VIEW/README.md"

# (c) The commit from the mount. The session file and the overlay belong to
# the user, so scn runs as the user, no sudo.
timeout 60 "$BIN/scn" --server "$SERVER" commit -m "from the mount" \
  --from-mount "$VIEW" \
  > "$WORK/from-mount.out" 2> "$WORK/from-mount.log" || {
  tail -n 20 "$WORK/from-mount.log" >&2
  die "the commit from the mount failed"
}
CS2=$(grep -Eo '[0-9a-f]{64}' "$WORK/from-mount.out" | tail -n 1)
[ -n "$CS2" ] || die "no changeset id from the commit from the mount"

# (d) The diff of main against the new changeset: exactly the rename and
# the delete, two lines.
diff_out=$(timeout 60 "$BIN/scn" --server "$SERVER" diff main "$CS2")
echo "$diff_out" | grep -q 'A' || true
echo "$diff_out" | grep -Eq '^A[^[:alnum:]]*renamed\.txt$' \
  || die "the diff lacks 'A renamed.txt': $diff_out"
echo "$diff_out" | grep -Eq '^D[^[:alnum:]]*README\.md$' \
  || die "the diff lacks 'D README.md': $diff_out"
[ "$(echo "$diff_out" | grep -c .)" -eq 2 ] || die "more than two lines: $diff_out"

# (e) The failure case the docs promise (architecture.md): a mount of a
# wrong path is refused. Tamper one byte of the secret in the target that
# the mount table shows, keep the port from the same line; the kernel
# client must get nothing, and the real mount must live on.
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
timeout 30 stat "$VIEW/renamed.txt" > /dev/null || die "the mount died"

# (f) The unmount: scn unmount as the user drops the session record, then
# the real umount under sudo, and the mount table is clean afterwards.
timeout 60 "$BIN/scn" --server "$SERVER" unmount "$VIEW" \
  > "$WORK/unmount.log" 2>&1 || die "scn unmount failed"
sudo umount -f -l "$VIEW" 2> /dev/null || true
if mount | grep -q " on $VIEW "; then
  die "$VIEW is still mounted"
fi

note "mount-nfs: all steps passed"
