#!/bin/sh
# Conformance of a non-Rust client (issue #32): grpcurl speaks the same API
# as the Rust client does, against a running scion-server, and every object
# it gets hashes to its ID with b3sum, domain-separated as format-v1.md says
# (BlobId = BLAKE3("scion\0blob\0v1\0" || bytes), TreeId with the tree
# prefix).
#
# The script needs: cargo, git, jq, python3. It fetches a pinned grpcurl
# release (its sha256 is pinned here and checked before it runs) and a
# pinned b3sum from crates.io.
set -eu

# The pins. Bump a version together with its sha256 line.
GRPCURL_VERSION=1.9.4
B3SUM_VERSION=1.8.7
GRPCURL_LINUX_X86_64=97e13d58d2733a0e62cd2571d1d5f0c02823f0d25282f08bddedf1ad9c5d1736
GRPCURL_OSX_ARM64=e0df111350acf8ee38f453f4e97e0474cfe1a987ee0c368ebe5383e6a4bccf25
GRPCURL_OSX_X86_64=0c1b24a82097862027af6abe88f362db73e4846859a3ceda2dd466e4dc971a06

note() { printf '%s %s\n' "grpc-conformance:" "$*"; }
die() { printf '%s %s\n' "grpc-conformance:" "$*" >&2; exit 1; }

BIN=${SCION_SERVER:-target/release/scion-server}
[ -x "$BIN" ] || die "no server at $BIN (build it first, or set SCION_SERVER)"

# A pinned grpcurl, downloaded only if the PATH has none.
GRPCURL=$(command -v grpcurl || true)
if [ -z "$GRPCURL" ]; then
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) asset=linux_x86_64; want=$GRPCURL_LINUX_X86_64 ;;
    Darwin-arm64) asset=osx_arm64; want=$GRPCURL_OSX_ARM64 ;;
    Darwin-x86_64) asset=osx_x86_64; want=$GRPCURL_OSX_X86_64 ;;
    *) die "no pinned grpcurl for $(uname -s)-$(uname -m)" ;;
  esac
  dir=${TMPDIR:-/tmp}/grpcurl-$GRPCURL_VERSION-bin
  mkdir -p "$dir"
  tarball=$dir/grpcurl.tar.gz
  if [ ! -x "$dir/grpcurl" ]; then
    url="https://github.com/fullstorydev/grpcurl/releases/download/v$GRPCURL_VERSION/grpcurl_${GRPCURL_VERSION}_${asset}.tar.gz"
    curl -sL -o "$tarball" "$url"
    got=$(shasum -a 256 "$tarball" | cut -d' ' -f1)
    [ "$got" = "$want" ] || die "grpcurl $asset sha256 $got is not the pinned $want"
    tar xzf "$tarball" -C "$dir"
  fi
  GRPCURL=$dir/grpcurl
fi

# A pinned b3sum.
if ! command -v b3sum > /dev/null; then
  cargo install b3sum --locked --version "$B3SUM_VERSION" --quiet
fi

WORK=$(mktemp -d "${TMPDIR:-/tmp}/scion-grpc.XXXXXX")
SERVER_PID=
cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT
WORK=$(cd "$WORK" && pwd -P)

# The fixture: one commit of two files, so the walk covers a nested tree.
git init -q -b main "$WORK/git"
printf '# conformance\n' > "$WORK/git/README.md"
mkdir "$WORK/git/src"
printf 'pub fn f() {}\n' > "$WORK/git/src/lib.rs"
git -C "$WORK/git" -c user.name=Ada -c user.email=ada@example.com \
  add -A
git -C "$WORK/git" -c user.name=Ada -c user.email=ada@example.com \
  commit -q -m conformance

NO_COLOR=1 "$BIN" --data "$WORK/data" import "$WORK/git" --repo r --ref main \
  --verify 0 > /dev/null

# The server, with a ticket key, on a port the OS picks.
NO_COLOR=1 "$BIN" --data "$WORK/data" ticket-key --out "$WORK/ticket.key" > "$WORK/ticket.pub"
# The token goes through the environment: the argv form warns.
SCION_TOKEN=t NO_COLOR=1 "$BIN" --data "$WORK/data" serve --listen 127.0.0.1:0 \
  --ticket-key "$WORK/ticket.key" > "$WORK/server.log" 2>&1 &
SERVER_PID=$!
SERVER_ADDR=
for _ in $(seq 1 300); do
  SERVER_ADDR=$(sed -n 's/.*serving.*listen=\([0-9.]*:[0-9]*\).*/\1/p' \
    "$WORK/server.log" | tail -1)
  [ -n "$SERVER_ADDR" ] && break
  kill -0 "$SERVER_PID" 2>/dev/null || { cat "$WORK/server.log" >&2; die "the server exited"; }
  sleep 0.1
done
[ -n "$SERVER_ADDR" ] || { cat "$WORK/server.log" >&2; die "the server never said its address"; }
note "server at $SERVER_ADDR"

# grpcurl against the proto file: the server serves no reflection. The
# script runs under /bin/sh, which is dash on Ubuntu: no arrays, so one
# helper carries the flags.
call() { # call METHOD JSON -> grpcurl's output on stdout
  "$GRPCURL" -import-path proto -proto scion.proto -plaintext \
    -H 'authorization: Bearer t' -d "$2" "$SERVER_ADDR" "$1"
}

# The ref, then the ticket of its root tree.
CS=$(call scion.v1.Scion/ResolveRef '{"repo":"r","name":"main"}' | jq -r .changesetId)
[ -n "$CS" ] || die "ResolveRef answered no changeset"
REPLY=$(call scion.v1.Objects/GetRootTree "{\"repo\":\"r\",\"changesetId\":\"$CS\"}")
ROOT=$(echo "$REPLY" | jq -r .treeId)
TICKET=$(echo "$REPLY" | jq -r .ticket)
[ -n "$ROOT" ] && [ "$ROOT" != "null" ] && [ -n "$TICKET" ] && [ "$TICKET" != "null" ] \
  || die "GetRootTree answered no tree or ticket"

# The objects: the root tree, then what its entries name. The reply streams
# ObjectParts; a small object is one part, so its `data` holds all bytes.
get() { # get KIND B64ID PATH -> raw bytes on stdout
  # `path` is a bytes field, so it rides base64 in the JSON, as the ID does.
  p64=$(printf '%s' "$3" | base64)
  call scion.v1.Objects/GetObjects \
    "{\"ticket\":\"$TICKET\",\"refs\":[{\"kind\":\"$1\",\"id\":\"$2\",\"path\":\"$p64\"}]}" \
    | jq -r .data | base64 -d
}
tree_entries() { # TreeV1 bytes on stdin: prints "name tag id64" per entry
  python3 -c '
import sys, base64
data = sys.stdin.buffer.read()
assert data[0] == 1, "not a TreeV1"
n = int.from_bytes(data[1:5], "big")
at = 5
for _ in range(n):
    ln = int.from_bytes(data[at:at+4], "big")
    at += 4
    name = data[at:at+ln]
    at += ln
    tag = data[at]
    at += 1
    if tag == 4:
        id_ = data[at:at+32]; at += 32
    elif tag == 1:
        id_ = data[at:at+32]; at += 33
    elif tag == 2:
        id_ = data[at:at+32]; at += 32
    else:
        kind = data[at]
        at += 1
        id_ = data[at:at+(20 if kind == 1 else 32)]; at += 20 if kind == 1 else 32
    print(name.decode(), tag, base64.b64encode(id_).decode())
'
}

# The root tree, and its ID with the tree prefix.
get OBJECT_KIND_TREE "$ROOT" "" > "$WORK/root.tree"
{
  printf 'scion\0tree\0v1\0'
  cat "$WORK/root.tree"
} | b3sum | awk '{print $1}' > "$WORK/root.id"
WANT=$(python3 -c "import sys, base64; print(base64.b64decode('$ROOT').hex())")
[ "$(cat "$WORK/root.id")" = "$WANT" ] || die "the root tree does not hash to its ID"

# The tree entries, and each file and subdirectory against its ID.
tree_entries < "$WORK/root.tree" > "$WORK/entries"
while read -r name tag id; do
  case $tag in
    4)
      get OBJECT_KIND_TREE "$id" "$name" > "$WORK/one.tree"
      { printf 'scion\0tree\0v1\0'; cat "$WORK/one.tree"; } | b3sum | awk '{print $1}' > "$WORK/one.id"
      want=$(python3 -c "import base64; print(base64.b64decode('$id').hex())")
      [ "$(cat "$WORK/one.id")" = "$want" ] || die "$name does not hash to its tree ID"
      # One level down: the blob of each file of the subdirectory.
      tree_entries < "$WORK/one.tree" | while read -r sub stag sid; do
        if [ "$stag" = 1 ]; then
          get OBJECT_KIND_BLOB "$sid" "$name/$sub" > "$WORK/one.blob"
          { printf 'scion\0blob\0v1\0'; cat "$WORK/one.blob"; } | b3sum | awk '{print $1}' > "$WORK/one.oid"
          w=$(python3 -c "import base64; print(base64.b64decode('$sid').hex())")
          [ "$(cat "$WORK/one.oid")" = "$w" ] || die "$name/$sub does not hash to its blob ID"
        fi
      done
      ;;
    1)
      get OBJECT_KIND_BLOB "$id" "$name" > "$WORK/one.blob"
      { printf 'scion\0blob\0v1\0'; cat "$WORK/one.blob"; } | b3sum | awk '{print $1}' > "$WORK/one.oid"
      want=$(python3 -c "import base64; print(base64.b64decode('$id').hex())")
      [ "$(cat "$WORK/one.oid")" = "$want" ] || die "$name does not hash to its blob ID"
      ;;
  esac
done < "$WORK/entries"

note "every ref answered with bytes that hash to its ID"
