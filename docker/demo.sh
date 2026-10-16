#!/bin/sh
# Builds the image and runs the whole demo: a server and a cache with TLS,
# a repo seeded with one commit, and a checkout through the cache.
#
# Every secret here is made at compose time and stays in docker/demo/certs:
# nothing is committed, and nothing is baked into the image.
set -eu
cd "$(dirname "$0")"

if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is needed to make the demo certificates" >&2
    exit 1
fi

# --- certificates: one CA, one certificate per service -----------------
rm -rf certs work
mkdir -p certs work

openssl req -x509 -newkey ed25519 -nodes -days 30 \
    -keyout certs/ca.key -out certs/ca.pem \
    -subj "/CN=scion demo ca" >/dev/null 2>&1

for name in server cache; do
    openssl req -newkey ed25519 -nodes \
        -keyout "certs/$name.key" -out "certs/$name.csr" \
        -subj "/CN=$name" >/dev/null 2>&1
    printf "subjectAltName=DNS:%s\n" "$name" > "certs/$name.ext"
    openssl x509 -req -days 30 \
        -in "certs/$name.csr" -CA certs/ca.pem -CAkey certs/ca.key \
        -CAcreateserial -extfile "certs/$name.ext" \
        -out "certs/$name.crt" >/dev/null 2>&1
    rm "certs/$name.csr" "certs/$name.ext"
done
chmod 600 certs/*.key
chmod 644 certs/*.pem certs/*.crt

# --- the image, then the server's ticket key ---------------------------
# CI builds the image first with the gha layer cache and tags it
# open-scion-demo; compose builds only when the image is missing.
docker image inspect open-scion-demo >/dev/null 2>&1 || docker compose build

# The subcommand prints the public key on its last line; the cache
# verifies tickets against it. The key file itself lands in the server's
# data volume, written by the server's own user.
PUB=$(docker compose run --rm --entrypoint scion-server server \
    --data /data ticket-key --out /data/ticket.key | tail -n 1)
export SCION_TICKET_PUB="$PUB"

echo "starting the server and the cache"
docker compose up -d server cache

# --- a repo with one commit, then a checkout through the cache ---------
echo "seeding the demo repo"
docker compose run --rm scn '
    set -eu
    mkdir -p /work/src
    printf "hello from scion\n" > /work/src/README.md
    scn commit -m "the demo commit" --put README.md=/work/src/README.md
' > work/commit.json
ID=$(sed -n "s/.*\"changeset\": *\"\([0-9a-f]*\)\".*/\1/p" work/commit.json)
docker compose run --rm scn "scn land $ID --onto main"

echo "checking out main through the cache"
docker compose run --rm scn '
    set -eu
    scn checkout main -d /work/out
    diff -r /work/src /work/out
    echo DEMO-OK
'

docker compose down --volumes
