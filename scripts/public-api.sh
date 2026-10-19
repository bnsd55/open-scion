#!/usr/bin/env bash
# Prints the public API of every publishable crate with
# cargo-public-api, one file per crate under the folder the caller
# gives, so the crates.io publish of #21 can diff what a release would
# expose against the previous release before it publishes anything.
#
#   scripts/public-api.sh api-v0.1.0
#
# Writes <folder>/<crate>.txt for each crate and prints one line per
# crate with the line count of its listing. The listing is built from
# a nightly rustdoc: cargo-public-api builds the rustdoc JSON with the
# nightly toolchain even when stable is active, and switches to it by
# itself. The script installs cargo-public-api (locked) and the
# nightly toolchain when either is missing.
set -euo pipefail

die() {
    echo "$(basename "$0"): $*" >&2
    exit 1
}

[ $# -eq 1 ] || die "usage: scripts/public-api.sh <output-folder>"

# The five publishable crates of the workspace, in dependency order.
# The fuzz crates sit outside the workspace and do not publish.
crates="open-scion-core open-scion-proto open-scion-tier open-scion-server open-scion"

if ! command -v cargo-public-api >/dev/null 2>&1; then
    echo "public-api.sh: installing cargo-public-api (cargo install --locked)" >&2
    cargo install --locked cargo-public-api
fi

if ! command -v rustup >/dev/null 2>&1; then
    die "rustup is required for the nightly rustdoc that cargo-public-api builds its listing from"
fi

# A missing nightly fails deep inside cargo-public-api's own run, so
# check for it here, where the fix is one command.
if ! rustup run nightly rustdoc --version >/dev/null 2>&1; then
    echo "public-api.sh: installing the nightly toolchain (rustup toolchain install nightly)" >&2
    rustup toolchain install nightly
fi

cd "$(dirname "${BASH_SOURCE[0]}")/.."
out=$1
mkdir -p "$out"

for crate in $crates; do
    file="$out/$crate.txt"
    cargo public-api -p "$crate" >"$file"
    lines=$(wc -l <"$file" | tr -d ' ')
    echo "$crate: $lines lines -> $file"
done
