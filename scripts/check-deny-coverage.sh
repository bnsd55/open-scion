#!/bin/sh
# Every Cargo.lock in the repository must sit where a cargo deny check
# reads it: the root lockfile (the supply-chain job's own check) or a
# fuzz sub-workspace (crates/*/fuzz, which the job's loop covers). A
# lockfile anywhere else is one cargo-deny never saw, and an advisory
# can ride it — this is how RUSTSEC-2026-0382 lived in the fuzz
# lockfiles unseen by the root check. The script fails and names the
# stray lockfile, so a new sub-workspace cannot slip past.
set -eu
cd "$(dirname "$0")/.."
fail=0
for lock in $(find . -name Cargo.lock -not -path './target/*'); do
  case "$lock" in
    ./Cargo.lock) ;;
    ./crates/*/fuzz/Cargo.lock) ;;
    *)
      echo "no cargo deny step covers $lock"
      echo "add its directory to the deny loop in ci.yml, or move the workspace under crates/*/fuzz"
      fail=1
      ;;
  esac
done
exit "$fail"
