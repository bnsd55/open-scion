#!/usr/bin/env bash
# Every `#[doc(hidden)]` item of the server is a test seam behind the
# `test-hooks` feature: #251 made the rule, and a merge that drops the
# gate still compiles, so this check fails instead. An item marked
# `#[doc(hidden)]` must carry the whole gate
# `cfg(any(test, feature = "test-hooks"))` on its line or within the
# next two non-comment lines. Comment lines neither trigger nor resolve.
#
# --self-test runs the scan over the fixtures in scripts/testdata, so CI
# proves the check itself: one fixture passes, one fails.
set -euo pipefail
cd "$(dirname "$0")/.."

scan() {
    awk '
        FNR == 1 {
            if (pending > 0) {
                printf "%s:%d: doc(hidden) item without the test-hooks gate\n", prevfile, pending_line
                bad = 1
            }
            pending = 0
        }
        /^[[:space:]]*\/\// { next }
        {
            resolved = ($0 ~ /cfg\(any\(test, feature = "test-hooks"\)/)
            if (pending > 0) {
                if (resolved) {
                    pending = 0
                } else {
                    pending--
                    if (pending == 0) {
                        printf "%s:%d: doc(hidden) item without the test-hooks gate\n", prevfile, pending_line
                        bad = 1
                    }
                }
            }
            if ($0 ~ /doc\(hidden\)/ && !resolved) {
                if (pending > 0) {
                    printf "%s:%d: doc(hidden) item without the test-hooks gate\n", prevfile, pending_line
                    bad = 1
                }
                pending = 2
                pending_line = FNR
            }
            prevfile = FILENAME
        }
        END {
            if (pending > 0) {
                printf "%s:%d: doc(hidden) item without the test-hooks gate\n", prevfile, pending_line
                bad = 1
            }
            exit bad ? 1 : 0
        }
    ' "$@"
}

if [ "${1:-}" = "--self-test" ]; then
    scan scripts/testdata/test-seams/gated.rs
    if scan scripts/testdata/test-seams/ungated.rs >/dev/null 2>&1; then
        echo "self-test: the ungated fixture must fail" >&2
        exit 1
    fi
    echo "self-test ok"
    exit 0
fi

if [ "$#" -gt 0 ]; then
    scan "$@"
    exit
fi
scan crates/scion-server/src/*.rs
