#!/usr/bin/env bash
# Every `#[doc(hidden)]` item of the server is a test seam behind the
# `test-hooks` feature: #251 made the rule, and a merge that drops the
# gate still compiles, so this check fails instead. An item marked
# `#[doc(hidden)]` must carry `#[cfg(any(test, feature = "test-hooks"))]`
# on the same line or within the next two. Comment lines do not count.
set -euo pipefail
cd "$(dirname "$0")/.."

exec awk '
    /^[[:space:]]*\/\// { next }
    /doc\(hidden\)/ && $0 !~ /cfg\(any\(test/ {
        gated = 0
        for (i = 0; i < 2 && (getline line) > 0; i++) {
            if (line ~ /cfg\(any\(test/) { gated = 1; break }
        }
        if (!gated) {
            printf "%s:%d: doc(hidden) item without the test-hooks gate\n", FILENAME, FNR
            bad = 1
        }
    }
    END { exit bad ? 1 : 0 }
' crates/scion-server/src/*.rs
