#!/usr/bin/env bash
# Tests scripts/merge-changelog.sh against a fixture: a CHANGELOG.md and
# two fragments, the output compared with the expected text. Run from the
# repository root; CI runs it in the lint job.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/changes"

cat >"$work/CHANGELOG.md" <<'EOF'
# Changelog

## [Unreleased]

## [v0.1.0] - unreleased

The first release.

### Added

- An existing row.

### Fixed

- An existing fix.
EOF

cat >"$work/changes/12.added.md" <<'EOF'
A new thing that the changelog will name first, wrapped at seventy-nine
  columns in the file, with no row marker.
EOF

cat >"$work/changes/13.security.md" <<'EOF'
A report whose subsection does not exist yet, so the merge creates it
  at the end of the release block.
EOF

(cd "$work" && "$here/merge-changelog.sh" v0.1.0) >"$work/out.txt"

if [ ! -f "$work/CHANGELOG.md" ]; then
    echo "the merge lost CHANGELOG.md" >&2
    exit 1
fi
if [ -d "$work/changes" ] && [ -n "$(ls -A "$work/changes")" ]; then
    echo "the merge left fragments behind:" >&2
    ls "$work/changes" >&2
    exit 1
fi

cat >"$work/expected.md" <<'EOF'
# Changelog

## [Unreleased]

## [v0.1.0] - unreleased

The first release.

### Added

- A new thing that the changelog will name first, wrapped at seventy-nine
  columns in the file, with no row marker.
- An existing row.

### Fixed

- An existing fix.

### Security

- A report whose subsection does not exist yet, so the merge creates it
  at the end of the release block.
EOF

if ! diff -u "$work/expected.md" "$work/CHANGELOG.md"; then
    echo "the merged changelog does not match the expected text" >&2
    exit 1
fi
echo "merge-changelog: fixture ok"
