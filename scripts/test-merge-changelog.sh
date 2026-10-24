#!/usr/bin/env bash
# Tests scripts/merge-changelog.sh against a fixture: a CHANGELOG.md and
# three fragments, two of them in one section, the output compared with
# the expected text. Two in one section is what catches a check on the
# fragment list that reads its own newlines as whitespace (2026-10-19:
# that bug refused every section that held more than one fragment). Run
# from the repository root; CI runs it in the lint job.
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

cat >"$work/changes/11.added.md" <<'EOF'
An earlier thing, whose file sorts before the other added fragment, so
the merged rows come out in the fragment names' order.
EOF

cat >"$work/changes/12.added.md" <<'EOF'
A new thing that the changelog will name first, in a fragment of three
lines of plain text, no row marker and no indent on any line, wrapped
at 76 columns so the merged row fits in 79.
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

- An earlier thing, whose file sorts before the other added fragment, so
  the merged rows come out in the fragment names' order.
- A new thing that the changelog will name first, in a fragment of three
  lines of plain text, no row marker and no indent on any line, wrapped
  at 76 columns so the merged row fits in 79.
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

# --check: the same rules, nothing merged. A fragment that fails the
# merge's own contract - front matter, a section the script would
# silently drop, an empty file, a row marker on the first line - must
# fail with the fragment named, and a clean fixture must pass.
check=$(mktemp -d)
trap 'rm -rf "$work" "$check"' EXIT
mkdir "$check/changes"
cat >"$check/CHANGELOG.md" <<'EOF'
## [v0.1.0] - unreleased
EOF

for bad in 'front-matter' 'bad-section' 'empty' 'row-marker'; do
    case $bad in
        front-matter)
            printf -- '---\nms: "x"\n---\n\nA thing.\n' >"$check/changes/1.fixed.md"
            ;;
        bad-section)
            printf 'A thing.\n' >"$check/changes/1.bad.md"
            ;;
        empty)
            : >"$check/changes/1.added.md"
            ;;
        row-marker)
            printf -- '- A thing.\n' >"$check/changes/1.fixed.md"
            ;;
    esac
    if (cd "$check" && "$here/merge-changelog.sh" --check) >/dev/null 2>&1; then
        echo "--check accepted a $bad fragment" >&2
        exit 1
    fi
done

rm -rf "$check/changes"
mkdir "$check/changes"
printf 'A thing that passes every rule, no front matter, no row\nmarker on the first line, no indent on any line.\n' >"$check/changes/1.fixed.md"
(cd "$check" && "$here/merge-changelog.sh" --check) >/dev/null
echo "merge-changelog --check: fixture ok"

# The argument is either --check or a release name: a flag nobody reads
# ("--dry-run" merged every fragment under "## [--dry-run]" and deleted
# them, 2026-10-24), a word like v1, or an empty string must be refused
# with exit 2 and must touch nothing - every fragment and CHANGELOG.md
# keep the checksum they had.
args=$(mktemp -d)
trap 'rm -rf "$work" "$check" "$args"' EXIT
mkdir "$args/changes"
printf 'A real fragment, still on the books.\n' >"$args/changes/1.fixed.md"
printf '## [v0.1.0] - unreleased\n' >"$args/CHANGELOG.md"
for bad in '--dry-run' 'v1' ''; do
    before=$(find "$args" -type f | sort | xargs cat | cksum)
    st=0
    (cd "$args" && "$here/merge-changelog.sh" "$bad") >/dev/null 2>&1 || st=$?
    if [ "$st" -eq 0 ]; then
        echo "the argument [$bad] was accepted; only --check or a release may be" >&2
        exit 1
    fi
    if [ "$st" -ne 2 ]; then
        echo "the argument [$bad] failed with status $st, not the usage exit 2" >&2
        exit 1
    fi
    after=$(find "$args" -type f | sort | xargs cat | cksum)
    if [ "$before" != "$after" ]; then
        echo "the argument [$bad] touched files before its refusal" >&2
        exit 1
    fi
done
echo "merge-changelog arguments: fixture ok"
