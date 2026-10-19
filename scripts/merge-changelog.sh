#!/usr/bin/env bash
# Merges the changes/ fragments into CHANGELOG.md under one release
# heading. Run from the repository root:
#
#   scripts/merge-changelog.sh v0.1.0
#
# Every changes/<slug>.<section>.md becomes a "- " row at the top of the
# matching `### <Section>` under `## [<release>]`; a subsection that does
# not exist yet is created at the end of the release's block, in the
# changelog's section order. The fragments are deleted after they land.
# Nothing else changes.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: scripts/merge-changelog.sh <release>   e.g. v0.1.0" >&2
    exit 2
fi
release=$1
changelog=CHANGELOG.md
frag_dir=changes

if [ ! -f "$changelog" ]; then
    echo "no CHANGELOG.md here; run from the repository root" >&2
    exit 1
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
merged=$work/merged.txt
: >"$merged"

# The sections in the order the changelog prints them, with their
# headings. ${section^} is not portable to the bash that ships on macOS.
for section in added changed fixed security; do
    case $section in
        added) Section=Added ;;
        changed) Section=Changed ;;
        fixed) Section=Fixed ;;
        security) Section=Security ;;
    esac
    fragments=$(find "$frag_dir" -maxdepth 1 -name "*.$section.md" -type f 2>/dev/null | LC_ALL=C sort || true)
    if [ -z "$fragments" ]; then
        continue
    fi
    rows=$work/rows.txt
    : >"$rows"
    for fragment in $fragments; do
        # A fragment is plain text: no row marker, no indent, and a file
        # name with no whitespace, which the row's own text would name as
        # two words. The list's newlines between paths are not names: the
        # check runs on each path, or two fragments in one section would
        # refuse the whole release.
        case $fragment in *[[:space:]]*)
            echo "fragment file names must not contain whitespace: $fragment" >&2
            exit 1
            ;;
        esac
        # A fragment is plain text: no row marker, no indent. The row
        # marker goes on the first line, and every later line takes the
        # row's two-space indent, as today's changelog rows read.
        sed '1s/^/- /; 2,$s/^/  /' "$fragment" >>"$rows"
        echo "$fragment" >>"$merged"
    done

    # The values name a release heading, a section heading and a scratch
    # file: no backslashes, so -v passes them through unchanged.
    awk -v ROWS="$rows" -v SECTION="$Section" -v RELEASE="$release" '
        # Passes the changelog through, inserting the rows of one section
        # at the top of its subsection in the named release block.
        BEGIN { in_release = 0; inserted = 0 }
        function flush_rows() {
            while ((getline row < ROWS) > 0) print row
            close(ROWS)
        }
        {
            if ($0 ~ /^## /) {
                if (in_release && !inserted) {
                    # The release block ended without the subsection: it
                    # is created at the end of the block.
                    printf "\n### %s\n\n", SECTION
                    flush_rows()
                    inserted = 1
                }
                in_release = ($0 ~ ("^## \\[" RELEASE "\\]"))
            }
            if (in_release && !inserted && $0 == "### " SECTION) {
                print
                if ((getline next_line) > 0 && next_line == "") {
                    print next_line
                    flush_rows()
                } else {
                    flush_rows()
                    print next_line
                    inserted = 1
                    next
                }
                inserted = 1
                next
            }
            print
        }
        END {
            if (in_release && !inserted) {
                printf "\n### %s\n\n", SECTION
                flush_rows()
            }
        }' "$changelog" >"$work/changelog.next"
    mv "$work/changelog.next" "$changelog"
done

if [ -s "$merged" ]; then
    while IFS= read -r fragment; do
        rm "$fragment"
    done <"$merged"
    echo "merged and deleted:"
    cat "$merged"
else
    echo "no fragments in $frag_dir/"
fi
