#!/usr/bin/env bash
# Merges the changes/ fragments into CHANGELOG.md under one release
# heading. Run from the repository root:
#
#   scripts/merge-changelog.sh v0.1.0
#
# With --check instead of a release, it applies the fragment rules and
# changes nothing: every changes/<slug>.<section>.md is read, and a
# fragment that the merge would turn into a broken changelog row fails
# with its file name and the broken rule. The rules:
#
#   - the name is <slug>.<section>.md, where the section is one of
#     added, changed, fixed, security, and neither half holds
#     whitespace;
#   - the file is not empty;
#   - the content is plain text: no front-matter preamble (the first
#     line is not "---"), no line that starts with a row marker ("- ")
#     or with whitespace, which the merge would keep in the row.
#
# Every changes/<slug>.<section>.md becomes a "- " row at the top of the
# matching `### <Section>` under `## [<release>]`; a subsection that does
# not exist yet is created at the end of the release's block, in the
# changelog's section order. The fragments are deleted after they land.
# Nothing else changes.
set -euo pipefail

usage() {
    echo "usage: scripts/merge-changelog.sh --check | <release>   e.g. v0.1.0" >&2
}

# One argument: --check, or a release name the changelog can hold
# (semver with an optional pre-release). Anything else - a flag nobody
# reads ("--dry-run" merged 142 fragments under "## [--dry-run]"), a
# word, an empty string - must never become the release's heading.
if [ "$#" -ne 1 ]; then
    usage
    exit 2
fi
if [ "$1" != "--check" ] &&
    ! [[ "$1" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]]; then
    usage
    exit 2
fi

# --check: the fragment rules, nothing merged. The same rules the merge
# relies on, from one place, so a fragment the release script refuses
# fails CI on its own PR instead of the release refresh.
if [ "$1" = "--check" ]; then
    check=1
    release=CHECK
    # A file in changes/ that no section's find matches never merges and
    # is never deleted: the release silently drops it. Refuse it here.
    known=$(find changes -maxdepth 1 -type f \( -name '*.added.md' \
        -o -name '*.changed.md' -o -name '*.fixed.md' \
        -o -name '*.security.md' \) | wc -l | tr -d ' ')
    all=$(find changes -maxdepth 1 -type f -name '*.md' | wc -l | tr -d ' ')
    if [ "$known" -ne "$all" ]; then
        find changes -maxdepth 1 -type f -name '*.md' \! \( -name '*.added.md' \
            -o -name '*.changed.md' -o -name '*.fixed.md' \
            -o -name '*.security.md' \) |
            while IFS= read -r stray; do
                echo "fragment's section is not one of added, changed, fixed, security: $stray" >&2
            done
        exit 1
    fi
else
    check=
    release=$1
fi
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
        if [ -n "$check" ]; then
            if [ ! -s "$fragment" ]; then
                echo "fragment is empty: $fragment" >&2
                exit 1
            fi
            if [ "$(head -n 1 "$fragment")" = "---" ]; then
                echo "fragment starts with front matter ('---'); it is plain text: $fragment" >&2
                exit 1
            fi
            # The first line becomes the row, so whitespace or a row
            # marker on it would double ("- - x" or an indented row).
            # Later lines may carry a nested bullet ("- ") or an indent,
            # which the merge keeps inside the row, as the landed
            # fragments write them.
            if head -n 1 "$fragment" | grep -qn '^\([[:space:]]\|- \)'; then
                echo "fragment's first line starts with whitespace or a row marker ('- '); it is plain text: $fragment" >&2
                exit 1
            fi
            continue
        fi
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
