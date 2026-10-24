#!/bin/sh
# Feeds scripts/store-upgrade.sh's tag resolution a repository whose
# release tags the resolver caps, and checks the two promises the doc
# makes: the ladder keeps the newest 8, and every tag outside the cap is
# named in a ::warning::, never dropped silently. The cap decision is
# what a CI run shows only with 9+ tags on the books — a state the
# workflow reaches months in, so it is decided here, on a fixture repo,
# with nine tags.
#
# The fixture is a throwaway git repository (no checkout's config is
# written; identity comes from the environment), nine v-tags plus one
# non-release ref the resolver must ignore. The script runs from a copy
# inside the fixture, because ROOT is where the script lives.
set -eu
cd "$(dirname "$0")"

# A CI runner carries no git identity; the commits below need one.
# The environment, never a config write.
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t
export GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

git init -q "$work/repo"
mkdir "$work/repo/scripts"
cp ./store-upgrade.sh ./drill-lib.sh "$work/repo/scripts/"
git -C "$work/repo" commit -q --allow-empty -m base
for i in 1 2 3 4 5 6 7 8 9; do
  git -C "$work/repo" commit -q --allow-empty -m "r$i"
  git -C "$work/repo" tag "v0.$i.0"
done
git -C "$work/repo" commit -q --allow-empty -m after
# Not a release tag: the v[0-9]* filter must drop it.
git -C "$work/repo" tag not-a-release

# The resolver needs the script's ROOT to be the fixture repo.
out=$(GITHUB_STEP_SUMMARY="$work/summary" \
  "$work/repo/scripts/store-upgrade.sh" --resolve-tags) || exit 1

kept=$(printf '%s\n' "$out" | grep -c '^v[0-9]')
[ "$kept" -eq 8 ] || { echo "FAIL: the ladder holds $kept tags, want 8"; exit 1; }
printf '%s\n' "$out" | grep -q '^v0.2.0$' || { echo "FAIL: the newest 8 must start at v0.2.0"; exit 1; }
printf '%s\n' "$out" | grep -q '^v0.1.0$' && { echo "FAIL: v0.1.0 is outside the cap and must not be kept"; exit 1; }

grep -q '::warning::' "$work/summary" || { echo "FAIL: no ::warning:: in the summary"; exit 1; }
grep -q 'v0.1.0' "$work/summary" || { echo "FAIL: the warning must name the skipped tag"; exit 1; }

# Eight tags exactly: no warning, all kept.
git -C "$work/repo" tag -d v0.1.0 > /dev/null
out=$("$work/repo/scripts/store-upgrade.sh" --resolve-tags) || exit 1
kept=$(printf '%s\n' "$out" | grep -c '^v[0-9]')
[ "$kept" -eq 8 ] || { echo "FAIL: with 8 tags the ladder holds $kept, want 8"; exit 1; }

echo "store-upgrade resolver: 9 tags -> 8 kept + warning naming v0.1.0; 8 tags -> 8 kept, no warning"
