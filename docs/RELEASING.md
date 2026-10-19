# Releasing

How to cut a release of open-scion. v0.1.x is a source release: a git
tag plus GitHub release notes. Nothing publishes crates: the crates
publish later, after their public APIs are narrowed
([#21](https://github.com/bnsd55/open-scion/issues/21)). Nothing here
tags by itself: each step is run by a person.

## Before a release

- Every required check is green on the release commit. A red check
  blocks the release; `gh pr checks` on the release PR lists them.
- The milestone for the version has no open issues.
- The `publish check` CI job runs on every PR and keeps the five
  crates valid for the later crates.io publish: each crate carries the
  metadata crates.io wants (a description, the license
  `MIT OR Apache-2.0`, the repository and homepage, keywords and
  categories, and the workspace `rust-version`), and the job packages
  and verifies them in one dependency-order `cargo package
  --workspace`, which fails loudly when a field is missing.

## The release PR

A release is cut through one PR from a branch named
`release/<version>`. Its checklist:

1. **Merge the CHANGELOG fragments.** Run
   `scripts/merge-changelog.sh v0.1.0` (or the release's section), so
   that the fragments under `changes/` (`changes/<slug>.<section>.md`)
   are merged into the version's section. Review the result, and
   commit it on the release branch.
2. **Set the date in `CHANGELOG.md`.** Rename the `## [v0.1.0] -
   unreleased` section to `## [v0.1.0] - YYYY-MM-DD` with the release
   day, and add a fresh empty `## [Unreleased]` above it. The entry
   must say what the release adds and changes in plain language,
   grouped by area, with the issue numbers that stay true after the
   release.
3. **Check the version.** All five crates share one version through
   `[workspace.package]` in the root `Cargo.toml`. A release bumps one
   number there and the four `version = "..."` entries of the internal
   dependencies in `[workspace.dependencies]` to the same value.
4. **Notes regenerated from the tagged commit.** The GitHub release
   notes come from the version's `CHANGELOG.md` section read at the
   tagged commit (`git show v0.1.0:CHANGELOG.md`), never from a local
   working tree or an edited copy.
5. **A clean-checkout build and quick-start smoke.** From a fresh
   clone of the tagged commit, build the workspace and run the
   README's `Try it` flow end to end.
6. **The umask-002 run.** Once #302's CI job exists, it runs on the
   release commit and its green result is part of the release.
7. **Benchmark numbers with their provenance.** A number that a
   release note states records the commit it measured, the OS, the
   filesystem, the dataset, the command, and the variance across runs.
   A release note never names the maintainer's machine.

## Release steps

1. **Tag.** Cut an annotated tag from the commit that the release
   notes describe:

   ```sh
   git tag -a v0.1.0 -m "open-scion v0.1.0"
   git push origin v0.1.0
   ```

   Then check the tag landed on the release commit:

   ```sh
   git ls-remote --tags origin v0.1.0
   ```

   The line it prints must name the commit the notes describe. If it
   does not, stop and redo the tag per "Undoing a bad tag" before any
   release notes point at it.

2. **GitHub release.** Open a release for the tag with the notes
   rebuilt from the `CHANGELOG.md` section of the version at that
   commit (`git show v0.1.0:CHANGELOG.md`), and the measured numbers
   with the provenance the checklist asks for.

## Undoing a bad tag

A tag that names the wrong commit, or a release whose notes are wrong,
is fixed one of two ways. Git's own advice (`git tag --help`, "On
Re-tagging") is not to reuse a name that others may have fetched.

- If the bad tag was public only briefly and the release had no users
  yet: delete the GitHub release that points at the tag, delete the
  tag on the remote and locally (`git push origin :refs/tags/v0.1.0`,
  then `git tag -d v0.1.0`), fix what was wrong, cut a new tag from
  the right commit, and open the release again. Anyone who fetched the
  old tag re-fetches with `git fetch --tags --force`; say so in the
  release thread.
- Otherwise leave the tag and its release: mark the release as bad in
  its notes, and cut the next patch version (v0.1.1) from the fixed
  commit.

Never move a tag to a different commit in place.

## After

- Close the milestone if it is still open.
- The tag and its release notes stay: they are what a later reader
  checks out, and what the later crates.io publish of #21 tags from.

## Notes

- Each crate ships both license files: `crates/<crate>/LICENSE-MIT` and
  `LICENSE-APACHE` are links to the files at the repository root, and
  `cargo package --list` shows them for every crate.
- Windows is not supported; the release notes say so.
- Never tag from a fork or a dirty worktree; the tag records the
  commit that built it.
