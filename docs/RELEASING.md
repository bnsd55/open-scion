# Releasing

How to cut a release of openscion. v0.1.x is a source release: one
SSH-signed annotated git tag plus one immutable GitHub release with
notes. Nothing publishes crates: the owner has decided (D12) that no
crate publishes before a real outside consumer exists, so a narrow
client crate is the only publish ever planned. Nothing here tags by
itself: each step is run by a person.

## Before a release

- Run the drills of docs/drills/ that the release's milestone lists on
  the release candidate, and record each run beside the template there
  (a drill is the proof of a runbook step no CI test can run).
- Every required check is green on the release commit. A red check
  blocks the release; `gh pr checks` on the release PR lists them.
- The milestone for the version has no open issues but the release's
  own tracking one, which closes with the release itself.
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
   day, and add a fresh empty `## [Unreleased]` above it. The note that
   the old `Unreleased` carried, pointing new entries at the version's
   section, goes with the rename: a dated section takes no pointer, and
   a fresh `Unreleased` needs none. The entry
   must say what the release adds and changes in plain language,
   grouped by area, with the issue numbers that stay true after the
   release.
3. **Check the version.** All five crates share one version through
   `[workspace.package]` in the root `Cargo.toml`. A release bumps one
   number there and the four `version = "..."` entries of the internal
   dependencies in `[workspace.dependencies]` to the same value. For
   the first release nothing is bumped: the five already say 0.1.0,
   and the step is the check that they are equal. A later release
   bumps all five in one commit.
4. **Notes regenerated from the tagged commit.** The GitHub release
   notes come from the version's `CHANGELOG.md` section read at the
   tagged commit (`git show v0.1.0:CHANGELOG.md`), never from a local
   working tree or an edited copy. The command prints the whole file;
   the version's section alone comes out of

   ```sh
   git show v0.1.0:CHANGELOG.md | awk \
     '/^## \[v0.1.0\]/{f=1} f && /^## \[/{if(++n==2) exit} f'
   ```
5. **A clean-checkout build and quick-start smoke.** From a fresh
   clone of the tagged commit, build the workspace and run the
   README's `Try it` flow end to end. The tagged commit is also the
   operator's source for the binaries (see `docs/operations.md`,
   "Upgrades"): a source release says so in its notes (the binaries
   build from this tag: `git checkout <tag>; cargo build --release
   --locked`); a release that ships binaries says that instead. Either
   way the notes point the operator at keeping the previous release's
   binaries beside the new ones, because a rollback needs them.
6. **The umask-002 run.** #302 is closed by choice: the group-writable
   rule stays fail-closed and the refusal names the chmod, so the
   release's umask-002 run proves the refusal, not an exception.
7. **Benchmark numbers with their provenance.** A number that a
   release note states records the commit it measured, the OS, the
   filesystem, the dataset, the command, and the variance across runs.
   A release note never names the maintainer's machine.

## Release steps

1. **Tag, signed.** Cut an annotated tag from the commit that the
   release notes describe. The owner signs it personally with their SSH
   key (D18), so the signature a verifier checks is the owner's, not a
   CI secret's:

   ```sh
   git -c gpg.format=ssh -c user.signingkey="$HOME/.ssh/id_ed25519.pub" \
     tag -s v0.1.0 -m "openscion v0.1.0"
   git push origin v0.1.0
   ```

   Verify the signature before the push (`git tag -v v0.1.0`), and that
   the tag points at the release commit:

   ```sh
   test "$(git rev-parse 'v0.1.0^{}')" = "$(git rev-parse <the release commit>)"
   ```

   Then push, and check the tag landed on the release commit. An
   annotated tag makes `git ls-remote` print the tag object's SHA, so
   compare the peeled ref to the commit:

   ```sh
   test "$(git ls-remote origin 'refs/tags/v0.1.0^{}' | cut -f1)" \
     = "$(git rev-parse <the release commit>)"
   ```

   If the check fails, stop and redo the tag per "Undoing a bad tag"
   before any release notes point at it.

2. **GitHub release, immutable.** The release workflow
   (`.github/workflows/release.yml`) does this step: on the tag push it
   builds every artifact twice per cell, fails on one differing byte,
   attests each artifact and SBOM, verifies the attestations, and
   opens the release immutable with the notes rebuilt from the
   `CHANGELOG.md` section of the version at that commit. A person does
   not run it; a person reads its run. What a person must turn on
   once, before the first tag (repository settings only the owner can
   change):

   - **A ruleset on `refs/tags/v*`** (Settings → Rules → Rulesets):
     creation restricted to the owner, deletion denied, and updates
     denied. It is what stops anyone else from pushing a `v*` tag the
     workflow would happily build. GitHub's older tag-protection rules
     are replaced by rulesets; this is the ruleset to create.
   - **Immutable releases** (Settings → General → Releases): the
     workflow also passes `--immutable` to `gh release create`, so a
     release is protected even before the repository-wide setting
     exists.
   - **The registry the SBOMs name is not secret; nothing else to
     add.** The workflow needs no key: the tag's SSH signature is
     verified by GitHub at push time against the key on the owner's
     account (D18), and the build signs nothing of its own — the
     provenance comes from GitHub's hosted builder.

   The dry-run proves all of it before the first tag: dispatch the
   workflow from `main` with `dry_run` on and the `commit` input set to
   a main commit's SHA, and push no tag — the run checks out that
   commit and refuses one that is not an ancestor of `main`. Read the
   run: the diff job prints `NO DIFFERENCE`, the `verify` job's `gh
   attestation verify` output is the proof — and nothing is published.

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

## The crates.io publish

The crates publish after the release tag ([#21]). One step guards
what they publish: on the tagged commit, run
`scripts/public-api.sh api-<version>`. The script writes one file per
crate (`api-<version>/open-scion-core.txt` and so on), built with
cargo-public-api from a nightly rustdoc (it installs both when they
are missing), and a `VERSIONS` file with the exact `cargo public-api`
and nightly `rustc` versions that made the listing. Diff each file
against the same listing taken from the previous release's tag, and
read the diff before anything publishes: every new `pub` item and
every changed signature is a promise crates.io cannot take back. A
diff between two listings that different tool versions made can show
format noise instead of an API change; compare the `VERSIONS` files,
and when they differ, remake one side with the other's versions
before you read the diff as an API change.

## Notes

- Each crate ships both license files: `crates/<crate>/LICENSE-MIT` and
  `LICENSE-APACHE` are links to the files at the repository root, and
  `cargo package --list` shows them for every crate.
- Windows is not supported; the release notes say so.
- Never tag from a fork or a dirty worktree; the tag records the
  commit that built it.
