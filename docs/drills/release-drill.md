# Drill: <date> - a second maintainer cuts a release

A recorded drill is the proof of a process step no CI test can run.
This one walks #393's second maintainer through
[RELEASING.md](../RELEASING.md) as it stands: starting with the dry
run, using only what a maintainer's account can reach, and marking
every step that today still needs the **owner** - their machine, their
SSH key, or a repository setting only they can change. That list of
marks is the v0.8 gap ("without one person's account, machine or
private key"). One file per run, in this shape.

- Version drilled: <the version and commit>
- Runbook: docs/RELEASING.md; docs/drills/ for the drills the
  milestone lists
- Who ran it: <name>

## Steps

### R1 - The dry run, before anything else (RELEASING.md "Release steps" 2, the dry-run paragraph)

1. Dispatch the release workflow from `main` with `dry_run` on and the
   `commit` input set to a main commit's SHA. Push no tag.

   Done when: the run checks out that commit (and refuses one that is
   not an ancestor of `main`), the diff job prints `NO DIFFERENCE`,
   and the `verify` job's `gh attestation verify` output is on the
   run's log. Nothing is published.

   Notes: ___

### R2 - The drills on the release candidate (RELEASING.md "Before a release")

1. Run the drills of docs/drills/ that the release's milestone lists,
   on the release candidate, and record each run beside
   docs/drills/TEMPLATE.md.

   Done when: every listed drill has a dated record file.

   Notes: ___

### R3 - The gates (RELEASING.md "Before a release")

1. `gh pr checks` on the release PR lists the required checks: every
   one is green on the release commit. A red check blocks the
   release.

2. The milestone for the version has no open issues but the release's
   own tracking one.

   Done when: both hold, and you can show them on the screen.

   Notes: ___

### R4 - The release PR: changelog, date, version (RELEASING.md "The release PR" 1-3)

1. `scripts/merge-changelog.sh v0.1.0` (or the release's section);
   review the result and commit it on the `release/<version>` branch.

2. Rename the version's `## [v0.1.0] - unreleased` to the release day,
   with a fresh empty `## [Unreleased]` above it.

3. The version check: all five crates share one version through
   `[workspace.package]`, and the four internal dependency entries say
   the same. For the first release nothing is bumped; a later release
   bumps all five in one commit.

   Done when: the PR carries the merged changelog, the dated section,
   and the version step.

   Notes: ___

### R5 - Notes from the tagged commit, and the smoke (RELEASING.md "The release PR" 4-7)

1. The GitHub release notes come from the version's `CHANGELOG.md`
   section read at the tagged commit, never a local working tree:

   ```sh
   git show v0.1.0:CHANGELOG.md | awk \
     '/^## \[v0.1.0\]/{f=1} f && /^## \[/{if(++n==2) exit} f'
   ```

2. From a fresh clone of the tagged commit, build the workspace and
   run the README's `Try it` flow end to end.

3. The benchmark numbers in the release note record the commit they
   measured, the OS, the filesystem, the dataset, the command, and the
   variance; they never name the maintainer's machine (the
   restore-bench workflow's fixed-runner numbers are the source).

   Done when: the notes text, the smoke, and the provenance are on the
   release PR.

   Notes: ___

### R6 - The umask-002 run (RELEASING.md "The release PR" 6)

1. Once #302's CI job exists, it runs on the release commit and its
   green result is part of the release. Until it exists, this step
   cannot run: write down what that block means for the release.

   Notes: ___

### R7 - The tag, signed - **OWNER** (RELEASING.md "Release steps" 1)

1. The owner signs the annotated tag personally with their SSH key
   (D18); the signature a verifier checks is the owner's, not a CI
   secret's:

   ```sh
   git -c gpg.format=ssh -c user.signingkey="$HOME/.ssh/id_ed25519.pub" \
     tag -s v0.1.0 -m "openscion v0.1.0"
   git push origin v0.1.0
   ```

   **This step needs the owner today.** A second maintainer has
   neither the key nor the ruleset right (R8); write down what you
   did instead (stop here, or watch the owner run it).

   Notes: ___

2. The verification a second maintainer CAN run, after the push:

   ```sh
   git tag -v v0.1.0
   test "$(git ls-remote origin 'refs/tags/v0.1.0^{}' | cut -f1)" \
     = "$(git rev-parse <the release commit>)"
   ```

   Done when: both pass.

   Notes: ___

### R8 - The tag ruleset and the immutable-releases setting - **OWNER** (RELEASING.md "Release steps" 2)

1. The ruleset on `refs/tags/v*` (creation restricted to the owner,
   deletion and updates denied) and the immutable-releases setting are
   repository settings only the owner can change, once, before the
   first tag.

   **This step needs the owner today.** Verify what a second
   maintainer can see instead: the ruleset exists, and a `v*` tag push
   from your account is refused.

   Notes: ___

### R9 - The release run, read by a person (RELEASING.md "Release steps" 2)

1. The release workflow builds every artifact twice per cell, fails on
   one differing byte, attests each artifact and SBOM, verifies the
   attestations, and opens the release immutable with the notes
   rebuilt from the version's `CHANGELOG.md` section. A person does
   not run it; a person reads its run.

   Done when: you have read the run's diff job (`NO DIFFERENCE`) and
   the `verify` job's attestation output, and the release page shows
   the artifacts and the notes.

   Notes: ___

## Gaps: the steps that need the owner today

The v0.8 list ("without one person's account, machine or private
key"), from this drill:

1. **The signed tag** (R7): the owner's personal SSH key, by decision
   D18. A second maintainer cannot tag a release.
2. **The `refs/tags/v*` ruleset** (R8): creation restricted to the
   owner, so tag rights live in one account.
3. **The immutable-releases setting** (R8): a repository setting only
   the owner can change.
4. **The umask-002 run** (R6): the CI job does not exist yet (#302) -
   not an owner gap, a missing-automation gap that today blocks the
   step for anyone.

Everything else - the dry run, the drills, the gates, the release PR,
the notes, the smoke, the provenance, and reading the release run - a
second maintainer's account runs today.

## Outcome

<what the drill saw: the steps that ran as RELEASING.md promises, or
the gap found and its issue link.>
