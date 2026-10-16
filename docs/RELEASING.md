# Releasing

How to cut a release of open-scion. The steps are the same for every
release. Nothing here publishes or tags by itself: each
step is run by a person.

## Before a release

- CI is green on `main`: `cargo fmt --check`, `cargo clippy --workspace
  --all-targets -- -D warnings`, `cargo test --workspace`, the MSRV check
  and the fuzz smoke test.
- The milestone for the version has no open issues.
- Every crate carries the metadata that crates.io wants: a description,
  the license (`MIT OR Apache-2.0`), the repository and homepage, keywords
  and categories, and the workspace `rust-version`. `cargo package -p
  <crate> --list` for each crate fails loudly when a field is missing.

## Release steps

1. **Set the date in `CHANGELOG.md`.** Rename the `## [v0.1.0] -
   unreleased` section to `## [v0.1.0] - YYYY-MM-DD` with the release day,
   and add a fresh empty `## [Unreleased]` above it. The entry must say
   what the release adds and changes in plain language, grouped by area,
   with the issue numbers that stay true after the release.
2. **Check the version.** All five crates share one version through
   `[workspace.package]` in the root `Cargo.toml`. A release bumps one
   number there and the four `version = "..."` entries of the internal
   dependencies in `[workspace.dependencies]` to the same value.
3. **Check the packages.** From a clean worktree (no uncommitted
   changes), package every crate:

   ```sh
   cargo package --workspace --no-verify
   cargo package -p scion-core
   ```

   The first command packages all five crates; it must end with
   `Packaged` five times. `cargo package` builds nothing, so it uploads
   nothing. The second verifies `scion-core` by building it, which the
   other four cannot do before they are published: their build resolves
   the unpublished internal dependencies from crates.io, and
   `scion-proto`'s build script needs `proto/scion.proto`, which sits
   outside the crate (the packaged crate cannot build; the generated
   code is built in place instead). A release that follows this list
   re-runs the full workspace test suite (step 1), so the verification
   gap is the resolver's, not the code's.
4. **Tag.** Cut an annotated tag from the commit that the release
   notes describe:

   ```sh
   git tag -a v0.1.0 -m "open-scion v0.1.0"
   git push origin v0.1.0
   ```

5. **GitHub release.** Open a release for the tag with the notes taken
   from the `CHANGELOG.md` section of the version, and the measured
   numbers from `docs/benchmarks.md`. The numbers are ratios measured on
   one machine; a release note never names the machine.
6. **Publish to crates.io**, one crate at a time, innermost first, and
   wait until each one is indexed before the next:

   ```sh
   cargo login   # once, with a crates.io API token
   cargo publish -p scion-core
   cargo publish -p scion-proto
   cargo publish -p scion-server
   cargo publish -p scion-tier
   cargo publish -p scn
   ```

   The order matters: `scion-server` depends on `scion-core` and
   `scion-proto`, `scion-tier` on `scion-core` and `scion-proto`, and
   `scn` on all four. Their `Cargo.toml` entries carry both a `version`
   and a `path`, so the workspace builds from the local paths while
   crates.io resolves the version of each published crate.

## After

- Watch the crates.io pages of the five crates until each shows the
  version, then install the client from crates.io and run it:

  ```sh
  cargo install scn --version 0.1.0 --root /tmp/scion-install
  /tmp/scion-install/bin/scn --version
  ```

  Remove `/tmp/scion-install` afterwards.
- Close the milestone if it is still open.

## Notes

- Each crate ships both license files: `crates/<crate>/LICENSE-MIT` and
  `LICENSE-APACHE` are links to the files at the repository root, and
  `cargo package --list` shows them for every crate.
- Windows is not supported; the release notes say so.
- Never publish or tag from a fork or a dirty worktree; the tag and the
  crates.io upload both record the commit that built them.
