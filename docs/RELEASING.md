# Releasing

How to cut a release of open-scion. The steps below made v0.1.0; follow
them for every release. Nothing here publishes or tags by itself: each
step is run by a person.

## Before a release

- CI is green on `main`: `cargo fmt --check`, `cargo clippy --workspace
  --all-targets -- -D warnings`, `cargo test --workspace`, the MSRV check
  and the fuzz smoke test.
- The milestone for the version has no open issues.
- The crate versions in `Cargo.toml` and the versions of the internal
  dependencies (`scion-core`, `scion-proto`, `scion-server`, `scion-tier`)
  are the version that is about to ship. They live in
  `[workspace.package]` and in `[workspace.dependencies]`: a release bump
  changes one `version` and four dependency lines.
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
3. **Check the packages.** For each crate, in dependency order:

   ```sh
   cargo package -p scion-core --allow-dirty --no-verify
   ```

   It must end with `Packaged`. A crate that fails here cannot publish.
   `cargo package --no-verify` still uploads nothing.
4. **Tag.** Cut an annotated tag from the commit that the release
   notes describe:

   ```sh
   git tag -a v0.1.0 -m "open-scion v0.1.0"
   git push origin v0.1.0
   ```

5. **GitHub release.** Open a release for the tag with the notes taken
   from the `CHANGELOG.md` section of the version, and the measured
   numbers from `docs/benchmarks.md` (method and machine class, not
   absolute claims).
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
  version, and build a fresh checkout that depends on `scn` from crates.io.
- Close the milestone if it is still open.

## Notes

- License text: the repository carries `LICENSE-MIT` and `LICENSE-APACHE`
  at its root. A crate manifest can only list license files inside its own
  directory, so the published crates carry the SPDX `license` field, and
  the license text ships with the GitHub release and the repository.
- Windows is not supported; the release notes say so.
- Never publish or tag from a fork or a dirty worktree; the tag and the
  crates.io upload both record the commit that built them.
