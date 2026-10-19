# Contributing to open-scion

Thank you for your interest. open-scion is at an early stage and the design
still moves, so the best first step is an issue: describe the problem or the
idea, and agree on the approach before writing a large change.

## Ground rules

- Be kind and constructive. This project follows the
  [Code of Conduct](CODE_OF_CONDUCT.md).
- Report security problems privately, as described in [SECURITY.md](SECURITY.md).
- Keep changes focused: one topic per pull request.

## Development setup

You need Rust 1.88 or newer, `protoc` (the Protocol Buffers compiler, for the
gRPC code) and `git` (the import tests build small git repositories). For fuzzing you also need a nightly toolchain and
`cargo-fuzz`.

```sh
git clone https://github.com/bnsd55/open-scion
cd open-scion
cargo test --workspace
```

## Before you open a pull request

Every pull request must pass the same checks as CI. Run these three
locally before you push:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI runs them again, and adds: `cargo deny check` (supply chain), the
power-loss suite, the grpcurl conformance check, the docker demo, the
publish check, the MSRV build on 1.88, the fuzz smoke test, and the same
tests on macOS. Run one test file or one test directly:

```sh
cargo test -p open-scion-server --test write
cargo test -p open-scion --test query ls_shows
```

## The changelog

A pull request does not edit `CHANGELOG.md`. It adds a fragment,
`changes/<slug>.<section>.md`, one per section it belongs to:

- `<section>` is one of `added`, `changed`, `fixed`, `security`.
- `<slug>` is the pull request's number when you know it, else a short
  kebab name (`rename-budget`).
- The file holds the row's text as it will read: wrapped at 79 columns,
  continuation lines carrying the row's two-space indent, no leading
  `- `. Write it so it stands on its own: what changed, for whom, and
  the pull request's number in parentheses.

At a release, `scripts/merge-changelog.sh <release>` turns the
fragments into the changelog's rows and deletes them. CI refuses a
pull request that edits `CHANGELOG.md` itself; only a `release/`
branch does.

## Code standards

- **Idiomatic and simple.** Prefer the smallest change that solves the real
  problem. No abstraction without a second use.
- **Documented.** Every public item has a doc comment. Functions that return
  `Result` have an `# Errors` section.
- **No hidden panics.** Avoid `unwrap` and `expect` outside tests, unless the
  message states why the value cannot be absent.
- **Honest shortcuts.** A deliberate simplification gets a `SHORTCUT:` comment
  that names its limit and its upgrade path.
- **Tests that can fail.** A test must be able to fail for a real reason. For
  logic changes, show that the test fails without the fix.
- **The format is frozen.** `docs/format-v1.md` defines the bytes that object
  IDs hash. A change there changes every ID and needs a new format version and
  a discussion first.
- `unsafe` code is not allowed (`unsafe_code = "forbid"`).

## Commit messages

Use a short subject in the imperative mood, prefixed with the crates you
changed, then a body that explains what changed and why:

```
scion-server: re-importing a tip the ref already contains is a no-op

A ref that holds lands on top of an earlier import already contains the
git tip, so a repeated import leaves it alone and succeeds.
```

## License

By contributing, you agree that your contributions are dual licensed under the
[Apache License, Version 2.0](LICENSE-APACHE) and the [MIT license](LICENSE-MIT),
as described in the README, without any additional terms or conditions.
