The workspace now denies unreachable `pub` items: `unreachable_pub = "warn"`
lands in `[workspace.lints.rust]`, and every crate's `[lints]` inherits it.

- Internal items that only the crate and its binaries use drop from `pub` to
  `pub(crate)`: the similar-object limits and helpers, the object/name/graph
  internals, the derived changeset helpers, the access constants, the metrics
  re-exports, and the test common modules' shared helpers.
- Items the test seam re-exports (`test_support`) keep `pub` with an
  `#[allow(unreachable_pub)]` and a reason: the lint does not see through a
  re-export, and their external reachability is the seam itself.
- `Metrics` is re-exported from `scion_proto` directly in lib.rs, since the
  `metrics` module is now crate-private.
