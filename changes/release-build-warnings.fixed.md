A plain cargo build --release printed about 15 warnings in scion-server:
unused imports, an unused batch index, and dead test helpers. The users
of every one of them sit behind cfg(any(test, feature = "test-hooks"))
while the items themselves did not, and CI never saw it, because the
workspace clippy with --all-targets unifies the dev-dependency that
turns the feature on. Each item now carries the same cfg as its users,
with no allow attribute; the release build of the SyncStep payloads
rides a unit-typed field instead. The fmt + clippy job gains
cargo clippy --workspace --lib --bins -- -D warnings, which builds the
libraries and binaries without dev-dependencies, so the next ungated
test-only item fails CI instead of a release build's stderr.
