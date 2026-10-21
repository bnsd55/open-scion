//! `db.rs`'s test hooks, as its own child module: `db.rs` declares
//! this file with `#[path]`, so everything here keeps the privacy of
//! its module unchanged, and the file sits under `src/test_support/`
//! for the coverage exclusion.

#![forbid(unsafe_code)]

use super::Error;

/// The error of a graph walk that reached its bound.
#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn too_deep(max_visits: usize) -> Error {
    Error::ResourceExhausted(format!(
        "the history is too deep: the answer needs more than {max_visits} changesets"
    ))
}
