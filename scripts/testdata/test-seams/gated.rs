// A fixture of scripts/check-test-seams.sh --self-test that must PASS:
// the gate sits on the line after the doc(hidden) attribute, the shape
// every test seam of the server keeps.
pub struct Gated;

/// A seam with its gate, the shape every doc(hidden) item keeps.
#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
pub fn gated() {}
