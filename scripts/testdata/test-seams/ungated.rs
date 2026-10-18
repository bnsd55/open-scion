// A fixture of scripts/check-test-seams.sh --self-test that must FAIL:
// two doc(hidden) items on adjacent lines, the second without its gate.
// The first must be reported because the second line consumed one of
// its two window lines without a gate, and the second at the end.
#[doc(hidden)]
#[doc(hidden)]
pub fn ungated() {}
