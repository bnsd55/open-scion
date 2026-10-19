//! Bearer-token check on every RPC. The cache tiers use the same check, so it
//! lives in `scion-proto`.

pub use scion_proto::runtime::auth::Caller;
// The name rides for the tests' builds; the crate reaches Auth through
// `scion-proto` directly.
#[cfg(any(test, feature = "test-hooks"))]
pub use scion_proto::runtime::auth::Auth;
