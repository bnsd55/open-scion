//! Bearer-token check on every RPC. The cache tiers use the same check, so it
//! lives in `scion-proto`.

// The test seam re-exports this (test_support.auth in lib.rs), and
// unreachable_pub does not see through a re-export.
#[allow(unreachable_pub)]
pub use scion_proto::runtime::auth::Caller;
