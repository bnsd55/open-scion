//! Bearer-token check on every RPC. The cache tiers use the same check, so it
//! lives in `scion-proto`.

pub use scion_proto::runtime::auth::Caller;
