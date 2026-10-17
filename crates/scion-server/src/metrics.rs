//! The operator metrics, shared with a tier: the registry and its HTTP
//! export live in [`scion_proto::metrics`].

pub use scion_proto::metrics::{Metrics, MetricsLayer, loopback_or_allow, serve, shutdown_signal};
