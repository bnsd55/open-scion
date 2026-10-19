//! The operator metrics, shared with a tier: the registry and its HTTP
//! export live in [`scion_proto::runtime::metrics`].

pub use scion_proto::runtime::metrics::{
    Metrics, MetricsLayer, land_error_label, loopback_or_allow, serve, shutdown_signal,
};
