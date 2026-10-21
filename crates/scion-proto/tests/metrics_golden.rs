//! The metric surface, as the machine form of stability.md's promise
//! for what `/metrics` serves: every `scion_*` name the registry
//! renders, its `# TYPE`, and its label sets - name, type and labels
//! in one golden, against a registry the test has exercised end to
//! end. The golden keeps the labels (they are the surface) and strips
//! the sample values (they are this run's counts).
//!
//! Updating on purpose: run this test with
//! `SCION_UPDATE_METRICS_GOLDEN=1`, commit the changed
//! `tests/metrics_v1.golden`, and name the added surface in a changes/
//! fragment. A removed name or a changed type is a break, never a
//! re-pin; the exercise below is what puts a conditional series (a
//! histogram that renders only once it counted) into the golden.

use std::time::{Duration, Instant};

use scion_proto::runtime::metrics::Metrics;

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/metrics_v1.golden");

/// The services' RPC paths, from the calls the wire protocol serves:
/// one request each puts every RPC's name, latency histogram and byte
/// counters into the render.
/// The services' RPC paths, read from the proto at test time: every
/// `service X { rpc Y }` of proto/scion.proto becomes
/// `/scion.v1.X/Y`. A series renders only after a request, so a new
/// RPC either fails `rpc(path)` here or enters the golden - the hand-
/// copied list this replaces would have let a new RPC skip both.
fn rpc_paths() -> Vec<String> {
    let proto = include_str!("../../../proto/scion.proto");
    let mut paths = Vec::new();
    let mut service = "";
    for line in proto.lines() {
        let line = line.trim();
        if let Some(name) = line.strip_prefix("service ") {
            service = name.trim_end_matches(" {");
        } else if let Some(name) = line.strip_prefix("rpc ") {
            let name = name.split('(').next().unwrap_or(name);
            paths.push(format!("/scion.v1.{service}/{}", name.trim()));
        }
    }
    paths
}

/// Exercised end to end: one request per RPC (one also cancelled, one
/// failed with a non-ok code), one land per outcome the code names,
/// and one reading of every gauge and histogram the registry holds.
fn exercised() -> Metrics {
    let m = Metrics::default();
    for (i, path) in rpc_paths().iter().enumerate() {
        let rpc = m
            .rpc(path)
            .unwrap_or_else(|| panic!("the registry knows {path}"));
        m.request(rpc, 0, Instant::now(), 7, 9);
        if i % 4 == 0 {
            m.cancelled(rpc, Instant::now(), 1, 2);
        }
        if i % 8 == 1 {
            m.request(rpc, 3, Instant::now(), 3, 1);
        }
    }
    for outcome in ["ok", "conflict", "aborted", "stale"] {
        m.land(outcome);
    }
    m.land_replan();
    m.cache_hit();
    m.cache_miss();
    m.blob_sync(1_500);
    m.collector_key_wait(2_500);
    m.data_free_bytes(4_242);
    m.derive_walk_started();
    m.derive_walk_wait_started();
    m.derive_walk_wait_ended();
    m.derive_walk_ended();
    m.view_tree_walked(3);
    m.pg_pool_wait(Duration::from_micros(900));
    m.pg_readers_busy(2);
    m.pg_tx_age(Duration::from_millis(40));
    m
}

/// The sample values go; the names, types, labels and label values
/// stay: they are the surface.
fn surface(render: &str) -> String {
    let mut out = String::new();
    for line in render.lines() {
        if line.starts_with('#') {
            out.push_str(line);
            out.push('\n');
            continue;
        }
        let series = line.split(' ').next().unwrap_or(line);
        out.push_str(series);
        out.push('\n');
    }
    out
}

#[test]
fn the_metric_surface_is_the_golden() {
    let render = exercised().render();
    assert!(render.contains("# TYPE scion_request_seconds histogram"));
    assert!(render.contains("scion_requests_total{rpc=\"Scion/PutBlob\""));
    assert!(render.contains("scion_data_free_bytes"));
    let text = surface(&render);
    if std::env::var_os("SCION_UPDATE_METRICS_GOLDEN").is_some() {
        std::fs::write(GOLDEN, &text).unwrap();
    }
    let golden = std::fs::read_to_string(GOLDEN).unwrap();
    assert!(
        !golden.is_empty(),
        "the golden must name the surface it freezes"
    );
    assert_eq!(
        text, golden,
        "the metric surface changed: a removed name or a changed type is a \
         break, never a re-pin; an added name re-pins on purpose with \
         SCION_UPDATE_METRICS_GOLDEN=1 and a changes/ fragment naming it"
    );
}
