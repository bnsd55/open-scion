//! A client's connection notices a server that died without a FIN or
//! RST: SIGSTOP freezes a real server with its socket open, and the
//! client's HTTP/2 keepalive ends its call in about the keepalive
//! interval plus its timeout, not at the call's deadline.

#![cfg(unix)]

mod common;

use std::time::Duration;

use common::*;
use rustix::process::{Signal, kill_process};
use scion_server::test_support::store::Store;

use scion_server::test_support::meta::MetaTx as _;

/// A call to a frozen server fails within a minute, not at the 600 s
/// deadline of a call. The frozen server holds the socket open and
/// answers nothing, which is what a kill without a FIN or RST leaves:
/// the process gone, the connection half-dead.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_to_a_frozen_server_ends_within_the_keepalive_window() {
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);
    let mut server = Server::start(&data, &log);

    // The product client, as `scn` builds it: the same deadlines and the
    // same keepalive every command uses.
    let mut client =
        open_scion::client::Ctx::connect(&server.url, None, TOKEN, None, "r".to_owned(), false)
            .await
            .unwrap();

    server.freeze();
    let started = std::time::Instant::now();
    let answer = tokio::time::timeout(Duration::from_secs(60), client.resolve("main")).await;
    let waited = started.elapsed();
    let why = answer
        .unwrap_or_else(|_| panic!("the call never ended, even at 60 s"))
        .expect_err("the call answered, to a server that cannot answer");
    let why = format!("{why:#}");
    assert!(
        why.contains("unavailable") && why.contains("KeepAliveTimedOut"),
        "the failure says the peer is gone, not something else: {why}"
    );
    assert!(
        waited < Duration::from_secs(60),
        "the call took {waited:?}: that is the deadline, not the keepalive"
    );

    // The frozen server wakes, and the test's own drop of it stops it:
    // the client's error must not depend on the server staying frozen.
    kill_process(
        rustix::process::Pid::from_raw(server.pid().cast_signed()).unwrap(),
        Signal::CONT,
    )
    .unwrap();
}

/// The store a stopped server was serving stays readable: the freeze
/// broke no data.
#[tokio::test(flavor = "multi_thread")]
async fn the_store_of_a_frozen_server_is_intact() {
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);
    let mut server = Server::start(&data, &log);
    let _client =
        open_scion::client::Ctx::connect(&server.url, None, TOKEN, None, "r".to_owned(), false)
            .await
            .unwrap();
    server.freeze();
    kill_process(
        rustix::process::Pid::from_raw(server.pid().cast_signed()).unwrap(),
        Signal::CONT,
    )
    .unwrap();
    drop(server);
    let store = Store::open(&data).unwrap();
    assert!(
        store
            .read(|c| {
                let repo = c.repo_id("r")?;
                c.get_ref(repo, "main")
            })
            .unwrap()
            .is_some(),
        "the ref the frozen server served is still there"
    );
}
