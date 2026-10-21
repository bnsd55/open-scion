//! A test client's ceilings hold when the server cannot answer: a
//! stopped server ends a call at the channel's ceiling, with a named
//! error, in seconds — not at the job's cap. This is the crash suite's
//! hang, made loud.

mod common;

use common::resolve_ref;
use std::process::Command;
use std::time::{Duration, Instant};

/// SIGCONT for the stopped server, when the test ends either way.
struct Lift(u32);
impl Drop for Lift {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-CONT", &self.0.to_string()])
            .status();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_to_a_stopped_server_ends_at_the_ceiling_with_a_name() {
    let dir = tempfile::tempdir().unwrap();
    let server = common::Server::start(&dir.path().join("data"), &dir.path().join("log"));
    // A ceiling of the test's own choosing: 2 s, so the proof is quick.
    let ceiling = Duration::from_secs(2);
    let (_scion, mut objects) = common::connect_within(&server.url, ceiling).await.unwrap();

    // Freeze the server: SIGSTOP answers nothing, keeps the store, and
    // lifts the moment the guard drops, so the drop of the server can
    // still drain it. The call runs against a process that will never
    // reply — exactly the shape that hung the crash suite for 25 min.
    let pid = server.pid();
    Command::new("kill")
        .args(["-STOP", &pid.to_string()])
        .status()
        .expect("SIGSTOP runs");
    let _lift = Lift(pid);

    let started = Instant::now();
    let error = resolve_ref(&mut objects, "main")
        .await
        .expect_err("a call to a stopped server fails at the ceiling");
    let waited = started.elapsed();

    // tonic surfaces the channel's own timeout as Cancelled
    // ("Timeout expired"), not DeadlineExceeded: the ceiling is the
    // transport's, not a per-request deadline the server saw.
    assert_eq!(error.code(), tonic::Code::Cancelled, "{error:?}");
    assert!(
        waited >= ceiling,
        "the ceiling fires no earlier than it is set: {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(20),
        "the ceiling fires in seconds, not minutes: {waited:?}"
    );
}
