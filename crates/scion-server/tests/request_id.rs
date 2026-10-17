//! Request-ID suite: the request ID that a call sends rides in the call's
//! span in the server's log, and a header that does not fit is dropped
//! whole, so nothing of it is logged.
#![cfg(unix)]

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

use scion_proto::v1::ResolveRefRequest;
use scion_proto::v1::scion_client::ScionClient;
use tempfile::TempDir;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

/// A running `scion-server` whose log lands in a file the test reads.
struct Server {
    child: Child,
    _data: TempDir,
    log: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts the real binary on a fresh data directory.
fn spawn() -> Server {
    let data = tempfile::tempdir().unwrap();
    let dir = data.path().join("log");
    fs::create_dir(&dir).unwrap();
    let log = dir.join("server.log");
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data.path())
        .args(["serve", "--token", "t", "--listen", "127.0.0.1:0"])
        // Plain log lines, so the test can read the address back.
        .env("NO_COLOR", "1")
        .stdout(log_file.try_clone().unwrap())
        .stderr(log_file)
        .spawn()
        .unwrap();
    Server {
        child,
        _data: data,
        log,
    }
}

/// The `listen=` address of the newest `serving` line in the log.
fn bound_addr(log: &Path) -> Option<SocketAddr> {
    let text = fs::read_to_string(log).ok()?;
    let line = text.lines().rev().find(|line| line.contains("serving"))?;
    line.split("listen=")
        .nth(1)?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// Waits for the server to print its address.
fn wait_url(server: &Server) -> String {
    // The bound address in the log is the state to wait for: the server
    // started and listens. The ceiling is generous, so a loaded machine
    // still reaches it.
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if let Some(addr) = bound_addr(&server.log) {
            return format!("http://{addr}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("the server did not start");
}

/// A client that sends a fixed request ID on every call, and no token, so
/// the call is refused: the refusal is a log line inside the call's span,
/// which names the request ID.
#[derive(Clone)]
struct Id(&'static str);

impl Interceptor for Id {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("x-request-id", self.0.parse().unwrap());
        Ok(request)
    }
}

async fn client(url: &str, id: &'static str) -> ScionClient<InterceptedService<Channel, Id>> {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    ScionClient::with_interceptor(channel, Id(id))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_calls_request_id_names_its_span_in_the_servers_log() {
    let server = spawn();
    let url = wait_url(&server);

    let mut client = client(&url, "e2e-req-1").await;
    let reply = client
        .resolve_ref(ResolveRefRequest {
            repo: "r".into(),
            name: "main".into(),
        })
        .await;
    assert_eq!(reply.err().unwrap().code(), tonic::Code::Unauthenticated);

    let text = fs::read_to_string(&server.log).unwrap();
    assert!(
        text.lines()
            .any(|line| line.contains("request_id=e2e-req-1")
                && line.contains("the call is unauthenticated")),
        "the refusal line names the request ID:\n{text}"
    );
}

/// A header that does not fit is dropped whole: its bytes never reach the
/// log, and the call still goes through the span machinery.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_id_that_does_not_fit_is_never_logged() {
    let server = spawn();
    let url = wait_url(&server);

    // 70 bytes plus a space: too long, and not visible ASCII.
    let junk = format!("{} ", "j".repeat(70));
    let id: &'static str = Box::leak(junk.into_boxed_str());
    let mut client = client(&url, id).await;
    let reply = client
        .resolve_ref(ResolveRefRequest {
            repo: "r".into(),
            name: "main".into(),
        })
        .await;
    assert_eq!(reply.err().unwrap().code(), tonic::Code::Unauthenticated);

    let text = fs::read_to_string(&server.log).unwrap();
    assert!(
        !text.contains(&"j".repeat(70)),
        "no byte of the refused header is logged:\n{text}"
    );
    // The span still closed: the refusal logged.
    assert!(text.contains("the call is unauthenticated"));
}
