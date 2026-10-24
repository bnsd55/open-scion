//! A client's connection notices a server that answers nothing with the
//! socket open: a relay stands between the client and the server, and
//! once the server has answered a call, the relay goes silent without
//! closing anything. That is the condition a kill without a FIN or RST
//! leaves: the process gone, the connection half-dead. The client's
//! HTTP/2 keepalive ends its call in about the keepalive interval plus
//! its timeout, not at the call's deadline.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::{REPO, TOKEN, changeset, files, land};
use open_scion::client::Ctx;
use scion_proto::runtime::listen::{CLIENT_KEEPALIVE_OVERRIDE, Limits};
use scion_server::test_support::service;
use scion_server::{Backend, Keys, Quotas};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Moves bytes from `reader` to `writer` while `silent` is clear. While
/// the flag is set it reads nothing, so the bytes of a half-dead
/// connection pile up in the kernel instead of arriving.
async fn forward<R, W>(mut reader: R, mut writer: W, silent: Arc<AtomicBool>) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = [0u8; 16 * 1024];
    loop {
        if silent.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(2)).await;
            continue;
        }
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        writer.write_all(&buf[..n]).await?;
        writer.flush().await?;
    }
}

/// The server of `store`, with a relay in front of it that the test can
/// silence. Returns the relay's URL (what a client connects to), the
/// flag that silences it, and the relay's own runtime's guard.
async fn serve_behind_a_relay(store: &Backend) -> (String, Arc<AtomicBool>, tempfile::TempDir) {
    let dirs = tempfile::tempdir().unwrap();
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = server.local_addr().unwrap();
    let tickets = Arc::new(Keys::generate());
    tokio::spawn(service::serve(
        store.clone(),
        server,
        TOKEN,
        service::ServeOptions {
            data: dirs.path().to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::default(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(tickets),
        std::future::pending(),
    ));

    let front = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", front.local_addr().unwrap());
    let silent = Arc::new(AtomicBool::new(false));
    let gate = silent.clone();
    tokio::spawn(async move {
        loop {
            let Ok((down, _)) = front.accept().await else {
                return;
            };
            let Ok(up) = TcpStream::connect(upstream).await else {
                return;
            };
            // Both directions go through the same flag, so a silent
            // relay is a full black hole: the client sends, the server
            // answers, and nothing arrives at either end.
            let flag = gate.clone();
            tokio::spawn(async move {
                let (down_rx, down_tx) = tokio::io::split(down);
                let (up_rx, up_tx) = tokio::io::split(up);
                let down = forward(up_rx, down_tx, flag.clone());
                let up = forward(down_rx, up_tx, flag);
                let _ = tokio::join!(down, up);
            });
        }
    });
    (url, silent, dirs)
}

/// A call to a server that answers nothing ends within the keepalive
/// window, not at the 600 s deadline of a call.
#[tokio::test]
async fn a_call_to_a_silent_server_ends_within_the_keepalive_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let store = Backend::open_test(&data).unwrap();
    let (files, _) = files(&store);
    land(&store, &changeset(None, &files));

    let (url, silent, _server_dirs) = serve_behind_a_relay(&store).await;

    // The product client, as `scn` builds it: the same deadlines and the
    // same keepalive every command uses. The keepalive is the seam's:
    // the channel was already connected before the silence, so a ping
    // goes to a peer that answers nothing, and the shortened keepalive
    // (1 s, 1 s) proves the same dead-peer failure in seconds.
    CLIENT_KEEPALIVE_OVERRIDE
        .with(|keepalive| keepalive.set(Some((Duration::from_secs(1), Duration::from_secs(1)))));
    let mut client = Ctx::connect(&url, None, TOKEN, None, REPO.to_owned(), false)
        .await
        .unwrap();

    silent.store(true, Ordering::Relaxed);
    let started = std::time::Instant::now();
    let answer = tokio::time::timeout(Duration::from_secs(60), client.resolve("main")).await;
    let waited = started.elapsed();
    let why = answer
        .unwrap_or_else(|_| panic!("the call never ended, even at 60 s"))
        .expect_err("the call answered, to a peer that cannot answer");
    let why = format!("{why:#}");
    assert!(
        why.contains("unavailable") && why.contains("KeepAliveTimedOut"),
        "the failure says the peer is gone, not something else: {why}"
    );
    assert!(
        waited < Duration::from_secs(30),
        "the call took {waited:?}: that is the product's keepalive, so the \
         seam did not shorten it"
    );

    // The relay wakes, and the test's own drop of it stops it: the
    // client's error must not depend on the server staying silent.
    silent.store(false, Ordering::Relaxed);
    CLIENT_KEEPALIVE_OVERRIDE.with(|keepalive| keepalive.set(None));
}
