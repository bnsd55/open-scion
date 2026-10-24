//! The reply-memory budget's mutation levers are permanent red proofs,
//! the #582 pattern: each lever of `SCION_TEST_REPLY_MUTATION` is the
//! planted bug the guard must catch, and each test runs the guard's
//! scenario against a child server with the lever set and without it.
//! The child, not the test process, carries the env, so the levers of
//! one test cannot leak into another.

use std::io::BufRead as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use prost::Message as _;
use scion_core::{Changeset, FileState, IdentityTime, RepoPath};
use scion_server::test_support::meta::MetaTx;
use scion_server::test_support::store::Store;
use tokio::net::TcpStream;

mod common;

const REPO: &str = "main";
const TOKEN: &str = "secret";
/// Names in the head changeset: a page of the diff is over the h2 send
/// buffer, so an unread reply parks in the server and holds its share.
const NAMES: usize = 40_000;
/// Parked replies before the count must have refused: the child runs
/// a 600 KB budget (`spawn_server` sets `SCION_TEST_REPLY_BUDGET`), and
/// one parked reply's hold is the HTTP/2 window it fills, a fraction
/// of that budget. Five parked replies crossed it in the runs; a run
/// that parks this many more without a refusal never will.
const PARK_CAP: usize = 60;

fn author() -> IdentityTime {
    IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 1_700_000_000, 0)
        .expect("identity fits the format")
}

/// Writes the repo the scenario diffs into `data`: a root and a head
/// whose root-to-head diff names `NAMES` long entries, so one reply is
/// far over the h2 send buffer.
fn seed(data: &Path) -> (scion_core::ChangesetId, scion_core::ChangesetId) {
    let store = Store::open(data).unwrap();
    let blob = store.blobs().put(b"x".to_vec().as_slice()).unwrap();
    let root = Changeset::new(
        vec![],
        author(),
        None,
        b"root\n".to_vec(),
        [(
            RepoPath::new(b"seed".as_slice()).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    let root_id = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            c.insert_changeset(repo, &root)
        })
        .unwrap();
    let head = Changeset::new(
        vec![root_id],
        author(),
        None,
        b"big\n".to_vec(),
        (0..NAMES)
            .map(|i| {
                // Long names: a page of the diff is over the h2 send
                // buffer, so an unread reply parks in the server.
                let name = format!("entry-{i:06}.{}", "n".repeat(120));
                (
                    RepoPath::new(name.as_bytes()).unwrap(),
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )
            })
            .collect(),
    )
    .expect("changeset fits the format");
    let head_id = store
        .write(|c| {
            let repo = c.repo_id(REPO).expect("the seeded repo is there");
            c.insert_changeset(repo, &head)
        })
        .unwrap();
    drop(store);
    (root_id, head_id)
}

/// Writes `text` to a 0600 token file under `dir`.
fn token_file(dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let file = dir.join("token");
    std::fs::write(&file, format!("{TOKEN}\n")).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    file
}

/// The child server of one scenario: `data` served, `lever` set in the
/// child's env alone (or removed), and the address it bound parsed
/// from its log. The child runs a small reply budget (the same 600 KB
/// the in-process guard test uses): the compiled 128 MiB cannot be
/// bound by the 256 streams one connection may open, whose parked
/// replies hold about 65 KB each.
fn spawn_server(data: &Path, token: &Path, lever: Option<&str>) -> (Child, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    command
        .arg("--data")
        .arg(data)
        .args([
            "serve",
            "--token-file",
            token.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
        ])
        .env_remove("SCION_TOKEN")
        .env_remove("SCION_TEST_REPLY_MUTATION")
        .env("SCION_TEST_REPLY_BUDGET", "600000")
        .stderr(Stdio::piped());
    if let Some(lever) = lever {
        command.env("SCION_TEST_REPLY_MUTATION", lever);
    }
    let mut child = command.spawn().expect("the server binary runs");
    let stderr = child.stderr.take().expect("stderr is piped");
    let mut reader = std::io::BufReader::new(stderr);
    let mut line = String::new();
    loop {
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => panic!("the server ended before it served"),
            Ok(_) => {
                if line.contains("serving") && !line.contains("metrics") {
                    break;
                }
                line.clear();
            }
        }
    }
    let listen = line
        .split("listen=")
        .nth(1)
        .unwrap_or_else(|| panic!("the serving line names the bound address: {line}"));
    let addr = listen
        .split_whitespace()
        .next()
        .unwrap_or_else(|| panic!("the serving line names the bound address: {line}"))
        .trim()
        .to_owned();
    // The pipe stays open for the child's life, drained by a thread of
    // its own: a closed stderr would end the child on its first
    // warning (a budget refusal logs one), and the server's death looks
    // like a connection reset.
    std::thread::spawn(move || {
        let mut rest = String::new();
        loop {
            match reader.read_line(&mut rest) {
                Ok(0) | Err(_) => break,
                Ok(_) => rest.clear(),
            }
        }
    });
    (child, addr)
}

/// The gRPC request body of `request`: the five-byte prefix, then the
/// encoded message.
fn grpc_body(request: &scion_proto::v1::DiffRequest) -> bytes::Bytes {
    let message = request.encode_to_vec();
    let mut body = vec![0_u8; 5];
    body[0] = 0; // not compressed
    let [a, b, c, d] = (u32::try_from(message.len()).expect("the message fits u32")).to_be_bytes();
    body[1..5].copy_from_slice(&[a, b, c, d]);
    body.extend_from_slice(&message);
    body.into()
}

/// Opens one unary `Diff` call on a raw HTTP/2 connection, waits for
/// its reply headers, and returns the reply's grpc-status and its
/// body, unread.
async fn open_unread(
    h2: &mut h2::client::SendRequest<bytes::Bytes>,
    request: &scion_proto::v1::DiffRequest,
) -> (Option<String>, h2::RecvStream, h2::SendStream<bytes::Bytes>) {
    let built = http::Request::builder()
        .method(http::Method::POST)
        .uri("http://x/scion.v1.Scion/Diff")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(())
        .unwrap();
    let body = grpc_body(request);
    let (response, mut send) = h2.send_request(built, false).unwrap();
    send.send_data(body, true).expect("the request body sends");
    let reply = response.await.expect("the reply's headers arrive");
    let status = reply
        .headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok().map(str::to_owned));
    (status, reply.into_body(), send)
}

/// Reads a reply body to its trailers, releasing the flow-control
/// window of every data frame, and returns the grpc-status it names.
async fn status_of(mut body: h2::RecvStream) -> Option<String> {
    while let Some(data) = body.data().await {
        let Ok(data) = data else {
            // The stream was reset: no trailers ride a reset.
            return None;
        };
        body.flow_control()
            .release_capacity(data.len())
            .expect("the window releases");
    }
    let trailers = body.trailers().await.expect("the reply ends");
    trailers.and_then(|trailers| {
        trailers
            .get("grpc-status")
            .and_then(|v| v.to_str().ok().map(str::to_owned))
    })
}

/// The scenario both lever tests run: a child server, a client that
/// parks unread replies until the budget refuses, then a reset of
/// every parked reply. Returns whether a fresh call is served whole
/// after the resets.
async fn fresh_call_after_resets(lever: Option<&str>) -> bool {
    let data = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let token = token_file(secrets.path());
    let (root_id, head_id) = seed(data.path());
    let (mut child, addr) = spawn_server(data.path(), &token, lever);
    let request = scion_proto::v1::DiffRequest {
        repo: REPO.into(),
        old_changeset_id: root_id.as_bytes().to_vec(),
        new_changeset_id: head_id.as_bytes().to_vec(),
        path: Vec::new(),
        after: Vec::new(),
    };
    let tcp = TcpStream::connect(&addr).await.unwrap();
    let (mut h2, connection) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Open calls until one is refused: the budget binds.
    let mut bodies = Vec::new();
    let refused = loop {
        let (status, body, send) = open_unread(&mut h2, &request).await;
        match status {
            Some(code) if code == "8" => break Some(()),
            Some(code) => panic!("unexpected refusal: grpc-status {code}"),
            None => bodies.push((body, send)),
        }
        assert!(bodies.len() < PARK_CAP, "the budget never bound");
    };
    let _ = refused;

    // Reset every held reply, the way a client that gives up does.
    for (_body, mut send) in bodies {
        send.send_reset(h2::Reason::CANCEL);
    }

    // A fresh call, after the resets landed: served whole, or refused
    // because the budget never came back.
    let mut served = None;
    for _ in 0..200 {
        match tokio::time::timeout(
            std::time::Duration::from_secs(2),
            open_unread(&mut h2, &request),
        )
        .await
        {
            Ok((status, body, _send)) => match status.as_deref() {
                Some("0") | None => {
                    let read = status_of(body).await;
                    assert_eq!(read.as_deref(), Some("0"), "the reply is whole");
                    served = Some(());
                    break;
                }
                _ => {
                    // Still spent: the resets have not landed yet.
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            },
            Err(elapsed) => panic!("a fresh call failed: the server stalled: {elapsed}"),
        }
    }
    let served = served.is_some();
    let _ = child.kill();
    let _ = child.wait();
    served
}

/// The guard's promise: after the parked replies are reset, a fresh
/// call is served whole. On a real server, no lever.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_resets_its_unread_replies_frees_the_budget() {
    assert!(
        fresh_call_after_resets(None).await,
        "the budget did not free after the resets"
    );
}

/// The lever, run for good: `held_forever` takes the budget and never
/// gives it back, so the same scenario ends refused. If a fresh call
/// is served anyway, the give-back is unconditional - the lever proves
/// nothing, and the guard cannot tell a leak from a free.
#[tokio::test(flavor = "multi_thread")]
async fn the_give_back_still_catches_a_budget_that_is_never_freed() {
    assert!(
        !fresh_call_after_resets(Some("held_forever")).await,
        "the fresh call was served although the budget was taken for good: \
         the give-back test cannot tell a leak from a free"
    );
}

/// The other lever, run for good: `uncounted` parks replies the budget
/// never hears of, so the refusal that binds at a dozen parked replies
/// never comes. If it comes anyway, the count does not depend on the
/// lever - the count test proves nothing.
#[tokio::test(flavor = "multi_thread")]
async fn the_count_still_catches_a_reply_that_is_never_counted() {
    let data = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let token = token_file(secrets.path());
    let (root_id, head_id) = seed(data.path());
    let (mut child, addr) = spawn_server(data.path(), &token, Some("uncounted"));
    let request = scion_proto::v1::DiffRequest {
        repo: REPO.into(),
        old_changeset_id: root_id.as_bytes().to_vec(),
        new_changeset_id: head_id.as_bytes().to_vec(),
        path: Vec::new(),
        after: Vec::new(),
    };
    let tcp = TcpStream::connect(&addr).await.unwrap();
    let (mut h2, connection) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Park `PARK_CAP` unread replies: none may be refused, because the
    // budget hears of none of them. Every body stays open: dropping it
    // resets the stream and frees the reply.
    let mut refused = None;
    let mut held = Vec::new();
    for i in 0..PARK_CAP {
        let (status, body, send) = open_unread(&mut h2, &request).await;
        if let Some(code) = status {
            refused = Some((i, code));
            break;
        }
        held.push((body, send));
    }
    let _ = held;
    let _ = child.kill();
    let _ = child.wait();
    assert_eq!(
        refused, None,
        "the budget refused although the reply was never counted: \
         the count does not depend on the lever"
    );
}

/// The count lever's control, on a real server: the same parking run
/// must refuse, which is the proof the holds are counted.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_parks_unread_replies_is_refused() {
    let data = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let token = token_file(secrets.path());
    let (root_id, head_id) = seed(data.path());
    let (mut child, addr) = spawn_server(data.path(), &token, None);
    let request = scion_proto::v1::DiffRequest {
        repo: REPO.into(),
        old_changeset_id: root_id.as_bytes().to_vec(),
        new_changeset_id: head_id.as_bytes().to_vec(),
        path: Vec::new(),
        after: Vec::new(),
    };
    let tcp = TcpStream::connect(&addr).await.unwrap();
    let (mut h2, connection) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut refused = None;
    let mut held = Vec::new();
    for i in 0..PARK_CAP {
        let (status, body, send) = open_unread(&mut h2, &request).await;
        if let Some(code) = status {
            refused = Some((i, code));
            break;
        }
        // The body stays open: dropping it resets the stream and frees
        // the reply.
        held.push((body, send));
    }
    let _ = held;
    let _ = child.kill();
    let _ = child.wait();
    let (at, code) = refused.expect("the budget never bound");
    assert_eq!(code, "8", "the refusal is the budget's, not another's");
    let _ = at;
}
