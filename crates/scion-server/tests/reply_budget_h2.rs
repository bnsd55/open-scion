//! The reply-memory budget at the HTTP/2 level: a client that opens
//! unary calls whose replies are large and never reads them holds the
//! budget; a client that resets them frees it.

use scion_core::{Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{DiffRequest, PutBlobRequest};
use scion_server::test_support::backend::from_store;
use scion_server::test_support::meta::MetaTx;
use std::sync::Arc;

use scion_server::Metrics;
use scion_server::test_support::{service, store::Store};

use prost::Message as _;
use tokio::net::TcpStream;

mod common;

const REPO: &str = "main";
const TOKEN: &str = "secret";

fn author() -> IdentityTime {
    IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 1_700_000_000, 0)
        .expect("identity fits the format")
}

/// Stores `repo` with `root` as its root changeset.
fn seed_root(store: &Store, root: &Changeset) -> scion_core::ChangesetId {
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            c.insert_changeset(repo, root)
        })
        .unwrap()
}

/// Serves a repo of `names` entries, whose root-to-head diff names
/// every one of them, and returns the diff request and the address.
/// The budget holds two parked replies, not three: a third unread
/// reply is refused, which is the proof the holds are counted.
async fn served_repo(names: usize) -> (tempfile::TempDir, DiffRequest, String) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let root = Changeset::new(
        vec![],
        author(),
        None,
        b"root\n".to_vec(),
        [(
            RepoPath::new(b"seed".as_slice()).unwrap(),
            Some(FileState::Regular {
                content: store.blobs().put(b"seed\n").unwrap(),
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    let root_id = seed_root(&store, &root);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let budget = 600_000;
    tokio::spawn(service::serve(
        from_store(store.clone()),
        listener,
        TOKEN,
        service::ServeOptions {
            data: dir.path().to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::new(Metrics::default()),
            reply_memory_budget: budget,
        },
        None,
        std::future::pending(),
    ));

    // The content of every file, uploaded once, so the repo has its blob.
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    let mut raw = ScionClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap());
    let blob = raw
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.into(),
            data: b"x".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .blob_id;
    let blob = scion_core::BlobId::try_from(blob.as_slice()).unwrap();

    let changeset = Changeset::new(
        vec![root_id],
        author(),
        None,
        b"big\n".to_vec(),
        (0..names)
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
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    let mut client = ScionClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap());
    let created = client
        .create_changeset(scion_proto::v1::CreateChangesetRequest {
            repo: REPO.into(),
            changeset: changeset.encode(),
        })
        .await
        .unwrap()
        .into_inner()
        .changeset_id;
    let created = ChangesetId::try_from(created.as_slice()).unwrap();
    (
        dir,
        DiffRequest {
            repo: REPO.into(),
            old_changeset_id: root_id.as_bytes().to_vec(),
            new_changeset_id: created.as_bytes().to_vec(),
            path: Vec::new(),
            after: Vec::new(),
        },
        addr.to_string(),
    )
}

/// The gRPC request body of `request`: the five-byte prefix, then the
/// encoded message.
fn grpc_body(request: &DiffRequest) -> bytes::Bytes {
    let message = request.encode_to_vec();
    let mut body = vec![0_u8; 5];
    body[0] = 0; // not compressed
    let [a, b, c, d] = (u32::try_from(message.len()).expect("the message fits u32")).to_be_bytes();
    body[1..5].copy_from_slice(&[a, b, c, d]);
    body.extend_from_slice(&message);
    body.into()
}

/// Opens one unary `Diff` call on a raw HTTP/2 connection, waits for
/// its reply headers, and returns the reply's body, unread.
async fn open_unread(
    h2: &mut h2::client::SendRequest<bytes::Bytes>,
    request: &DiffRequest,
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

#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_resets_its_unread_replies_frees_the_budget() {
    let (_dir, request, addr) = served_repo(40_000).await;
    let tcp = TcpStream::connect(&addr).await.unwrap();
    let (mut h2, connection) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Two replies, opened and never read: their bytes sit in the
    // server's memory, counted. The third call is refused, which is
    // the proof the first two are counted.
    // Open calls until one is refused. The first two or three leave
    // the body quickly (their bytes fit the h2 send buffer, so the
    // body ends and the budget frees them); from there, every further
    // unread reply parks and holds its share, until the budget is
    // spent. The refusal is a trailers-only reply: its status rides
    // in the headers.
    let mut bodies = Vec::new();
    let refused = loop {
        let (status, body, send) = open_unread(&mut h2, &request).await;
        match status {
            Some(code) if code == "8" => break Some(()),
            Some(code) => panic!("unexpected refusal: grpc-status {code}"),
            None => bodies.push((body, send)),
        }
        assert!(bodies.len() < 40, "the budget never bound");
    };

    // Reset every held reply, the way a client that gives up does.
    let _ = refused;
    for (_body, mut send) in bodies {
        send.send_reset(h2::Reason::CANCEL);
    }

    // The bytes come back: a fresh call is served whole, once the
    // resets reach the server.
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
                    // Served: the reply's data frames arrive, and its
                    // trailers name OK.
                    let read = status_of(body).await;
                    assert_eq!(read.as_deref(), Some("0"), "the reply is whole");
                    served = Some(());
                    break;
                }
                _ => {
                    // Still spent: the reset has not landed yet.
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            },
            Err(elapsed) => panic!("a fresh call failed: the server stalled: {elapsed}"),
        }
    }
    assert!(served.is_some(), "the budget freed after the resets");
}
