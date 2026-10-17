//! Per-principal quotas over gRPC: requests in flight and upload bytes in
//! flight. The proof that a principal at its cap does not slow another one
//! is which calls complete while it is held; no test asserts a duration.

use scion_core::{Changeset, FileState, RepoPath};
use scion_proto::auth::token_digest;
use scion_proto::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{GetLandRequest, LogRequest, PutBlobRequest};
use scion_server::access::Access;
use scion_server::meta::{MetaTx as _, RefState};
use scion_server::quotas::Quotas;
use scion_server::{Store, service};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::{InterceptedService, Interceptor};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const REPO: &str = "main";

/// Adds `authorization: Bearer <token>` to every request.
#[derive(Clone)]
struct Bearer(AsciiMetadataValue);

impl Interceptor for Bearer {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let value = self.0.clone();
        request.metadata_mut().insert("authorization", value);
        Ok(request)
    }
}

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

fn hex(token: &str) -> String {
    blake3::Hash::from(token_digest(token)).to_hex().to_string()
}

/// `token` and `write` lines for each principal, so every named principal
/// may read and write `main`.
fn access(principals: &[&str]) -> Access {
    let mut text = String::new();
    for name in principals {
        let line = format!("token\t{name}\t{}\n", hex(name));
        text.push_str(&line);
        text.push_str("write\t");
        text.push_str(name);
        text.push('\t');
        text.push_str(REPO);
        text.push('\n');
    }
    Access::parse(&text).unwrap()
}

/// A store whose repo `main` has one root changeset that puts `README`,
/// with its blob registered, as an upload through `PutBlob` would leave it.
fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let blob = store.blobs().put(b"# hello\n").unwrap();
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(
                repo,
                &Changeset {
                    parents: vec![],
                    author: scion_core::IdentityTime {
                        name: b"Ada".to_vec(),
                        email: b"ada@example.com".to_vec(),
                        unix_secs: 1_700_000_000,
                        tz_offset_minutes: 0,
                    },
                    committer: None,
                    message: b"root\n".to_vec(),
                    changes: [(
                        RepoPath::new("README").unwrap(),
                        Some(FileState::Regular {
                            content: blob,
                            executable: false,
                        }),
                    )]
                    .into(),
                },
            )?;
            c.cas_ref(repo, "main", None, root)?;
            c.add_repo_blob(repo, blob)?;
            Ok(root)
        })
        .unwrap();
    (dir, store)
}

type Stop = tokio::sync::oneshot::Sender<()>;

/// Where `main` of `REPO` points.
fn main_ref(store: &Store) -> RefState {
    store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            c.get_ref(repo, "main")
        })
        .unwrap()
        .unwrap()
}

/// A server of `store` with `access` and `quotas`. Hold the stop signal, or
/// the server shuts down.
async fn server(store: &Store, access: Access, quotas: Quotas) -> (Stop, Channel) {
    server_with_tickets(store, access, quotas, None).await
}

/// The same, with the ticket keys of `GetRootTree` and the object reads.
async fn server_with_tickets(
    store: &Store,
    access: Access,
    quotas: Quotas,
    tickets: Option<scion_server::tickets::Keys>,
) -> (Stop, Channel) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(service::serve(
        store.clone(),
        listener,
        access,
        service::ServeOptions {
            limits: Limits::default(),
            quotas,
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::default(),
        },
        tickets.map(std::sync::Arc::new),
        async {
            stopped.await.ok();
        },
    ));
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (stop, channel)
}

fn client(channel: Channel, token: &str) -> Scion {
    let bearer = Bearer(format!("Bearer {token}").parse().unwrap());
    ScionClient::with_interceptor(channel, bearer)
}

/// A request that fails only on the quotas: land 1 does not exist, so a
/// caller with free slots sees `NOT_FOUND`, and a caller at its request cap
/// sees `RESOURCE_EXHAUSTED`. The caller is reused, so the probes share one
/// connection instead of opening thousands.
async fn probe(caller: &Scion) -> Code {
    caller
        .clone()
        .get_land(GetLandRequest {
            repo: REPO.to_string(),
            land_id: "1".into(),
        })
        .await
        .err()
        .map(|status| status.code())
        .unwrap()
}

/// Waits until `want` says the server has moved to the state that the test
/// waits for. Nothing asserts how long that takes.
async fn until<F, Fut>(f: F)
where
    F: Fn() -> Fut,
    F: Send,
    Fut: Future<Output = bool> + Send,
{
    // Generous, so a loaded CI machine still reaches the state; the
    // test asserts the state, never a duration.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while !f().await {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

/// An upload of `principal` that has sent its one message and stays open:
/// the server holds a request slot and the message's bytes while the test
/// keeps the sender. Dropping the sender lets the upload finish.
async fn hold_upload(
    channel: Channel,
    token: &str,
    data: &[u8],
) -> (
    tokio::sync::mpsc::Sender<PutBlobRequest>,
    tokio::task::JoinHandle<Result<tonic::Response<scion_proto::v1::PutBlobResponse>, Status>>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel::<PutBlobRequest>(1);
    sender
        .send(PutBlobRequest {
            repo: REPO.to_string(),
            data: data.to_vec(),
        })
        .await
        .unwrap();
    let mut client = client(channel, token);
    let reply = tokio::spawn(async move { client.put_blob(ReceiverStream::new(receiver)).await });
    (sender, reply)
}

/// Holds one request slot of `principal` with a half-open upload. A probe
/// that reaches the server first takes the slot and gets the upload
/// refused; that round is dropped and the hold is tried again.
async fn hold_slot(
    channel: &Channel,
    principal: &str,
    data: &[u8],
) -> (
    tokio::sync::mpsc::Sender<PutBlobRequest>,
    tokio::task::JoinHandle<Result<tonic::Response<scion_proto::v1::PutBlobResponse>, Status>>,
    Scion,
) {
    for _ in 0..20 {
        let (sender, reply) = hold_upload(channel.clone(), principal, data).await;
        let caller = client(channel.clone(), principal);
        let held = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                // The upload lost the race and was refused: rearm.
                if reply.is_finished() {
                    break false;
                }
                if probe(&caller).await == Code::ResourceExhausted {
                    break true;
                }
            }
        })
        .await
        .unwrap_or(false);
        if held && !reply.is_finished() {
            return (sender, reply, caller);
        }
        drop(sender);
    }
    panic!("the upload never got its request slot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_principal_at_its_cap_is_refused_while_another_goes_on() {
    let (_dir, store) = store();
    let access = access(&["alice", "bob"]);
    // One request in flight per principal.
    let (stop, channel) = server(&store, access, Quotas::new(Some(1), None)).await;

    // Alice's upload sends its first message and stays open, so the server
    // holds her only request slot. A probe that reaches the server before
    // the upload would take the slot first and get the upload refused, so
    // such a round is rearmed.
    let (sender, reply, alice) = hold_slot(&channel, "alice", b"held").await;

    // Alice's next request is refused, and the error names her flag.
    let denied = alice
        .clone()
        .get_land(GetLandRequest {
            repo: REPO.to_string(),
            land_id: "1".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::ResourceExhausted, "{denied:?}");
    assert!(denied.to_string().contains("--max-principal-requests 1"));

    // Bob is under his own cap: while alice is held, his calls complete.
    client(channel.clone(), "bob")
        .log(LogRequest {
            repo: REPO.to_string(),
            changeset_id: main_ref(&store).target.as_bytes().to_vec(),
            limit: 10,
            path: Vec::new(),
        })
        .await
        .unwrap();
    let _ = probe(&client(channel.clone(), "bob")).await;

    // Alice ends her upload, so her slot comes back and her next request
    // goes through again.
    drop(sender);
    reply.await.unwrap().unwrap();
    until(|| async { probe(&alice).await == Code::NotFound }).await;
    stop.send(()).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upload_over_the_byte_cap_is_refused_and_frees_on_exit() {
    let (_dir, store) = store();
    let access = access(&["alice", "bob"]);
    let (stop, channel) = server(&store, access, Quotas::new(None, Some(10))).await;

    // The first message alone is over alice's byte cap.
    let over = client(channel.clone(), "alice")
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.to_string(),
            data: vec![b'x'; 16],
        }))
        .await
        .unwrap_err();
    assert_eq!(over.code(), Code::ResourceExhausted, "{over:?}");
    assert!(over.to_string().contains("--max-principal-upload-bytes 10"));

    // A later message that crosses the cap is refused too, and the client
    // sees the error on the reply.
    let (sender, receiver) = tokio::sync::mpsc::channel::<PutBlobRequest>(4);
    sender
        .send(PutBlobRequest {
            repo: REPO.to_string(),
            data: b"12345".to_vec(),
        })
        .await
        .unwrap();
    // The later messages of one upload name no repo.
    sender
        .send(PutBlobRequest {
            repo: String::new(),
            data: b"6789012345".to_vec(),
        })
        .await
        .unwrap();
    drop(sender);
    let status = client(channel.clone(), "alice")
        .put_blob(ReceiverStream::new(receiver))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");

    // The bytes of the refused and finished calls are back, so alice can
    // upload again; and bob's count is his own.
    client(channel.clone(), "alice")
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.to_string(),
            data: b"small".to_vec(),
        }))
        .await
        .unwrap();
    client(channel.clone(), "bob")
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.to_string(),
            data: vec![b'y'; 10],
        }))
        .await
        .unwrap();
    stop.send(()).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_holds_its_slot_until_the_stream_ends() {
    let (_dir, store) = store();
    let access = access(&["alice", "bob"]);
    // One request in flight per principal.
    let (stop, channel) = server(&store, access, Quotas::new(Some(1), None)).await;
    // A file of several parts, so its reply has a producer that stays alive
    // after the handler has returned, and that waits on a client which
    // takes no message.
    let big = vec![b'z'; 3 * 1024 * 1024];
    let blob = store.blobs().put(&big).unwrap();
    // The root's `README` is small, so point `main` at a changeset whose
    // `README` is the big one.
    store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            c.add_repo_blob(repo, blob)?;
            let cs = Changeset {
                parents: vec![],
                author: scion_core::IdentityTime {
                    name: b"Ada".to_vec(),
                    email: b"ada@example.com".to_vec(),
                    unix_secs: 1_700_000_000,
                    tz_offset_minutes: 0,
                },
                committer: None,
                message: b"big\n".to_vec(),
                changes: [(
                    RepoPath::new("README").unwrap(),
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )]
                .into(),
            };
            let id = c.insert_changeset(repo, &cs)?;
            let version = c.get_ref(repo, "main")?.unwrap().version;
            assert!(c.cas_ref(repo, "main", Some(version), id)?);
            Ok(())
        })
        .unwrap();
    let mut alice = client(channel.clone(), "alice");
    let prober = client(channel.clone(), "alice");
    // Alice opens a reply and stops reading it. The producer of the reply
    // stays alive, and with it her only request slot.
    let stream = alice
        .read_file(scion_proto::v1::ReadFileRequest {
            repo: REPO.to_string(),
            changeset_id: main_ref(&store).target.as_bytes().to_vec(),
            path: b"README".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    until(|| async { probe(&prober).await == Code::ResourceExhausted }).await;

    // Bob is under his own cap: his calls complete while alice holds hers.
    let _ = probe(&client(channel.clone(), "bob")).await;

    // Alice drops her reply, so the stream ends and her slot comes back.
    drop(stream);
    drop(alice);
    until(|| async { probe(&prober).await == Code::NotFound }).await;
    stop.send(()).ok();
}

/// A client of `Objects` that carries no token: `GetObjects` needs none,
/// its permission is the ticket.
fn objects(channel: Channel) -> scion_proto::v1::objects_client::ObjectsClient<Channel> {
    scion_proto::v1::objects_client::ObjectsClient::new(channel)
}

/// The ticket of `principal` for the root tree of `main`, signed by the
/// server's own keys.
fn ticket_for(keys: &scion_server::tickets::Keys, principal: &str, store: &Store) -> Vec<u8> {
    let cs = main_ref(store).target;
    let root = scion_server::read::root_tree(store, REPO, cs).unwrap();
    keys.sign(REPO, principal, *root.as_bytes()).unwrap()
}

/// A read of `blob` as `main`'s `README`, over `ticket`.
fn read_request(ticket: Vec<u8>, blob: scion_core::BlobId) -> scion_proto::v1::GetObjectsRequest {
    scion_proto::v1::GetObjectsRequest {
        ticket,
        accept: Vec::new(),
        refs: vec![scion_proto::v1::ObjectRef {
            kind: scion_proto::v1::ObjectKind::Blob as i32,
            id: blob.as_bytes().to_vec(),
            path: b"README".to_vec(),
        }],
    }
}

/// A `GetObjects` stream of a blob of several parts, which the test
/// holds without reading, so its producer stays alive.
async fn hold_objects(
    channel: Channel,
    ticket: Vec<u8>,
    blob: scion_core::BlobId,
) -> tonic::Streaming<scion_proto::v1::ObjectPart> {
    objects(channel)
        .get_objects(read_request(ticket, blob))
        .await
        .unwrap()
        .into_inner()
}

/// The quota of a `GetObjects` stream names the principal that its
/// ticket signs: a principal at its cap gets `RESOURCE_EXHAUSTED` on one
/// more read, while another principal's read goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_get_objects_stream_holds_its_principals_slot() {
    let (_dir, store) = store();
    let access = access(&["alice", "bob"]);
    let keys = scion_server::tickets::Keys::generate();
    // One request in flight per principal.
    let (stop, channel) = server_with_tickets(
        &store,
        access,
        Quotas::new(Some(1), None),
        Some(keys.clone()),
    )
    .await;
    // A file of several parts and well above the h2 windows of a tonic
    // client, so the reply's producer stays blocked on a client that takes
    // no message.
    let big = vec![b'z'; 8 * 1024 * 1024];
    let blob = store.blobs().put(&big).unwrap();
    store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            c.add_repo_blob(repo, blob)?;
            let cs = Changeset {
                parents: vec![],
                author: scion_core::IdentityTime {
                    name: b"Ada".to_vec(),
                    email: b"ada@example.com".to_vec(),
                    unix_secs: 1_700_000_000,
                    tz_offset_minutes: 0,
                },
                committer: None,
                message: b"big\n".to_vec(),
                changes: [(
                    RepoPath::new("README").unwrap(),
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )]
                .into(),
            };
            let id = c.insert_changeset(repo, &cs)?;
            let version = c.get_ref(repo, "main")?.unwrap().version;
            assert!(c.cas_ref(repo, "main", Some(version), id)?);
            Ok(())
        })
        .unwrap();
    let alice_ticket = ticket_for(&keys, "alice", &store);
    let bob_ticket = ticket_for(&keys, "bob", &store);

    // Alice opens a GetObjects stream and stops reading it: the producer
    // stays alive, and with it the slot of her ticket's principal.
    let stream = hold_objects(channel.clone(), alice_ticket.clone(), blob).await;
    // Her next GetObjects is refused while the first holds the slot.
    let denied = objects(channel.clone())
        .get_objects(read_request(alice_ticket.clone(), blob))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::ResourceExhausted, "{denied:?}");

    // Bob's ticket names his own principal: his read goes through while
    // alice holds hers.
    let _ = objects(channel.clone())
        .get_objects(read_request(bob_ticket, blob))
        .await
        .unwrap();

    // Alice drops her stream, so her slot comes back.
    drop(stream);
    until(|| async {
        objects(channel.clone())
            .get_objects(read_request(alice_ticket.clone(), blob))
            .await
            .is_ok()
    })
    .await;
    stop.send(()).ok();
}
