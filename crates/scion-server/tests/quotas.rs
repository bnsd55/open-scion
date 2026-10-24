//! Per-principal quotas over gRPC: requests in flight and upload bytes in
//! flight. The proof that a principal at its cap does not slow another one
//! is which calls complete while it is held; no test asserts a duration.

use scion_core::{Changeset, FileState, RepoPath};
use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{GetLandRequest, LogRequest, PutBlobRequest};
use scion_server::Access;
use scion_server::Quotas;
use scion_server::test_support::meta::{Meta as _, MetaTx as _, RefState};
use scion_server::{Backend, test_support::service};
use std::fmt::Write as _;
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
        let _ = writeln!(text, "read\t{name}\t{REPO}");
        let _ = writeln!(text, "write\t{name}\t{REPO}");
    }
    Access::parse(&text).unwrap()
}

/// A store whose repo `main` has one root changeset that puts `README`,
/// with its blob registered, as an upload through `PutBlob` would leave it.
fn store() -> (tempfile::TempDir, Backend) {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let blob = store.blobs().put(b"# hello\n").unwrap();
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(
                repo,
                &Changeset::new(
                    vec![],
                    scion_core::IdentityTime::new(
                        b"Ada".to_vec(),
                        b"ada@example.com".to_vec(),
                        1_700_000_000,
                        0,
                    )
                    .expect("identity fits the format"),
                    None,
                    b"root\n".to_vec(),
                    [(
                        RepoPath::new("README").unwrap(),
                        Some(FileState::Regular {
                            content: blob,
                            executable: false,
                        }),
                    )]
                    .into(),
                )
                .expect("changeset fits the format"),
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
fn main_ref(store: &Backend) -> RefState {
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
async fn server(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
    quotas: Quotas,
) -> (Stop, Channel) {
    server_with_tickets(store, dir, access, quotas, None).await
}

/// The same, with the ticket keys of `GetRootTree` and the object reads.
async fn server_with_tickets(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
    quotas: Quotas,
    tickets: Option<scion_server::Keys>,
) -> (Stop, Channel) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(service::serve(
        store.clone(),
        listener,
        access,
        service::ServeOptions {
            data: dir.to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas,
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::default(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        tickets.map(std::sync::Arc::new),
        async {
            stopped.await.ok();
        },
    ));
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
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
    let (dir, store) = store();
    let access = access(&["alice", "bob"]);
    // One request in flight per principal.
    let (stop, channel) = server(&store, dir.path(), access, Quotas::new(Some(1), None)).await;

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
    let mut pages = client(channel.clone(), "bob")
        .log(LogRequest {
            repo: REPO.to_string(),
            changeset_id: main_ref(&store).target.as_bytes().to_vec(),
            limit: 10,
            path: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    while pages.message().await.unwrap().is_some() {}
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
    let (dir, store) = store();
    let access = access(&["alice", "bob"]);
    let (stop, channel) = server(&store, dir.path(), access, Quotas::new(None, Some(10))).await;

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
    let (dir, store) = store();
    let access = access(&["alice", "bob"]);
    // One request in flight per principal. A clone of the caps shares the
    // server's counts, so the test reads its own state instead of polling
    // through the network: the prober rides the same connection whose
    // flow control the unread reply holds, and under load its reply says
    // nothing about the quota.
    let quotas = Quotas::new(Some(1), None);
    let observed = quotas.clone();
    let (stop, channel) = server(&store, dir.path(), access, quotas).await;
    // A file far past what the reply path can absorb while the client
    // reads nothing, so the producer really parks: about 4 MiB of
    // userspace (the reply's mpsc, hyper's read-ahead, the h2 send
    // buffer) plus up to about 10 MB of loopback TCP autotuning (the
    // server's send buffer and the client's receive buffer, whatever the
    // runner tunes them to) can soak up a reply before the producer must
    // wait for a read. A 3 MiB reply once fit whole and freed the slot
    // before the probe (#291); 16 MiB sat about 14 MB of buffers plus a
    // thin margin by arithmetic, and no failure was ever seen at 16 —
    // 64 MiB is about 4x what we measured. Raise it if this failure ever
    // comes back, and raise the hold assert below with it.
    let big = vec![b'z'; 64 * 1024 * 1024];
    let blob = store.blobs().put(&big).unwrap();
    // The root's `README` is small, so point `main` at a changeset whose
    // `README` is the big one.
    store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            c.add_repo_blob(repo, blob)?;
            let cs = Changeset::new(
                vec![],
                scion_core::IdentityTime::new(
                    b"Ada".to_vec(),
                    b"ada@example.com".to_vec(),
                    1_700_000_000,
                    0,
                )
                .expect("identity fits the format"),
                None,
                b"big\n".to_vec(),
                [(
                    RepoPath::new("README").unwrap(),
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )]
                .into(),
            )
            .expect("changeset fits the format");
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
    until(|| async { observed.in_flight("alice") == 1 }).await;
    assert_eq!(probe(&prober).await, Code::ResourceExhausted);
    // The producer is still parked while the probe runs: the slot is held
    // by a live reply, not by one that already ended. If the reply buffers
    // ever grow past the file again, this says so loudly instead of leaving
    // the probe to race the producer's end.
    assert_eq!(observed.in_flight("alice"), 1);

    // Bob is under his own cap: his calls complete while alice holds hers.
    let _ = probe(&client(channel.clone(), "bob")).await;

    // Alice drops her reply, so the stream ends and her slot comes back.
    drop(stream);
    drop(alice);
    until(|| async { observed.in_flight("alice") == 0 }).await;
    assert_eq!(probe(&prober).await, Code::NotFound);
    stop.send(()).ok();
}

/// A client of `Objects` that carries no token: `GetObjects` needs none,
/// its permission is the ticket.
fn objects(channel: Channel) -> scion_proto::v1::objects_client::ObjectsClient<Channel> {
    scion_proto::v1::objects_client::ObjectsClient::new(channel)
}

/// The ticket of `principal` for the root tree of `main`, signed by the
/// server's own keys.
fn ticket_for(keys: &scion_server::Keys, principal: &str, store: &Backend) -> Vec<u8> {
    let cs = main_ref(store).target;
    let root = scion_server::test_support::read::root_tree(
        store,
        REPO,
        cs,
        &scion_server::test_support::deadline::Deadline::new(
            std::time::Instant::now() + std::time::Duration::from_secs(300),
        ),
    )
    .unwrap();
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
    let (dir, store) = store();
    let access = access(&["alice", "bob"]);
    let keys = scion_server::Keys::generate();
    // One request in flight per principal.
    let (stop, channel) = server_with_tickets(
        &store,
        dir.path(),
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
            let cs = Changeset::new(
                vec![],
                scion_core::IdentityTime::new(
                    b"Ada".to_vec(),
                    b"ada@example.com".to_vec(),
                    1_700_000_000,
                    0,
                )
                .expect("identity fits the format"),
                None,
                b"big\n".to_vec(),
                [(
                    RepoPath::new("README").unwrap(),
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )]
                .into(),
            )
            .expect("changeset fits the format");
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

#[path = "common/mod.rs"]
mod common;
