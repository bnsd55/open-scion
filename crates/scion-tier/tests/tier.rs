//! A tier against a fake upstream: it checks what upstream sends, fetches a
//! shared miss once, keeps one object's failure to that object, and refuses
//! callers without the token.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use scion_core::{BlobId, Entry, FileState, PathElem, Tree};
use scion_proto::CHUNK_BYTES;
use scion_proto::object::{self, Assembler, Header};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind, ObjectPart,
    ObjectRef, ResolveRefRequest, ResolveRefResponse,
};
use scion_tier::{CACHE_UPSTREAM_TIMEOUT, DiskStore, MemoryStore, Tier, bind_unix};
use tokio::net::TcpListener;
use tokio_stream::Stream;
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Request, Response, Status};

const TOKEN: &str = "secret";

/// An object as a client sees it once unpacked.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Object {
    kind: i32,
    id: Vec<u8>,
    data: Vec<u8>,
}

impl Object {
    fn header(&self, encoding: Encoding, encoded_size: usize) -> Header {
        Header {
            kind: ObjectKind::try_from(self.kind).unwrap(),
            id: self.id.clone().try_into().unwrap(),
            encoding,
            encoded_size,
            size: self.data.len(),
        }
    }

    /// The parts that send it RAW.
    fn raw(&self) -> Vec<ObjectPart> {
        let header = self.header(Encoding::Raw, self.data.len());
        header.parts(object::chunks(self.data.clone()))
    }

    /// The parts that send `frame` as its zstd form.
    fn zstd(&self, frame: Vec<u8>) -> Vec<ObjectPart> {
        self.header(Encoding::Zstd, frame.len())
            .parts(object::chunks(frame))
    }
}

/// How the fake upstream's `GetObjects` replies go wrong.
#[derive(Clone, Copy, Debug, Default)]
enum Mode {
    #[default]
    Honest,
    /// One more object after those asked for.
    Extra,
    /// Each object with the other kind.
    WrongKind,
    /// The objects in reverse order.
    Reversed,
    /// Only the first object, as a reply that stops early.
    Short,
}

/// An upstream that sends fixed parts for each object (so possibly the wrong
/// bytes); the first `GetObjects` reply only after `delay`, so other calls
/// can join that fetch. A missing object ends the reply with `NOT_FOUND`, as
/// on the server. It counts the `GetObjects` and the `GetRootTree` calls.
#[derive(Clone, Default)]
struct Fake {
    objects: Arc<HashMap<Vec<u8>, Vec<ObjectPart>>>,
    delay: Duration,
    mode: Mode,
    calls: Arc<AtomicUsize>,
    roots: Arc<AtomicUsize>,
}

impl Fake {
    /// Sends each object RAW.
    fn new(objects: &[&Object], delay: Duration) -> Self {
        let sent = objects.iter().map(|o| (o.id.clone(), o.raw()));
        Self::sending(sent, delay)
    }

    /// Sends the given parts for each ID.
    fn sending(
        sent: impl IntoIterator<Item = (Vec<u8>, Vec<ObjectPart>)>,
        delay: Duration,
    ) -> Self {
        Self {
            objects: Arc::new(sent.into_iter().collect()),
            delay,
            ..Self::default()
        }
    }

    fn with_mode(self, mode: Mode) -> Self {
        Self { mode, ..self }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn roots(&self) -> usize {
        self.roots.load(Ordering::SeqCst)
    }
}

type Replies = Pin<Box<dyn Stream<Item = Result<ObjectPart, Status>> + Send>>;

#[tonic::async_trait]
impl Objects for Fake {
    async fn resolve_ref(
        &self,
        _: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        Err(Status::unimplemented("the fake has no refs"))
    }

    /// By the changeset's first byte: 0 is not found, 1 gets a tree ID of
    /// the wrong length, anything else gets a good one.
    async fn get_root_tree(
        &self,
        request: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        self.roots.fetch_add(1, Ordering::SeqCst);
        let tree_id = match request.into_inner().changeset_id.first() {
            Some(0) => return Err(Status::not_found("no such changeset")),
            Some(1) => vec![7; 31],
            _ => vec![7; 32],
        };
        Ok(Response::new(GetRootTreeResponse { tree_id }))
    }

    type GetObjectsStream = Replies;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Replies>, Status> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(self.delay).await;
        }
        let request = request.into_inner();
        // A repo that upstream does not have fails the call, as on the server.
        if request.repo == "gone" {
            return Err(Status::not_found("no such repo"));
        }
        // The parts of each object, or the error that ends the reply.
        let mut replies: Vec<Result<Vec<ObjectPart>, Status>> = Vec::new();
        for r in request.refs {
            if let Some(parts) = self.objects.get(&r.id) {
                replies.push(Ok(parts.clone()));
            } else {
                replies.push(Err(Status::not_found("no such object")));
                break;
            }
        }
        match self.mode {
            Mode::Honest => {}
            Mode::Extra => replies.push(Ok(blob(b"not asked for\n").raw())),
            Mode::WrongKind => {
                for parts in replies.iter_mut().flatten() {
                    parts[0].kind = ObjectKind::Tree.into();
                }
            }
            Mode::Reversed => replies.reverse(),
            Mode::Short => replies.truncate(1),
        }
        let replies: Vec<Result<ObjectPart, Status>> = replies
            .into_iter()
            .flat_map(|object| match object {
                Ok(parts) => parts.into_iter().map(Ok).collect(),
                Err(status) => vec![Err(status)],
            })
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(replies))))
    }
}

async fn serve_fake(fake: Fake) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        Server::builder()
            .add_service(ObjectsServer::new(fake))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    url
}

/// A `GetObjects` call for `objects` in `repo`, from a client that accepts
/// `zstd` or not: the objects it returns, unpacked, with the number of parts
/// and the encoding of each, or the error that ends it. `Err` from the outer
/// result is the call's status.
async fn get_parts(
    client: &mut Client,
    repo: &str,
    objects: &[&Object],
    zstd: bool,
) -> Result<Result<Vec<(Object, usize, Encoding)>, Status>, Status> {
    let refs = objects
        .iter()
        .map(|o| ObjectRef {
            kind: o.kind,
            id: o.id.clone(),
        })
        .collect();
    let accept = if zstd {
        vec![Encoding::Zstd.into()]
    } else {
        Vec::new()
    };
    let request = GetObjectsRequest {
        repo: repo.into(),
        refs,
        accept,
    };
    let mut stream = client.get_objects(request).await?.into_inner();
    let mut got = Vec::new();
    loop {
        let first = match stream.message().await {
            Ok(Some(part)) => part,
            Ok(None) => return Ok(Ok(got)),
            Err(status) => return Ok(Err(status)),
        };
        let mut object = Assembler::new(Header::of(&first, zstd).unwrap(), first);
        let mut parts = 1;
        while !object.is_done() {
            match stream.message().await {
                Ok(Some(part)) => object.push(&part).unwrap(),
                Ok(None) => panic!("the reply ended in the middle of an object"),
                Err(status) => return Ok(Err(status)),
            }
            parts += 1;
        }
        let (header, data) = object.finish();
        let data = object::decode(&header, data).unwrap();
        let object = Object {
            kind: header.kind.into(),
            id: header.id.to_vec(),
            data,
        };
        got.push((object, parts, header.encoding));
    }
}

/// [`get_parts`] from a client that accepts only RAW, without the parts.
async fn get_in(
    client: &mut Client,
    repo: &str,
    objects: &[&Object],
) -> Result<Result<Vec<Object>, Status>, Status> {
    let got = get_parts(client, repo, objects, false).await?;
    Ok(got.map(|got| got.into_iter().map(|(object, ..)| object).collect()))
}

/// A cache that waits at most `timeout` for `upstream`, and its address.
async fn serve_cache(upstream: &str, timeout: Duration) -> (Tier<MemoryStore>, String) {
    let store = MemoryStore::new(16 << 20);
    let cache = Tier::new(store, upstream, TOKEN, timeout).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(cache.clone().serve(TOKEN, incoming, std::future::pending()));
    (cache, url)
}

type Client = ObjectsClient<InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Bearer + Send>>>;
type Bearer = Result<Request<()>, Status>;

/// A client that sends `token`, or no token.
async fn connect(url: &str, token: Option<&str>) -> Client {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let header: Option<MetadataValue<Ascii>> =
        token.map(|token| format!("Bearer {token}").parse().unwrap());
    let auth: Box<dyn Fn(Request<()>) -> Bearer + Send> = Box::new(move |mut request| {
        if let Some(header) = &header {
            request
                .metadata_mut()
                .insert("authorization", header.clone());
        }
        Ok(request)
    });
    ObjectsClient::with_interceptor(channel, auth)
}

/// The objects a `GetObjects` call in repo `r` returns, or the error it
/// ends with.
async fn get(client: &mut Client, objects: &[&Object]) -> Result<Vec<Object>, Status> {
    get_in(client, "r", objects).await?
}

fn blob(data: &[u8]) -> Object {
    Object {
        kind: ObjectKind::Blob.into(),
        id: BlobId::of(data).as_bytes().to_vec(),
        data: data.to_vec(),
    }
}

/// `len` bytes that zstd cannot make smaller.
fn noise(len: usize) -> Vec<u8> {
    let mut state = 1_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[3]
        })
        .collect()
}

#[tokio::test]
async fn a_tier_refuses_an_object_whose_bytes_do_not_match_its_id() {
    let good = blob(b"good\n");
    let bad_blob = Object {
        data: b"evil\n".to_vec(),
        ..blob(b"bad\n")
    };
    // A valid tree, sent for the ID of another one.
    let tree = Tree {
        entries: [(
            PathElem::new("f").unwrap(),
            Entry::File(FileState::Regular {
                content: BlobId::of(b"good\n"),
                executable: false,
            }),
        )]
        .into(),
    };
    let bad_tree = Object {
        kind: ObjectKind::Tree.into(),
        id: tree.id().as_bytes().to_vec(),
        data: Tree::default().encode(),
    };
    let fake = Fake::new(&[&good, &bad_blob, &bad_tree], Duration::ZERO);
    let upstream = serve_fake(fake.clone()).await;
    let (_cache, url) = serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT).await;
    let mut client = connect(&url, Some(TOKEN)).await;

    // A good object passes and is kept, so the second call stays in the cache.
    for _ in 0..2 {
        assert_eq!(
            get(&mut client, &[&good]).await.unwrap(),
            vec![good.clone()]
        );
    }
    assert_eq!(fake.calls(), 1);

    for wrong in [&bad_blob, &bad_tree] {
        let before = fake.calls();
        for _ in 0..2 {
            let status = get(&mut client, &[wrong]).await.unwrap_err();
            assert_eq!(status.code(), Code::DataLoss, "{status:?}");
        }
        // Not kept: each call asked upstream again.
        assert_eq!(fake.calls(), before + 2);
    }
}

#[tokio::test]
async fn a_tier_refuses_a_bad_zstd_frame_or_part_sequence() {
    let content = b"the content of a blob, the content of a blob\n";
    let target = blob(content);
    let frame = object::pack(content).unwrap();
    let mut bad: Vec<(&str, Vec<ObjectPart>)> = Vec::new();
    // A good frame of other bytes.
    bad.push(("other bytes", target.zstd(object::pack(b"evil\n").unwrap())));
    // The object's size says one byte more than the frame header.
    let mut parts = target.zstd(frame.clone());
    parts[0].size += 1;
    bad.push(("size above the header", parts));
    // The frame header says one byte more than the frame holds. Byte 5 is the
    // content size of a small frame.
    let mut lying = frame.clone();
    lying[5] += 1;
    assert_eq!(object::frame_size(&lying), Ok(content.len() + 1));
    let mut parts = target.zstd(lying);
    parts[0].size += 1;
    bad.push(("a lying frame header", parts));
    // A frame with no size in its header.
    let mut streamed = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
    std::io::Write::write_all(&mut streamed, content).unwrap();
    bad.push(("no size", target.zstd(streamed.finish().unwrap())));
    // Two parts announced, one sent: the reply ends in the middle.
    let big = blob(&noise(CHUNK_BYTES + 10));
    let mut parts = big.raw();
    parts.truncate(1);
    bad.push(("a truncated part sequence", parts));
    // A later part that is too short.
    let mut parts = big.raw();
    parts[1].data.pop();
    bad.push(("a short part", parts));

    for (what, parts) in bad {
        let id = parts[0].id.clone();
        let wanted = Object {
            id: id.clone(),
            ..blob(b"")
        };
        let fake = Fake::sending([(id, parts)], Duration::ZERO);
        let (_cache, url) = cache_for(&fake).await;
        let mut client = connect(&url, Some(TOKEN)).await;
        for zstd in [false, true] {
            let status = get_parts(&mut client, "r", &[&wanted], zstd)
                .await
                .and_then(|got| got)
                .unwrap_err();
            assert_eq!(status.code(), Code::DataLoss, "{what}: {status:?}");
        }
        // Not kept: each call asked upstream again.
        assert_eq!(fake.calls(), 2, "{what}");
    }
}

#[tokio::test]
async fn a_large_blob_arrives_in_parts_is_checked_and_sent_on_as_it_came() {
    let big = blob(&noise(3 * CHUNK_BYTES + 5));
    let frame = object::pack(&big.data).unwrap();
    assert!(frame.len() > 3 * CHUNK_BYTES);
    let fake = Fake::sending([(big.id.clone(), big.zstd(frame))], Duration::ZERO);
    let (_cache, url) = cache_for(&fake).await;
    let mut client = connect(&url, Some(TOKEN)).await;

    // A client that accepts zstd gets the frame in four parts; another gets
    // the content in four parts. Both from one fetch.
    for (zstd, encoding) in [(true, Encoding::Zstd), (false, Encoding::Raw)] {
        let got = get_parts(&mut client, "r", &[&big], zstd).await.unwrap();
        assert_eq!(got.unwrap(), vec![(big.clone(), 4, encoding)]);
    }
    assert_eq!(fake.calls(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_misses_for_one_object_share_one_fetch() {
    let object = blob(b"wanted by every job\n");
    // Slow, so every client misses while the first fetch runs.
    let fake = Fake::new(&[&object], Duration::from_millis(300));
    let upstream = serve_fake(fake.clone()).await;
    let (cache, url) = serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT).await;

    let mut clients = Vec::new();
    for _ in 0..10 {
        let mut client = connect(&url, Some(TOKEN)).await;
        let object = object.clone();
        clients.push(tokio::spawn(
            async move { get(&mut client, &[&object]).await },
        ));
    }
    for client in clients {
        assert_eq!(client.await.unwrap().unwrap(), vec![object.clone()]);
    }
    assert_eq!(fake.calls(), 1);
    assert_eq!(cache.upstream_calls(), 1);
}

#[tokio::test]
async fn each_tier_refuses_a_call_without_the_token() {
    let fake = Fake::default();
    let upstream = serve_fake(fake.clone()).await;
    let (cache, cache_url) = serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT).await;
    let dir = tempfile::tempdir().unwrap();
    // The socket's directory must be private.
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("daemon.sock");
    let store = DiskStore::open(&dir.path().join("data"), 1 << 20).unwrap();
    let daemon = Tier::new(store, &cache_url, TOKEN, CACHE_UPSTREAM_TIMEOUT).unwrap();
    let incoming = UnixListenerStream::new(bind_unix(&socket).await.unwrap());
    tokio::spawn(
        daemon
            .clone()
            .serve(TOKEN, incoming, std::future::pending()),
    );

    // Only this user can connect to the daemon, and only one daemon listens.
    let mode = std::fs::metadata(&socket).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    let taken = bind_unix(&socket).await.unwrap_err();
    assert_eq!(taken.kind(), std::io::ErrorKind::AddrInUse);
    // A socket in a directory that others can reach is refused.
    let open = dir.path().join("open");
    std::fs::create_dir(&open).unwrap();
    std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o777)).unwrap();
    let refused = bind_unix(&open.join("daemon.sock")).await.unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied);

    let daemon_url = format!("unix://{}", socket.display());
    for url in [&cache_url, &daemon_url] {
        for token in [None, Some("wrong")] {
            let mut client = connect(url, token).await;
            let resolve = ResolveRefRequest {
                repo: "r".into(),
                name: "main".into(),
            };
            let status = client.resolve_ref(resolve).await.unwrap_err();
            assert_eq!(status.code(), Code::Unauthenticated, "{url} {token:?}");
            let status = get(&mut client, &[&blob(b"x")]).await.unwrap_err();
            assert_eq!(status.code(), Code::Unauthenticated, "{url} {token:?}");
        }
    }
    assert_eq!(daemon.upstream_calls(), 0);
    assert_eq!(cache.upstream_calls(), 0);

    // With the token, the call gets through both tiers to the fake, which
    // has no refs.
    let mut client = connect(&daemon_url, Some(TOKEN)).await;
    let resolve = ResolveRefRequest {
        repo: "r".into(),
        name: "main".into(),
    };
    let status = client.resolve_ref(resolve).await.unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented, "{status:?}");
    assert_eq!((daemon.upstream_calls(), cache.upstream_calls()), (1, 1));
}

/// A fake upstream and a cache in front of it. Returns the cache and its
/// address.
async fn cache_for(fake: &Fake) -> (Tier<MemoryStore>, String) {
    let upstream = serve_fake(fake.clone()).await;
    serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT).await
}

/// Starts a `GetObjects` call for `objects` from a new client of `url`, and
/// gives the first upstream call time to start.
async fn spawn_get(
    url: &str,
    objects: &[Object],
) -> tokio::task::JoinHandle<Result<Vec<Object>, Status>> {
    let mut client = connect(url, Some(TOKEN)).await;
    let objects = objects.to_vec();
    let call =
        tokio::spawn(async move { get(&mut client, &objects.iter().collect::<Vec<_>>()).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    call
}

#[tokio::test]
async fn one_missing_object_fails_only_itself() {
    let (missing, there) = (blob(b"not upstream\n"), blob(b"upstream\n"));
    // Slow, so the second call joins the first call's fetch.
    let fake = Fake::new(&[&there], Duration::from_millis(300));
    let (_cache, url) = cache_for(&fake).await;
    let leader = spawn_get(&url, &[missing, there.clone()]).await;

    // The second call waits for the same fetch, and gets its object.
    let mut second = connect(&url, Some(TOKEN)).await;
    assert_eq!(get(&mut second, &[&there]).await.unwrap(), vec![there]);
    let status = leader.await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    // One call for both objects, then one for the object after the missing one.
    assert_eq!(fake.calls(), 2);
}

#[tokio::test]
async fn objects_that_upstream_should_not_send_are_refused_and_not_kept() {
    let (a, b) = (blob(b"a\n"), blob(b"b\n"));
    for mode in [Mode::WrongKind, Mode::Reversed] {
        let fake = Fake::new(&[&a, &b], Duration::ZERO).with_mode(mode);
        let (_cache, url) = cache_for(&fake).await;
        let mut client = connect(&url, Some(TOKEN)).await;
        for _ in 0..2 {
            let status = get(&mut client, &[&a, &b]).await.unwrap_err();
            assert_eq!(status.code(), Code::Internal, "{mode:?}: {status:?}");
        }
        assert_eq!(fake.calls(), 2, "{mode:?}: nothing was kept");
    }

    // An object after the last one asked for: the objects asked for pass,
    // and the extra one is not kept.
    let fake = Fake::new(&[&a], Duration::ZERO).with_mode(Mode::Extra);
    let (_cache, url) = cache_for(&fake).await;
    let mut client = connect(&url, Some(TOKEN)).await;
    assert_eq!(get(&mut client, &[&a]).await.unwrap(), vec![a.clone()]);
    let extra = blob(b"not asked for\n");
    let status = get(&mut client, &[&extra]).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    assert_eq!(fake.calls(), 2);
}

#[tokio::test]
async fn a_reply_that_stops_early_is_asked_again_for_the_rest() {
    let objects = [blob(b"one\n"), blob(b"two\n"), blob(b"three\n")];
    let all: Vec<&Object> = objects.iter().collect();
    // Each upstream reply sends one object; the first one slowly.
    let fake = Fake::new(&all, Duration::from_millis(200)).with_mode(Mode::Short);
    let (_cache, url) = cache_for(&fake).await;
    let leader = spawn_get(&url, &objects).await;

    // The second call waits for the last object of the first call's fetch,
    // which gets it only by asking upstream again.
    let mut second = connect(&url, Some(TOKEN)).await;
    let last = &objects[2];
    assert_eq!(get(&mut second, &[last]).await.unwrap(), vec![last.clone()]);
    assert_eq!(leader.await.unwrap().unwrap(), objects);
    assert_eq!(fake.calls(), 3);
}

#[tokio::test]
async fn a_caller_that_goes_away_does_not_fail_the_calls_that_wait_with_it() {
    let (x, y) = (blob(b"x\n"), blob(b"y\n"));
    let fake = Fake::new(&[&x, &y], Duration::from_millis(300));
    let (_cache, url) = cache_for(&fake).await;
    let leader = spawn_get(&url, &[x, y.clone()]).await;
    let joiner = spawn_get(&url, std::slice::from_ref(&y)).await;
    // The first caller goes away while the fetch it started runs.
    leader.abort();

    assert_eq!(joiner.await.unwrap().unwrap(), vec![y]);
    assert_eq!(fake.calls(), 1);
}

#[tokio::test]
async fn a_failed_fetch_is_not_kept() {
    let missing = blob(b"not upstream\n");
    let fake = Fake::new(&[], Duration::ZERO);
    let (_cache, url) = cache_for(&fake).await;
    let mut client = connect(&url, Some(TOKEN)).await;
    for _ in 0..2 {
        let status = get(&mut client, &[&missing]).await.unwrap_err();
        assert_eq!(status.code(), Code::NotFound, "{status:?}");
    }
    assert_eq!(fake.calls(), 2);
}

#[tokio::test]
async fn only_a_good_root_tree_answer_is_kept() {
    let fake = Fake::default();
    let (_cache, url) = cache_for(&fake).await;
    let mut client = connect(&url, Some(TOKEN)).await;
    // By first byte: not found, a bad tree ID, a good tree ID.
    for (first, code, upstream_calls) in [
        (0, Code::NotFound, 2),
        (1, Code::Internal, 2),
        (2, Code::Ok, 1),
    ] {
        let before = fake.roots();
        for _ in 0..2 {
            let request = GetRootTreeRequest {
                repo: "r".into(),
                changeset_id: vec![first; 32],
            };
            match client.get_root_tree(request).await {
                Ok(reply) => {
                    assert_eq!(code, Code::Ok);
                    assert_eq!(reply.into_inner().tree_id, vec![7; 32]);
                }
                Err(status) => assert_eq!(status.code(), code, "{status:?}"),
            }
        }
        assert_eq!(fake.roots() - before, upstream_calls, "first byte {first}");
    }
}

#[tokio::test]
async fn a_slow_upstream_fails_the_call_with_unavailable() {
    let object = blob(b"slow\n");
    let fake = Fake::new(&[&object], Duration::from_secs(5));
    let upstream = serve_fake(fake.clone()).await;
    let (_cache, url) = serve_cache(&upstream, Duration::from_millis(50)).await;
    let mut client = connect(&url, Some(TOKEN)).await;
    let status = get(&mut client, &[&object]).await.unwrap_err();
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert!(status.message().contains("upstream did not"), "{status:?}");
}

#[tokio::test]
async fn a_call_level_failure_is_the_status_of_a_call_never_an_error_in_its_stream() {
    let (kept, other) = (blob(b"kept\n"), blob(b"not kept\n"));
    let fake = Fake::new(&[&kept, &other], Duration::ZERO);
    let (_cache, url) = cache_for(&fake).await;
    let mut client = connect(&url, Some(TOKEN)).await;
    get(&mut client, &[&kept]).await.unwrap();

    // The first object is a miss: the call's status is upstream's.
    let status = get_in(&mut client, "gone", &[&other]).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    // A hit first, then a miss: the reply sends the hit and ends without an
    // error. Asking again for the rest gets the call's status.
    let first = get_in(&mut client, "gone", &[&kept, &other]).await.unwrap();
    assert_eq!(first.unwrap(), vec![kept]);
    let status = get_in(&mut client, "gone", &[&other]).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
}
