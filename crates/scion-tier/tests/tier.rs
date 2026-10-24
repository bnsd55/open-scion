//! A tier against a fake upstream: it checks what upstream sends, fetches a
//! shared miss once, keeps one object's failure to that object, and refuses
//! callers without the token.

use std::collections::{HashMap, HashSet};
use std::os::unix::fs::PermissionsExt as _;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use scion_core::{BlobId, Entry, FileState, PathElem, Tree};
use scion_proto::CHUNK_BYTES;
use scion_proto::runtime::listen::Limits;
use scion_proto::runtime::listen::bind_unix;
use scion_proto::runtime::object::{self, Assembler, Header};
use scion_proto::runtime::tls::{ClientTls, ServerTls};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind, ObjectPart,
    ObjectRef, ResolveRefRequest, ResolveRefResponse,
};
use scion_tier::{CACHE_UPSTREAM_TIMEOUT, DiskStore, Key, MemoryStore, Store, Tier};
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
        header.parts(object::read_chunks(&self.data[..], self.data.len()).unwrap())
    }

    /// The parts that send `frame` as its zstd form.
    fn zstd(&self, frame: &[u8]) -> Vec<ObjectPart> {
        self.header(Encoding::Zstd, frame.len())
            .parts(object::read_chunks(frame, frame.len()).unwrap())
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
#[derive(Clone)]
struct Fake {
    objects: Arc<HashMap<Vec<u8>, Vec<ObjectPart>>>,
    /// The IDs whose `GetObjects` call fails whole, as a repo that
    /// upstream does not have did.
    poison: Arc<HashSet<Vec<u8>>>,
    /// The key that signs the tickets, the root tree of its tickets, and
    /// the path of each object of `new` in that tree.
    key: ed25519_dalek::SigningKey,
    root: scion_core::TreeId,
    paths: Arc<HashMap<Vec<u8>, Vec<u8>>>,
    delay: Duration,
    mode: Mode,
    calls: Arc<AtomicUsize>,
    roots: Arc<AtomicUsize>,
    /// How often each ID rode on a `GetObjects` reply, so a test can tell
    /// a joined fetch from a repeated one.
    sent: Arc<Mutex<HashMap<Vec<u8>, usize>>>,
}

impl Fake {
    /// Sends each object RAW, plus a root tree that reaches them at their
    /// index paths, so a ticket for it serves them.
    fn new(objects: &[&Object], delay: Duration) -> Self {
        let paths: Vec<Vec<u8>> = (0..objects.len())
            .map(|i| i.to_string().into_bytes())
            .collect();
        let entries: Vec<(PathElem, scion_core::Entry)> = objects
            .iter()
            .zip(&paths)
            .map(|(o, path)| {
                let state = if o.kind == i32::from(ObjectKind::Blob) {
                    scion_core::Entry::File(scion_core::FileEntry::new(
                        FileState::Regular {
                            content: BlobId::try_from(o.id.as_slice()).unwrap(),
                            executable: false,
                        },
                        0,
                    ))
                } else {
                    scion_core::Entry::Dir(scion_core::TreeId::try_from(o.id.as_slice()).unwrap())
                };
                (PathElem::new(path.clone()).unwrap(), state)
            })
            .collect();
        let (tree_bytes, root) = Tree::new(entries.into_iter().collect())
            .expect("tree fits the format")
            .encode_with_id();
        let tree = Object {
            kind: ObjectKind::Tree.into(),
            id: root.as_bytes().to_vec(),
            data: tree_bytes.clone(),
        };
        let mut all: Vec<&Object> = objects.to_vec();
        all.push(&tree);
        let paths: HashMap<_, _> = objects
            .iter()
            .zip(&paths)
            .map(|(o, path)| (o.id.clone(), path.clone()))
            .collect();
        let sent = all.iter().map(|o| (o.id.clone(), o.raw()));
        let mut fake = Self::sending(sent, delay);
        fake.root = root;
        fake.paths = Arc::new(paths);
        fake
    }

    /// Makes the tickets of this fake reach `objects` at their index
    /// paths: the root tree is served too, and its paths are kept. Parts
    /// already sent for an object stay.
    fn with_reach(self, objects: &[&Object]) -> Self {
        let paths: Vec<Vec<u8>> = (0..objects.len())
            .map(|i| i.to_string().into_bytes())
            .collect();
        let entries: Vec<(PathElem, scion_core::Entry)> = objects
            .iter()
            .zip(&paths)
            .map(|(o, path)| {
                let state = if o.kind == i32::from(ObjectKind::Blob) {
                    scion_core::Entry::File(scion_core::FileEntry::new(
                        FileState::Regular {
                            content: BlobId::try_from(o.id.as_slice()).unwrap(),
                            executable: false,
                        },
                        0,
                    ))
                } else {
                    scion_core::Entry::Dir(scion_core::TreeId::try_from(o.id.as_slice()).unwrap())
                };
                (PathElem::new(path.clone()).unwrap(), state)
            })
            .collect();
        let (tree_bytes, root) = Tree::new(entries.into_iter().collect())
            .expect("tree fits the format")
            .encode_with_id();
        let tree = Object {
            kind: ObjectKind::Tree.into(),
            id: root.as_bytes().to_vec(),
            data: tree_bytes.clone(),
        };
        let mut objects_map = (*self.objects).clone();
        objects_map
            .entry(tree.id.clone())
            .or_insert_with(|| tree.raw());
        let paths: HashMap<_, _> = objects
            .iter()
            .zip(&paths)
            .map(|(o, path)| (o.id.clone(), path.clone()))
            .collect();
        Self {
            objects: Arc::new(objects_map),
            root,
            paths: Arc::new(paths),
            ..self
        }
    }

    /// Serves `object`'s parts too, without giving it a path: a tree that
    /// another ticket's walk needs, which `path` must not name.
    fn with_object(self, object: &Object) -> Self {
        let mut objects_map = (*self.objects).clone();
        objects_map
            .entry(object.id.clone())
            .or_insert_with(|| object.raw());
        Self {
            objects: Arc::new(objects_map),
            ..self
        }
    }

    /// The signed ticket for the root tree, in repo `r`, verified for its
    /// shape so the bytes ride on as they would from the server.
    fn ticket(&self) -> Vec<u8> {
        let signed = scion_proto::ticket::sign(
            &self.key,
            "r",
            "test",
            *self.root.as_bytes(),
            scion_proto::ticket::now(),
        )
        .unwrap();
        scion_proto::ticket::parse(
            &signed,
            &[self.key.verifying_key()],
            scion_proto::ticket::now(),
        )
        .unwrap();
        signed
    }

    /// The path of `object` in the root tree, or `nowhere` for an object
    /// the tree does not reach, which the walk then refuses.
    fn path(&self, object: &Object) -> Vec<u8> {
        self.paths
            .get(&object.id)
            .cloned()
            .unwrap_or_else(|| b"nowhere".to_vec())
    }

    /// The public keys that verify this fake's tickets.
    fn keys(&self) -> Vec<ed25519_dalek::VerifyingKey> {
        vec![self.key.verifying_key()]
    }

    /// Sends the given parts for each ID, with a throwaway key whose root
    /// tree serves nothing.
    fn sending(
        sent: impl IntoIterator<Item = (Vec<u8>, Vec<ObjectPart>)>,
        delay: Duration,
    ) -> Self {
        let (key, root, tree) = Self::throwaway();
        let mut objects: HashMap<_, _> = sent.into_iter().collect();
        objects.insert(tree.id.clone(), tree.raw());
        Self {
            objects: Arc::new(objects),
            key,
            root,
            paths: Arc::default(),
            poison: Arc::default(),
            mode: Mode::default(),
            delay,
            calls: Arc::default(),
            roots: Arc::default(),
            sent: Arc::default(),
        }
    }

    /// A throwaway key and its empty root tree, for fakes whose callers
    /// never need a ticket that reaches anything.
    fn throwaway() -> (ed25519_dalek::SigningKey, scion_core::TreeId, Object) {
        let (tree_bytes, root) = Tree::default().encode_with_id();
        let tree = Object {
            kind: ObjectKind::Tree.into(),
            id: root.as_bytes().to_vec(),
            data: tree_bytes.clone(),
        };
        (ed25519_dalek::SigningKey::from_bytes(&[5; 32]), root, tree)
    }

    /// Fails the whole call when one of `ids` is asked for.
    fn with_call_fail(self, ids: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            poison: Arc::new(ids.into_iter().collect()),
            ..self
        }
    }

    fn with_mode(self, mode: Mode) -> Self {
        Self { mode, ..self }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn sent(&self, id: &[u8]) -> usize {
        self.sent.lock().unwrap().get(id).copied().unwrap_or(0)
    }

    fn sent_total(&self) -> usize {
        self.sent.lock().unwrap().values().sum()
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
        Ok(Response::new(GetRootTreeResponse {
            tree_id,
            ticket: Vec::new(),
        }))
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
        for r in &request.refs {
            *self.sent.lock().unwrap().entry(r.id.clone()).or_insert(0) += 1;
        }
        // A call for a poisoned ID fails before its reply starts, so the
        // tier sends the status of the call, not an error in its stream.
        if request.refs.iter().any(|r| self.poison.contains(&r.id)) {
            return Err(Status::not_found("no such object"));
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
async fn message_of<I>(step: &str, stream: &mut tonic::Streaming<I>) -> Result<Option<I>, Status>
where
    I: Unpin,
{
    tokio::time::timeout(std::time::Duration::from_secs(30), stream.message())
        .await
        .unwrap_or_else(|_| panic!("the step {step} never finished within 30 s"))
}

async fn get_parts(
    client: &mut Client,
    fake: &Fake,
    objects: &[&Object],
    zstd: bool,
) -> Result<Result<Vec<(Object, usize, Encoding)>, Status>, Status> {
    let refs = objects
        .iter()
        .map(|o| ObjectRef {
            kind: o.kind,
            id: o.id.clone(),
            path: fake.path(o),
        })
        .collect();
    let accept = if zstd {
        vec![Encoding::Zstd.into()]
    } else {
        Vec::new()
    };
    let request = GetObjectsRequest {
        refs,
        accept,
        ticket: fake.ticket(),
    };
    let mut stream = client.get_objects(request).await?.into_inner();
    let mut got = Vec::new();
    loop {
        let first = match message_of("the first object part", &mut stream).await {
            Ok(Some(part)) => part,
            Ok(None) => return Ok(Ok(got)),
            Err(status) => return Ok(Err(status)),
        };
        let mut object = Assembler::new(Header::of(&first, zstd).unwrap(), first);
        let mut parts = 1;
        while !object.is_done() {
            match message_of("the next object part", &mut stream).await {
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
    fake: &Fake,
    objects: &[&Object],
) -> Result<Result<Vec<Object>, Status>, Status> {
    let got = get_parts(client, fake, objects, false).await?;
    Ok(got.map(|got| got.into_iter().map(|(object, ..)| object).collect()))
}

/// The reply to a `GetObjects` call with the given refs and ticket: the
/// objects it returns, unpacked, or the error that ends it.
async fn get_with(
    client: &mut Client,
    refs: Vec<ObjectRef>,
    ticket: Vec<u8>,
) -> Result<Result<Vec<Object>, Status>, Status> {
    let request = GetObjectsRequest {
        refs,
        accept: Vec::new(),
        ticket,
    };
    let mut stream = client.get_objects(request).await?.into_inner();
    let mut got = Vec::new();
    loop {
        let first = match message_of("the first object part", &mut stream).await {
            Ok(Some(part)) => part,
            Ok(None) => return Ok(Ok(got)),
            Err(status) => return Ok(Err(status)),
        };
        let mut object = Assembler::new(Header::of(&first, false).unwrap(), first);
        while !object.is_done() {
            match message_of("the next object part", &mut stream).await {
                Ok(Some(part)) => object.push(&part).unwrap(),
                Ok(None) => panic!("the reply ended in the middle of an object"),
                Err(status) => return Ok(Err(status)),
            }
        }
        let (header, data) = object.finish();
        let data = object::decode(&header, data).unwrap();
        got.push(Object {
            kind: header.kind.into(),
            id: header.id.to_vec(),
            data,
        });
    }
}

/// A cache that waits at most `timeout` for `upstream`, and its address.
async fn serve_cache(
    upstream: &str,
    timeout: Duration,
    keys: &[ed25519_dalek::VerifyingKey],
) -> (Tier<MemoryStore>, String) {
    let store = MemoryStore::new(16 << 20);
    let cache = Tier::new(store, upstream, TOKEN, timeout, None, keys.to_vec()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(cache.clone().serve(
        TOKEN,
        incoming,
        Limits::default(),
        None,
        std::future::pending(),
    ));
    (cache, url)
}

type Client = ObjectsClient<InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Bearer + Send>>>;
type Bearer = Result<Request<()>, Status>;

/// A client that sends `token`, or no token.
async fn connect(url: &str, token: Option<&str>) -> Client {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        // A server or cache that died mid-reply must not leave the test
        // waiting forever: the dial and every call are bounded.
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
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
async fn get(client: &mut Client, fake: &Fake, objects: &[&Object]) -> Result<Vec<Object>, Status> {
    get_in(client, fake, objects).await?
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
    let tree = Tree::new(
        [(
            PathElem::new("f").unwrap(),
            Entry::File(scion_core::FileEntry::new(
                FileState::Regular {
                    content: BlobId::of(b"good\n"),
                    executable: false,
                },
                b"good\n".len() as u64,
            )),
        )]
        .into(),
    )
    .expect("tree fits the format");
    let bad_tree = Object {
        kind: ObjectKind::Tree.into(),
        id: tree.id().as_bytes().to_vec(),
        data: Tree::default().encode(),
    };
    let fake = Fake::new(&[&good, &bad_blob, &bad_tree], Duration::ZERO);
    let upstream = serve_fake(fake.clone()).await;
    let (_cache, url) = serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT, &fake.keys()).await;
    let mut client = connect(&url, Some(TOKEN)).await;

    // A good object passes and is kept, so the second call stays in the cache.
    for _ in 0..2 {
        assert_eq!(
            get(&mut client, &fake, &[&good]).await.unwrap(),
            vec![good.clone()]
        );
    }
    // The root tree of the walk, then the object.
    assert_eq!(fake.calls(), 2);

    for wrong in [&bad_blob, &bad_tree] {
        let before = fake.calls();
        for _ in 0..2 {
            let status = get(&mut client, &fake, &[wrong]).await.unwrap_err();
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
    bad.push((
        "other bytes",
        target.zstd(&object::pack(b"evil\n").unwrap()),
    ));
    // The object's size says one byte more than the frame header.
    let mut parts = target.zstd(&frame);
    parts[0].size += 1;
    bad.push(("size above the header", parts));
    // The frame header says one byte more than the frame holds. Byte 5 is the
    // content size of a small frame.
    let mut lying = frame.clone();
    lying[5] += 1;
    assert_eq!(object::frame_size(&lying), Ok(content.len() + 1));
    let mut parts = target.zstd(&lying);
    parts[0].size += 1;
    bad.push(("a lying frame header", parts));
    // A frame with no size in its header.
    let mut streamed = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
    std::io::Write::write_all(&mut streamed, content).unwrap();
    bad.push(("no size", target.zstd(&streamed.finish().unwrap())));
    // Two parts announced, one sent: the reply ends in the middle.
    let big = blob(&noise(CHUNK_BYTES + 10));
    let mut parts = big.raw();
    parts.truncate(1);
    bad.push(("a truncated part sequence", parts));
    // A later part past the encoded size, and an empty one.
    let mut parts = big.raw();
    parts[1].data = vec![0; 11].into();
    bad.push(("a part past the end", parts));
    let mut parts = big.raw();
    parts[1].data = scion_proto::Bytes::new();
    bad.push(("an empty part", parts));

    for (what, parts) in bad {
        let id = parts[0].id.clone();
        let wanted = Object {
            id: id.clone(),
            ..blob(b"")
        };
        let fake = Fake::sending([(id, parts)], Duration::ZERO).with_reach(&[&wanted]);
        let (_cache, url) = cache_for(&fake).await;
        let mut client = connect(&url, Some(TOKEN)).await;
        for zstd in [false, true] {
            let status = get_parts(&mut client, &fake, &[&wanted], zstd)
                .await
                .and_then(|got| got)
                .unwrap_err();
            assert_eq!(status.code(), Code::DataLoss, "{what}: {status:?}");
        }
        // Not kept: each call asked upstream again.
        assert_eq!(fake.calls(), 3, "{what}");
    }
}

#[tokio::test]
async fn a_large_blob_arrives_in_parts_is_checked_and_sent_on_as_it_came() {
    let big = blob(&noise(3 * CHUNK_BYTES + 5));
    let frame = object::pack(&big.data).unwrap();
    assert!(frame.len() > 3 * CHUNK_BYTES);
    let fake =
        Fake::sending([(big.id.clone(), big.zstd(&frame))], Duration::ZERO).with_reach(&[&big]);
    let (_cache, url) = cache_for(&fake).await;
    let mut client = connect(&url, Some(TOKEN)).await;

    // A client that accepts zstd gets the frame in four parts; another gets
    // the content in four parts. Both from one fetch.
    for (zstd, encoding) in [(true, Encoding::Zstd), (false, Encoding::Raw)] {
        let got = get_parts(&mut client, &fake, &[&big], zstd).await.unwrap();
        assert_eq!(got.unwrap(), vec![(big.clone(), 4, encoding)]);
    }
    // The root tree of the walk, then the object, once.
    assert_eq!(fake.calls(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_misses_for_one_object_share_one_fetch() {
    let object = blob(b"wanted by every job\n");
    // Slow, so every client misses while the first fetch runs.
    let fake = Fake::new(&[&object], Duration::from_millis(300));
    let upstream = serve_fake(fake.clone()).await;
    let (cache, url) = serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT, &fake.keys()).await;

    let mut clients = Vec::new();
    for _ in 0..10 {
        let mut client = connect(&url, Some(TOKEN)).await;
        let object = object.clone();
        let fake = fake.clone();
        clients.push(tokio::spawn(async move {
            get(&mut client, &fake, &[&object]).await
        }));
    }
    for client in clients {
        assert_eq!(client.await.unwrap().unwrap(), vec![object.clone()]);
    }
    // One fetch of the object, shared, plus the root tree of the walk.
    // Each job mints its own ticket (get() signs a fresh one), so the
    // flight is joined across tickets, not within one.
    assert_eq!(fake.calls(), 2);
    assert_eq!(cache.upstream_calls(), 2);
}

#[tokio::test]
async fn each_tier_refuses_a_call_without_the_token() {
    let fake = Fake::sending([], Duration::ZERO);
    let upstream = serve_fake(fake.clone()).await;
    let (cache, cache_url) = serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT, &fake.keys()).await;
    let dir = tempfile::tempdir().unwrap();
    // The socket's directory must be private.
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("daemon.sock");
    let store = DiskStore::open(&dir.path().join("data"), 1 << 20).unwrap();
    let daemon = Tier::new(
        store,
        &cache_url,
        TOKEN,
        CACHE_UPSTREAM_TIMEOUT,
        None,
        Vec::new(),
    )
    .unwrap();
    let incoming = UnixListenerStream::new(bind_unix(&socket).await.unwrap());
    tokio::spawn(daemon.clone().serve(
        TOKEN,
        incoming,
        Limits::default(),
        None,
        std::future::pending(),
    ));

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
    // Through a symlinked directory, the socket is bound where the link
    // pointed at the check, and its address names that path. The daemon logs
    // and removes that path, not one through the link.
    let private = dir.path().join("private");
    std::fs::create_dir(&private).unwrap();
    std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700)).unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(dir.path(), &link).unwrap();
    let listener = bind_unix(&link.join("private/other.sock")).await.unwrap();
    let bound = listener.local_addr().unwrap();
    let real = std::fs::canonicalize(&private).unwrap().join("other.sock");
    assert_eq!(bound.as_pathname(), Some(real.as_path()));

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
            let status = get(&mut client, &fake, &[&blob(b"x")]).await.unwrap_err();
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
    // No tier forwards refs or root trees; the daemon answers by itself.
    assert_eq!((daemon.upstream_calls(), cache.upstream_calls()), (0, 0));
}

/// A fake upstream and a cache in front of it. Returns the cache and its
/// address.
async fn cache_for(fake: &Fake) -> (Tier<MemoryStore>, String) {
    let upstream = serve_fake(fake.clone()).await;
    serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT, &fake.keys()).await
}

/// Waits until the fake's counters stop moving, so an assert sees the final
/// counts, not the middle of a background chain. 10 s ceiling.
async fn quiesce(fake: &Fake) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let before = (fake.calls(), fake.roots(), fake.sent_total());
        tokio::time::sleep(Duration::from_millis(50)).await;
        if before == (fake.calls(), fake.roots(), fake.sent_total()) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the counters never settled"
        );
    }
}

/// Starts a `GetObjects` call for `objects` from a new client of `url`, and
/// gives the first upstream call time to start.
async fn spawn_get(
    url: &str,
    fake: &Fake,
    objects: &[Object],
) -> tokio::task::JoinHandle<Result<Vec<Object>, Status>> {
    let mut client = connect(url, Some(TOKEN)).await;
    let wanted: Vec<Vec<u8>> = objects.iter().map(|o| o.id.clone()).collect();
    let objects = objects.to_vec();
    let fake = fake.clone();
    let server = fake.clone();
    let call = tokio::spawn(async move {
        get(&mut client, &server, &objects.iter().collect::<Vec<_>>()).await
    });
    // Every object of this call must have reached upstream before the
    // caller goes on: the tier registers each object's flight before it
    // asks upstream, so a later call finds the flights and joins them
    // instead of starting its own fetch. Wait for the state, not for a
    // duration.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for id in wanted {
        while fake.sent(id.as_slice()) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the first call never reached upstream"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    call
}

#[tokio::test]
async fn one_missing_object_fails_only_itself() {
    let (missing, there) = (blob(b"not upstream\n"), blob(b"upstream\n"));
    // Slow, so the second call joins the first call's fetch.
    let fake = Fake::new(&[&there], Duration::from_millis(300));
    let (_cache, url) = cache_for(&fake).await;
    // No join wait for this leader: its chain may stop at the missing
    // object before it asks `there` at all.
    let mut leader_client = connect(&url, Some(TOKEN)).await;
    let fake_for_leader = fake.clone();
    let there_for_leader = there.clone();
    let leader = tokio::spawn(async move {
        get(
            &mut leader_client,
            &fake_for_leader,
            &[&missing, &there_for_leader],
        )
        .await
    });

    // The second call waits for the same fetch, and gets its object.
    let mut second = connect(&url, Some(TOKEN)).await;
    assert_eq!(
        get(&mut second, &fake, &[&there]).await.unwrap(),
        vec![there.clone()]
    );
    let status = leader.await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    quiesce(&fake).await;
    // One call for both objects, then one for the object after the missing one.
    assert_eq!(fake.calls(), 2);
    // The join: `there` rode on the first call's reply alone. Had the
    // second call fetched alone, it would have ridden twice.
    assert_eq!(fake.sent(there.id.as_slice()), 1, "there was fetched twice");
}

#[tokio::test]
async fn objects_that_upstream_should_not_send_are_refused_and_not_kept() {
    let (a, b) = (blob(b"a\n"), blob(b"b\n"));
    for mode in [Mode::WrongKind, Mode::Reversed] {
        let fake = Fake::new(&[&a, &b], Duration::ZERO).with_mode(mode);
        let (_cache, url) = cache_for(&fake).await;
        let mut client = connect(&url, Some(TOKEN)).await;
        for _ in 0..2 {
            let status = get(&mut client, &fake, &[&a, &b]).await.unwrap_err();
            assert_eq!(status.code(), Code::Internal, "{mode:?}: {status:?}");
        }
        // The root tree of the walk, then each refused call again.
        assert_eq!(fake.calls(), 3, "{mode:?}: nothing was kept");
    }

    // An object after the last one asked for: the objects asked for pass,
    // and the extra one is not kept.
    let fake = Fake::new(&[&a], Duration::ZERO).with_mode(Mode::Extra);
    let (_cache, url) = cache_for(&fake).await;
    let mut client = connect(&url, Some(TOKEN)).await;
    assert_eq!(
        get(&mut client, &fake, &[&a]).await.unwrap(),
        vec![a.clone()]
    );
    let extra = blob(b"not asked for\n");
    let status = get(&mut client, &fake, &[&extra]).await.unwrap_err();
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
    let leader = spawn_get(&url, &fake, &objects).await;

    // The second call waits for the last object of the first call's fetch,
    // which gets it only by asking upstream again. The short replies make
    // both callers' chains refetch `three`, so no count of the requests
    // that carry it is stable; what the test pins is the final call count.
    let mut second = connect(&url, Some(TOKEN)).await;
    let last = &objects[2];
    assert_eq!(
        get(&mut second, &fake, &[last]).await.unwrap(),
        vec![last.clone()]
    );
    assert_eq!(leader.await.unwrap().unwrap(), objects);
    quiesce(&fake).await;
    // The root tree of the walk, then the three fetches the reply needed.
    assert_eq!(fake.calls(), 4);
}

#[tokio::test]
async fn a_caller_that_goes_away_does_not_fail_the_calls_that_wait_with_it() {
    let (x, y) = (blob(b"x\n"), blob(b"y\n"));
    let fake = Fake::new(&[&x, &y], Duration::from_millis(300));
    let (_cache, url) = cache_for(&fake).await;
    let leader = spawn_get(&url, &fake, &[x, y.clone()]).await;
    let joiner = spawn_get(&url, &fake, std::slice::from_ref(&y)).await;
    // The first caller goes away while the fetch it started runs.
    leader.abort();

    assert_eq!(joiner.await.unwrap().unwrap(), vec![y.clone()]);
    quiesce(&fake).await;
    // The root tree of the walk, then the shared fetch of both objects.
    assert_eq!(fake.calls(), 2);
    // The join: `y` rode on the leader's call alone, which the joiner
    // survived; had it fetched alone, `y` would have ridden twice.
    assert_eq!(fake.sent(y.id.as_slice()), 1, "y was fetched twice");
}

#[tokio::test]
async fn a_failed_fetch_is_not_kept() {
    let missing = blob(b"not upstream\n");
    let fake = Fake::new(&[], Duration::ZERO);
    let (_cache, url) = cache_for(&fake).await;
    let mut client = connect(&url, Some(TOKEN)).await;
    for _ in 0..2 {
        let status = get(&mut client, &fake, &[&missing]).await.unwrap_err();
        assert_eq!(status.code(), Code::NotFound, "{status:?}");
    }
    // The walk refuses what the ticket's tree does not reach, so only the
    // root tree of the first call is ever fetched; the second call walks
    // from the kept tree and refuses without upstream.
    assert_eq!(fake.calls(), 1);
}

#[tokio::test]
async fn a_tier_serves_no_root_trees_or_refs() {
    // The caller gets its ticket from the server, never from a tier: a tier
    // that fetched root trees for callers would be a way to get a ticket
    // for any repo.
    let object = blob(b"nothing here\n");
    let fake = Fake::new(&[&object], Duration::ZERO);
    let (_cache, url) = cache_for(&fake).await;
    let mut client = connect(&url, Some(TOKEN)).await;
    let request = GetRootTreeRequest {
        repo: "r".into(),
        changeset_id: vec![2; 32],
    };
    let status = client.get_root_tree(request).await.unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented, "{status:?}");
    let resolve = ResolveRefRequest {
        repo: "r".into(),
        name: "main".into(),
    };
    let status = client.resolve_ref(resolve).await.unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented, "{status:?}");
    assert_eq!(fake.roots(), 0);
}

#[tokio::test]
async fn a_slow_upstream_fails_the_call_with_unavailable() {
    let object = blob(b"slow\n");
    let fake = Fake::new(&[&object], Duration::from_secs(5));
    let upstream = serve_fake(fake.clone()).await;
    let (_cache, url) = serve_cache(&upstream, Duration::from_millis(50), &fake.keys()).await;
    let mut client = connect(&url, Some(TOKEN)).await;
    let status = get(&mut client, &fake, &[&object]).await.unwrap_err();
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert!(status.message().contains("upstream did not"), "{status:?}");
}

#[tokio::test]
async fn a_call_level_failure_is_the_status_of_a_call_never_an_error_in_its_stream() {
    let (kept, other) = (blob(b"kept\n"), blob(b"not kept\n"));
    let fake = Fake::new(&[&kept, &other], Duration::ZERO).with_call_fail([other.id.clone()]);
    let (_cache, url) = cache_for(&fake).await;
    let mut client = connect(&url, Some(TOKEN)).await;
    get(&mut client, &fake, &[&kept]).await.unwrap();

    // The first object is a miss: the call's status is upstream's.
    let status = get_in(&mut client, &fake, &[&other]).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    // A hit first, then a miss: the reply sends the hit and ends without an
    // error. Asking again for the rest gets the call's status.
    let first = get_in(&mut client, &fake, &[&kept, &other]).await.unwrap();
    assert_eq!(first.unwrap(), vec![kept]);
    let status = get_in(&mut client, &fake, &[&other]).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
}

// The certificates here and in scion-proto's listen tests are separate, so
// each crate's tests stand alone.

/// A leaf certificate and its key, both PEM files in a temp directory.
struct TestCert {
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
}

/// A certificate authority for one test: it signs the server and client
/// certificates, and its own certificate is what a peer trusts.
struct TestAuthority {
    dir: tempfile::TempDir,
    ca: TestCert,
    key: rcgen::KeyPair,
    params: rcgen::CertificateParams,
}

impl TestAuthority {
    /// A CA with no name that a test needs.
    fn new(name: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        let ca = write_pem(
            &dir,
            &format!("{name}-ca"),
            &cert.pem(),
            &key.serialize_pem(),
        );
        Self {
            dir,
            ca,
            key,
            params,
        }
    }

    /// Signs a leaf for `names`, used as `purpose`.
    fn sign(
        &self,
        name: &str,
        names: &[&str],
        purpose: rcgen::ExtendedKeyUsagePurpose,
    ) -> TestCert {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(
            names
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        params.extended_key_usages = vec![purpose];
        let issuer = rcgen::Issuer::from_params(&self.params, &self.key);
        let cert = params.signed_by(&key, &issuer).unwrap();
        write_pem(&self.dir, name, &cert.pem(), &key.serialize_pem())
    }
}

/// Writes a certificate and its key into `dir`, as `name.pem` and
/// `name.key`.
fn write_pem(dir: &tempfile::TempDir, name: &str, cert: &str, key: &str) -> TestCert {
    use std::os::unix::fs::PermissionsExt as _;
    let cert_path = dir.path().join(format!("{name}.pem"));
    std::fs::write(&cert_path, cert).unwrap();
    // A certificate is a shared file: the checked read refuses one that
    // group or others can write, whatever the umask of the run.
    std::fs::set_permissions(&cert_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let key_path = dir.path().join(format!("{name}.key"));
    std::fs::write(&key_path, key).unwrap();
    // A key is a secret: the checked read refuses one that group or
    // others can read.
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    TestCert {
        cert: cert_path,
        key: key_path,
    }
}

/// The certificates of one test: an authority, its server and client
/// certificates, and a second authority with a client certificate of its
/// own, so a test can trust or present the wrong one.
struct TestCerts {
    authority: TestAuthority,
    server: TestCert,
    client: TestCert,
    /// Kept for its temp directory, which holds the client certificate's
    /// files.
    _stranger: TestAuthority,
    stranger_client: TestCert,
}

fn test_certs() -> TestCerts {
    let authority = TestAuthority::new("main");
    let server = authority.sign(
        "server",
        &["localhost", "127.0.0.1"],
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
    );
    let client = authority.sign(
        "client",
        &["client"],
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    );
    let stranger = TestAuthority::new("stranger");
    let stranger_client = stranger.sign(
        "stranger-client",
        &["client"],
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    );
    TestCerts {
        authority,
        server,
        client,
        _stranger: stranger,
        stranger_client,
    }
}

/// A cache that serves `https://` with the certificate of `certs`, and, with
/// `client_ca`, requires a client certificate that CA signed. Its https
/// address.
async fn serve_tls_cache(
    upstream: &str,
    certs: &TestCerts,
    client_ca: Option<&TestCert>,
    keys: &[ed25519_dalek::VerifyingKey],
) -> String {
    let tls = ServerTls::load(
        &certs.server.cert,
        &certs.server.key,
        client_ca.map(|ca| ca.cert.as_path()),
    )
    .unwrap();
    let cache = Tier::new(
        MemoryStore::new(16 << 20),
        upstream,
        TOKEN,
        CACHE_UPSTREAM_TIMEOUT,
        None,
        keys.to_vec(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://{}", listener.local_addr().unwrap());
    tokio::spawn(cache.serve(
        TOKEN,
        TcpListenerStream::new(listener),
        Limits::default(),
        Some(tls),
        std::future::pending(),
    ));
    url
}

/// A daemon whose upstream is `cache_url`, trusting `ca` and presenting
/// `client` (a client certificate) for mTLS, on a private Unix socket. Its
/// `unix://` address, and the directory that keeps the socket alive. `client`
/// is `None` for a daemon without one.
async fn serve_daemon(
    cache_url: &str,
    ca: Option<&TestCert>,
    client: Option<&TestCert>,
) -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("daemon.sock");
    let store = DiskStore::open(&dir.path().join("data"), 1 << 20).unwrap();
    let client_tls = ClientTls::load(
        ca.map(|ca| ca.cert.as_path()),
        client.map(|c| c.cert.as_path()),
        client.map(|c| c.key.as_path()),
    )
    .unwrap();
    let daemon = Tier::new(
        store,
        cache_url,
        TOKEN,
        CACHE_UPSTREAM_TIMEOUT,
        Some(&client_tls),
        Vec::new(),
    )
    .unwrap();
    let incoming = UnixListenerStream::new(bind_unix(&socket).await.unwrap());
    tokio::spawn(daemon.serve(
        TOKEN,
        incoming,
        Limits::default(),
        None,
        std::future::pending(),
    ));
    (format!("unix://{}", socket.display()), dir)
}

/// A daemon fetches its miss from an mTLS cache over https, with the client
/// certificate that the cache's CA signed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_daemon_fetches_from_an_mtls_cache_over_https() {
    let object = blob(b"fetched through mutual TLS\n");
    let fake = Fake::new(&[&object], Duration::ZERO);
    let upstream = serve_fake(fake.clone()).await;
    let certs = test_certs();
    let cache_url =
        serve_tls_cache(&upstream, &certs, Some(&certs.authority.ca), &fake.keys()).await;
    let (daemon_url, _dir) =
        serve_daemon(&cache_url, Some(&certs.authority.ca), Some(&certs.client)).await;
    let mut client = connect(&daemon_url, Some(TOKEN)).await;
    assert_eq!(
        get(&mut client, &fake, &[&object]).await.unwrap(),
        vec![object]
    );
}

/// A daemon without a client certificate fetches nothing from an mTLS cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_daemon_without_a_client_certificate_is_refused() {
    let object = blob(b"never fetched\n");
    let fake = Fake::new(&[&object], Duration::ZERO);
    let upstream = serve_fake(fake.clone()).await;
    let certs = test_certs();
    let cache_url =
        serve_tls_cache(&upstream, &certs, Some(&certs.authority.ca), &fake.keys()).await;
    let (daemon_url, _dir) = serve_daemon(&cache_url, Some(&certs.authority.ca), None).await;
    let mut client = connect(&daemon_url, Some(TOKEN)).await;
    assert!(get(&mut client, &fake, &[&object]).await.is_err());
}

/// A daemon whose client certificate another CA signed fetches nothing from
/// an mTLS cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_daemon_with_a_certificate_of_another_ca_is_refused() {
    let object = blob(b"never fetched either\n");
    let fake = Fake::new(&[&object], Duration::ZERO);
    let upstream = serve_fake(fake.clone()).await;
    let certs = test_certs();
    let cache_url =
        serve_tls_cache(&upstream, &certs, Some(&certs.authority.ca), &fake.keys()).await;
    let (daemon_url, _dir) = serve_daemon(
        &cache_url,
        Some(&certs.authority.ca),
        Some(&certs.stranger_client),
    )
    .await;
    let mut client = connect(&daemon_url, Some(TOKEN)).await;
    assert!(get(&mut client, &fake, &[&object]).await.is_err());
}

/// An https upstream without a CA to trust is refused when the tier is
/// built, not on its first call.
#[tokio::test]
async fn an_https_upstream_needs_a_ca() {
    let store = MemoryStore::new(1 << 20);
    let tier = Tier::new(
        store,
        "https://127.0.0.1:1",
        TOKEN,
        CACHE_UPSTREAM_TIMEOUT,
        None,
        Vec::new(),
    );
    assert!(tier.is_err());
}

/// A daemon checks no ticket and runs no walk: a miss forwards the ticket
/// and the path upstream and fetches the object, with no fetch of the
/// ticket's root tree. One upstream call per miss.
#[tokio::test]
async fn a_daemon_miss_fetches_the_object_only() {
    let object = blob(b"served unproved to its one user\n");
    let fake = Fake::new(&[&object], Duration::ZERO);
    let upstream = serve_fake(fake.clone()).await;
    let (cache, cache_url) = serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT, &fake.keys()).await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("daemon.sock");
    let store = DiskStore::open(&dir.path().join("data"), 1 << 20).unwrap();
    // No ticket keys: a daemon, which checks no ticket and runs no walk.
    let daemon = Tier::new(
        store,
        &cache_url,
        TOKEN,
        CACHE_UPSTREAM_TIMEOUT,
        None,
        Vec::new(),
    )
    .unwrap();
    let incoming = UnixListenerStream::new(bind_unix(&socket).await.unwrap());
    tokio::spawn(daemon.clone().serve(
        TOKEN,
        incoming,
        Limits::default(),
        None,
        std::future::pending(),
    ));
    let mut client = connect(&format!("unix://{}", socket.display()), Some(TOKEN)).await;
    assert_eq!(
        get(&mut client, &fake, &[&object]).await.unwrap(),
        vec![object]
    );
    // The daemon fetched the object through the cache, and no root tree:
    // its own upstream calls are the object's fetch alone.
    assert_eq!(daemon.upstream_calls(), 1, "no walk, so no tree fetch");
    assert_eq!(fake.calls(), 2, "the cache fetched the tree and the object");
    // A miss with a ticket whose tree reaches nothing is refused by the
    // cache's walk before anything is fetched: the daemon forwards, the
    // upstream proves.
    let stranger = blob(b"not in the ticket's tree\n");
    let status = get(&mut client, &fake, &[&stranger]).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    assert_eq!(fake.calls(), 2, "the cache fetched nothing for it");
    let _ = cache;
}

/// A partial reader's view ticket cannot reach a blob that the cache
/// holds for a whole-repo reader: the walk from the ticket's root tree
/// refuses it as missing, and the answer is byte for byte the answer a
/// truly missing path gets. Through a daemon, which runs no walk of its
/// own, the cache behind it refuses too.
#[tokio::test]
async fn a_view_ticket_cannot_reach_a_warm_blob_outside_its_view() {
    let secret = blob(b"docs/secret, outside the view\n");
    // The fake's whole-repo tree reaches the blob at its index path; its
    // tickets name that root, so alice's fetch proves and warms it.
    let fake = Fake::new(&[&secret], Duration::ZERO);
    // bob's view: a root tree that lists `src` only, signed by the same
    // key and served, so the walk of his ticket can read it.
    let (view_bytes, view_root) = Tree::new(
        [(
            PathElem::new(b"src".to_vec()).unwrap(),
            Entry::Dir(Tree::default().encode_with_id().1),
        )]
        .into(),
    )
    .expect("tree fits the format")
    .encode_with_id();
    let view_tree = Object {
        kind: ObjectKind::Tree.into(),
        id: view_root.as_bytes().to_vec(),
        data: view_bytes,
    };
    let fake = fake.with_object(&view_tree);
    let upstream = serve_fake(fake.clone()).await;
    let (_cache, url) = serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT, &fake.keys()).await;
    let bob_ticket = scion_proto::ticket::sign(
        &fake.key,
        "r",
        "bob",
        *view_root.as_bytes(),
        scion_proto::ticket::now(),
    )
    .unwrap();

    // alice (whole repo) warms the cache; her second call is a hit.
    let mut alice = connect(&url, Some(TOKEN)).await;
    get(&mut alice, &fake, &[&secret]).await.unwrap();
    get(&mut alice, &fake, &[&secret]).await.unwrap();
    assert_eq!(fake.calls(), 2, "the second call stayed in the cache");

    // bob asks for the warm blob over its true path, with his view
    // ticket: refused, though the blob sits in the cache.
    let mut bob = connect(&url, Some(TOKEN)).await;
    let refs = |path: Vec<u8>| {
        vec![ObjectRef {
            kind: secret.kind,
            id: secret.id.clone(),
            path,
        }]
    };
    let hidden = get_with(&mut bob, refs(fake.path(&secret)), bob_ticket.clone())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(hidden.code(), Code::NotFound, "{hidden:?}");
    assert!(hidden.message().contains("is not in the ticket"));

    // The same call for a path that nothing reaches: the same answer,
    // byte for byte. Hidden equals missing at the tier.
    let missing = get_with(&mut bob, refs(b"nowhere".to_vec()), bob_ticket.clone())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound, "{missing:?}");
    assert_eq!(
        hidden.message(),
        missing.message(),
        "hidden must equal missing"
    );

    // The walk fetched only the trees on the way, never the blob.
    assert_eq!(fake.calls(), 3, "the blob was never fetched for bob");

    // And a second ask stays refused: nothing was learned or stored for
    // bob's ticket.
    let again = get_with(&mut bob, refs(fake.path(&secret)), bob_ticket)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(again.code(), Code::NotFound, "{again:?}");
}

/// The same refusal through a daemon: the daemon runs no walk, so the
/// cache behind it is the check. It refuses with the blob unfetched.
#[tokio::test]
async fn a_daemons_cache_still_refuses_a_view_ticket_outside_its_view() {
    let secret = blob(b"docs/secret, outside the view\n");
    let fake = Fake::new(&[&secret], Duration::ZERO);
    let (view_bytes, view_root) = Tree::new(
        [(
            PathElem::new(b"src".to_vec()).unwrap(),
            Entry::Dir(Tree::default().encode_with_id().1),
        )]
        .into(),
    )
    .expect("tree fits the format")
    .encode_with_id();
    let view_tree = Object {
        kind: ObjectKind::Tree.into(),
        id: view_root.as_bytes().to_vec(),
        data: view_bytes,
    };
    let fake = fake.with_object(&view_tree);
    let upstream = serve_fake(fake.clone()).await;
    let (cache, cache_url) = serve_cache(&upstream, CACHE_UPSTREAM_TIMEOUT, &fake.keys()).await;
    let bob_ticket = scion_proto::ticket::sign(
        &fake.key,
        "r",
        "bob",
        *view_root.as_bytes(),
        scion_proto::ticket::now(),
    )
    .unwrap();

    // alice warms the cache through her own client.
    let mut alice = connect(&cache_url, Some(TOKEN)).await;
    get(&mut alice, &fake, &[&secret]).await.unwrap();
    let warmed = fake.calls();

    // bob's daemon: no ticket keys, so no walk of its own.
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("daemon.sock");
    let store = DiskStore::open(&dir.path().join("data"), 1 << 20).unwrap();
    let daemon = Tier::new(
        store,
        &cache_url,
        TOKEN,
        CACHE_UPSTREAM_TIMEOUT,
        None,
        Vec::new(),
    )
    .unwrap();
    let incoming = UnixListenerStream::new(bind_unix(&socket).await.unwrap());
    tokio::spawn(daemon.clone().serve(
        TOKEN,
        incoming,
        Limits::default(),
        None,
        std::future::pending(),
    ));
    let mut bob = connect(&format!("unix://{}", socket.display()), Some(TOKEN)).await;
    let refs = vec![ObjectRef {
        kind: secret.kind,
        id: secret.id.clone(),
        path: fake.path(&secret),
    }];
    let hidden = get_with(&mut bob, refs, bob_ticket)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(hidden.code(), Code::NotFound, "{hidden:?}");
    assert!(hidden.message().contains("is not in the ticket"));
    // The cache walked bob's ticket: only the view tree was fetched.
    assert_eq!(
        fake.calls(),
        warmed + 1,
        "the cache fetched the view tree, never the blob"
    );
    let _ = cache;
}

/// Starts the real daemon on `data`, and returns it with the URL of its
/// socket, which it opens within 10 s.
async fn start_daemon(
    data: &std::path::Path,
    socket: &std::path::Path,
    upstream: &str,
    limit: usize,
) -> (Kill, String) {
    use std::time::Instant;

    use std::process::Command;
    let child = Command::new(env!("CARGO_BIN_EXE_scion-daemon"))
        .arg("--data")
        .arg(data)
        .arg("--socket")
        .arg(socket)
        .arg("--upstream")
        .arg(upstream)
        .arg("--max-bytes")
        .arg(limit.to_string())
        .arg("--token")
        .arg(TOKEN)
        .arg("--upstream-token")
        .arg(TOKEN)
        .spawn()
        .map(Kill)
        .unwrap();
    // The child is up when its socket file is there (10 s ceiling).
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(
            Instant::now() < deadline,
            "the daemon never opened its socket"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    (child, format!("unix://{}", socket.display()))
}

/// Kills the daemon when the test ends, however it ends: a leftover child
/// holds the captured output open and the harness hangs.
struct Kill(std::process::Child);
impl Drop for Kill {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A daemon killed with SIGKILL while it fills its store, and restarted:
/// no torn object is ever served, the index and the files agree after the
/// restart, the byte limit still holds, and reads go on
/// (docs/architecture.md, Cache tiers: after a crash the tier opens and
/// checks).
///
/// The cut falls between file writes, and the test says where plainly: a
/// stored file reaches its final name only when a durable temp file is
/// renamed, so the objects that the store misses at the kill are the cut.
/// The kill lands when three files are on disk of two hundred that the
/// fill is still writing; the test asserts the store missed many, so a
/// clean stop cannot pass unseen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_daemon_killed_mid_fill_serves_no_torn_object_after_its_restart() {
    use std::time::Instant;

    // Two hundred blobs of about 64 KiB each: a fill whose file writes
    // take many polls, in a store that must evict to hold its limit.
    let fill: Vec<Object> = (0..200).map(|i| blob(&noise(64 * 1024 + i))).collect();
    let limit = 8 << 20;
    let fake = Fake::new(&fill.iter().collect::<Vec<_>>(), Duration::ZERO);
    let upstream = serve_fake(fake.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("daemon.sock");
    let data = dir.path().join("data");

    let (mut child, url) = start_daemon(&data, &socket, &upstream, limit).await;
    let fill_task = spawn_get(&url, &fake, &fill).await;

    // The kill lands while the fill writes: the store's object tree holds
    // three files of the two hundred still coming.
    let objects = data.join("objects");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if count_files(&objects) >= 3 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the store never wrote a file during the fill"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = fill_task.await;
    child.0.kill().unwrap();
    child.0.wait().unwrap();

    // The cut is real: files landed, and the fill had far more to write.
    let files = stored_files(&objects);
    let landed = files.iter().filter(|(kind, ..)| kind == "blob").count();
    assert!(landed >= 3, "the kill landed before the fill wrote");
    assert!(
        landed < fill.len(),
        "the kill landed after the whole fill: {landed} files"
    );

    // Restarted on the same directory, the tier serves every object with
    // its own bytes: the copies that the kill cut arrive from upstream
    // again, and no torn file is served on the way (reads go on).
    let store = DiskStore::open(&data, limit).unwrap();
    let restarted = Tier::new(
        store,
        &upstream,
        TOKEN,
        CACHE_UPSTREAM_TIMEOUT,
        None,
        Vec::new(),
    )
    .unwrap();
    let socket = dir.path().join("daemon-restarted.sock");
    let incoming = UnixListenerStream::new(bind_unix(&socket).await.unwrap());
    let serve_task = tokio::spawn(restarted.clone().serve(
        TOKEN,
        incoming,
        Limits::default(),
        None,
        std::future::pending(),
    ));
    let mut client = connect(&format!("unix://{}", socket.display()), Some(TOKEN)).await;
    let got = get(&mut client, &fake, &fill.iter().collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(got, fill, "every object answers, byte for byte");

    // The restart's store lets go of the directory before the check opens
    // it again: the abort drops the serving tier, and the open waits for
    // the lock (10 s ceiling).
    serve_task.abort();
    drop(restarted);
    let deadline = Instant::now() + Duration::from_secs(10);
    let store = loop {
        match DiskStore::open(&data, limit) {
            Ok(store) => break store,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "the restarted store never let go of the directory"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => panic!("the reopened store never opened: {error}"),
        }
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if files.iter().all(|file| agrees(&store, file)) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the walk never adopted the files that the kill stranded"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // No stored file is torn: each unpacks to bytes that hash to the ID
    // in its name. The tier's serve-time check would drop a bad one and
    // fetch it again; this asserts none is bad in the first place.
    for (kind, id, path, _) in &files {
        let body = std::fs::read(path).unwrap();
        if kind == "blob" {
            unpacks_to_its_id(&body, id);
        }
    }

    // The byte limit still holds, after the walk and the fills.
    let kept: u64 = files.iter().map(|(.., len)| *len).sum();
    assert!(
        kept <= limit as u64,
        "the store holds {kept} bytes over its limit"
    );
}

/// A stored blob unpacks, to bytes that hash to the ID in its name.
fn unpacks_to_its_id(body: &[u8], id: &[u8]) {
    use std::io::Read as _;
    let mut data = Vec::new();
    object::unpacker(body)
        .expect("a stored blob unpacks")
        .read_to_end(&mut data)
        .expect("the stored blob is whole");
    assert_eq!(&BlobId::of(&data).as_bytes()[..], id, "the ID matches");
}

/// The files of a store's object tree, as `(kind, id, path, len)`.
fn stored_files(objects: &std::path::Path) -> Vec<(String, Vec<u8>, std::path::PathBuf, u64)> {
    let mut files = Vec::new();
    for kind in ["tree", "blob"] {
        let Ok(firsts) = std::fs::read_dir(objects.join(kind)) else {
            continue;
        };
        for first in firsts.flatten() {
            let Ok(seconds) = std::fs::read_dir(first.path()) else {
                continue;
            };
            for second in seconds.flatten() {
                let Ok(meta) = second.metadata() else {
                    continue;
                };
                if !meta.is_file() {
                    continue;
                }
                let Some(id) = id_of(&first.file_name(), &second.file_name()) else {
                    continue;
                };
                files.push((kind.to_owned(), id, second.path(), meta.len()));
            }
        }
    }
    files
}

/// How many files a store's object tree holds.
fn count_files(objects: &std::path::Path) -> usize {
    stored_files(objects).len()
}

/// Joins the two parts of a stored file's name into its 32-byte ID.
fn id_of(first: &std::ffi::OsStr, second: &std::ffi::OsStr) -> Option<Vec<u8>> {
    let mut hex = first.to_str()?.to_owned();
    hex.push_str(second.to_str()?);
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

/// One file agrees with the index: the store knows its key at the file's
/// size.
fn agrees(store: &DiskStore, file: &(String, Vec<u8>, std::path::PathBuf, u64)) -> bool {
    let (kind, id, _, len) = file;
    let key = if kind == "blob" {
        BlobId::try_from(id.as_slice()).map(Key::Blob)
    } else {
        scion_core::TreeId::try_from(id.as_slice()).map(Key::Tree)
    };
    match key {
        Ok(key) => store
            .size(key)
            .ok()
            .flatten()
            .is_some_and(|size| size as u64 == *len),
        Err(_) => false,
    }
}

/// The daemon's stderr stays quiet and colorless across reads: no escape
/// byte (color only when stderr is a terminal), and nothing per call (the
/// `GetObjects` line is DEBUG; the hit and miss counts are the metrics'
/// `cache_counts`). The 20 reads return their objects, so the quiet is
/// not a broken daemon.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_daemons_stderr_stays_quiet_and_colorless_across_gets() {
    use std::process::Command;
    use std::time::Instant;

    let object = blob(b"once per request is not a log level\n");
    let fake = Fake::new(&[&object], Duration::ZERO);
    let upstream = serve_fake(fake.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("daemon.sock");
    let log = dir.path().join("daemon.log");
    let data = dir.path().join("data");
    let child = Command::new(env!("CARGO_BIN_EXE_scion-daemon"))
        .arg("--data")
        .arg(&data)
        .arg("--socket")
        .arg(&socket)
        .arg("--upstream")
        .arg(&upstream)
        .arg("--max-bytes")
        .arg((8 << 20).to_string())
        .arg("--token")
        .arg(TOKEN)
        .arg("--upstream-token")
        .arg(TOKEN)
        // The caller's RUST_LOG must not steer the daemon's log filter:
        // RUST_LOG=debug would bring the per-call line back, RUST_LOG=off
        // would silence the check entirely.
        .env_remove("RUST_LOG")
        .stderr(std::fs::File::create(&log).unwrap())
        .spawn()
        .map(Kill)
        .unwrap();
    // The child is up when its socket file is there (10 s ceiling).
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(
            Instant::now() < deadline,
            "the daemon never opened its socket"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mut client = connect(&format!("unix://{}", socket.display()), Some(TOKEN)).await;

    // One read, then measure what the daemon has logged.
    get(&mut client, &fake, &[&object]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after_one = std::fs::read(&log).unwrap().len();

    // Nineteen more reads: a line per call would add about 19 lines.
    for _ in 0..19 {
        get(&mut client, &fake, &[&object]).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let bytes = std::fs::read(&log).unwrap();
    assert!(
        !bytes.contains(&0x1b),
        "the daemon's stderr carries escape bytes: color is not a terminal's business"
    );
    let grew = bytes.len() - after_one;
    assert!(
        grew < 200,
        "the daemon logged {grew} bytes across 19 reads: a line per call"
    );
    drop(child);
}
