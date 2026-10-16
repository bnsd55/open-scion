//! Object reads with tickets, against an in-process server: a ticket from
//! `GetRootTree` serves the objects that its walk reaches, and nothing else.

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::auth::Bearer;
use scion_proto::listen::Limits;
use scion_proto::object::{Assembler, Header};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::{GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectRef};
use scion_server::tickets::Keys;
use scion_server::{Store, db, service};
use tokio::net::TcpListener;
use tonic::Code;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const REPO: &str = "main";
const TOKEN: &str = "secret";
const CONTENT: &[u8] = b"# hello\n";

type Objects = ObjectsClient<InterceptedService<Channel, Bearer>>;

/// A server with ticket keys, one changeset of `main` that adds `README`,
/// and the file's blob. Returns the client, the keys, the changeset, the
/// blob's ID, and the data directory, which must outlive the server.
async fn server() -> (
    Objects,
    Keys,
    ChangesetId,
    BlobId,
    tokio::sync::oneshot::Sender<()>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let readme = store.blobs().put(CONTENT).unwrap();
    let changeset = Changeset {
        parents: Vec::new(),
        author: IdentityTime {
            name: b"Ada".to_vec(),
            email: b"ada@example.com".to_vec(),
            unix_secs: 1_700_000_000,
            tz_offset_minutes: 0,
        },
        committer: None,
        message: b"add\n".to_vec(),
        changes: [(
            RepoPath::new("README").unwrap(),
            Some(FileState::Regular {
                content: readme,
                executable: false,
            }),
        )]
        .into_iter()
        .collect(),
    };
    let (_bytes, id) = changeset.encode_with_id();
    store
        .write(|c| {
            let repo = db::create_repo(c, REPO)?;
            db::insert_changeset(c, repo, &changeset)
        })
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let keys = Keys::generate();
    tokio::spawn(service::serve(
        store,
        listener,
        TOKEN,
        Limits::default(),
        None,
        Some(keys.clone().into()),
        async {
            stopped.await.ok();
        },
    ));
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (
        ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap()),
        keys,
        id,
        readme,
        stop,
        dir,
    )
}

/// The ticket and root tree of `changeset`, from the server.
async fn root(objects: &mut Objects, changeset: ChangesetId) -> (Vec<u8>, Vec<u8>) {
    let reply = objects
        .get_root_tree(GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: changeset.as_bytes().to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    (reply.ticket, reply.tree_id)
}

/// The bytes that one `GetObjects` call returns, or the status that ends it.
async fn get(
    objects: &mut Objects,
    ticket: &[u8],
    refs: Vec<ObjectRef>,
) -> Result<Vec<(ObjectKind, Vec<u8>)>, tonic::Status> {
    let mut stream = objects
        .get_objects(GetObjectsRequest {
            refs,
            accept: Vec::new(),
            ticket: ticket.to_vec(),
        })
        .await?
        .into_inner();
    let mut got = Vec::new();
    while let Some(first) = stream.message().await? {
        let kind = ObjectKind::try_from(first.kind).unwrap();
        let mut object = Assembler::new(
            scion_proto::object::Header::of(&first, false).unwrap(),
            first,
        );
        while !object.is_done() {
            let part = stream
                .message()
                .await?
                .expect("the reply ended in the middle of an object");
            object.push(&part).expect("the parts of one object");
        }
        let (_, bytes) = object.finish();
        got.push((kind, bytes));
    }
    Ok(got)
}

fn blob_ref(id: &[u8], path: &str) -> ObjectRef {
    ObjectRef {
        kind: ObjectKind::Blob.into(),
        id: id.to_vec(),
        path: path.as_bytes().to_vec(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ticket_serves_what_its_walk_reaches() {
    let (mut objects, keys, cs, blob, _stop, _dir) = server().await;
    let (ticket, tree) = root(&mut objects, cs).await;
    // The ticket is real, for this principal, this repo, and this tree.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let parsed = scion_proto::ticket::parse(&ticket, keys.for_verify(), now)
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(parsed.repo, REPO);
    assert_eq!(parsed.principal, "default");
    assert_eq!(parsed.root_tree, tree[..]);

    // The blob, by its path; and the root tree, by its empty path.
    let got = get(
        &mut objects,
        &ticket,
        vec![
            blob_ref(blob.as_bytes(), "README"),
            ObjectRef {
                kind: ObjectKind::Tree.into(),
                id: tree.clone(),
                path: Vec::new(),
            },
        ],
    )
    .await
    .unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].0, ObjectKind::Blob);
    assert_eq!(got[0].1, CONTENT);
    assert_eq!(got[1].0, ObjectKind::Tree);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ticket_serves_nothing_beyond_its_walk() {
    let (mut objects, _keys, cs, blob, _stop, _dir) = server().await;
    let (ticket, tree) = root(&mut objects, cs).await;
    // A path that is not in the tree, and an ID that no path names.
    let status = get(
        &mut objects,
        &ticket,
        vec![blob_ref(blob.as_bytes(), "no-such-file")],
    )
    .await
    .unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    let status = get(&mut objects, &ticket, vec![blob_ref(&[9; 32], "README")])
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    // The root tree, named as a blob: the kind does not match the walk.
    let status = get(
        &mut objects,
        &ticket,
        vec![ObjectRef {
            kind: ObjectKind::Blob.into(),
            id: tree,
            path: Vec::new(),
        }],
    )
    .await
    .unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn only_a_ticket_of_this_server_serves() {
    let (mut objects, _keys, cs, blob, _stop, _dir) = server().await;
    let (ticket, _tree) = root(&mut objects, cs).await;

    // No ticket, a foreign key's ticket, and a damaged one: all refused.
    let stranger = Keys::generate();
    let foreign = stranger
        .sign(REPO, "default", [0; 32])
        .map_err(|why| panic!("{why}"))
        .unwrap();
    let mut tampered = ticket.clone();
    tampered[1 + 8] ^= 0xff;
    for bad in [Vec::new(), foreign, tampered] {
        let status = get(
            &mut objects,
            &bad,
            vec![blob_ref(blob.as_bytes(), "README")],
        )
        .await
        .unwrap_err();
        assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    }
    // A good ticket still serves after the refusals.
    let got = get(
        &mut objects,
        &ticket,
        vec![blob_ref(blob.as_bytes(), "README")],
    )
    .await
    .unwrap();
    assert_eq!(got[0].1, CONTENT);
}

/// A cache tier with no token of the server fetches over a ticket: the
/// permission of an object read is the ticket, not a token. A call without
/// a ticket is refused even so.
/// A server with one `README`, its keys, and a stop handle to hold.
async fn server_of_readme() -> (
    std::net::SocketAddr,
    Keys,
    ChangesetId,
    BlobId,
    tokio::sync::oneshot::Sender<()>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let server_store = Store::open(dir.path()).unwrap();
    let readme = server_store.blobs().put(CONTENT).unwrap();
    let changeset = Changeset {
        parents: Vec::new(),
        author: IdentityTime {
            name: b"Ada".to_vec(),
            email: b"ada@example.com".to_vec(),
            unix_secs: 1_700_000_000,
            tz_offset_minutes: 0,
        },
        committer: None,
        message: b"add\n".to_vec(),
        changes: [(
            RepoPath::new("README").unwrap(),
            Some(FileState::Regular {
                content: readme,
                executable: false,
            }),
        )]
        .into_iter()
        .collect(),
    };
    let (_bytes, cs) = changeset.encode_with_id();
    server_store
        .write(|c| {
            let repo = db::create_repo(c, REPO)?;
            db::insert_changeset(c, repo, &changeset)
        })
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let keys = Keys::generate();
    tokio::spawn(service::serve(
        server_store,
        listener,
        TOKEN,
        Limits::default(),
        None,
        Some(keys.clone().into()),
        async {
            stopped.await.ok();
        },
    ));
    (server_addr, keys, cs, readme, stop, dir)
}

#[tokio::test]
async fn a_cache_without_a_token_fetches_over_a_ticket() {
    use scion_proto::v1::GetRootTreeRequest as Root;
    use scion_tier::{MemoryStore, Tier};

    let (_objects, _keys, _cs, _blob, stop, _dir) = server().await;

    // A server with the data, and a cache in front of it with no token.
    let (server_addr, keys, cs, readme, stop_server, _dir2) = server_of_readme().await;

    // The cache: no token, only the server's public key.
    let cache = Tier::new(
        MemoryStore::new(16 << 20),
        &format!("http://{server_addr}"),
        "",
        std::time::Duration::from_secs(5),
        None,
        keys.for_verify().to_vec(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cache_addr = listener.local_addr().unwrap();
    let (stop_cache, stopped_cache) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(cache.clone().serve(
        TOKEN,
        tokio_stream::wrappers::TcpListenerStream::new(listener),
        Limits::default(),
        None,
        async {
            stopped_cache.await.ok();
        },
    ));

    // The caller holds the CACHE's token (the tier's own door), but has no
    // token of the SERVER anywhere.
    let channel = tonic::transport::Channel::from_shared(format!("http://{cache_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut tier = ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap());

    // A tier resolves nothing; the caller asks the server, with a token.
    let status = tier
        .get_root_tree(Root {
            repo: REPO.into(),
            changeset_id: cs.as_bytes().to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented, "{status:?}");
    let channel = tonic::transport::Channel::from_shared(format!("http://{server_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut server = ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap());
    let (ticket, _tree) = root(&mut server, cs).await;

    // The ticket alone buys the object through the token-less tier.
    let refs = vec![ObjectRef {
        kind: ObjectKind::Blob.into(),
        id: readme.as_bytes().to_vec(),
        path: b"README".to_vec(),
    }];
    let mut stream = tier
        .get_objects(GetObjectsRequest {
            refs: refs.clone(),
            accept: Vec::new(),
            ticket,
        })
        .await
        .unwrap()
        .into_inner();
    let first = stream.message().await.unwrap().unwrap();
    let mut object = Assembler::new(Header::of(&first, false).unwrap(), first);
    while !object.is_done() {
        object
            .push(&stream.message().await.unwrap().unwrap())
            .unwrap();
    }
    let (_header, data) = object.finish();
    assert_eq!(data, CONTENT);

    // Without a ticket, the same call is refused.
    let status = tier
        .get_objects(GetObjectsRequest {
            refs,
            accept: Vec::new(),
            ticket: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    stop_server.send(()).ok();
    stop_cache.send(()).ok();
    stop.send(()).ok();
}
