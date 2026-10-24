//! Object reads with tickets, against an in-process server: a ticket from
//! `GetRootTree` serves the objects that its walk reaches, and nothing else.

mod common;

use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileState, IdentityTime, RepoPath, Tree, TreeId,
};
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::listen::Limits;
use scion_proto::runtime::object::{Assembler, Header};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::{GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectRef};
use scion_server::Keys;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::{backend::from_store, service, store::Store};
use std::sync::Arc;
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
    let changeset = Changeset::new(
        Vec::new(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"add\n".to_vec(),
        [(
            RepoPath::new("README").unwrap(),
            Some(FileState::Regular {
                content: readme,
                executable: false,
            }),
        )]
        .into_iter()
        .collect(),
    )
    .expect("changeset fits the format");
    let (_bytes, id) = changeset.encode_with_id();
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            c.insert_changeset(repo, &changeset)
        })
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let keys = Keys::generate();
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
            metrics: Arc::default(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(keys.clone().into()),
        async {
            stopped.await.ok();
        },
    ));
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    (
        ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap()),
        keys,
        id,
        readme,
        stop,
        dir,
    )
}

/// The ticket and root tree of `changeset` of `repo`, from the server.
async fn root(objects: &mut Objects, repo: &str, changeset: ChangesetId) -> (Vec<u8>, Vec<u8>) {
    let reply = objects
        .get_root_tree(GetRootTreeRequest {
            repo: repo.to_owned(),
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
            scion_proto::runtime::object::Header::of(&first, false).unwrap(),
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
    let (ticket, tree) = root(&mut objects, REPO, cs).await;
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
    let (ticket, tree) = root(&mut objects, REPO, cs).await;
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
    let (ticket, _tree) = root(&mut objects, REPO, cs).await;

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
    let changeset = Changeset::new(
        Vec::new(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"add\n".to_vec(),
        [(
            RepoPath::new("README").unwrap(),
            Some(FileState::Regular {
                content: readme,
                executable: false,
            }),
        )]
        .into_iter()
        .collect(),
    )
    .expect("changeset fits the format");
    let (_bytes, cs) = changeset.encode_with_id();
    server_store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            c.insert_changeset(repo, &changeset)
        })
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let keys = Keys::generate();
    tokio::spawn(service::serve(
        from_store(server_store.clone()),
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
            metrics: Arc::default(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
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
    let channel = common::channel(&format!("http://{cache_addr}"))
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
    let channel = common::channel(&format!("http://{server_addr}"))
        .await
        .unwrap();
    let mut server = ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap());
    let (ticket, _tree) = root(&mut server, REPO, cs).await;

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
    let first = common::ceiling("the first object part", stream.message())
        .await
        .unwrap()
        .unwrap();
    let mut object = Assembler::new(Header::of(&first, false).unwrap(), first);
    while !object.is_done() {
        let part = common::ceiling("the next object part", stream.message())
            .await
            .unwrap()
            .unwrap();
        object.push(&part).unwrap();
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

/// The `now` of the ticket clock, in seconds.
fn now() -> u64 {
    scion_proto::ticket::now()
}

/// A server with ticket keys, and a cache in front of it that knows the
/// server's public key. The store holds two repos that share blobs:
/// - `main`: one changeset whose tree has `README`, `src/lib.rs`,
///   `docs/a.md`, `docs/b.md`, and the gitlink `sub`, whose commit is the
///   ID of a real blob;
/// - `side`: one changeset whose tree has only `other.txt`.
struct Tiers {
    server: Objects,
    cache: Objects,
    keys: Keys,
    main_cs: ChangesetId,
    side_cs: ChangesetId,
    readme: BlobId,
    lib: BlobId,
    sub: BlobId,
    other: BlobId,
}

/// The two changesets of the fixture: `main` over the given blobs, and
/// `side`, which shares one of them.
fn changesets(
    shared: BlobId,
    readme: BlobId,
    lib: BlobId,
    sub: BlobId,
    other: BlobId,
) -> (Changeset, Changeset) {
    let file = |content: BlobId| {
        Some(FileState::Regular {
            content,
            executable: false,
        })
    };
    let main = Changeset::new(
        Vec::new(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"main\n".to_vec(),
        [
            (RepoPath::new("README").unwrap(), file(readme)),
            (RepoPath::new("src/lib.rs").unwrap(), file(lib)),
            (RepoPath::new("docs/a.md").unwrap(), file(readme)),
            (RepoPath::new("docs/b.md").unwrap(), file(shared)),
            (RepoPath::new("shared.txt").unwrap(), file(shared)),
            (
                RepoPath::new("sub").unwrap(),
                Some(FileState::Gitlink {
                    commit: scion_core::GitOid::Sha256(*sub.as_bytes()),
                }),
            ),
        ]
        .into_iter()
        .collect(),
    )
    .expect("changeset fits the format");
    let side = Changeset::new(
        Vec::new(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_001,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"side\n".to_vec(),
        [
            (RepoPath::new("other.txt").unwrap(), file(other)),
            (RepoPath::new("shared.txt").unwrap(), file(shared)),
        ]
        .into_iter()
        .collect(),
    )
    .expect("changeset fits the format");
    (main, side)
}

async fn tiers() -> (Tiers, tokio::sync::oneshot::Sender<()>, tempfile::TempDir) {
    use scion_tier::{MemoryStore, Tier};

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    // One blob in both repos, and one in each alone.
    let shared = store.blobs().put(b"shared\n").unwrap();
    let readme = store.blobs().put(b"# main\n").unwrap();
    let lib = store.blobs().put(b"pub fn f() {}\n").unwrap();
    let other = store.blobs().put(b"other\n").unwrap();
    // A gitlink that names the ID of a real blob: the walk must end there,
    // so the bytes behind the ID never go out under this path.
    let sub = store.blobs().put(b"behind a gitlink\n").unwrap();
    let (main, side) = changesets(shared, readme, lib, sub, other);
    let (_bytes, main_cs) = main.encode_with_id();
    let (_bytes, side_cs) = side.encode_with_id();
    store
        .write(|c| {
            let repo = c.create_repo("main")?;
            c.insert_changeset(repo, &main)?;
            let repo = c.create_repo("side")?;
            c.insert_changeset(repo, &side)
        })
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let keys = Keys::generate();
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
            metrics: Arc::default(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(keys.clone().into()),
        async {
            stopped.await.ok();
        },
    ));

    let cache = Tier::new(
        MemoryStore::new(16 << 20),
        &format!("http://{addr}"),
        TOKEN,
        std::time::Duration::from_secs(5),
        None,
        keys.for_verify().to_vec(),
    )
    .unwrap();
    let cache_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cache_addr = cache_listener.local_addr().unwrap();
    tokio::spawn(cache.clone().serve(
        TOKEN,
        tokio_stream::wrappers::TcpListenerStream::new(cache_listener),
        Limits::default(),
        None,
        std::future::pending(),
    ));

    let connect = |url: String| async move {
        ObjectsClient::with_interceptor(
            common::channel(&url).await.unwrap(),
            Bearer::new(TOKEN).unwrap(),
        )
    };
    let server = connect(format!("http://{addr}")).await;
    let cache = connect(format!("http://{cache_addr}")).await;
    let tiers = Tiers {
        server,
        cache,
        keys,
        main_cs,
        side_cs,
        readme,
        lib,
        sub,
        other,
    };
    (tiers, stop, dir)
}

/// Signs a ticket with the server's own key, with `expires` set as if the
/// server had minted it at `minted_at`.
fn signed_with(keys: &Keys, repo: &str, root_tree: [u8; 32], minted_at: u64) -> Vec<u8> {
    let seed = *blake3::Hash::from_hex(keys.seed_hex().trim())
        .unwrap()
        .as_bytes();
    let key = ed25519_dalek::SigningKey::from_bytes(&seed);
    scion_proto::ticket::sign(&key, repo, "default", root_tree, minted_at).unwrap()
}

/// Tickets that are not in time, not in shape, or not from this server's
/// key are refused by the server and by the cache alike.
#[tokio::test(flavor = "multi_thread")]
async fn tickets_out_of_time_or_shape_are_refused() {
    let (tiers, _stop, _dir) = tiers().await;
    let mut server = tiers.server;
    let mut cache = tiers.cache;

    let (ticket, root) = root(&mut server, "main", tiers.main_cs).await;
    let refs = vec![blob_ref(tiers.readme.as_bytes(), "README")];
    let good = get(&mut server, &ticket, refs.clone()).await.unwrap();
    assert_eq!(good[0].1, b"# main\n");

    // An expired ticket, and one so far ahead that no server could have
    // minted it now.
    let expired = signed_with(
        &tiers.keys,
        "main",
        root[..].try_into().unwrap(),
        now() - 900 - 60 - 10,
    );
    let ahead = signed_with(
        &tiers.keys,
        "main",
        root[..].try_into().unwrap(),
        now() + 900 + 120 + 10,
    );
    // A ticket of another key, and a ticket with bytes after its signature.
    let stranger = signed_with(
        &Keys::generate(),
        "main",
        root[..].try_into().unwrap(),
        now(),
    );
    let mut trailing = ticket.clone();
    trailing.push(0);
    for bad in [expired, ahead, stranger, trailing] {
        let status = get(&mut server, &bad, refs.clone()).await.unwrap_err();
        assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
        let status = get(&mut cache, &bad, refs.clone()).await.unwrap_err();
        assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    }

    // The good ticket still serves after the refusals.
    let got = get(&mut server, &ticket, refs).await.unwrap();
    assert_eq!(got[0].1, b"# main\n");
}

/// A ticket of another repo is a real ticket: the server minted it. It
/// still serves nothing of this repo, because its walk starts at another
/// root tree.
#[tokio::test(flavor = "multi_thread")]
async fn a_ticket_of_another_repo_serves_nothing_of_this_one() {
    let (tiers, _stop, _dir) = tiers().await;
    let mut server = tiers.server;
    let mut cache = tiers.cache;

    let (side_ticket, _side_root) = root(&mut server, "side", tiers.side_cs).await;
    for client in [&mut server, &mut cache] {
        // A path of `main` that `side` does not name, and one that it names
        // with other bytes: the shared blob is at another root tree, so its
        // ID at this path is not the walk's.
        for refs in [
            vec![blob_ref(tiers.readme.as_bytes(), "README")],
            vec![blob_ref(tiers.lib.as_bytes(), "src/lib.rs")],
        ] {
            let status = get(client, &side_ticket, refs).await.unwrap_err();
            assert_eq!(status.code(), Code::NotFound, "{status:?}");
        }
    }
}

/// A gitlink ends the walk: the bytes of the blob whose ID the gitlink
/// holds never go out under the gitlink's path, on the server or on a
/// cache.
#[tokio::test(flavor = "multi_thread")]
async fn a_gitlink_ends_the_walk() {
    let (tiers, _stop, _dir) = tiers().await;
    let mut server = tiers.server;
    let mut cache = tiers.cache;

    let (ticket, root) = root(&mut server, "main", tiers.main_cs).await;
    // The blob behind the gitlink, asked for as a blob and as a tree at the
    // gitlink's path.
    for client in [&mut server, &mut cache] {
        for refs in [
            vec![blob_ref(tiers.sub.as_bytes(), "sub")],
            vec![ObjectRef {
                kind: ObjectKind::Tree.into(),
                id: tiers.sub.as_bytes().to_vec(),
                path: b"sub".to_vec(),
            }],
        ] {
            let status = get(client, &ticket, refs).await.unwrap_err();
            assert_eq!(status.code(), Code::NotFound, "{status:?}");
        }
        // The bytes do go out under their own path of another repo, whose
        // ticket reaches them there... but no repo holds them as a file, so
        // they never go out at all. The gitlink's bytes are of no repo.
        let _ = root;
    }
}

/// A seeded xorshift, so a failure repeats from the seed it prints.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Every object that the ticket's walk can reach: the root tree at the
/// empty path, each tree at its path, each file's blob at its path. The
/// trees are decoded from the server's own reply, each checked against its
/// ID. A gitlink ends the walk: nothing under it is reachable.
async fn reachable(objects: &mut Objects, ticket: &[u8], root: &[u8]) -> Vec<ObjectRef> {
    let mut found = Vec::new();
    let mut level = vec![(Vec::new(), TreeId::try_from(root).unwrap())];
    while let Some((path, id)) = level.pop() {
        let got = get(
            objects,
            ticket,
            vec![ObjectRef {
                kind: ObjectKind::Tree.into(),
                id: id.as_bytes().to_vec(),
                path: path.clone(),
            }],
        )
        .await
        .unwrap_or_else(|status| panic!("the walk of the tree at {path:?}: {status:?}"));
        found.push(ObjectRef {
            kind: ObjectKind::Tree.into(),
            id: id.as_bytes().to_vec(),
            path: path.clone(),
        });
        let tree = Tree::decode(&got[0].1).unwrap();
        for (name, entry) in tree.entries() {
            let mut below = path.clone();
            if !below.is_empty() {
                below.push(b'/');
            }
            below.extend_from_slice(name.as_bytes());
            match entry {
                Entry::Dir(id) => level.push((below, *id)),
                Entry::File(file) => {
                    if let Some(blob) = file.state().blob() {
                        found.push(ObjectRef {
                            kind: ObjectKind::Blob.into(),
                            id: blob.as_bytes().to_vec(),
                            path: below,
                        });
                    }
                }
            }
        }
    }
    found
}

/// A ref is refused alone, on the server and on the cache.
async fn refused(client: &mut Objects, ticket: &[u8], decoy: ObjectRef, where_: &str) {
    let status = get(client, ticket, vec![decoy]).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound, "{where_}: {status:?}");
}

/// Over repos that share blobs and a random pick of repo, changeset and
/// decoys, a `GetObjects` call serves exactly what the ticket's walk
/// reaches: every reached object goes out, every other object is refused,
/// on the server and on the cache alike.
#[tokio::test(flavor = "multi_thread")]
async fn whatever_is_served_is_reachable_from_the_ticket() {
    let (tiers, _stop, _dir) = tiers().await;
    let seed = 0x5EED_u64.wrapping_mul(1_700_000_009);
    let mut rng = Rng(seed);

    for round in 0..16 {
        // A random read rule: whose ticket this round spends, main or side.
        let (repo, cs) = if rng.below(2) == 0 {
            ("main", tiers.main_cs)
        } else {
            ("side", tiers.side_cs)
        };
        let (ticket, root_id) = root(&mut tiers.server.clone(), repo, cs).await;
        let reaches = reachable(&mut tiers.server.clone(), &ticket, &root_id).await;
        assert!(!reaches.is_empty(), "seed {seed:#x} round {round}");

        // Every reached object, in one call, is served with its bytes.
        for client in [&mut tiers.server.clone(), &mut tiers.cache.clone()] {
            let got = get(client, &ticket, reaches.clone()).await.unwrap();
            assert_eq!(
                got.len(),
                reaches.len(),
                "seed {seed:#x} round {round} {repo}"
            );
        }

        // The gitlink's blob never goes out under the gitlink's path.
        if repo == "main" {
            let decoy = blob_ref(tiers.sub.as_bytes(), "sub");
            refused(
                &mut tiers.server.clone(),
                &ticket,
                decoy.clone(),
                "gitlink as blob",
            )
            .await;
            refused(&mut tiers.cache.clone(), &ticket, decoy, "gitlink as blob").await;
        }
        // A real object of the other repo, at its own path, and at a path
        // of this repo: the walk reaches neither.
        let foreign = if repo == "main" {
            tiers.other
        } else {
            tiers.readme
        };
        let foreign_path = if repo == "main" {
            "other.txt"
        } else {
            "README"
        };
        for decoy in [
            blob_ref(foreign.as_bytes(), foreign_path),
            blob_ref(foreign.as_bytes(), own_path(&mut rng)),
        ] {
            refused(
                &mut tiers.server.clone(),
                &ticket,
                decoy.clone(),
                "foreign object",
            )
            .await;
            refused(&mut tiers.cache.clone(), &ticket, decoy, "foreign object").await;
        }
    }
}

/// A path that holds no object of this ticket's repo, picked by `rng`.
fn own_path(rng: &mut Rng) -> &'static str {
    let pick = rng.below(3);
    match pick {
        0 => "no/such/file",
        1 => "src",
        _ => "docs",
    }
}
