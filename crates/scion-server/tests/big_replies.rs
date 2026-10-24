//! `ListDir` and `GetChangeset` replies whose encoded bytes are over one
//! message: they stream in parts, as `ReadFile` streams a file, and the
//! client puts the parts back together and checks what it got.

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath, Tree};
use scion_proto::MAX_PART_MESSAGE_BYTES;
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{
    CreateChangesetRequest, GetChangesetRequest, ListDirRequest, PutBlobRequest,
};
use scion_server::test_support::backend::from_store;
use scion_server::test_support::meta::MetaTx;
use std::sync::Arc;

use scion_server::Metrics;
use scion_server::test_support::{service, store::Store};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tonic::Status;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const REPO: &str = "main";
const TOKEN: &str = "secret";

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

/// The identity of every changeset in these tests.
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

/// A repo whose one changeset adds `names` files at the root, so its
/// `TreeV1` and `ChangesetV1` bytes are over one part. The changeset is made
/// through the RPC, as a client would. The client under test accepts one
/// part per message and no more, so a reply that arrived as one message
/// would fail to decode.
async fn big_repo(names: usize) -> (tempfile::TempDir, Scion, ChangesetId) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    // The root changeset, so the changeset under test has a parent.
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
    let root = seed_root(&store, &root);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
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
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
        std::future::pending(),
    ));
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    let mut client = ScionClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap())
        .max_decoding_message_size(MAX_PART_MESSAGE_BYTES);

    // The content of every file, uploaded once, so the repo has its blob.
    let mut raw = ScionClient::with_interceptor(
        common::channel(&format!("http://{addr}")).await.unwrap(),
        Bearer::new(TOKEN).unwrap(),
    );
    let blob = raw
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.into(),
            data: b"x".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .blob_id;
    let blob = BlobId::try_from(blob.as_slice()).unwrap();

    let changeset = Changeset::new(
        vec![root],
        author(),
        None,
        b"big\n".to_vec(),
        (0..names)
            .map(|i| {
                (
                    RepoPath::new(format!("entry-{i:06}.bin").as_bytes()).unwrap(),
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )
            })
            .collect(),
    )
    .expect("changeset fits the format");
    let created = client
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: changeset.encode(),
        })
        .await
        .unwrap()
        .into_inner()
        .changeset_id;
    let created = ChangesetId::try_from(created.as_slice()).unwrap();
    (dir, client, created)
}

/// Reads a whole streamed reply.
async fn whole<T, S, F>(stream: &mut S, field: F) -> (usize, Vec<u8>)
where
    S: tokio_stream::Stream<Item = Result<T, Status>> + Unpin,
    F: Fn(&T) -> &[u8],
{
    let mut parts = 0;
    let mut bytes = Vec::new();
    while let Some(part) = stream.next().await {
        bytes.extend_from_slice(field(&part.unwrap()));
        parts += 1;
    }
    (parts, bytes)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tree_and_a_changeset_over_one_message_arrive_in_parts() {
    let (_dir, mut client, created) = big_repo(40_000).await;

    // The whole tree, in parts that each fit one message.
    let mut stream = client
        .list_dir(ListDirRequest {
            repo: REPO.into(),
            changeset_id: created.as_bytes().to_vec(),
            path: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let (parts, tree_bytes) = whole(&mut stream, |p| &p.tree).await;
    assert!(parts > 1, "the tree arrived in {parts} parts");
    let tree = Tree::decode(&tree_bytes).expect("the parts are a whole TreeV1");
    assert_eq!(tree.entries().len(), 40_001, "every entry came");

    // The whole changeset, in parts, and it is the one that was asked for.
    let mut stream = client
        .get_changeset(GetChangesetRequest {
            repo: REPO.into(),
            changeset_id: created.as_bytes().to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    let (parts, cs_bytes) = whole(&mut stream, |p| &p.changeset).await;
    assert!(parts > 1, "the changeset arrived in {parts} parts");
    let (cs, id) = Changeset::decode_with_id(&cs_bytes).expect("the parts are a whole ChangesetV1");
    assert_eq!(
        id, created,
        "the bytes are the changeset that was asked for"
    );
    assert_eq!(cs.changes().len(), 40_000, "every change came");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_stops_mid_reply_can_read_on() {
    let (_dir, mut client, created) = big_repo(40_000).await;
    let request = ListDirRequest {
        repo: REPO.into(),
        changeset_id: created.as_bytes().to_vec(),
        path: Vec::new(),
    };

    // Take one part of the tree and stop reading.
    let mut stream = client.list_dir(request.clone()).await.unwrap().into_inner();
    let _first = stream.next().await.unwrap().unwrap();
    drop(stream);

    // The reply's budget share came back with it, so a client that reads
    // the whole tree gets it whole.
    let mut stream = client.list_dir(request).await.unwrap().into_inner();
    let (_, tree_bytes) = whole(&mut stream, |p| &p.tree).await;
    let tree = Tree::decode(&tree_bytes).expect("the second reply is whole");
    assert_eq!(tree.entries().len(), 40_001);
}

#[path = "common/mod.rs"]
mod common;
