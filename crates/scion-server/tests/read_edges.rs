//! The edges of `ReadFile` and `ListDir` replies, through the public API:
//! the empty, the exactly-one-part, and the one-part-plus-a-little, the
//! name at the cap, and the path over it.

use std::sync::Arc;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath, Tree};
use scion_proto::CHUNK_BYTES;
use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{ListDirRequest, ReadFileRequest};
use scion_server::Access;
use scion_server::Backend;
use scion_server::test_support::backend::from_store;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::{meta::Meta as _, service, store::Store};
use tokio::net::TcpListener;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const REPO: &str = "main";
const TOKEN: &str = "alice";

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

type Stop = tokio::sync::oneshot::Sender<()>;

/// Adds `authorization: Bearer <token>` to every request.
#[derive(Clone)]
struct Bearer(Option<String>);

impl tonic::service::Interceptor for Bearer {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = &self.0 {
            let value: tonic::metadata::AsciiMetadataValue =
                format!("Bearer {token}").parse().unwrap();
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

fn hex(token: &str) -> String {
    blake3::Hash::from(token_digest(token)).to_hex().to_string()
}

fn regular(content: BlobId) -> FileState {
    FileState::Regular {
        content,
        executable: false,
    }
}

fn author() -> IdentityTime {
    IdentityTime::new(
        b"root".to_vec(),
        b"a@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

/// A store whose repo has one root changeset that puts `files` at the repo
/// root. An empty `files` makes an empty directory.
fn store(files: &[(Vec<u8>, &[u8])]) -> (tempfile::TempDir, Backend, ChangesetId) {
    let dir = tempfile::tempdir().unwrap();
    let store = from_store(Store::open(dir.path()).unwrap());
    let root = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut changes = Vec::new();
            for (name, body) in files {
                let blob = store.blobs().put(body).unwrap();
                c.add_repo_blob(repo, blob)?;
                changes.push((RepoPath::new(name.as_slice()).unwrap(), Some(regular(blob))));
            }
            let root = Changeset::new(
                vec![],
                author(),
                None,
                b"root\n".to_vec(),
                changes.into_iter().collect(),
            )
            .expect("changeset fits the format");
            let root = c.insert_changeset(repo, &root)?;
            c.cas_ref(repo, "main", None, root)?;
            Ok(root)
        })
        .unwrap();
    (dir, store, root)
}

/// Serves `store`, and returns the stop signal and a client as `TOKEN`.
async fn serve(store: &Backend, dir: &std::path::Path) -> (Stop, Scion) {
    let access = Access::parse(&format!(
        "token\t{TOKEN}\t{}\nread\t{TOKEN}\t{REPO}\n",
        hex(TOKEN)
    ))
    .unwrap();
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
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::new(scion_server::Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(Arc::new(scion_server::Keys::generate())),
        async {
            stopped.await.ok();
        },
    ));
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    let client = ScionClient::with_interceptor(channel, Bearer(Some(TOKEN.to_string())));
    (stop, client)
}

/// One `ListDir` call, with its parts joined: how many parts, and the tree
/// bytes they carry.
async fn list_dir(client: &mut Scion, changeset_id: &ChangesetId, path: &[u8]) -> (usize, Vec<u8>) {
    let mut pages = client
        .list_dir(ListDirRequest {
            repo: REPO.to_string(),
            changeset_id: changeset_id.as_bytes().to_vec(),
            path: path.to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut parts = 0;
    let mut bytes = Vec::new();
    while let Some(page) = pages.message().await.unwrap() {
        bytes.extend_from_slice(&page.tree);
        parts += 1;
    }
    (parts, bytes)
}

/// One `ReadFile` call, with its parts joined: how many parts, and the
/// content they carry.
async fn read_file(
    client: &mut Scion,
    changeset_id: &ChangesetId,
    path: &[u8],
) -> (usize, Vec<u8>) {
    let mut pages = client
        .read_file(ReadFileRequest {
            repo: REPO.to_string(),
            changeset_id: changeset_id.as_bytes().to_vec(),
            path: path.to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut parts = 0;
    let mut bytes = Vec::new();
    while let Some(page) = pages.message().await.unwrap() {
        bytes.extend_from_slice(&page.data);
        parts += 1;
    }
    (parts, bytes)
}

/// An empty directory still lists: its `TreeV1` bytes are the version and
/// the count of no entries, in one part. A client that took an empty reply
/// for a broken one would never see the empty tree.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_directory_lists_as_one_part_of_no_entries() {
    let (dir, store, root) = store(&[]);
    let (_stop, mut client) = serve(&store, dir.path()).await;
    let (parts, bytes) = list_dir(&mut client, &root, b"").await;
    assert_eq!(parts, 1, "the empty tree is its header only");
    let tree = Tree::decode(&bytes).expect("the part is a whole TreeV1");
    assert!(tree.entries().is_empty(), "{tree:?}");
}

/// A directory whose `TreeV1` bytes are exactly one part arrives in one
/// part, and one entry more makes two: the part boundary between them
/// keeps every entry. The byte total is computed from the v1 format: a
/// header of 5 bytes, and a regular entry of 46 bytes plus its name's.
#[tokio::test(flavor = "multi_thread")]
async fn the_part_seam_of_a_directory_loses_no_entry() {
    // `small` entries of 6-byte names (52 bytes each), and one pad entry
    // whose name makes the whole exactly `CHUNK_BYTES`.
    let small = 20_086;
    let pad_len = CHUNK_BYTES - 5 - small * 52 - 46;
    assert!((1..=4096).contains(&pad_len), "pad name of {pad_len} bytes");
    let mut files: Vec<(Vec<u8>, &[u8])> = (0..small)
        .map(|i| (format!("e{i:05}").into_bytes(), &b"x\n"[..]))
        .collect();
    files.push((vec![b'z'; pad_len], b"x\n"));
    let (dir_a, store_a, root) = store(&files);
    let (_stop, mut client) = serve(&store_a, dir_a.path()).await;

    // Exactly one part, byte for byte as the format says.
    let (parts, bytes) = list_dir(&mut client, &root, b"").await;
    assert_eq!(parts, 1, "a tree of exactly {CHUNK_BYTES} bytes");
    assert_eq!(bytes.len(), CHUNK_BYTES, "the whole tree in one part");
    let tree = Tree::decode(&bytes).expect("the part is a whole TreeV1");
    assert_eq!(tree.entries().len(), small + 1, "every entry came");

    // One entry more: one part plus one entry, in two parts.
    let mut files: Vec<(Vec<u8>, &[u8])> = (0..small)
        .map(|i| (format!("e{i:05}").into_bytes(), &b"x\n"[..]))
        .collect();
    files.push((vec![b'z'; pad_len], b"x\n"));
    files.push((b"zz".to_vec(), b"x\n"));
    let (dir_b, store_b, root) = store(&files);
    let (_stop, mut client) = serve(&store_b, dir_b.path()).await;
    let (parts, bytes) = list_dir(&mut client, &root, b"").await;
    assert_eq!(parts, 2, "one entry over the part size");
    assert_eq!(
        bytes.len(),
        CHUNK_BYTES + 48,
        "the last part carries the one entry over"
    );
    let tree = Tree::decode(&bytes).expect("the parts are a whole TreeV1");
    assert_eq!(tree.entries().len(), small + 2, "every entry came");
}

/// An entry whose name is at the 4096-byte path cap lists with its whole
/// name, and the file reads back byte for byte under that name.
#[tokio::test(flavor = "multi_thread")]
async fn a_name_at_the_cap_lists_and_reads_back() {
    let name = vec![b'f'; 4096];
    let body = b"cap\n".to_vec();
    let (dir, store, root) = store(&[(name.clone(), &body)]);
    let (_stop, mut client) = serve(&store, dir.path()).await;

    let (parts, bytes) = list_dir(&mut client, &root, b"").await;
    assert_eq!(parts, 1);
    let tree = Tree::decode(&bytes).unwrap();
    assert_eq!(tree.entries().len(), 1);
    let listed = tree.entries().keys().next().unwrap();
    assert_eq!(listed.as_bytes(), &name, "the name lost no byte");

    let (parts, content) = read_file(&mut client, &root, &name).await;
    assert_eq!(parts, 1);
    assert_eq!(content, body, "the content is byte for byte");
}

/// A path over the 4096-byte cap is refused before any read: both calls
/// answer `INVALID_ARGUMENT`, not `INTERNAL`.
#[tokio::test(flavor = "multi_thread")]
async fn a_path_over_the_cap_is_refused_as_invalid_argument() {
    let (dir, store, root) = store(&[(b"file.txt".to_vec(), b"body\n")]);
    let (_stop, mut client) = serve(&store, dir.path()).await;
    let over = vec![b'g'; 4097];
    for note in ["list", "read"] {
        let err = if note == "list" {
            client
                .list_dir(ListDirRequest {
                    repo: REPO.to_string(),
                    changeset_id: root.as_bytes().to_vec(),
                    path: over.clone(),
                })
                .await
                .unwrap_err()
        } else {
            client
                .read_file(ReadFileRequest {
                    repo: REPO.to_string(),
                    changeset_id: root.as_bytes().to_vec(),
                    path: over.clone(),
                })
                .await
                .unwrap_err()
        };
        assert_eq!(err.code(), Code::InvalidArgument, "{note}: {err}");
    }
}

/// A file of zero bytes is a reply with no message; a file of exactly one
/// part arrives in one part; one byte more arrives in two. The parts
/// together are the content, byte for byte.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_of_zero_one_part_and_one_part_plus_one_byte() {
    let empty: &[u8] = b"";
    let one: Vec<u8> = vec![b'a'; CHUNK_BYTES];
    let mut over = one.clone();
    over.push(b'b');
    let (dir, store, root) = store(&[
        (b"empty.txt".to_vec(), empty),
        (b"one.bin".to_vec(), &one),
        (b"over.bin".to_vec(), &over),
    ]);
    let (_stop, mut client) = serve(&store, dir.path()).await;

    let (parts, content) = read_file(&mut client, &root, b"empty.txt").await;
    assert_eq!(parts, 0, "an empty file is a reply with no message");
    assert_eq!(content.len(), 0);

    let (parts, content) = read_file(&mut client, &root, b"one.bin").await;
    assert_eq!(parts, 1, "a file of exactly one part");
    assert_eq!(content.len(), CHUNK_BYTES);
    assert_eq!(content, one);

    let (parts, content) = read_file(&mut client, &root, b"over.bin").await;
    assert_eq!(parts, 2, "one byte over the part size");
    assert_eq!(content, over, "the parts together are the content");
}

#[path = "common/mod.rs"]
mod common;
