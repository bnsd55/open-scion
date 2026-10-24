//! The write path over gRPC: `PutBlob`, `CreateChangeset`, then `ReadFile`,
//! against an in-process server.

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::CHUNK_BYTES;
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, PutBlobRequest, ReadFileRequest};
use scion_server::Metrics;
use scion_server::test_support::meta::MetaTx as _;
use std::sync::Arc;

use scion_server::test_support::meta::Meta as _;
use scion_server::{Backend, test_support::service};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tonic::Code;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const REPO: &str = "main";
const TOKEN: &str = "secret";

type Client = ScionClient<InterceptedService<Channel, Bearer>>;

/// A blob uploaded to another repo cannot be named in `main`, until it is
/// uploaded to `main` too. `root` is in `main`, and repo `other` exists.
async fn blobs_are_per_repo(mut client: Client, root: ChangesetId) {
    let content = b"other's file";
    let uploader = client.clone();
    let put = |repo: &str| {
        let upload = PutBlobRequest {
            repo: repo.into(),
            data: content.to_vec(),
        };
        let mut client = uploader.clone();
        async move { client.put_blob(tokio_stream::once(upload)).await.unwrap() }
    };
    put("other").await;
    let theirs = changeset(vec![root], "theirs", Some(regular(BlobId::of(content))));
    let create = CreateChangesetRequest {
        repo: REPO.into(),
        changeset: theirs.encode(),
    };
    let status = client.create_changeset(create.clone()).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert!(status.message().contains("is not stored"), "{status:?}");
    put(REPO).await;
    client.create_changeset(create).await.unwrap();
}

fn changeset(parents: Vec<ChangesetId>, path: &str, state: Option<FileState>) -> Changeset {
    Changeset::new(
        parents,
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"change\n".to_vec(),
        [(RepoPath::new(path).unwrap(), state)].into(),
    )
    .expect("changeset fits the format")
}

fn regular(content: BlobId) -> FileState {
    FileState::Regular {
        content,
        executable: false,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn uploaded_file_reads_back_through_a_created_changeset() {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let readme = store.blobs().put(b"# hello\n").unwrap();
    let root = store
        .write(|c| {
            c.create_repo("other")?;
            let repo = c.create_repo(REPO)?;
            c.insert_changeset(repo, &changeset(vec![], "README", Some(regular(readme))))
        })
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(service::serve(
        store,
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
        async {
            stopped.await.ok();
        },
    ));
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    let mut client = ScionClient::with_interceptor(channel.clone(), Bearer::new(TOKEN).unwrap());

    // More than one chunk, not a multiple of the chunk size, and a period
    // (251) that does not divide it, so reordered chunks change the bytes.
    let data: Vec<u8> = (0..251_u8).cycle().take(CHUNK_BYTES * 5 / 2).collect();
    let uploads: Vec<PutBlobRequest> = data
        .chunks(CHUNK_BYTES)
        .enumerate()
        .map(|(i, chunk)| PutBlobRequest {
            repo: if i == 0 { REPO.into() } else { String::new() },
            data: chunk.to_vec(),
        })
        .collect();
    assert_eq!(uploads.len(), 3);
    let blob = client
        .put_blob(tokio_stream::iter(uploads))
        .await
        .unwrap()
        .into_inner()
        .blob_id;
    assert_eq!(blob, BlobId::of(&data).as_bytes());

    let (bytes, expected) =
        changeset(vec![root], "big.bin", Some(regular(BlobId::of(&data)))).encode_with_id();
    let created = client
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        })
        .await
        .unwrap()
        .into_inner()
        .changeset_id;
    assert_eq!(created, expected.as_bytes());

    let mut stream = client
        .read_file(ReadFileRequest {
            repo: REPO.into(),
            changeset_id: created,
            path: b"big.bin".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut read = Vec::new();
    while let Some(chunk) = stream.next().await {
        read.extend(chunk.unwrap().data);
    }
    assert!(
        read == data,
        "read back {} bytes, wrote {}",
        read.len(),
        data.len()
    );

    blobs_are_per_repo(client.clone(), root).await;

    // A change that does not apply is the client's fault.
    let status = client
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: changeset(vec![root], "missing", None).encode(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");

    // A valid request, so only the missing token can reject it.
    let status = ScionClient::new(channel)
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: changeset(vec![root], "README", None).encode(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[path = "common/mod.rs"]
mod common;
