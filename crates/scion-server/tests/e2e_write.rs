//! The write path over gRPC: `PutBlob`, `CreateChangeset`, then `ReadFile`,
//! against an in-process server.

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::CHUNK_BYTES;
use scion_proto::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, PutBlobRequest, ReadFileRequest};
use scion_server::{Store, db, service};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const REPO: &str = "main";
const TOKEN: &str = "secret";

fn changeset(parents: Vec<ChangesetId>, path: &str, state: Option<FileState>) -> Changeset {
    Changeset {
        parents,
        author: IdentityTime {
            name: b"Ada".to_vec(),
            email: b"ada@example.com".to_vec(),
            unix_secs: 1_700_000_000,
            tz_offset_minutes: 0,
        },
        committer: None,
        message: b"change\n".to_vec(),
        changes: [(RepoPath::new(path).unwrap(), state)].into(),
    }
}

fn regular(content: BlobId) -> FileState {
    FileState::Regular {
        content,
        executable: false,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn uploaded_file_reads_back_through_a_created_changeset() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let readme = store.blobs().put(b"# hello\n").unwrap();
    let root = store
        .write(|c| {
            let repo = db::create_repo(c, REPO)?;
            db::insert_changeset(c, repo, &changeset(vec![], "README", Some(regular(readme))))
        })
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(service::serve(
        store,
        listener,
        TOKEN,
        Limits::default(),
        async {
            stopped.await.ok();
        },
    ));
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let bearer: AsciiMetadataValue = format!("Bearer {TOKEN}").parse().unwrap();
    let mut client = ScionClient::with_interceptor(channel.clone(), move |mut req: Request<()>| {
        req.metadata_mut().insert("authorization", bearer.clone());
        Ok::<_, Status>(req)
    });

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
