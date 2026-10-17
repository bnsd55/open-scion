//! Access rules over gRPC: which principal may read, create, land and
//! upload, and what a caller that may not read a repo sees.

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::auth::token_digest;
use scion_proto::listen::Limits;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{
    BlameRequest, CreateChangesetRequest, DiffRequest, GetChangesetRequest, GetLandRequest,
    GetObjectsRequest, GetRootTreeRequest, IsAncestorRequest, LandRequest, ListDirRequest,
    LogRequest, MergeBaseRequest, ObjectKind, ObjectRef, PutBlobRequest, ReadFileRequest,
    ResolveRefRequest,
};
use scion_server::access::Access;
use scion_server::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, service};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
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
type Objects = ObjectsClient<InterceptedService<Channel, Bearer>>;

/// alice may write `docs` of `main`, bob may only read `main`, ci may write
/// all of `main`. Every token is its principal's name.
fn access() -> Access {
    let text = format!(
        "token\talice\t{}\n\
         token\tbob\t{}\n\
         token\tci\t{}\n\
         write\talice\t{REPO}\tdocs\n\
         read\tbob\t{REPO}\n\
         write\tci\t{REPO}\n",
        hex("alice"),
        hex("bob"),
        hex("ci"),
    );
    Access::parse(&text).unwrap()
}

/// A token with a principal, but no rule for any repo.
fn no_rules() -> Access {
    Access::parse(&format!("token\tmallory\t{}\n", hex("mallory"))).unwrap()
}

fn hex(token: &str) -> String {
    blake3::Hash::from(token_digest(token)).to_hex().to_string()
}

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

/// A store whose repo `main` has one root changeset that puts `README`,
/// and its blob. Returns the store, root changeset, and blob id.
fn store() -> (tempfile::TempDir, Backend, ChangesetId, BlobId) {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let blob = store.blobs().put(b"# hello\n").unwrap();
    let root = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root =
                c.insert_changeset(repo, &changeset(vec![], "README", Some(regular(blob))))?;
            c.cas_ref(repo, "main", None, root)?;
            // As if uploaded to `main` with `PutBlob`.
            c.add_repo_blob(repo, blob)?;
            Ok(root)
        })
        .unwrap();
    (dir, store, root, blob)
}

/// Stores `content` as a blob of `REPO`, as an upload through `PutBlob`
/// would.
fn put(store: &Backend, content: &[u8]) -> BlobId {
    let blob = store.blobs().put(content).unwrap();
    store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            c.add_repo_blob(repo, blob)?;
            Ok(())
        })
        .unwrap();
    blob
}

/// A server of `store` with `access`, and a client of each service as
/// `token`. The first reply is the server's stop signal: hold it, or the
/// server shuts down when the caller drops it.
type Stop = tokio::sync::oneshot::Sender<()>;
async fn server(store: &Backend, access: Access, token: &str) -> (Stop, Scion, Objects) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(service::serve(
        store.clone(),
        listener,
        access,
        service::ServeOptions {
            limits: Limits::default(),
            quotas: scion_server::quotas::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
        },
        async {
            stopped.await.ok();
        },
    ));
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let bearer = Bearer(format!("Bearer {token}").parse().unwrap());
    (
        stop,
        ScionClient::with_interceptor(channel.clone(), bearer.clone()),
        ObjectsClient::with_interceptor(channel, bearer),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_outside_a_prefix_are_denied_with_the_path() {
    let (_dir, store, root, _blob) = store();
    let access = access();
    let (_alice_stop, mut alice, _) = server(&store, access.clone(), "alice").await;

    // Inside the prefix: accepted.
    let guide = put(&store, b"guide");
    let (bytes, _) = changeset(vec![root], "docs/guide", Some(regular(guide))).encode_with_id();
    alice
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        })
        .await
        .unwrap();

    // Outside it: denied, and the message names the path.
    let source = put(&store, b"source");
    let (bytes, _) = changeset(vec![root], "src/a", Some(regular(source))).encode_with_id();
    let status = alice
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    assert!(status.message().contains("src/a"), "{}", status.message());

    // Not by bytes alone: `docsx` is not below `docs`.
    let source = put(&store, b"sx");
    let (bytes, _) = changeset(vec![root], "docsx/readme", Some(regular(source))).encode_with_id();
    let status = alice
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    assert!(status.message().contains("docsx"), "{}", status.message());
}

#[tokio::test(flavor = "multi_thread")]
async fn put_upload_create_and_land_need_write_rules() {
    let (_dir, store, root, _blob) = store();
    let access = access();
    let (_bob_stop, mut bob, _) = server(&store, access, "bob").await;

    // A blob upload names no path, so a read-only principal is denied.
    let status = bob
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.into(),
            data: b"up".to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");

    // A create is denied with the path it would change.
    let x = put(&store, b"x");
    let (bytes, _) = changeset(vec![root], "docs/x", Some(regular(x))).encode_with_id();
    let status = bob
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    assert!(status.message().contains("docs/x"), "{}", status.message());

    // A land is denied: who may not write, may not land either.
    let status = bob
        .land(LandRequest {
            repo: REPO.into(),
            changeset_id: root.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "bob-1".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_lander_of_another_principals_changeset_needs_write() {
    let (_dir, store, root, _blob) = store();
    let access = access();

    // alice creates a changeset inside her prefix.
    let (_alice_stop, mut alice, _) = server(&store, access.clone(), "alice").await;
    let guide = put(&store, b"guide");
    let (bytes, id) = changeset(vec![root], "docs/guide", Some(regular(guide))).encode_with_id();
    alice
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        })
        .await
        .unwrap();

    // bob, read-only, cannot land it.
    let (_bob_stop, mut bob, _) = server(&store, access, "bob").await;
    let status = bob
        .land(LandRequest {
            repo: REPO.into(),
            changeset_id: id.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "bob-2".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
}

/// Fails the test unless `reply` is the not-found answer of a hidden repo.
fn denied<T>(name: &str, reply: Result<T, Status>) {
    let Err(status) = reply else {
        panic!("{name}: accepted")
    };
    assert_eq!(status.code(), Code::NotFound, "{name}: {status:?}");
}

/// The request of each RPC, with values that only the read check rejects.
mod req {
    use super::*;

    pub fn put_blob() -> PutBlobRequest {
        PutBlobRequest {
            repo: REPO.into(),
            data: b"x".to_vec(),
        }
    }

    pub fn create_changeset() -> CreateChangesetRequest {
        CreateChangesetRequest {
            repo: REPO.into(),
            changeset: b"not read".to_vec(),
        }
    }

    pub fn land(root: &[u8]) -> LandRequest {
        LandRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
            target_ref: "main".into(),
            request_key: "k".into(),
        }
    }

    pub fn get_land() -> GetLandRequest {
        GetLandRequest {
            repo: REPO.into(),
            land_id: "1".into(),
        }
    }

    pub fn resolve_ref() -> ResolveRefRequest {
        ResolveRefRequest {
            repo: REPO.into(),
            name: "main".into(),
        }
    }

    pub fn get_changeset(root: &[u8]) -> GetChangesetRequest {
        GetChangesetRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
        }
    }

    pub fn read_file(root: &[u8], path: &[u8]) -> ReadFileRequest {
        ReadFileRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
            path: path.to_vec(),
        }
    }

    pub fn list_dir(root: &[u8]) -> ListDirRequest {
        ListDirRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
            path: Vec::new(),
        }
    }

    pub fn log(root: &[u8]) -> LogRequest {
        LogRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
            limit: 1,
            path: Vec::new(),
        }
    }

    pub fn blame(root: &[u8], path: &[u8]) -> BlameRequest {
        BlameRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
            path: path.to_vec(),
            cursor: Vec::new(),
        }
    }

    pub fn merge_base(root: &[u8]) -> MergeBaseRequest {
        MergeBaseRequest {
            repo: REPO.into(),
            a_changeset_id: root.to_vec(),
            b_changeset_id: root.to_vec(),
        }
    }

    pub fn is_ancestor(root: &[u8]) -> IsAncestorRequest {
        IsAncestorRequest {
            repo: REPO.into(),
            ancestor_id: root.to_vec(),
            descendant_id: root.to_vec(),
        }
    }

    pub fn diff(root: &[u8]) -> DiffRequest {
        DiffRequest {
            repo: REPO.into(),
            old_changeset_id: root.to_vec(),
            new_changeset_id: root.to_vec(),
            path: Vec::new(),
            after: Vec::new(),
        }
    }

    pub fn get_root_tree(root: &[u8]) -> GetRootTreeRequest {
        GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
        }
    }

    pub fn get_objects(blob: &[u8]) -> GetObjectsRequest {
        GetObjectsRequest {
            repo: REPO.into(),
            refs: vec![ObjectRef {
                kind: ObjectKind::Blob as i32,
                id: blob.to_vec(),
            }],
            accept: Vec::new(),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_principal_without_a_read_rule_sees_no_repo_on_any_rpc() {
    let (_dir, store, root, blob) = store();
    let access = access();

    // Control: with a rule, the repo is there.
    let (_ci_stop, _, mut objects) = server(&store, access, "ci").await;
    let seen = objects.resolve_ref(req::resolve_ref()).await;
    assert!(seen.is_ok(), "{seen:?}");

    // mallory has a token, but no rule for any repo. Every RPC answers as
    // for a repo that does not exist.
    let (_mallory_stop, mut mallory, mut objects) = server(&store, no_rules(), "mallory").await;
    let (root, blob) = (root.as_bytes(), blob.as_bytes());

    denied(
        "PutBlob",
        mallory.put_blob(tokio_stream::once(req::put_blob())).await,
    );
    denied(
        "CreateChangeset",
        mallory.create_changeset(req::create_changeset()).await,
    );
    denied("Land", mallory.land(req::land(root)).await);
    denied("GetLand", mallory.get_land(req::get_land()).await);
    denied(
        "Scion/ResolveRef",
        mallory.resolve_ref(req::resolve_ref()).await,
    );
    denied(
        "GetChangeset",
        mallory.get_changeset(req::get_changeset(root)).await,
    );
    denied(
        "ReadFile",
        mallory.read_file(req::read_file(root, b"README")).await,
    );
    denied("ListDir", mallory.list_dir(req::list_dir(root)).await);
    denied("Log", mallory.log(req::log(root)).await);
    denied("Blame", mallory.blame(req::blame(root, b"README")).await);
    denied("MergeBase", mallory.merge_base(req::merge_base(root)).await);
    denied(
        "IsAncestor",
        mallory.is_ancestor(req::is_ancestor(root)).await,
    );
    denied("Diff", mallory.diff(req::diff(root)).await);
    denied(
        "Objects/ResolveRef",
        objects.resolve_ref(req::resolve_ref()).await,
    );
    denied(
        "GetRootTree",
        objects.get_root_tree(req::get_root_tree(root)).await,
    );
    denied(
        "GetObjects",
        objects.get_objects(req::get_objects(blob)).await,
    );
}

/// Four `Blame` calls of one big file run at once: each takes one of the 4
/// blame slots, and holds it for the seconds its diff takes, while it also
/// holds one of the 256 permits that writes and lands share. A `ReadFile`
/// must still run at once: the slots bound how many permits blame can hold.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_file_runs_while_the_blame_slots_are_full() {
    let (_dir, store, root, _blob) = store();
    // Five versions of a 1,048,576-line file: one blame call diffs at
    // least one version pair, which takes seconds, and stops at its diff
    // budget with a cursor.
    let line = "x".repeat(15) + "\n";
    let mut text = String::new();
    let mut puts = Vec::new();
    for n in 0..5 {
        text = format!("version {n}\n") + &line.repeat(1 << 20);
        puts.push(Some(regular(store.blobs().put(text.as_bytes()).unwrap())));
    }
    let tip = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let mut id = root;
            for put in puts {
                id = c.insert_changeset(repo, &changeset(vec![id], "big", put))?;
            }
            Ok(id)
        })
        .unwrap();
    // bob may read the repo, and so may blame and read its files.
    let (_stop, mut scion, _) = server(&store, access(), "bob").await;

    let mut blames = Vec::new();
    for _ in 0..4 {
        let mut client = scion.clone();
        let request = req::blame(tip.as_bytes(), b"big");
        blames.push(tokio::spawn(async move {
            client.blame(request).await.unwrap().into_inner()
        }));
    }
    // The blames hold their slots while they diff. A read needs none of
    // the 4 slots, and still gets a permit of the 256.
    let mut read = scion
        .read_file(req::read_file(tip.as_bytes(), b"big"))
        .await
        .unwrap()
        .into_inner();
    let mut read_back = Vec::new();
    while let Some(chunk) = read.next().await {
        read_back.extend(chunk.unwrap().data);
    }
    assert_eq!(read_back, text.as_bytes(), "the read answered at once");
    for blamed in blames {
        let reply = blamed.await.unwrap();
        assert!(!reply.ranges.is_empty(), "the blame answered");
    }
}
