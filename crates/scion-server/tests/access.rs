//! Access rules over gRPC: which principal may read, create, land and
//! upload, and what a caller that may not read a repo sees.

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath, Tree, TreeId};
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
use std::sync::Arc;

use scion_server::access::Access;
use scion_server::meta::MetaTx as _;
use scion_server::metrics::Metrics;
use scion_server::{Store, service};
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
         write\tci\t{REPO}\n\
         read\tci\t{REPO}\n",
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
fn store() -> (tempfile::TempDir, Store, ChangesetId, BlobId) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
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
fn put(store: &Store, content: &[u8]) -> BlobId {
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
async fn server(store: &Store, access: Access, token: &str) -> (Stop, Scion, Objects) {
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
            metrics: Arc::new(Metrics::default()),
        },
        Some(Arc::new(scion_server::tickets::Keys::generate())),
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

/// The write rules still decide, for a whole-repo writer, which path a
/// change may name. A partial reader writes nothing in v0.1 of the folder
/// rules, so its one answer is the uniform `UNIMPLEMENTED`.
#[tokio::test(flavor = "multi_thread")]
async fn writes_outside_a_prefix_are_denied_with_the_path() {
    let (_dir, store, root, _blob) = store();
    let access = access();

    // ci may write every path: a change outside `docs` is fine.
    let (_ci_stop, mut ci, _) = server(&store, access.clone(), "ci").await;
    let guide = put(&store, b"guide");
    let (bytes, _) = changeset(vec![root], "src/guide", Some(regular(guide))).encode_with_id();
    ci.create_changeset(CreateChangesetRequest {
        repo: REPO.into(),
        changeset: bytes,
    })
    .await
    .unwrap();

    // alice may read only `docs` (her write prefix): she writes nothing,
    // inside or outside her prefix, and both answers are the same.
    let (_alice_stop, mut alice, _) = server(&store, access, "alice").await;
    for at in ["docs/guide", "src/a", "docsx/readme"] {
        let source = put(&store, b"source");
        let (bytes, _) = changeset(vec![root], at, Some(regular(source))).encode_with_id();
        unimplemented(
            at,
            alice
                .create_changeset(CreateChangesetRequest {
                    repo: REPO.into(),
                    changeset: bytes,
                })
                .await,
        );
    }
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

    // ci, who may write every path, creates a changeset.
    let (_ci_stop, mut ci, _) = server(&store, access.clone(), "ci").await;
    let guide = put(&store, b"guide");
    let (bytes, id) = changeset(vec![root], "docs/guide", Some(regular(guide))).encode_with_id();
    ci.create_changeset(CreateChangesetRequest {
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
            refs: vec![ObjectRef {
                kind: ObjectKind::Blob as i32,
                id: blob.to_vec(),
                path: Vec::new(),
            }],
            accept: Vec::new(),
            ticket: Vec::new(),
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
    // SHORTCUT: GetObjects fails closed until C5 verifies tickets; then it
    // hides like the rest and this goes back to `denied`.
    let Err(status) = objects.get_objects(req::get_objects(blob)).await else {
        panic!("GetObjects: accepted")
    };
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
}

/// Fails the test unless `reply` is the uniform `UNIMPLEMENTED` answer that
/// a partial reader gets for the calls that the v0.1 of the folder rules
/// does not serve.
fn unimplemented<T>(name: &str, reply: Result<T, Status>) {
    let Err(status) = reply else {
        panic!("{name}: accepted")
    };
    assert_eq!(status.code(), Code::Unimplemented, "{name}: {status:?}");
    assert!(
        status.message().contains("folder read rules"),
        "{name}: {}",
        status.message()
    );
}

/// pat may read only `docs` of `main`: a partial reader, by folder rules.
fn partial_access() -> Access {
    let text = format!(
        "token\tpat\t{}\n\
         read\tpat\t{REPO}\tdocs\n",
        hex("pat"),
    );
    Access::parse(&text).unwrap()
}

/// The repo of the other tests: a root changeset with `README`, and a child
/// with `docs/a` and `secret/b`. The tip's ID, and the two blobs'.
fn repo_with_docs_and_secret() -> (tempfile::TempDir, Store, ChangesetId, BlobId, BlobId) {
    let (dir, store, root, _blob) = store();
    let (docs_blob, secret_blob) = (put(&store, b"a\n"), put(&store, b"b\n"));
    let tip = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let mut cs = changeset(vec![root], "docs/a", Some(regular(docs_blob)));
            cs.changes.insert(
                RepoPath::new("secret/b").unwrap(),
                Some(regular(secret_blob)),
            );
            c.insert_changeset(repo, &cs)
        })
        .unwrap();
    (dir, store, tip, docs_blob, secret_blob)
}

/// A partial reader resolves its ref and gets its view root: `docs` is
/// there, `secret` is not, a visible file reads back, and a hidden path
/// answers exactly like a missing one.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_reader_reads_only_its_view() {
    let (_dir, store, tip, _docs, _secret) = repo_with_docs_and_secret();
    let (_stop, mut scion, mut objects) = server(&store, partial_access(), "pat").await;

    // ResolveRef answers as today: the ref's real changeset.
    let seen = scion.resolve_ref(req::resolve_ref()).await.unwrap();
    assert!(
        !seen.into_inner().changeset_id.is_empty(),
        "the ref resolved"
    );

    // GetRootTree answers the view root, with a ticket bound to it.
    let view = objects
        .get_root_tree(req::get_root_tree(tip.as_bytes()))
        .await
        .unwrap()
        .into_inner();
    assert!(!view.ticket.is_empty(), "the ticket was minted");
    let view_root = TreeId::try_from(view.tree_id.as_slice()).unwrap();

    // ListDir from the view root: docs is there, secret is not.
    let elem = |raw: &[u8]| scion_core::PathElem::new(raw.to_vec()).unwrap();
    let mut listing = scion
        .list_dir(req::list_dir(tip.as_bytes()))
        .await
        .unwrap()
        .into_inner();
    let mut tree_bytes = Vec::new();
    while let Some(part) = listing.message().await.unwrap() {
        tree_bytes.extend_from_slice(&part.tree);
    }
    let (listed, listed_id) = Tree::decode_with_id(&tree_bytes).unwrap();
    assert_eq!(listed_id, view_root, "the list is of the view root");
    assert!(listed.entries.contains_key(&elem(b"docs")), "{listed:?}");
    assert!(!listed.entries.contains_key(&elem(b"secret")), "{listed:?}");

    // ReadFile serves a visible path and hides a hidden one, the same as a
    // missing one.
    let mut reply = scion
        .read_file(req::read_file(tip.as_bytes(), b"docs/a"))
        .await
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(part) = reply.message().await.unwrap() {
        bytes.extend_from_slice(&part.data);
    }
    assert_eq!(bytes, b"a\n");
    let hidden = scion
        .read_file(req::read_file(tip.as_bytes(), b"secret/b"))
        .await
        .unwrap_err();
    let missing = scion
        .read_file(req::read_file(tip.as_bytes(), b"no/such"))
        .await
        .unwrap_err();
    assert_eq!(hidden.code(), Code::NotFound, "{hidden:?}");
    assert_eq!(missing.code(), Code::NotFound, "{missing:?}");
    // The same shape of answer, with no hint that the path is hidden.
    assert!(
        hidden.message().starts_with("path "),
        "{}",
        hidden.message()
    );
}

/// The history calls that the v0.1 of the folder rules does not serve, each
/// with the same uniform refusal.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_reader_gets_no_history() {
    let (_dir, store, tip, _docs, _secret) = repo_with_docs_and_secret();
    let (_stop, mut scion, _) = server(&store, partial_access(), "pat").await;
    unimplemented(
        "GetChangeset",
        scion
            .get_changeset(req::get_changeset(tip.as_bytes()))
            .await,
    );
    unimplemented("Log", scion.log(req::log(tip.as_bytes())).await);
    unimplemented(
        "Blame",
        scion.blame(req::blame(tip.as_bytes(), b"docs/a")).await,
    );
    unimplemented(
        "MergeBase",
        scion.merge_base(req::merge_base(tip.as_bytes())).await,
    );
    unimplemented(
        "IsAncestor",
        scion.is_ancestor(req::is_ancestor(tip.as_bytes())).await,
    );
    unimplemented("Diff", scion.diff(req::diff(tip.as_bytes())).await);
}

/// A partial reader's ticket reaches only its view: the visible blob by its
/// path, a hidden blob refused however it is named, and a second reader
/// with the same prefixes deriving the same view.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_readers_ticket_reaches_only_its_view() {
    let (_dir, store, tip, docs, secret) = repo_with_docs_and_secret();
    let (_stop, _scion, mut objects) = server(&store, partial_access(), "pat").await;
    let view = objects
        .get_root_tree(req::get_root_tree(tip.as_bytes()))
        .await
        .unwrap()
        .into_inner();
    let view_root = TreeId::try_from(view.tree_id.as_slice()).unwrap();

    // The visible blob by its path in the view.
    let mut objects_reply = objects
        .get_objects(GetObjectsRequest {
            refs: vec![ObjectRef {
                kind: ObjectKind::Blob as i32,
                id: docs.as_bytes().to_vec(),
                path: b"docs/a".to_vec(),
            }],
            accept: Vec::new(),
            ticket: view.ticket.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut got = Vec::new();
    while let Some(part) = objects_reply.message().await.unwrap() {
        got.extend_from_slice(&part.data);
    }
    assert_eq!(got, b"a\n");

    // The hidden blob: the walk from the view root has no `secret`, so the
    // ID is refused however it is named.
    let mut refused = objects
        .get_objects(GetObjectsRequest {
            refs: vec![ObjectRef {
                kind: ObjectKind::Blob as i32,
                id: secret.as_bytes().to_vec(),
                path: b"docs/a".to_vec(),
            }],
            accept: Vec::new(),
            ticket: view.ticket,
        })
        .await
        .unwrap()
        .into_inner();
    let status = match refused.message().await {
        Err(status) => status,
        Ok(Some(part)) => panic!("the hidden blob came back: {part:?}"),
        Ok(None) => panic!("the hidden blob request answered nothing"),
    };
    assert_eq!(status.code(), Code::NotFound, "{status:?}");

    // A second reader with the same prefixes derives the same view: a
    // shared view root.
    let text = format!("token\tpat2\t{}\nread\tpat2\t{REPO}\tdocs\n", hex("pat2"));
    let shared = Access::parse(&text).unwrap();
    let (_stop2, _scion2, mut objects2) = server(&store, shared, "pat2").await;
    let again = objects2
        .get_root_tree(req::get_root_tree(tip.as_bytes()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        again.tree_id,
        view_root.as_bytes().to_vec(),
        "same prefixes, one view"
    );
}

/// A partial reader writes nothing: the write calls all answer the same
/// uniform `UNIMPLEMENTED`, with no per-object difference.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_reader_writes_nothing() {
    let (_dir, store, root, _blob) = store();
    let (_stop, mut scion, _) = server(&store, partial_access(), "pat").await;
    unimplemented(
        "PutBlob",
        scion.put_blob(tokio_stream::once(req::put_blob())).await,
    );
    unimplemented(
        "CreateChangeset",
        scion.create_changeset(req::create_changeset()).await,
    );
    unimplemented("Land", scion.land(req::land(root.as_bytes())).await);
    unimplemented("GetLand", scion.get_land(req::get_land()).await);
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
