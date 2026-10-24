//! Access rules over gRPC: which principal may read, create, land and
//! upload, and what a caller that may not read a repo sees.

use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileState, IdentityTime, RepoPath, Tree, TreeId,
};
use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{
    BlameRequest, CreateChangesetRequest, DiffRequest, GetChangesetRequest, GetLandRequest,
    GetObjectsRequest, GetRootTreeRequest, IsAncestorRequest, LandRequest, ListDirRequest,
    LogRequest, MergeBaseRequest, ObjectKind, ObjectRef, PutBlobRequest, ReadFileRequest,
    ResolveRefRequest,
};
use std::sync::Arc;

use scion_server::Access;
use scion_server::test_support::backend::from_store;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::store::Store;

/// The second repo of the two-world comparisons: its tree holds `docs`
/// only, so the same path is genuinely missing there.
const OTHER: &str = "other";
use scion_server::Metrics;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::{Backend, test_support::service};
use std::future::Future;
use std::pin::Pin;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::{InterceptedService, Interceptor};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const REPO: &str = "main";

/// Adds `authorization: Bearer <token>` to every request. `None` adds
/// nothing: the client of a caller that sends no token, of the same client
/// type as one that does.
#[derive(Clone)]
struct Bearer(Option<AsciiMetadataValue>);

impl Bearer {
    fn token(value: AsciiMetadataValue) -> Self {
        Self(Some(value))
    }
}

impl Interceptor for Bearer {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = self.0.clone() {
            request.metadata_mut().insert("authorization", value);
        }
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
         read\talice\t{REPO}\n\
         write\talice\t{REPO}\tdocs\n\
         read\tbob\t{REPO}\n\
         write\tci\t{REPO}\n\
         read\tci\t{REPO}\n\
         token\tpat\t{}\n\
         read\tpat\t{REPO}\tdocs\n",
        hex("alice"),
        hex("bob"),
        hex("ci"),
        hex("pat"),
    );
    Access::parse(&text).unwrap()
}

/// A token with a principal, but no rule for any repo.
fn no_rules() -> Access {
    Access::parse(&format!("token\tmallory\t{}\n", hex("mallory"))).unwrap()
}

/// A line the parser does not know is refused at load: the access
/// file's keys are additive-only in 1.x, and an unknown key is an
/// error, never a silent rule (docs/stability.md, the file-format
/// row).
#[test]
fn an_unknown_line_is_refused() {
    let text = format!("token\talice\t{}\ndelete\talice\tr\n", hex("alice"));
    let error = Access::parse(&text).unwrap_err();
    assert!(
        error.contains("expected `token`, `read` or `write`"),
        "{error}"
    );
    // An unknown key on a well-formed line is refused too: a fifth
    // field is no rule the 0.x file promised.
    let text = "read\talice\tr\textra\textra\n".to_string();
    let error = Access::parse(&text).unwrap_err();
    assert!(
        error.contains("expected `token`, `read` or `write`"),
        "{error}"
    );
}

fn hex(token: &str) -> String {
    blake3::Hash::from(token_digest(token)).to_hex().to_string()
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

/// A server of `store` with `access`. The first reply is the server's stop
/// signal: hold it, or the server shuts down when the caller drops it. The
/// second is its `http://` URL.
type Stop = tokio::sync::oneshot::Sender<()>;
async fn serve(store: &Backend, dir: &std::path::Path, access: Access) -> (Stop, String) {
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
            metrics: Arc::new(Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(Arc::new(scion_server::Keys::generate())),
        async {
            stopped.await.ok();
        },
    ));
    (stop, format!("http://{addr}"))
}

/// A client of `url` as `token`; `None` sends no token at all.
async fn scion(url: &str, token: Option<&str>) -> Scion {
    let channel = common::channel(url).await.unwrap();
    match token {
        None => ScionClient::with_interceptor(channel, Bearer(None)),
        Some(token) => {
            let bearer = Bearer::token(format!("Bearer {token}").parse().unwrap());
            ScionClient::with_interceptor(channel, bearer)
        }
    }
}

/// An `Objects` client of `url` as `token`; `None` sends no token.
async fn objects(url: &str, token: Option<&str>) -> Objects {
    let channel = common::channel(url).await.unwrap();
    match token {
        None => ObjectsClient::with_interceptor(channel, Bearer(None)),
        Some(token) => {
            let bearer = Bearer::token(format!("Bearer {token}").parse().unwrap());
            ObjectsClient::with_interceptor(channel, bearer)
        }
    }
}

/// A server of `store` with `access`, and a client of each service as
/// `token`. The first reply is the server's stop signal: hold it, or the
/// server shuts down when the caller drops it.
async fn server(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
    token: &str,
) -> (Stop, Scion, Objects) {
    let (stop, url) = serve(store, dir, access).await;
    (
        stop,
        scion(&url, Some(token)).await,
        objects(&url, Some(token)).await,
    )
}

/// The write rules still decide, for a whole-repo writer, which path a
/// change may name. A partial reader writes nothing in v0.1 of the folder
/// rules, so its one answer is the uniform `UNIMPLEMENTED`.
#[tokio::test(flavor = "multi_thread")]
async fn writes_outside_a_prefix_are_denied_with_the_path() {
    let (dir, store, root, _blob) = store();
    let access = access();

    // ci may write every path: a change outside `docs` is fine.
    let (_ci_stop, mut ci, _) = server(&store, dir.path(), access.clone(), "ci").await;
    let guide = put(&store, b"guide");
    let (bytes, _) = changeset(vec![root], "src/guide", Some(regular(guide))).encode_with_id();
    ci.create_changeset(CreateChangesetRequest {
        repo: REPO.into(),
        changeset: bytes,
    })
    .await
    .unwrap();

    // alice may write `docs` only: inside the prefix is accepted, and a
    // path outside it is named by the denial.
    let (_alice_stop, mut alice, _) = server(&store, dir.path(), access, "alice").await;
    let source = put(&store, b"source");
    let (bytes, _) = changeset(vec![root], "docs/guide", Some(regular(source))).encode_with_id();
    alice
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        })
        .await
        .unwrap();
    for denied in ["src/a", "docsx/readme"] {
        let source = put(&store, b"source");
        let (bytes, _) = changeset(vec![root], denied, Some(regular(source))).encode_with_id();
        match alice
            .create_changeset(CreateChangesetRequest {
                repo: REPO.into(),
                changeset: bytes,
            })
            .await
        {
            Err(status) => {
                assert_eq!(
                    status.code(),
                    Code::PermissionDenied,
                    "{denied}: {status:?}"
                );
                assert!(
                    status.message().contains(&format!("{denied:?}")),
                    "{denied}: {}",
                    status.message()
                );
            }
            Ok(_) => panic!("{denied}: accepted"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn put_upload_create_and_land_need_write_rules() {
    let (dir, store, root, _blob) = store();
    let access = access();
    let (_bob_stop, mut bob, _) = server(&store, dir.path(), access, "bob").await;

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
    let (dir, store, root, _blob) = store();
    let access = access();

    // ci, who may write every path, creates a changeset.
    let (_ci_stop, mut ci, _) = server(&store, dir.path(), access.clone(), "ci").await;
    let guide = put(&store, b"guide");
    let (bytes, id) = changeset(vec![root], "docs/guide", Some(regular(guide))).encode_with_id();
    ci.create_changeset(CreateChangesetRequest {
        repo: REPO.into(),
        changeset: bytes,
    })
    .await
    .unwrap();

    // bob, read-only, cannot land it.
    let (_bob_stop, mut bob, _) = server(&store, dir.path(), access, "bob").await;
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

    pub(crate) fn put_blob() -> PutBlobRequest {
        PutBlobRequest {
            repo: REPO.into(),
            data: b"x".to_vec(),
        }
    }

    pub(crate) fn create_changeset() -> CreateChangesetRequest {
        CreateChangesetRequest {
            repo: REPO.into(),
            changeset: b"not read".to_vec(),
        }
    }

    pub(crate) fn land(root: &[u8]) -> LandRequest {
        LandRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
            target_ref: "main".into(),
            request_key: "k".into(),
        }
    }

    pub(crate) fn get_land() -> GetLandRequest {
        GetLandRequest {
            repo: REPO.into(),
            land_id: "1".into(),
        }
    }

    pub(crate) fn resolve_ref() -> ResolveRefRequest {
        ResolveRefRequest {
            repo: REPO.into(),
            name: "main".into(),
        }
    }

    pub(crate) fn get_changeset(root: &[u8]) -> GetChangesetRequest {
        GetChangesetRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
        }
    }

    pub(crate) fn read_file(root: &[u8], path: &[u8]) -> ReadFileRequest {
        read_file_at(REPO, root, path)
    }

    pub(crate) fn read_file_at(repo: &str, root: &[u8], path: &[u8]) -> ReadFileRequest {
        ReadFileRequest {
            repo: repo.into(),
            changeset_id: root.to_vec(),
            path: path.to_vec(),
        }
    }

    pub(crate) fn list_dir(root: &[u8]) -> ListDirRequest {
        ListDirRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
            path: Vec::new(),
        }
    }

    pub(crate) fn list_dir_at(repo: &str, root: &[u8], path: &[u8]) -> ListDirRequest {
        ListDirRequest {
            repo: repo.into(),
            changeset_id: root.to_vec(),
            path: path.to_vec(),
        }
    }

    pub(crate) fn log(root: &[u8]) -> LogRequest {
        LogRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
            limit: 1,
            path: Vec::new(),
        }
    }

    pub(crate) fn blame(root: &[u8], path: &[u8]) -> BlameRequest {
        BlameRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
            path: path.to_vec(),
            cursor: Vec::new(),
        }
    }

    pub(crate) fn merge_base(root: &[u8]) -> MergeBaseRequest {
        MergeBaseRequest {
            repo: REPO.into(),
            a_changeset_id: root.to_vec(),
            b_changeset_id: root.to_vec(),
        }
    }

    pub(crate) fn is_ancestor(root: &[u8]) -> IsAncestorRequest {
        IsAncestorRequest {
            repo: REPO.into(),
            ancestor_id: root.to_vec(),
            descendant_id: root.to_vec(),
        }
    }

    pub(crate) fn diff(root: &[u8]) -> DiffRequest {
        DiffRequest {
            repo: REPO.into(),
            old_changeset_id: root.to_vec(),
            new_changeset_id: root.to_vec(),
            path: Vec::new(),
            after: Vec::new(),
        }
    }

    pub(crate) fn get_root_tree(root: &[u8]) -> GetRootTreeRequest {
        GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: root.to_vec(),
        }
    }

    pub(crate) fn get_objects(blob: &[u8]) -> GetObjectsRequest {
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
    let (dir, store, root, blob) = store();
    let access = access();

    // Control: with a rule, the repo is there.
    let (_ci_stop, _, mut objects) = server(&store, dir.path(), access, "ci").await;
    let seen = objects.resolve_ref(req::resolve_ref()).await;
    assert!(seen.is_ok(), "{seen:?}");

    // mallory has a token, but no rule for any repo. Every RPC answers as
    // for a repo that does not exist.
    let (_mallory_stop, mut mallory, mut objects) =
        server(&store, dir.path(), no_rules(), "mallory").await;
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
    // The ticket is the only permission of an object read, so a call with
    // no ticket, or a bad one, says nothing about any object: it is
    // refused with `PERMISSION_DENIED` and names none. What hides is an
    // object outside a valid ticket's walk: that one is answered
    // `NOT_FOUND`, as tests/tickets.rs pins (`refused`).
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
fn repo_with_docs_and_secret() -> (tempfile::TempDir, Backend, ChangesetId, BlobId, BlobId) {
    let (dir, store, root, _blob) = store();
    let (docs_blob, secret_blob) = (put(&store, b"a\n"), put(&store, b"b\n"));
    let tip = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let cs = changeset(vec![root], "docs/a", Some(regular(docs_blob)));
            let mut changes = cs.changes().clone();
            changes.insert(
                RepoPath::new("secret/b").unwrap(),
                Some(regular(secret_blob)),
            );
            let cs = Changeset::new(
                cs.parents().to_vec(),
                cs.author().clone(),
                cs.committer().cloned(),
                cs.message().to_vec(),
                changes,
            )
            .expect("changeset fits the format");
            c.insert_changeset(repo, &cs)
        })
        .unwrap();
    (dir, store, tip, docs_blob, secret_blob)
}

/// The tree ID of `secret` under the real root of `tip`.
fn root_tree_id(store: &Backend, tip: ChangesetId) -> TreeId {
    let repo_id = store.read(|c| c.repo_id(REPO)).unwrap();
    let root = scion_server::test_support::tree::root_tree(
        store,
        repo_id,
        tip,
        &scion_server::test_support::deadline::Deadline::new(
            std::time::Instant::now() + std::time::Duration::from_secs(300),
        ),
    )
    .unwrap();
    let tree = store.read(|c| c.get_tree(root)).unwrap();
    match tree
        .entries()
        .get(&scion_core::PathElem::new(b"secret".to_vec()).unwrap())
    {
        Some(Entry::Dir(id)) => *id,
        other => panic!("secret is a directory of the real root: {other:?}"),
    }
}

/// A partial reader resolves its ref and gets its view root: `docs` is
/// there, `secret` is not, a visible file reads back, and a hidden path
/// answers exactly like a missing one.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_reader_reads_only_its_view() {
    let (dir, store, tip, _docs, _secret) = repo_with_docs_and_secret();
    let (_stop, mut scion, mut objects) = server(&store, dir.path(), partial_access(), "pat").await;
    // The missing world: a second repo whose tree has `docs` only, served
    // to a whole reader, for the same-path comparison below.
    let docs_only = put(&store, b"a\n");
    let other_tip = store
        .write(|c| {
            let repo = c.create_repo(OTHER)?;
            c.insert_changeset(repo, &changeset(vec![], "docs/a", Some(regular(docs_only))))
        })
        .unwrap();
    let whole_text = format!("token\tbob\t{}\nread\tbob\t{OTHER}\n", hex("bob"));
    let (_whole_stop, mut whole_scion, _whole_objects) = server(
        &store,
        dir.path(),
        Access::parse(&whole_text).unwrap(),
        "bob",
    )
    .await;

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
    assert!(listed.entries().contains_key(&elem(b"docs")), "{listed:?}");
    assert!(
        !listed.entries().contains_key(&elem(b"secret")),
        "{listed:?}"
    );

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
    // A hidden path answers exactly like a missing one: the same code and
    // the same message bytes for the SAME path, in the world where the
    // rules hide it (pat's view) and in one where it does not exist (a
    // second repo whose tree has only docs).
    let hidden = scion
        .read_file(req::read_file(tip.as_bytes(), b"secret/b"))
        .await
        .unwrap_err();
    let missing = whole_scion
        .read_file(req::read_file_at(OTHER, other_tip.as_bytes(), b"secret/b"))
        .await
        .unwrap_err();
    assert_eq!(hidden.code(), missing.code(), "{hidden:?} vs {missing:?}");
    assert_eq!(hidden.message(), missing.message());
    let hidden = scion
        .list_dir(req::list_dir_at(REPO, tip.as_bytes(), b"secret"))
        .await
        .unwrap_err();
    let missing = whole_scion
        .list_dir(req::list_dir_at(OTHER, other_tip.as_bytes(), b"secret"))
        .await
        .unwrap_err();
    assert_eq!(hidden.code(), missing.code(), "{hidden:?} vs {missing:?}");
    assert_eq!(hidden.message(), missing.message());
}

/// The history calls that the v0.1 of the folder rules does not serve, each
/// with the same uniform refusal.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_reader_gets_no_history() {
    let (dir, store, tip, _docs, _secret) = repo_with_docs_and_secret();
    let (_stop, mut scion, _) = server(&store, dir.path(), partial_access(), "pat").await;
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
    let (dir, store, tip, docs, secret) = repo_with_docs_and_secret();
    let (_stop, _scion, mut objects) = server(&store, dir.path(), partial_access(), "pat").await;
    // A whole-repo reader, for the real root it still gets.
    let whole_text = format!("token\tbob\t{}\nread\tbob\t{REPO}\n", hex("bob"));
    let (_whole_stop, _whole_scion, mut whole) = server(
        &store,
        dir.path(),
        Access::parse(&whole_text).unwrap(),
        "bob",
    )
    .await;
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
    // ID is refused however it is named. So are the real root tree and the
    // real `secret` tree: nothing that the view cannot reach comes back.
    let real = whole
        .get_root_tree(req::get_root_tree(tip.as_bytes()))
        .await
        .unwrap()
        .into_inner();
    let real_root = TreeId::try_from(real.tree_id.as_slice()).unwrap();
    assert_ne!(real_root, view_root, "the whole reader sees the real root");
    for refused_ref in [
        ObjectRef {
            kind: ObjectKind::Blob as i32,
            id: secret.as_bytes().to_vec(),
            path: b"docs/a".to_vec(),
        },
        ObjectRef {
            kind: ObjectKind::Blob as i32,
            id: secret.as_bytes().to_vec(),
            path: b"secret/b".to_vec(),
        },
        ObjectRef {
            kind: ObjectKind::Tree as i32,
            id: real_root.as_bytes().to_vec(),
            path: Vec::new(),
        },
        ObjectRef {
            kind: ObjectKind::Tree as i32,
            id: root_tree_id(&store, tip).as_bytes().to_vec(),
            path: b"secret".to_vec(),
        },
    ] {
        let mut refused = objects
            .get_objects(GetObjectsRequest {
                refs: vec![refused_ref],
                accept: Vec::new(),
                ticket: view.ticket.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        let status = match refused.message().await {
            Err(status) => status,
            Ok(Some(part)) => panic!("the hidden object came back: {part:?}"),
            Ok(None) => panic!("the hidden object request answered nothing"),
        };
        assert_eq!(status.code(), Code::NotFound, "{status:?}");
    }

    // A second reader with the same prefixes derives the same view: a
    // shared view root.
    let text = format!("token\tpat2\t{}\nread\tpat2\t{REPO}\tdocs\n", hex("pat2"));
    let shared = Access::parse(&text).unwrap();
    let (_stop2, _scion2, mut objects2) = server(&store, dir.path(), shared, "pat2").await;
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
/// The side channel the threat model names: a partial reader sees the
/// ref's real changeset ID, so a hidden-only landing shows through a new
/// ID over an unchanged view, with every path answer the same.
/// A partial reader writes nothing: the write calls all answer the same
/// uniform `UNIMPLEMENTED`, with no per-object difference.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_reader_writes_nothing() {
    let (dir, store, root, _blob) = store();
    let (_stop, mut scion, _) = server(&store, dir.path(), partial_access(), "pat").await;
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

/// The side channel the threat model names: a partial reader sees the
/// ref's real changeset ID, so a hidden-only landing shows through a new
/// ID over an unchanged view, with every path answer the same.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_reader_sees_a_hidden_landing_through_the_ref_id() {
    let (dir, store, tip, _docs, _secret) = repo_with_docs_and_secret();
    let (_stop, mut pat, mut objects) = server(&store, dir.path(), partial_access(), "pat").await;

    let first = pat
        .resolve_ref(req::resolve_ref())
        .await
        .unwrap()
        .into_inner()
        .changeset_id;
    let view = objects
        .get_root_tree(req::get_root_tree(tip.as_bytes()))
        .await
        .unwrap()
        .into_inner();

    // A change outside the reader's prefix lands on main, as a real land
    // would: the ref moves to it.
    let hidden = put(&store, b"hidden\n");
    let _landed = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let cs = c.insert_changeset(
                repo,
                &changeset(vec![tip], "secret/c", Some(regular(hidden))),
            )?;
            let version = c.get_ref(repo, "main")?.unwrap().version;
            assert!(c.cas_ref(repo, "main", Some(version), cs)?);
            Ok(cs)
        })
        .unwrap();

    let second = pat
        .resolve_ref(req::resolve_ref())
        .await
        .unwrap()
        .into_inner()
        .changeset_id;
    assert_ne!(first, second, "the landing shows through the ref's ID");
    let after = objects
        .get_root_tree(req::get_root_tree(&second))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(view.tree_id, after.tree_id, "the view root is unchanged");
}

/// The window the operations guide promises: a narrowed access file cuts
/// a principal at the next start, but an outstanding ticket keeps working
/// for its whole life - at most 15 minutes from signing - while the key
/// holds, and a new key cuts it at once.
#[tokio::test(flavor = "multi_thread")]
async fn an_outstanding_ticket_survives_a_narrowed_access_file_until_the_key_rotates() {
    let (dir, store, tip, docs, _secret) = repo_with_docs_and_secret();
    let full = Access::parse(&format!("token\tpat\t{}\nread\tpat\t{REPO}\n", hex("pat"))).unwrap();
    let narrowed = Access::parse(&format!("token\tnobody\t{}\n", hex("nobody"))).unwrap();

    let keys = scion_server::Keys::generate();
    let (stop, url) = serve_keys(&store, dir.path(), full, keys.clone()).await;
    let mut pat = objects(&url, Some("pat")).await;
    let ticket = pat
        .get_root_tree(req::get_root_tree(tip.as_bytes()))
        .await
        .unwrap()
        .into_inner()
        .ticket;
    let get = |ticket: Vec<u8>, url: String| async move {
        let mut objects = objects(&url, None).await;
        objects
            .get_objects(GetObjectsRequest {
                refs: vec![ObjectRef {
                    kind: ObjectKind::Blob as i32,
                    id: docs.as_bytes().to_vec(),
                    path: b"docs/a".to_vec(),
                }],
                accept: Vec::new(),
                ticket,
            })
            .await
    };
    let mut reply = get(ticket.clone(), url.clone()).await.unwrap().into_inner();
    while (reply.message().await.unwrap()).is_some() {}
    drop(pat);
    stop.send(()).ok();

    // The access file no longer knows `pat`. The outstanding ticket still
    // serves, because its permission is the walk, not the rules.
    let (_stop, url) = serve_keys(&store, dir.path(), narrowed.clone(), keys.clone()).await;
    let mut reply = get(ticket.clone(), url.clone()).await.unwrap().into_inner();
    while (reply.message().await.unwrap()).is_some() {}

    // A new key cuts the old ticket at once.
    let (_stop, url) =
        serve_keys(&store, dir.path(), narrowed, scion_server::Keys::generate()).await;
    let err = get(ticket, url).await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    assert!(err.to_string().contains("bad ticket"), "{err:?}");
}

/// `serve` with explicit ticket keys, so a restart can keep the key - and
/// every outstanding ticket with it.
async fn serve_keys(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
    keys: scion_server::Keys,
) -> (Stop, String) {
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
            metrics: Arc::new(Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(Arc::new(keys)),
        async {
            stopped.await.ok();
        },
    ));
    (stop, format!("http://{addr}"))
}

/// The code that says "accepted".
const OK: Code = Code::Ok;
/// `UNAUTHENTICATED`, the answer to a call with no token, or a wrong one.
const U: Code = Code::Unauthenticated;
/// `NOT_FOUND`, the answer that hides a repo the caller may not read.
const N: Code = Code::NotFound;
/// `PERMISSION_DENIED`, the answer of a caller that may read but not write.
const P: Code = Code::PermissionDenied;
/// `UNIMPLEMENTED`, the answer of a partial reader's history and write
/// calls in v0.1 of the folder rules: the feature is off for such a
/// caller, the same text on every call.
const I: Code = Code::Unimplemented;

/// One caller column of the matrix. Adding a principal kind is one row
/// here and one entry in each row's `want`.
struct Who {
    name: &'static str,
    /// The server to call: the one with the rules of `access`, or the one
    /// whose only principal has no rule for any repo.
    rules: bool,
    /// The token to send; `None` sends no token at all.
    token: Option<&'static str>,
}

/// The caller columns, in the order of every row's `want`.
const CALLERS: [Who; 7] = [
    Who {
        name: "no token",
        rules: true,
        token: None,
    },
    Who {
        name: "wrong token",
        rules: true,
        token: Some("wrong"),
    },
    Who {
        name: "no rule",
        rules: false,
        token: Some("mallory"),
    },
    Who {
        name: "read-only",
        rules: true,
        token: Some("bob"),
    },
    Who {
        name: "prefix write",
        rules: true,
        token: Some("alice"),
    },
    Who {
        name: "full write",
        rules: true,
        token: Some("ci"),
    },
    Who {
        name: "folder read",
        rules: true,
        token: Some("pat"),
    },
];

/// The clients of one caller column.
#[derive(Clone)]
struct Clients {
    scion: Scion,
    objects: Objects,
}

/// The IDs the calls name.
struct Prep {
    root: Vec<u8>,
    blob: Vec<u8>,
    /// A changeset of `main` that puts `docs/guide`, inside alice's
    /// prefix, stored on the server.
    guide: Vec<u8>,
    /// A blob the repo has, for the rows that create changesets.
    content: Vec<u8>,
    /// Each caller column's own land id, as its land row answered. A
    /// column whose land was refused has none.
    lands: std::sync::Arc<std::sync::Mutex<[String; 7]>>,
}

type Fut = Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>;
type Call = fn(&Clients, &Prep, usize) -> Fut;

/// One row of the matrix: an RPC of one service, and the code it answers
/// each caller column with.
struct Row {
    rpc: &'static str,
    want: [Code; 7],
    call: Call,
}

fn put_blob(clients: &Clients, _prep: &Prep, _col: usize) -> Fut {
    let mut scion = clients.scion.clone();
    Box::pin(async move {
        scion.put_blob(tokio_stream::once(req::put_blob())).await?;
        Ok(())
    })
}

fn create_inside(clients: &Clients, prep: &Prep, _col: usize) -> Fut {
    let mut scion = clients.scion.clone();
    let bytes = changeset_bytes(prep, "docs/guide");
    Box::pin(async move {
        scion
            .create_changeset(CreateChangesetRequest {
                repo: REPO.into(),
                changeset: bytes,
            })
            .await?;
        Ok(())
    })
}

fn create_outside(clients: &Clients, prep: &Prep, _col: usize) -> Fut {
    let mut scion = clients.scion.clone();
    let bytes = changeset_bytes(prep, "src/a");
    Box::pin(async move {
        scion
            .create_changeset(CreateChangesetRequest {
                repo: REPO.into(),
                changeset: bytes,
            })
            .await?;
        Ok(())
    })
}

fn land_inside(clients: &Clients, prep: &Prep, col: usize) -> Fut {
    let mut scion = clients.scion.clone();
    let id = prep.guide.clone();
    let lands = std::sync::Arc::clone(&prep.lands);
    Box::pin(async move {
        let reply = scion
            .land(land_req(&id, "matrix-inside"))
            .await?
            .into_inner();
        lands.lock().unwrap()[col] = reply.land_id;
        Ok(())
    })
}

fn land_outside(clients: &Clients, prep: &Prep, col: usize) -> Fut {
    let mut scion = clients.scion.clone();
    let id = prep.root.clone();
    let lands = std::sync::Arc::clone(&prep.lands);
    Box::pin(async move {
        let reply = scion
            .land(land_req(&id, "matrix-outside"))
            .await?
            .into_inner();
        lands.lock().unwrap()[col] = reply.land_id;
        Ok(())
    })
}

/// Reads the outcome of the caller's own land: the id its land row
/// recorded. A caller whose land was refused has none, and its answer
/// hides like any other.
fn get_land(clients: &Clients, prep: &Prep, col: usize) -> Fut {
    let mut scion = clients.scion.clone();
    let id = prep.lands.lock().unwrap()[col].clone();
    Box::pin(async move {
        let req = GetLandRequest {
            repo: REPO.into(),
            land_id: if id.is_empty() { "1".into() } else { id },
        };
        scion.get_land(req).await?;
        Ok(())
    })
}

macro_rules! read_row {
    ($rpc:literal, $call:expr) => {
        Row {
            rpc: $rpc,
            want: [U, U, N, OK, OK, OK, OK],
            call: $call,
        }
    };
}

/// The read rows that a partial reader answers `UNIMPLEMENTED`: its
/// history is not served in v0.1 of the folder rules ([#121]).
macro_rules! history_row {
    ($rpc:literal, $call:expr) => {
        Row {
            rpc: $rpc,
            want: [U, U, N, OK, OK, OK, I],
            call: $call,
        }
    };
}

/// The encoded bytes of a changeset on top of the root, at `path`.
fn changeset_bytes(prep: &Prep, path: &str) -> Vec<u8> {
    let parent = ChangesetId::from_bytes(prep.root.clone().try_into().unwrap());
    let content = BlobId::from_bytes(prep.content.clone().try_into().unwrap());
    changeset(vec![parent], path, Some(regular(content))).encode()
}

fn land_req(id: &[u8], key: &str) -> LandRequest {
    LandRequest {
        repo: REPO.into(),
        changeset_id: id.to_vec(),
        target_ref: "main".into(),
        request_key: key.to_string(),
    }
}

/// A read of one changeset, drained to its end.
macro_rules! read_call {
    ($method:ident, $req:expr) => {
        |clients: &Clients, prep: &Prep, _col: usize| {
            let mut scion = clients.scion.clone();
            let req = $req(prep);
            Box::pin(async move {
                scion.$method(req).await?;
                Ok(())
            }) as Fut
        }
    };
}

/// A streaming read, drained to its end.
macro_rules! stream_call {
    ($method:ident, $req:expr) => {
        |clients: &Clients, prep: &Prep, _col: usize| {
            let mut scion = clients.scion.clone();
            let req = $req(prep);
            Box::pin(async move {
                let mut stream = scion.$method(req).await?.into_inner();
                while let Some(part) = stream.message().await? {
                    let _ = part;
                }
                Ok(())
            }) as Fut
        }
    };
}

fn resolve_ref_scion(clients: &Clients, _prep: &Prep, _col: usize) -> Fut {
    let mut scion = clients.scion.clone();
    Box::pin(async move {
        scion.resolve_ref(req::resolve_ref()).await?;
        Ok(())
    })
}

fn resolve_ref_objects(clients: &Clients, _prep: &Prep, _col: usize) -> Fut {
    let mut objects = clients.objects.clone();
    Box::pin(async move {
        objects.resolve_ref(req::resolve_ref()).await?;
        Ok(())
    })
}

fn get_root_tree(clients: &Clients, prep: &Prep, _col: usize) -> Fut {
    let mut objects = clients.objects.clone();
    let root = prep.root.clone();
    Box::pin(async move {
        objects.get_root_tree(req::get_root_tree(&root)).await?;
        Ok(())
    })
}

fn get_objects(clients: &Clients, prep: &Prep, _col: usize) -> Fut {
    let mut objects = clients.objects.clone();
    let blob = prep.blob.clone();
    Box::pin(async move {
        let mut stream = objects
            .get_objects(req::get_objects(&blob))
            .await?
            .into_inner();
        while let Some(part) = stream.message().await? {
            let _ = part;
        }
        Ok(())
    })
}

/// The matrix: every RPC of both services, and the code it answers each
/// caller column with. The rows that write name the paths they write, so
/// a prefix principal's cells split inside and outside its prefixes.
/// The table is data, so a new principal column is one row in `CALLERS`
/// and one entry in each `want`. The `folder read` column is the RPC
/// table of #121: `ResolveRef`, `GetRootTree`, ticket-carried reads and
/// path reads serve the caller's view; history and writes answer
/// `UNIMPLEMENTED` until the projected history exists.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn the_authz_matrix_answers_as_documented() {
    let (dir, store, root, blob) = store();

    // A changeset inside alice's prefix, stored as if she had created it.
    let guide_blob = put(&store, b"guide");
    let guide = store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let id = c.insert_changeset(
                repo,
                &changeset(vec![root], "docs/guide", Some(regular(guide_blob))),
            )?;
            c.add_repo_blob(repo, guide_blob)?;
            Ok(id)
        })
        .unwrap();
    let prep = Prep {
        root: root.as_bytes().to_vec(),
        blob: blob.as_bytes().to_vec(),
        guide: guide.as_bytes().to_vec(),
        content: put(&store, b"matrix").as_bytes().to_vec(),
        lands: std::sync::Mutex::new([const { String::new() }; 7]).into(),
    };

    // Two servers: one with the rules of `access`, one whose only
    // principal has no rule for any repo. Every column calls its own.
    let (_rules_stop, rules_url) = serve(&store, dir.path(), access()).await;
    let (_no_rules_stop, no_rules_url) = serve(&store, dir.path(), no_rules()).await;
    let mut clients = Vec::new();
    for who in &CALLERS {
        let url = if who.rules { &rules_url } else { &no_rules_url };
        clients.push(Clients {
            scion: scion(url, who.token).await,
            objects: objects(url, who.token).await,
        });
    }

    let rows: Vec<Row> = vec![
        Row {
            rpc: "PutBlob",
            // An upload names no path: a caller that may change some path
            // of the repo may upload.
            want: [U, U, N, P, OK, OK, I],
            call: put_blob,
        },
        Row {
            rpc: "CreateChangeset (inside the prefix)",
            want: [U, U, N, P, OK, OK, I],
            call: create_inside,
        },
        Row {
            rpc: "CreateChangeset (outside the prefix)",
            want: [U, U, N, P, P, OK, I],
            call: create_outside,
        },
        Row {
            rpc: "Land (inside the prefix)",
            want: [U, U, N, P, OK, OK, I],
            call: land_inside,
        },
        Row {
            rpc: "Land (outside the prefix)",
            want: [U, U, N, P, P, OK, I],
            call: land_outside,
        },
        Row {
            rpc: "GetLand (the caller's own)",
            // Land outcomes are scoped to the caller that made them: a
            // read-only principal has none, so its answer is the same
            // NOT_FOUND as a land it never made.
            want: [U, U, N, N, OK, OK, I],
            call: get_land,
        },
        read_row!("Scion/ResolveRef", resolve_ref_scion),
        history_row!(
            "GetChangeset",
            stream_call!(get_changeset, |p: &Prep| req::get_changeset(&p.root))
        ),
        // README is outside the folder reader's prefixes: hidden, the
        // same as a missing path.
        Row {
            rpc: "ReadFile",
            want: [U, U, N, OK, OK, OK, N],
            call: stream_call!(read_file, |p: &Prep| req::read_file(&p.root, b"README")),
        },
        // The folder reader's view of the root changeset is empty (the
        // changeset puts only README), which is a valid root: the list
        // answers empty and OK.
        read_row!(
            "ListDir",
            stream_call!(list_dir, |p: &Prep| req::list_dir(&p.root))
        ),
        history_row!("Log", read_call!(log, |p: &Prep| req::log(&p.root))),
        history_row!(
            "Blame",
            read_call!(blame, |p: &Prep| req::blame(&p.root, b"README"))
        ),
        history_row!(
            "MergeBase",
            read_call!(merge_base, |p: &Prep| req::merge_base(&p.root))
        ),
        history_row!(
            "IsAncestor",
            read_call!(is_ancestor, |p: &Prep| req::is_ancestor(&p.root))
        ),
        history_row!("Diff", read_call!(diff, |p: &Prep| req::diff(&p.root))),
        // A partial reader's ref resolves as today, and GetRootTree
        // answers its view root, with a ticket minted on it.
        read_row!("Objects/ResolveRef", resolve_ref_objects),
        read_row!("GetRootTree", get_root_tree),
        Row {
            rpc: "GetObjects (no ticket)",
            // The ticket is the only permission of an object read: a call
            // with none is refused, and names no object.
            want: [P, P, P, P, P, P, P],
            call: get_objects,
        },
    ];

    for row in &rows {
        for (col, who) in CALLERS.iter().enumerate() {
            let reply = (row.call)(&clients[col], &prep, col).await;
            let got = match reply {
                Ok(()) => OK,
                Err(status) => status.code(),
            };
            assert_eq!(got, row.want[col], "{} / {}", row.rpc, who.name);
        }
    }
}

/// Four `Blame` calls of one big file run at once: each takes one of the 4
/// blame slots, and holds it for the seconds its diff takes, while it also
/// holds one of the 256 permits that writes and lands share. A `ReadFile`
/// must still run at once: the slots bound how many permits blame can hold.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_file_runs_while_the_blame_slots_are_full() {
    let (dir, store, root, _blob) = store();
    // Five versions of a 1,048,576-line file (the line cap, so the
    // blames are served): one blame call diffs at least one version
    // pair, which takes seconds, and stops at its diff budget with a
    // cursor.
    let line = "x".repeat(15) + "\n";
    let mut text = String::new();
    let mut puts = Vec::new();
    for n in 0..5 {
        text = format!("version {n}\n") + &line.repeat((1 << 20) - 1);
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
    let (_stop, mut scion, _) = server(&store, dir.path(), access(), "bob").await;

    let mut blames = Vec::new();
    for _ in 0..4 {
        let mut client = scion.clone();
        let request = req::blame(tip.as_bytes(), b"big");
        blames.push(tokio::spawn(async move {
            let mut pages = client.blame(request).await.unwrap().into_inner();
            let mut ranges = Vec::new();
            while let Some(page) = pages.message().await.unwrap() {
                ranges.extend(page.ranges);
            }
            ranges
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
        let ranges = blamed.await.unwrap();
        assert!(!ranges.is_empty(), "the blame answered");
    }
}
// ---- The model check through the server: a partial reader's reads of
// ---- every path answer exactly like the model.

use proptest::prelude::*;

/// The file paths of a random tree: unique, nested, names from a small
/// alphabet. A path with a descendant in the set loses its file role.
fn file_paths() -> impl proptest::strategy::Strategy<Value = Vec<String>> {
    // Depth 1..4, so nested partial folders are reached; "-" and "." sort
    // before "/", so names that stress the prefix boundary are tried.
    proptest::collection::vec(proptest::collection::vec("[ab.-]{1,3}", 1..4), 2..12)
        .prop_filter("no dot-only part", |parts| {
            !parts
                .iter()
                .any(|names| names.iter().any(|n| n == "." || n == ".."))
        })
        .prop_map(|parts| {
            let mut paths: Vec<String> = Vec::new();
            for names in parts {
                let candidate = names.join("/");
                if !paths.contains(&candidate) {
                    paths.push(candidate);
                }
            }
            let has_descendant = |p: &String| paths.iter().any(|q| q.starts_with(&format!("{p}/")));
            let mut keep: Vec<String> = paths
                .iter()
                .filter(|p| !has_descendant(p))
                .cloned()
                .collect();
            keep.sort();
            keep
        })
}

/// The content of a file: unique per path.
fn content_of(path: &str) -> Vec<u8> {
    format!("content of {path}\n").into_bytes()
}

/// The read prefixes of the rules: a sorted subset of the paths and
/// their directories, with covered ones dropped (the file
/// canonicalizes anyway).
fn rules(paths: &[String], seeds: &[u16]) -> Vec<String> {
    let mut all: Vec<String> = Vec::new();
    for p in paths {
        let parts: Vec<&str> = p.split('/').collect();
        for i in 1..parts.len() {
            let dir = parts[..i].join("/");
            if !all.contains(&dir) {
                all.push(dir);
            }
        }
        if !all.contains(p) {
            all.push(p.clone());
        }
    }
    all.sort();
    let mut picked: Vec<String> = Vec::new();
    for s in seeds {
        let pick = &all[usize::from(*s) % all.len()];
        if !picked.contains(pick) {
            picked.push(pick.clone());
        }
    }
    picked.sort();
    let mut kept_rules: Vec<String> = Vec::new();
    for r in &picked {
        let covered = picked
            .iter()
            .any(|k| k != r && r.starts_with(&format!("{k}/")));
        if !covered {
            kept_rules.push(r.clone());
        }
    }
    kept_rules
}

/// The model: a path is kept iff a rule covers it.
fn kept(path: &str, rules: &[String]) -> bool {
    rules
        .iter()
        .any(|r| path == r || path.starts_with(&format!("{r}/")))
}

/// What the model shows at `dir` (the root at the empty path): the
/// names of the kept entries, files and directories.
fn shown_children(dir: &str, paths: &[String], rules: &[String]) -> Vec<Vec<u8>> {
    let prefix = if dir.is_empty() {
        String::new()
    } else {
        format!("{dir}/")
    };
    let mut direct: Vec<String> = Vec::new();
    for p in paths {
        if let Some(rest) = p.strip_prefix(prefix.as_str()) {
            let first = rest.split('/').next().unwrap().to_owned();
            if !direct.contains(&first) {
                direct.push(first);
            }
        }
    }
    // The tree lists its entries by name bytes; the first-seen order of
    // the paths is not that order once a name sorts before "/" ("-" vs
    // "---").
    direct.sort();
    let mut shown = Vec::new();
    for name in direct {
        let child = if dir.is_empty() {
            name.clone()
        } else {
            format!("{prefix}{name}")
        };
        let is_dir = paths.iter().any(|p| {
            p.strip_prefix(prefix.as_str())
                .is_some_and(|rest| rest.starts_with(&format!("{name}/")))
        });
        let kept = if is_dir {
            paths.iter().any(|p| {
                p.strip_prefix(prefix.as_str())
                    .is_some_and(|rest| rest.starts_with(&format!("{name}/")))
                    && kept(p, rules)
            })
        } else {
            kept(&child, rules)
        };
        if kept {
            shown.push(name.into_bytes());
        }
    }
    shown.sort();
    shown
}

/// One random case of the model check: two worlds, one server each. The
/// two request families read every path and list every directory against
/// the model and the twin world; splitting them would not shorten it.
#[allow(clippy::too_many_lines)]
fn a_partial_reader_case(paths: &[String], rule_seeds: &[u16]) -> Result<(), TestCaseError> {
    use std::fmt::Write as _;
    let rule_set = rules(paths, rule_seeds);
    if paths.is_empty() || rule_set.is_empty() {
        return Ok(());
    }
    let fail = |what: String| Err(TestCaseError::Fail(what.into()));

    // The store: a chain of changesets whose tip tree holds every path,
    // and each file's blob, as an upload would leave them.
    let dir = tempfile::tempdir().unwrap();
    let store = from_store(Store::open(dir.path()).unwrap());
    let tip = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut head: Option<ChangesetId> = None;
            for p in paths {
                let blob = store.blobs().put(&content_of(p)).unwrap();
                let parents = head.map(|id| vec![id]).unwrap_or_default();
                head = Some(c.insert_changeset(repo, &changeset(parents, p, Some(regular(blob))))?);
                c.add_repo_blob(repo, blob)?;
            }
            let tip = head.expect("a case holds at least one path");
            c.cas_ref(repo, "main", None, tip)?;
            Ok(tip)
        })
        .unwrap();

    // The access file: pat reads through the prefixes.
    let mut text = format!("token\tpat\t{}\n", hex("pat"));
    for r in &rule_set {
        let _ = writeln!(text, "read\tpat\t{REPO}\t{r}");
    }
    let access = Access::parse(&text).unwrap();

    // The second world: the same rules, but a tree where every hidden
    // path does not exist. A hidden path of the first world must answer
    // like its absent twin here, with the same code and message.
    let kept_paths: Vec<String> = paths
        .iter()
        .filter(|p| kept(p, &rule_set))
        .cloned()
        .collect();
    let dir_b = tempfile::tempdir().unwrap();
    let store_b = from_store(Store::open(dir_b.path()).unwrap());
    let tip_b = store_b
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut head: Option<ChangesetId> = None;
            for p in &kept_paths {
                let blob = store_b.blobs().put(&content_of(p)).unwrap();
                let parents = head.map(|id| vec![id]).unwrap_or_default();
                head = Some(c.insert_changeset(repo, &changeset(parents, p, Some(regular(blob))))?);
                c.add_repo_blob(repo, blob)?;
            }
            let tip = head.expect("a case keeps at least one path");
            c.cas_ref(repo, "main", None, tip)?;
            Ok(tip)
        })
        .unwrap();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (_stop, mut scion, _objects) = server(&store, dir.path(), access.clone(), "pat").await;
        let (_stop_b, mut scion_b, _objects_b) = server(&store_b, dir.path(), access, "pat").await;
        // What two answers must share to be the same: the code and the
        // message, not the bytes of the transport.
        let same = |a: &tonic::Status, b: &tonic::Status| {
            a.code() == b.code() && a.message() == b.message()
        };
        let read_all = |reply: tonic::Response<
            tonic::Streaming<scion_proto::v1::ReadFileResponse>,
        >| async move {
            let mut bytes = Vec::new();
            let mut stream = reply.into_inner();
            while let Some(part) = stream.message().await.unwrap() {
                bytes.extend_from_slice(&part.data);
            }
            bytes
        };

        // ReadFile of every file path. The two worlds answer together:
        // a covered path shows its content in both, a hidden path answers
        // like its absent twin, never with content of its own.
        for p in paths {
            let at = p.as_bytes();
            let (a, b) = (
                scion.read_file(req::read_file(tip.as_bytes(), at)).await,
                scion_b
                    .read_file(req::read_file(tip_b.as_bytes(), at))
                    .await,
            );
            match (a, b) {
                (Ok(a), Ok(b)) => {
                    let (bytes, bytes_b) = (read_all(a).await, read_all(b).await);
                    if bytes != bytes_b {
                        return fail(format!("the worlds differ for {p}"));
                    }
                    if !kept(p, &rule_set) {
                        return fail(format!("the hidden file {p} answered content"));
                    }
                    if bytes != content_of(p) {
                        return fail(format!("the content of {p} differs from the model"));
                    }
                }
                (Err(a), Err(b)) => {
                    if !same(&a, &b) {
                        return fail(format!(
                            "the hidden file {p} answers {a:?}, the absent one {b:?}"
                        ));
                    }
                    if kept(p, &rule_set) {
                        return fail(format!("the covered file {p} answers {a:?}"));
                    }
                }
                (a, b) => {
                    return fail(format!("the answers for {p} differ: {a:?} against {b:?}"));
                }
            }
        }

        // ListDir of the root and of every directory, kept or not, again
        // against the twin world.
        let mut dirs: Vec<String> = vec![String::new()];
        for p in paths {
            let parts: Vec<&str> = p.split('/').collect();
            for i in 1..parts.len() {
                dirs.push(parts[..i].join("/"));
            }
        }
        dirs.sort();
        dirs.dedup();
        for d in &dirs {
            let (a, b) = (
                scion
                    .list_dir(ListDirRequest {
                        repo: REPO.into(),
                        changeset_id: tip.as_bytes().to_vec(),
                        path: d.as_bytes().to_vec(),
                    })
                    .await,
                scion_b
                    .list_dir(ListDirRequest {
                        repo: REPO.into(),
                        changeset_id: tip_b.as_bytes().to_vec(),
                        path: d.as_bytes().to_vec(),
                    })
                    .await,
            );
            let expected = shown_children(d, paths, &rule_set);
            match (a, b) {
                (Ok(a), Ok(b)) => {
                    let names = |reply: tonic::Response<
                        tonic::Streaming<scion_proto::v1::ListDirResponse>,
                    >| async move {
                        let mut bytes = Vec::new();
                        let mut stream = reply.into_inner();
                        while let Some(part) = stream.message().await.unwrap() {
                            bytes.extend_from_slice(&part.tree);
                        }
                        let (listed, _id) = Tree::decode_with_id(&bytes).unwrap();
                        listed
                            .entries()
                            .keys()
                            .map(|k| k.as_bytes().to_vec())
                            .collect::<Vec<_>>()
                    };
                    let (shown_a, shown_b) = (names(a).await, names(b).await);
                    if shown_a != shown_b {
                        return fail(format!(
                            "the worlds list {d:?} differently: {shown_a:?} against {shown_b:?}"
                        ));
                    }
                    if kept(d, &rule_set) && shown_a != expected {
                        return fail(format!(
                            "the listing of {d:?} differs from the model: {shown_a:?}"
                        ));
                    }
                }
                (Err(a), Err(b)) => {
                    if !expected.is_empty() {
                        return fail(format!("the kept directory {d:?} answers {a:?}"));
                    }
                    if !same(&a, &b) {
                        return fail(format!(
                            "the hidden directory {d} answers {a:?}, the absent one {b:?}"
                        ));
                    }
                }
                (a, b) => {
                    return fail(format!("the listings of {d:?} differ: {a:?} against {b:?}"));
                }
            }
        }
        Ok(())
    })
}

/// Through the server: for random trees and rules, a partial reader's
/// `ReadFile` of every file path answers the covered content, and a
/// hidden path answers with the same code and message as the same path
/// in a second world with the same rules where it does not exist;
/// `ListDir` of every directory answers exactly the model's names, and a
/// hidden directory answers the same as an absent one. The runner is
/// built by hand with a fixed case count, so CI's `PROPTEST_CASES` does
/// not multiply these server-backed cases.
#[test]
fn a_partial_reader_answers_like_the_model() {
    let strategy = (
        file_paths(),
        proptest::collection::vec(proptest::num::u16::ANY, 1..5),
    );
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases: 12,
        ..proptest::test_runner::Config::default()
    });
    runner
        .run(&strategy, |(paths, rule_seeds)| {
            a_partial_reader_case(&paths, &rule_seeds)
        })
        .unwrap();
}

/// The pass-4 attacks on the shape of a prefix: a path that differs from
/// the rule only by case, a name that only starts like the rule, and a
/// principal name that differs by case. Paths are bytes: `DOCS/a` and
/// `docsx/a` are outside the rule `docs`, and `Pat` is not `pat`.
#[tokio::test(flavor = "multi_thread")]
async fn nothing_near_a_rule_is_served() {
    // One repo: `docs/a` inside the rule, `DOCS/a` and `docsx/a` beside
    // it, `secret/b` outside it.
    let (dir, store, tip) = {
        let (dir, store, root, _blob) = store();
        let (docs, upper, docsx, secret) = (
            put(&store, b"a\n"),
            put(&store, b"A\n"),
            put(&store, b"x\n"),
            put(&store, b"b\n"),
        );
        let cs = changeset(vec![root], "docs/a", Some(regular(docs)));
        let mut changes = cs.changes().clone();
        changes.insert(RepoPath::new("DOCS/a").unwrap(), Some(regular(upper)));
        changes.insert(RepoPath::new("docsx/a").unwrap(), Some(regular(docsx)));
        changes.insert(RepoPath::new("secret/b").unwrap(), Some(regular(secret)));
        let cs = Changeset::new(
            cs.parents().to_vec(),
            cs.author().clone(),
            cs.committer().cloned(),
            cs.message().to_vec(),
            changes,
        )
        .expect("changeset fits the format");
        let tip = store
            .write(|c| {
                let repo = c.repo_id(REPO).unwrap();
                c.insert_changeset(repo, &cs)
            })
            .unwrap();
        (dir, store, tip)
    };

    // A whole reader sees every one of them: only the rule hides.
    let whole = partial_access_whole();
    let (_stop, mut boss, _) = server(&store, dir.path(), whole, "boss").await;
    for path in ["DOCS/a", "docsx/a", "secret/b"] {
        boss.read_file(req::read_file(tip.as_bytes(), path.as_bytes()))
            .await
            .unwrap_or_else(|s| panic!("a whole reader reads {path}: {s:?}"));
    }

    // pat reads `docs`: its file is served; every near-miss answers
    // exactly as a path that is nowhere in the tree.
    let (_stop, mut pat, _) = server(&store, dir.path(), partial_access(), "pat").await;
    let served = pat
        .read_file(req::read_file(tip.as_bytes(), b"docs/a"))
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let mut stream = served.into_inner();
    while let Some(part) = stream.message().await.unwrap() {
        bytes.extend_from_slice(&part.data);
    }
    assert_eq!(bytes, b"a\n");
    let absent = pat
        .read_file(req::read_file(tip.as_bytes(), b"zzz/none"))
        .await
        .unwrap_err();
    for near in ["DOCS/a", "docsx/a", "Docs/a", "secret/b"] {
        let got = pat
            .read_file(req::read_file(tip.as_bytes(), near.as_bytes()))
            .await
            .unwrap_err();
        assert_eq!(
            got.code(),
            absent.code(),
            "{near} must answer as a missing path: {got:?} vs {absent:?}"
        );
        // The same refusal shape, with the path the caller sent echoed:
        // the reply tells a hidden path from a missing one by nothing but
        // the name the caller itself put in the request.
        let shape = |s: &tonic::Status| {
            let m = s.message();
            m.starts_with("path \"") && m.ends_with("\" not found")
        };
        assert!(shape(&got) && shape(&absent), "{near}: {got:?}");
    }
}

/// The access file that `nothing_near_a_rule_is_served`'s whole reader
/// uses: boss reads the whole of `main`.
fn partial_access_whole() -> Access {
    let text = format!("token\tboss\t{}\nread\tboss\t{REPO}\n", hex("boss"));
    Access::parse(&text).unwrap()
}

/// A prefix that is not a path is refused when the access file parses:
/// a trailing slash, an empty prefix, a parent step, and a walk through
/// one. The server refuses to start over such a rule.
#[test]
fn a_prefix_that_is_not_a_path_is_refused() {
    for bad in ["docs/", "", "..", "docs/../secret", "docs//a"] {
        let text = format!("token\tpat\t{}\nread\tpat\t{REPO}\t{bad}\n", hex("pat"));
        let why = Access::parse(&text).expect_err(&format!("the prefix {bad:?} must be refused"));
        assert!(
            why.contains("bad path"),
            "{bad:?}: the refusal must name the path: {why}"
        );
    }
}

/// A principal name that differs by case is another principal: the rules
/// of `pat` do not follow `Pat`, who sees no repo on any RPC.
#[tokio::test(flavor = "multi_thread")]
async fn a_principal_name_that_differs_by_case_is_another_principal() {
    let (dir, store, tip, _docs, _secret) = repo_with_docs_and_secret();
    let (_stop, mut pat, _) = server(&store, dir.path(), partial_access(), "pat").await;
    pat.resolve_ref(req::resolve_ref())
        .await
        .unwrap_or_else(|s| panic!("the rules' own principal reads: {s:?}"));

    // The token digest names the principal: `Pat` is no one here, so the
    // answer is the token refusal, not a repo answer.
    let (_stop, mut upper, _) = server(&store, dir.path(), partial_access(), "Pat").await;
    match upper.resolve_ref(req::resolve_ref()).await {
        Err(status) => assert_eq!(status.code(), Code::Unauthenticated, "{status:?}"),
        Ok(reply) => panic!("a token with no principal read the repo: {reply:?}"),
    }
    let _ = tip;
}

#[path = "common/mod.rs"]
mod common;
