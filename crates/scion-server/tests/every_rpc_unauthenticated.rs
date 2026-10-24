//! Every RPC of both services answers `UNAUTHENTICATED` to a call that
//! carries no token or a wrong token, and none of the calls has a side
//! effect. The list of RPCs comes from `proto/scion.proto` itself: a new
//! RPC without a row here fails this test, so a new method cannot land
//! without auth coverage.

use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::SERVICE_NAME as OBJECTS_SERVICE;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::scion_server::SERVICE_NAME as SCION_SERVICE;
use scion_proto::v1::{
    BlameRequest, CreateChangesetRequest, DiffRequest, GetChangesetRequest, GetLandRequest,
    GetObjectsRequest, GetRootTreeRequest, IsAncestorRequest, LandRequest, ListDirRequest,
    LogRequest, MergeBaseRequest, PutBlobRequest, ReadFileRequest, ResolveRefRequest,
};

use scion_server::Access;
use scion_server::Keys;
use scion_server::Metrics;
use scion_server::test_support::backend::BackendExt;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, test_support::service};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::service::interceptor::{InterceptedService, Interceptor};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const REPO: &str = "main";

/// Adds `authorization: Bearer <token>` to every request. `None` adds
/// nothing: the client of a caller that sends no token.
#[derive(Clone)]
struct Bearer(Option<tonic::metadata::AsciiMetadataValue>);

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

fn access() -> Access {
    let digest = blake3::Hash::from(token_digest("ci")).to_hex().to_string();
    let text = format!("token\tci\t{digest}\nread\tci\t{REPO}\nwrite\tci\t{REPO}\n");
    Access::parse(&text).expect("the access file parses")
}

/// A store whose repo `main` has one root changeset that puts `README`,
/// and its blob.
async fn store() -> (tempfile::TempDir, Backend, scion_core::ChangesetId) {
    let dir = tempfile::tempdir().unwrap();
    let store = {
        let path = dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || Backend::open_test(&path).unwrap())
            .await
            .unwrap()
    };
    let blob = store.blobs().put(b"# hello\n").unwrap();
    let cs = scion_core::Changeset::new(
        vec![],
        scion_core::IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"root\n".to_vec(),
        [(
            scion_core::RepoPath::new("README").unwrap(),
            Some(scion_core::FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    let root = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(repo, &cs)?;
            c.cas_ref(repo, "main", None, root)?;
            c.add_repo_blob(repo, blob)?;
            Ok(root)
        })
        .unwrap();
    (dir, store, root)
}

/// The server of `store` behind `access`, as a `http://` URL. Hold the
/// stop signal, or the server shuts down when it drops.
async fn serve(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
) -> (tokio::sync::oneshot::Sender<()>, String) {
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
        Some(Arc::new(Keys::generate())),
        async {
            stopped.await.ok();
        },
    ));
    (stop, format!("http://{addr}"))
}

/// A client pair of `url` as `token`; `None` sends no token at all.
struct Pair {
    scion: Scion,
    objects: Objects,
}

fn client_with(channel: Channel, token: Option<&str>) -> Pair {
    let bearer = match token {
        None => Bearer(None),
        Some(token) => Bearer::from_token(token),
    };
    Pair {
        scion: ScionClient::with_interceptor(channel.clone(), bearer.clone()),
        objects: ObjectsClient::with_interceptor(channel, bearer),
    }
}

impl Bearer {
    fn from_token(token: &str) -> Self {
        Self(Some(
            format!("Bearer {token}")
                .parse()
                .expect("the header value is visible ASCII"),
        ))
    }
}

/// The requests that must be refused only for their token: real bytes, a
/// real changeset on `main`, and a real land. If the server refused one
/// of these for another reason, the side-effect check would say nothing.
struct Payload {
    put_blob: PutBlobRequest,
    create_changeset: CreateChangesetRequest,
    land: LandRequest,
}

/// One call of one RPC, drained to its end.
type Call = fn(&Pair, &Payload) -> Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>;

/// The RPCs of one service, and the call that reaches each. The row's
/// name is the method name as the proto spells it; the test fails when a
/// row has no `rpc` line, or an `rpc` line has no row.
struct Row {
    service: &'static str,
    rpc: &'static str,
    call: Call,
}

/// The same method on the client that has it; the other client's call of
/// the same name (the two services share `ResolveRef`) is covered by its
/// own row.
macro_rules! scion_only {
    ($method:ident, $request:ident) => {
        |pair: &Pair, _payload: &Payload| {
            let mut scion = pair.scion.clone();
            Box::pin(async move {
                scion.$method(<$request as Default>::default()).await?;
                Ok(())
            }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
        }
    };
}

macro_rules! objects_only {
    ($method:ident, $request:ty) => {
        |pair: &Pair, _payload: &Payload| {
            let mut objects = pair.objects.clone();
            Box::pin(async move {
                objects.$method(<$request as Default>::default()).await?;
                Ok(())
            }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
        }
    };
}

/// The rows: one per RPC of `proto/scion.proto`.
#[allow(clippy::too_many_lines)]
fn rows() -> Vec<Row> {
    vec![
        Row {
            service: "scion.v1.Scion",
            rpc: "PutBlob",
            call: |pair: &Pair, payload: &Payload| {
                let mut scion = pair.scion.clone();
                let request = payload.put_blob.clone();
                Box::pin(async move {
                    scion.put_blob(tokio_stream::once(request)).await?;
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
            },
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "CreateChangeset",
            call: |pair: &Pair, payload: &Payload| {
                let mut scion = pair.scion.clone();
                let request = payload.create_changeset.clone();
                Box::pin(async move {
                    scion.create_changeset(request).await?;
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
            },
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "Land",
            call: |pair: &Pair, payload: &Payload| {
                let mut scion = pair.scion.clone();
                let request = payload.land.clone();
                Box::pin(async move {
                    scion.land(request).await?;
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
            },
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "GetLand",
            call: scion_only!(get_land, GetLandRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "ResolveRef",
            call: scion_only!(resolve_ref, ResolveRefRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "GetChangeset",
            call: scion_only!(get_changeset, GetChangesetRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "ReadFile",
            call: scion_only!(read_file, ReadFileRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "ListDir",
            call: scion_only!(list_dir, ListDirRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "Log",
            call: scion_only!(log, LogRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "Blame",
            call: scion_only!(blame, BlameRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "MergeBase",
            call: scion_only!(merge_base, MergeBaseRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "IsAncestor",
            call: scion_only!(is_ancestor, IsAncestorRequest),
        },
        Row {
            service: "scion.v1.Scion",
            rpc: "Diff",
            call: scion_only!(diff, DiffRequest),
        },
        Row {
            service: "scion.v1.Objects",
            rpc: "ResolveRef",
            call: objects_only!(resolve_ref, ResolveRefRequest),
        },
        Row {
            service: "scion.v1.Objects",
            rpc: "GetRootTree",
            call: objects_only!(get_root_tree, GetRootTreeRequest),
        },
        Row {
            service: "scion.v1.Objects",
            rpc: "GetObjects",
            // The one RPC with no token check: its permission is the
            // read ticket (docs/architecture.md, section 8). A call with
            // no ticket at all is refused with PERMISSION_DENIED.
            call: |pair: &Pair, _payload: &Payload| {
                let mut objects = pair.objects.clone();
                Box::pin(async move {
                    objects
                        .get_objects(GetObjectsRequest {
                            refs: vec![scion_proto::v1::ObjectRef {
                                kind: scion_proto::v1::ObjectKind::Blob as i32,
                                id: vec![0; 32],
                                path: Vec::new(),
                            }],
                            accept: Vec::new(),
                            ticket: Vec::new(),
                        })
                        .await?;
                    Ok(())
                }) as Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>
            },
        },
    ]
}

/// The `(service, rpc)` pairs that `proto/scion.proto` declares, and the
/// service names the generated code claims. A new RPC without a row
/// fails the test; so does a row whose RPC is gone.
fn declared() -> (Vec<(String, String)>, Vec<String>) {
    // The proto of the repository: the same file the build compiles.
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto/scion.proto");
    let text = std::fs::read_to_string(&path).expect("proto/scion.proto is readable");
    let package = text
        .lines()
        .map(str::trim)
        .find_map(|line| {
            line.strip_prefix("package ")
                .and_then(|rest| rest.strip_suffix(';'))
        })
        .expect("the proto declares a package")
        .to_owned();
    let mut service = String::new();
    let mut declared = Vec::new();
    for line in text.lines().map(str::trim) {
        if let Some(name) = line
            .strip_prefix("service ")
            .and_then(|r| r.strip_suffix(" {"))
        {
            // The file's package makes the full name the generated code
            // publishes as SERVICE_NAME.
            service = format!("{package}.{}", name.trim());
        } else if let Some(rest) = line.strip_prefix("rpc ") {
            let name = rest.split('(').next().expect("rpc NAME(").trim().to_owned();
            declared.push((service.clone(), name));
        }
    }
    let generated = vec![SCION_SERVICE.to_owned(), OBJECTS_SERVICE.to_owned()];
    (declared, generated)
}

/// The repo's state that the calls must not change: the version of
/// `main`, and the row count of every metadata table. The tables come
/// from the store's own catalog, so a new table joins the check on its
/// own. On Postgres the rows are counted through a client of the test
/// database, in the schema of this data directory. The reads block, so
/// they run on a thread outside the runtime.
async fn state_of(store: &Backend, dir: &Path) -> (i64, Vec<i64>) {
    let store = store.clone();
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || state_of_blocking(&store, &dir))
        .await
        .unwrap()
}

fn state_of_blocking(store: &Backend, dir: &Path) -> (i64, Vec<i64>) {
    if Backend::test_is_postgres() {
        let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
        let schema = Backend::test_schema(dir).expect("the tests run on Postgres");
        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        let tables = client
            .query(
                "SELECT tablename FROM pg_tables
                 WHERE schemaname = current_schema()
                 ORDER BY tablename",
                &[],
            )
            .unwrap();
        let counts = tables
            .iter()
            .map(|row| {
                let table: String = row.get(0);
                client
                    .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
                    .unwrap()
                    .get(0)
            })
            .collect();
        let version = client
            .query_one("SELECT version FROM refs WHERE name = 'main'", &[])
            .unwrap()
            .get(0);
        return (version, counts);
    }
    let store = store.sqlite().expect("the test backend is SQLite");
    let counts = store
        .read(|c| {
            let mut names = c
                .prepare(
                    "SELECT name FROM sqlite_master
                     WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                     ORDER BY name",
                )
                .map_err(scion_server::test_support::error::from_sqlite)?;
            let names = names
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(scion_server::test_support::error::from_sqlite)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(scion_server::test_support::error::from_sqlite)?;
            names
                .iter()
                .map(|name| {
                    c.query_row::<i64, _, _>(&format!("SELECT COUNT(*) FROM {name}"), [], |row| {
                        row.get(0)
                    })
                    .map_err(scion_server::test_support::error::from_sqlite)
                })
                .collect()
        })
        .expect("the row counts read");
    let version = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?.expect("main exists").version)
        })
        .expect("main reads");
    (version, counts)
}

/// The changeset bytes of a new child of `root` that sets `path` to the
/// blob `content`, and its id.
fn changeset_on(
    root: &scion_core::ChangesetId,
    content: scion_core::BlobId,
    path: &str,
) -> (Vec<u8>, scion_core::ChangesetId) {
    let cs = scion_core::Changeset::new(
        vec![*root],
        scion_core::IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_001,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"a change\n".to_vec(),
        [(
            scion_core::RepoPath::new(path).unwrap(),
            Some(scion_core::FileState::Regular {
                content,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    let (bytes, id) = cs.encode_with_id();
    (bytes, id)
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn every_rpc_answers_unauthenticated_without_a_valid_token() {
    let (declared, generated) = declared();
    // The proto and the generated code agree on the services.
    for name in &generated {
        assert!(
            declared.iter().any(|(service, _)| service == name),
            "the generated service {name} is not in proto/scion.proto"
        );
    }
    // Every declared RPC has a row, and every row a declared RPC.
    let rows = rows();
    for (service, rpc) in &declared {
        assert!(
            rows.iter()
                .any(|row| row.service == service && row.rpc == rpc),
            "RPC {service}.{rpc} has no row: add one, so it cannot land without auth coverage"
        );
    }
    for row in &rows {
        assert!(
            declared
                .iter()
                .any(|(service, rpc)| service == row.service && rpc == row.rpc),
            "row {}.{}, but the proto does not declare it",
            row.service,
            row.rpc
        );
    }

    let (dir, store, root) = store().await;
    let (stop, url) = serve(&store, dir.path(), access()).await;

    let channel = common::channel(&url).await.unwrap();
    let none = client_with(channel.clone(), None);
    let wrong = client_with(channel.clone(), Some("wrong"));
    let mut ci = client_with(channel, Some("ci"));

    // With the right token, a real upload stores a blob. Its id feeds
    // the changeset and land payloads.
    let blob = ci
        .scion
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.into(),
            data: b"# side effect\n".to_vec(),
        }))
        .await
        .expect("the right token may upload")
        .into_inner()
        .blob_id;
    let content = scion_core::BlobId::from_bytes(blob.as_slice().try_into().unwrap());
    let (landed, landed_id) = changeset_on(&root, content, "LANDED");
    let (refused, _refused_id) = changeset_on(&root, content, "REFUSED");
    let payload = Payload {
        put_blob: PutBlobRequest {
            repo: REPO.into(),
            data: b"refused\n".to_vec(),
        },
        create_changeset: CreateChangesetRequest {
            repo: REPO.into(),
            changeset: refused,
        },
        land: LandRequest {
            repo: REPO.into(),
            changeset_id: landed_id.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "refused".into(),
        },
    };
    // The land names a changeset the server holds.
    ci.scion
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: landed,
        })
        .await
        .expect("the right token may create the changeset the land names");
    // The baseline: everything the setup stored, nothing more. The
    // refused calls and the controls are compared against it.
    let before = state_of(&store, dir.path()).await;

    for row in &rows {
        for (who, pair) in [("no token", &none), ("wrong token", &wrong)] {
            let reply = (row.call)(pair, &payload).await;
            let status = match reply {
                Ok(()) => panic!("{}.{}, {who}: the call went through", row.service, row.rpc),
                Err(status) => status,
            };
            let want = if row.rpc == "GetObjects" {
                Code::PermissionDenied
            } else {
                Code::Unauthenticated
            };
            assert_eq!(
                status.code(),
                want,
                "{}.{}, {who}: {}",
                row.service,
                row.rpc,
                status.message()
            );
        }
    }
    // No refused call moved a ref, stored a blob, or landed anything.
    let after = state_of(&store, dir.path()).await;
    assert_eq!(before, after, "a refused call changed the repo");

    // The positive control: the same requests, sent with the right
    // token, do change the state. Without this, the no-change assert
    // above would pass for a server that refuses everything.
    ci.scion
        .put_blob(tokio_stream::once(payload.put_blob.clone()))
        .await
        .expect("the upload the refused calls carried is a valid one");
    assert_ne!(
        state_of(&store, dir.path()).await,
        after,
        "the control upload changed nothing"
    );
    ci.scion
        .create_changeset(payload.create_changeset.clone())
        .await
        .expect("the changeset the refused calls carried is a valid one");
    assert_ne!(
        state_of(&store, dir.path()).await,
        after,
        "the control changeset changed nothing"
    );
    ci.scion
        .land(LandRequest {
            repo: REPO.into(),
            changeset_id: payload.land.changeset_id.clone(),
            target_ref: "main".into(),
            request_key: "control".into(),
        })
        .await
        .expect("the land the refused calls carried is a valid one");
    assert_ne!(
        state_of(&store, dir.path()).await,
        after,
        "the control land changed nothing"
    );

    // And with the right token, no row answers UNAUTHENTICATED: the
    // refusal of the calls above is about the token, nothing else.
    for row in &rows {
        let reply = (row.call)(&ci, &payload).await;
        if let Err(status) = reply {
            assert_ne!(
                status.code(),
                Code::Unauthenticated,
                "{}.{} with the right token: {}",
                row.service,
                row.rpc,
                status.message()
            );
        }
    }

    stop.send(()).ok();
}

#[path = "common/mod.rs"]
mod common;
