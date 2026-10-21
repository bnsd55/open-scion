//! The audit log's line surface: every (event, outcome) kind the
//! server writes, and the field names and JSON types of each kind's
//! lines, in one golden. The values are this run's (the clock, the
//! ids, the counts) and pin nothing; the names and types are the
//! surface a log reader (and docs/audit-log.md) can rely on.
//!
//! The census is source-scan, so a kind the exercise misses cannot
//! hide: every `Event::new` literal and every `audit_intent` call's
//! event literal in src must be either exercised here or in
//! `NOT_YET_DRIVING` with its reason, and the exercised set must equal
//! the scanned set - nothing in src the golden misses, nothing in the
//! golden the code no longer writes.
//!
//! Updating on purpose: run with `SCION_UPDATE_AUDIT_GOLDEN=1`, commit
//! the changed `tests/audit_v1.golden`, and name the changed kind in a
//! changes/ fragment. A removed kind or a changed field type is a
//! break, never a re-pin.

use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, LandRequest};

use scion_server::test_support::audit::{Audit, Event};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::service;
use scion_server::{Access, Backend, Keys, Metrics, serve};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::service::interceptor::{InterceptedService, Interceptor};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/audit_v1.golden");
const REPO: &str = "main";

/// The kinds this test cannot drive in-process, with why. The census
/// [`the_audit_census_of_the_source_is_complete`] holds the full set;
/// this list is the gap the exercise leaves, exactly.
const NOT_EXERCISED: &[(&str, &str, &str)] = &[(
    "land",
    "failed",
    "the land verdict's own error is a ref race or a corrupt row; no \
     in-process call drives it, and its field shape joins the golden \
     when one does",
)];

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

/// Adds `authorization: Bearer <token>` to every request.
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

/// Adds `authorization: Bearer ci` and an `x-request-id`, so the
/// audit's intent and result lines join by it.
#[derive(Clone)]
struct WriteCall(String);

impl Interceptor for WriteCall {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let value = "Bearer ci".to_string().parse().unwrap();
        request.metadata_mut().insert("authorization", value);
        let id = self.0.parse().unwrap();
        request.metadata_mut().insert("x-request-id", id);
        Ok(request)
    }
}

type Writer = ScionClient<InterceptedService<Channel, WriteCall>>;

fn hex(name: &str) -> String {
    blake3::Hash::from(token_digest(name)).to_hex().to_string()
}

/// Polls the audit file until it holds `n` lines, or fails with what
/// it holds: the drain is a job, so the lines land after the reply.
async fn wait_for_lines(path: &std::path::Path, n: usize) {
    for _ in 0..100 {
        if fs::read(path).is_ok_and(|bytes| String::from_utf8_lossy(&bytes).lines().count() >= n) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let bytes = fs::read(path).unwrap_or_default();
    panic!(
        "the audit file never reached {n} lines: {:?}",
        String::from_utf8_lossy(&bytes)
    );
}

/// Polls the audit file until a line of `event`/`outcome` is in it.
/// The refused-auth line is the meter's: it flushes every ten seconds
/// and once more at stop, so a line count alone can pass while it is
/// still on the way.
async fn wait_for_kind(path: &std::path::Path, event: &str, outcome: &str) {
    for _ in 0..100 {
        let has = fs::read(path).is_ok_and(|bytes| {
            String::from_utf8_lossy(&bytes).lines().any(|line| {
                serde_json::from_str::<serde_json::Value>(line).is_ok_and(|line| {
                    line["event"] == *event && line["outcome"] == *outcome
                })
            })
        });
        if has {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let bytes = fs::read(path).unwrap_or_default();
    panic!(
        "the audit file never held a {event}/{outcome} line: {:?}",
        String::from_utf8_lossy(&bytes)
    );
}

/// The parsed lines of the audit file.
fn read_lines(path: &std::path::Path) -> Vec<serde_json::Value> {
    let bytes = fs::read(path).unwrap();
    String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

/// A store whose repo `main` has one root changeset that puts
/// `README`, and a writer who may change every path.
fn store(dir: &std::path::Path) -> Backend {
    let store = Backend::open_test(dir).unwrap();
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
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(repo, &cs)?;
            c.cas_ref(repo, "main", None, root)?;
            c.add_repo_blob(repo, blob)?;
            Ok(root)
        })
        .unwrap();
    store
}

/// The server of `store` with a real audit file, as a `http://` URL.
async fn serve_with_audit(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
    audit: scion_server::test_support::audit::Audit,
) -> (tokio::sync::oneshot::Sender<()>, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(
        store.clone(),
        listener,
        access,
        service::ServeOptions {
            data: dir.to_path_buf(),
            audit,
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::new(Metrics::default()),
        },
        Some(Arc::new(Keys::generate())),
        async {
            stopped.await.ok();
        },
    ));
    (stop, format!("http://{addr}"))
}

/// A client with a good token, or a wrong one for the refused line.
async fn client(url: &str, token: &str) -> Scion {
    let channel = Channel::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let value = format!("Bearer {token}").parse().unwrap();
    ScionClient::with_interceptor(channel, Bearer(Some(value)))
}

/// A write call with an `x-request-id`, so the pairs join.
async fn writer(url: &str, request_id: &str) -> Writer {
    let channel = Channel::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    ScionClient::with_interceptor(channel, WriteCall(request_id.to_owned()))
}

/// A changeset with one parent that deletes `path`.
/// A changeset with one parent that puts `path`.
fn changeset(
    parents: Vec<scion_core::ChangesetId>,
    path: &str,
    content: scion_core::BlobId,
) -> scion_core::Changeset {
    use scion_core::{FileState, IdentityTime, RepoPath};
    scion_core::Changeset::new(
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
        [(
            RepoPath::new(path).unwrap(),
            Some(FileState::Regular {
                content,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format")
}

/// A writer's access: the `ci` token may change every path of `main`.
fn write_access() -> Access {
    let text = format!(
        "token\tci\t{}\nwrite\tci\t{REPO}\nread\tci\t{REPO}\n",
        hex("ci")
    );
    Access::parse(&text).expect("the access file parses")
}

/// The (event, outcome) kinds the source writes, read from src at test
/// time: every `Event::new("k", "o")` literal, plus every event an
/// `audit_intent` call names (its first string argument).
fn source_kinds() -> Vec<String> {
    let mut kinds = Vec::new();
    let mut stack = vec![PathBuf::from(ROOT).join("crates/scion-server/src")];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("src reads") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let Ok(text) = fs::read_to_string(&path) else {
                continue;
            };
            let mut rest = text.as_str();
            while let Some(at) = rest.find("Event::new(") {
                let after = &rest[at + "Event::new(".len()..];
                let mut parts = after.split('"');
                parts.next();
                let event = parts.next().unwrap_or_default().to_owned();
                parts.next();
                let outcome = parts.next().unwrap_or_default().to_owned();
                let named = |k: &str| {
                    !k.is_empty() && k.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
                };
                if named(&event) && named(&outcome) {
                    kinds.push(format!("{event}/{outcome}"));
                }
                rest = &rest[at + 11..];
            }
        }
    }
    kinds.sort();
    kinds.dedup();
    kinds
}

/// The event kinds of the intent lines, read from the `audit_intent`
/// call sites (the event is the call's first string argument).
fn source_intent_kinds() -> Vec<String> {
    let mut kinds = Vec::new();
    let mut stack = vec![PathBuf::from(ROOT).join("crates/scion-server/src")];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("src reads") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let Ok(text) = fs::read_to_string(&path) else {
                continue;
            };
            let mut rest = text.as_str();
            while let Some(at) = rest.find("audit_intent(") {
                let after = &rest[at + 13..];
                // The matching close paren, so nested calls like
                // `json!(...)` stay inside the window.
                let mut depth = 0usize;
                let mut close = 0;
                for (i, c) in after.char_indices() {
                    match c {
                        '(' => depth += 1,
                        ')' => {
                            if depth == 0 {
                                close = i;
                                break;
                            }
                            depth -= 1;
                        }
                        _ => {}
                    }
                }
                if let Some(quoted) = after[..close].split('"').nth(1) {
                    let named = !quoted.is_empty()
                        && quoted.bytes().all(|b| b.is_ascii_lowercase() || b == b'_');
                    if named {
                        kinds.push(format!("{quoted}/intent"));
                    }
                }
                rest = &rest[at + 13..];
            }
        }
    }
    kinds
}

#[test]
fn the_audit_census_of_the_source_is_complete() {
    let kinds = source_kinds();
    assert_eq!(
        kinds,
        vec![
            "admin/ok",
            "auth/refused",
            "land/failed",
            "land/ok",
            "ref/ok",
            "write/failed",
            "write/ok",
        ],
        "a new or changed kind updates this census and the golden"
    );
    let mut intents = source_intent_kinds();
    intents.sort();
    intents.dedup();
    assert_eq!(
        intents,
        vec!["land/intent", "write/intent"],
        "the state changes that record an intent line, read from src"
    );
}

/// The line shape of one rendered audit line: field name to JSON type.
fn shape(line: &serde_json::Value) -> BTreeMap<String, &'static str> {
    line.as_object()
        .expect("an audit line is an object")
        .iter()
        .map(|(k, v)| {
            let kind = match v {
                serde_json::Value::Null => "null",
                serde_json::Value::Bool(_) => "bool",
                serde_json::Value::Number(_) => "number",
                serde_json::Value::String(_) => "string",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Object(_) => "object",
            };
            (k.clone(), kind)
        })
        .collect()
}

/// The exercise: one refused call, the start line, a put, a create,
/// and the failed create. Returns the audit's parsed lines.
async fn exercise() -> (tempfile::TempDir, std::path::PathBuf) {
    use scion_proto::v1::{PutBlobRequest, ResolveRefRequest};
    use tokio_stream::once;

    // One refused call: one counted line for this process's address.
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    let audit_path = dir.path().join("audit.log");
    let audit = Audit::open(&audit_path).unwrap();
    audit
        .record(Event::new(
            "admin",
            "ok",
            serde_json::json!({ "what": "start" }),
        ))
        .await
        .unwrap();
    let (stop, url) = serve_with_audit(&store, dir.path(), write_access(), audit).await;
    let mut refused = client(&url, "wrong-token-9f2c7a").await;
    let err = refused
        .resolve_ref(ResolveRefRequest {
            repo: REPO.into(),
            name: "main".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated, "{err:?}");

    // The writes: an upload that lands, a create that fails in the
    // store job (its parent does not exist), a land that moves the ref.
    let mut ci = writer(&url, "t-put").await;
    let blob = store.blobs().put(b"guide\n").unwrap();
    ci.put_blob(once(PutBlobRequest {
        repo: REPO.into(),
        data: b"guide\n".to_vec(),
    }))
    .await
    .expect("the upload is allowed");
    let root = store.resolve_ref(REPO, "main").unwrap();
    let (bytes, id) = changeset(vec![root], "docs/guide", blob).encode_with_id();
    ci.create_changeset(CreateChangesetRequest {
        repo: REPO.into(),
        changeset: bytes,
    })
    .await
    .expect("the create is allowed");
    // A changeset whose parent is not in the store: the store job
    // fails, and the write's result line names the failure.
    let (_, never_stored) = changeset(vec![], "docs/never", blob).encode_with_id();
    let orphan = changeset(vec![never_stored], "docs/orphan", blob)
        .encode_with_id()
        .0;
    ci.create_changeset(CreateChangesetRequest {
        repo: REPO.into(),
        changeset: orphan,
    })
    .await
    .expect_err("the parent is not in the store");
    let mut ci = writer(&url, "t-land").await;
    ci.land(LandRequest {
        repo: REPO.into(),
        changeset_id: id.as_bytes().to_vec(),
        target_ref: "main".into(),
        request_key: "t-1".into(),
    })
    .await
    .expect("the land is allowed");

    // The stop flushes the meter's counts before the server ends.
    stop.send(()).unwrap();
    wait_for_kind(&audit_path, "auth", "refused").await;
    wait_for_lines(&audit_path, 10).await;
    (dir, audit_path)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_audit_lines_are_the_golden() {
    let (_dir, audit_path) = exercise().await;
    let lines = read_lines(&audit_path);
    let mut kinds: BTreeMap<String, BTreeMap<String, &'static str>> = BTreeMap::new();
    for line in &lines {
        let key = format!(
            "{}/{}",
            line["event"].as_str().unwrap_or("?"),
            line["outcome"].as_str().unwrap_or("")
        );
        kinds.entry(key).or_default().extend(shape(line));
    }
    // The exercise plus the declared-undrivable list must equal the
    // kinds the source writes, exactly.
    let undriven = NOT_EXERCISED
        .iter()
        .map(|(e, o, _)| format!("{e}/{o}"))
        .collect::<Vec<_>>();
    let mut all = kinds.keys().cloned().collect::<Vec<_>>();
    all.extend(undriven);
    all.sort();
    all.dedup();
    let mut every = source_kinds();
    every.extend(source_intent_kinds());
    every.sort();
    every.dedup();
    assert_eq!(
        all, every,
        "the exercise plus the declared-undrivable list must equal the \
         kinds the source writes, exactly"
    );

    let mut text = String::new();
    for (kind, fields) in &kinds {
        writeln!(text, "[{kind}]").unwrap();
        for (name, kind) in fields {
            writeln!(text, "{name}:{kind}").unwrap();
        }
    }
    if std::env::var_os("SCION_UPDATE_AUDIT_GOLDEN").is_some() {
        fs::write(GOLDEN, &text).unwrap();
    }
    let golden = fs::read_to_string(GOLDEN).unwrap();
    assert!(!golden.is_empty(), "the golden names the line surface");
    assert_eq!(
        text, golden,
        "the audit lines' surface changed: a removed kind or a changed \
         field shape is a break, never a re-pin; an added kind re-pins \
         on purpose with SCION_UPDATE_AUDIT_GOLDEN=1 and a changes/ \
         fragment naming it"
    );
}
