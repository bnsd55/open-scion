//! The audit log of a served server: refused authentication makes one
//! counted line per peer, never a line per call, and no line ever
//! carries the refused token or its digest.

use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::ResolveRefRequest;
use scion_proto::v1::scion_client::ScionClient;

use scion_core::ChangesetId;
use scion_server::test_support::audit::{Audit, Event};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::service;
use scion_server::{Access, Backend, Keys, Metrics, serve};
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::service::interceptor::{InterceptedService, Interceptor};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const REPO: &str = "main";

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

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

fn access() -> Access {
    let digest = blake3::Hash::from(token_digest("ci")).to_hex().to_string();
    let text = format!("token\tci\t{digest}\nread\tci\t{REPO}\n");
    Access::parse(&text).expect("the access file parses")
}

/// A store whose repo `main` has one root changeset that puts `README`,
/// and its blob.
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
/// Hold the stop signal, or the server shuts down when it drops.
async fn serve_with_audit(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
    audit: Audit,
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

async fn client(url: &str, token: &str) -> Scion {
    let channel = Channel::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let value = format!("Bearer {token}")
        .parse()
        .expect("a bearer header parses");
    ScionClient::with_interceptor(channel, Bearer(Some(value)))
}

fn read_lines(path: &std::path::Path) -> Vec<serde_json::Value> {
    let bytes = std::fs::read(path).unwrap();
    String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[tokio::test]
async fn refused_authentication_is_one_counted_line_per_peer_and_names_no_token() {
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
    let (stop, url) = serve_with_audit(&store, dir.path(), access(), audit).await;

    // Two peers, each with a stream of refused calls. The wrong token
    // is a secret: no line may ever carry it.
    let wrong = "wrong-token-9f2c7a";
    let (a, b) = tokio::join!(refused_stream(&url, wrong), refused_stream(&url, wrong));
    assert_eq!(a, Code::Unauthenticated);
    assert_eq!(b, Code::Unauthenticated);

    // The stop flushes the meter's counts before the server ends.
    stop.send(()).unwrap();
    wait_for_lines(&audit_path, 2).await;

    let lines = read_lines(&audit_path);
    assert_eq!(lines[0]["event"], "admin");
    let refusals: Vec<_> = lines[1..]
        .iter()
        .filter(|line| line["event"] == "auth" && line["outcome"] == "refused")
        .collect();
    // Both connections come from this test process, so one address:
    // one counted line, with every refusal on it. One line per peer,
    // never per call, and a port rotation cannot bury anyone.
    assert_eq!(refusals.len(), 1, "one line per address, not per call");
    assert_eq!(refusals[0]["count"], 40, "every refusal is counted");
    assert_eq!(refusals[0]["peer"], "127.0.0.1");
    let bytes = std::fs::read(&audit_path).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(!text.contains(wrong), "the refused token never appears");
    let digest = blake3::Hash::from(token_digest(wrong)).to_hex().to_string();
    assert!(
        !text.contains(&digest),
        "the refused token's digest never appears"
    );
}

/// Twenty refused calls from one connection.
async fn refused_stream(url: &str, token: &str) -> Code {
    let mut client = client(url, token).await;
    let mut code = None;
    for _ in 0..20 {
        let err = client
            .resolve_ref(ResolveRefRequest {
                repo: REPO.into(),
                name: "main".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated, "{err:?}");
        code = Some(err.code());
    }
    code.expect("twenty calls made")
}

/// The hex of a token, as the access file names it.
fn hex(name: &str) -> String {
    blake3::Hash::from(token_digest(name)).to_hex().to_string()
}

/// A store whose repo `main` has one root changeset that puts `README`,
/// and a writer who may change every path.
fn write_access() -> Access {
    let text = format!(
        "token\tci\t{}\nwrite\tci\t{REPO}\nread\tci\t{REPO}\n",
        hex("ci")
    );
    Access::parse(&text).expect("the access file parses")
}

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

/// Adds `authorization: Bearer ci` and an `x-request-id` to every
/// request, so the audit's intent and result lines join by it.
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

async fn writer(url: &str, request_id: &str) -> Writer {
    let channel = Channel::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    ScionClient::with_interceptor(channel, WriteCall(request_id.to_owned()))
}

/// Every state change records an intent line before it runs and a
/// result line after, and the pair joins by the request ID. No line
/// names a blob or a tree.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_changeset_and_land_record_intent_and_result_pairs() {
    use scion_proto::v1::{CreateChangesetRequest, LandRequest, PutBlobRequest};
    use tokio_stream::once;

    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    let audit_path = dir.path().join("audit.log");
    let audit = Audit::open(&audit_path).unwrap();
    let (stop, url) = serve_with_audit(&store, dir.path(), write_access(), audit).await;
    let mut ci = writer(&url, "t-put").await;

    // The write: an upload of a guide blob.
    let blob = store.blobs().put(b"guide\n").unwrap();
    ci.put_blob(once(PutBlobRequest {
        repo: REPO.into(),
        data: b"guide\n".to_vec(),
    }))
    .await
    .expect("the upload is allowed");

    // The changeset on top of the root, then the land.
    let root = store.resolve_ref(REPO, "main").unwrap();
    let (bytes, id) = changeset(vec![root], "docs/guide", blob).encode_with_id();
    let mut ci = writer(&url, "t-create").await;
    ci.create_changeset(CreateChangesetRequest {
        repo: REPO.into(),
        changeset: bytes.clone(),
    })
    .await
    .expect("the create is allowed");
    let mut ci = writer(&url, "t-land").await;
    ci.land(LandRequest {
        repo: REPO.into(),
        changeset_id: id.as_bytes().to_vec(),
        target_ref: "main".into(),
        request_key: "t-1".into(),
    })
    .await
    .expect("the land is allowed");

    stop.send(()).unwrap();
    wait_for_lines(&audit_path, 7).await;

    let lines = read_lines(&audit_path);
    // Every pair joins by its request ID, intent before result.

    // The write pair: bytes, and no blob digest anywhere.
    let writes = pairs(&lines, "write");
    assert_eq!(
        writes.len(),
        2,
        "one pair for the upload, one for the create"
    );
    let put = writes
        .iter()
        .find(|(intent, _)| intent["rpc"] == "PutBlob")
        .expect("the upload pair");
    assert_eq!(put.1["bytes"], 6, "the upload names its bytes");
    let create = writes
        .iter()
        .find(|(intent, _)| intent["rpc"] == "CreateChangeset")
        .expect("the create pair");
    assert!(
        create.1["changeset"].is_string(),
        "the create names the changeset"
    );

    // The land pair: state, land id, and the ref's move.
    let lands = pairs(&lines, "land");
    assert_eq!(lands.len(), 1, "one land pair");
    assert_eq!(lands[0].1["state"], "landed");
    assert_eq!(lands[0].1["ref"], "main");
    assert!(lands[0].1["land_id"].is_i64());
    let refs: Vec<_> = lines
        .iter()
        .filter(|l| l["event"] == "ref" && l["outcome"] == "ok")
        .collect();
    assert_eq!(refs.len(), 1, "one ref line: the move's record");
    assert!(refs[0]["new"].is_string(), "the move names its new target");
    assert!(
        refs[0]["old"].is_null() || refs[0]["old"].is_string(),
        "the move names what it moved from"
    );

    // No line ever names a blob or a tree: the upload's data digest is
    // not in the file.
    let text = std::fs::read(&audit_path).unwrap();
    let text = String::from_utf8(text).unwrap();
    let mut blob_hex = String::with_capacity(blob.as_bytes().len() * 2);
    for byte in blob.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(blob_hex, "{byte:02x}");
    }
    assert!(!text.contains(&blob_hex), "no blob digest in the audit");
}

/// A write whose audit intent was not made durable does not happen:
/// the upload is refused, and the store holds no new blob.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_whose_intent_fails_is_refused() {
    use scion_proto::v1::{PutBlobRequest, ResolveRefRequest};
    use tokio_stream::once;

    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    let audit_path = dir.path().join("audit.log");
    let audit = Audit::open(&audit_path).unwrap();
    audit.fail_next_appends(1).unwrap();
    let (stop, url) = serve_with_audit(&store, dir.path(), write_access(), audit).await;
    let mut ci = writer(&url, "t-fail").await;

    let status = ci
        .put_blob(once(PutBlobRequest {
            repo: REPO.into(),
            data: b"never lands\n".to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");

    // The blob is not in the store: resolve it? An upload names no
    // ref, so read the blob list the way the service does.
    // The store() fixture's repo holds exactly one blob (the README's).
    // The refused upload must not have added its content.
    let id = scion_core::BlobId::of(b"never lands\n");
    let missing = store.blobs().get(id).is_err();
    assert!(missing, "the refused write stored nothing");

    // The readiness loss latches: the health names flip and the server
    // drains, so a later call is refused too. The log has a hole; the
    // supervisor restarts a server that can prove its lines again.
    // The call rides the connection the drain is still finishing, so
    // poll with a deadline instead of racing the drain once.
    let mut drained = false;
    for _ in 0..100 {
        let later = ci
            .resolve_ref(ResolveRefRequest {
                repo: REPO.into(),
                name: "main".into(),
            })
            .await;
        if later.is_err() {
            drained = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(drained, "a server whose audit lost a line drains");

    let _ = stop.send(());
}

/// The intent and result of every `event` in `lines`, joined by their
/// request ID, the intent first.
fn pairs<'a>(
    lines: &'a [serde_json::Value],
    event: &str,
) -> Vec<(&'a serde_json::Value, &'a serde_json::Value)> {
    let mut found = vec![];
    for (i, line) in lines.iter().enumerate() {
        if line["event"] == event && line["outcome"] == "intent" {
            let (j, result) = lines
                .iter()
                .enumerate()
                .find(|(_, r)| {
                    r["event"] == event
                        && r["request_id"] == line["request_id"]
                        && r["outcome"] != "intent"
                })
                .expect("every intent has its result");
            assert!(i < j, "the intent comes before its result");
            found.push((line, result));
        }
    }
    found
}

/// Lands contend on one ref: eight lands at once, each retried until
/// it settles. The audit's ref lines, read in file order, must tell a
/// history that happened: the first move's `old` is the ref's head
/// before any land, and every later `old` is the `new` of the line
/// before it. A move recorded from outside the ref's lock reads a head
/// that a concurrent land has already replaced, and the chain breaks.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn concurrent_lands_ref_lines_tell_one_true_history() {
    use scion_core::{FileState, IdentityTime, RepoPath};
    use scion_proto::v1::LandRequest;
    use std::fmt::Write as _;

    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    let audit_path = dir.path().join("audit.log");
    let audit = Audit::open(&audit_path).unwrap();
    let start = store.resolve_ref(REPO, "main").unwrap();
    let (stop, url) = serve_with_audit(&store, dir.path(), write_access(), audit).await;

    // Eight changesets on the root, one per lander, inserted straight
    // into the store: the land is what this test drives.
    let lands: Vec<_> = (0..8)
        .map(|i| {
            let count = 2;
            let mut blobs = Vec::new();
            let changes: std::collections::BTreeMap<_, _> = (0..count)
                .map(|j| {
                    let blob = store
                        .blobs()
                        .put(format!("land {i} file {j}\n").as_bytes())
                        .unwrap();
                    blobs.push(blob);
                    (
                        RepoPath::new(format!("docs/{i}/{j}")).unwrap(),
                        Some(FileState::Regular {
                            content: blob,
                            executable: false,
                        }),
                    )
                })
                .collect();
            let change = scion_core::Changeset::new(
                vec![start],
                IdentityTime::new(
                    b"Ada".to_vec(),
                    b"ada@example.com".to_vec(),
                    1_700_000_000,
                    i,
                )
                .expect("identity fits the format"),
                None,
                format!("land {i}\n").into_bytes(),
                changes,
            )
            .expect("changeset fits the format");
            store
                .write(|c| {
                    let repo = c.repo_id(REPO)?;
                    let id = c.insert_changeset(repo, &change)?;
                    for blob in &blobs {
                        c.add_repo_blob(repo, *blob)?;
                    }
                    Ok(id)
                })
                .unwrap()
        })
        .collect();
    let mut start_hex = String::new();
    for byte in start.as_bytes() {
        write!(start_hex, "{byte:02x}").unwrap();
    }

    // Eight landers, one changeset each, retrying until the land
    // settles. A land whose ref moved under it answers Aborted and
    // stays pending; the same request key resumes it.
    let join = |i: usize| {
        let url = url.clone();
        let cs = lands[i];
        tokio::spawn(async move {
            let mut ci = writer(&url, &format!("t-land-{i}")).await;
            loop {
                match ci
                    .land(LandRequest {
                        repo: REPO.into(),
                        changeset_id: cs.as_bytes().to_vec(),
                        target_ref: "main".into(),
                        request_key: format!("k-{i}"),
                    })
                    .await
                {
                    Ok(_) => break,
                    Err(status)
                        if status.code() == tonic::Code::Aborted
                            || status.code() == tonic::Code::Unavailable => {}

                    Err(status) => panic!("land {i} failed: {status:?}"),
                }
            }
        })
    };
    // Hold the store's write connection, so no land's job can record
    // itself, let alone move the ref, until every call has read the
    // ref: the calls' first tasks need only the readers, and the
    // intents in the audit file say they are past them. Then let go:
    // the ref lock serializes the jobs' moves, and each move's
    // `previous` (or, on the racing code, the call's own early read)
    // must be the head the move actually replaced.
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let blocker = std::thread::spawn({
        let store = store.clone();
        move || {
            store
                .read_unflushed(|_| {
                    held_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                    Ok(())
                })
                .unwrap();
        }
    });
    held_rx.recv().unwrap();
    for i in 0..8 {
        join(i);
    }
    // The eight intents say every call is past its intent; their
    // first tasks read the ref within microseconds of it.
    wait_for_lines(&audit_path, 8).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    go_tx.send(()).unwrap();
    blocker.join().unwrap();
    for i in 0..8 {
        join(i).await.unwrap();
    }

    // A replay of a landed land: the same request key, after the land
    // settled. It reports the stored outcome and moved nothing, so its
    // land line names no old, and no ref line appears for it.
    let mut replay = writer(&url, "t-land-0").await;
    replay
        .land(LandRequest {
            repo: REPO.into(),
            changeset_id: lands[0].as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "k-0".into(),
        })
        .await
        .unwrap_or_else(|status| panic!("the replay failed: {status:?}"));

    stop.send(()).unwrap();
    // Eight intents, eight land results, eight ref lines, and the
    // replay's land line.
    wait_for_lines(&audit_path, 25).await;

    let lines = read_lines(&audit_path);
    let land_lines: Vec<&serde_json::Value> = lines
        .iter()
        .filter(|line| line["event"] == "land" && line["outcome"] == "ok")
        .collect();
    let replay_line = land_lines
        .iter()
        .rev()
        .find(|line| line["request_id"] == "t-land-0")
        .unwrap_or_else(|| panic!("the replay's land line is missing"));
    assert!(
        replay_line.get("old").is_none(),
        "a replay moved nothing, so its land line names no old: {replay_line}"
    );
    // The move's own land line names the head the move replaced: the
    // auditor finds it under the same land_id, and it agrees with the
    // move's ref line.
    let mover = land_lines
        .iter()
        .find(|line| line["land_id"] == replay_line["land_id"] && line.get("old").is_some())
        .unwrap_or_else(|| panic!("the move's land line names no old"));
    let mover_ref = refs_needed(&lines, "t-land-0");
    assert_eq!(
        mover["old"], mover_ref["old"],
        "the move's land line and its ref line name the same head"
    );

    let refs: Vec<&serde_json::Value> = lines
        .iter()
        .filter(|line| line["event"] == "ref" && line["outcome"] == "ok")
        .collect();
    assert_eq!(
        refs.len(),
        8,
        "only the calls that moved the ref have ref lines: {}",
        refs.len()
    );
    let chain: Vec<(String, String)> = refs
        .iter()
        .map(|line| {
            (
                line["old"].as_str().unwrap_or_default().to_string(),
                line["new"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    assert_eq!(
        chain.first().map(|(old, _)| old.as_str()),
        Some(start_hex.as_str()),
        "the first move moved the head the lands started from"
    );
    for pair in chain.windows(2) {
        assert_eq!(
            pair[0].1, pair[1].0,
            "each move moved the ref its predecessor left it at"
        );
    }
}

/// The ref line of the call whose request id is `id`.
fn refs_needed<'a>(lines: &'a [serde_json::Value], id: &str) -> &'a serde_json::Value {
    lines
        .iter()
        .find(|line| line["event"] == "ref" && line["request_id"] == id)
        .unwrap_or_else(|| panic!("no ref line for {id}"))
}

/// Waits until the file holds `n` lines: the stop's flush runs on its
/// own task, so the lines land a moment after the stop.
async fn wait_for_lines(path: &std::path::Path, n: usize) {
    for _ in 0..100 {
        if std::fs::read(path)
            .is_ok_and(|bytes| String::from_utf8_lossy(&bytes).lines().count() >= n)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let bytes = std::fs::read(path).unwrap_or_default();
    panic!(
        "the audit file never reached {n} lines: {:?}",
        String::from_utf8_lossy(&bytes)
    );
}

/// The evidence does not depend on the call living: a call cut after
/// the land's commit — a client that left, a deadline, a shutdown —
/// still leaves the land's result line and the ref line in the file,
/// because the store job that moved the ref writes them itself. The
/// cut call's reply is gone; the audit's answer to its intent is not.
#[tokio::test(flavor = "multi_thread")]
async fn a_land_whose_call_is_cut_still_writes_its_result_and_ref_lines() {
    use scion_proto::v1::{CreateChangesetRequest, LandRequest, PutBlobRequest};

    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    let audit_path = dir.path().join("audit.log");
    let audit = Audit::open(&audit_path).unwrap();
    let (stop, url) = serve_with_audit(&store, dir.path(), write_access(), audit).await;
    let lines_before = read_lines(&audit_path).len();

    // The blob and the changeset the land moves to main.
    let mut ci = writer(&url, "t-setup").await;
    let blob = store.blobs().put(b"guide\n").unwrap();
    ci.put_blob(tokio_stream::once(PutBlobRequest {
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
    let intent_lines = read_lines(&audit_path).len();

    // Hold the store's write connection, so the land's job cannot move
    // the ref yet: the call hangs on its commit.
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let blocker = std::thread::spawn({
        let store = store.clone();
        move || {
            store
                .read_unflushed(|_| {
                    held_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                    Ok(())
                })
                .unwrap();
        }
    });
    held_rx.recv().unwrap();

    // The land call runs on its own; its intent is in the file, so the
    // call is inside its job. Then the call is cut: the client's task
    // is aborted, and the handler's future is dropped mid-await. The
    // job keeps running — that is the case the old code lost.
    let mut cut = writer(&url, "t-cut").await;
    let call = tokio::task::spawn(async move {
        let req = LandRequest {
            repo: REPO.into(),
            changeset_id: id.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "k-cut".into(),
        };
        let _ = cut.land(req).await;
    });
    wait_for_lines(&audit_path, intent_lines + 1).await;
    call.abort();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Let the land commit. The job, not the dead call, now owes the
    // audit its result and its ref line.
    go_tx.send(()).unwrap();
    blocker.join().unwrap();
    // The intent, the land's result and the ref line — waited for with
    // a deadline, not raced once.
    for _ in 0..100 {
        let lines = read_lines(&audit_path);
        let answered = lines.iter().any(|line| {
            line["event"] == "land" && line["outcome"] == "ok" && line["request_id"] == "t-cut"
        }) && lines
            .iter()
            .any(|line| line["event"] == "ref" && line["request_id"] == "t-cut");
        if answered {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // The land's result line is the promise under test: the job, not
    // the call, writes it. What the line says depends on a race the
    // design owns: the cut call's deadline guard ends the deadline, and
    // the land's plan checks it between steps (land.rs), so on a slower
    // backend the land may stop as cancelled — still pending, still
    // resumable — instead of landing. Either way the job wrote the
    // evidence.
    let lines = read_lines(&audit_path);
    assert!(
        lines.len() > lines_before,
        "the cut call left no lines at all"
    );
    let land_line = lines
        .iter()
        .find(|line| {
            line["event"] == "land"
                && line["request_id"] == "t-cut"
                && (line["outcome"] == "ok" || line["outcome"] == "failed")
        })
        .unwrap_or_else(|| {
            panic!(
                "the cut call's land result never reached the file; lines: {}",
                serde_json::to_string(&lines).unwrap()
            )
        });
    if land_line["outcome"] == "ok" {
        assert_landed_lines(&store, &lines, land_line, root);
    } else {
        assert_cancelled_lines(&store, land_line, root);
    }

    stop.send(()).unwrap();
}

/// A landed result names the head it moved to, and its ref line names
/// the same new head and the old one it replaced.
fn assert_landed_lines(
    store: &Backend,
    lines: &[serde_json::Value],
    land_line: &serde_json::Value,
    root: ChangesetId,
) {
    assert_eq!(
        land_line["new"],
        serde_json::Value::String(store.resolve_ref(REPO, "main").unwrap().to_string()),
        "the land's result names the head it moved to"
    );
    let ref_line = refs_needed(lines, "t-cut");
    assert_eq!(
        ref_line["old"],
        serde_json::Value::String(root.to_string()),
        "the move names the head it replaced"
    );
    assert_eq!(
        ref_line["new"], land_line["new"],
        "the ref line and the land line name the same new head"
    );
}

/// A cancelled result names its cancellation, and the land moved no
/// ref: it stays pending for a retry with the same request key.
fn assert_cancelled_lines(store: &Backend, land_line: &serde_json::Value, root: ChangesetId) {
    assert_eq!(
        land_line["why"], "cancelled",
        "a cut call's land fails only by its own cancellation"
    );
    assert_eq!(
        store.resolve_ref(REPO, "main").unwrap(),
        root,
        "a cancelled land moved no ref; it stays pending for a retry"
    );
}

/// A call that sends no request ID — or one the server refuses to log
/// — still leaves lines that join: the server makes the ID, and the
/// intent and the result of one write carry the same one.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_without_a_request_id_gets_one_from_the_server() {
    use scion_proto::v1::PutBlobRequest;

    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    let audit_path = dir.path().join("audit.log");
    let audit = Audit::open(&audit_path).unwrap();
    let (stop, url) = serve_with_audit(&store, dir.path(), write_access(), audit).await;

    // The empty header is dropped by the server's own rule (a value is
    // 1 to 64 bytes), so this call arrives without one.
    let mut ci = writer(&url, "").await;
    ci.put_blob(tokio_stream::once(PutBlobRequest {
        repo: REPO.into(),
        data: b"guide\n".to_vec(),
    }))
    .await
    .expect("the upload is allowed");
    let mut ci = writer(&url, "").await;
    ci.put_blob(tokio_stream::once(PutBlobRequest {
        repo: REPO.into(),
        data: b"second\n".to_vec(),
    }))
    .await
    .expect("the second upload is allowed");

    stop.send(()).unwrap();
    // Two writes, each with its intent and its result.
    wait_for_lines(&audit_path, 4).await;

    let lines = read_lines(&audit_path);
    let pairs = pairs(&lines, "write");
    assert_eq!(pairs.len(), 2, "each write left an intent and a result");
    for (intent, result) in &pairs {
        let id = intent["request_id"].as_str().unwrap_or("");
        assert!(
            !id.is_empty(),
            "a call with no header still gets a request_id: {intent}"
        );
        assert_eq!(
            result["request_id"].as_str().unwrap_or(""),
            id,
            "the pair joins by the server-made ID"
        );
    }
    let first = pairs[0].0["request_id"].as_str().unwrap();
    let second = pairs[1].0["request_id"].as_str().unwrap();
    assert_ne!(first, second, "two calls made in one process differ");
}
