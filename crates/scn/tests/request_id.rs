//! Request-ID suite: one checkout carries one request ID through the
//! daemon and the cache to the server, and a tier forwards the ID its
//! caller sent rather than making its own.
#![cfg(unix)]

use std::path::Path;
use std::process::Command;
use std::sync::LazyLock;

use scion_core::FileState;
use scion_proto::v1::GetObjectsRequest;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_server::Backend;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use tracing_subscriber::fmt::MakeWriter;

mod common;
use common::{REPO, TOKEN, Tiers, changeset, connect, land, tree_ref};

/// The log of the test binary, into this one buffer: the spans of the
/// server, the cache and the daemon, which all run in this process.
static LOG: LazyLock<Captured> = LazyLock::new(Captured::default);

/// The tests share one log, so they run one at a time.
static SERIAL: LazyLock<std::sync::Mutex<()>> = LazyLock::new(|| std::sync::Mutex::new(()));

fn init_log() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::level_filters::LevelFilter::TRACE)
            .with_writer(Captured::clone(&*LOG))
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
    });
}

fn log_text() -> String {
    String::from_utf8(LOG.0.lock().unwrap().clone()).unwrap()
}

/// A log writer that collects what the test binary logs.
#[derive(Clone, Default)]
struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Captured {
    type Writer = Captured;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// `scn checkout REV --into dest` through the daemon.
fn checkout(tiers: &Tiers, rev: &str, dest: &Path) {
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args([
            "--server",
            &tiers.server,
            "--objects",
            &tiers.socket,
            "--repo",
            REPO,
            "checkout",
            rev,
            "--into",
        ])
        .arg(dest)
        .env("SCION_TOKEN", TOKEN)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn a_checkout_shows_one_id_at_every_hop() {
    let _serial = SERIAL.lock().unwrap();
    init_log();
    // The two tests share one log, so this test reads only the lines that
    // it logged itself.
    let mark = log_text().len();
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(&data.path().join("scion")).unwrap();
    let content = store.blobs().put(b"the demo file\n").unwrap();
    let files = vec![(
        "f.txt",
        FileState::Regular {
            content,
            executable: false,
        },
    )];
    land(&store, &changeset(None, &files));
    let tiers = Tiers::start(tempfile::tempdir().unwrap(), store);

    let dest = data.path().join("out");
    checkout(&tiers, "main", &dest);
    let text = &log_text()[mark..];

    // Every hop of the one checkout — the daemon's and the cache's
    // GetObjects calls, and the server's ResolveRef — names the one
    // request ID that the scn command made and sent, a 16-digit hex ID.
    let ids = span_ids(text);
    assert!(!ids.is_empty(), "no call spans in the log:\n{text}");
    assert_eq!(ids.len(), 1, "one ID across every hop: {ids:?}");
    let id = *ids.iter().next().unwrap();
    assert!(is_generated(id), "scn made the ID: {id}");
}

/// A call with a request ID the test picked rides it upstream: the daemon's
/// fetch logs the caller's ID, not a made one.
#[test]
fn a_tier_forwards_the_id_its_caller_sent() {
    let _serial = SERIAL.lock().unwrap();
    init_log();
    // The two phases share one log, so this phase reads only the lines
    // that it logged itself.
    let mark = log_text().len();
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(&data.path().join("scion")).unwrap();
    let content = store.blobs().put(b"the demo file\n").unwrap();
    let files = vec![(
        "f.txt",
        FileState::Regular {
            content,
            executable: false,
        },
    )];
    let changeset = land(&store, &changeset(None, &files));
    let tiers = Tiers::start(tempfile::tempdir().unwrap(), store);

    // The ticket comes from the server; the read goes to the daemon, which
    // has no copy, so it fetches from the cache.
    let (ticket, root) = tiers.runtime.block_on(async {
        let mut server = connect(&tiers.server).await;
        let reply = server
            .get_root_tree(scion_proto::v1::GetRootTreeRequest {
                repo: REPO.into(),
                changeset_id: changeset.as_bytes().to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        (reply.ticket, reply.tree_id)
    });
    let request = GetObjectsRequest {
        refs: vec![tree_ref(
            scion_core::TreeId::from_bytes(root.try_into().unwrap()),
            b"",
        )],
        accept: Vec::new(),
        ticket,
    };
    tiers.runtime.block_on(async {
        let mut daemon = client(&tiers.socket, "chosen-id-42").await;
        daemon.get_objects(request).await.unwrap();
    });

    let text = &log_text()[mark..];
    // The daemon's, the cache's and the server's GetObjects spans all name
    // the caller's ID: it rode every hop, and no tier made its own.
    let ids = span_ids(text);
    assert!(!ids.is_empty(), "no call spans in the log:\n{text}");
    assert_eq!(
        ids,
        ["chosen-id-42"].into_iter().collect(),
        "the caller's ID rode every hop: {ids:?}"
    );
}

/// The request IDs that the `call` spans of `GetObjects` calls logged.
fn span_ids(text: &str) -> std::collections::BTreeSet<&str> {
    text.lines()
        .filter(|line| line.contains("GetObjects") && line.contains("request_id="))
        .filter_map(|line| {
            // The span may record more fields after this one: principal,
            // repo. The ID holds no space, and the span context closes with
            // `}: `, so the ID ends at the first space or brace. An empty
            // ID is kept: a hop that logs none is a difference too.
            let id = line.split("request_id=").nth(1)?;
            let id = id.split([' ', '}']).next()?;
            let id = id.strip_suffix(':').unwrap_or(id);
            Some(id)
        })
        .collect()
}

/// A client that sends the token and a fixed request ID on every call.
#[derive(Clone)]
struct Id(&'static str);

impl Interceptor for Id {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        request
            .metadata_mut()
            .insert("x-request-id", self.0.parse().unwrap());
        Ok(request)
    }
}

async fn client(url: &str, id: &'static str) -> ObjectsClient<InterceptedService2> {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    ObjectsClient::with_interceptor(channel, Id(id))
}

type InterceptedService2 = tonic::service::interceptor::InterceptedService<Channel, Id>;

/// The 16-digit hex form that this process makes for a call with none.
fn is_generated(id: &str) -> bool {
    id.len() == 16
        && id
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
