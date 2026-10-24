//! `Log` and `Blame` stream their replies in parts, as `ListDir` and
//! `GetChangeset` stream theirs: the parts take the one counted path, so a
//! client that stops reading is counted and cut off. The unary replies
//! that are left are bounded by the message limit: a reply over it is
//! refused by the server's encoder, and a test pins that wiring, so nobody
//! removes it by accident.

use std::num::NonZeroU64;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

#[path = "common/log_fields.rs"]
mod log_fields;

use tracing_subscriber::fmt::MakeWriter;

use scion_core::{Changeset, FileState, RepoPath};
use scion_proto::MAX_MESSAGE_BYTES;
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::BlameRequest;
use scion_proto::v1::LogRequest;
use scion_proto::v1::scion_client::ScionClient;
use scion_server::Metrics;
use scion_server::test_support::backend::from_store;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::tree;
use scion_server::{
    Backend,
    test_support::{service, store::Store},
};
use tonic::codec::Streaming;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

const REPO: &str = "main";
const TOKEN: &str = "secret";

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

/// A repo whose chain of `count` changesets each change one file, with a
/// `name_len`-byte author name, so a `Log` or `Blame` reply of it grows by
/// about `name_len` per entry.
fn chain(
    count: usize,
    name_len: usize,
) -> (tempfile::TempDir, Backend, Vec<scion_core::ChangesetId>) {
    let dir = tempfile::tempdir().unwrap();
    let store = from_store(Store::open(dir.path()).unwrap());
    let ids = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut ids = Vec::new();
            for i in 0..count {
                let mut content = Vec::new();
                for line in 0..=i {
                    content.extend_from_slice(format!("line {line}\n").as_bytes());
                }
                let regular = FileState::Regular {
                    content: store.blobs().put(&content).unwrap(),
                    executable: false,
                };
                let cs = Changeset::new(
                    ids.last().copied().into_iter().collect(),
                    scion_core::IdentityTime::new(
                        vec![b'a'; name_len],
                        b"t@x".to_vec(),
                        1_700_000_000,
                        0,
                    )
                    .expect("identity fits the format"),
                    None,
                    format!("change {i}\n").into_bytes(),
                    [(RepoPath::new(b"d/f.txt").unwrap(), Some(regular))].into(),
                )
                .expect("changeset fits the format");
                ids.push(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0);
            }
            Ok(ids)
        })
        .unwrap();
    (dir, store, ids)
}

/// Serves `store` with `limits`.
async fn serve(store: Backend, dir: &std::path::Path, limits: Limits) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(service::serve(
        store,
        listener,
        TOKEN,
        service::ServeOptions {
            data: dir.to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits,
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::new(Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
        std::future::pending(),
    ));
    format!("http://{addr}")
}

async fn client(url: &str) -> Scion {
    // The smallest windows, as the stall tests of `listen.rs` build their
    // clients: with the defaults, the client's connection buffers take a
    // whole reply of a few parts, and nothing stalls.
    let channel = Channel::from_shared(url.to_owned())
        .unwrap()
        .initial_stream_window_size(65_535)
        .initial_connection_window_size(65_535)
        .connect_timeout(common::CONNECT_TIMEOUT)
        .connect()
        .await
        .unwrap();
    ScionClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap())
}

/// A `Log` call whose reply the caller never reads.
async fn stalled_log(client: &mut Scion, from: &[u8]) -> Streaming<scion_proto::v1::LogResponse> {
    client
        .log(LogRequest {
            repo: REPO.to_string(),
            changeset_id: from.to_vec(),
            limit: 1_000,
            path: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner()
}

/// A client that stops reading: the parts of the reply pile up in HTTP/2,
/// The log of the test binary, into this one buffer: the close notice of
/// a stalled connection names its peer there.
static LOG: LazyLock<Captured> = LazyLock::new(Captured::default);

/// The tests share one log, so they run one at a time.
static SERIAL: LazyLock<std::sync::Mutex<()>> = LazyLock::new(|| std::sync::Mutex::new(()));

fn init_log() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::level_filters::LevelFilter::WARN)
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

/// the reply's producer stops at its stall, and the stalled bytes go on the
/// connection's count. Past `--max-stalled-bytes` the connection closes, so
/// the stalled stream ends with the transport error. The clock is paused
/// and moved past the stall, so the test does not wait the real 120 s out.
/// The reply stays unread until the end: reading it grants the client's
/// HTTP/2 windows back, and the reply flows instead of stalling.
#[tokio::test(start_paused = true)]
async fn a_stalled_log_reply_is_counted_and_closes_the_connection() {
    init_log();
    {
        let _serial = SERIAL.lock().unwrap();
        LOG.0.lock().unwrap().clear();
    }
    // Twenty changesets with 300 KB messages each: the tip's page is about
    // six parts, and the client's connection takes under two, so a client
    // that never reads stalls the producer inside the reply.
    let (dir, store, ids) = chain(20, 300_000);
    // The cap of the test connection: one stalled reply of 4 MiB is over it.
    let limits = Limits {
        stalled_bytes: NonZeroU64::new(1 << 20).unwrap(),
        ..Limits::default()
    };
    let url = serve(store, dir.path(), limits).await;
    let mut client = client(&url).await;

    let tip = ids[ids.len() - 1];
    let mut reply = stalled_log(&mut client, tip.as_bytes()).await;

    // The clock moves only when every task waits, and the reply's parts
    // travel over real sockets, so the stall is waited out in steps while
    // the tasks run. The reply stays unread: its client reads nothing.
    for _ in 0..140 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    // The count is over the cap: the connection's read side failed, and
    // hyper closed the connection, so the stalled stream ends with the
    // transport error. A clean end would mean the reply flowed, and
    // nothing was counted.
    let end = reply.message().await;
    assert!(end.is_err(), "the stalled reply was not cut off: {end:?}");

    // The close notice names the connection's peer, so the runbook's
    // `find the client` step has an address to grep for.
    let text = log_text();
    let notice = log_fields::find_line(
        &text,
        "closing a connection: its stalled replies hold too much",
    )
    .unwrap_or_else(|| panic!("the close notice is not in the log: {text}"));
    let peer = notice
        .field("peer")
        .unwrap_or_else(|| panic!("the close notice names no peer: {notice:?}"));
    // The event logs the peer as Debug: `Some(127.0.0.1:54321)`. The
    // runbook's `find the client` step needs a real address, not a
    // placeholder: a loopback host and a port the client actually
    // held.
    let peer = peer
        .strip_prefix("Some(")
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(peer)
        .parse::<std::net::SocketAddr>()
        .unwrap_or_else(|_| panic!("the close notice names a parseable peer: {peer}"));
    assert!(
        peer.ip().is_loopback() && peer.port() != 0,
        "the close notice names the client's address: {peer}"
    );
}

/// A service whose one unary reply is one byte over the largest message a
/// client accepts, so the server's encoder refuses it.
struct Huge;

fn dead<T>() -> tokio_stream::wrappers::ReceiverStream<T> {
    tokio_stream::wrappers::ReceiverStream::new(tokio::sync::mpsc::channel(1).1)
}

#[tonic::async_trait]
impl scion_proto::v1::scion_server::Scion for Huge {
    async fn put_blob(
        &self,
        _: Request<tonic::Streaming<scion_proto::v1::PutBlobRequest>>,
    ) -> Result<Response<scion_proto::v1::PutBlobResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }

    async fn create_changeset(
        &self,
        _: Request<scion_proto::v1::CreateChangesetRequest>,
    ) -> Result<Response<scion_proto::v1::CreateChangesetResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }

    async fn land(
        &self,
        _: Request<scion_proto::v1::LandRequest>,
    ) -> Result<Response<scion_proto::v1::LandResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }

    async fn get_land(
        &self,
        _: Request<scion_proto::v1::GetLandRequest>,
    ) -> Result<Response<scion_proto::v1::LandResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }

    async fn resolve_ref(
        &self,
        _: Request<scion_proto::v1::ResolveRefRequest>,
    ) -> Result<Response<scion_proto::v1::ResolveRefResponse>, Status> {
        Ok(Response::new(scion_proto::v1::ResolveRefResponse {
            changeset_id: vec![0; MAX_MESSAGE_BYTES + 1],
        }))
    }

    type GetChangesetStream = tokio_stream::wrappers::ReceiverStream<
        Result<scion_proto::v1::GetChangesetResponse, Status>,
    >;

    async fn get_changeset(
        &self,
        _: Request<scion_proto::v1::GetChangesetRequest>,
    ) -> Result<Response<Self::GetChangesetStream>, Status> {
        Ok(Response::new(dead()))
    }

    type ReadFileStream =
        tokio_stream::wrappers::ReceiverStream<Result<scion_proto::v1::ReadFileResponse, Status>>;

    async fn read_file(
        &self,
        _: Request<scion_proto::v1::ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        Ok(Response::new(dead()))
    }

    type ListDirStream =
        tokio_stream::wrappers::ReceiverStream<Result<scion_proto::v1::ListDirResponse, Status>>;

    async fn list_dir(
        &self,
        _: Request<scion_proto::v1::ListDirRequest>,
    ) -> Result<Response<Self::ListDirStream>, Status> {
        Ok(Response::new(dead()))
    }

    type LogStream =
        tokio_stream::wrappers::ReceiverStream<Result<scion_proto::v1::LogResponse, Status>>;

    async fn log(
        &self,
        _: Request<scion_proto::v1::LogRequest>,
    ) -> Result<Response<Self::LogStream>, Status> {
        Ok(Response::new(dead()))
    }

    type BlameStream =
        tokio_stream::wrappers::ReceiverStream<Result<scion_proto::v1::BlameResponse, Status>>;

    async fn blame(
        &self,
        _: Request<scion_proto::v1::BlameRequest>,
    ) -> Result<Response<Self::BlameStream>, Status> {
        Ok(Response::new(dead()))
    }

    async fn merge_base(
        &self,
        _: Request<scion_proto::v1::MergeBaseRequest>,
    ) -> Result<Response<scion_proto::v1::MergeBaseResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }

    async fn is_ancestor(
        &self,
        _: Request<scion_proto::v1::IsAncestorRequest>,
    ) -> Result<Response<scion_proto::v1::IsAncestorResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }

    async fn diff(
        &self,
        _: Request<scion_proto::v1::DiffRequest>,
    ) -> Result<Response<scion_proto::v1::DiffResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }
}

/// A unary reply over the message limit is refused by the server's encoder,
/// with the code `OUT_OF_RANGE`, so the ceiling of the memory a client that
/// never reads holds for one holds by construction. The test serves the
/// stub through the same wiring the real server uses, so removing the
/// limits there fails here.
#[tokio::test]
async fn a_unary_reply_over_the_message_limit_is_refused() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(scion_proto::runtime::listen::Limits::default().serve(
        |mut server| server.add_service(service::into_server(Huge)),
        scion_proto::runtime::listen::tcp(listener),
        None,
        std::future::pending(),
        std::sync::Arc::new(scion_proto::runtime::metrics::Metrics::default()),
    ));
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    let mut client = scion_proto::v1::scion_client::ScionClient::new(channel);
    let err = client
        .resolve_ref(scion_proto::v1::ResolveRefRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::OutOfRange, "{err}");
    assert!(err.to_string().contains("encoded message length"), "{err}");
}

/// A client that stops reading a `Blame` reply: the reply's producer stops
/// at its stall, and the stalled bytes go on the connection's count. Past
/// `--max-stalled-bytes` the connection closes, so the stalled stream ends
/// with the transport error. The clock is paused and moved past the stall,
/// and the reply stays unread: reading it would grant the client's HTTP/2
/// windows back, and the reply would flow instead of stalling.
#[tokio::test(start_paused = true)]
async fn a_stalled_blame_reply_is_counted_and_closes_the_connection() {
    // The log is this test's own: under nextest every test runs in its
    // own process, so the sibling's init does not carry here, and the
    // close notice would be asserted against an empty log. The suites
    // take the log one test at a time, and stale lines go.
    init_log();
    {
        let _serial = SERIAL.lock().unwrap();
        LOG.0.lock().unwrap().clear();
    }
    // Twenty changesets with 300 KB messages each: a blame of the file
    // names each author, so its reply is about six parts, and the client's
    // connection takes under two.
    let (dir, store, ids) = chain(20, 300_000);
    // The cap of the test connection: one stalled reply of 4 MiB is over it.
    let limits = Limits {
        stalled_bytes: NonZeroU64::new(1 << 20).unwrap(),
        ..Limits::default()
    };
    let url = serve(store, dir.path(), limits).await;
    let mut client = client(&url).await;

    let tip = ids[ids.len() - 1];
    let mut reply = client
        .blame(BlameRequest {
            repo: REPO.to_string(),
            changeset_id: tip.as_bytes().to_vec(),
            path: b"d/f.txt".to_vec(),
            cursor: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();

    // The stall is waited out in steps while the tasks run.
    for _ in 0..140 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    // The count is over the cap: the connection's read side failed, and
    // hyper closed the connection, so the stalled stream ends with the
    // transport error. A clean end would mean the reply flowed, and
    // nothing was counted.
    let end = reply.message().await;
    assert!(end.is_err(), "the stalled reply was not cut off: {end:?}");

    // The close notice names the connection's peer, so the runbook's
    // `find the client` step has an address to grep for.
    let text = log_text();
    let notice = log_fields::find_line(
        &text,
        "closing a connection: its stalled replies hold too much",
    )
    .unwrap_or_else(|| panic!("the close notice is not in the log: {text}"));
    let peer = notice
        .field("peer")
        .unwrap_or_else(|| panic!("the close notice names no peer: {notice:?}"));
    // The event logs the peer as Debug: `Some(127.0.0.1:54321)`. The
    // runbook's `find the client` step needs a real address, not a
    // placeholder: a loopback host and a port the client actually
    // held.
    let peer = peer
        .strip_prefix("Some(")
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(peer)
        .parse::<std::net::SocketAddr>()
        .unwrap_or_else(|_| panic!("the close notice names a parseable peer: {peer}"));
    assert!(
        peer.ip().is_loopback() && peer.port() != 0,
        "the close notice names the client's address: {peer}"
    );
}

#[path = "common/mod.rs"]
mod common;
