//! Connection to the server, plus helpers every command shares.

use std::borrow::Cow;
use std::fs;
use std::io::Read as _;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath};
use scion_proto::runtime::auth::{Bearer, check_ancestors};
use scion_proto::runtime::request_id;
use scion_proto::runtime::tls::ClientTls;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, PutBlobRequest, ResolveRefRequest};
use scion_proto::{CHUNK_BYTES, MAX_MESSAGE_BYTES, MAX_PART_MESSAGE_BYTES};
use tokio_stream::{StreamExt as _, wrappers::ReceiverStream};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

/// The deadline of every call: the server's own limit for a call, so it
/// fits the longest call that `scn` makes (a land, a graph query, an upload
/// of 100 MiB). It goes out as `grpc-timeout`, so each tier on the way waits
/// no longer than what is left of it, and `scn` gives up on its own then too.
const CALL_TIMEOUT: Duration = Duration::from_secs(600);

/// How long `scn` waits to connect.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A client's connection notices a dead peer: the HTTP/2 pings and the
/// TCP probes run while the connection is idle too, so a server that
/// died without a FIN or RST (a frozen process, a host gone) ends every
/// call in about [`scion_proto::runtime::listen::CLIENT_KEEPALIVE_INTERVAL`]
/// plus its timeout, not at this client's 600 s deadline.
fn live(endpoint: Endpoint) -> Endpoint {
    use scion_proto::runtime::listen as keepalive;
    let (interval, timeout) = keepalive::client_keepalive();
    endpoint
        .http2_keep_alive_interval(interval)
        .keep_alive_timeout(timeout)
        // An idle channel must notice a dead peer too: the next call
        // answers at once, not after the call's own deadline.
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(keepalive::client_tcp_keepalive()))
}

/// Most bytes that the parts of one streamed reply may join to. The server
/// bounds one part at 1 MiB, but not the whole reply: a directory or a
/// changeset may be as big as its repo made it. The client joins the parts
/// in memory before it decodes, so a broken or hostile server could stream
/// until `scn` runs out of memory. One GiB is far over what this client
/// makes in one object (its largest changeset comes from an import of a
/// machine's history, well under 100 MiB), so hitting it is a server fault,
/// not a repo that grew.
pub(crate) const MAX_JOINED_REPLY_BYTES: usize = 1 << 30;

/// A connected client.
pub type Client = ScionClient<InterceptedService<Channel, Call>>;

/// A connected client for the object reads that a cache tier can answer.
pub type Objects = ObjectsClient<InterceptedService<Channel, Call>>;

/// Adds the bearer token, the deadline (`CALL_TIMEOUT`) and the request
/// ID to every call. One command makes one ID ([`Ctx::connect`]) and sends
/// it on every call, to the server and to the tier, so one operation can
/// be followed across the logs of every hop.
#[derive(Clone)]
pub struct Call {
    bearer: Bearer,
    request_id: MetadataValue<Ascii>,
}

impl Call {
    /// One command's interceptor: its token and its request ID.
    pub fn new(bearer: Bearer, request_id: MetadataValue<Ascii>) -> Self {
        Self { bearer, request_id }
    }
}

impl Interceptor for Call {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let mut request = self.bearer.call(request)?;
        request.set_timeout(CALL_TIMEOUT);
        request
            .metadata_mut()
            .insert(request_id::REQUEST_ID, self.request_id.clone());
        Ok(request)
    }
}

/// Maps a read failure of a streaming put to the refusal that names the
/// file: it changed while it was read, so the read saw a torn mix. Other
/// errors pass as themselves.
fn changed(file: &Path, e: std::io::Error) -> std::io::Error {
    if e.kind() == std::io::ErrorKind::UnexpectedEof || e.kind() == std::io::ErrorKind::InvalidData
    {
        std::io::Error::other(format!(
            "{} changed while it was read; commit again",
            file.display()
        ))
    } else {
        e
    }
}

/// The pause between the file's metadata read and its first chunk, for
/// the test that truncates the file there. One-shot: the first put
/// takes it, the rest pass. Test-hooks only.
#[cfg(any(test, feature = "test-hooks"))]
static PUT_PAUSE: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>> =
    std::sync::Mutex::new(None);

/// Installs the pause the next streaming put takes between its metadata
/// read and its first chunk read, or takes the installed one back with
/// `None`.
#[cfg(any(test, feature = "test-hooks"))]
pub fn set_put_pause(pause: Option<Box<dyn Fn() + Send + Sync>>) {
    *PUT_PAUSE.lock().unwrap() = pause;
}

/// Runs the installed pause, if any.
#[cfg(any(test, feature = "test-hooks"))]
fn take_put_pause() {
    if let Some(pause) = PUT_PAUSE.lock().unwrap().take() {
        pause();
    }
}

/// The pause never exists outside the tests.
#[cfg(not(any(test, feature = "test-hooks")))]
fn take_put_pause() {}

/// Whether the streaming put's whole-file mutation lever is flipped
/// (`SCION_TEST_COMMIT_MUTATION` = `whole_file`). Test-hooks only: the
/// ceiling guard sets it to prove the guard is not vacuous.
#[cfg(any(test, feature = "test-hooks"))]
fn mutation_whole_file() -> bool {
    static ENV: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    static ON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    *ENV.get_or_init(|| {
        std::env::var_os("SCION_TEST_COMMIT_MUTATION").as_deref()
            == Some(std::ffi::OsStr::new("whole_file"))
    }) || ON.load(std::sync::atomic::Ordering::SeqCst)
}

/// The `Objects` client of `url`, for the tests, with a 10s timeout.
///
/// # Panics
///
/// Panics when `url` is not a valid address.
#[cfg(any(test, feature = "test-hooks"))]
pub async fn test_objects(url: &str, token: &str) -> Objects {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let call = Call::new(
        Bearer::new(token).unwrap(),
        request_id::generate()
            .parse()
            .expect("the request ID is ASCII"),
    );
    ObjectsClient::with_interceptor(channel, call)
}

/// What every command needs.
#[derive(Clone)]
pub struct Ctx {
    /// The gRPC client.
    pub client: Client,
    /// The `Objects` client for reads of trees and blobs: `--objects`, or
    /// the server when no tier is given. Its `GetObjects` needs a ticket.
    pub objects: Objects,
    /// The `Objects` client of the server: `ResolveRef` and `GetRootTree`
    /// go there, with a token, never to a tier.
    pub roots: Objects,
    /// Repository name.
    pub repo: String,
    /// Print JSON instead of text.
    pub json: bool,
}

impl Ctx {
    /// Connects to `server`: `http://HOST:PORT`, or `unix:///PATH` for a
    /// daemon on this node.
    ///
    /// # Errors
    ///
    /// Errors when the address is bad, the server does not answer, or the
    /// token is refused.
    pub async fn connect(
        server: &str,
        objects: Option<&str>,
        token: &str,
        ca: Option<&Path>,
        repo: String,
        json: bool,
    ) -> Result<Self> {
        let mut address = server.to_owned();
        if let Some(socket) = server
            .strip_prefix("unix://")
            .or_else(|| server.strip_prefix("unix:"))
        {
            let socket = check_socket(Path::new(socket), rustix::process::geteuid().as_raw())?;
            // The checked path, so no symlink in `server` is followed again.
            let socket = socket.to_str().context("the socket path is not UTF-8")?;
            address = format!("unix://{socket}");
        }
        // `Endpoint` reads `unix:` addresses too, unlike `Channel::from_shared`.
        let endpoint = Endpoint::from_shared(address).context("bad server address")?;
        let endpoint = ClientTls::load(ca, None, None)
            .and_then(|tls| tls.apply_to(server, endpoint))
            .with_context(|| format!("cannot trust the certificate of {server}"))?;
        // A server at its connection cap leaves a new connection unread, so
        // a call also has a limit here, until its reply starts.
        let channel = live(endpoint)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(CALL_TIMEOUT)
            .connect()
            .await
            .with_context(|| format!("cannot connect to {server}"))?;
        let call = Call::new(
            Bearer::new(token).context("token is not valid ASCII")?,
            request_id::generate()
                .parse()
                .context("the request ID is not ASCII")?,
        );
        let roots = ObjectsClient::with_interceptor(channel.clone(), call.clone())
            .max_decoding_message_size(MAX_PART_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        let objects = match objects {
            // The reads of trees and blobs go to the tier, with the ticket
            // that the server minted; the token of the tier is this
            // client's token, which the deployment shares with its daemon.
            Some(objects) => {
                let mut address = objects.to_owned();
                if let Some(socket) = objects
                    .strip_prefix("unix://")
                    .or_else(|| objects.strip_prefix("unix:"))
                {
                    let socket =
                        check_socket(Path::new(socket), rustix::process::geteuid().as_raw())?;
                    let socket = socket.to_str().context("the socket path is not UTF-8")?;
                    address = format!("unix://{socket}");
                }
                let endpoint = Endpoint::from_shared(address).context("bad --objects address")?;
                let endpoint = ClientTls::load(ca, None, None)
                    .and_then(|tls| tls.apply_to(objects, endpoint))
                    .with_context(|| format!("cannot trust the certificate of {objects}"))?;
                let channel = live(endpoint)
                    .connect_timeout(CONNECT_TIMEOUT)
                    .timeout(CALL_TIMEOUT)
                    .connect()
                    .await
                    .with_context(|| format!("cannot connect to {objects}"))?;
                ObjectsClient::with_interceptor(channel, call.clone())
                    .max_decoding_message_size(MAX_PART_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_MESSAGE_BYTES)
            }
            None => ObjectsClient::with_interceptor(channel.clone(), call.clone())
                .max_decoding_message_size(MAX_PART_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
        };
        let client = ScionClient::with_interceptor(channel, call)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        Ok(Self {
            client,
            objects,
            roots,
            repo,
            json,
        })
    }

    /// Turns a ref name or a 64-digit changeset ID into a changeset ID. A
    /// ref is resolved on the server: a tier serves no refs.
    pub async fn resolve(&mut self, rev: &str) -> Result<ChangesetId> {
        if let Ok(id) = rev.parse() {
            return Ok(id);
        }
        let reply = self
            .roots
            .resolve_ref(ResolveRefRequest {
                repo: self.repo.clone(),
                name: rev.to_owned(),
            })
            .await
            .with_context(|| format!("cannot resolve {rev:?}"))?
            .into_inner();
        id_from(&reply.changeset_id)
    }

    /// Uploads one file's content in chunks streamed from the file
    /// itself. The first message names the repo; an empty file still
    /// sends one (empty) message. The caller has checked the file is
    /// regular: a stream of a device or FIFO never ends. The file must
    /// keep the size and mtime it had when this was called: if it
    /// shrank, grew or was rewritten while it was read, the upload
    /// fails and names the file.
    ///
    /// # Errors
    ///
    /// Errors when the file changed while it was read, or the upload
    /// fails.
    pub async fn put_blob_file(&mut self, file: &Path) -> Result<BlobId> {
        #[cfg(any(test, feature = "test-hooks"))]
        if mutation_whole_file() {
            // The ceiling guard's lever: the old whole-file read, so
            // the guard can prove it is not vacuous.
            return self.put_blob(fs::read(file)?).await;
        }
        let path = file.to_path_buf();
        // Two chunks in flight, so the reader never runs far past the
        // network drain.
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
        // The read's verdict, after its last chunk or its refusal.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<std::io::Result<()>>();
        std::thread::spawn(move || {
            let read = (|| -> std::io::Result<()> {
                // The size and mtime come from the handle, so the
                // numbers belong to the file that was read.
                let mut f = fs::File::open(&path)?;
                let before = f.metadata()?;
                let len = before.len();
                let modified = before.modified()?;
                // The torn-read tests truncate the file here, between
                // the metadata read and the first chunk.
                take_put_pause();
                let mut left = len;
                loop {
                    // The last chunk is the short one; CHUNK_BYTES fits
                    // any pointer width.
                    let want = usize::try_from(left.min(CHUNK_BYTES as u64))
                        .expect("the chunk size fits usize");
                    let mut chunk = vec![0_u8; want];
                    f.read_exact(&mut chunk).map_err(|e| changed(&path, e))?;
                    left -= want as u64;
                    if tx.blocking_send(chunk).is_err() {
                        return Ok(()); // the call died; no one reads on
                    }
                    if left == 0 {
                        break;
                    }
                }
                // A byte past the length: the file grew.
                let mut one = [0_u8; 1];
                if f.read(&mut one)? > 0 {
                    return Err(changed(
                        &path,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "grew"),
                    ));
                }
                let after = f.metadata()?;
                // SHORTCUT: the rewrite check compares mtime, so a
                // same-size rewrite inside the filesystem's timestamp
                // granularity (FAT/exFAT 2 s, some NFS/FUSE mounts
                // 1 s) passes and a torn mix commits with no error;
                // ext4 and APFS are nanosecond, so a production cell
                // cannot hit it. Upgrade path: compare ctime too, or
                // re-read and hash the file.
                if after.len() != len || after.modified()? != modified {
                    return Err(changed(
                        &path,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "rewritten"),
                    ));
                }
                Ok(())
            })();
            // The chunks the call never took die with the channel.
            drop(tx);
            let _ = done_tx.send(read);
        });
        let mut first = Some(self.repo.clone());
        let requests = ReceiverStream::new(rx).map(move |chunk| PutBlobRequest {
            repo: first.take().unwrap_or_default(),
            data: chunk,
        });
        // The reply decides with the read: a refusal of the read names
        // the file, whatever the server answered about the parts.
        let reply = self.client.put_blob(requests).await;
        // The reader always answers, even when the call died first; if
        // it died itself, the call's own error tells the story.
        if let Ok(Err(e)) = done_rx.await {
            return Err(e.into());
        }
        let reply = reply?.into_inner();
        id_from(&reply.blob_id)
    }

    /// Uploads one file's content in chunks. The first message names the repo;
    /// an empty file still sends one (empty) message.
    pub async fn put_blob(&mut self, data: Vec<u8>) -> Result<BlobId> {
        let repo = self.repo.clone();
        let messages = data.len().div_ceil(CHUNK_BYTES).max(1);
        // Copies one chunk at a time, when the stream asks for it.
        let requests = (0..messages).map(move |i| {
            let end = data.len().min((i + 1) * CHUNK_BYTES);
            PutBlobRequest {
                repo: if i == 0 { repo.clone() } else { String::new() },
                data: data[i * CHUNK_BYTES..end].to_vec(),
            }
        });
        let reply = self
            .client
            .put_blob(tokio_stream::iter(requests))
            .await?
            .into_inner();
        id_from(&reply.blob_id)
    }

    /// Stores a changeset on the server. Moves no ref.
    pub async fn create_changeset(&mut self, changeset: &Changeset) -> Result<ChangesetId> {
        let reply = self
            .client
            .create_changeset(CreateChangesetRequest {
                repo: self.repo.clone(),
                changeset: changeset.encode(),
            })
            .await
            .context("cannot create the changeset")?
            .into_inner();
        id_from(&reply.changeset_id)
    }
}

/// Refuses a socket that is not a socket file owned by user `me` or by root,
/// so the token is never sent to another user's socket. A symlink is
/// refused too, and so is a socket under a directory that another user can
/// change ([`check_ancestors`]). Returns the path to connect to, with its
/// directory resolved.
fn check_socket(path: &Path, me: u32) -> Result<PathBuf> {
    let context = || format!("cannot use socket {}", path.display());
    let resolved = check_ancestors(path, me).with_context(context)?;
    let meta = fs::symlink_metadata(&resolved).with_context(context)?;
    if !meta.file_type().is_socket() {
        bail!("refusing {}: it is not a socket", path.display());
    }
    let owner = meta.uid();
    if owner != me && owner != 0 {
        bail!(
            "refusing socket {}: another user (uid {owner}) owns it",
            path.display()
        );
    }
    Ok(resolved)
}

/// Parses 32 raw ID bytes from a reply.
pub fn id_from<T: for<'a> TryFrom<&'a [u8]>>(bytes: &[u8]) -> Result<T> {
    T::try_from(bytes).map_err(|_| anyhow::anyhow!("server sent a bad ID"))
}

/// Splits `REV:PATH` into its parts. The path is optional.
pub(crate) fn split_spec(spec: &str) -> Result<(&str, Option<RepoPath>)> {
    match spec.split_once(':') {
        None => Ok((spec, None)),
        Some((rev, "")) => Ok((rev, None)),
        Some((rev, path)) => match RepoPath::new(path) {
            Ok(path) => Ok((rev, Some(path))),
            Err(e) => bail!("bad path {path:?}: {e}"),
        },
    }
}

/// Parses a path given on the command line. A trailing `/` is allowed.
pub fn parse_repo_path(path: &str) -> Result<RepoPath> {
    RepoPath::new(path.trim_end_matches('/')).with_context(|| format!("bad path {path:?}"))
}

/// Raw bytes as text for display.
///
/// The JSON output keeps the exact bytes beside the lossy text:
/// [`put_json_bytes`] writes a sibling `<field>_b64`, standard base64
/// with padding, only when the bytes are not valid UTF-8.
pub(crate) fn lossy(bytes: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

/// Sets `field` on `value` to the lossy text of `bytes` and, only when
/// the bytes are not valid UTF-8, adds the sibling `<field>_b64` with the
/// exact bytes, standard base64 with padding. A script that reads scn's
/// JSON gets the real bytes back from the sibling; a value that is valid
/// UTF-8 prints as before, with no extra field.
pub(crate) fn put_json_bytes(value: &mut serde_json::Value, field: &str, bytes: &[u8]) {
    value[field] = lossy(bytes).into();
    if std::str::from_utf8(bytes).is_err() {
        use base64::Engine as _;
        value[format!("{field}_b64")] = base64::engine::general_purpose::STANDARD
            .encode(bytes)
            .into();
    }
}

/// Seconds since the Unix epoch.
pub(crate) fn now_unix_secs() -> Result<i64> {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("the system clock is before 1970")?
        .as_secs();
    i64::try_from(secs).context("the system clock is too far in the future")
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_proto::runtime::deadline::Until;

    /// The deadline goes out on every call, so a waiting `scn` never hangs
    /// silently, and each tier on the way can end its waits in time.
    #[test]
    fn every_call_carries_the_deadline() {
        let mut call = Call::new(
            Bearer::new("token").unwrap(),
            "0123456789abcdef".parse().unwrap(),
        );
        let request = call.call(Request::new(())).unwrap();
        // 600 s less the margin that a reader of `grpc-timeout` keeps.
        let wait = Until::of(&request).wait(Duration::MAX);
        assert!(wait > Duration::from_secs(595) && wait <= Duration::from_secs(599));
    }

    /// One command sends its one request ID on every call.
    #[test]
    fn every_call_carries_the_request_id() {
        let mut call = Call::new(Bearer::new("token").unwrap(), "test-id-1".parse().unwrap());
        let request = call.call(Request::new(())).unwrap();
        assert_eq!(
            request.metadata().get(request_id::REQUEST_ID).unwrap(),
            "test-id-1"
        );
    }

    #[test]
    fn a_socket_that_another_user_owns_is_refused() {
        // Right in `/tmp`, whose directories root owns, so they pass the
        // check for every user and only the socket's owner counts.
        let socket = tempfile::Builder::new()
            .suffix(".sock")
            .make_in("/tmp", |path| std::os::unix::net::UnixListener::bind(path))
            .unwrap();
        let path = socket.path();
        let owner = fs::symlink_metadata(path).unwrap().uid();
        assert_eq!(
            check_socket(path, owner).unwrap(),
            fs::canonicalize(path).unwrap()
        );
        // The same socket, as another user sees it. A socket that root owns
        // is accepted for every user.
        if owner != 0 {
            let err = check_socket(path, owner + 1).unwrap_err();
            assert!(err.to_string().contains("owns it"), "{err:#}");
        }
    }

    #[test]
    fn a_socket_under_a_directory_that_others_can_write_is_refused() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let open = tmp.path().join("open");
        fs::create_dir(&open).unwrap();
        let path = open.join("daemon.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let owner = fs::symlink_metadata(&path).unwrap().uid();
        fs::set_permissions(&open, fs::Permissions::from_mode(0o777)).unwrap();
        let err = check_socket(&path, owner).unwrap_err();
        assert!(
            format!("{err:#}").contains("/open can be written"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn a_socket_through_a_symlinked_directory_is_reached_at_its_resolved_path() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        fs::create_dir(&real).unwrap();
        let _listener = std::os::unix::net::UnixListener::bind(real.join("daemon.sock")).unwrap();
        // A link whose name is too long for a socket address (104 bytes on
        // macOS, 108 on Linux): only the resolved path can connect.
        let link = tmp.path().join("l".repeat(120));
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let path = link.join("daemon.sock");
        let owner = fs::symlink_metadata(&path).unwrap().uid();
        let resolved = fs::canonicalize(&real).unwrap().join("daemon.sock");
        assert_eq!(check_socket(&path, owner).unwrap(), resolved);
        let server = format!("unix://{}", path.display());
        Ctx::connect(&server, None, "token", None, "r".into(), false)
            .await
            .unwrap();
    }
}
