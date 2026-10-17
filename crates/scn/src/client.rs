//! Connection to the server, plus helpers every command shares.

use std::borrow::Cow;
use std::fs;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath};
use scion_proto::auth::{Bearer, check_ancestors};
use scion_proto::request_id;
use scion_proto::tls::ClientTls;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, PutBlobRequest, ResolveRefRequest};
use scion_proto::{CHUNK_BYTES, MAX_MESSAGE_BYTES, MAX_PART_MESSAGE_BYTES};
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

/// Adds the bearer token, the deadline ([`CALL_TIMEOUT`]) and the request
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

/// The `Objects` client of `url`, for the tests, with a 10s timeout.
///
/// # Panics
///
/// Panics when `url` is not a valid address.
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
        let channel = endpoint
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
                let channel = endpoint
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
pub fn split_spec(spec: &str) -> Result<(&str, Option<RepoPath>)> {
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
// SHORTCUT: lossy, also in JSON output, so non-UTF-8 names and messages print
// with replacement characters. Upgrade: escape or base64 raw bytes in JSON.
pub fn lossy(bytes: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

/// Seconds since the Unix epoch.
pub fn now_unix_secs() -> Result<i64> {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("the system clock is before 1970")?
        .as_secs();
    i64::try_from(secs).context("the system clock is too far in the future")
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_proto::deadline::Until;

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
