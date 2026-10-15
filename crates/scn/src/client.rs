//! Connection to the server, plus helpers every command shares.

use std::borrow::Cow;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath};
use scion_proto::auth::Bearer;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, PutBlobRequest, ResolveRefRequest};
use scion_proto::{CHUNK_BYTES, MAX_MESSAGE_BYTES, MAX_OBJECT_MESSAGE_BYTES};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

/// A connected client.
pub type Client = ScionClient<InterceptedService<Channel, Bearer>>;

/// A connected client for the object reads that a cache tier can answer.
pub type Objects = ObjectsClient<InterceptedService<Channel, Bearer>>;

/// What every command needs.
#[derive(Clone)]
pub struct Ctx {
    /// The gRPC client.
    pub client: Client,
    /// The `Objects` client, on the same connection.
    pub objects: Objects,
    /// Repository name.
    pub repo: String,
    /// Print JSON instead of text.
    pub json: bool,
}

impl Ctx {
    /// Connects to `server`: `http://HOST:PORT`, or `unix:///PATH` for a
    /// daemon on this node.
    pub async fn connect(server: &str, token: &str, repo: String, json: bool) -> Result<Self> {
        // `Endpoint` reads `unix:` addresses too, unlike `Channel::from_shared`.
        let channel = Endpoint::from_shared(server.to_owned())
            .context("bad server address")?
            .connect()
            .await
            .with_context(|| format!("cannot connect to {server}"))?;
        let bearer = Bearer::new(token).context("token is not valid ASCII")?;
        let objects = ObjectsClient::with_interceptor(channel.clone(), bearer.clone())
            .max_decoding_message_size(MAX_OBJECT_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        let client = ScionClient::with_interceptor(channel, bearer)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        Ok(Self {
            client,
            objects,
            repo,
            json,
        })
    }

    /// Turns a ref name or a 64-digit changeset ID into a changeset ID. A ref
    /// is resolved through `Objects`, which a cache tier forwards.
    pub async fn resolve(&mut self, rev: &str) -> Result<ChangesetId> {
        if let Ok(id) = rev.parse() {
            return Ok(id);
        }
        let reply = self
            .objects
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
