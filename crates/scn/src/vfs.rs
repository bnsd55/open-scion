//! A virtual checkout over `NFSv3`: `scn mount` serves one ref of the
//! repository as a filesystem, so files load when they are first read
//! ([#11](https://github.com/bnsd55/openscion/issues/11)), through the
//! same `Objects` calls that a checkout sends.
//!
//! What the article says: files load when they are first read. The rest is
//! this project's choice: the NFS server is `nfsserve`, mounted with the
//! operating system's NFS client; a directory's tree is fetched when the
//! directory is first listed or looked up in; a file's blob when the file
//! is first read. Writes go to an overlay directory beside the
//! mount, which is the change list that `scn commit --from-mount` commits.
//!
//! A file's size comes from its tree entry, so a stat never fetches the
//! blob. A read checks the blob's bytes against the size the tree claimed
//! and refuses a mismatch.
//!
//! No protection: see `docs/architecture.md`, section 8. Any local user who
//! can read the mount table can reach the mounted content through the NFS
//! port, and root always can. The mount secret and the secret in every file
//! handle stop stray and accidental callers only.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
#[cfg(feature = "unstable-writable-mount")]
use std::future::Future;
use std::io::Read as _;
#[cfg(feature = "unstable-writable-mount")]
use std::io::Write as _;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
#[cfg(feature = "unstable-writable-mount")]
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, anyhow, bail};
use async_trait::async_trait;
use hashlink::LinkedHashMap;
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfs_fh3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
};
#[cfg(feature = "unstable-writable-mount")]
use nfsserve::nfs::{set_mode3, set_size3};
// The test seam re-exports this (test_support.vfs), and
// unreachable_pub does not see through a re-export.
#[allow(unreachable_pub)]
pub use nfsserve::vfs::NFSFileSystem;
use nfsserve::vfs::{DirEntry, ReadDirResult, VFSCapabilities};
#[cfg(feature = "unstable-writable-mount")]
use rustix::fs::{Mode, OFlags};
use scion_core::{BlobId, ChangesetId, Entry, FileState, RepoPath, Tree, TreeId};
use scion_proto::runtime::object::{self, Assembler, Header};
use scion_proto::ticket::{self, TICKET_SKEW_SECS};
use scion_proto::v1::{Encoding, GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectRef};

use crate::client::{Objects, id_from};
use crate::materialize::check_name;

/// The name of the file that the commit reads the deletions from.
pub(crate) const DELETES: &str = "deletes.json";

/// The subdirectory of the overlay that mirrors the changed paths.
pub(crate) const CHANGES: &str = "tree";

/// The bytes of decoded blobs kept in memory, least recently used first.
/// The daemon holds the stored form on disk; this cache saves the unpacking
/// of a file that is read again.
const BLOB_CACHE_BYTES: usize = 128 << 20;

/// The handle of the root directory. 0 is reserved in NFS.
const ROOT_ID: u64 = 1;

/// The bytes of the blobs that are in memory, least recently used first,
/// and their bytes together.
type Blobs = tokio::sync::Mutex<(LinkedHashMap<BlobId, Arc<Vec<u8>>>, usize)>;

/// The mount's read ticket: the bytes the calls carry, and when they stop
/// working. The expiry comes from `parse_unverified`, which a daemon may
/// run: it forwards tickets, and never vouches for one.
struct Grant {
    bytes: Vec<u8>,
    /// When the ticket expires, in seconds since the epoch.
    expires: u64,
}

impl Grant {
    /// The grant of `bytes`; a ticket that does not parse counts as
    /// already expired, so the first read mints a fresh one.
    fn of(bytes: &[u8]) -> Self {
        let expires = match ticket::parse_unverified(bytes, ticket::now()) {
            Ok(parsed) => parsed.expires,
            Err(_) => 0,
        };
        Self {
            bytes: bytes.to_vec(),
            expires,
        }
    }

    /// Whether the ticket still works with time to spare: more than the
    /// clock-skew window left. A tier's clock may be ahead by up to that
    /// window, so a ticket with less left can be refused at any moment,
    /// and the mount mints a fresh one first.
    fn fresh(&self, now: u64) -> bool {
        self.expires.saturating_sub(now) > TICKET_SKEW_SECS
    }
}

/// The seconds of now, since the epoch. A clock before the epoch counts as
/// expired tickets, which only costs a refresh.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Is this a failure that a fresh ticket can fix?
fn expired(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::PermissionDenied && status.message().contains("ticket expired")
}

/// One node: a directory, a regular file or a symlink, of the ref or of the
/// overlay.
struct Node {
    /// None for the root directory.
    path: Option<RepoPath>,
    kind: Kind,
    /// Whether the ref names this path, so removing it is a deletion.
    #[cfg(feature = "unstable-writable-mount")]
    from_ref: bool,
}

// The overlay variants are constructed only by the write path, which a
// build without the feature compiles out; the read side still names them.
#[cfg_attr(not(feature = "unstable-writable-mount"), allow(dead_code))]
enum Kind {
    /// A directory. Its tree is fetched once, when it is first listed or
    /// looked up in; `listed` then names each entry's handle.
    Dir {
        tree: Option<TreeId>,
        listed: Option<BTreeMap<Vec<u8>, u64>>,
    },
    /// A directory made on the mount. It holds nothing of the ref.
    LocalDir,
    /// A regular file of the ref: its blob and the length its tree claims.
    Store { blob: BlobId, exec: bool, len: u64 },
    /// A regular file of the overlay: its bytes are the file.
    Local { exec: bool },
    /// A symlink of the ref: the blob that holds the target bytes, and the
    /// length its tree claims for it.
    StoreLink { blob: BlobId, len: u64 },
    /// A symlink made on the mount.
    Link { target: Vec<u8> },
}

impl Kind {
    fn is_dir(&self) -> bool {
        matches!(self, Kind::Dir { .. } | Kind::LocalDir)
    }
}

/// A change that `scn commit --from-mount` commits for one path.
pub(crate) enum Change {
    /// The file at the overlay path replaces the path, or adds it.
    Put {
        /// Whether the file is executable.
        exec: bool,
    },
    /// The symlink with this target replaces the path.
    PutLink(Vec<u8>),
    /// The path of the ref is deleted.
    Delete,
}

/// Every node of one mount.
struct Nodes {
    next: u64,
    by_id: BTreeMap<u64, Node>,
    by_path: HashMap<Option<RepoPath>, u64>,
    /// Paths of the ref that a remove or a rename took away, so a listing
    /// does not bring them back. The commit note holds them too.
    deleted: HashSet<RepoPath>,
}

impl Nodes {
    /// Puts a new node into the map, with a fresh handle.
    #[cfg(feature = "unstable-writable-mount")]
    #[cfg_attr(not(feature = "unstable-writable-mount"), allow(unused_variables))]
    fn add(&mut self, path: Option<RepoPath>, kind: Kind, from_ref: bool) -> u64 {
        let id = self.next;
        self.next += 1;
        self.by_path.insert(path.clone(), id);
        self.by_id.insert(
            id,
            Node {
                path,
                kind,
                #[cfg(feature = "unstable-writable-mount")]
                from_ref,
            },
        );
        id
    }

    /// Drops the node `id`, and its name from its directory's listing.
    /// Drops the node `id`, and its name from the map of paths. A deletion
    /// note stays: a re-created path takes it off (`forget_delete`).
    #[cfg(feature = "unstable-writable-mount")]
    fn drop_node(&mut self, id: u64) {
        if let Some(node) = self.by_id.remove(&id) {
            self.by_path.remove(&node.path);
        }
    }
}

/// The NFS file system of one mount.
// The test seam re-exports this (test_support.vfs), and
// unreachable_pub does not see through a re-export.
#[allow(unreachable_pub)]
pub struct Vfs {
    objects: Objects,
    /// The client of the server, for the refresh of the ticket: a
    /// `GetRootTree` goes there, never to a tier, which serves no refs and
    /// mints no tickets.
    roots: Objects,
    repo: String,
    /// The changeset this mount holds. Its root tree is what a fresh
    /// ticket names, so a ref that moved after the mount never changes
    /// what this mount reads.
    changeset: ChangesetId,
    /// The mount's read ticket, and when it stops working. Every
    /// `GetObjects` carries it; the mount mints a fresh one through
    /// `roots` before it expires, and again on a refusal that says
    /// `ticket expired`.
    ticket: tokio::sync::RwLock<Grant>,
    /// Mints one ticket at a time: reads that find an old ticket together
    /// wait for one `GetRootTree`, and the first to finish refreshes for
    /// all of them.
    mint: tokio::sync::Mutex<()>,
    nodes: tokio::sync::RwLock<Nodes>,
    /// Decoded blobs, least recently used first, with their bytes together.
    /// The daemon holds the stored form on disk; this saves the unpacking
    /// of a file that is read again.
    blobs: Blobs,
    /// Where the changes of this mount live.
    overlay: PathBuf,
    /// Whether the mount takes writes: v0.1 mounts are read-only unless
    /// `scn mount --unstable-writable` said otherwise, because a symlinked
    /// parent directory in the overlay can still redirect a write outside
    /// it. The write path exists only in a build with the
    /// `unstable-writable-mount` feature; a default or release build has
    /// no write path at all.
    #[cfg(feature = "unstable-writable-mount")]
    writable: bool,
    /// The secret that a mount path names and every file handle carries.
    /// Not a protection: see section 8 of the docs.
    secret: [u8; 16],
    started: SystemTime,
    uid: u32,
    gid: u32,
}

impl Vfs {
    /// A virtual checkout of `changeset`, whose root tree is `root`, of
    /// `repo`. The objects come from `objects`, which may be a tier; the
    /// ticket is refreshed through `roots`, which is the server. `ticket`
    /// is the read ticket that minted for `root`; a ticket that does not
    /// parse is stored as expired, so the first read mints a fresh one.
    /// Changes go to `overlay`.
    // The test seam re-exports the Vfs surface (test_support.vfs), and
    // unreachable_pub does not see through a re-export.
    #[allow(unreachable_pub)]
    pub fn new(
        objects: Objects,
        roots: Objects,
        repo: String,
        changeset: ChangesetId,
        root: TreeId,
        ticket: &[u8],
        overlay: PathBuf,
    ) -> Self {
        let grant = Grant::of(ticket);
        Self {
            objects,
            roots,
            repo,
            changeset,
            ticket: tokio::sync::RwLock::new(grant),
            mint: tokio::sync::Mutex::new(()),
            nodes: tokio::sync::RwLock::new(Nodes {
                next: 2,
                by_id: BTreeMap::from([(
                    ROOT_ID,
                    Node {
                        path: None,
                        kind: Kind::Dir {
                            tree: Some(root),
                            listed: None,
                        },
                        #[cfg(feature = "unstable-writable-mount")]
                        from_ref: true,
                    },
                )]),
                by_path: HashMap::from([(None, ROOT_ID)]),
                deleted: HashSet::new(),
            }),
            blobs: tokio::sync::Mutex::new((LinkedHashMap::new(), 0)),
            secret: secret_from_urandom(),
            started: SystemTime::now(),
            uid: rustix::process::geteuid().as_raw(),
            gid: rustix::process::getegid().as_raw(),
            overlay,
            #[cfg(feature = "unstable-writable-mount")]
            writable: false,
        }
    }

    /// Takes writes on this mount, or not: `scn mount --unstable-writable`
    /// is the only caller that passes true. A read-only VFS answers every
    /// mutating call with `EROFS`, before it looks at its arguments.
    #[must_use]
    #[cfg(feature = "unstable-writable-mount")]
    // The test seam re-exports the Vfs surface (test_support.vfs), and
    // unreachable_pub does not see through a re-export.
    #[allow(unreachable_pub)]
    pub fn writable(mut self, on: bool) -> Self {
        self.writable = on;
        self
    }

    #[cfg(feature = "unstable-writable-mount")]
    /// Refuses a mutating call on a read-only mount. A build without the
    /// writable-mount feature refuses every mutating call: it has no
    /// write path at all.
    fn want_write(&self) -> Result<(), nfsstat3> {
        #[cfg(not(feature = "unstable-writable-mount"))]
        {
            Err(nfsstat3::NFS3ERR_ROFS)
        }
        #[cfg(feature = "unstable-writable-mount")]
        if self.writable {
            Ok(())
        } else {
            Err(nfsstat3::NFS3ERR_ROFS)
        }
    }

    /// The mount secret, as the file-name part of a mount path.
    // The test seam re-exports the Vfs surface (test_support.vfs), and
    // unreachable_pub does not see through a re-export.
    #[allow(unreachable_pub)]
    pub fn secret_name(&self) -> String {
        self.secret.iter().fold(String::new(), |mut all, b| {
            use std::fmt::Write as _;
            let _ = write!(all, "{b:02x}");
            all
        })
    }

    /// The path of `path` under the overlay directory, from the raw
    /// bytes: a name that is not UTF-8 must name its own file, not the
    /// file its lossy spelling names.
    // The test seam re-exports the Vfs surface (test_support.vfs), and
    // unreachable_pub does not see through a re-export.
    #[allow(unreachable_pub)]
    pub fn overlay_file(&self, path: &RepoPath) -> PathBuf {
        let mut file = self.overlay.clone();
        for part in path.as_bytes().split(|&b| b == b'/') {
            file.push(OsStr::from_bytes(part));
        }
        file
    }

    /// The directory of `path`: None for a name at the root.
    fn parent(path: &RepoPath) -> Option<RepoPath> {
        let bytes = path.as_bytes();
        let at = bytes.iter().rposition(|&b| b == b'/')?;
        Some(RepoPath::new(&bytes[..at]).expect("a prefix of a path is a path"))
    }

    /// Joins `name` onto `dir`, and checks both names.
    fn child(dir: Option<&RepoPath>, name: &[u8]) -> Result<RepoPath, nfsstat3> {
        if check_name(name).is_err() {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        let bytes = match dir {
            None => name.to_vec(),
            Some(dir) => {
                let mut bytes = dir.as_bytes().to_vec();
                bytes.push(b'/');
                bytes.extend_from_slice(name);
                bytes
            }
        };
        RepoPath::new(bytes).map_err(|_| nfsstat3::NFS3ERR_INVAL)
    }

    /// Puts a new node into the map, with a fresh handle.
    #[cfg_attr(not(feature = "unstable-writable-mount"), allow(unused_variables))]
    fn add(nodes: &mut Nodes, path: Option<RepoPath>, kind: Kind, from_ref: bool) -> u64 {
        let id = nodes.next;
        nodes.next += 1;
        nodes.by_path.insert(path.clone(), id);
        nodes.by_id.insert(
            id,
            Node {
                path,
                kind,
                #[cfg(feature = "unstable-writable-mount")]
                from_ref,
            },
        );
        id
    }

    /// The node at `path`, if the overlay or the already read part of the
    /// ref holds one.
    fn id_of(nodes: &Nodes, path: Option<&RepoPath>) -> Option<u64> {
        nodes.by_path.get(&path.map(ToOwned::to_owned)).copied()
    }

    /// The ticket to read with: the current one while it has time left,
    /// and else a fresh mint over the MOUNTED changeset, never over a ref,
    /// which may have moved since the mount ([#11]). With `force`, always
    /// mints, for a refusal that said `ticket expired`.
    async fn ticket(&self, force: bool) -> Result<Vec<u8>> {
        if !force {
            let grant = self.ticket.read().await;
            if grant.fresh(now_secs()) {
                return Ok(grant.bytes.clone());
            }
        }
        // One mint at a time; the others take the ticket it made.
        let _once = self.mint.lock().await;
        if !force {
            let grant = self.ticket.read().await;
            if grant.fresh(now_secs()) {
                return Ok(grant.bytes.clone());
            }
        }
        let reply = self
            .roots
            .clone()
            .get_root_tree(GetRootTreeRequest {
                repo: self.repo.clone(),
                changeset_id: self.changeset.as_bytes().to_vec(),
            })
            .await
            .context("cannot refresh the mount's ticket")?
            .into_inner();
        if reply.ticket.is_empty() {
            bail!("the server sent no ticket for the mount");
        }
        *self.ticket.write().await = Grant::of(&reply.ticket);
        Ok(reply.ticket)
    }

    /// One object of `kind` at `path` from the mounted root tree, checked,
    /// in its plain form. The empty path names the root tree itself; the
    /// path is what the ticket's walk reaches. The mount's ticket goes out
    /// with every call: refreshed before it expires, and again on a
    /// refusal that says `ticket expired`, then the call is asked once
    /// more.
    async fn fetch_object(
        &self,
        kind: ObjectKind,
        id: [u8; 32],
        path: Option<&RepoPath>,
        expect: Option<u64>,
    ) -> Result<Vec<u8>> {
        let mut objects = self.objects.clone();
        let ask = |ticket: Vec<u8>| GetObjectsRequest {
            refs: vec![ObjectRef {
                kind: i32::from(kind),
                id: id.to_vec(),
                path: path.map(|p| p.as_bytes().to_vec()).unwrap_or_default(),
            }],
            accept: vec![Encoding::Zstd.into()],
            ticket,
        };
        let mut ticket = self.ticket(false).await?;
        let mut call = objects.get_objects(ask(ticket.clone())).await;
        if let Err(status) = &call
            && expired(status)
        {
            // The ticket died mid-mount: mint over the mounted changeset
            // and ask again, once.
            ticket = self.ticket(true).await?;
            call = objects.get_objects(ask(ticket)).await;
        }
        let mut stream = call.context("cannot fetch objects")?.into_inner();
        let first = stream
            .message()
            .await
            .context("cannot fetch objects")?
            .context("the reply holds no object")?;
        if first.kind != i32::from(kind) || id_from::<[u8; 32]>(&first.id)? != id {
            bail!("the server sent an object that was not asked for");
        }
        let bad = |why| anyhow!("the server sent a bad object: {why}");
        let header = Header::of(&first, true).map_err(bad)?;
        // The tree said how big this blob is. A first part whose size
        // disagrees ends the fetch before any part of the body reads.
        if let Some(want) = expect
            && header.size as u64 != want
        {
            bail!(
                "the server sent {} bytes, the tree says {want}",
                header.size
            );
        }
        let mut assembled = Assembler::new(header, first);
        while !assembled.is_done() {
            let part = stream
                .message()
                .await
                .context("cannot fetch objects")?
                .context("the reply ended in the middle of an object")?;
            assembled.push(&part).map_err(bad)?;
        }
        let (header, data) = assembled.finish();
        object::decode(&header, data).map_err(bad)
    }

    /// The tree `id`, checked: the directory entry at `path` from the
    /// mounted root tree.
    async fn fetch_tree(&self, id: TreeId, path: Option<&RepoPath>) -> Result<Tree> {
        let data = self
            .fetch_object(ObjectKind::Tree, *id.as_bytes(), path, None)
            .await
            .with_context(|| format!("cannot read tree {id}"))?;
        let (tree, actual) = Tree::decode_with_id(&data)
            .with_context(|| format!("the server sent a bad tree for {id}"))?;
        if actual != id {
            bail!("the server sent tree {actual} for {id}");
        }
        Ok(tree)
    }

    /// The blob `id`, checked, through the cache of decoded blobs.
    /// The blob `id`, checked, through the cache of decoded blobs. The
    /// lock is held only for the look up and the insert, never over the
    /// network fetch: one slow fetch must not stop the other reads of the
    /// mount. Two calls that race on one blob both fetch, and both insert
    /// a correct copy; the last one wins.
    async fn fetch_blob(
        &self,
        id: BlobId,
        path: Option<&RepoPath>,
        len: u64,
    ) -> Result<Arc<Vec<u8>>> {
        if let Some(data) = {
            let mut blobs = self.blobs.lock().await;
            blobs.0.get_mut(&id).map(|data| Arc::clone(data))
        } {
            return Ok(data);
        }
        let raw = self
            .fetch_object(ObjectKind::Blob, *id.as_bytes(), path, Some(len))
            .await
            .with_context(|| format!("cannot read blob {id}"))?;
        if BlobId::of(&raw) != id {
            bail!("the server sent other bytes than what {id} names");
        }
        let data = Arc::new(raw);
        let mut blobs = self.blobs.lock().await;
        blobs.1 += data.len();
        blobs.0.insert(id, Arc::clone(&data));
        while blobs.1 > BLOB_CACHE_BYTES {
            let bytes = blobs.0.front().map_or(0, |(_, data)| data.len());
            blobs.0.pop_front();
            blobs.1 = blobs.1.saturating_sub(bytes);
        }
        Ok(data)
    }

    /// Fetches the tree of the directory `id` into its listing, once.
    async fn list_dir(&self, nodes: &mut Nodes, id: u64) -> Result<(), nfsstat3> {
        let (dir_path, tree) = {
            let node = nodes.by_id.get(&id).ok_or(nfsstat3::NFS3ERR_STALE)?;
            match &node.kind {
                Kind::Dir {
                    listed: Some(_), ..
                }
                | Kind::LocalDir => return Ok(()),
                Kind::Dir { tree, listed: None } => (node.path.clone(), *tree),
                _ => return Err(nfsstat3::NFS3ERR_NOTDIR),
            }
        };
        let mut listed = BTreeMap::new();
        if let Some(tree) = tree {
            let tree = self
                .fetch_tree(tree, dir_path.as_ref())
                .await
                .map_err(|e| {
                    warn("cannot read a tree of the mount", &e);
                    nfsstat3::NFS3ERR_IO
                })?;
            for (name, entry) in tree.entries() {
                let path = Self::child(dir_path.as_ref(), name.as_bytes())?;
                if nodes.deleted.contains(&path) || nodes.by_path.contains_key(&Some(path.clone()))
                {
                    continue;
                }
                let kind = match entry {
                    Entry::Dir(tree) => Kind::Dir {
                        tree: Some(*tree),
                        listed: None,
                    },
                    Entry::File(file) => match (file.state(), file.size()) {
                        (
                            FileState::Regular {
                                content,
                                executable,
                            },
                            len,
                        ) => Kind::Store {
                            blob: content,
                            exec: executable,
                            len,
                        },
                        (FileState::Symlink { target }, len) => {
                            Kind::StoreLink { blob: target, len }
                        }
                        // A gitlink names a commit of another repository,
                        // not an object of this one.
                        (FileState::Gitlink { .. }, _) => continue,
                    },
                };
                let child = Self::add(nodes, Some(path), kind, true);
                listed.insert(name.as_bytes().to_vec(), child);
            }
        }
        let node = nodes.by_id.get_mut(&id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let Kind::Dir { listed: slot, .. } = &mut node.kind else {
            unreachable!("the kind was checked above");
        };
        *slot = Some(listed);
        Ok(())
    }

    /// Loads the listing of `dirid` if it is a directory.
    async fn ensure_listed(
        &self,
        nodes: &mut Nodes,
        dirid: u64,
    ) -> Result<Option<RepoPath>, nfsstat3> {
        let dir_path = {
            let node = nodes.by_id.get(&dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;
            if !node.kind.is_dir() {
                return Err(nfsstat3::NFS3ERR_NOTDIR);
            }
            node.path.clone()
        };
        self.list_dir(nodes, dirid).await?;
        Ok(dir_path)
    }

    /// The names and handles of the entries of the directory `id`: the
    /// tree's, with the overlay's changes merged in.
    async fn merged_listing(
        &self,
        nodes: &mut Nodes,
        dirid: u64,
    ) -> Result<Vec<(Vec<u8>, u64)>, nfsstat3> {
        self.list_dir(nodes, dirid).await?;
        let dir_path = nodes
            .by_id
            .get(&dirid)
            .ok_or(nfsstat3::NFS3ERR_STALE)?
            .path
            .clone();
        let mut listed = match nodes.by_id.get(&dirid) {
            Some(Node {
                kind:
                    Kind::Dir {
                        listed: Some(listed),
                        ..
                    },
                ..
            }) => listed
                .iter()
                .map(|(name, id)| (name.clone(), *id))
                .collect::<Vec<_>>(),
            // A LocalDir holds only overlay entries.
            Some(_) => Vec::new(),
            None => unreachable!("checked above"),
        };
        let dir_len = dir_path.as_ref().map_or(0, |d| d.as_bytes().len() + 1);
        for (path, id) in &nodes.by_path {
            let Some(path) = path else { continue };
            if Self::parent(path).as_ref() != dir_path.as_ref() || nodes.deleted.contains(path) {
                continue;
            }
            let name = path.as_bytes()[dir_len..].to_vec();
            if !listed.iter().any(|(other, _)| *other == name) {
                listed.push((name, *id));
            }
        }
        listed.sort();
        Ok(listed)
    }
}

#[async_trait]
impl NFSFileSystem for Vfs {
    fn capabilities(&self) -> VFSCapabilities {
        // A build without the writable-mount feature serves read-only,
        // whatever the mount said: it has no write path at all.
        #[cfg(not(feature = "unstable-writable-mount"))]
        return VFSCapabilities::ReadOnly;
        #[cfg(feature = "unstable-writable-mount")]
        if self.writable {
            VFSCapabilities::ReadWrite
        } else {
            VFSCapabilities::ReadOnly
        }
    }

    fn root_dir(&self) -> fileid3 {
        ROOT_ID
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let mut nodes = self.nodes.write().await;
        let dir_path = self.ensure_listed(&mut nodes, dirid).await?;
        let path = Self::child(dir_path.as_ref(), filename.as_ref())?;
        if nodes.deleted.contains(&path) {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }
        Self::id_of(&nodes, Some(&path)).ok_or(nfsstat3::NFS3ERR_NOENT)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        let nodes = self.nodes.read().await;
        self.attr_of(&nodes, id)
    }

    #[cfg(not(feature = "unstable-writable-mount"))]
    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    #[cfg(feature = "unstable-writable-mount")]
    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        self.want_write()?;
        let mut nodes = self.nodes.write().await;
        match nodes.by_id.get(&id) {
            Some(node) if node.kind.is_dir() => {
                // The mode of a directory is fixed; the times are not kept.
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            None => return Err(nfsstat3::NFS3ERR_STALE),
            _ => {}
        }
        self.materialize(&mut nodes, id).await?;
        let file = self.overlay_file(nodes.by_id.get(&id).unwrap().path.as_ref().unwrap());
        if let set_mode3::mode(m) = setattr.mode {
            let exec = m & 0o111 != 0;
            let handle = open_overlay_chmod(&file).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            // RawMode is u32 on Linux and u16 on macOS, so the conversion
            // is real only on macOS.
            #[allow(clippy::useless_conversion)]
            let mode = Mode::from_raw_mode(file_mode(0o644, exec).try_into().unwrap());
            rustix::fs::fchmod(&handle, mode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            if let Some(Node {
                kind: Kind::Local { exec: slot },
                ..
            }) = nodes.by_id.get_mut(&id)
            {
                *slot = exec;
            }
        }
        if let set_size3::size(bytes) = setattr.size {
            let f = open_overlay_write(&file, true).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            f.set_len(bytes).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        }
        self.attr_of(&nodes, id)
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        // One read copies at most `count` bytes: a ref file slices the
        // cached blob, an overlay file is read from its offset. The client
        // reads in pieces, so a whole-file copy would make a big file
        // quadratic.
        let nodes = self.nodes.read().await;
        let node = nodes.by_id.get(&id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        match &node.kind {
            Kind::Store { blob, len, .. } => {
                let data = self
                    .fetch_blob(*blob, node.path.as_ref(), *len)
                    .await
                    .map_err(io_err)?;
                if data.len() as u64 != *len {
                    // The tree said one size, the blob holds another: one of
                    // them is wrong, and reading on would serve lies.
                    let why = format!(
                        "{}: {} bytes, the tree says {len}",
                        node.path.as_ref().map_or_else(
                            || "<unnamed>".to_owned(),
                            |p| String::from_utf8_lossy(p.as_bytes()).to_string()
                        ),
                        data.len()
                    );
                    warn("a blob does not match the size of its tree entry", &why);
                    return Err(nfsstat3::NFS3ERR_IO);
                }
                let len = data.len() as u64;
                let from = usize::try_from(offset.min(len)).unwrap_or(usize::MAX);
                let to = usize::try_from(offset.saturating_add(u64::from(count)).min(len))
                    .unwrap_or(usize::MAX);
                Ok((data[from..to].to_vec(), to as u64 >= len))
            }
            Kind::Local { .. } => {
                use std::io::SeekFrom;
                use std::io::{Read as _, Seek as _};

                let file = self.overlay_file(node.path.as_ref().expect("a file has a path"));
                let mut file = std::fs::File::open(&file).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                let len = file.metadata().map_err(|_| nfsstat3::NFS3ERR_IO)?.len();
                let from = offset.min(len);
                file.seek(SeekFrom::Start(from))
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                let want = usize::try_from(count).unwrap_or(usize::MAX);
                let mut data = Vec::new();
                let taken = file
                    .take(u64::try_from(want).unwrap_or(u64::MAX))
                    .read_to_end(&mut data)
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                let eof = from + taken as u64 >= len;
                Ok((data, eof))
            }
            Kind::Dir { .. } | Kind::LocalDir => Err(nfsstat3::NFS3ERR_ISDIR),
            Kind::Link { .. } | Kind::StoreLink { .. } => Err(nfsstat3::NFS3ERR_INVAL),
        }
    }

    #[cfg(not(feature = "unstable-writable-mount"))]
    async fn write(&self, _id: fileid3, _offset: u64, __data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    #[cfg(feature = "unstable-writable-mount")]
    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        use std::io::Seek as _;

        self.want_write()?;
        let mut nodes = self.nodes.write().await;
        let file = match nodes.by_id.get(&id) {
            None => return Err(nfsstat3::NFS3ERR_STALE),
            Some(node) => match &node.path {
                None => return Err(nfsstat3::NFS3ERR_ISDIR),
                Some(path) => self.overlay_file(path),
            },
        };
        match nodes.by_id.get(&id).unwrap().kind {
            Kind::Dir { .. } | Kind::LocalDir => return Err(nfsstat3::NFS3ERR_ISDIR),
            Kind::Link { .. } | Kind::StoreLink { .. } => return Err(nfsstat3::NFS3ERR_INVAL),
            _ => {}
        }
        self.materialize(&mut nodes, id).await?;
        let mut file = open_overlay_write(&file, false).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        file.seek(std::io::SeekFrom::Start(offset))
            .and_then(|_| file.write_all(data))
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let meta = file.metadata().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let exec = meta.permissions().mode() & 0o111 != 0;
        Ok(self.attr(ftype3::NF3REG, file_mode(0o644, exec), meta.len(), id))
    }

    #[cfg(not(feature = "unstable-writable-mount"))]
    async fn create(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    #[cfg(feature = "unstable-writable-mount")]
    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.want_write()?;
        let mut nodes = self.nodes.write().await;
        let dir_path = self.ensure_listed(&mut nodes, dirid).await?;
        let path = Self::child(dir_path.as_ref(), filename.as_ref())?;
        let file = self.overlay_file(&path);
        let exec = match attr.mode {
            set_mode3::mode(m) => m & 0o111 != 0,
            set_mode3::Void => false,
        };
        write_file(&file, &[], exec).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let id = nodes.add(Some(path.clone()), Kind::Local { exec }, false);
        self.forget_delete(&mut nodes, &path);
        note_child(&mut nodes, dirid, filename.as_ref(), id);
        Ok((id, self.attr(ftype3::NF3REG, file_mode(0o644, exec), 0, id)))
    }

    #[cfg(not(feature = "unstable-writable-mount"))]
    async fn create_exclusive(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    #[cfg(feature = "unstable-writable-mount")]
    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        self.want_write()?;
        let mut nodes = self.nodes.write().await;
        let dir_path = self.ensure_listed(&mut nodes, dirid).await?;
        let path = Self::child(dir_path.as_ref(), filename.as_ref())?;
        if Self::id_of(&nodes, Some(&path)).is_some() && !nodes.deleted.contains(&path) {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }
        let file = self.overlay_file(&path);
        write_file(&file, &[], false).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let id = nodes.add(Some(path.clone()), Kind::Local { exec: false }, false);
        self.forget_delete(&mut nodes, &path);
        note_child(&mut nodes, dirid, filename.as_ref(), id);
        Ok(id)
    }

    #[cfg(not(feature = "unstable-writable-mount"))]
    async fn mkdir(
        &self,
        _dirid: fileid3,
        _dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    #[cfg(feature = "unstable-writable-mount")]
    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.want_write()?;
        let mut nodes = self.nodes.write().await;
        let dir_path = self.ensure_listed(&mut nodes, dirid).await?;
        let path = Self::child(dir_path.as_ref(), dirname.as_ref())?;
        if Self::id_of(&nodes, Some(&path)).is_some() && !nodes.deleted.contains(&path) {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }
        let file = self.overlay_file(&path);
        std::fs::create_dir_all(&file).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let id = nodes.add(Some(path.clone()), Kind::LocalDir, false);
        self.forget_delete(&mut nodes, &path);
        note_child(&mut nodes, dirid, dirname.as_ref(), id);
        Ok((id, self.attr(ftype3::NF3DIR, 0o755, 4096, id)))
    }

    #[cfg(not(feature = "unstable-writable-mount"))]
    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    #[cfg(feature = "unstable-writable-mount")]
    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        self.want_write()?;
        let mut nodes = self.nodes.write().await;
        let dir_path = self.ensure_listed(&mut nodes, dirid).await?;
        let path = Self::child(dir_path.as_ref(), filename.as_ref())?;
        let id = Self::id_of(&nodes, Some(&path))
            .filter(|_| !nodes.deleted.contains(&path))
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;
        let (is_dir, from_ref) = {
            let node = &nodes.by_id[&id];
            (node.kind.is_dir(), node.from_ref)
        };
        if is_dir {
            // Empty only: no overlay child, and no name of the tree left.
            let overlay_child = nodes.by_path.iter().any(|(p, _)| {
                p.as_ref()
                    .is_some_and(|p| Self::parent(p).as_ref() == dir_path.as_ref())
            });
            let tree_child = match &nodes.by_id[&id].kind {
                Kind::Dir {
                    listed: Some(listed),
                    ..
                } => !listed.is_empty(),
                _ => false,
            };
            if overlay_child || tree_child {
                return Err(nfsstat3::NFS3ERR_NOTEMPTY);
            }
            if from_ref {
                // A directory of the ref stays; the commit deletes its
                // files instead.
                return Err(nfsstat3::NFS3ERR_ROFS);
            }
            std::fs::remove_dir(self.overlay_file(&path)).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        } else {
            if matches!(
                nodes.by_id[&id].kind,
                Kind::Local { .. } | Kind::Link { .. }
            ) {
                std::fs::remove_file(self.overlay_file(&path)).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            }
            if from_ref {
                self.note_delete(&mut nodes, &path);
            }
        }
        drop_listed(&mut nodes, dirid, filename.as_ref());
        nodes.drop_node(id);
        Ok(())
    }

    #[cfg(not(feature = "unstable-writable-mount"))]
    async fn rename(
        &self,
        _from_dirid: fileid3,
        _from_filename: &filename3,
        _to_dirid: fileid3,
        _to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    #[cfg(feature = "unstable-writable-mount")]
    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        self.want_write()?;
        let mut nodes = self.nodes.write().await;
        let from_dir = self.ensure_listed(&mut nodes, from_dirid).await?;
        let to_dir = self.ensure_listed(&mut nodes, to_dirid).await?;
        let from_path = Self::child(from_dir.as_ref(), from_filename.as_ref())?;
        let to_path = Self::child(to_dir.as_ref(), to_filename.as_ref())?;
        if from_path == to_path {
            return Ok(());
        }
        let from_id = Self::id_of(&nodes, Some(&from_path))
            .filter(|_| !nodes.deleted.contains(&from_path))
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;
        // A directory rename is checked before anything moves: a target
        // that is a file is not a directory, an existing non-empty
        // directory is not empty, and a move into the mover's own subtree
        // is invalid (the children would be written under the mover).
        if nodes.by_id[&from_id].kind.is_dir() {
            let mut under = from_path.as_bytes().to_vec();
            under.push(b'/');
            if to_path.as_bytes().starts_with(&under) {
                return Err(nfsstat3::NFS3ERR_INVAL);
            }
            // The target of a directory rename must be a directory, and
            // empty at most. The check runs before anything moves.
            let target =
                Self::id_of(&nodes, Some(&to_path)).filter(|_| !nodes.deleted.contains(&to_path));
            if let Some(to_id) = target {
                if !nodes.by_id[&to_id].kind.is_dir() {
                    return Err(nfsstat3::NFS3ERR_NOTDIR);
                }
                if !self.dir_is_empty(&mut nodes, to_id, &to_path).await? {
                    return Err(nfsstat3::NFS3ERR_NOTEMPTY);
                }
            }
            // A directory moves with everything under it: each file and
            // symlink of the ref is materialized at the new path, and its old
            // path is a deletion.
            self.move_dir(&mut nodes, from_id, &from_path, &to_path)
                .await?;
            // The move landed; an empty target goes away now, so a failed
            // move leaves both directories as they were. Its directory on
            // disk stays: the moved children land in it, and an empty
            // directory holds no tree entry (trees keep only dirs with
            // children), so a target of the ref needs no deletion note.
            if let Some(to_id) = Self::id_of(&nodes, Some(&to_path)).filter(|id| *id != from_id) {
                drop_listed(&mut nodes, to_dirid, to_filename.as_ref());
                nodes.drop_node(to_id);
            }
            drop_listed(&mut nodes, from_dirid, from_filename.as_ref());
            nodes.drop_node(from_id);
            // The new directory is of the overlay; its children are its
            // changes.
            let new_id = nodes.add(Some(to_path.clone()), Kind::LocalDir, false);
            self.forget_delete(&mut nodes, &to_path);
            note_child(&mut nodes, to_dirid, to_filename.as_ref(), new_id);
            return Ok(());
        }
        // The target, if it is there, goes away.
        if let Some(to_id) =
            Self::id_of(&nodes, Some(&to_path)).filter(|_| !nodes.deleted.contains(&to_path))
        {
            if nodes.by_id[&to_id].kind.is_dir() {
                return Err(nfsstat3::NFS3ERR_NOTEMPTY);
            }
            if matches!(
                nodes.by_id[&to_id].kind,
                Kind::Local { .. } | Kind::Link { .. }
            ) {
                std::fs::remove_file(self.overlay_file(&to_path))
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            }
            if nodes.by_id[&to_id].from_ref {
                self.note_delete(&mut nodes, &to_path);
            }
            drop_listed(&mut nodes, to_dirid, to_filename.as_ref());
            nodes.drop_node(to_id);
        }
        // The bytes move to the new overlay path.
        let bytes = self.file_bytes(&nodes, from_id).await?;
        let to_file = self.overlay_file(&to_path);
        let is_link = matches!(
            nodes.by_id[&from_id].kind,
            Kind::Link { .. } | Kind::StoreLink { .. }
        );
        if is_link {
            symlink_bytes(&bytes, &to_file).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        } else {
            let exec = match &nodes.by_id[&from_id].kind {
                Kind::Store { exec, .. } | Kind::Local { exec } => *exec,
                _ => false,
            };
            write_file(&to_file, &bytes, exec).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        }
        if nodes.by_id[&from_id].from_ref {
            self.note_delete(&mut nodes, &from_path);
        }
        if matches!(
            nodes.by_id[&from_id].kind,
            Kind::Local { .. } | Kind::Link { .. }
        ) {
            std::fs::remove_file(self.overlay_file(&from_path))
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        }
        drop_listed(&mut nodes, from_dirid, from_filename.as_ref());
        nodes.drop_node(from_id);
        // The change is a new file, or a new symlink.
        let kind = if is_link {
            Kind::Link { target: bytes }
        } else {
            Kind::Local { exec: false }
        };
        let new_id = nodes.add(Some(to_path.clone()), kind, false);
        self.forget_delete(&mut nodes, &to_path);
        note_child(&mut nodes, to_dirid, to_filename.as_ref(), new_id);
        Ok(())
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let mut nodes = self.nodes.write().await;
        self.ensure_listed(&mut nodes, dirid).await?;
        let entries = self.merged_listing(&mut nodes, dirid).await?;
        let total = entries.len();
        let begin = if start_after == 0 {
            0
        } else {
            entries
                .iter()
                .position(|(_, id)| *id == start_after)
                .map_or(0, |at| at + 1)
        };
        let mut out = Vec::new();
        for (name, id) in entries.into_iter().skip(begin).take(max_entries) {
            let attr = self.attr_of(&nodes, id)?;
            out.push(DirEntry {
                fileid: id,
                name: filename3::from(name),
                attr,
            });
        }
        let end = begin + out.len() >= total;
        Ok(ReadDirResult { entries: out, end })
    }

    #[cfg(not(feature = "unstable-writable-mount"))]
    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    #[cfg(feature = "unstable-writable-mount")]
    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.want_write()?;
        let mut nodes = self.nodes.write().await;
        let dir_path = self.ensure_listed(&mut nodes, dirid).await?;
        let path = Self::child(dir_path.as_ref(), linkname.as_ref())?;
        let target = symlink.as_ref().to_vec();
        symlink_bytes(&target, &self.overlay_file(&path)).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let id = nodes.add(Some(path.clone()), Kind::Link { target }, false);
        self.forget_delete(&mut nodes, &path);
        note_child(&mut nodes, dirid, linkname.as_ref(), id);
        let size = match &nodes.by_id[&id].kind {
            Kind::Link { target } => target.len() as u64,
            _ => 0,
        };
        Ok((id, self.attr(ftype3::NF3LNK, 0o777, size, id)))
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        let nodes = self.nodes.read().await;
        let data = self.file_bytes(&nodes, id).await?;
        Ok(nfspath3::from(data))
    }

    fn id_to_fh(&self, id: fileid3) -> nfs_fh3 {
        let mut data = self.secret.to_vec();
        data.extend_from_slice(&id.to_le_bytes());
        nfs_fh3 { data }
    }

    fn fh_to_id(&self, fh: &nfs_fh3) -> Result<fileid3, nfsstat3> {
        if fh.data.len() == 24 && fh.data[..16] == self.secret {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&fh.data[16..]);
            return Ok(u64::from_le_bytes(bytes));
        }
        Err(nfsstat3::NFS3ERR_STALE)
    }

    async fn path_to_id(&self, path: &[u8]) -> Result<fileid3, nfsstat3> {
        let parts: Vec<&[u8]> = path
            .split(|&b| b == b'/')
            .filter(|part| !part.is_empty())
            .collect();
        let Some((name, rest)) = parts.split_first() else {
            return Err(nfsstat3::NFS3ERR_ACCES);
        };
        if self.secret_name() != String::from_utf8_lossy(name) {
            return Err(nfsstat3::NFS3ERR_ACCES);
        }
        let mut id = ROOT_ID;
        for part in rest {
            id = self.lookup(id, &filename3::from(part.to_vec())).await?;
        }
        Ok(id)
    }
}

impl Vfs {
    /// The attributes of the node `id`.
    fn attr_of(&self, nodes: &Nodes, id: u64) -> Result<fattr3, nfsstat3> {
        let node = nodes.by_id.get(&id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        match &node.kind {
            Kind::Dir { .. } | Kind::LocalDir => Ok(self.attr(ftype3::NF3DIR, 0o755, 4096, id)),
            Kind::Store { exec, len, .. } => {
                Ok(self.attr(ftype3::NF3REG, file_mode(0o644, *exec), *len, id))
            }
            Kind::Local { .. } => {
                // The file may have been chmodded: take the real mode.
                let file = self.overlay_file(node.path.as_ref().expect("a file has a path"));
                let meta = std::fs::metadata(&file).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                let exec = meta.permissions().mode() & 0o111 != 0;
                Ok(self.attr(ftype3::NF3REG, file_mode(0o644, exec), meta.len(), id))
            }
            Kind::StoreLink { len, .. } => Ok(self.attr(ftype3::NF3LNK, 0o777, *len, id)),
            Kind::Link { target } => Ok(self.attr(ftype3::NF3LNK, 0o777, target.len() as u64, id)),
        }
    }

    /// The NFS attributes: the mount time for the times, since nothing
    /// keeps the real ones.
    fn attr(&self, ftype: ftype3, mode: u32, size: u64, id: u64) -> fattr3 {
        let when = self.nfstime();
        fattr3 {
            ftype,
            mode,
            nlink: if matches!(ftype, ftype3::NF3DIR) {
                2
            } else {
                1
            },
            uid: self.uid,
            gid: self.gid,
            size,
            used: size,
            rdev: specdata3::default(),
            fsid: 1,
            fileid: id,
            atime: when,
            mtime: when,
            ctime: when,
        }
    }

    fn nfstime(&self) -> nfstime3 {
        let since = self.started.duration_since(UNIX_EPOCH).unwrap_or_default();
        nfstime3 {
            seconds: u32::try_from(since.as_secs()).unwrap_or(u32::MAX),
            nseconds: since.subsec_nanos(),
        }
    }

    #[cfg(feature = "unstable-writable-mount")]
    /// The path is in the overlay again: take it off the note of deletions.
    fn forget_delete(&self, nodes: &mut Nodes, path: &RepoPath) {
        if nodes.deleted.remove(path)
            && let Err(e) = write_deletes(&self.overlay, &nodes.deleted)
        {
            warn("cannot rewrite the note of deletions", &e);
        }
    }

    #[cfg(feature = "unstable-writable-mount")]
    /// The first write of a file of the ref: its blob goes to the overlay.
    /// Keeps the executable bit.
    async fn materialize(&self, nodes: &mut Nodes, id: u64) -> Result<(), nfsstat3> {
        let (blob, path, exec, len) = {
            let node = nodes.by_id.get(&id).ok_or(nfsstat3::NFS3ERR_STALE)?;
            match &node.kind {
                Kind::Store {
                    blob, exec, len, ..
                } => (
                    *blob,
                    node.path.clone().expect("a file has a path"),
                    *exec,
                    *len,
                ),
                Kind::Local { .. } => return Ok(()),
                Kind::Dir { .. } | Kind::LocalDir => return Err(nfsstat3::NFS3ERR_ISDIR),
                Kind::Link { .. } | Kind::StoreLink { .. } => {
                    return Err(nfsstat3::NFS3ERR_PERM);
                }
            }
        };
        let bytes = self
            .fetch_blob(blob, Some(&path), len)
            .await
            .map_err(io_err)?;
        write_file(&self.overlay_file(&path), &bytes, exec).map_err(|e| {
            warn("cannot write a file of the mount", &e);
            nfsstat3::NFS3ERR_IO
        })?;
        let node = nodes.by_id.get_mut(&id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        node.kind = Kind::Local { exec };
        Ok(())
    }

    /// Reads the bytes of the node `id`: the overlay file, the blob of the
    /// ref, or the target of a symlink.
    async fn file_bytes(&self, nodes: &Nodes, id: u64) -> Result<Vec<u8>, nfsstat3> {
        let node = nodes.by_id.get(&id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        match &node.kind {
            Kind::Store { blob, len, .. } | Kind::StoreLink { blob, len } => {
                let bytes = self
                    .fetch_blob(*blob, node.path.as_ref(), *len)
                    .await
                    .map_err(io_err)?;
                if bytes.len() as u64 != *len {
                    let why = format!(
                        "{}: {} bytes, the tree says {len}",
                        node.path.as_ref().map_or_else(
                            || "<unnamed>".to_owned(),
                            |p| String::from_utf8_lossy(p.as_bytes()).to_string()
                        ),
                        bytes.len()
                    );
                    warn("a blob does not match the size of its tree entry", &why);
                    return Err(nfsstat3::NFS3ERR_IO);
                }
                Ok(bytes.to_vec())
            }
            Kind::Local { .. } => {
                let file = self.overlay_file(node.path.as_ref().expect("a file has a path"));
                std::fs::read(&file).map_err(|_| nfsstat3::NFS3ERR_IO)
            }
            Kind::Link { target } => Ok(target.clone()),
            Kind::Dir { .. } | Kind::LocalDir => Err(nfsstat3::NFS3ERR_ISDIR),
        }
    }

    #[cfg(feature = "unstable-writable-mount")]
    /// Notes in the overlay that the path of the ref is deleted.
    fn note_delete(&self, nodes: &mut Nodes, path: &RepoPath) {
        if nodes.deleted.insert(path.clone())
            && let Err(e) = write_deletes(&self.overlay, &nodes.deleted)
        {
            warn("cannot record a deletion", &e);
        }
    }
}

/// The version byte of a deletions note. Version 1 holds raw paths: one
/// `u32` little-endian count, then per path a `u32` length and the bytes.
/// The note of a build older than this one starts with `[`: a JSON array
/// of lossy names (the old format lost non-UTF-8 names at write time; it
/// is read the best it can).
const DELETES_VERSION_1: u8 = 1;

#[cfg(feature = "unstable-writable-mount")]
/// Writes the note of the paths that the ref held and the mount deleted:
/// a temp file, an fsync, and a rename, so a crash mid-write leaves the
/// old note whole, not a half-written one.
fn write_deletes(overlay: &Path, deleted: &HashSet<RepoPath>) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut bytes = Vec::with_capacity(1 + 4 + deleted.len() * 8);
    bytes.push(DELETES_VERSION_1);
    bytes.extend_from_slice(
        &u32::try_from(deleted.len())
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "too many deletions")
            })?
            .to_le_bytes(),
    );
    for path in deleted {
        bytes.extend_from_slice(
            &(u32::try_from(path.as_bytes().len())
                .expect("a path fits")
                .to_le_bytes()),
        );
        bytes.extend_from_slice(path.as_bytes());
    }
    let tmp = overlay.join(".deletes.tmp");
    // The temp note is made like every overlay write: no symlink at its
    // path is followed. The rename then replaces whatever is there.
    let mut file = open_overlay_write(&tmp, true)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&tmp, overlay.join(DELETES))?;
    // The rename's directory entry is as durable as the note: a crash
    // that loses it brings back the old note, and with it every
    // deletion the mount made.
    std::fs::File::open(overlay)?.sync_all()
}

/// Reads the note of deletions back, for `scn commit --from-mount`. A
/// note that is not there holds none. A damaged note is an error, not
/// "no deletions": a commit that reads a broken note as empty would
/// keep every path the mount deleted.
pub(crate) fn read_deletes(overlay: &Path) -> anyhow::Result<Vec<RepoPath>> {
    let bytes = match std::fs::read(overlay.join(DELETES)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(anyhow::anyhow!(
                "cannot read the note of deletions ({e}): re-make the \
                 deletions on the mount, or run `scn unmount DIR` and \
                 mount again"
            ));
        }
    };
    if bytes.first() == Some(&b'[') {
        // A note of the old format: a JSON array of lossy names.
        let names: Vec<String> =
            serde_json::from_slice(&bytes).map_err(|e| broken_note(e.into()))?;
        return names
            .into_iter()
            .map(|name| {
                RepoPath::new(name.into_bytes())
                    .map_err(|_| broken_note(anyhow!("a name of the note is not a path")))
            })
            .collect();
    }
    if bytes.first() != Some(&DELETES_VERSION_1) {
        return Err(broken_note(anyhow!(
            "the note holds no deletions format this build reads"
        )));
    }
    let mut at = 1usize;
    let mut take = |want: usize| -> anyhow::Result<&[u8]> {
        if bytes.len() < at + want {
            return Err(broken_note(anyhow!("the note ends in the middle")));
        }
        let head = &bytes[at..at + want];
        at += want;
        Ok(head)
    };
    // The count is not trusted for the size of the vector: a damaged
    // note can name millions of entries. Each entry costs at least four
    // bytes, so a count the note cannot pay for is a damaged note, and
    // a count the bytes outlive is one too.
    let count = u32::from_le_bytes(take(4)?.try_into().expect("four bytes")) as usize;
    if count > (bytes.len() - 1) / 4 {
        return Err(broken_note(anyhow!(
            "the note names more deletions than it holds"
        )));
    }
    let mut deleted = Vec::with_capacity(count);
    for _ in 0..count {
        let len = u32::from_le_bytes(take(4)?.try_into().expect("four bytes")) as usize;
        let path = RepoPath::new(take(len)?)
            .map_err(|_| broken_note(anyhow!("the note holds a name that is not a path")));
        deleted.push(path?);
    }
    if at != bytes.len() {
        return Err(broken_note(anyhow!(
            "the note holds bytes past its last deletion"
        )));
    }
    Ok(deleted)
}

/// The error for a deletions note this build cannot read.
fn broken_note(why: anyhow::Error) -> anyhow::Error {
    why.context(
        "the note of deletions in the mount's overlay is damaged; \
         re-make the deletions on the mount, or run `scn unmount DIR` \
         and mount again",
    )
}

#[cfg(feature = "unstable-writable-mount")]
/// Records `name` in the listing of the directory `dirid`, if it is loaded.
fn note_child(nodes: &mut Nodes, dirid: u64, name: &[u8], id: u64) {
    if let Some(node) = nodes.by_id.get_mut(&dirid)
        && let Kind::Dir {
            listed: Some(listed),
            ..
        } = &mut node.kind
    {
        listed.insert(name.to_vec(), id);
    }
}

#[cfg(feature = "unstable-writable-mount")]
/// Takes `name` off the listing of the directory `dirid`.
fn drop_listed(nodes: &mut Nodes, dirid: u64, name: &[u8]) {
    if let Some(node) = nodes.by_id.get_mut(&dirid)
        && let Kind::Dir {
            listed: Some(listed),
            ..
        } = &mut node.kind
    {
        listed.remove(name);
    }
}

/// The mode of a file: `0o644` with the executable bit when `exec`.
fn file_mode(base: u32, exec: bool) -> u32 {
    if exec { base | 0o111 } else { base }
}

/// Maps a fetch error: warn once, and answer I/O error.
// SHORTCUT: takes the error by value so `.map_err(io_err)` reads clean.
#[allow(clippy::needless_pass_by_value)]
fn io_err(e: anyhow::Error) -> nfsstat3 {
    eprintln!("scn mount: the mount lost its objects: {e:#}");
    nfsstat3::NFS3ERR_IO
}

/// Prints `why` and the error once, on stderr: the mount keeps serving.
fn warn<E: std::fmt::Display>(what: &str, error: &E) {
    eprintln!("scn mount: {what}: {error}");
}

#[cfg(feature = "unstable-writable-mount")]
/// Writes `bytes` to `file` under the overlay, with the mode of `exec`.
fn write_file(file: &Path, bytes: &[u8], exec: bool) -> std::io::Result<()> {
    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let made = open_overlay_write(file, true)?;
    (&made).write_all(bytes)?;
    made.set_permissions(std::fs::Permissions::from_mode(file_mode(0o644, exec)))
}

#[cfg(feature = "unstable-writable-mount")]
/// Opens an overlay file for the VFS to write, and refuses a symlink at
/// its path: the mount is reachable by anyone who can read the mount
/// table, so the VFS never writes through an OS symlink a client could
/// have planted there. SHORTCUT: the final name is checked only; a
/// planted symlink one level up still redirects. Upgrade: open every
/// parent through handles, as `materialize.rs` does.
fn open_overlay_write(file: &Path, create: bool) -> std::io::Result<std::fs::File> {
    let mut flags = OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW;
    if create {
        flags |= OFlags::CREATE | OFlags::TRUNC;
    }
    rustix::fs::open(file, flags, Mode::from_raw_mode(0o644))
        .map(std::fs::File::from)
        .map_err(std::io::Error::from)
}

#[cfg(feature = "unstable-writable-mount")]
/// Opens an overlay file without following a symlink, for a mode
/// change: a read-only handle is enough for `fchmod`, and a read-only
/// open needs no write bit.
fn open_overlay_chmod(file: &Path) -> std::io::Result<std::fs::File> {
    rustix::fs::open(
        file,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map(std::fs::File::from)
    .map_err(std::io::Error::from)
}

#[cfg(feature = "unstable-writable-mount")]
/// Makes a symlink that names `target`.
fn symlink_bytes(target: &[u8], file: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;

    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::os::unix::fs::symlink(std::ffi::OsStr::from_bytes(target), file)
}

/// The mount secret, from the system's source of random bytes.
fn secret_from_urandom() -> [u8; 16] {
    let mut secret = [0u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut secret))
        .expect("cannot read /dev/urandom");
    secret
}

impl Vfs {
    #[cfg(feature = "unstable-writable-mount")]
    /// Whether the directory `id` (`path`) holds nothing: no overlay child
    /// and no listed child of the tree. The tree of a directory that was
    /// never listed is fetched first, so the answer is about the real
    /// contents.
    async fn dir_is_empty(
        &self,
        nodes: &mut Nodes,
        id: u64,
        path: &RepoPath,
    ) -> Result<bool, nfsstat3> {
        if let Some(node) = nodes.by_id.get(&id)
            && matches!(node.kind, Kind::Dir { listed: None, .. })
        {
            self.list_dir(nodes, id).await?;
        }
        let overlay_child = nodes.by_path.iter().any(|(p, _)| {
            p.as_ref()
                .is_some_and(|p| Self::parent(p).as_ref() == Some(path))
        });
        let tree_child = match nodes.by_id.get(&id) {
            Some(Node {
                kind:
                    Kind::Dir {
                        listed: Some(listed),
                        ..
                    },
                ..
            }) => !listed.is_empty(),
            _ => false,
        };
        Ok(!overlay_child && !tree_child)
    }

    #[cfg(feature = "unstable-writable-mount")]
    /// Moves the directory `from_id` (`from_path`) to `to_path`. Boxed:
    /// the walk recurses.
    fn move_dir<'a>(
        &'a self,
        nodes: &'a mut Nodes,
        dir_id: u64,
        from: &'a RepoPath,
        to: &'a RepoPath,
    ) -> Pin<Box<dyn Future<Output = Result<(), nfsstat3>> + Send + 'a>> {
        Box::pin(self.move_dir_inner(nodes, dir_id, from, to))
    }

    #[cfg(feature = "unstable-writable-mount")]
    /// Moves the directory `from_id` (`from_path`) to `to_path`: each file
    /// and symlink under it, of the ref or of the overlay, is written to
    /// the new overlay path, and its old path is a deletion. A directory
    /// of the ref whose files were never read is read in full.
    async fn move_dir_inner(
        &self,
        nodes: &mut Nodes,
        dir_id: u64,
        from: &RepoPath,
        to: &RepoPath,
    ) -> Result<(), nfsstat3> {
        // Load this level, then take the names: the moves below change the
        // maps the listing lives in.
        self.list_dir(nodes, dir_id).await?;
        let mut children: Vec<(Vec<u8>, u64)> = match nodes.by_id.get(&dir_id) {
            Some(Node {
                kind:
                    Kind::Dir {
                        listed: Some(listed),
                        ..
                    },
                ..
            }) => listed
                .iter()
                .map(|(name, id)| (name.clone(), *id))
                .collect(),
            _ => Vec::new(),
        };
        for (path, id) in &nodes.by_path {
            let Some(path) = path else { continue };
            if Self::parent(path).as_ref() != Some(from) || nodes.deleted.contains(path) {
                continue;
            }
            let name = path.as_bytes()[from.as_bytes().len() + 1..].to_vec();
            if !children.iter().any(|(other, _)| *other == name) {
                children.push((name, *id));
            }
        }
        children.sort();
        for (name, id) in children {
            let child_from = Self::child(Some(from), &name)?;
            let child_to = Self::child(Some(to), &name)?;
            let kind = nodes.by_id[&id].kind.is_dir();
            if kind {
                self.move_dir(nodes, id, &child_from, &child_to).await?;
                nodes.drop_node(id);
            } else {
                let bytes = self.file_bytes(nodes, id).await?;
                let is_link = matches!(
                    nodes.by_id[&id].kind,
                    Kind::Link { .. } | Kind::StoreLink { .. }
                );
                if is_link {
                    symlink_bytes(&bytes, &self.overlay_file(&child_to))
                        .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                } else {
                    let exec = match &nodes.by_id[&id].kind {
                        Kind::Store { exec, .. } | Kind::Local { exec } => *exec,
                        _ => false,
                    };
                    write_file(&self.overlay_file(&child_to), &bytes, exec)
                        .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                }
                if nodes.by_id[&id].from_ref {
                    self.note_delete(nodes, &child_from);
                } else {
                    let _ = std::fs::remove_file(self.overlay_file(&child_from));
                }
                if matches!(
                    nodes.by_id[&id].kind,
                    Kind::Local { .. } | Kind::Link { .. }
                ) {
                    let _ = std::fs::remove_file(self.overlay_file(&child_from));
                }
                nodes.drop_node(id);
                let new_kind = if is_link {
                    Kind::Link { target: bytes }
                } else {
                    Kind::Local { exec: false }
                };
                let new_id = nodes.add(Some(child_to.clone()), new_kind, false);
                self.forget_delete(nodes, &child_to);
                if let Some(new_dir_id) = Self::id_of(nodes, Some(to)) {
                    note_child(nodes, new_dir_id, &name, new_id);
                }
            }
        }
        // The old directory's name goes from every listing, but it is not
        // a deletion: a changeset deletes files, and this was a directory.
        nodes.deleted.insert(from.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_core::{Changeset, FileEntry, FileState, IdentityTime, PathElem};
    use scion_proto::runtime::listen::Limits;
    use scion_proto::v1::objects_server::{Objects, ObjectsServer};
    use scion_proto::v1::{
        GetRootTreeRequest as Ask, GetRootTreeResponse, ObjectPart, ResolveRefRequest,
        ResolveRefResponse,
    };
    use scion_server::test_support::meta::{Meta as _, MetaTx as _};
    use scion_server::{
        Backend,
        test_support::{service, tree},
    };
    use std::collections::HashMap as StdHashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status, transport::Server};
    const REPO: &str = "r";
    const TOKEN: &str = "secret";

    /// A server with a repo whose `main` holds `files`, and a mount of it.
    struct Test {
        vfs: Vfs,
        #[allow(dead_code)]
        // Keeps the server's and the overlay's directories alive.
        dirs: [TempDir; 2],
    }

    /// Starts a server, lands `files` as `main`, and mounts the ref.
    /// Starts a server, lands `files` as `main`, and mounts the ref.
    /// A file is `Some(exec)` for a regular file, `None` for a symlink
    /// whose blob holds the target bytes.
    async fn start(files: &[(&str, &[u8], Option<bool>)]) -> Test {
        let data = tempfile::tempdir().unwrap();
        let overlay = tempfile::tempdir().unwrap();
        let store = Backend::open_test(&data.path().join("store")).unwrap();
        let mut contents = Vec::new();
        let mut lens = tree::lens_of(&store);
        let base = store
            .write(|c| {
                let repo = c.create_repo(REPO)?;
                let mut blobs = store.blobs().batch();
                for (_path, bytes, exec) in files {
                    let content = blobs.put(bytes).unwrap();
                    contents.push(match exec {
                        Some(exec) => FileState::Regular {
                            content,
                            executable: *exec,
                        },
                        None => FileState::Symlink { target: content },
                    });
                }
                drop(blobs);
                let changes: Vec<(RepoPath, Option<FileState>)> = files
                    .iter()
                    .zip(&contents)
                    .map(|((path, _, _), state)| (RepoPath::new(*path).unwrap(), Some(*state)))
                    .collect();
                let cs = Changeset::new(
                    vec![],
                    IdentityTime::new(
                        b"Ada".to_vec(),
                        b"ada@example.com".to_vec(),
                        1_700_000_000,
                        0,
                    )
                    .expect("identity fits the format"),
                    None,
                    b"test\n".to_vec(),
                    changes.into_iter().collect(),
                )
                .expect("changeset fits the format");
                let (id, _) = tree::store_changeset(c, repo, &cs, &mut lens)?;
                let version = c.get_ref(repo, "main")?.map(|r| r.version);
                c.cas_ref(repo, "main", version, id)?;
                Ok(id)
            })
            .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(service::serve(
            store.clone(),
            listener,
            TOKEN,
            service::ServeOptions {
                data: data.path().to_path_buf(),
                audit: scion_server::test_support::audit::Audit::off(),
                limits: Limits::default(),
                quotas: scion_server::Quotas::off(),
                pending_lands: None,
                land_stale_secs: 1 << 40,
                tls: None,
                metrics: std::sync::Arc::new(scion_server::Metrics::default()),
                reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
            },
            Some(std::sync::Arc::new(scion_server::Keys::generate())),
            std::future::pending(),
        ));
        let client = crate::client::test_objects(&url, TOKEN).await;
        // The server of the mount: the client that mints fresh tickets.
        let roots = crate::client::test_objects(&url, TOKEN).await;
        let reply = client
            .clone()
            .get_root_tree(GetRootTreeRequest {
                repo: REPO.to_owned(),
                changeset_id: base.as_bytes().to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        let root = id_from(&reply.tree_id).unwrap();
        let vfs = Vfs::new(
            client,
            roots,
            REPO.to_owned(),
            base,
            root,
            &reply.ticket,
            overlay.path().to_path_buf(),
        );
        // The unit tests of the VFS carry writes: a `--unstable-writable`
        // mount is what they exercise. A build without the feature has no
        // write path; its tests here are the read-side ones.
        #[cfg(feature = "unstable-writable-mount")]
        let vfs = vfs.writable(true);
        Test {
            vfs,
            dirs: [data, overlay],
        }
    }

    /// Looks up `path` from the root.
    async fn find(vfs: &Vfs, path: &str) -> Result<fileid3, nfsstat3> {
        let mut id = NFSFileSystem::root_dir(vfs);
        for part in path.split('/') {
            id = vfs
                .lookup(id, &filename3::from(part.as_bytes().to_vec()))
                .await?;
        }
        Ok(id)
    }

    #[tokio::test]
    async fn lists_and_reads_a_file_of_the_ref() {
        let test = start(&[
            ("README.md", b"hello\n", Some(false)),
            ("src/lib.rs", b"fn f() {}\n", Some(true)),
        ])
        .await;
        let root = NFSFileSystem::root_dir(&test.vfs);
        let listed = test.vfs.readdir(root, 0, 10).await.unwrap();
        let names: Vec<Vec<u8>> = listed.entries.iter().map(|e| e.name.0.clone()).collect();
        assert_eq!(names, [b"README.md".to_vec(), b"src".to_vec()]);

        let id = find(&test.vfs, "README.md").await.unwrap();
        let (data, eof) = test.vfs.read(id, 0, 1000).await.unwrap();
        assert_eq!(data, b"hello\n");
        assert!(eof);
        // Reading part of the file, from an offset.
        let (data, eof) = test.vfs.read(id, 2, 2).await.unwrap();
        assert_eq!(data, b"ll");
        assert!(!eof);
        let attr = test.vfs.getattr(id).await.unwrap();
        assert!(matches!(attr.ftype, ftype3::NF3REG));
        assert_eq!(attr.size, 6);
        // The blob is on the server's disk now.
        let blob = test.vfs.nodes.read().await.by_id[&id].path.clone().unwrap();
        let _ = blob;
    }

    #[tokio::test]
    async fn reads_a_file_only_when_it_is_needed() {
        // A lookup fetches the tree of the directory, but no blob; the
        // blob comes with the first read, and the cache answers the second
        // one. (A listing fetches sizes too: nfsserve's readdir carries
        // attributes, and until #172 a size comes from the blob.)
        let test = start(&[("a", b"data\n", Some(false))]).await;
        let root = NFSFileSystem::root_dir(&test.vfs);
        let id = test
            .vfs
            .lookup(root, &filename3::from(b"a".to_vec()))
            .await
            .unwrap();
        let cached = || async { test.vfs.blobs.lock().await.0.len() };
        assert_eq!(cached().await, 0, "the blob was fetched before any read");
        let (data, _) = test.vfs.read(id, 0, 1000).await.unwrap();
        assert_eq!(data, b"data\n");
        assert_eq!(cached().await, 1, "the read did not fetch the blob");
        let (data, _) = test.vfs.read(id, 0, 1000).await.unwrap();
        assert_eq!(data, b"data\n");
        assert_eq!(cached().await, 1, "the second read fetched the blob again");
    }

    #[tokio::test]
    async fn a_read_at_a_huge_offset_does_not_panic() {
        let test = start(&[("a", b"data\n", Some(false))]).await;
        let root = NFSFileSystem::root_dir(&test.vfs);
        let id = test
            .vfs
            .lookup(root, &filename3::from(b"a".to_vec()))
            .await
            .unwrap();
        // A client offset near the top of the range must not overflow.
        let (data, eof) = test.vfs.read(id, u64::MAX, 16).await.unwrap();
        assert_eq!(data, b"");
        assert!(eof);
        let (data, eof) = test.vfs.read(id, 3, u32::MAX).await.unwrap();
        assert_eq!(data, b"a\n");
        assert!(eof);
    }

    #[tokio::test]
    async fn a_wrong_secret_is_refused() {
        let test = start(&[("a", b"data\n", Some(false))]).await;
        // The mount path must name the secret.
        let refused = test.vfs.path_to_id(b"/no-such-secret").await.unwrap_err();
        assert!(matches!(refused, nfsstat3::NFS3ERR_ACCES));
        let empty = test.vfs.path_to_id(b"/").await.unwrap_err();
        assert!(matches!(empty, nfsstat3::NFS3ERR_ACCES));
        // The right secret reaches the root.
        let path = format!("/{}", test.vfs.secret_name());
        let id = test.vfs.path_to_id(path.as_bytes()).await.unwrap();
        assert_eq!(id, NFSFileSystem::root_dir(&test.vfs));
        // A handle with the wrong secret is stale.
        let wrong = nfs_fh3 {
            data: vec![0_u8; 24],
        };
        let stale = test.vfs.fh_to_id(&wrong).unwrap_err();
        assert!(matches!(stale, nfsstat3::NFS3ERR_STALE));
        // The right secret round-trips a handle.
        let id = test.vfs.path_to_id(&path.into_bytes()).await.unwrap();
        let fh = test.vfs.id_to_fh(id);
        assert_eq!(test.vfs.fh_to_id(&fh).unwrap(), id);
    }
    #[cfg(feature = "unstable-writable-mount")]
    #[tokio::test]
    async fn a_write_lands_in_the_overlay() {
        let test = start(&[("README.md", b"hello\n", Some(false))]).await;
        let root = NFSFileSystem::root_dir(&test.vfs);
        // Change a file of the ref: the kernel truncates first, then writes.
        let id = find(&test.vfs, "README.md").await.unwrap();
        let _ = test
            .vfs
            .setattr(
                id,
                sattr3 {
                    size: set_size3::size(0),
                    ..sattr3::default()
                },
            )
            .await
            .unwrap();
        let attr = test.vfs.write(id, 0, b"bye\n").await.unwrap();
        assert_eq!(attr.size, 4);
        let file = test
            .vfs
            .overlay_file(&test.vfs.nodes.read().await.by_id[&id].path.clone().unwrap());
        assert_eq!(std::fs::read(&file).unwrap(), b"bye\n");
        // A new file, and a directory for it.
        let (dir, _) = test
            .vfs
            .mkdir(root, &filename3::from(b"docs".to_vec()))
            .await
            .unwrap();
        let (new_id, _) = test
            .vfs
            .create(dir, &filename3::from(b"note".to_vec()), sattr3::default())
            .await
            .unwrap();
        test.vfs.write(new_id, 0, b"hi\n").await.unwrap();
        assert_eq!(
            std::fs::read(test.vfs.overlay_file(&RepoPath::new("docs/note").unwrap())).unwrap(),
            b"hi\n"
        );
        // Removing a file of the ref records the deletion.
        test.vfs
            .remove(root, &filename3::from(b"README.md".to_vec()))
            .await
            .unwrap();
        assert!(find(&test.vfs, "README.md").await.is_err());
        assert_eq!(
            read_deletes(&test.vfs.overlay).unwrap(),
            [RepoPath::new("README.md").unwrap()]
        );
    }

    /// A symlink planted at an overlay path redirects nothing: the mount
    /// is reachable by anyone who can read the mount table, so the VFS
    #[cfg(feature = "unstable-writable-mount")]
    #[tokio::test]
    async fn a_planted_symlink_is_never_written_through() {
        let test = start(&[("README.md", b"hello\n", Some(false))]).await;
        let id = find(&test.vfs, "README.md").await.unwrap();
        // The overlay file exists only once the VFS materializes it.
        let _ = test
            .vfs
            .setattr(
                id,
                sattr3 {
                    mode: set_mode3::mode(0o644),
                    ..sattr3::default()
                },
            )
            .await
            .unwrap();
        let file = test
            .vfs
            .overlay_file(&test.vfs.nodes.read().await.by_id[&id].path.clone().unwrap());
        let outside = tempfile::tempdir().unwrap();
        let target = outside.path().join("outside");
        std::fs::write(&target, b"keep\n").unwrap();
        std::fs::remove_file(&file).unwrap();
        std::os::unix::fs::symlink(&target, &file).unwrap();

        // A write is refused, and the target keeps its bytes.
        assert!(matches!(
            test.vfs.write(id, 0, b"planted\n").await,
            Err(nfsstat3::NFS3ERR_IO)
        ));
        assert_eq!(std::fs::read(&target).unwrap(), b"keep\n");
        // So are a mode change and a truncate.
        assert!(matches!(
            test.vfs
                .setattr(
                    id,
                    sattr3 {
                        mode: set_mode3::mode(0o755),
                        ..sattr3::default()
                    },
                )
                .await,
            Err(nfsstat3::NFS3ERR_IO)
        ));
        assert!(matches!(
            test.vfs
                .setattr(
                    id,
                    sattr3 {
                        size: set_size3::size(0),
                        ..sattr3::default()
                    },
                )
                .await,
            Err(nfsstat3::NFS3ERR_IO)
        ));
        assert_eq!(std::fs::read(&target).unwrap(), b"keep\n");

        // The note of deletions refuses a planted symlink: no temp note
        // is made, and the symlink's target keeps its bytes. Without the
        // symlink, the note lands, and the rename replaces nothing.
        let note = tempfile::tempdir().unwrap();
        let outside = note.path().join("outside");
        std::fs::write(&outside, b"keep\n").unwrap();
        std::os::unix::fs::symlink(&outside, note.path().join(".deletes.tmp")).unwrap();
        let mut deleted = std::collections::HashSet::new();
        deleted.insert(RepoPath::new("gone.txt").unwrap());
        assert!(write_deletes(note.path(), &deleted).is_err());
        assert_eq!(std::fs::read(&outside).unwrap(), b"keep\n");
        std::fs::remove_file(note.path().join(".deletes.tmp")).unwrap();
        write_deletes(note.path(), &deleted).unwrap();
        assert_eq!(
            read_deletes(note.path()).unwrap(),
            [RepoPath::new("gone.txt").unwrap()]
        );
    }
    #[cfg(feature = "unstable-writable-mount")]
    #[tokio::test]
    async fn a_symlink_round_trips() {
        // The ref holds a symlink too, so its blob is read on `readlink`.
        let test = start(&[("run", b"", Some(true)), ("ref-link", b"run", None)]).await;
        let root = NFSFileSystem::root_dir(&test.vfs);
        let ref_id = find(&test.vfs, "ref-link").await.unwrap();
        let ref_target = test.vfs.readlink(ref_id).await.unwrap();
        assert_eq!(ref_target.as_ref(), b"run");
        let (_, attr) = test
            .vfs
            .symlink(
                root,
                &filename3::from(b"latest".to_vec()),
                &nfspath3::from(b"run".to_vec()),
                &sattr3::default(),
            )
            .await
            .unwrap();
        assert!(matches!(attr.ftype, ftype3::NF3LNK));
        let id = find(&test.vfs, "latest").await.unwrap();
        let target = test.vfs.readlink(id).await.unwrap();
        assert_eq!(target.as_ref(), b"run");
        // The overlay holds a real symlink with the target.
        let file = test.vfs.overlay_file(&RepoPath::new("latest").unwrap());
        assert_eq!(
            std::fs::read_link(&file).unwrap(),
            std::path::Path::new("run")
        );
    }

    /// The target of a directory rename must be a directory, and empty
    /// at most ([F1 of the hunt]: a rename once merged two directories).
    /// Fails on main, which merges the target's children in.
    #[cfg(feature = "unstable-writable-mount")]
    #[tokio::test]
    async fn a_rename_of_a_dir_over_a_non_empty_dir_is_refused() {
        let test = start(&[("x/a", b"ax\n", Some(false)), ("y/b", b"by\n", Some(false))]).await;
        let root = NFSFileSystem::root_dir(&test.vfs);
        test.vfs
            .rename(
                root,
                &filename3::from(b"x".to_vec()),
                root,
                &filename3::from(b"y".to_vec()),
            )
            .await
            .unwrap_err();
        // Both directories are as they were.
        assert!(find(&test.vfs, "x/a").await.is_ok());
        assert!(find(&test.vfs, "y/b").await.is_ok());
        // The shape of the refusal: the same rename answers NOTEMPTY.
        let from = filename3::from(b"x".to_vec());
        let refused = test
            .vfs
            .rename(root, &from, root, &filename3::from(b"y".to_vec()))
            .await
            .unwrap_err();
        assert!(matches!(refused, nfsstat3::NFS3ERR_NOTEMPTY));
    }

    /// A rename of a directory over an existing file is not a directory
    #[cfg(feature = "unstable-writable-mount")]
    #[tokio::test]
    async fn a_rename_of_a_dir_over_a_file_is_refused() {
        let test = start(&[("d/f", b"df\n", Some(false)), ("f", b"fx\n", Some(false))]).await;
        let root = NFSFileSystem::root_dir(&test.vfs);
        let refused = test
            .vfs
            .rename(
                root,
                &filename3::from(b"d".to_vec()),
                root,
                &filename3::from(b"f".to_vec()),
            )
            .await
            .unwrap_err();
        assert!(matches!(refused, nfsstat3::NFS3ERR_NOTDIR));
    }

    /// A rename of a directory into its own subtree is invalid ([F2]).
    /// Fails on main, which accepts it and loses the mover's children at
    /// the commit.
    #[cfg(feature = "unstable-writable-mount")]
    #[tokio::test]
    async fn a_rename_of_a_dir_into_itself_is_refused() {
        let test = start(&[("a/b", b"b\n", Some(false)), ("a/c", b"c\n", Some(false))]).await;
        let a = find(&test.vfs, "a").await.unwrap();
        let refused = test
            .vfs
            .rename(
                NFSFileSystem::root_dir(&test.vfs),
                &filename3::from(b"a".to_vec()),
                a,
                &filename3::from(b"b".to_vec()),
            )
            .await
            .unwrap_err();
        assert!(matches!(refused, nfsstat3::NFS3ERR_INVAL));
    }

    #[cfg(feature = "unstable-writable-mount")]
    #[tokio::test]
    async fn a_rename_of_a_dir_over_an_empty_local_dir_replaces_it() {
        let test = start(&[("x/a", b"ax\n", Some(false))]).await;
        let root = NFSFileSystem::root_dir(&test.vfs);
        test.vfs
            .mkdir(root, &filename3::from(b"y".to_vec()))
            .await
            .unwrap();
        test.vfs
            .rename(
                NFSFileSystem::root_dir(&test.vfs),
                &filename3::from(b"x".to_vec()),
                NFSFileSystem::root_dir(&test.vfs),
                &filename3::from(b"y".to_vec()),
            )
            .await
            .unwrap();
        assert!(find(&test.vfs, "x").await.is_err());
        let y = find(&test.vfs, "y").await.unwrap();
        let listed = test.vfs.readdir(y, 0, 10).await.unwrap();
        let names: Vec<Vec<u8>> = listed.entries.iter().map(|e| e.name.0.clone()).collect();
        assert_eq!(names, [b"a".to_vec()]);
    }

    /// A damaged note of deletions is an error, not "no deletions" ([F5]):
    /// a commit that read one as empty would keep every path the mount
    /// deleted.
    #[test]
    fn a_damaged_note_of_deletions_is_an_error() {
        let overlay = tempfile::tempdir().unwrap();
        // A note that is not there holds none.
        assert_eq!(read_deletes(overlay.path()).unwrap(), Vec::new());
        // The old format still reads: a note of an older build.
        std::fs::write(overlay.path().join(DELETES), r#"["gone"]"#).unwrap();
        assert_eq!(
            read_deletes(overlay.path()).unwrap(),
            [RepoPath::new("gone").unwrap()]
        );
        // A truncated note of the new format refuses.
        let mut bytes = vec![DELETES_VERSION_1];
        bytes.extend_from_slice(&7u32.to_le_bytes());
        std::fs::write(overlay.path().join(DELETES), &bytes).unwrap();
        let why = read_deletes(overlay.path()).unwrap_err().to_string();
        assert!(why.contains("damaged"), "{why}");
    }

    /// The note holds raw bytes, so a name that is not UTF-8 comes back
    /// as itself ([F6]). The lossy names of main would delete the wrong
    #[cfg(feature = "unstable-writable-mount")]
    #[test]
    fn a_note_of_a_name_that_is_not_utf8_round_trips() {
        let overlay = tempfile::tempdir().unwrap();
        let mut deleted = std::collections::HashSet::new();
        deleted.insert(RepoPath::new(b"bad\xffname".as_slice()).unwrap());
        write_deletes(overlay.path(), &deleted).unwrap();
        assert_eq!(
            read_deletes(overlay.path()).unwrap(),
            deleted.into_iter().collect::<Vec<_>>()
        );
    }

    #[cfg(feature = "unstable-writable-mount")]
    #[tokio::test]
    async fn a_rename_moves_the_overlay_file() {
        let test = start(&[("old", b"same\n", Some(false))]).await;
        let root = NFSFileSystem::root_dir(&test.vfs);
        let from = filename3::from(b"old".to_vec());
        let id = find(&test.vfs, "old").await.unwrap();
        // Touch the file, so the overlay holds it.
        test.vfs.write(id, 5, b"\n").await.unwrap();
        test.vfs
            .rename(root, &from, root, &filename3::from(b"new".to_vec()))
            .await
            .unwrap();
        assert!(find(&test.vfs, "old").await.is_err());
        let new_id = find(&test.vfs, "new").await.unwrap();
        let (data, _) = test.vfs.read(new_id, 0, 100).await.unwrap();
        assert_eq!(data, b"same\n\n");
        // The old path is a deletion, the new one a change; the old
        // overlay file is gone.
        assert_eq!(
            read_deletes(&test.vfs.overlay).unwrap(),
            [RepoPath::new("old").unwrap()]
        );
        assert!(
            test.vfs
                .overlay_file(&RepoPath::new("new").unwrap())
                .exists()
        );
        assert!(
            !test
                .vfs
                .overlay_file(&RepoPath::new("old").unwrap())
                .exists()
        );
    }

    /// The attributes of a ticket near and past its expiry: fresh only
    /// with more than the clock-skew window left, and an unparsed ticket
    /// counts as expired.
    #[test]
    fn a_ticket_near_its_expiry_is_not_fresh() {
        let grant = Grant {
            bytes: Vec::new(),
            expires: 1_000,
        };
        assert!(grant.fresh(1_000 - TICKET_SKEW_SECS - 1));
        assert!(!grant.fresh(1_000 - TICKET_SKEW_SECS));
        assert!(!grant.fresh(1_000));
        assert!(!grant.fresh(2_000));
        assert!(!Grant::of(b"not a ticket").fresh(0));
    }

    /// An object as the fake server sends it: RAW, one part.
    struct Sent {
        kind: i32,
        id: Vec<u8>,
        data: Vec<u8>,
    }

    impl Sent {
        /// The parts of the object: its header, then its bytes.
        fn parts(&self) -> Vec<Result<ObjectPart, Status>> {
            let header = Header {
                kind: ObjectKind::try_from(self.kind).unwrap(),
                id: self.id.clone().try_into().unwrap(),
                encoding: Encoding::Raw,
                encoded_size: self.data.len(),
                size: self.data.len(),
            };
            header
                .parts(object::read_chunks(&self.data[..], self.data.len()).unwrap())
                .into_iter()
                .map(Ok)
                .collect::<Vec<Result<ObjectPart, Status>>>()
        }
    }

    /// An objects server whose first ticket is refused: a `GetObjects`
    /// that carries it ends permission-denied with `ticket expired`, and
    /// every ticket minted after works. It counts its mints, so the test
    /// can see the refresh.
    struct Expiring {
        root: TreeId,
        objects: StdHashMap<Vec<u8>, Sent>,
        key: std::sync::Arc<scion_server::Keys>,
        /// The ticket of the first mint, which `get_objects` refuses.
        first: std::sync::Mutex<Vec<u8>>,
        /// Shared with the test, which reads it while the server runs.
        mints: std::sync::Arc<AtomicUsize>,
    }

    impl Expiring {
        /// One file `a` of one byte, under the root tree.
        fn new() -> Self {
            let blob = Sent {
                kind: ObjectKind::Blob.into(),
                id: BlobId::of(b"x\n").as_bytes().to_vec(),
                data: b"x\n".to_vec(),
            };
            let tree = Tree::new(
                [(
                    PathElem::new("a").unwrap(),
                    Entry::File(FileEntry::new(
                        FileState::Regular {
                            content: BlobId::try_from(blob.id.as_slice()).unwrap(),
                            executable: false,
                        },
                        blob.data.len() as u64,
                    )),
                )]
                .into(),
            )
            .expect("tree fits the format");
            let (data, root) = tree.encode_with_id();
            let tree = Sent {
                kind: ObjectKind::Tree.into(),
                id: root.as_bytes().to_vec(),
                data,
            };
            Self {
                root,
                objects: [(blob.id.clone(), blob), (tree.id.clone(), tree)]
                    .into_iter()
                    .collect(),
                key: std::sync::Arc::new(scion_server::Keys::generate()),
                first: std::sync::Mutex::new(Vec::new()),
                mints: std::sync::Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[tonic::async_trait]
    impl Objects for Expiring {
        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            Err(Status::unimplemented("a mount reads by ID"))
        }

        async fn get_root_tree(
            &self,
            _: Request<Ask>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            let mint = self.mints.fetch_add(1, Ordering::SeqCst) + 1;
            // The mint in the principal: two tickets of one second are
            // otherwise the same bytes, and the fake could not tell the
            // first one from a fresh one.
            let ticket = self
                .key
                .sign(REPO, &format!("default-{mint}"), *self.root.as_bytes())
                .map_err(Status::internal)
                .unwrap();
            if mint == 1 {
                *self.first.lock().unwrap() = ticket.clone();
            }
            Ok(Response::new(GetRootTreeResponse {
                tree_id: self.root.as_bytes().to_vec(),
                ticket,
            }))
        }

        type GetObjectsStream =
            std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<ObjectPart, Status>> + Send>>;

        async fn get_objects(
            &self,
            request: Request<GetObjectsRequest>,
        ) -> Result<Response<Self::GetObjectsStream>, Status> {
            let GetObjectsRequest { refs, ticket, .. } = request.into_inner();
            if ticket == *self.first.lock().unwrap() {
                return Err(Status::permission_denied("bad ticket: ticket expired"));
            }
            let parts = refs
                .iter()
                .flat_map(|r| self.objects[&r.id].parts())
                .collect::<Vec<Result<ObjectPart, Status>>>();
            Ok(Response::new(Box::pin(tokio_stream::iter(parts))))
        }
    }

    /// A mount that lives past the life of its first ticket keeps reading:
    /// the refusal mints a fresh ticket over the mounted changeset, once,
    /// and the read is asked again with it. The next read mints no more.
    #[tokio::test]
    async fn a_mount_past_its_ticket_keeps_reading() {
        let fake = Expiring::new();
        let mints = fake.mints.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(fake))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let objects = crate::client::test_objects(&url, TOKEN).await;
        let roots = crate::client::test_objects(&url, TOKEN).await;
        let reply = objects
            .clone()
            .get_root_tree(Ask {
                repo: REPO.to_owned(),
                changeset_id: [7u8; 32].to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        let root = id_from(&reply.tree_id).unwrap();
        let overlay = tempfile::tempdir().unwrap();
        let vfs = Vfs::new(
            objects,
            roots,
            REPO.to_owned(),
            ChangesetId::from_bytes([7u8; 32]),
            root,
            &reply.ticket,
            overlay.path().to_path_buf(),
        );
        // The first read: the ticket it was minted is refused, so the
        // mount mints again and reads with the fresh one.
        let id = vfs
            .lookup(
                NFSFileSystem::root_dir(&vfs),
                &filename3::from(b"a".to_vec()),
            )
            .await
            .unwrap();
        let (data, eof) = vfs.read(id, 0, 1000).await.unwrap();
        assert_eq!(data, b"x\n");
        assert!(eof);
        assert_eq!(mints.load(Ordering::SeqCst), 2);
        // The second read mints no more: the fresh ticket works.
        let (data, _) = vfs.read(id, 0, 1000).await.unwrap();
        assert_eq!(data, b"x\n");
        assert_eq!(mints.load(Ordering::SeqCst), 2);
    }
}
