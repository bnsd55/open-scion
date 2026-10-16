//! A virtual checkout over `NFSv3`: `scn mount` serves one ref of the
//! repository as a filesystem, so files load when they are first read
//! ([#11](https://github.com/bnsd55/open-scion/issues/11)), through the
//! same `Objects` calls that a checkout sends.
//!
//! What the article says: files load when they are first read. The rest is
//! this project's choice: the NFS server is `nfsserve`, mounted with the
//! operating system's NFS client; a directory's tree is fetched when the
//! directory is first listed or looked up in; a file's blob when the file
//! is first read or sized. Writes go to an overlay directory beside the
//! mount, which is the change list that `scn commit --from-mount` commits.
//!
//! // SHORTCUT: a file's size comes from its blob, so the first stat of a
//! // file that was never read fetches the blob, and `ls -l` of a
//! // directory fetches every blob in it. Upgrade, after #13 PR-C: a
//! // `GetSizes` call, served from the stored frame's header. The one
//! // place to change is `size_of`.
//!
//! No protection: see `docs/architecture.md`, section 8. Any local user who
//! can read the mount table can reach the mounted content through the NFS
//! port, and root always can. The mount secret and the secret in every file
//! handle stop stray and accidental callers only.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read as _, Write as _};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, anyhow, bail};
use async_trait::async_trait;
use hashlink::LinkedHashMap;
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfs_fh3, nfspath3, nfsstat3, nfstime3, sattr3, set_mode3,
    set_size3, specdata3,
};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use scion_core::{BlobId, Entry, FileState, RepoPath, Tree, TreeId};
use scion_proto::object::{self, Assembler, Header};
use scion_proto::v1::{Encoding, GetObjectsRequest, ObjectKind, ObjectRef};

use crate::client::{Objects, id_from};
use crate::materialize::check_name;

/// The name of the file that the commit reads the deletions from.
pub const DELETES: &str = "deletes.json";

/// The subdirectory of the overlay that mirrors the changed paths.
pub const CHANGES: &str = "tree";

/// The bytes of decoded blobs kept in memory, least recently used first.
/// The daemon holds the stored form on disk; this cache saves the unpacking
/// of a file that is read again.
const BLOB_CACHE_BYTES: usize = 128 << 20;

/// The handle of the root directory. 0 is reserved in NFS.
const ROOT_ID: u64 = 1;

/// The bytes of the blobs that are in memory, least recently used first,
/// and their bytes together.
type Blobs = tokio::sync::Mutex<(LinkedHashMap<BlobId, Arc<Vec<u8>>>, usize)>;

/// One node: a directory, a regular file or a symlink, of the ref or of the
/// overlay.
struct Node {
    /// None for the root directory.
    path: Option<RepoPath>,
    kind: Kind,
    /// Whether the ref names this path, so removing it is a deletion.
    from_ref: bool,
}

enum Kind {
    /// A directory. Its tree is fetched once, when it is first listed or
    /// looked up in; `listed` then names each entry's handle.
    Dir {
        tree: Option<TreeId>,
        listed: Option<BTreeMap<Vec<u8>, u64>>,
    },
    /// A directory made on the mount. It holds nothing of the ref.
    LocalDir,
    /// A regular file of the ref.
    Store { blob: BlobId, exec: bool },
    /// A regular file of the overlay: its bytes are the file.
    Local { exec: bool },
    /// A symlink of the ref: the blob that holds the target bytes.
    StoreLink { blob: BlobId },
    /// A symlink made on the mount.
    Link { target: Vec<u8> },
}

impl Kind {
    fn is_dir(&self) -> bool {
        matches!(self, Kind::Dir { .. } | Kind::LocalDir)
    }
}

/// A change that `scn commit --from-mount` commits for one path.
pub enum Change {
    /// The file at the overlay path replaces the path, or adds it.
    Put { exec: bool },
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
    fn add(&mut self, path: Option<RepoPath>, kind: Kind, from_ref: bool) -> u64 {
        let id = self.next;
        self.next += 1;
        self.by_path.insert(path.clone(), id);
        self.by_id.insert(
            id,
            Node {
                path,
                kind,
                from_ref,
            },
        );
        id
    }

    /// Drops the node `id`, and its name from its directory's listing.
    /// Drops the node `id`, and its name from the map of paths. A deletion
    /// note stays: a re-created path takes it off (`forget_delete`).
    fn drop_node(&mut self, id: u64) {
        if let Some(node) = self.by_id.remove(&id) {
            self.by_path.remove(&node.path);
        }
    }
}

/// The NFS file system of one mount.
pub struct Vfs {
    objects: Objects,
    repo: String,
    nodes: tokio::sync::RwLock<Nodes>,
    /// Decoded blobs, least recently used first, with their bytes together.
    /// The daemon holds the stored form on disk; this saves the unpacking
    /// of a file that is read again.
    blobs: Blobs,
    /// Where the changes of this mount live.
    overlay: PathBuf,
    /// The secret that a mount path names and every file handle carries.
    /// Not a protection: see section 8 of the docs.
    secret: [u8; 16],
    started: SystemTime,
    uid: u32,
    gid: u32,
}

impl Vfs {
    /// A virtual checkout of the changeset whose root tree is `root`, of
    /// `repo`. The objects come from `objects`, which may be a daemon.
    /// Changes go to `overlay`.
    pub fn new(objects: Objects, repo: String, root: TreeId, overlay: PathBuf) -> Self {
        Self {
            objects,
            repo,
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
        }
    }

    /// The mount secret, as the file-name part of a mount path.
    pub fn secret_name(&self) -> String {
        self.secret.iter().fold(String::new(), |mut all, b| {
            use std::fmt::Write as _;
            let _ = write!(all, "{b:02x}");
            all
        })
    }

    /// The path of `path` under the overlay directory.
    fn overlay_file(&self, path: &RepoPath) -> PathBuf {
        let mut file = self.overlay.clone();
        for part in path.as_bytes().split(|&b| b == b'/') {
            file.push(String::from_utf8_lossy(part).as_ref());
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
    fn add(nodes: &mut Nodes, path: Option<RepoPath>, kind: Kind, from_ref: bool) -> u64 {
        let id = nodes.next;
        nodes.next += 1;
        nodes.by_path.insert(path.clone(), id);
        nodes.by_id.insert(
            id,
            Node {
                path,
                kind,
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

    /// One object of `kind`, checked, in its plain form.
    async fn fetch_object(&self, kind: ObjectKind, id: [u8; 32]) -> Result<Vec<u8>> {
        let mut objects = self.objects.clone();
        let refs = vec![ObjectRef {
            kind: i32::from(kind),
            id: id.to_vec(),
        }];
        let mut stream = objects
            .get_objects(GetObjectsRequest {
                repo: self.repo.clone(),
                refs,
                accept: vec![Encoding::Zstd.into()],
            })
            .await
            .context("cannot fetch objects")?
            .into_inner();
        let first = stream
            .message()
            .await
            .context("cannot fetch objects")?
            .context("the reply holds no object")?;
        if first.kind != i32::from(kind) || id_from::<[u8; 32]>(&first.id)? != id {
            bail!("the server sent an object that was not asked for");
        }
        let bad = |why| anyhow!("the server sent a bad object: {why}");
        let mut assembled = Assembler::new(Header::of(&first, true).map_err(bad)?, first);
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

    /// The tree `id`, checked.
    async fn fetch_tree(&self, id: TreeId) -> Result<Tree> {
        let data = self
            .fetch_object(ObjectKind::Tree, *id.as_bytes())
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
    async fn fetch_blob(&self, id: BlobId) -> Result<Arc<Vec<u8>>> {
        let mut blobs = self.blobs.lock().await;
        if let Some(data) = blobs.0.get_mut(&id) {
            return Ok(Arc::clone(data));
        }
        let raw = self
            .fetch_object(ObjectKind::Blob, *id.as_bytes())
            .await
            .with_context(|| format!("cannot read blob {id}"))?;
        if BlobId::of(&raw) != id {
            bail!("the server sent other bytes than what {id} names");
        }
        let data = Arc::new(raw);
        blobs.1 += data.len();
        blobs.0.insert(id, Arc::clone(&data));
        while blobs.1 > BLOB_CACHE_BYTES {
            let bytes = blobs.0.front().map_or(0, |(_, data)| data.len());
            blobs.0.pop_front();
            blobs.1 = blobs.1.saturating_sub(bytes);
        }
        Ok(data)
    }

    /// // SHORTCUT: the size comes from the blob, so a stat of a file that
    /// was never read fetches the blob. After #13 PR-C, a `GetSizes` call
    /// served from the stored frame's header replaces this.
    async fn size_of(&self, blob: BlobId) -> Result<u64, nfsstat3> {
        Ok(self.fetch_blob(blob).await.map_err(io_err)?.len() as u64)
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
            let tree = self.fetch_tree(tree).await.map_err(|e| {
                warn("cannot read a tree of the mount", &e);
                nfsstat3::NFS3ERR_IO
            })?;
            for (name, entry) in tree.entries {
                let path = Self::child(dir_path.as_ref(), name.as_bytes())?;
                if nodes.deleted.contains(&path) || nodes.by_path.contains_key(&Some(path.clone()))
                {
                    continue;
                }
                let kind = match entry {
                    Entry::Dir(tree) => Kind::Dir {
                        tree: Some(tree),
                        listed: None,
                    },
                    Entry::File(FileState::Regular {
                        content,
                        executable,
                    }) => Kind::Store {
                        blob: content,
                        exec: executable,
                    },
                    Entry::File(FileState::Symlink { target }) => Kind::StoreLink { blob: target },
                    // A gitlink names a commit of another repository, not
                    // an object of this one.
                    Entry::File(FileState::Gitlink { .. }) => continue,
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
        VFSCapabilities::ReadWrite
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
        self.attr_of(&nodes, id).await
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
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
            std::fs::set_permissions(
                &file,
                std::fs::Permissions::from_mode(file_mode(0o644, exec)),
            )
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            if let Some(Node {
                kind: Kind::Local { exec: slot },
                ..
            }) = nodes.by_id.get_mut(&id)
            {
                *slot = exec;
            }
        }
        if let set_size3::size(bytes) = setattr.size {
            std::fs::File::create(&file).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&file)
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            f.set_len(bytes).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        }
        self.attr_of(&nodes, id).await
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let nodes = self.nodes.read().await;
        let data = self.file_bytes(&nodes, id).await?;
        let from = usize::try_from(offset.min(data.len() as u64)).unwrap_or(usize::MAX);
        let to = usize::try_from((offset + u64::from(count)).min(data.len() as u64))
            .unwrap_or(usize::MAX);
        Ok((data[from..to].to_vec(), to >= data.len()))
    }

    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        use std::io::Seek as _;

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
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file)
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        file.seek(std::io::SeekFrom::Start(offset))
            .and_then(|_| file.write_all(data))
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let meta = file.metadata().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let exec = meta.permissions().mode() & 0o111 != 0;
        Ok(self.attr(ftype3::NF3REG, file_mode(0o644, exec), meta.len(), id))
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
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

    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
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

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
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

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
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

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
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
        match &nodes.by_id[&from_id].kind {
            Kind::Dir { .. } | Kind::LocalDir => return Err(nfsstat3::NFS3ERR_NOTSUPP),
            _ => {}
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
            let attr = self.attr_of(&nodes, id).await?;
            out.push(DirEntry {
                fileid: id,
                name: filename3::from(name),
                attr,
            });
        }
        let end = begin + out.len() >= total;
        Ok(ReadDirResult { entries: out, end })
    }

    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
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
    async fn attr_of(&self, nodes: &Nodes, id: u64) -> Result<fattr3, nfsstat3> {
        let node = nodes.by_id.get(&id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        match &node.kind {
            Kind::Dir { .. } | Kind::LocalDir => Ok(self.attr(ftype3::NF3DIR, 0o755, 4096, id)),
            Kind::Store { blob, exec } => {
                let size = self.size_of(*blob).await?;
                Ok(self.attr(ftype3::NF3REG, file_mode(0o644, *exec), size, id))
            }
            Kind::Local { .. } => {
                // The file may have been chmodded: take the real mode.
                let file = self.overlay_file(node.path.as_ref().expect("a file has a path"));
                let meta = std::fs::metadata(&file).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                let exec = meta.permissions().mode() & 0o111 != 0;
                Ok(self.attr(ftype3::NF3REG, file_mode(0o644, exec), meta.len(), id))
            }
            Kind::StoreLink { blob } => {
                let size = self.size_of(*blob).await?;
                Ok(self.attr(ftype3::NF3LNK, 0o777, size, id))
            }
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

    /// The path is in the overlay again: take it off the note of deletions.
    fn forget_delete(&self, nodes: &mut Nodes, path: &RepoPath) {
        if nodes.deleted.remove(path)
            && let Err(e) = write_deletes(&self.overlay, &nodes.deleted)
        {
            warn("cannot rewrite the note of deletions", &e);
        }
    }

    /// The first write of a file of the ref: its blob goes to the overlay.
    /// Keeps the executable bit.
    async fn materialize(&self, nodes: &mut Nodes, id: u64) -> Result<(), nfsstat3> {
        let (blob, path, exec) = {
            let node = nodes.by_id.get(&id).ok_or(nfsstat3::NFS3ERR_STALE)?;
            match &node.kind {
                Kind::Store { blob, exec } => {
                    (*blob, node.path.clone().expect("a file has a path"), *exec)
                }
                Kind::Local { .. } => return Ok(()),
                Kind::Dir { .. } | Kind::LocalDir => return Err(nfsstat3::NFS3ERR_ISDIR),
                Kind::Link { .. } | Kind::StoreLink { .. } => {
                    return Err(nfsstat3::NFS3ERR_PERM);
                }
            }
        };
        let bytes = self.fetch_blob(blob).await.map_err(io_err)?;
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
            Kind::Store { blob, .. } | Kind::StoreLink { blob } => {
                Ok(self.fetch_blob(*blob).await.map_err(io_err)?.to_vec())
            }
            Kind::Local { .. } => {
                let file = self.overlay_file(node.path.as_ref().expect("a file has a path"));
                std::fs::read(&file).map_err(|_| nfsstat3::NFS3ERR_IO)
            }
            Kind::Link { target } => Ok(target.clone()),
            Kind::Dir { .. } | Kind::LocalDir => Err(nfsstat3::NFS3ERR_ISDIR),
        }
    }

    /// Notes in the overlay that the path of the ref is deleted.
    fn note_delete(&self, nodes: &mut Nodes, path: &RepoPath) {
        if nodes.deleted.insert(path.clone())
            && let Err(e) = write_deletes(&self.overlay, &nodes.deleted)
        {
            warn("cannot record a deletion", &e);
        }
    }
}

/// The note of the paths that the ref held and the mount deleted.
fn write_deletes(overlay: &Path, deleted: &HashSet<RepoPath>) -> std::io::Result<()> {
    let names: Vec<String> = deleted
        .iter()
        .map(|p| String::from_utf8_lossy(p.as_bytes()).into_owned())
        .collect();
    let file = std::fs::File::create(overlay.join(DELETES))?;
    serde_json::to_writer(std::io::BufWriter::new(file), &names)?;
    Ok(())
}

/// Reads the note of deletions back, for `scn commit --from-mount`.
pub fn read_deletes(overlay: &Path) -> Vec<String> {
    std::fs::read_to_string(overlay.join(DELETES))
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

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

/// Writes `bytes` to `file` under the overlay, with the mode of `exec`.
fn write_file(file: &Path, bytes: &[u8], exec: bool) -> std::io::Result<()> {
    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(file, bytes)?;
    std::fs::set_permissions(
        file,
        std::fs::Permissions::from_mode(file_mode(0o644, exec)),
    )
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use scion_core::{Changeset, FileState, IdentityTime};
    use scion_proto::listen::Limits;
    use scion_proto::v1::GetRootTreeRequest;
    use scion_server::{Store, db, service, tree};
    use tempfile::TempDir;
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
        let store = Store::open(&data.path().join("store")).unwrap();
        let mut contents = Vec::new();
        let base = store
            .write(|c| {
                let repo = db::create_repo(c, REPO)?;
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
                let cs = Changeset {
                    parents: vec![],
                    author: IdentityTime {
                        name: b"Ada".to_vec(),
                        email: b"ada@example.com".to_vec(),
                        unix_secs: 1_700_000_000,
                        tz_offset_minutes: 0,
                    },
                    committer: None,
                    message: b"test\n".to_vec(),
                    changes: changes.into_iter().collect(),
                };
                let (id, _) = tree::store_changeset(c, repo, &cs)?;
                let version = db::get_ref(c, repo, "main")?.map(|r| r.version);
                db::cas_ref(c, repo, "main", version, id)?;
                Ok(id)
            })
            .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(service::serve(
            store.clone(),
            listener,
            TOKEN,
            Limits::default(),
            None,
            std::future::pending(),
        ));
        let client = crate::client::test_objects(&url, TOKEN).await;
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
        let vfs = Vfs::new(client, REPO.to_owned(), root, overlay.path().to_path_buf());
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
        // attributes, and until #13 PR-C a size comes from the blob.)
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
        let note = std::fs::read_to_string(test.vfs.overlay.join(DELETES)).unwrap();
        assert_eq!(note, r#"["README.md"]"#);
    }

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
        let note = std::fs::read_to_string(test.vfs.overlay.join(DELETES)).unwrap();
        assert_eq!(note, r#"["old"]"#);
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
}
