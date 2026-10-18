//! Object model and canonical byte format for Scion.
//!
//! This is the lowest layer of the system: the objects that every other
//! crate of [open-scion] stores, sends and checks. A server keeps them, a
//! tier caches their bytes, and the client ([`scn`]) builds and reads
//! them; all of them speak this one format, so they stay interoperable.
//!
//! [open-scion]: https://github.com/bnsd55/open-scion
//! [`scn`]: https://docs.rs/open-scion
//!
//! Scion stores two kinds of structured objects, both content-addressed:
//!
//! - [`Changeset`]: one commit, stored as the files that changed relative to
//!   its first parent.
//! - [`Tree`]: one directory level of a full snapshot.
//!
//! File contents are addressed by [`BlobId`]. Every ID is a BLAKE3 hash of the
//! object's canonical encoding under a per-kind domain prefix, so the byte
//! format is a frozen contract. It is specified in `docs/format-v1.md` and
//! pinned by golden tests.
//!
//! A tree is a map of names to entries; encoding it yields the bytes whose
//! hash is its [`TreeId`], and decoding those bytes yields the same tree:
//!
//! ```
//! use std::collections::BTreeMap;
//! use scion_core::{BlobId, Entry, FileEntry, FileState, PathElem, Tree, TreeId};
//!
//! let tree = Tree {
//!     entries: BTreeMap::from([
//!         (
//!             PathElem::new("README.md")?,
//!             Entry::File(FileEntry::new(
//!                 FileState::Regular {
//!                     content: BlobId::of(b"# hello\n"),
//!                     executable: false,
//!                 },
//!                 b"# hello\n".len() as u64,
//!             )),
//!         ),
//!         (PathElem::new("src")?, Entry::Dir(TreeId::from_bytes([7; 32]))),
//!     ]),
//! };
//! let (bytes, id) = tree.encode_with_id();
//! assert_eq!(Tree::decode_with_id(&bytes)?.1, id);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod codec;
mod id;
mod path;

use std::collections::BTreeMap;

pub use codec::DecodeError;
pub use id::{BlobHasher, BlobId, ChangesetId, ParseIdError, TreeId};
pub use path::{PathElem, PathError, RepoPath};

/// A person and the moment they acted, as recorded on a changeset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityTime {
    /// Display name, raw bytes.
    pub name: Vec<u8>,
    /// Email address, raw bytes.
    pub email: Vec<u8>,
    /// Seconds since the Unix epoch.
    pub unix_secs: i64,
    /// Offset from UTC, in minutes. `+05:30` is `330`.
    pub tz_offset_minutes: i32,
}

/// A git object name, tagged with its hash algorithm.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GitOid {
    /// SHA-1 object name.
    Sha1([u8; 20]),
    /// SHA-256 object name.
    Sha256([u8; 32]),
}

impl GitOid {
    /// The raw object name bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Sha1(oid) => oid,
            Self::Sha256(oid) => oid,
        }
    }
}

/// Lowercase hex, as git prints it.
impl std::fmt::Display for GitOid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_bytes()
            .iter()
            .try_for_each(|b| write!(f, "{b:02x}"))
    }
}

/// What a path holds when it is not a directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileState {
    /// A regular file.
    Regular {
        /// The file content.
        content: BlobId,
        /// Whether the file has the executable bit.
        executable: bool,
    },
    /// A symbolic link.
    Symlink {
        /// Blob that holds the link target bytes.
        target: BlobId,
    },
    /// A git submodule pointer. It has no blob.
    Gitlink {
        /// The submodule commit.
        commit: GitOid,
    },
}

impl FileState {
    /// The blob this state points at. A gitlink has none.
    #[must_use]
    pub fn blob(&self) -> Option<BlobId> {
        match self {
            Self::Regular { content: blob, .. } | Self::Symlink { target: blob } => Some(*blob),
            Self::Gitlink { .. } => None,
        }
    }
}

/// One commit, stored as the files that changed relative to its first parent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changeset {
    /// Parents, in order. `parents[0]` is the diff base. No parents means a root.
    pub parents: Vec<ChangesetId>,
    /// Who wrote the change.
    pub author: IdentityTime,
    /// Who committed it, when different from the author. Set only by git import.
    pub committer: Option<IdentityTime>,
    /// Commit message, raw bytes.
    pub message: Vec<u8>,
    /// Paths that differ from `parents[0]`. `None` deletes the path.
    ///
    /// A map keeps paths sorted and unique, so every value of this type has
    /// exactly one encoding.
    pub changes: BTreeMap<RepoPath, Option<FileState>>,
}

/// A file entry of a tree: what the path holds, and the byte length of
/// the blob that backs it. The length is the file's size for a regular
/// entry and the link target's length for a symlink; it is part of the
/// tree's ID, so it is as trustworthy as the tree. A gitlink names no
/// blob: its `len` is 0 and its encoding carries no length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileEntry {
    /// What the path holds.
    state: FileState,
    /// Byte length of the blob `state` names, 0 for a gitlink.
    len: u64,
}

impl FileEntry {
    /// Wraps a state that names a blob, with that blob's byte length. A
    /// gitlink has no blob, so it wraps as a plain entry of length 0.
    #[must_use]
    pub fn new(state: FileState, len: u64) -> Self {
        let len = if state.blob().is_some() { len } else { 0 };
        Self { state, len }
    }

    /// What the path holds.
    #[must_use]
    pub fn state(&self) -> FileState {
        self.state
    }

    /// Byte length of the blob `state` names, 0 for a gitlink. A file's
    /// size: what a stat of the file answers.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.len
    }

    /// The length the tree encodes: `len` for a blob-backed state, and
    /// nothing at all for a gitlink, whose payload ends at the commit.
    pub(crate) fn encoded_len(&self) -> Option<u64> {
        self.state.blob().map(|_| self.len)
    }
}

/// One entry in a [`Tree`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Entry {
    /// A file, symlink or gitlink, with the length of its blob.
    File(FileEntry),
    /// A subdirectory.
    Dir(TreeId),
}

/// One directory level of a snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tree {
    /// Entries by name, sorted by raw name bytes.
    pub entries: BTreeMap<PathElem, Entry>,
}
