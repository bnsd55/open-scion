//! Object model and canonical byte format for Scion.
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
//! ```
//! use std::collections::BTreeMap;
//! use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};
//!
//! let readme = BlobId::of(b"# hello\n");
//! let cs = Changeset {
//!     parents: vec![],
//!     author: IdentityTime {
//!         name: b"Ada".to_vec(),
//!         email: b"ada@example.com".to_vec(),
//!         unix_secs: 1_700_000_000,
//!         tz_offset_minutes: 0,
//!     },
//!     committer: None,
//!     message: b"init\n".to_vec(),
//!     changes: BTreeMap::from([(
//!         RepoPath::new("README.md")?,
//!         Some(FileState::Regular { content: readme, executable: false }),
//!     )]),
//! };
//! let bytes = cs.encode();
//! assert_eq!(Changeset::decode(&bytes)?, cs);
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

/// One entry in a [`Tree`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Entry {
    /// A file, symlink or gitlink.
    File(FileState),
    /// A subdirectory.
    Dir(TreeId),
}

/// One directory level of a snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tree {
    /// Entries by name, sorted by raw name bytes.
    pub entries: BTreeMap<PathElem, Entry>,
}
