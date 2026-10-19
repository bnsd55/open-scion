//! v1 encoder and strict decoder. See `docs/format-v1.md`.

use std::collections::BTreeMap;
use std::fmt;

use crate::{
    BlobId, Changeset, ChangesetId, Entry, FileEntry, FileState, GitOid, IdentityTime, PathElem,
    PathError, RepoPath, Tree, TreeId,
};

const VERSION: u8 = 1;

/// The largest length or count the v1 format can carry: every such field
/// is a `u32be`.
pub(crate) const MAX_FIELD: usize = u32::MAX as usize;

/// Why a value cannot be encoded. Constructors refuse values that break a
/// v1 bound, so [`Changeset::encode`] and [`Tree::encode`] never see one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeError {
    /// A field or count is above the v1 format's `u32be` bound.
    FieldTooLong(&'static str),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FieldTooLong(what) => {
                write!(f, "the {what} is above the v1 format's u32::MAX bound")
            }
        }
    }
}

impl std::error::Error for EncodeError {}

/// The boundary the constructors enforce: a length of `cap` fits, one more
/// does not. `cap` is a parameter so tests can pin the boundary without
/// allocating `u32::MAX` bytes.
pub(crate) fn check_fit(len: usize, cap: usize, what: &'static str) -> Result<(), EncodeError> {
    if len <= cap {
        Ok(())
    } else {
        Err(EncodeError::FieldTooLong(what))
    }
}

const TAG_DELETE: u8 = 0;
const TAG_REGULAR: u8 = 1;
const TAG_SYMLINK: u8 = 2;
const TAG_GITLINK: u8 = 3;
const TAG_DIR: u8 = 4;

const GIT_SHA1: u8 = 1;
const GIT_SHA256: u8 = 2;

/// Why bytes are not a canonical v1 object, or break a limit the caller set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The input ends before the object does, or a length points past the end.
    UnexpectedEnd,
    /// Bytes remain after the object.
    TrailingBytes,
    /// The version byte is not 1.
    BadVersion(u8),
    /// An entry tag is unknown here.
    BadTag(u8),
    /// A flag byte is not 0 or 1.
    BadFlag(u8),
    /// A git hash algorithm byte is unknown.
    BadGitAlgo(u8),
    /// Paths or names are out of order, or repeated.
    Unsorted,
    /// A path or name breaks the path rules.
    BadPath(PathError),
    /// The change count is above the caller's limit.
    TooManyChanges(usize),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEnd => f.write_str("object ends early"),
            Self::TrailingBytes => f.write_str("bytes after the end of the object"),
            Self::BadVersion(v) => write!(f, "unknown format version {v}"),
            Self::BadTag(t) => write!(f, "unknown tag {t}"),
            Self::BadFlag(b) => write!(f, "flag byte {b} is not 0 or 1"),
            Self::BadGitAlgo(a) => write!(f, "unknown git hash algorithm {a}"),
            Self::Unsorted => f.write_str("paths are not strictly increasing"),
            Self::BadPath(e) => write!(f, "bad path: {e}"),
            Self::TooManyChanges(max) => write!(f, "more than {max} changes"),
        }
    }
}

impl std::error::Error for DecodeError {}

impl Changeset {
    /// Encodes the changeset in the v1 canonical format.
    ///
    /// # Panics
    ///
    /// If a field is longer than `u32::MAX` bytes or items.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut w = vec![VERSION];
        put_len(&mut w, self.parents.len());
        for parent in &self.parents {
            w.extend_from_slice(parent.as_bytes());
        }
        put_identity(&mut w, &self.author);
        match &self.committer {
            None => w.push(0),
            Some(committer) => {
                w.push(1);
                put_identity(&mut w, committer);
            }
        }
        put_bytes(&mut w, &self.message);
        put_len(&mut w, self.changes.len());
        for (path, change) in &self.changes {
            put_bytes(&mut w, path.as_bytes());
            match change {
                None => w.push(TAG_DELETE),
                Some(file) => put_file(&mut w, file),
            }
        }
        w
    }

    /// Decodes a v1 changeset.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] unless `bytes` is exactly the canonical encoding
    /// of some changeset.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::decode_max_changes(bytes, usize::MAX)
    }

    /// Decodes a v1 changeset and returns its ID, hashing `bytes` directly.
    /// Strict decoding means `bytes` is the canonical encoding, so no
    /// re-encode is needed.
    ///
    /// # Errors
    ///
    /// As [`Changeset::decode`].
    pub fn decode_with_id(bytes: &[u8]) -> Result<(Self, ChangesetId), DecodeError> {
        Ok((Self::decode(bytes)?, ChangesetId::hash(bytes)))
    }

    /// As [`Changeset::decode_with_id`], with at most `max_changes` changes.
    /// The count is checked before any change is read, so a server can bound
    /// the work of one request.
    ///
    /// # Errors
    ///
    /// [`DecodeError::TooManyChanges`] if the change count is above
    /// `max_changes`. Otherwise as [`Changeset::decode`].
    pub fn decode_with_id_max_changes(
        bytes: &[u8],
        max_changes: usize,
    ) -> Result<(Self, ChangesetId), DecodeError> {
        Ok((
            Self::decode_max_changes(bytes, max_changes)?,
            ChangesetId::hash(bytes),
        ))
    }

    fn decode_max_changes(bytes: &[u8], max_changes: usize) -> Result<Self, DecodeError> {
        let mut r = Reader(bytes);
        r.version()?;
        let mut parents = Vec::new();
        for _ in 0..r.u32()? {
            parents.push(ChangesetId::from_bytes(r.array()?));
        }
        let author = r.identity()?;
        let committer = if r.flag()? { Some(r.identity()?) } else { None };
        let message = r.bytes()?.to_vec();
        let n_changes = r.u32()?;
        if n_changes > max_changes {
            return Err(DecodeError::TooManyChanges(max_changes));
        }
        let mut changes = Vec::new();
        for _ in 0..n_changes {
            let path = RepoPath::new(r.bytes()?).map_err(DecodeError::BadPath)?;
            ensure_after(&changes, &path)?;
            let change = match r.u8()? {
                TAG_DELETE => None,
                tag => Some(r.file(tag)?),
            };
            changes.push((path, change));
        }
        r.finish()?;
        Ok(Self {
            parents,
            author,
            committer,
            message,
            changes: BTreeMap::from_iter(changes),
        })
    }

    /// The content address of this changeset.
    #[must_use]
    pub fn id(&self) -> ChangesetId {
        self.encode_with_id().1
    }

    /// The canonical encoding and the ID, from a single encode.
    #[must_use]
    pub fn encode_with_id(&self) -> (Vec<u8>, ChangesetId) {
        let bytes = self.encode();
        let id = ChangesetId::hash(&bytes);
        (bytes, id)
    }
}

impl Tree {
    /// Encodes the tree in the v1 canonical format.
    ///
    /// # Panics
    ///
    /// If a field is longer than `u32::MAX` bytes or items.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut w = vec![VERSION];
        put_len(&mut w, self.entries.len());
        for (name, entry) in &self.entries {
            put_bytes(&mut w, name.as_bytes());
            match entry {
                Entry::File(file) => {
                    put_file(&mut w, &file.state);
                    if let Some(len) = file.encoded_len() {
                        w.extend_from_slice(&len.to_be_bytes());
                    }
                }
                Entry::Dir(id) => {
                    w.push(TAG_DIR);
                    w.extend_from_slice(id.as_bytes());
                }
            }
        }
        w
    }

    /// Decodes a v1 tree.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] unless `bytes` is exactly the canonical encoding
    /// of some tree.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = Reader(bytes);
        r.version()?;
        let mut entries = Vec::new();
        for _ in 0..r.u32()? {
            let name = PathElem::new(r.bytes()?).map_err(DecodeError::BadPath)?;
            ensure_after(&entries, &name)?;
            let tag = r.u8()?;
            let entry = match tag {
                TAG_DIR => Entry::Dir(TreeId::from_bytes(r.array()?)),
                TAG_REGULAR | TAG_SYMLINK => Entry::File(FileEntry {
                    state: r.file(tag)?,
                    len: u64::from_be_bytes(r.array()?),
                }),
                _ => Entry::File(FileEntry {
                    state: r.file(tag)?,
                    len: 0,
                }),
            };
            entries.push((name, entry));
        }
        r.finish()?;
        Ok(Self {
            entries: BTreeMap::from_iter(entries),
        })
    }

    /// Decodes a v1 tree and returns its ID, hashing `bytes` directly. Strict
    /// decoding means `bytes` is the canonical encoding, so no re-encode is
    /// needed.
    ///
    /// # Errors
    ///
    /// As [`Tree::decode`].
    pub fn decode_with_id(bytes: &[u8]) -> Result<(Self, TreeId), DecodeError> {
        Ok((Self::decode(bytes)?, TreeId::hash(bytes)))
    }

    /// The content address of this tree.
    #[must_use]
    pub fn id(&self) -> TreeId {
        self.encode_with_id().1
    }

    /// The canonical encoding and the ID, from a single encode.
    #[must_use]
    pub fn encode_with_id(&self) -> (Vec<u8>, TreeId) {
        let bytes = self.encode();
        let id = TreeId::hash(&bytes);
        (bytes, id)
    }
}

// ---- encode helpers ----

fn put_len(w: &mut Vec<u8>, len: usize) {
    // The types guarantee this: every value is built through a checked
    // constructor or by the decoder, whose lengths come from u32be fields.
    debug_assert!(len <= MAX_FIELD, "object field longer than u32::MAX");
    // A release build that somehow broke the invariant writes u32::MAX
    // rather than a truncated neighbor: the object's ID then fails its
    // check, and no stored byte is silently misread.
    let len = u32::try_from(len).unwrap_or(u32::MAX);
    w.extend_from_slice(&len.to_be_bytes());
}

fn put_bytes(w: &mut Vec<u8>, bytes: &[u8]) {
    put_len(w, bytes.len());
    w.extend_from_slice(bytes);
}

fn put_identity(w: &mut Vec<u8>, who: &IdentityTime) {
    put_bytes(w, &who.name);
    put_bytes(w, &who.email);
    w.extend_from_slice(&who.unix_secs.to_be_bytes());
    w.extend_from_slice(&who.tz_offset_minutes.to_be_bytes());
}

fn put_file(w: &mut Vec<u8>, file: &FileState) {
    match file {
        FileState::Regular {
            content,
            executable,
        } => {
            w.push(TAG_REGULAR);
            w.extend_from_slice(content.as_bytes());
            w.push(u8::from(*executable));
        }
        FileState::Symlink { target } => {
            w.push(TAG_SYMLINK);
            w.extend_from_slice(target.as_bytes());
        }
        FileState::Gitlink { commit } => {
            let algo = match commit {
                GitOid::Sha1(_) => GIT_SHA1,
                GitOid::Sha256(_) => GIT_SHA256,
            };
            w.extend_from_slice(&[TAG_GITLINK, algo]);
            w.extend_from_slice(commit.as_bytes());
        }
    }
}

// ---- decode helpers ----

/// Keys must be strictly increasing, which also rules out repeats.
fn ensure_after<K: Ord, V>(items: &[(K, V)], key: &K) -> Result<(), DecodeError> {
    match items.last() {
        Some((last, _)) if last >= key => Err(DecodeError::Unsorted),
        _ => Ok(()),
    }
}

/// Reads from the front of a slice. Nothing is allocated ahead of the bytes
/// that back it: a count loop stops at the first read past the end.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let (head, rest) = self
            .0
            .split_at_checked(n)
            .ok_or(DecodeError::UnexpectedEnd)?;
        self.0 = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let (head, rest) = self
            .0
            .split_first_chunk::<N>()
            .ok_or(DecodeError::UnexpectedEnd)?;
        self.0 = rest;
        Ok(*head)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        let [b] = self.array()?;
        Ok(b)
    }

    fn u32(&mut self) -> Result<usize, DecodeError> {
        Ok(u32::from_be_bytes(self.array()?) as usize)
    }

    fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.u32()?;
        self.take(len)
    }

    fn flag(&mut self) -> Result<bool, DecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            b => Err(DecodeError::BadFlag(b)),
        }
    }

    fn version(&mut self) -> Result<(), DecodeError> {
        match self.u8()? {
            VERSION => Ok(()),
            v => Err(DecodeError::BadVersion(v)),
        }
    }

    fn identity(&mut self) -> Result<IdentityTime, DecodeError> {
        Ok(IdentityTime::new(
            self.bytes()?.to_vec(),
            self.bytes()?.to_vec(),
            i64::from_be_bytes(self.array()?),
            i32::from_be_bytes(self.array()?),
        )
        .expect("identity fits the format"))
    }

    fn file(&mut self, tag: u8) -> Result<FileState, DecodeError> {
        match tag {
            TAG_REGULAR => Ok(FileState::Regular {
                content: BlobId::from_bytes(self.array()?),
                executable: self.flag()?,
            }),
            TAG_SYMLINK => Ok(FileState::Symlink {
                target: BlobId::from_bytes(self.array()?),
            }),
            TAG_GITLINK => {
                let commit = match self.u8()? {
                    GIT_SHA1 => GitOid::Sha1(self.array()?),
                    GIT_SHA256 => GitOid::Sha256(self.array()?),
                    algo => return Err(DecodeError::BadGitAlgo(algo)),
                };
                Ok(FileState::Gitlink { commit })
            }
            tag => Err(DecodeError::BadTag(tag)),
        }
    }

    fn finish(self) -> Result<(), DecodeError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_length_at_the_cap_fits_and_one_more_does_not() {
        assert_eq!(check_fit(76, 76, "field"), Ok(()));
        assert_eq!(
            check_fit(77, 76, "field"),
            Err(EncodeError::FieldTooLong("field"))
        );
        assert_eq!(check_fit(0, 0, "field"), Ok(()));
    }

    #[test]
    fn the_format_bound_is_u32_max() {
        assert_eq!(MAX_FIELD, u32::MAX as usize);
        assert_eq!(
            check_fit(usize::MAX, MAX_FIELD, "field"),
            Err(EncodeError::FieldTooLong("field"))
        );
    }
}
