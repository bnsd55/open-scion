use std::fmt;
use std::io;
use std::str::FromStr;

const BLOB_DOMAIN: &[u8] = b"scion\0blob\0v1\0";
const CHANGESET_DOMAIN: &[u8] = b"scion\0changeset\0v1\0";
const TREE_DOMAIN: &[u8] = b"scion\0tree\0v1\0";

fn domain_hasher(domain: &[u8]) -> blake3::Hasher {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher
}

fn domain_hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    *domain_hasher(domain).update(bytes).finalize().as_bytes()
}

macro_rules! object_id {
    ($(#[$doc:meta])* $name:ident, $domain:expr) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Wraps raw ID bytes. Does not check that any object has this ID.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            /// The raw ID bytes.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }

            /// Hashes `bytes` under this ID kind's domain prefix.
            pub(crate) fn hash(bytes: &[u8]) -> Self {
                Self(domain_hash($domain, bytes))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&blake3::Hash::from_bytes(self.0).to_hex())
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self)
            }
        }

        /// Parses 64 hex digits.
        impl FromStr for $name {
            type Err = ParseIdError;

            fn from_str(s: &str) -> Result<Self, ParseIdError> {
                blake3::Hash::from_hex(s)
                    .map(|hash| Self(*hash.as_bytes()))
                    .map_err(|_| ParseIdError)
            }
        }

        /// Takes exactly 32 raw bytes.
        impl TryFrom<&[u8]> for $name {
            type Error = ParseIdError;

            fn try_from(bytes: &[u8]) -> Result<Self, ParseIdError> {
                bytes.try_into().map(Self).map_err(|_| ParseIdError)
            }
        }
    };
}

/// Text or bytes that are not an object ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParseIdError;

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not an object ID: expected 64 hex digits or 32 bytes")
    }
}

impl std::error::Error for ParseIdError {}

object_id!(
    /// Content address of a file's raw bytes.
    BlobId,
    BLOB_DOMAIN
);
object_id!(
    /// Content address of an encoded [`Changeset`](crate::Changeset).
    ChangesetId,
    CHANGESET_DOMAIN
);
object_id!(
    /// Content address of an encoded [`Tree`](crate::Tree).
    TreeId,
    TREE_DOMAIN
);

impl BlobId {
    /// The ID of a file's raw, uncompressed content.
    #[must_use]
    pub fn of(raw: &[u8]) -> Self {
        Self::hash(raw)
    }

    /// The ID of the content that `reader` gives until its end. Reads in
    /// small parts, so it never holds the whole content.
    ///
    /// # Errors
    ///
    /// Returns the first error of `reader`.
    pub fn of_reader(reader: impl io::Read) -> io::Result<Self> {
        let mut hasher = BlobHasher::default();
        hasher.0.update_reader(reader)?;
        Ok(hasher.finalize())
    }
}

/// Computes a [`BlobId`] from content that arrives in parts.
pub struct BlobHasher(blake3::Hasher);

impl Default for BlobHasher {
    fn default() -> Self {
        Self(domain_hasher(BLOB_DOMAIN))
    }
}

impl BlobHasher {
    /// Adds the next part of the content.
    pub fn update(&mut self, part: &[u8]) {
        self.0.update(part);
    }

    /// The ID of all the parts so far, in order.
    #[must_use]
    pub fn finalize(&self) -> BlobId {
        BlobId(*self.0.finalize().as_bytes())
    }
}
