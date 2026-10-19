use std::fmt;

const MAX_PATH_BYTES: usize = 4096;
const MAX_PATH_PARTS: usize = 256;

/// Why a path or name was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathError {
    /// The path is empty.
    Empty,
    /// The path is longer than 4096 bytes.
    TooLong,
    /// The path has more than 256 parts.
    TooManyParts,
    /// A part is empty: leading, trailing or double `/`.
    EmptyPart,
    /// A part is `.` or `..`.
    DotPart,
    /// The path contains a NUL byte.
    Nul,
    /// A name contains `/`.
    Slash,
}

impl fmt::Display for PathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("path is empty"),
            Self::TooLong => write!(f, "path is longer than {MAX_PATH_BYTES} bytes"),
            Self::TooManyParts => write!(f, "path has more than {MAX_PATH_PARTS} parts"),
            Self::EmptyPart => {
                f.write_str("path has an empty part (leading, trailing or double '/')")
            }
            Self::DotPart => f.write_str("path has a '.' or '..' part"),
            Self::Nul => f.write_str("path contains a NUL byte"),
            Self::Slash => f.write_str("name contains '/'"),
        }
    }
}

impl std::error::Error for PathError {}

/// A validated repo-relative path: raw bytes split on `/`.
///
/// Case and Unicode form are kept exactly as given. Ordering is by raw bytes.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepoPath(Vec<u8>);

impl RepoPath {
    /// Validates `bytes` as a repo path.
    ///
    /// # Errors
    ///
    /// Returns [`PathError`] if the path breaks a rule in `docs/format-v1.md`.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, PathError> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(PathError::Empty);
        }
        if bytes.len() > MAX_PATH_BYTES {
            return Err(PathError::TooLong);
        }
        for (i, part) in bytes.split(|&b| b == b'/').enumerate() {
            if i == MAX_PATH_PARTS {
                return Err(PathError::TooManyParts);
            }
            match part {
                [] => return Err(PathError::EmptyPart),
                b"." | b".." => return Err(PathError::DotPart),
                _ if part.contains(&0) => return Err(PathError::Nul),
                _ => {}
            }
        }
        Ok(Self(bytes))
    }

    /// The raw path bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A validated single path part: a [`Tree`](crate::Tree) entry name.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathElem(Vec<u8>);

impl PathElem {
    /// Validates `bytes` as one path part.
    ///
    /// # Errors
    ///
    /// Returns [`PathError`] if `bytes` contains `/` or is not a valid one-part
    /// [`RepoPath`].
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, PathError> {
        let bytes = bytes.into();
        if bytes.contains(&b'/') {
            return Err(PathError::Slash);
        }
        RepoPath::new(bytes).map(|path| Self(path.0))
    }

    /// The raw name bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Shows raw bytes as an escaped string, without losing non-UTF-8 bytes.
fn fmt_raw(bytes: &[u8], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "\"{}\"", bytes.escape_ascii())
}

impl fmt::Debug for RepoPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_raw(&self.0, f)
    }
}

impl fmt::Debug for PathElem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_raw(&self.0, f)
    }
}
