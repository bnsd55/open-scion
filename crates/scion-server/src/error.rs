//! Server errors and their mapping to gRPC status codes.

use scion_core::RepoPath;
use tonic::Status;

/// Why a changeset cannot apply to a tree.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ApplyError {
    /// A delete names a path that does not exist.
    #[error("cannot delete {0:?}: it does not exist")]
    DeleteMissing(RepoPath),
    /// A delete names a directory. Changes are file-level.
    #[error("cannot delete {0:?}: it is a directory")]
    NotAFile(RepoPath),
    /// A put needs a directory where a file is.
    #[error("cannot put {path:?}: {file:?} is a file")]
    FileInTheWay {
        /// The path being put.
        path: RepoPath,
        /// The file that is in the way.
        file: RepoPath,
    },
    /// A put targets a path that is a non-empty directory.
    #[error("cannot put {0:?}: it is a directory")]
    DirInTheWay(RepoPath),
}

/// A server error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A named thing does not exist, or the caller may not see it.
    #[error("{0} not found")]
    NotFound(String),
    /// The request is malformed.
    #[error("{0}")]
    InvalidArgument(String),
    /// The request is well formed, but the state does not allow it (for
    /// example a ref that has diverged from the history being imported).
    #[error("{0}")]
    FailedPrecondition(String),
    /// A changeset does not apply to its parent tree.
    #[error(transparent)]
    Apply(#[from] ApplyError),
    /// Stored data does not match its ID or format.
    #[error("corrupt data: {0}")]
    Corrupt(String),
    /// The database failed.
    #[error("database: {0}")]
    Db(#[from] rusqlite::Error),
    /// The object folder failed.
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),
    /// A git repository could not be read during import.
    #[error("git: {0}")]
    Git(String),
    /// Another writer kept winning a race. Nothing was written; the same
    /// request may succeed if retried.
    #[error("{0}")]
    Aborted(String),
    /// The call reached its bound on work before it found the answer. The
    /// same request fails again.
    #[error("{0}")]
    ResourceExhausted(String),
}

/// Result with the server [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for Status {
    fn from(e: Error) -> Self {
        match e {
            Error::NotFound(_) => Status::not_found(e.to_string()),
            Error::InvalidArgument(_) | Error::Apply(_) => Status::invalid_argument(e.to_string()),
            Error::Aborted(_) => Status::aborted(e.to_string()),
            Error::FailedPrecondition(_) => Status::failed_precondition(e.to_string()),
            Error::ResourceExhausted(_) => Status::resource_exhausted(e.to_string()),
            Error::Corrupt(_) | Error::Db(_) | Error::Io(_) | Error::Git(_) => {
                // Details stay in the server log, not in the reply.
                tracing::error!(error = %e, "internal error");
                Status::internal("internal error")
            }
        }
    }
}
