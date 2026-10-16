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
    /// The caller may read the repo, but not make this change. The message
    /// names the path.
    #[error("{0}")]
    PermissionDenied(String),
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
    /// The request's deadline passed, or its call has gone, before the work
    /// was done. A blocking job checks this in its long loops, because a
    /// dropped call does not stop the job.
    #[error("{0}")]
    DeadlineExceeded(String),
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
            Error::PermissionDenied(_) => Status::permission_denied(e.to_string()),
            Error::Aborted(_) => Status::aborted(e.to_string()),
            Error::DeadlineExceeded(_) => Status::deadline_exceeded(e.to_string()),
            Error::FailedPrecondition(_) => Status::failed_precondition(e.to_string()),
            Error::ResourceExhausted(_) => Status::resource_exhausted(e.to_string()),
            // Another process that shares the data directory held the
            // database's write lock for longer than `busy_timeout`.
            Error::Db(ref db)
                if db.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy) =>
            {
                // No repo on this line: the database is shared by repos. The
                // call whose work failed logs the repo and the caller.
                tracing::warn!(error = %e, "the database stayed busy");
                Status::unavailable("the database is busy; retry")
            }
            Error::Corrupt(_) | Error::Db(_) | Error::Io(_) | Error::Git(_) => {
                // Details stay in the server log, not in the reply.
                tracing::error!(error = %e, "internal error");
                Status::internal("internal error")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::ffi;
    use tonic::Code;

    use super::*;

    #[test]
    fn a_busy_database_is_unavailable_so_that_the_client_retries() {
        let db = |code| Error::Db(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None));
        assert_eq!(Status::from(db(ffi::SQLITE_BUSY)).code(), Code::Unavailable);
        assert_eq!(Status::from(db(ffi::SQLITE_CORRUPT)).code(), Code::Internal);
    }
}
