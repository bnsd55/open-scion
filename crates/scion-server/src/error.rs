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
    Db(rusqlite::Error),
    /// The Postgres database failed.
    #[error("database: {0}")]
    Pg(#[from] postgres::Error),
    /// The database's write lock stayed busy past `busy_timeout`. A caller
    /// that shares the data directory held it; the transaction rolled back
    /// and wrote nothing, so the same work may run again.
    #[error("database is busy")]
    Busy,
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
    /// The server cannot take the work now: a shared resource (the
    /// Postgres reader pool) is exhausted by other calls. The same
    /// request may succeed when the pool drains.
    #[error("{0}")]
    Unavailable(String),
}

/// Result with the server [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        // A busy database is not a database failure: the transaction rolled
        // back and wrote nothing, so the same work may run again.
        if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy) {
            Error::Busy
        } else {
            Error::Db(e)
        }
    }
}

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
            // The pool of read connections is drained: a retry after it
            // refills may succeed.
            Error::Unavailable(_) => Status::unavailable(e.to_string()),
            // Another process that shares the data directory held the
            // database's write lock for longer than `busy_timeout`.
            Error::Busy => {
                // No repo on this line: the database is shared by repos. The
                // call whose work failed logs the repo and the caller.
                tracing::warn!(error = %e, "the database stayed busy");
                Status::unavailable("the database is busy; retry")
            }
            // The disk is full, or the quota spent: the server's own
            // storage, not the caller's request. The reply is fixed, so it
            // names no server path; the path and the operation are in the
            // log line of the write that failed.
            Error::Io(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
                ) =>
            {
                tracing::error!(error = %e, "the server is out of disk space");
                Status::resource_exhausted("the server is out of disk space; retry later")
            }
            // A remount left the disk read-only: the server's own
            // storage, not the caller's request. The reply is fixed, so
            // it names no server path; the path and the operation are in
            // the log line of the write that failed. An EIO (a failing
            // device) stays on the internal arm below: a bad sector does
            // not heal on retry, and an error to the caller keeps the
            // server's own log loud.
            Error::Io(e) if e.kind() == std::io::ErrorKind::ReadOnlyFilesystem => {
                tracing::error!(error = %e, "the disk is read-only");
                Status::unavailable("the disk is read-only; retry later")
            }
            Error::Corrupt(_) | Error::Git(_) => {
                // Details stay in the server log, not in the reply.
                tracing::error!(error = %e, "internal error");
                Status::internal("internal error")
            }
            // A database out of space is the same shortage, wherever the
            // metadata lives: on Postgres it answers 53100, disk_full.
            Error::Pg(e) if e.code() == Some(&PG_DISK_FULL) => {
                tracing::error!(error = %e, "the server is out of disk space");
                Status::resource_exhausted("the server is out of disk space; retry later")
            }
            Error::Pg(_) => {
                // Details stay in the server log, not in the reply.
                tracing::error!(error = %e, "internal error");
                Status::internal("internal error")
            }
            // A busy database that reached the error as a raw SQLite error
            // (not through `From<rusqlite::Error>`) is still only busy: the
            // transaction rolled back and wrote nothing.
            Error::Db(e) if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy) => {
                tracing::warn!(error = %e, "the database stayed busy");
                Status::unavailable("the database is busy; retry")
            }
            Error::Db(e) if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) => {
                tracing::error!(error = %e, "the server is out of disk space");
                Status::resource_exhausted("the server is out of disk space; retry later")
            }
            Error::Io(_) | Error::Db(_) => {
                tracing::error!(error = %e, "internal error");
                Status::internal("internal error")
            }
        }
    }
}

/// The SQLSTATE of a Postgres database out of space: 53100, `disk_full`.
pub(crate) const PG_DISK_FULL: postgres::error::SqlState = postgres::error::SqlState::DISK_FULL;

#[cfg(test)]
mod tests {
    use rusqlite::ffi;
    use tonic::Code;

    use super::*;

    #[test]
    fn a_busy_database_is_unavailable_so_that_the_client_retries() {
        let db = |code| Error::Db(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None));
        assert_eq!(Status::from(Error::Busy).code(), Code::Unavailable);
        assert_eq!(Status::from(db(ffi::SQLITE_BUSY)).code(), Code::Unavailable);
        assert_eq!(Status::from(db(ffi::SQLITE_CORRUPT)).code(), Code::Internal);
    }

    /// A drained pool of Postgres readers answers `unavailable`, so the
    /// client retries after the pool refills.
    #[test]
    fn a_drained_reader_pool_is_unavailable() {
        let err = Error::Unavailable("no Postgres reader free".to_owned());
        assert_eq!(Status::from(err).code(), Code::Unavailable);
    }

    #[test]
    fn an_out_of_space_write_is_resource_exhausted_with_a_fixed_reply() {
        // A file write that the disk refused (ENOSPC, errno 28 on Linux
        // and macOS both): the reply names no path.
        let io = std::io::Error::from_raw_os_error(28);
        assert_eq!(io.kind(), std::io::ErrorKind::StorageFull);
        let status = Status::from(Error::Io(io));
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            status.message(),
            "the server is out of disk space; retry later"
        );
        // A quota refusal maps the same; the kind is built, because the
        // EDQUOT number differs between the platforms.
        let io = std::io::Error::from(std::io::ErrorKind::QuotaExceeded);
        assert_eq!(Status::from(Error::Io(io)).code(), Code::ResourceExhausted);
        // The metadata database out of space, on either backend.
        let db = |code| Error::Db(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None));
        let status = Status::from(db(ffi::SQLITE_FULL));
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            status.message(),
            "the server is out of disk space; retry later"
        );
        // The Postgres arm has no unit test: a `postgres::Error` of a
        // chosen code cannot be built outside the crate, so the arm keys
        // on the SQLSTATE alone, 53100, disk_full.
    }
}
