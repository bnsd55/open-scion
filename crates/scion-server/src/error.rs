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

/// A server error. The enum is not exhaustive: a caller outside this
/// crate matches it with a wildcard, because a server gains error kinds
/// without that being a breaking change.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
    /// A storage engine failed. The class says how the caller may go on;
    /// the engine's own error is the source, for the log. Which engine the
    /// server embeds, and its error's own type, are not part of the API: a
    /// bump of the engine is then a private change.
    #[error("{0}")]
    Storage(#[source] Storage),
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
    /// The request's deadline passed before the work was done. A blocking
    /// job checks this in its long loops, because a dropped call does not
    /// stop the job.
    #[error("{0}")]
    DeadlineExceeded(String),
    /// The call that asked for the work has gone: the client cancelled, or
    /// its connection ended. A job that outlives its call stops at its
    /// next check. Nothing was written, and the reply reaches no one; the
    /// code keeps the metrics honest (`cancelled`, not `deadline_exceeded`).
    #[error("{0}")]
    Cancelled(String),
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

/// How a storage engine failed, and so how the caller may go on. Not
/// exhaustive: a server may embed another engine, or split the classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageClass {
    /// The engine's write lock stayed busy past `busy_timeout`, or the
    /// connection to it was cut: the transaction rolled back and wrote
    /// nothing, so the same work may run again.
    Busy,
    /// The engine is out of space.
    DiskFull,
    /// The engine refused the work for its own reasons. Nothing was
    /// written; a bug of the backend, to log, not to retry blindly.
    Internal,
}

impl std::fmt::Display for StorageClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageClass::Busy => write!(f, "the database is busy"),
            StorageClass::DiskFull => write!(f, "the database is out of space"),
            StorageClass::Internal => write!(f, "the database failed"),
        }
    }
}

/// A storage engine's failure: its class, and the engine's own error as
/// the source. The fields are private, so the engine's error type never
/// crosses the API; [`Storage::class`] tells the caller how to go on.
#[derive(Debug)]
pub struct Storage {
    class: StorageClass,
    source: Source,
}

/// The engine's own error, boxed: it is only ever read as a source.
type Source = Box<dyn std::error::Error + Send + Sync>;

impl Storage {
    /// The engine stayed busy, or the connection to it was cut: the
    /// transaction rolled back and wrote nothing, so the same work may run
    /// again.
    pub(crate) fn busy(source: impl Into<Source>) -> Self {
        Self {
            class: StorageClass::Busy,
            source: source.into(),
        }
    }

    /// The engine is out of space.
    pub(crate) fn disk_full(source: impl Into<Source>) -> Self {
        Self {
            class: StorageClass::DiskFull,
            source: source.into(),
        }
    }

    /// The engine refused the work for its own reasons.
    pub(crate) fn internal(source: impl Into<Source>) -> Self {
        Self {
            class: StorageClass::Internal,
            source: source.into(),
        }
    }

    /// How the caller may go on.
    #[must_use]
    pub fn class(&self) -> StorageClass {
        self.class
    }
}

impl std::fmt::Display for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.class)
    }
}

impl std::error::Error for Storage {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Result with the server [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Whether the storage was busy: the work rolled back and wrote
    /// nothing, so a retry loop may run it again.
    #[must_use]
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            Error::Storage(storage) if storage.class() == StorageClass::Busy
        )
    }
}

/// Classifies a SQLite engine error, once, where it is built: a busy
/// database is not a database failure (the transaction rolled back and
/// wrote nothing), a full one is the server's own shortage, and the rest
/// keep the engine's error as the source. The test seams export it, so
/// the integration tests convert the way the crate does.
// The test seam re-exports this (test_support.error), and
// unreachable_pub does not see through a re-export.
#[allow(unreachable_pub)]
#[must_use]
pub fn from_sqlite(e: rusqlite::Error) -> Error {
    if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy) {
        Error::Storage(Storage::busy(e))
    } else if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
        Error::Storage(Storage::disk_full(e))
    } else {
        Error::Storage(Storage::internal(e))
    }
}

/// A storage error's class text with its causes, for a log line: a
/// [`Storage`] names only its class, and the engine's detail sits in the
/// sources.
fn storage_causes(storage: &Storage) -> String {
    let mut text = storage.to_string();
    let mut source = std::error::Error::source(storage);
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

impl From<Error> for Status {
    fn from(e: Error) -> Self {
        match e {
            Error::NotFound(_) => Status::not_found(e.to_string()),
            Error::InvalidArgument(_) | Error::Apply(_) => Status::invalid_argument(e.to_string()),
            Error::PermissionDenied(_) => Status::permission_denied(e.to_string()),
            Error::Aborted(_) => Status::aborted(e.to_string()),
            // The call went; the job that served it stopped at its next
            // check. Nobody reads the reply, so the message is for the log.
            Error::Cancelled(_) => Status::cancelled(e.to_string()),
            Error::DeadlineExceeded(_) => Status::deadline_exceeded(e.to_string()),
            Error::FailedPrecondition(_) => Status::failed_precondition(e.to_string()),
            Error::ResourceExhausted(_) => Status::resource_exhausted(e.to_string()),
            // The pool of read connections is drained: a retry after it
            // refills may succeed.
            Error::Unavailable(_) => Status::unavailable(e.to_string()),
            // The engine's class decides, wherever the metadata lives:
            // on SQLite or on Postgres, the caller reads the same code
            // for the same shortage.
            Error::Storage(storage) => match storage.class() {
                // The engine's write lock stayed busy, or the connection
                // was cut: a retry after it clears may succeed.
                StorageClass::Busy => {
                    // No repo on this line: the database is shared by
                    // repos. The call whose work failed logs the repo and
                    // the caller.
                    tracing::warn!(error = %storage_causes(&storage), "the database stayed busy");
                    Status::unavailable("the database is busy; retry")
                }
                // The disk is full, or the quota spent: the server's own
                // storage, not the caller's request. The reply is fixed,
                // so it names no server path; the path and the operation
                // are in the log line of the write that failed.
                StorageClass::DiskFull => {
                    tracing::error!(error = %storage_causes(&storage), "the server is out of disk space");
                    Status::resource_exhausted("the server is out of disk space; retry later")
                }
                // Details stay in the server log, not in the reply.
                StorageClass::Internal => {
                    tracing::error!(error = %storage_causes(&storage), "internal error");
                    Status::internal("internal error")
                }
            },
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
            Error::Corrupt(_) | Error::Git(_) | Error::Io(_) => {
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

    /// A SQLite error of a chosen engine code.
    fn db(code: std::ffi::c_int) -> Error {
        from_sqlite(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None))
    }

    /// A busy store answers `unavailable`, so the client retries: the
    /// transaction rolled back and wrote nothing.
    #[test]
    fn a_busy_store_is_unavailable_with_the_fixed_reply() {
        let status = Status::from(db(ffi::SQLITE_BUSY));
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), "the database is busy; retry");
        assert!(db(ffi::SQLITE_BUSY).is_busy());
    }

    /// An out-of-space store answers `resource_exhausted` with the fixed
    /// reply, the same shortage as the file path's.
    #[test]
    fn an_out_of_space_store_is_resource_exhausted_with_the_fixed_reply() {
        let status = Status::from(db(ffi::SQLITE_FULL));
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            status.message(),
            "the server is out of disk space; retry later"
        );
        assert!(!db(ffi::SQLITE_FULL).is_busy());
    }

    /// The engine refusing the work for its own reasons stays internal,
    /// and its detail stays in the server log, not in the reply.
    #[test]
    fn a_store_failure_is_internal_with_the_fixed_reply() {
        let status = Status::from(db(ffi::SQLITE_CORRUPT));
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), "internal error");
    }

    /// The engine's own error stays reachable through `source()`, for the
    /// log and for the tests that name the cause: the error's source is
    /// the [`Storage`], and the storage's source is the engine's error.
    #[test]
    fn the_engine_error_stays_reachable_through_source() {
        let e = db(ffi::SQLITE_CORRUPT);
        let Error::Storage(storage) = &e else {
            panic!("{e:?}")
        };
        assert_eq!(storage.class(), StorageClass::Internal);
        let source = std::error::Error::source(&e).expect("the storage is the source");
        let storage = source.downcast_ref::<Storage>().expect("it is the storage");
        let source = std::error::Error::source(storage).expect("the engine error is the source");
        let cause = source
            .downcast_ref::<rusqlite::Error>()
            .expect("it is the engine's own error");
        assert_eq!(
            cause.sqlite_error_code(),
            Some(rusqlite::ErrorCode::DatabaseCorrupt)
        );
    }

    /// A cancelled call maps to its own code, not a deadline's.
    #[test]
    fn a_cancelled_call_answers_cancelled_not_deadline_exceeded() {
        let err = Error::Cancelled("its call has gone".to_owned());
        assert_eq!(Status::from(err).code(), Code::Cancelled);
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
    }
}
