//! Durable state: `SQLite` for metadata and trees, a folder of zstd files for
//! blobs.
//!
//! ```text
//! <data>/lock                      shared by every process that uses <data>
//! <data>/open.lock                 held while a process opens <data>
//! <data>/server.lock               held by the one process that may land
//! <data>/scion.db                  metadata and trees (see migrations/)
//! <data>/objects/blob/ab/cd..      one zstd file per blob, named by ID
//! <data>/tmp/                      staging for atomic blob writes and uploads,
//!                                  one folder per process, held with a lock
//! ```
//!
//! The locks are `flock`s on open files. A forked child shares its parent's
//! file descriptions, and with them the locks, until it execs, so a child
//! spawned while a store is dropped can hold the dropped store's lock for
//! that window. This server never forks, and a store drops only when its
//! last user is done with it, so a server cannot hit the window. A library
//! user that forks while it drops a `Store` can: its tests that assert a
//! released lock must keep child spawns out of that window, as
//! `spawn_guard` does in this crate's tests.

use std::collections::BTreeSet;
use std::fs;
use std::io::{self, ErrorKind, Read, Seek, Write};
use std::ops::Deref;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use rustix::fs::FlockOperation;
use rustix::io::Errno;
use scion_core::{BlobHasher, BlobId};

use crate::db::SqliteTx;
use crate::deadline::Deadline;
use crate::derived::DeriveFlights;
use crate::error::{Error, Result};

/// The schema version this build makes and reads.
pub(crate) const SCHEMA_VERSION: u32 = 8;

/// Schema migrations in order. Migration `n`, from 1, moves the database from
/// version `n - 1` to version `n`. A released migration never changes.
const MIGRATIONS: [&str; SCHEMA_VERSION as usize] = [
    include_str!("../migrations/0001_init.sql"),
    include_str!("../migrations/0002_land_retention.sql"),
    include_str!("../migrations/0003_derived_values.sql"),
    include_str!("../migrations/0004_repo_blobs.sql"),
    include_str!("../migrations/0005_path_history.sql"),
    include_str!("../migrations/0006_land_created_at.sql"),
    include_str!("../migrations/0007_tree_sizes.sql"),
    include_str!("../migrations/0008_view_roots.sql"),
];

/// Largest object accepted or read back, uncompressed: 100 MiB, the hard
/// per-file limit on GitHub, so any file from a GitHub repository fits.
pub const MAX_OBJECT_BYTES: usize = 100 << 20;

/// Largest zstd window that a [`BlobReader`] accepts, as a power of two: 2 MiB,
/// the most that [`Blobs::put`] writes at its level. A frame that needs a
/// larger window is refused as corrupt.
const READER_WINDOW_LOG: u32 = 21;

/// Most memory that a [`BlobReader`] holds between parts: its zstd window and
/// the decoder's buffers and tables, which take less than 1 MiB.
pub const READER_BYTES: usize = (1 << READER_WINDOW_LOG) + (1 << 20);

/// A step the tests run at a counted batch boundary of the import, with
/// the count of batches done so far.
#[doc(hidden)]
pub type AfterBatch = Box<dyn FnMut(usize) + Send>;

/// The import's test hooks, the same state on both backends: the count of
/// busy retries, so a test proves the retry ran instead of only that the
/// import survived, and the step the tests run after each batch commits,
/// so a test that must act at a counted batch boundary runs there,
/// instead of racing the batch's end.
#[doc(hidden)]
#[derive(Default)]
pub struct ImportHooks {
    busy_retries: AtomicU64,
    after_batch: Mutex<Option<AfterBatch>>,
}

#[doc(hidden)]
impl ImportHooks {
    /// Counts one busy retry of the import's batch.
    pub fn note_busy_retry(&self) {
        self.busy_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// How many busy retries of the import's batches ran so far.
    #[must_use]
    pub fn busy_retries(&self) -> u64 {
        self.busy_retries.load(Ordering::Relaxed)
    }

    /// Sets the step the tests run after each batch of the import commits.
    pub fn set_after_batch(&self, step: Option<AfterBatch>) {
        *self
            .after_batch
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = step;
    }

    /// Runs the test step of a batch boundary, with the count of batches
    /// done so far.
    pub fn run_after_batch(&self, batches: usize) {
        if let Some(step) = &mut *self
            .after_batch
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
        {
            step(batches);
        }
    }
}

/// Handle to one data directory. Cheap to clone.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

/// Read-only connections of one [`Store`], so at most this many reads run at
/// once. Each has its own page cache, so they are few. They are more than
/// the graph queries that can run at once, so those never take every one.
pub(crate) const READERS: usize = 8;

/// The cap on derive walks in flight, across all kinds and repos of one
/// [`Store`]. Each walk holds the write connection in turns, so walks
/// beyond the cap only repeat work and starve writes ([#142]).
pub(crate) const DERIVE_WALKS_CAP: usize = 8;

/// Counts one writer that is waiting for the write connection, and
/// subtracts it again, with a wake, when the writer gets in or gives up.
/// A derive walk between steps waits while the count is above zero.
struct WriteWaiter<'a>(&'a Mutex<usize>, &'a Condvar);

impl Drop for WriteWaiter<'_> {
    fn drop(&mut self) {
        let mut waiting = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        *waiting -= 1;
        self.1.notify_all();
    }
}

struct Inner {
    /// Read-only connections for [`Store::read`]. They come before `db`, so
    /// they close first: the last connection to close checkpoints the log,
    /// and a read-only one cannot.
    readers: Readers,
    /// The write connection, for every commit and [`Store::read_unflushed`].
    db: Mutex<Connection>,
    /// Commits that a read may see. It grows under the `db` lock, right
    /// before each COMMIT runs.
    committing: AtomicU64,
    /// Commits that ended. It grows under the `db` lock, right after each
    /// COMMIT returns, also if it failed.
    committed: AtomicU64,
    /// How many of the ended commits a finished full flush covers.
    flushed: AtomicU64,
    blobs: Blobs,
    /// This `Store` holds the directory alone: no other process may use it,
    /// so a write cannot make one wait.
    alone: bool,
    /// `<data>/lock`, shared, until the last clone is dropped.
    _lock: fs::File,
    /// `<data>/server.lock`, for a server `Store`.
    _server_lock: Option<fs::File>,
    /// Write transactions run so far, for tests of how imports share the
    /// write lock.
    txes: AtomicU64,
    /// How many of the next commits fail as a busy database, for tests of
    /// the import's busy retry: the transaction ran and rolled back, so
    /// nothing was written, exactly as a real busy commit ends.
    fail_commits: AtomicU64,
    /// The import's test hooks and busy-retry count, as Postgres keeps
    /// its own.
    hooks: ImportHooks,
    /// The derive walks of this store: which are in flight, and the cap
    /// on how many may be.
    derive: DeriveFlights,
    /// Writers that are waiting for the write connection, with the condvar
    /// woken when one gets in or gives up. A derive walk between steps
    /// waits while the count is above zero, so writers have strict
    /// priority over walk steps: a steady stream of writers delays a cold
    /// read up to its deadline, never a land ([#142]).
    writers: Mutex<usize>,
    write_woken: Condvar,
}

impl Store {
    /// Opens or creates a data directory as its server: the one `Store` that
    /// may land. Lands are serialized in memory, so a second server `Store`,
    /// in this process or another, is refused. Shared `Store`s
    /// ([`Store::open_shared`]) may use the directory at the same time.
    ///
    /// The locks are released when the last clone of the returned `Store` is
    /// dropped.
    ///
    /// # Errors
    ///
    /// Fails if another server `Store` has the directory open, or a process
    /// of an older build that holds it alone. Fails if the directory or
    /// database cannot be created, or if the schema is newer than this build,
    /// or older while another `Store` uses the directory.
    pub fn open(dir: &Path) -> Result<Self> {
        Self::open_as(dir, true, &build(), DERIVE_WALKS_CAP)
    }

    /// Opens or creates a data directory as its server, with a cap of
    /// `walks` derive walks in flight. For tests of the cap.
    #[cfg(test)]
    pub(crate) fn open_with_walks(dir: &Path, walks: usize) -> Result<Self> {
        Self::open_as(dir, true, &build(), walks)
    }

    /// Opens or creates a data directory for a process that does not land,
    /// such as `scion-server import`. Any number of shared `Store`s may use
    /// the directory, beside one server `Store`.
    ///
    /// Each commit of a shared `Store` is durable before any other process
    /// can read it: the commit runs a full flush of the drive cache
    /// (`fullfsync`). A server replies with what it reads without a flush of
    /// its own, so this keeps its replies durable. The other way round,
    /// [`Store::read`] of a shared `Store` waits only for its own commits: a
    /// caller that relies on a commit of the server calls [`Store::flush`].
    /// A shared `Store` must not land.
    ///
    /// # Errors
    ///
    /// As [`Store::open`], except that a server `Store` is no conflict.
    pub fn open_shared(dir: &Path) -> Result<Self> {
        Self::open_as(dir, false, &build(), DERIVE_WALKS_CAP)
    }

    /// Opens a data directory for a process of build `build` ([`build`]).
    fn open_as(dir: &Path, server: bool, build: &str, walks_cap: usize) -> Result<Self> {
        fs::create_dir_all(dir)?;
        // The blobs and the metadata are only as private as the directory:
        // a group- or others-writable directory lets another user read or
        // replace them, and so does one under a directory another user can
        // change.
        let me = rustix::process::geteuid().as_raw();
        let meta = fs::symlink_metadata(dir)?;
        if meta.mode() & 0o022 != 0 {
            return Err(io::Error::new(
                ErrorKind::PermissionDenied,
                format!(
                    "{} can be written by group or others (fix: chmod go-w {})",
                    dir.display(),
                    dir.display()
                ),
            )
            .into());
        }
        let dir = scion_proto::auth::check_ancestors(dir, me)?;
        // A second server fails here, before it waits for anything.
        let server_lock = if server {
            let file = lock_file(&dir, "server.lock")?;
            if !flock(&file, FlockOperation::NonBlockingLockExclusive)? {
                return Err(in_use(&dir, "server.lock", "another server"));
            }
            Some(file)
        } else {
            None
        };
        // Opens take turns, so no other open of this build runs until this
        // one ends. If `lock` can be taken exclusive, no other process uses
        // the directory. Shared from then on, it keeps out a process of an
        // older build, which holds it exclusive for its whole life.
        let opening = lock_file(&dir, "open.lock")?;
        flock(&opening, FlockOperation::LockExclusive)?;
        let lock = lock_file(&dir, "lock")?;
        let alone = flock(&lock, FlockOperation::NonBlockingLockExclusive)?;
        if !flock(&lock, FlockOperation::NonBlockingLockShared)? {
            return Err(in_use(&dir, "lock", "a process that holds it alone"));
        }
        // The first process writes its build into `lock`, and every other
        // one must have the same build.
        if alone {
            lock.set_len(0)?;
            (&lock).write_all(build.as_bytes())?;
        } else {
            let mut theirs = String::new();
            (&lock).read_to_string(&mut theirs)?;
            if theirs != build {
                return Err(Error::FailedPrecondition(format!(
                    "data directory {} is in use by a process of another build \
                     ({theirs}); this build is {build}. Stop it, or use the same build",
                    dir.display()
                )));
            }
        }
        let blobs = Blobs::open(&dir)?;
        // Every open removes the folders of dead processes; only an open
        // that is alone also removes files, which a build older than the
        // folders may have left.
        blobs.sweep_tmp(alone)?;
        let path = dir.join("scion.db");
        let db = connect(&path, OpenFlags::default())?;
        // `fullfsync = OFF` for a server: a commit ends with a plain fsync,
        // and `write` then waits for a full flush of the drive cache that it
        // shares with concurrent commits and blob syncs. With `ON`, each
        // commit runs its own full flush under the write connection's lock,
        // which a
        // shared `Store` needs (see `open_shared`). Its commits are few and
        // large. `checkpoint_fullfsync`: a checkpoint, which is rare, fully
        // flushes the pages it copies before the log can be reused.
        db.execute_batch(&format!(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA fullfsync = {};
             PRAGMA checkpoint_fullfsync = ON;
             PRAGMA foreign_keys = ON;",
            if server { "OFF" } else { "ON" }
        ))?;
        // Only a `Store` that is alone changes the schema.
        let version = user_version(&mut SqliteTx(&db))?;
        if !alone && version < SCHEMA_VERSION {
            return Err(Error::FailedPrecondition(format!(
                "database schema version {version} is older than this build \
                 ({SCHEMA_VERSION}); stop the other processes that use {} so \
                 that this one can upgrade it",
                dir.display()
            )));
        }
        migrate(&db, SCHEMA_VERSION)?;
        drop(opening);
        // After the migrations, so no read sees an older schema.
        let readers = Readers::open(&path)?;
        Ok(Self {
            inner: Arc::new(Inner {
                readers,
                db: Mutex::new(db),
                committing: AtomicU64::new(0),
                committed: AtomicU64::new(0),
                flushed: AtomicU64::new(0),
                blobs,
                alone,
                _lock: lock,
                _server_lock: server_lock,
                txes: AtomicU64::new(0),
                fail_commits: AtomicU64::new(0),
                hooks: ImportHooks::default(),
                derive: DeriveFlights::new(walks_cap),
                writers: Mutex::new(0),
                write_woken: Condvar::new(),
            }),
        })
    }

    /// How many writers are waiting for the write connection.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn writers_waiting(&self) -> usize {
        *self
            .inner
            .writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits while writers are waiting for the write connection, until the
    /// deadline. Writers have strict priority over walk steps: a steady
    /// stream of writers delays a cold read up to its deadline, never a
    /// land ([#142]).
    ///
    /// # Errors
    ///
    /// Returns [`Error::DeadlineExceeded`] once the deadline has passed.
    pub(crate) fn wait_for_writers(&self, deadline: Option<&Deadline>) -> Result<()> {
        let mut waiting = self
            .inner
            .writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while *waiting > 0 {
            let Some(deadline) = deadline else {
                waiting = self
                    .inner
                    .write_woken
                    .wait(waiting)
                    .unwrap_or_else(PoisonError::into_inner);
                continue;
            };
            let budget = deadline.at().saturating_duration_since(Instant::now());
            let (guard, timed_out) = self
                .inner
                .write_woken
                .wait_timeout_while(waiting, budget, |waiting| *waiting > 0)
                .unwrap_or_else(PoisonError::into_inner);
            waiting = guard;
            if timed_out.timed_out() {
                return Err(Error::DeadlineExceeded(
                    "the call ran past its deadline".to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// The derive walks of this store: which are in flight, and the cap on
    /// how many may be.
    #[must_use]
    pub(crate) fn derive_flights(&self) -> &DeriveFlights {
        &self.inner.derive
    }

    /// The blob folder.
    #[must_use]
    pub fn blobs(&self) -> &Blobs {
        &self.inner.blobs
    }

    /// Commits made so far.
    #[doc(hidden)]
    #[must_use]
    pub fn commits(&self) -> u64 {
        self.inner.committed.load(Ordering::Acquire)
    }

    /// Runs `f` on a read-only connection, outside any explicit transaction.
    /// Each statement sees its own snapshot: every commit that it can see
    /// when it starts. So a caller that needs one consistent view must read
    /// it in one statement. (A snapshot held across the statements of
    /// back-to-back reads would keep the log from ever restarting.) Reads run
    /// beside each other and beside a write. If every read connection is in
    /// use, this waits for one. Returns once every commit that `f` could see
    /// is durable, so a reply never shows a commit that a power loss can
    /// undo.
    ///
    /// # Errors
    ///
    /// Returns what `f` returns, or an error from [`Store::flush`].
    pub fn read<T>(&self, f: impl for<'x> FnOnce(&'x mut SqliteTx<'x>) -> Result<T>) -> Result<T> {
        let (result, seen) = self.read_counted(f);
        // An error is an answer too.
        self.wait_durable(seen)?;
        result
    }

    /// Like [`Store::read`], but returns before what `f` saw is durable. Also
    /// returns a count that covers every commit `f` could see. Pass the
    /// largest count of a reply's reads to [`Store::wait_durable`] before it
    /// replies.
    pub(crate) fn read_counted<T>(
        &self,
        f: impl for<'x> FnOnce(&'x mut SqliteTx<'x>) -> Result<T>,
    ) -> (Result<T>, u64) {
        let reader = self.inner.readers.take();
        let result = f(&mut SqliteTx(&reader));
        (result, self.inner.committing.load(Ordering::Acquire))
    }

    /// Returns once every commit that a read with count `seen` could see is
    /// durable ([`Store::read_counted`]). Call it after that read.
    ///
    /// # Errors
    ///
    /// An error from [`Store::flush`].
    pub(crate) fn wait_durable(&self, seen: u64) -> Result<()> {
        // Why no reply shows a commit that is not durable. Say the read saw
        // commit k: the k-th commit of this `Store` (they take turns under
        // the `db` lock).
        // 1. `commit` counts k in `committing` before its COMMIT runs, and
        //    the read sees it only after that COMMIT made it visible. So
        //    `seen`, loaded after the read, is at least k. (SQLite makes a
        //    commit visible through its log index, under locks and memory
        //    barriers, so the count is visible to the read too.)
        // 2. If `flushed >= seen`, a flush loaded `committed >= k`, after
        //    commit k ended, and then finished a full flush that covers it.
        // 3. Else this call flushes. That flush starts after the read, and
        //    with `synchronous = FULL` a commit's log reaches the drive
        //    before the commit is visible. So the flush covers commit k.
        // Counted after its COMMIT instead, a commit could be seen before it
        // is counted, and the read would skip the flush.
        if seen > self.inner.flushed.load(Ordering::Acquire) {
            self.flush()?;
        }
        Ok(())
    }

    /// Runs `f` on the write connection, outside any explicit transaction.
    /// It waits for a write in progress, and writes wait for it. Unlike
    /// [`Store::read`], it returns before what `f` saw is durable. Use it
    /// only inside a write that calls [`Store::flush`] before it replies.
    ///
    /// # Errors
    ///
    /// Returns what `f` returns.
    pub fn read_unflushed<T>(
        &self,
        f: impl for<'x> FnOnce(&'x mut SqliteTx<'x>) -> Result<T>,
    ) -> Result<T> {
        let db = self.lock();
        f(&mut SqliteTx(&db))
    }

    /// Whether another process may use the directory at the same time. A
    /// [`Store::open_shared`] shares even if it found nobody; only one that
    /// took the directory alone is not shared.
    pub(crate) fn is_shared(&self) -> bool {
        !self.inner.alone
    }

    /// Sets how many of the next commits fail as a busy database. Tests of
    /// the import's busy retry use it: the failing transaction ran and
    /// rolled back, so the retry re-runs it whole.
    #[doc(hidden)]
    pub fn fail_next_commits(&self, n: u32) {
        self.inner
            .fail_commits
            .store(u64::from(n), Ordering::Relaxed);
    }

    /// Sets how long the write connection waits for a busy database. Tests
    /// use it to make a busy lock fail fast, so a retry is deterministic.
    #[doc(hidden)]
    pub fn set_busy_timeout(&self, timeout: Duration) {
        let _ = self.lock().busy_timeout(timeout);
    }

    /// The import's test hooks and busy-retry count.
    #[doc(hidden)]
    #[must_use]
    pub fn hooks(&self) -> &ImportHooks {
        &self.inner.hooks
    }

    /// Write transactions run so far.
    #[doc(hidden)]
    #[must_use]
    pub fn txes(&self) -> u64 {
        self.inner.txes.load(Ordering::Relaxed)
    }

    /// Runs `f` in one write transaction. Commits if `f` returns `Ok`, rolls
    /// back otherwise. Returns once the commit is durable: [`Store::commit`],
    /// then [`Store::flush`].
    ///
    /// # Errors
    ///
    /// As [`Store::commit`] and [`Store::flush`].
    pub fn write<T>(&self, f: impl for<'x> FnOnce(&'x mut SqliteTx<'x>) -> Result<T>) -> Result<T> {
        let value = self.commit(f)?;
        self.flush()?;
        Ok(value)
    }

    /// Like [`Store::write`], but returns before the commit is durable. Call
    /// [`Store::flush`] before replying with anything the commit wrote.
    ///
    /// # Errors
    ///
    /// Returns what `f` returns, or a database error from begin or commit.
    pub fn commit<T>(
        &self,
        f: impl for<'x> FnOnce(&'x mut SqliteTx<'x>) -> Result<T>,
    ) -> Result<T> {
        *self
            .inner
            .writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner) += 1;
        let _waiting = WriteWaiter(&self.inner.writers, &self.inner.write_woken);
        let mut db = self.lock();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = f(&mut SqliteTx(&tx))?;
        // Counted before a read can see it, and again once it ended (see
        // `read` and `flush`).
        self.inner.committing.fetch_add(1, Ordering::Release);
        if self.inner.fail_commits.load(Ordering::Relaxed) > 0 {
            self.inner.fail_commits.fetch_sub(1, Ordering::Relaxed);
            // Counted as ended, as a real failed commit is. The transaction
            // drops here, which rolls it back: nothing was written, exactly
            // as a commit past `busy_timeout` ends.
            self.inner.committed.fetch_add(1, Ordering::Release);
            return Err(Error::Busy);
        }
        let committed = tx.commit();
        self.inner.committed.fetch_add(1, Ordering::Release);
        #[cfg(test)]
        self.inner.txes.fetch_add(1, Ordering::Relaxed);
        committed?;
        Ok(value)
    }

    /// Returns once every commit made before this call is durable.
    ///
    /// No write acknowledged after a flush is lost. A full flush of the drive
    /// cache covers every commit before it, and the log recovered after a
    /// crash is a prefix of the commits (the write-ahead log's checksums are
    /// chained). So a land that builds on a head that is not durable yet
    /// replies only after a flush that covers that head too.
    ///
    /// # Errors
    ///
    /// Fails if the flush fails. The commits stay, but may not be durable; a
    /// retry sees them.
    pub fn flush(&self) -> Result<()> {
        // The full flush starts after this load, so it covers these commits:
        // each ended, so its log reached the drive. Not `committing`: a
        // commit counted there may not have written its log yet.
        let covers = self.inner.committed.load(Ordering::Acquire);
        // Concurrent calls share one flush, made outside the write
        // connection's lock.
        if FSYNC_LEAVES_DRIVE_CACHE {
            self.inner.blobs.full_flush()?;
        }
        self.inner.flushed.fetch_max(covers, Ordering::Release);
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        // A panic mid-call drops its transaction, which rolls back, so the
        // connection is still usable.
        self.inner.db.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Opens a connection to `path` with the settings that every connection
/// needs.
fn connect(path: &Path, flags: OpenFlags) -> Result<Connection> {
    let db = Connection::open_with_flags(path, flags)?;
    // Writes of the processes take turns: one waits up to 5 s for the
    // other's transaction. A read waits as long while another connection
    // recovers the log.
    db.busy_timeout(Duration::from_secs(5))?;
    // Enough for every hot statement, so none is prepared twice.
    db.set_prepared_statement_cache_capacity(64);
    Ok(db)
}

/// The [`READERS`] read-only connections of a [`Store`]. In WAL mode they
/// read beside the write connection.
struct Readers {
    path: PathBuf,
    idle: Mutex<Vec<Connection>>,
    returned: Condvar,
}

impl Readers {
    fn open(path: &Path) -> Result<Self> {
        let idle = (0..READERS)
            .map(|_| Self::connect(path))
            .collect::<Result<_>>()?;
        Ok(Self {
            path: path.to_owned(),
            idle: Mutex::new(idle),
            returned: Condvar::new(),
        })
    }

    fn connect(path: &Path) -> Result<Connection> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        connect(path, flags)
    }

    /// Takes an idle connection, or waits until one is returned.
    fn take(&self) -> Reader<'_> {
        let mut idle = self.idle();
        loop {
            if let Some(conn) = idle.pop() {
                return Reader {
                    readers: self,
                    conn: Some(conn),
                };
            }
            idle = self
                .returned
                .wait(idle)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn idle(&self) -> MutexGuard<'_, Vec<Connection>> {
        // Nothing panics while the lock is held.
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A connection taken from [`Readers`]. It goes back when dropped, also when
/// a read panics.
struct Reader<'a> {
    readers: &'a Readers,
    /// `None` only while it goes back.
    conn: Option<Connection>,
}

impl Deref for Reader<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn.as_ref().expect("a reader holds its connection")
    }
}

impl Drop for Reader<'_> {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else {
            return;
        };
        // A connection left in a transaction would keep its snapshot for the
        // next reads, and keep the log from restarting. So it is replaced.
        let conn = if conn.is_autocommit() {
            conn
        } else {
            drop(conn);
            tracing::error!("a read left a transaction open; its connection is replaced");
            match Readers::connect(&self.readers.path) {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::error!(%e, "cannot open a read connection; the pool has one fewer");
                    return;
                }
            }
        };
        self.readers.idle().push(conn);
        self.readers.returned.notify_one();
    }
}

/// What processes that share a data directory must agree on: the schema,
/// and the version of each kind of derived data. Each process removes the
/// derived rows of older versions, which a process of an older build may
/// still read.
fn build() -> String {
    use crate::derived::Kind;
    use crate::graph::CommitGraph;
    use crate::history::PathHistory;
    use crate::tree::RootTree;
    let kinds = [
        (RootTree::NAME, RootTree::VERSION),
        (CommitGraph::NAME, CommitGraph::VERSION),
        (PathHistory::NAME, PathHistory::VERSION),
    ];
    let kinds = kinds.map(|(name, version)| format!("{name} {version}"));
    format!("schema {SCHEMA_VERSION}, {}", kinds.join(", "))
}

/// Opens the lock file `<dir>/<name>`, made if missing. A lock on it lasts as
/// long as the file is open.
fn lock_file(dir: &Path, name: &str) -> io::Result<fs::File> {
    fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join(name))
}

/// Locks `file` with `op`. Returns false if `op` does not wait and another
/// holder's lock conflicts. Locks on two opens of a file conflict, even in
/// one process.
fn flock(file: &fs::File, op: FlockOperation) -> io::Result<bool> {
    match rustix::fs::flock(file, op) {
        Ok(()) => Ok(true),
        Err(Errno::WOULDBLOCK) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

fn in_use(dir: &Path, name: &str, by: &str) -> Error {
    io::Error::new(
        ErrorKind::WouldBlock,
        format!(
            "data directory {} is in use by {by} ({} is locked)",
            dir.display(),
            dir.join(name).display()
        ),
    )
    .into()
}

fn user_version(db: &mut SqliteTx<'_>) -> Result<u32> {
    Ok(db.pragma_query_value(None, "user_version", |row| row.get(0))?)
}

/// Runs the migrations from the database's version up to `target`. Only
/// tests stop below [`SCHEMA_VERSION`], to make an older database.
pub(crate) fn migrate(db: &Connection, target: u32) -> Result<()> {
    let version = user_version(&mut SqliteTx(db))?;
    if version > SCHEMA_VERSION {
        return Err(Error::Corrupt(format!(
            "database schema version {version} is newer than this build ({SCHEMA_VERSION})"
        )));
    }
    // Each migration runs in its own transaction, with the version it sets.
    // A crash rolls it back, and the next open runs it again from its start.
    for (to, sql) in (1..=target).zip(MIGRATIONS).skip(version as usize) {
        let tx = db.unchecked_transaction()?;
        tx.execute_batch(sql)?;
        if to == 4 {
            // SQL cannot read the blob IDs inside changeset bytes.
            crate::db::fill_repo_blobs(&tx)?;
        }
        tx.pragma_update(None, "user_version", to)?;
        tx.commit()?;
    }
    Ok(())
}

/// What is stored under a blob ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobStatus {
    /// No file.
    Missing,
    /// The file unpacks to content with this ID.
    Valid,
    /// A file exists but is not this blob: torn, truncated or overwritten.
    Corrupt,
}

/// Immutable file contents, one zstd-compressed file per [`BlobId`].
/// Immutable file contents, one zstd-compressed file per [`BlobId`].
pub struct Blobs {
    root: PathBuf,
    /// The staging folder for temp files of every process.
    tmp: PathBuf,
    /// This process's folder in `tmp/`, held with a lock, so an open of
    /// another process can tell that this one lives.
    mine: PathBuf,
    /// The lock file of `mine`, held for as long as this `Blobs` lives.
    /// Never read: its life is the point.
    _mine_lock: fs::File,
    group_flush: GroupFlush,
    /// Calls to [`Blobs::verify`] so far.
    #[cfg(test)]
    verifies: std::sync::atomic::AtomicU64,
}

impl Blobs {
    pub(crate) fn open(dir: &Path) -> Result<Self> {
        // Folders of `tmp/` made in this process, so each `Blobs` gets its own.
        static FOLDERS: AtomicU64 = AtomicU64::new(0);
        let objects = dir.join("objects");
        let root = objects.join("blob");
        let tmp = dir.join("tmp");
        fs::create_dir_all(&root)?;
        fs::create_dir_all(&tmp)?;
        // This process stages its temp files in its own folder of `tmp/`, and
        // holds the folder's lock file for as long as it lives. An open that
        // finds no holder for a folder removes it, so a crashed import leaves
        // nothing once any process opens the directory again (issue #101).
        // Opens hold `open.lock` exclusively, so no other process creates a
        // folder or sweeps while this one does.
        let (mine, mine_lock) = loop {
            let name = format!(
                "{:x}-{:x}",
                process::id(),
                FOLDERS.fetch_add(1, Ordering::Relaxed)
            );
            let mine = tmp.join(name);
            if let Err(e) = fs::create_dir(&mine) {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    continue;
                }
                return Err(e.into());
            }
            let lock = fs::File::create(mine.join(".lock"))?;
            if !flock(&lock, FlockOperation::NonBlockingLockExclusive)? {
                // Another process holds a folder of the same name. The
                // process id and the counter of this one are new, so that
                // cannot be this process's own folder.
                continue;
            }
            break (mine, lock);
        };
        // A blob file is only as durable as the directories above it. A batch
        // flushes the fan-out directories it writes to; these are made once.
        for path in [&root, &objects, &tmp, dir] {
            fsync_dir(path)?;
        }
        Ok(Self {
            root,
            tmp,
            mine,
            _mine_lock: mine_lock,
            group_flush: GroupFlush::default(),
            #[cfg(test)]
            verifies: std::sync::atomic::AtomicU64::default(),
        })
    }

    /// Removes what crashes left in `tmp/`: the folders of processes that no
    /// longer hold them, with the blob writes that were never renamed, and,
    /// when this open is alone, files of a build older than the folders. No
    /// blob name points at any of them. The folders of live processes are
    /// held, and this process's own folder is held too.
    pub(crate) fn sweep_tmp(&self, alone: bool) -> io::Result<()> {
        let mut removed = 0;
        for entry in fs::read_dir(&self.tmp)? {
            let path = entry?.path();
            if path.is_dir() {
                if path == self.mine {
                    continue;
                }
                // A lock file that cannot be opened was never flocked: its
                // process died before it made one.
                let held = match fs::File::open(path.join(".lock")) {
                    Ok(lock) => !flock(&lock, FlockOperation::NonBlockingLockExclusive)?,
                    Err(_) => false,
                };
                if !held {
                    fs::remove_dir_all(&path)?;
                    removed += 1;
                }
            } else if alone {
                fs::remove_file(&path)?;
                removed += 1;
            }
        }
        if removed > 0 {
            tracing::info!(removed, "removed temp folders that a crash left behind");
        }
        Ok(())
    }

    /// Waits for a full flush that starts after this call: the blob root
    /// directory, then the drive cache. That covers everything that reached
    /// the drive before it, the database too, which lives on the same drive.
    pub(crate) fn full_flush(&self) -> io::Result<()> {
        self.group_flush
            .run(|| fs::File::open(&self.root)?.sync_all())
    }

    /// Stores one file's content durably. Safe to repeat.
    ///
    /// # Errors
    ///
    /// Fails if the content is larger than [`MAX_OBJECT_BYTES`] or the write fails.
    pub fn put(&self, raw: &[u8]) -> Result<BlobId> {
        let mut batch = self.batch();
        let id = batch.put(raw)?;
        batch.sync()?;
        Ok(id)
    }

    /// Starts an upload: content that arrives in parts.
    ///
    /// # Errors
    ///
    /// Fails if the spool file cannot be made.
    pub fn upload(&self) -> Result<Upload> {
        Ok(Upload {
            spool: tempfile::tempfile_in(&self.mine)?,
            hasher: BlobHasher::default(),
            len: 0,
        })
    }

    /// Stores a finished upload. The content streams from the spool file
    /// through the compressor, so it is never all in memory. An intact stored
    /// copy is kept only if `keep_stored`; else the file is written again.
    /// Safe to repeat.
    ///
    /// The blob is durable after the next [`Store::flush`]: each file and
    /// directory that changed is flushed with a plain `fsync`, so only the
    /// full flush of the drive cache is left, which that flush shares.
    ///
    /// # Errors
    ///
    /// Fails if a read or write fails.
    pub fn put_upload(&self, upload: Upload, keep_stored: bool) -> Result<BlobId> {
        let Upload {
            mut spool,
            hasher,
            len,
        } = upload;
        let id = hasher.finalize();
        spool.rewind()?;
        let read_bytes = zstd::zstd_safe::CCtx::in_size();
        let raw = io::BufReader::with_capacity(read_bytes, spool);
        let mut batch = self.batch();
        if keep_stored {
            batch.write(id, len, raw)?;
        } else {
            // The upload may replace an intact stored copy that other repos
            // name. The full flush runs on every call, so the time of the
            // reply does not show whether a copy exists.
            batch.replace_stored(id, len, raw)?;
        }
        if batch.flush_dirs()? {
            // A new fan-out directory is an entry in the root. The full flush
            // of `sync` would cover it.
            fsync_dir(&self.root)?;
        }
        Ok(id)
    }

    /// Starts a batch of writes that become durable together.
    pub fn batch(&self) -> BlobBatch<'_> {
        BlobBatch {
            blobs: self,
            dirs: BTreeSet::new(),
        }
    }

    /// Reads a file's content and checks it against its ID.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent, [`Error::Corrupt`] if the file is not
    /// this blob, [`Error::Io`] if it cannot be read.
    pub fn get(&self, id: BlobId) -> Result<Vec<u8>> {
        let file = self.open_file(id)?;
        let corrupt = |why: String| Error::Corrupt(format!("blob {id}: {why}"));
        // The whole file is read before unpacking, so a read failure stays an
        // I/O error and only bad bytes count as corruption.
        let limit = zstd::zstd_safe::compress_bound(MAX_OBJECT_BYTES);
        let mut packed = Vec::new();
        file.take(limit as u64 + 1).read_to_end(&mut packed)?;
        if packed.len() > limit {
            return Err(corrupt(format!("file is larger than {limit} bytes")));
        }
        let raw = zstd::bulk::decompress(&packed, MAX_OBJECT_BYTES)
            .map_err(|e| corrupt(format!("cannot unpack: {e}")))?;
        if BlobId::of(&raw) != id {
            return Err(corrupt("content does not match the ID".into()));
        }
        Ok(raw)
    }

    /// A blob's uncompressed size, from the zstd frame header, without
    /// reading the content. Every blob is written with its size in the header.
    /// An empty file gives 0, because [`Blobs::get`] reads it as empty.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent, [`Error::Corrupt`] if the header is bad,
    /// has no size, or gives a size above [`MAX_OBJECT_BYTES`], [`Error::Io`]
    /// if it cannot be read.
    pub fn size(&self, id: BlobId) -> Result<usize> {
        frame_size(id, &mut self.open_file(id)?)
    }

    /// Opens a blob to read in parts with [`BlobReader::read_part`]. Between
    /// parts the reader holds at most [`READER_BYTES`].
    ///
    /// # Errors
    ///
    /// As [`Blobs::size`]. An empty file that is not the empty blob is
    /// [`Error::Corrupt`].
    pub fn reader(&self, id: BlobId) -> Result<BlobReader> {
        let mut file = self.open_file(id)?;
        let size = frame_size(id, &mut file)?;
        let mut reader = BlobReader {
            id,
            left: size,
            unpacked: None,
            hasher: BlobHasher::default(),
        };
        // As in `verify`: an empty file holds no zstd frame.
        if file.metadata()?.len() == 0 {
            reader.end()?;
            return Ok(reader);
        }
        file.rewind()?;
        let watched = Watched {
            inner: file,
            failed: false,
        };
        let mut unpacked = zstd::stream::read::Decoder::new(watched)?;
        unpacked.window_log_max(READER_WINDOW_LOG)?;
        reader.unpacked = Some(unpacked);
        Ok(reader)
    }

    /// The folder of blob files, `objects/blob`.
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Opens a blob's file: one zstd frame, which nothing here unpacks or
    /// checks.
    pub(crate) fn open_file(&self, id: BlobId) -> Result<fs::File> {
        fs::File::open(self.location(id).1).map_err(|e| match e.kind() {
            ErrorKind::NotFound => Error::NotFound(format!("blob {id}")),
            _ => e.into(),
        })
    }

    /// Whether a blob is stored intact. Streams the file through the decoder
    /// into the hasher, so it holds the zstd window and small buffers, not
    /// the whole blob. It gives the same answer as [`Blobs::get`].
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the file exists but cannot be read.
    pub fn verify(&self, id: BlobId) -> Result<BlobStatus> {
        #[cfg(test)]
        self.verifies
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let file = match self.open_file(id) {
            Ok(file) => file,
            Err(Error::NotFound(_)) => return Ok(BlobStatus::Missing),
            Err(e) => return Err(e),
        };
        let corrupt = |why: String| {
            // No repo on this line: a blob may be shared by repos. The call
            // whose read failed logs the repo and the caller.
            tracing::warn!(blob = %id, %why, "stored blob is corrupt");
            Ok(BlobStatus::Corrupt)
        };
        let len = file.metadata()?.len();
        let limit = zstd::zstd_safe::compress_bound(MAX_OBJECT_BYTES);
        if len > limit as u64 {
            return corrupt(format!("file is larger than {limit} bytes"));
        }
        // `get` unpacks an empty file to empty content, though it holds no
        // zstd frame. The streaming decoder would call it torn.
        if len == 0 && id == BlobId::of(b"") {
            return Ok(BlobStatus::Valid);
        }
        let mut file = Watched {
            inner: file,
            failed: false,
        };
        // One byte over the limit is enough to know the blob is too large.
        let mut unpacked =
            zstd::stream::read::Decoder::new(&mut file)?.take(MAX_OBJECT_BYTES as u64 + 1);
        let hashed = BlobId::of_reader(&mut unpacked);
        let oversized = unpacked.limit() == 0;
        drop(unpacked);
        match hashed {
            // A read error of the file is not bad bytes.
            Err(e) if file.failed => Err(e.into()),
            Err(e) => corrupt(format!("cannot unpack: {e}")),
            Ok(_) if oversized => corrupt(format!("unpacks to more than {MAX_OBJECT_BYTES} bytes")),
            Ok(got) if got != id => corrupt("content does not match the ID".into()),
            Ok(_) => Ok(BlobStatus::Valid),
        }
    }

    /// The fan-out directory and the file path of a blob.
    pub(crate) fn location(&self, id: BlobId) -> (PathBuf, PathBuf) {
        let hex = id.to_string();
        let dir = self.root.join(&hex[..2]);
        let path = dir.join(&hex[2..]);
        (dir, path)
    }

    /// Full flushes finished so far.
    #[cfg(test)]
    pub(crate) fn full_flushes(&self) -> u64 {
        self.group_flush.lock().finished
    }

    /// Calls to [`Blobs::verify`] so far.
    #[cfg(test)]
    pub(crate) fn verifies(&self) -> u64 {
        self.verifies.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// The content size in a blob file's zstd frame header. An empty file holds
/// empty content, as in [`Blobs::get`].
fn frame_size(id: BlobId, file: &mut fs::File) -> Result<usize> {
    /// Longest zstd frame header (`ZSTD_FRAMEHEADERSIZE_MAX`).
    const FRAME_HEADER_MAX: u64 = 18;
    let mut header = Vec::new();
    file.take(FRAME_HEADER_MAX).read_to_end(&mut header)?;
    if header.is_empty() {
        return Ok(0);
    }
    let corrupt = |why: &str| Error::Corrupt(format!("blob {id}: {why}"));
    match zstd::zstd_safe::get_frame_content_size(&header) {
        Ok(Some(size)) => usize::try_from(size)
            .ok()
            .filter(|&size| size <= MAX_OBJECT_BYTES)
            .ok_or_else(|| corrupt("its header gives a size above the object limit")),
        Ok(None) => Err(corrupt("its header has no content size")),
        Err(_) => Err(corrupt("bad zstd frame header")),
    }
}

/// The streaming zstd decoder over a blob file.
type Unpacked = zstd::stream::read::Decoder<'static, io::BufReader<Watched<fs::File>>>;

/// A blob's content, read in parts and checked against its ID. Made by
/// [`Blobs::reader`]. It holds the zstd window, not the whole blob.
pub struct BlobReader {
    id: BlobId,
    /// Bytes of the content not read yet, from the frame header.
    left: usize,
    /// `None` once the content has ended, or for an empty file.
    unpacked: Option<Unpacked>,
    hasher: BlobHasher,
}

impl BlobReader {
    /// The next part of the content, of at most `max` bytes (`max > 0`), or
    /// `None` after the end. The part that reaches the end comes only after
    /// the whole content matched the ID, so a caller never gets all of a blob
    /// that is not intact. At the end the reader frees its decoder.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if the content does not unpack, is not the size in
    /// its header, or does not match the ID. [`Error::Io`] if the file cannot
    /// be read.
    pub fn read_part(&mut self, max: usize) -> Result<Option<Vec<u8>>> {
        let Some(unpacked) = self.unpacked.as_mut() else {
            return Ok(None);
        };
        let mut part = vec![0; self.left.min(max)];
        if let Err(e) = unpacked.read_exact(&mut part) {
            return Err(unpack_error(self.id, unpacked, e));
        }
        self.hasher.update(&part);
        self.left -= part.len();
        if self.left == 0 {
            self.end()?;
        }
        Ok((!part.is_empty()).then_some(part))
    }

    /// Whether the whole content has been read and checked.
    #[must_use]
    pub fn ended(&self) -> bool {
        self.unpacked.is_none()
    }

    /// Checks that nothing follows the content and that the content matches
    /// the ID, then frees the decoder.
    fn end(&mut self) -> Result<()> {
        let id = self.id;
        let corrupt = |why: &str| Error::Corrupt(format!("blob {id}: {why}"));
        if let Some(mut unpacked) = self.unpacked.take() {
            match unpacked.read(&mut [0]) {
                Ok(0) => {}
                Ok(_) => return Err(corrupt("it unpacks to more than its header says")),
                Err(e) => return Err(unpack_error(id, &unpacked, e)),
            }
        }
        if self.hasher.finalize() != id {
            return Err(corrupt("content does not match the ID"));
        }
        Ok(())
    }
}

/// A read error of the file is I/O; anything else is bad bytes.
fn unpack_error(id: BlobId, unpacked: &Unpacked, e: io::Error) -> Error {
    if unpacked.get_ref().get_ref().failed {
        e.into()
    } else {
        Error::Corrupt(format!("blob {id}: cannot unpack: {e}"))
    }
}

/// A reader that notes whether it failed, so [`Blobs::verify`] can tell a
/// read error of the file from bad bytes in it.
struct Watched<R> {
    inner: R,
    failed: bool,
}

impl<R: Read> Read for Watched<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf);
        // An interrupted read is retried, so it is not a failure.
        self.failed |= matches!(&read, Err(e) if e.kind() != ErrorKind::Interrupted);
        read
    }
}

/// A blob that arrives in parts. Each part goes to a spool file in `tmp/` and
/// into the hasher, so an upload holds no more than one part in memory. The
/// spool file holds raw bytes: the zstd frame header needs the size before
/// the first compressed byte, and the size is known only at the end. The
/// spool file has no name, so a dropped upload, or a crash, leaves nothing
/// behind. Store it with [`Blobs::put_upload`].
pub struct Upload {
    /// The raw content so far.
    spool: fs::File,
    hasher: BlobHasher,
    len: usize,
}

impl Upload {
    /// Adds the next part of the content. On an error the upload is dropped.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] if the content gets larger than
    /// [`MAX_OBJECT_BYTES`], [`Error::Io`] if the write fails.
    pub fn append(mut self, part: &[u8]) -> Result<Self> {
        if part.len() > MAX_OBJECT_BYTES - self.len {
            return Err(too_large());
        }
        self.spool.write_all(part)?;
        self.hasher.update(part);
        self.len += part.len();
        Ok(self)
    }

    /// The ID of the content so far.
    #[must_use]
    pub fn id(&self) -> BlobId {
        self.hasher.finalize()
    }
}

fn too_large() -> Error {
    Error::InvalidArgument(format!("blob is larger than {MAX_OBJECT_BYTES} bytes"))
}

/// Blobs written but not yet durable. Call [`BlobBatch::sync`] before
/// committing any metadata that names them.
///
/// Each blob is flushed to the drive when written (a plain `fsync`, cheap);
/// `sync` then flushes the directories that changed and waits for one full
/// flush of the drive cache for the whole batch. On macOS a full flush
/// (`F_FULLFSYNC`) costs about 3 ms, so one per batch, not one per blob, is
/// what makes a large import fast. Batches that sync at the same time share
/// one full flush, so many small uploads are fast too.
#[must_use = "blobs in a batch are not durable until `sync`"]
pub struct BlobBatch<'a> {
    blobs: &'a Blobs,
    dirs: BTreeSet<PathBuf>,
}

impl BlobBatch<'_> {
    /// Writes one blob, or keeps the stored file if it is intact. A file that
    /// is not intact is replaced. Safe to repeat.
    ///
    /// # Errors
    ///
    /// Fails if the content is larger than [`MAX_OBJECT_BYTES`] or a read or
    /// write fails.
    pub fn put(&mut self, raw: &[u8]) -> Result<BlobId> {
        if raw.len() > MAX_OBJECT_BYTES {
            return Err(too_large());
        }
        let id = BlobId::of(raw);
        self.write(id, raw.len(), raw)?;
        Ok(id)
    }

    /// Writes blob `id`, whose `len` bytes `raw` gives, unless the stored file
    /// is intact. A file that is not intact is replaced.
    fn write(&mut self, id: BlobId, len: usize, raw: impl Read) -> Result<()> {
        match self.keep(id)? {
            BlobStatus::Valid => {
                // A put that keeps the stored copy writes nothing, so the
                // file's mtime would stay at its first upload. Touch it:
                // the grace period of `gc` counts from the last upload,
                // not the first, so a client that uploaded again does not
                // lose the blob before it names it.
                self.touch(id)?;
                return Ok(());
            }
            // The rename below replaces it in one step. No repo on this
            // line: a blob may be shared by repos.
            BlobStatus::Corrupt => tracing::warn!(blob = %id, "rewriting a corrupt blob file"),
            BlobStatus::Missing => {}
        }
        self.replace(id, len, raw)
    }

    /// Refreshes the mtime of a stored blob file, as one `utimensat`.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the file cannot be opened or its time not set.
    fn touch(&self, id: BlobId) -> Result<()> {
        let (_, path) = self.blobs.location(id);
        let file = fs::File::options().write(true).open(&path)?;
        file.set_modified(SystemTime::now())?;
        Ok(())
    }

    /// Writes blob `id`, whose `len` bytes `raw` gives, whether or not the
    /// stored file is intact. Then the time it takes does not show whether
    /// the blob was stored.
    fn replace(&mut self, id: BlobId, len: usize, raw: impl Read) -> Result<()> {
        let (dir, path) = self.blobs.location(id);
        // Write a temp file and flush it before the rename, so the name never
        // points at data that is not on the drive. The frame header holds the
        // size, for `Blobs::size`. Compression fails if `raw` does not give
        // exactly `len` bytes.
        let mut tmp = tempfile::NamedTempFile::new_in(&self.blobs.mine)?;
        zstd::stream::compress_sized_into(raw, &mut tmp, 3, len as u64)?;
        fsync(tmp.as_file())?;
        fs::create_dir_all(&dir)?;
        tmp.persist(&path).map_err(|e| e.error)?;
        self.dirs.insert(dir);
        Ok(())
    }

    /// Writes blob `id`, whose `len` bytes `raw` gives, over whatever is
    /// stored: a full flush runs on the temp file before the rename, every
    /// time, so the time it takes does not show whether a copy exists. For a
    /// put that may replace an intact stored copy that other repos name: the
    /// temp file is durable first, so a power loss never leaves the stored
    /// blob with data that the drive lost. A put of a blob that is missing or
    /// corrupt replaces no intact file, and its batch `sync` covers the
    /// rename.
    fn replace_stored(&mut self, id: BlobId, len: usize, raw: impl Read) -> Result<()> {
        self.blobs.full_flush()?;
        self.replace(id, len, raw)
    }

    /// Checks a stored blob and, if it is intact, keeps it: `sync` makes it
    /// durable too. Use it before committing metadata that names a blob this
    /// batch did not write.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the file exists but cannot be read.
    pub fn keep(&mut self, id: BlobId) -> Result<BlobStatus> {
        let status = self.blobs.verify(id)?;
        if status == BlobStatus::Valid {
            // Another batch may have written it and not synced yet, so this
            // batch flushes its directory too.
            // SHORTCUT: so keeping a stored blob costs a full flush, shared
            // with concurrent batches. Upgrade: track the blobs written but
            // not yet synced, and flush every blob directory and the drive at
            // open; a stored blob outside that set is then durable (#35).
            self.dirs.insert(self.blobs.location(id).0);
        }
        Ok(status)
    }

    /// Makes every blob written or kept in this batch durable.
    ///
    /// # Errors
    ///
    /// Fails if a flush fails.
    pub fn sync(self) -> Result<()> {
        let blobs = self.blobs;
        if self.flush_dirs()? {
            blobs.full_flush()?;
        }
        Ok(())
    }

    /// Like [`BlobBatch::sync`], but leaves the full flush of the drive cache
    /// to the next [`Store::flush`]. Returns whether the batch wrote or kept
    /// any blob.
    // SHORTCUT: the power-loss suite's cut drops unsynced file data, but
    // LazyFS persists a rename at rename time, so the directory fsync here
    // is never exercised by it. Upgrade: a block-level replay that drops
    // un-flushed writes in order (dm-log-writes replay to each FLUSH), when
    // a bug report or a change to this rename path says a directory entry
    // was lost.
    fn flush_dirs(self) -> Result<bool> {
        for dir in &self.dirs {
            fsync_dir(dir)?;
        }
        Ok(!self.dirs.is_empty())
    }
}

/// Group commit: concurrent callers share one full flush of the drive cache.
///
/// A full flush is slow, and one covers every write that reached the drive
/// before it started. So a caller first makes its writes reach the drive (a
/// plain `fsync`), then waits for a flush that starts after that; one waiting
/// caller runs it for all the others.
///
/// ```text
/// A: fsync │ runs flush 1 ────────┤ Ok
/// B:      fsync │ waits ──────────┤ runs flush 2 ────────┤ Ok
/// C:          fsync │ waits ──────┴──────────────────────┤ Ok
/// ```
///
/// B and C arrive while flush 1 runs. It may have started before their writes,
/// so they need flush 2, and B runs it for both.
#[derive(Default)]
struct GroupFlush {
    state: Mutex<FlushState>,
    finished: Condvar,
}

/// Flushes are numbered from 1. One runs while `started > finished`.
#[derive(Default)]
struct FlushState {
    started: u64,
    finished: u64,
    /// The latest flush that failed, and its error.
    failed: Option<(u64, Arc<io::Error>)>,
    /// Callers of [`GroupFlush::run`] in the wait, for tests.
    #[cfg(test)]
    waiting: usize,
}

impl GroupFlush {
    /// Waits for a run of `flush` that starts after this call, and returns its
    /// result. Only one caller's `flush` runs (as in `OnceLock::get_or_init`),
    /// so every caller must pass the same operation, and it must not panic or
    /// later callers wait forever. A caller may also see the error of a later
    /// failed run, which errs on the safe side.
    fn run(&self, flush: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let mut state = self.lock();
        // A running flush may have started before this caller's writes.
        let target = state.started + 1;
        #[cfg(test)]
        {
            state.waiting += 1;
        }
        state = self
            .finished
            .wait_while(state, |s| s.started > s.finished && s.finished < target)
            .unwrap_or_else(PoisonError::into_inner);
        if state.finished < target {
            // No flush is running and `target` is the next one: lead it.
            state.started = target;
            drop(state);
            let result = flush();
            state = self.lock();
            state.finished = target;
            if let Err(e) = result {
                state.failed = Some((target, Arc::new(e)));
            }
            self.finished.notify_all();
        }
        match &state.failed {
            Some((failed, e)) if *failed >= target => Err(io::Error::new(e.kind(), Arc::clone(e))),
            _ => Ok(()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, FlushState> {
        // Nothing panics while the lock is held.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Whether [`fsync`] can leave data in the drive cache: true on Apple
/// platforms. Elsewhere it flushes the cache too.
pub(crate) const FSYNC_LEAVES_DRIVE_CACHE: bool = cfg!(target_vendor = "apple");

/// A plain `fsync`: data reaches the drive, not necessarily past its cache.
/// (`File::sync_all` is a full cache flush on macOS, about 20x slower.)
fn fsync(file: &fs::File) -> io::Result<()> {
    rustix::fs::fsync(file).map_err(Into::into)
}

fn fsync_dir(path: &Path) -> io::Result<()> {
    fsync(&fs::File::open(path)?)
}

/// Keeps child processes out of the window where a test drops a store and
/// asserts its lock released. A forked child shares the flock of every open
/// file description until it execs, so a child spawned while another test
/// drops a store can hold the dropped store's lock past that assertion.
/// Every test that spawns a child takes the read side across the spawn, and
/// a test that asserts a lock is released takes the write side across its
/// drop and reopen: a pending write side also stops new read sides, so the
/// spawners finish before the release window opens.
#[cfg(test)]
pub(crate) mod spawn_guard {
    use super::{flock, lock_file};
    use rustix::fs::FlockOperation;
    use std::path::Path;
    use std::sync::RwLock;
    use std::time::{Duration, Instant};

    static SPAWNS: RwLock<()> = RwLock::new(());

    /// Holds the read side while `f` spawns its child: any number of tests
    /// may spawn at once.
    pub fn across_spawn<T>(f: impl FnOnce() -> T) -> T {
        let _held = SPAWNS.read().expect("the spawn guard is not poisoned");
        f()
    }

    /// Waits until the server lock of `dir` is free. Call it on the
    /// write side (inside [`Self::across_release`]), after dropping the
    /// stores and before the reopen that must see the lock released.
    ///
    /// The poll takes the real condition — the lock is free — instead of
    /// a timing guess: it opens `<dir>/server.lock` and takes a
    /// non-blocking exclusive flock every 1 ms, up to 2 s, then fails
    /// with the holder message, so a real leak still fails. The delay it
    /// absorbs is the operating system's: macOS tears a reaped child's
    /// inherited `flock` down one tick after `waitpid`, and about 7% of
    /// immediate reopens after a Rust spawn and its wait fail with
    /// `EWOULDBLOCK` there, sub-ms; Linux shows 0 of 2,000 rounds (a
    /// standalone probe). The server never spawns a child, so
    /// `Store::open` itself needs none of this.
    pub fn wait_for_lock_release(dir: &Path) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let file = lock_file(dir, "server.lock")
                .unwrap_or_else(|e| panic!("cannot open the lock of {dir:?}: {e}"));
            let free = flock(&file, FlockOperation::NonBlockingLockExclusive).unwrap_or(false);
            drop(file);
            if free {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the server lock of {} was never released within 2 s",
                dir.display()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Primes gix's one-time `git` installation lookup under the read
    /// side. The first `gix::open` in a process forks `git` outside our
    /// control (gix-path), and caches the result afterwards. Every test
    /// that reaches `gix` calls this first, so that fork waits out any
    /// release window like our own spawns do.
    pub fn prime_gix_lookup() {
        across_spawn(|| {
            let _ = gix::path::env::installation_config();
        });
    }

    /// Holds the write side while `f` drops a store and checks its lock is
    /// released: no child is in its fork-to-exec window, and no new spawn
    /// starts, until `f` is done.
    pub fn across_release<T>(f: impl FnOnce() -> T) -> T {
        let _held = SPAWNS.write().expect("the spawn guard is not poisoned");
        f()
    }
}

#[cfg(test)]
mod tests {
    use crate::db::SqliteTx;
    use crate::meta::MetaTx as _;
    use std::cell::{Cell, RefCell};
    use std::panic;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Barrier, mpsc};
    use std::thread;

    use super::*;

    #[test]
    fn an_open_refuses_a_data_directory_that_group_or_others_can_write() {
        use std::os::unix::fs::PermissionsExt as _;
        let writable = |path: &Path| {
            let mut mode = fs::metadata(path).unwrap().permissions();
            mode.set_mode(0o777);
            fs::set_permissions(path, mode).unwrap();
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data");
        fs::create_dir(&path).unwrap();
        writable(&path);
        let Err(err) = Store::open(&path) else {
            panic!("the open of a writable data directory must fail");
        };
        assert!(
            err.to_string()
                .contains("can be written by group or others"),
            "{err}"
        );
        // The same through a parent that another user can change: a
        // group-writable directory without the sticky bit above the data.
        let parent = dir.path().join("shared");
        fs::create_dir(&parent).unwrap();
        writable(&parent);
        let path = parent.join("data");
        fs::create_dir(&path).unwrap();
        let Err(err) = Store::open(&path) else {
            panic!("the open under a swappable parent must fail");
        };
        assert!(
            err.to_string().contains("could swap")
                || err.to_string().contains("not private")
                || err.to_string().contains("owned by another user"),
            "{err}"
        );
    }

    #[test]
    fn a_blob_size_comes_from_its_frame_header() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        for raw in [b"".to_vec(), b"hello".to_vec(), vec![7; 100_000]] {
            let id = store.blobs().put(&raw).unwrap();
            assert_eq!(store.blobs().size(id).unwrap(), raw.len());
        }

        // A frame with no size in its header is refused, not guessed.
        let id = BlobId::of(b"streamed");
        let (fan_out, file) = store.blobs().location(id);
        fs::create_dir_all(fan_out).unwrap();
        fs::write(file, zstd::stream::encode_all(&b"streamed"[..], 3).unwrap()).unwrap();
        assert!(matches!(store.blobs().size(id), Err(Error::Corrupt(_))));
        let absent = BlobId::of(b"absent");
        assert!(matches!(
            store.blobs().size(absent),
            Err(Error::NotFound(_))
        ));
    }

    /// A forked child shares the flock of every open file description
    /// until it execs, so a `git` spawn beside a dropped store can keep
    /// the dropped store's lock for the fork-to-exec window. Without
    /// [`spawn_guard`] this test fails within a few hundred cycles; with
    /// it, the spawns wait out every release window.
    ///
    /// The read side covers a spawn until the child has exited
    /// (`status`), so no child of this binary holds any descriptor once
    /// the read side is released, whatever the platform does at `exec`.
    /// Spawns outside the guard, like gix's one-time `git` lookup, are
    /// primed under the read side before any test opens a store (see
    /// [`spawn_guard::prime_gix_lookup`]).
    #[test]
    fn a_dropped_store_lock_is_shared_by_a_forked_child_until_it_execs() {
        use std::process::Command;
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let spawner = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    super::spawn_guard::across_spawn(|| {
                        // Any child shares the lock between its fork and
                        // its exec; `/bin/true` keeps this test fast, and
                        // the unguarded repro failed with `git` too, with
                        // and without `current_dir`.
                        let _ = Command::new("/bin/true").status();
                    });
                }
            })
        };
        for _ in 0..100 {
            let dir = tempfile::tempdir().unwrap();
            let first = Store::open(dir.path()).unwrap();
            let clone = first.clone();
            super::spawn_guard::across_release(|| {
                drop(first);
                drop(clone);
                // The window between the drops and this open is where a
                // forked child still holds the lock, one tick past the
                // reaper on macOS. The poll waits for the real
                // condition: the lock is free.
                super::spawn_guard::wait_for_lock_release(dir.path());
                Store::open(dir.path()).unwrap();
            });
        }
        stop.store(true, Ordering::Relaxed);
        spawner.join().unwrap();
    }

    #[test]
    fn a_data_directory_opens_in_one_server_store_at_a_time() {
        // The last part of this test drops every store and asserts the
        // lock released, so no child may spawn beside it.
        super::spawn_guard::across_release(|| {
            let dir = tempfile::tempdir().unwrap();
            let first = Store::open(dir.path()).unwrap();
            let clone = first.clone();
            assert_in_use(Store::open(dir.path()), dir.path(), "another server");
            // Shared stores open beside it, and a server beside them.
            let shared = [0, 1].map(|_| Store::open_shared(dir.path()).unwrap());
            drop(first);
            assert!(
                Store::open(dir.path()).is_err(),
                "a live clone keeps the lock"
            );
            drop(clone);
            super::spawn_guard::wait_for_lock_release(dir.path());
            Store::open(dir.path()).unwrap();
            drop(shared);
        });
    }

    /// Asserts that an open failed because `by` uses the directory.
    fn assert_in_use(opened: Result<Store>, dir: &Path, by: &str) {
        match opened {
            Err(Error::Io(e)) => {
                let msg = e.to_string();
                assert!(msg.contains(&dir.display().to_string()), "{msg}");
                assert!(msg.contains(&format!("in use by {by}")), "{msg}");
            }
            other => panic!("expected the open to fail, got {:?}", other.err()),
        }
    }

    /// Holds `<dir>/lock` with `op`, as a process of build `build` would.
    fn hold_lock(dir: &Path, op: FlockOperation, build: &str) -> fs::File {
        let file = lock_file(dir, "lock").unwrap();
        assert!(flock(&file, op).unwrap());
        (&file).write_all(build.as_bytes()).unwrap();
        file
    }

    #[test]
    fn only_an_open_that_is_alone_removes_temp_files() {
        // The drops and reopens of this test must see a lock released, so
        // no child may be in its fork-to-exec window (see `spawn_guard`).
        super::spawn_guard::across_release(|| {
            let dir = tempfile::tempdir().unwrap();
            let tmp = dir.path().join("tmp");
            let server = Store::open(dir.path()).unwrap();
            // The temp file of a write that the server has not renamed yet.
            fs::write(tmp.join("live"), b"x").unwrap();
            let shared = Store::open_shared(dir.path()).unwrap();
            assert!(tmp.join("live").exists(), "another process uses it");
            drop(server);
            drop(shared);
            super::spawn_guard::wait_for_lock_release(dir.path());
            // Now it is what a crash left behind.
            Store::open_shared(dir.path()).unwrap();
            assert!(!tmp.join("live").exists());
        });
    }

    #[test]
    fn an_open_removes_the_temp_files_of_a_dead_process() {
        // The drops and reopens of this test must see a lock released, so
        // no child may be in its fork-to-exec window (see `spawn_guard`).
        super::spawn_guard::across_release(|| {
            let dir = tempfile::tempdir().unwrap();
            let tmp = dir.path().join("tmp");
            let server = Store::open(dir.path()).unwrap();
            // A live importer: its folder is held with its lock, and a blob
            // write of it that was never renamed sits in the folder.
            let importer = Store::open_shared(dir.path()).unwrap();
            let live_dir = importer.blobs().mine.clone();
            let live_file = live_dir.join("stage");
            fs::write(&live_file, b"x").unwrap();
            // What a crashed import left: a folder whose holder is gone.
            let dead = tmp.join("dead-1");
            fs::create_dir(&dead).unwrap();
            fs::write(dead.join(".lock"), b"").unwrap();
            fs::write(dead.join("stage"), b"x").unwrap();
            // Another open, while the server and the importer use the directory:
            // it removes what the dead process left, and nothing else.
            Store::open_shared(dir.path()).unwrap();
            assert!(!dead.exists(), "a dead process's folder is removed");
            assert!(live_dir.exists(), "a live folder is not swept");
            assert!(
                live_file.exists(),
                "a live process's temp file is not swept"
            );
            // The server's own staging folder is held, and still works.
            let id = server.blobs().put(b"x").unwrap();
            assert_eq!(server.blobs().verify(id).unwrap(), BlobStatus::Valid);
            // The importer's folder is held only while it lives. Once it is
            // gone, the next open removes the folder, with the file in it.
            drop(importer);
            // The server store stays alive, so the directory stays in use:
            // no release to wait for.
            Store::open_shared(dir.path()).unwrap();
            assert!(!live_file.exists(), "a dead process's folder is removed");
        });
    }

    #[test]
    fn only_an_open_that_is_alone_upgrades_the_schema() {
        // The drops and reopens of this test must see a lock released, so
        // no child may be in its fork-to-exec window (see `spawn_guard`).
        super::spawn_guard::across_release(|| {
            let dir = tempfile::tempdir().unwrap();
            {
                let c = Connection::open(dir.path().join("scion.db")).unwrap();
                migrate(&c, 1).unwrap();
            }
            let version = |store: &Store| store.read(user_version).unwrap();
            // Another process of this build uses the directory.
            let live = hold_lock(dir.path(), FlockOperation::NonBlockingLockShared, &build());
            for open in [Store::open, Store::open_shared] {
                match open(dir.path()) {
                    Err(Error::FailedPrecondition(msg)) => {
                        assert!(msg.contains("version 1 is older"), "{msg}");
                    }
                    other => panic!("expected the open to fail, got {:?}", other.err()),
                }
            }
            drop(live);
            super::spawn_guard::wait_for_lock_release(dir.path());
            let store = Store::open_shared(dir.path()).unwrap();
            assert_eq!(version(&store), SCHEMA_VERSION);
            // A newer schema is refused, alone or not.
            store
                .write(|c| Ok(c.pragma_update(None, "user_version", 99)?))
                .unwrap();
            drop(store);
            super::spawn_guard::wait_for_lock_release(dir.path());
            for open in [Store::open, Store::open_shared] {
                assert!(matches!(open(dir.path()), Err(Error::Corrupt(_))));
            }
        });
    }

    #[test]
    fn a_process_that_holds_the_directory_alone_keeps_every_store_out() {
        // The drops and reopens of this test must see a lock released, so
        // no child may be in its fork-to-exec window (see `spawn_guard`).
        super::spawn_guard::across_release(|| {
            let dir = tempfile::tempdir().unwrap();
            let old = hold_lock(dir.path(), FlockOperation::NonBlockingLockExclusive, "");
            let by = "a process that holds it alone";
            assert_in_use(Store::open(dir.path()), dir.path(), by);
            assert_in_use(Store::open_shared(dir.path()), dir.path(), by);
            drop(old);
            super::spawn_guard::wait_for_lock_release(dir.path());
            // A store of this build holds the directory shared once it is open,
            // so an older build, which takes it alone, is kept out in turn.
            let store = Store::open(dir.path()).unwrap();
            let file = lock_file(dir.path(), "lock").unwrap();
            assert!(!flock(&file, FlockOperation::NonBlockingLockExclusive).unwrap());
            drop(store);
            super::spawn_guard::wait_for_lock_release(dir.path());
        });
    }

    #[test]
    fn processes_of_two_builds_never_share_a_directory() {
        // The drops and reopens of this test must see a lock released, so
        // no child may be in its fork-to-exec window (see `spawn_guard`).
        super::spawn_guard::across_release(|| {
            let dir = tempfile::tempdir().unwrap();
            let ours = build();
            // A build with one more kind.
            let other = format!("{ours}, blame 1");
            let refused = |opened: Result<Store>| match opened {
                Err(Error::FailedPrecondition(msg)) => {
                    assert!(msg.contains("another build"), "{msg}");
                }
                other => panic!("expected the open to fail, got {:?}", other.err()),
            };
            let server = Store::open(dir.path()).unwrap();
            refused(Store::open_as(dir.path(), false, &other, DERIVE_WALKS_CAP));
            Store::open_shared(dir.path()).unwrap();
            drop(server);
            // The import of the other build opened alone, so its build counts.
            let import = Store::open_as(dir.path(), false, &other, DERIVE_WALKS_CAP).unwrap();
            refused(Store::open(dir.path()));
            refused(Store::open_shared(dir.path()));
            drop(import);
            super::spawn_guard::wait_for_lock_release(dir.path());
            Store::open(dir.path()).unwrap();
        });
    }

    #[test]
    fn an_open_removes_temp_files_before_another_open_ends() {
        // The drops and reopens of this test must see a lock released, so
        // no child may be in its fork-to-exec window (see `spawn_guard`).
        super::spawn_guard::across_release(|| {
            // What a crashed import left: its staging folder with many temp
            // files, so removing it takes a while.
            const LEFT: usize = 5_000;
            let dir = tempfile::tempdir().unwrap();
            let tmp = dir.path().join("tmp");
            let dead = tmp.join("dead");
            fs::create_dir_all(&dead).unwrap();
            fs::write(dead.join(".lock"), b"").unwrap();
            for i in 0..LEFT {
                fs::write(dead.join(format!("left{i}")), b"").unwrap();
            }
            // The sweep removes the dead folder's files in the order that the
            // folder lists them.
            let listed_first = fs::read_dir(&dead).unwrap().next().unwrap().unwrap().path();
            let path = dir.path().to_owned();
            let first = thread::spawn(move || Store::open(&path));
            while listed_first.exists() && !first.is_finished() {
                thread::yield_now();
            }
            // The first open is removing the dead folder. A second open waits
            // for it, so the folder that it writes next is safe from that sweep:
            // the dead folder is gone, and the two live ones stay.
            let second = Store::open_shared(dir.path()).unwrap();
            assert_eq!(fs::read_dir(&tmp).unwrap().count(), 2);
            drop((first.join().unwrap().unwrap(), second));
        });
    }

    #[test]
    fn opens_take_turns() {
        // The drops and reopens of this test must see a lock released, so
        // no child may be in its fork-to-exec window (see `spawn_guard`).
        super::spawn_guard::across_release(|| {
            let dir = tempfile::tempdir().unwrap();
            // Another store is opening the directory.
            let opening = lock_file(dir.path(), "open.lock").unwrap();
            assert!(flock(&opening, FlockOperation::NonBlockingLockExclusive).unwrap());
            let path = dir.path().to_owned();
            let open = thread::spawn(move || Store::open_shared(&path).map(drop));
            thread::sleep(Duration::from_millis(200));
            assert!(!open.is_finished(), "an open waits for the one in progress");
            drop(opening);
            open.join().unwrap().unwrap();
        });
    }

    #[test]
    fn a_shared_store_commits_with_full_flushes() {
        let dir = tempfile::tempdir().unwrap();
        // Of the write connection, which commits.
        let fullfsync = |store: &Store| -> bool {
            store
                .read_unflushed(|c| Ok(c.pragma_query_value(None, "fullfsync", |row| row.get(0))?))
                .unwrap()
        };
        assert!(!fullfsync(&Store::open(dir.path()).unwrap()));
        assert!(fullfsync(&Store::open_shared(dir.path()).unwrap()));
    }

    #[test]
    fn concurrent_puts_share_full_flushes() {
        const WRITERS: u64 = 32;
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        // Odd writers put a blob that is already stored.
        blobs.put(b"stored").unwrap();
        let content = |i: u64| {
            if i.is_multiple_of(2) {
                format!("blob {i}")
            } else {
                "stored".to_owned()
            }
        };
        let before = blobs.full_flushes();
        // Run callers so far, so the arrival count below is relative.
        let runs_before = blobs
            .group_flush
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .waiting as u64;
        let running = Barrier::new(2);
        let ids: Vec<BlobId> = thread::scope(|s| {
            // A first flush that ends only when every writer waits in the
            // group, so they all share the next one, whatever the speed of
            // the machine or its drive.
            let slow = s.spawn(|| {
                blobs.group_flush.run(|| {
                    running.wait();
                    loop {
                        let state = blobs
                            .group_flush
                            .state
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner);
                        // The slow caller counts itself, so all have arrived
                        // at the runs before the test plus one more.
                        if state.waiting as u64 >= runs_before + 1 + WRITERS {
                            break;
                        }
                        drop(state);
                        thread::yield_now();
                    }
                    Ok(())
                })
            });
            running.wait();
            let writers: Vec<_> = (0..WRITERS)
                .map(|i| {
                    let (blobs, content) = (&blobs, &content);
                    s.spawn(move || blobs.put(content(i).as_bytes()))
                })
                .collect();
            slow.join().unwrap().unwrap();
            writers
                .into_iter()
                .map(|w| w.join().unwrap().unwrap())
                .collect()
        });
        // Not counting the slow flush: each writer waits in the group once,
        // while it runs, so one flush serves them all.
        let flushes = blobs.full_flushes() - before - 1;
        assert_eq!(flushes, 1, "{WRITERS} writers ran {flushes} full flushes");
        for id in ids {
            assert_eq!(blobs.verify(id).unwrap(), BlobStatus::Valid);
        }
    }

    /// What [`Blobs::get`] says about a blob, as a status.
    fn status_by_get(blobs: &Blobs, id: BlobId) -> Result<BlobStatus> {
        match blobs.get(id) {
            Ok(_) => Ok(BlobStatus::Valid),
            Err(Error::NotFound(_)) => Ok(BlobStatus::Missing),
            Err(Error::Corrupt(_)) => Ok(BlobStatus::Corrupt),
            Err(e) => Err(e),
        }
    }

    /// What a [`BlobReader`] says about a blob, read in parts of 3 bytes.
    /// Checks that it gives all of the content only for an intact blob.
    fn status_by_reader(blobs: &Blobs, id: BlobId) -> Result<BlobStatus> {
        let mut content = Vec::new();
        let mut read = || -> Result<()> {
            let mut reader = blobs.reader(id)?;
            while let Some(part) = reader.read_part(3)? {
                assert!((1..=3).contains(&part.len()), "a part of 1 to 3 bytes");
                content.extend(part);
            }
            assert!(reader.ended());
            Ok(())
        };
        let status = match read() {
            Ok(()) => BlobStatus::Valid,
            Err(Error::NotFound(_)) => BlobStatus::Missing,
            Err(Error::Corrupt(_)) => BlobStatus::Corrupt,
            Err(e) => return Err(e),
        };
        let whole = BlobId::of(&content) == id;
        assert_eq!(whole, status == BlobStatus::Valid, "{content:?}");
        Ok(status)
    }

    #[test]
    fn a_reader_gives_what_put_wrote_in_parts() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        // Larger than the window, so the frame uses the whole window.
        let raw: Vec<u8> = (0..5_u32 << 20)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
            .collect();
        let id = blobs.put(&raw).unwrap();
        let mut reader = blobs.reader(id).unwrap();
        let mut read = Vec::new();
        while let Some(part) = reader.read_part(1 << 20).unwrap() {
            assert!(part.len() <= 1 << 20);
            read.extend(part);
        }
        assert!(read == raw, "read {} bytes of {}", read.len(), raw.len());
    }

    #[test]
    fn verify_gives_the_same_answer_as_get() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        let packed = zstd::bulk::compress(b"content", 3).unwrap();
        let mut flipped = packed.clone();
        *flipped.last_mut().unwrap() ^= 1;
        // One byte more than a blob may hold, built with zstd directly, as
        // `put` refuses it.
        let too_large = || io::repeat(0).take(MAX_OBJECT_BYTES as u64 + 1);
        let content = BlobId::of(b"content");
        let truncated = packed[..packed.len() - 1].to_vec();
        let cases = [
            ("valid", content, Some(packed), BlobStatus::Valid),
            ("missing", content, None, BlobStatus::Missing),
            (
                "a flipped byte",
                content,
                Some(flipped),
                BlobStatus::Corrupt,
            ),
            ("truncated", content, Some(truncated), BlobStatus::Corrupt),
            (
                "another blob's file",
                content,
                Some(zstd::bulk::compress(b"another blob", 3).unwrap()),
                BlobStatus::Corrupt,
            ),
            (
                "empty, not the empty blob",
                content,
                Some(vec![]),
                BlobStatus::Corrupt,
            ),
            (
                "too large",
                BlobId::of_reader(too_large()).unwrap(),
                Some(zstd::stream::encode_all(too_large(), 1).unwrap()),
                BlobStatus::Corrupt,
            ),
            (
                "empty, empty blob",
                BlobId::of(b""),
                Some(vec![]),
                BlobStatus::Valid,
            ),
        ];
        for (case, id, file, want) in cases {
            let (dir, path) = blobs.location(id);
            fs::create_dir_all(dir).unwrap();
            match &file {
                Some(bytes) => fs::write(&path, bytes).unwrap(),
                None => fs::remove_file(&path).unwrap(),
            }
            assert_eq!(blobs.verify(id).unwrap(), want, "{case}");
            assert_eq!(status_by_get(&blobs, id).unwrap(), want, "{case}");
            assert_eq!(status_by_reader(&blobs, id).unwrap(), want, "{case}");
        }
    }

    #[test]
    fn verify_reports_a_read_error_as_io_not_as_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        // A directory opens like a file, but every read of it fails.
        let id = BlobId::of(b"content");
        fs::create_dir_all(blobs.location(id).1).unwrap();
        assert!(matches!(blobs.verify(id), Err(Error::Io(_))));
        assert!(matches!(status_by_get(&blobs, id), Err(Error::Io(_))));
        assert!(matches!(status_by_reader(&blobs, id), Err(Error::Io(_))));
    }

    /// Entries in the temp directory.
    fn staged(blobs: &Blobs) -> usize {
        fs::read_dir(&blobs.mine)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name() != ".lock")
            .count()
    }

    /// Blob files in the blob folder.
    fn stored(blobs: &Blobs) -> usize {
        fs::read_dir(&blobs.root)
            .unwrap()
            .map(|dir| fs::read_dir(dir.unwrap().path()).unwrap().count())
            .sum()
    }

    /// An upload of `parts`, in order.
    fn fill<'a>(blobs: &Blobs, parts: impl IntoIterator<Item = &'a [u8]>) -> Result<Upload> {
        parts.into_iter().try_fold(blobs.upload()?, Upload::append)
    }

    #[test]
    fn an_upload_stores_the_blob_of_all_its_parts() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        let cases: [&[&[u8]]; 4] = [&[], &[b""], &[b"a", b"", b"bc"], &[b"abc"]];
        for parts in cases {
            let content = parts.concat();
            let id = blobs
                .put_upload(fill(&blobs, parts.iter().copied()).unwrap(), true)
                .unwrap();
            assert_eq!(id, BlobId::of(&content), "{parts:?}");
            assert_eq!(blobs.get(id).unwrap(), content, "{parts:?}");
            assert_eq!(blobs.size(id).unwrap(), content.len(), "{parts:?}");
        }
        // `b"abc"` in one part or three is one blob.
        assert_eq!(stored(&blobs), 2);
        assert_eq!(staged(&blobs), 0);
    }

    #[test]
    fn an_upload_keeps_its_parts_on_disk_not_in_memory() {
        const PART: usize = 1 << 20;
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        // A period (251) that does not divide the part size, so parts stored
        // in the wrong order give other bytes.
        let content: Vec<u8> = (0..251_u8).cycle().take(3 * PART + 7).collect();
        let mut upload = blobs.upload().unwrap();
        let mut sent = 0;
        for part in content.chunks(PART) {
            upload = upload.append(part).unwrap();
            sent += part.len();
            assert_eq!(upload.spool.metadata().unwrap().len(), sent as u64);
        }
        let id = blobs.put_upload(upload, true).unwrap();
        assert_eq!(id, BlobId::of(&content));
        assert!(blobs.get(id).unwrap() == content, "content differs");
        assert_eq!(blobs.size(id).unwrap(), content.len());
    }

    #[test]
    fn one_byte_over_the_limit_fails_an_upload() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        // As if all but one byte of the limit had arrived.
        let mut upload = blobs.upload().unwrap();
        upload.len = MAX_OBJECT_BYTES - 1;
        let at_limit = upload.append(b"x").unwrap();
        match at_limit.append(b"y").err() {
            Some(Error::InvalidArgument(why)) => assert!(why.contains("larger than"), "{why}"),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
        assert_eq!(staged(&blobs), 0);
    }

    #[test]
    fn a_crash_before_the_rename_leaves_no_blob_and_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let content = b"never stored";
        let id = BlobId::of(content);
        // A crash during an upload: no destructor runs.
        std::mem::forget(store.blobs().upload().unwrap().append(content).unwrap());
        // A crash after the compressed file is written, before its rename,
        // in the staging folder of the process that crashed.
        let mut torn = tempfile::NamedTempFile::new_in(&store.blobs().mine).unwrap();
        torn.write_all(&zstd::bulk::compress(content, 3).unwrap())
            .unwrap();
        torn.keep().unwrap();
        assert_eq!(staged(store.blobs()), 1, "the spool file has no name");
        drop(store);

        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.blobs().verify(id).unwrap(), BlobStatus::Missing);
        // The folder of the dead process is gone, and this one is empty.
        assert_eq!(staged(store.blobs()), 0, "open removes what a crash left");
    }

    #[test]
    fn concurrent_uploads_of_one_blob_leave_one_file() {
        const UPLOADERS: usize = 8;
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        let content: Vec<u8> = (0..251_u8).cycle().take(3 << 20).collect();
        let ready = Barrier::new(UPLOADERS);
        let ids: Vec<BlobId> = thread::scope(|s| {
            let uploaders: Vec<_> = (0..UPLOADERS)
                .map(|_| {
                    s.spawn(|| {
                        let upload = fill(&blobs, content.chunks(1 << 20))?;
                        // All store at once, so their writes overlap. None
                        // keeps a stored copy, so each writes the file.
                        ready.wait();
                        blobs.put_upload(upload, false)
                    })
                })
                .collect();
            uploaders
                .into_iter()
                .map(|u| u.join().unwrap().unwrap())
                .collect()
        });
        let id = BlobId::of(&content);
        assert!(ids.iter().all(|got| *got == id), "{ids:?}");
        assert_eq!(blobs.verify(id).unwrap(), BlobStatus::Valid);
        assert_eq!(stored(&blobs), 1);
        assert_eq!(staged(&blobs), 0);
    }

    #[test]
    fn a_put_of_a_stored_blob_still_waits_for_a_full_flush() {
        // Another put may have renamed the file and not synced it yet.
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        blobs.put(b"stored").unwrap();
        let before = blobs.full_flushes();
        blobs.put(b"stored").unwrap();
        assert_eq!(blobs.full_flushes(), before + 1);
    }

    #[test]
    fn a_failed_full_flush_fails_every_caller_that_shares_it() {
        const CALLERS: usize = 8;
        let group = GroupFlush::default();
        let runs = AtomicUsize::new(0);
        let arrive = Barrier::new(CALLERS);
        let results: Vec<io::Result<()>> = thread::scope(|s| {
            let callers: Vec<_> = (0..CALLERS)
                .map(|_| {
                    s.spawn(|| {
                        arrive.wait();
                        // Slow, so the other callers arrive while it runs.
                        group.run(|| {
                            runs.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(Duration::from_millis(100));
                            Err(io::Error::other("the drive is gone"))
                        })
                    })
                })
                .collect();
            callers.into_iter().map(|c| c.join().unwrap()).collect()
        });
        let runs = runs.into_inner();
        assert!(runs < CALLERS, "{CALLERS} callers ran {runs} flushes");
        for result in results {
            let err = result.expect_err("every caller sees the failure");
            assert_eq!(err.to_string(), "the drive is gone");
        }
    }

    #[test]
    fn a_write_returns_after_a_full_flush_where_fsync_leaves_the_cache() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let before = store.blobs().full_flushes();
        store
            .write(|c| Ok(c.execute_batch("CREATE TABLE t (x)")?))
            .unwrap();
        // Elsewhere the commit's own fsync flushes the cache: no extra flush.
        assert_eq!(
            store.blobs().full_flushes(),
            before + u64::from(FSYNC_LEAVES_DRIVE_CACHE)
        );
    }

    /// Full flushes that one read runs.
    fn flushes_in_read(store: &Store) -> u64 {
        let before = store.blobs().full_flushes();
        store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM repos", [], |_| Ok(()))?))
            .unwrap();
        store.blobs().full_flushes() - before
    }

    fn create_repo(c: &mut SqliteTx<'_>) -> Result<()> {
        c.create_repo("main").map(drop)
    }

    #[test]
    fn a_read_with_no_unflushed_commit_runs_no_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(flushes_in_read(&store), 0);
    }

    #[test]
    fn a_read_of_an_unflushed_commit_waits_for_a_full_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.commit(create_repo).unwrap();
        // None where the commit's own fsync flushes the drive cache.
        assert_eq!(flushes_in_read(&store), u64::from(FSYNC_LEAVES_DRIVE_CACHE));
        // That flush covers the commit.
        assert_eq!(flushes_in_read(&store), 0);
    }

    #[test]
    fn a_read_after_a_write_runs_no_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.write(create_repo).unwrap();
        assert_eq!(flushes_in_read(&store), 0);
    }

    fn repos(c: &mut SqliteTx<'_>) -> Result<i64> {
        Ok(c.query_row("SELECT count(*) FROM repos", [], |row| row.get(0))?)
    }

    fn add_repo(name: &'static str) -> impl Fn(&mut SqliteTx<'_>) -> Result<()> {
        move |c| c.create_repo(name).map(drop)
    }

    #[test]
    fn a_long_read_blocks_no_write_and_no_other_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.write(add_repo("a")).unwrap();
        let (held, holding) = mpsc::channel();
        let (done, finished) = mpsc::channel();
        thread::scope(|s| {
            let store = &store;
            let long = s.spawn(move || {
                store.read(|c| {
                    assert_eq!(repos(c)?, 1);
                    held.send(()).unwrap();
                    finished
                        .recv_timeout(Duration::from_secs(10))
                        .expect("a write or a read waited for this read");
                    // Each statement has its own snapshot, so this one sees
                    // the write.
                    assert_eq!(repos(c)?, 2);
                    Ok(())
                })
            });
            holding.recv().unwrap();
            store.write(add_repo("b")).unwrap();
            assert_eq!(store.read(repos).unwrap(), 2);
            done.send(()).unwrap();
            long.join().unwrap().unwrap();
        });
        // A read cannot write, so no commit skips the counts of `commit`.
        let write = store.read(|c| Ok(c.execute("DELETE FROM repos", [])?));
        assert!(write.is_err());
    }

    #[test]
    fn a_write_in_progress_blocks_no_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.write(add_repo("a")).unwrap();
        let (held, holding) = mpsc::channel();
        let (done, finished) = mpsc::channel();
        thread::scope(|s| {
            // As a server write that waits for the write lock of an import,
            // this one holds the write connection.
            let store = &store;
            let write = s.spawn(move || {
                store.write(|c| {
                    add_repo("b")(c)?;
                    held.send(()).unwrap();
                    finished
                        .recv_timeout(Duration::from_secs(10))
                        .expect("a read waited for this write");
                    Ok(())
                })
            });
            holding.recv().unwrap();
            // Not committed yet, so not seen.
            assert_eq!(store.read(repos).unwrap(), 1);
            done.send(()).unwrap();
            write.join().unwrap().unwrap();
        });
        assert_eq!(store.read(repos).unwrap(), 2);
    }

    thread_local! {
        /// What the WAL hook of a test runs.
        static ON_COMMIT: RefCell<Option<Box<dyn FnMut()>>> = const { RefCell::new(None) };
    }

    #[test]
    fn a_read_that_sees_a_commit_waits_until_it_is_durable() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        // None where the commit's own fsync flushes the drive cache.
        let flush = u64::from(FSYNC_LEAVES_DRIVE_CACHE);

        // A commit that ends after the read began, before its first
        // statement.
        let before = store.blobs().full_flushes();
        let seen = store.read(|c| {
            store.commit(add_repo("a"))?;
            repos(c)
        });
        assert_eq!(seen.unwrap(), 1);
        assert_eq!(store.blobs().full_flushes() - before, flush);

        // A commit that is visible while its COMMIT still runs. SQLite runs
        // a WAL hook on the thread that commits, once the commit is visible
        // to other connections, before COMMIT returns.
        store
            .read_unflushed(|c| {
                c.wal_hook(Some(|_, _| {
                    ON_COMMIT.with_borrow_mut(|f| f.as_mut().map(|f| f()));
                    Ok(())
                }));
                Ok(())
            })
            .unwrap();
        let seen = Rc::new(Cell::new(None));
        ON_COMMIT.set(Some(Box::new({
            let (store, seen) = (store.clone(), Rc::clone(&seen));
            move || {
                let before = store.blobs().full_flushes();
                let repos = store.read(repos).unwrap();
                seen.set(Some((repos, store.blobs().full_flushes() - before)));
            }
        })));
        store.commit(add_repo("b")).unwrap();
        ON_COMMIT.set(None);
        assert_eq!(seen.get(), Some((2, flush)));
    }

    #[test]
    fn a_read_that_panics_returns_its_connection() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let readers = store.inner.readers.idle().len();
        let read = panic::catch_unwind(|| store.read(|_| -> Result<()> { panic!("a bug") }));
        assert!(read.is_err());
        assert_eq!(store.inner.readers.idle().len(), readers);
        // Its transaction ended, so the connection reads again.
        assert_eq!(store.read(repos).unwrap(), 0);
    }

    #[test]
    fn the_last_store_to_close_moves_the_log_into_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.write(add_repo("a")).unwrap();
        assert_eq!(store.read(repos).unwrap(), 1);
        drop(store);
        // So a copy of `scion.db` alone holds every commit.
        assert!(!dir.path().join("scion.db-wal").exists());
    }

    #[test]
    fn the_log_restarts_while_reads_run_back_to_back() {
        const COMMITS: usize = 600;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store
            .write(|c| Ok(c.execute_batch("CREATE TABLE t (x BLOB)")?))
            .unwrap();
        let done = AtomicBool::new(false);
        thread::scope(|s| {
            // Two long reads at a time, as graph walks are.
            for _ in 0..2 {
                s.spawn(|| {
                    while !done.load(Ordering::Relaxed) {
                        store
                            .read(|c| (0..10_000).try_for_each(|_| repos(c).map(drop)))
                            .unwrap();
                    }
                });
            }
            // 600 commits of 64 KiB: 37.5 MiB, more than 9 times the 4 MiB
            // after which each commit checkpoints the log.
            for _ in 0..COMMITS {
                let insert = "INSERT INTO t VALUES (zeroblob(65536))";
                store
                    .commit(|c| Ok(c.execute(insert, []).map(drop)?))
                    .unwrap();
            }
            done.store(true, Ordering::Relaxed);
        });
        // The log file never shrinks, so its size is the most it held: about
        // 4 MiB. With a snapshot held for each whole read, the log never
        // restarts and holds every commit.
        let log = fs::metadata(dir.path().join("scion.db-wal")).unwrap().len();
        assert!(log < 16 << 20, "the log grew to {} MiB", log >> 20);
    }

    #[test]
    fn a_connection_left_in_a_transaction_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.write(add_repo("a")).unwrap();
        store
            .read(|c| {
                c.execute_batch("BEGIN")?;
                repos(c).map(drop)
            })
            .unwrap();
        store.write(add_repo("b")).unwrap();
        // The pool gives the connection returned last to the next read. Kept,
        // its snapshot would show one repo.
        assert_eq!(store.read(repos).unwrap(), 2);
        assert_eq!(store.inner.readers.idle().len(), READERS);
    }
}
