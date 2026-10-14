//! Durable state: `SQLite` for metadata and trees, a folder of zstd files for
//! blobs.
//!
//! ```text
//! <data>/lock                      locked by the one process that uses <data>
//! <data>/scion.db                  metadata and trees (see migrations/)
//! <data>/objects/blob/ab/cd..      one zstd file per blob, named by ID
//! <data>/tmp/                      staging for atomic blob writes
//! ```

use std::collections::BTreeSet;
use std::fs;
use std::io::{self, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use rusqlite::{Connection, TransactionBehavior};
use rustix::fs::FlockOperation;
use rustix::io::Errno;
use scion_core::BlobId;

use crate::error::{Error, Result};

const SCHEMA: &str = include_str!("../migrations/0001_init.sql");
const SCHEMA_VERSION: u32 = 1;

/// Largest object accepted or read back, uncompressed: 100 MiB, the hard
/// per-file limit on GitHub, so any file from a GitHub repository fits.
// SHORTCUT: an upload is buffered in memory up to this size before it is
// hashed and written, so each concurrent upload can hold 100 MiB. Upgrade:
// stream uploads to a temp file and hash the chunks as they arrive.
pub const MAX_OBJECT_BYTES: usize = 100 << 20;

/// Handle to one data directory. Cheap to clone.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

struct Inner {
    // SHORTCUT: one connection behind a mutex, so reads wait for writes.
    // Upgrade: a pool of read-only connections (WAL allows parallel readers).
    db: Mutex<Connection>,
    blobs: Blobs,
    /// Keeps the data directory locked until the last clone is dropped.
    _lock: fs::File,
}

impl Store {
    /// Opens or creates a data directory and locks it for this `Store`.
    ///
    /// Lands are serialized in memory, so only one `Store` may use a data
    /// directory at a time. The lock is released when the last clone of the
    /// returned `Store` is dropped.
    ///
    /// # Errors
    ///
    /// Fails if another `Store`, in this process or another, has the
    /// directory open, if the directory or database cannot be created, or if
    /// the schema is newer than this build.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        // SHORTCUT: one process per data directory, so an import needs the
        // server stopped. Upgrade: allow a separate importer process. Lands
        // already re-plan when the ref moves (`MAX_ATTEMPTS`).
        let lock = lock_dir(dir)?;
        let blobs = Blobs::open(dir)?;
        let db = Connection::open(dir.join("scion.db"))?;
        db.execute_batch(
            // `fullfsync`: on macOS a plain fsync leaves the commit in the
            // drive cache, so a power loss could drop a land that replied
            // LANDED. Blobs already use F_FULLFSYNC. No effect elsewhere.
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA fullfsync = ON;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;",
        )?;
        // Enough for every hot statement, so none is prepared twice.
        db.set_prepared_statement_cache_capacity(64);
        migrate(&db)?;
        Ok(Self {
            inner: Arc::new(Inner {
                db: Mutex::new(db),
                blobs,
                _lock: lock,
            }),
        })
    }

    /// The blob folder.
    #[must_use]
    pub fn blobs(&self) -> &Blobs {
        &self.inner.blobs
    }

    /// Runs `f` on the database, outside any explicit transaction.
    ///
    /// # Errors
    ///
    /// Returns what `f` returns.
    pub fn read<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        f(&self.lock())
    }

    /// Runs `f` in one write transaction. Commits if `f` returns `Ok`, rolls
    /// back otherwise.
    ///
    /// # Errors
    ///
    /// Returns what `f` returns, or a database error from begin or commit.
    pub fn write<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let mut db = self.lock();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = f(&tx)?;
        tx.commit()?;
        Ok(value)
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        // A panic mid-call drops its transaction, which rolls back, so the
        // connection is still usable.
        self.inner.db.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Takes an exclusive lock on `<dir>/lock` without waiting. The lock lasts as
/// long as the returned file is open.
fn lock_dir(dir: &Path) -> Result<fs::File> {
    let path = dir.join("lock");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)?;
    match rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(file),
        Err(Errno::WOULDBLOCK) => Err(io::Error::new(
            ErrorKind::WouldBlock,
            format!(
                "data directory {} is in use by another server or import ({} is locked)",
                dir.display(),
                path.display()
            ),
        )
        .into()),
        Err(e) => Err(io::Error::from(e).into()),
    }
}

fn migrate(db: &Connection) -> Result<()> {
    let version: u32 = db.pragma_query_value(None, "user_version", |row| row.get(0))?;
    match version {
        0 => {
            db.execute_batch(&format!(
                "BEGIN; {SCHEMA} PRAGMA user_version = {SCHEMA_VERSION}; COMMIT;"
            ))?;
            Ok(())
        }
        SCHEMA_VERSION => Ok(()),
        newer => Err(Error::Corrupt(format!(
            "database schema version {newer} is newer than this build ({SCHEMA_VERSION})"
        ))),
    }
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
pub struct Blobs {
    root: PathBuf,
    tmp: PathBuf,
    /// The full flush that ends every [`BlobBatch::sync`].
    full_flush: GroupFlush,
}

impl Blobs {
    fn open(dir: &Path) -> Result<Self> {
        let objects = dir.join("objects");
        let root = objects.join("blob");
        let tmp = dir.join("tmp");
        fs::create_dir_all(&root)?;
        fs::create_dir_all(&tmp)?;
        // A blob file is only as durable as the directories above it. A batch
        // flushes the fan-out directories it writes to; these are made once.
        for path in [&root, &objects, &tmp, dir] {
            fsync_dir(path)?;
        }
        Ok(Self {
            root,
            tmp,
            full_flush: GroupFlush::default(),
        })
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
        let file = match fs::File::open(self.location(id).1) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(Error::NotFound(format!("blob {id}")));
            }
            Err(e) => return Err(e.into()),
        };
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

    /// Whether a blob is stored intact. Reads, unpacks and hashes the file.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the file exists but cannot be read.
    // SHORTCUT: holds the packed and the unpacked bytes, up to about 200 MiB
    // per call. Upgrade: stream the unpacked bytes into the hasher.
    pub fn verify(&self, id: BlobId) -> Result<BlobStatus> {
        match self.get(id) {
            Ok(_) => Ok(BlobStatus::Valid),
            Err(Error::NotFound(_)) => Ok(BlobStatus::Missing),
            Err(Error::Corrupt(why)) => {
                tracing::warn!(%why, "stored blob is corrupt");
                Ok(BlobStatus::Corrupt)
            }
            Err(e) => Err(e),
        }
    }

    /// The fan-out directory and the file path of a blob.
    fn location(&self, id: BlobId) -> (PathBuf, PathBuf) {
        let hex = id.to_string();
        let dir = self.root.join(&hex[..2]);
        let path = dir.join(&hex[2..]);
        (dir, path)
    }
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
            return Err(Error::InvalidArgument(format!(
                "blob is larger than {MAX_OBJECT_BYTES} bytes"
            )));
        }
        let id = BlobId::of(raw);
        let (dir, path) = self.blobs.location(id);
        match self.blobs.verify(id)? {
            BlobStatus::Valid => {
                // Another batch may have written it and not synced yet, so
                // this batch flushes its directory too.
                // SHORTCUT: so a lone put of a stored blob still costs a full
                // flush. Upgrade: track the blobs written but not yet synced,
                // and flush every blob directory and the drive at open; a
                // stored blob outside that set is then durable.
                self.dirs.insert(dir);
                return Ok(id);
            }
            // The rename below replaces it in one step.
            BlobStatus::Corrupt => tracing::warn!(blob = %id, "rewriting a corrupt blob file"),
            BlobStatus::Missing => {}
        }
        let compressed = zstd::bulk::compress(raw, 3)?;
        // Write a temp file and flush it before the rename, so the name never
        // points at data that is not on the drive.
        let mut tmp = tempfile::NamedTempFile::new_in(&self.blobs.tmp)?;
        tmp.write_all(&compressed)?;
        fsync(tmp.as_file())?;
        fs::create_dir_all(&dir)?;
        tmp.persist(&path).map_err(|e| e.error)?;
        self.dirs.insert(dir);
        Ok(id)
    }

    /// Makes every blob written or kept in this batch durable.
    ///
    /// # Errors
    ///
    /// Fails if a flush fails.
    pub fn sync(self) -> Result<()> {
        if self.dirs.is_empty() {
            return Ok(());
        }
        for dir in &self.dirs {
            fsync_dir(dir)?;
        }
        // Flushes the root directory, then the drive cache, which covers
        // everything above.
        self.blobs
            .full_flush
            .run(|| fs::File::open(&self.blobs.root)?.sync_all())?;
        Ok(())
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

/// A plain `fsync`: data reaches the drive, not necessarily past its cache.
/// (`File::sync_all` is a full cache flush on macOS, about 20x slower.)
fn fsync(file: &fs::File) -> io::Result<()> {
    rustix::fs::fsync(file).map_err(Into::into)
}

fn fsync_dir(path: &Path) -> io::Result<()> {
    fsync(&fs::File::open(path)?)
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_data_directory_opens_in_one_store_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let first = Store::open(dir.path()).unwrap();
        let clone = first.clone();
        match Store::open(dir.path()) {
            Err(Error::Io(e)) => {
                let msg = e.to_string();
                assert!(msg.contains(&dir.path().display().to_string()), "{msg}");
                assert!(msg.contains("in use"), "{msg}");
            }
            other => panic!("expected the second open to fail, got {:?}", other.err()),
        }
        drop(first);
        assert!(
            Store::open(dir.path()).is_err(),
            "a live clone keeps the lock"
        );
        drop(clone);
        Store::open(dir.path()).unwrap();
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
        let before = blobs.full_flush.lock().started;
        let running = Barrier::new(2);
        let ids: Vec<BlobId> = thread::scope(|s| {
            // A slow first flush, as on a busy drive, so the writers arrive
            // while it runs whatever the speed of the test machine's drive.
            let slow = s.spawn(|| {
                blobs.full_flush.run(|| {
                    running.wait();
                    thread::sleep(Duration::from_millis(200));
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
        // Not counting the slow flush.
        let flushes = blobs.full_flush.lock().started - before - 1;
        assert!(
            flushes <= WRITERS / 4,
            "{WRITERS} writers ran {flushes} full flushes"
        );
        for id in ids {
            assert_eq!(blobs.verify(id).unwrap(), BlobStatus::Valid);
        }
    }

    #[test]
    fn a_put_of_a_stored_blob_still_waits_for_a_full_flush() {
        // Another put may have renamed the file and not synced it yet.
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        blobs.put(b"stored").unwrap();
        let before = blobs.full_flush.lock().started;
        blobs.put(b"stored").unwrap();
        assert_eq!(blobs.full_flush.lock().started, before + 1);
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
    fn commits_flush_through_the_drive_cache() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (sync, full): (i64, i64) = store
            .read(|db| {
                let sync = db.pragma_query_value(None, "synchronous", |r| r.get(0))?;
                let full = db.pragma_query_value(None, "fullfsync", |r| r.get(0))?;
                Ok((sync, full))
            })
            .unwrap();
        assert_eq!(
            (sync, full),
            (2, 1),
            "synchronous = FULL and fullfsync = ON"
        );
    }
}
