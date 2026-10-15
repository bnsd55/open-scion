//! Durable state: `SQLite` for metadata and trees, a folder of zstd files for
//! blobs.
//!
//! ```text
//! <data>/lock                      locked by the one process that uses <data>
//! <data>/scion.db                  metadata and trees (see migrations/)
//! <data>/objects/blob/ab/cd..      one zstd file per blob, named by ID
//! <data>/tmp/                      staging for atomic blob writes
//! ```

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io::{self, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use rusqlite::{Connection, TransactionBehavior};
use rustix::fs::FlockOperation;
use rustix::io::Errno;
use scion_core::BlobId;

use crate::error::{Error, Result};

const SCHEMA_VERSION: u32 = 2;

/// Schema migrations in order. Migration `n`, from 1, moves the database from
/// version `n - 1` to version `n`. A released migration never changes.
const MIGRATIONS: [&str; SCHEMA_VERSION as usize] = [
    include_str!("../migrations/0001_init.sql"),
    include_str!("../migrations/0002_land_retention.sql"),
];

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
            // `fullfsync = OFF`: a commit ends with a plain fsync, and
            // `write` then waits for a full flush of the drive cache that it
            // shares with concurrent commits and blob syncs. With `ON`, each
            // commit would run its own full flush under the database mutex.
            // `checkpoint_fullfsync`: a checkpoint, which is rare, fully
            // flushes the pages it copies before the log can be reused.
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA fullfsync = OFF;
             PRAGMA checkpoint_fullfsync = ON;
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
    /// back otherwise. Returns once the commit is durable: [`Store::commit`],
    /// then [`Store::flush`].
    ///
    /// # Errors
    ///
    /// As [`Store::commit`] and [`Store::flush`].
    pub fn write<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
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
    pub fn commit<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let mut db = self.lock();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = f(&tx)?;
        tx.commit()?;
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
    // SHORTCUT: between a commit and its flush, other calls can read the
    // commit, and a power loss can then undo what they saw. Upgrade: hide
    // commits until their flush, or a lander that batches lands (#18).
    pub fn flush(&self) -> Result<()> {
        // Concurrent calls share one flush, made outside the database lock.
        if FSYNC_LEAVES_DRIVE_CACHE {
            self.inner.blobs.full_flush()?;
        }
        Ok(())
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
    if version > SCHEMA_VERSION {
        return Err(Error::Corrupt(format!(
            "database schema version {version} is newer than this build ({SCHEMA_VERSION})"
        )));
    }
    // Each migration runs in its own transaction, with the version it sets.
    for (to, sql) in (1..=SCHEMA_VERSION).zip(MIGRATIONS).skip(version as usize) {
        db.execute_batch(&format!("BEGIN; {sql} PRAGMA user_version = {to}; COMMIT;"))?;
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
pub struct Blobs {
    root: PathBuf,
    tmp: PathBuf,
    group_flush: GroupFlush,
    /// The blobs that a batch renamed into place and has not made durable
    /// yet, each with the number of such batches. A count, not a set: two
    /// batches can write the same blob, and the first one to sync must not
    /// hide the other.
    ///
    /// Invariant: a stored blob that is not in this map is durable.
    /// - `open` flushes every fan-out directory and the drive, so every
    ///   file stored before it is durable.
    /// - A batch adds a blob before its rename, and removes it only after
    ///   its full flush ends. A batch that fails or is dropped leaves it.
    /// - [`BlobBatch::keep`] reads the file first and checks this map after.
    ///   If the map does not have the blob then, every rename of it so far
    ///   has ended its flush.
    unsynced: Mutex<HashMap<BlobId, usize>>,
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
        // The full flush below covers the blob root.
        for path in [&objects, &tmp, dir] {
            fsync_dir(path)?;
        }
        // A crash can stop a batch after its rename and before its flush. So
        // flush every fan-out directory and the drive once: then every blob
        // stored before now is durable (see `unsynced`).
        for entry in fs::read_dir(&root)? {
            fsync_dir(&entry?.path())?;
        }
        let blobs = Self {
            root,
            tmp,
            group_flush: GroupFlush::default(),
            unsynced: Mutex::default(),
        };
        blobs.full_flush()?;
        Ok(blobs)
    }

    /// Waits for a full flush that starts after this call: the blob root
    /// directory, then the drive cache. That covers everything that reached
    /// the drive before it, the database too, which lives on the same drive.
    fn full_flush(&self) -> io::Result<()> {
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

    /// Starts a batch of writes that become durable together.
    pub fn batch(&self) -> BlobBatch<'_> {
        BlobBatch {
            blobs: self,
            dirs: BTreeSet::new(),
            written: Vec::new(),
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
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent, [`Error::Corrupt`] if the header is bad,
    /// has no size, or gives a size above [`MAX_OBJECT_BYTES`], [`Error::Io`]
    /// if it cannot be read.
    pub fn size(&self, id: BlobId) -> Result<usize> {
        /// Longest zstd frame header (`ZSTD_FRAMEHEADERSIZE_MAX`).
        const FRAME_HEADER_MAX: u64 = 18;
        let mut header = Vec::new();
        self.open_file(id)?
            .take(FRAME_HEADER_MAX)
            .read_to_end(&mut header)?;
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

    /// Opens a blob's file.
    fn open_file(&self, id: BlobId) -> Result<fs::File> {
        fs::File::open(self.location(id).1).map_err(|e| match e.kind() {
            ErrorKind::NotFound => Error::NotFound(format!("blob {id}")),
            _ => e.into(),
        })
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

    fn unsynced(&self) -> MutexGuard<'_, HashMap<BlobId, usize>> {
        // Nothing panics while the lock is held.
        self.unsynced.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The fan-out directory and the file path of a blob.
    fn location(&self, id: BlobId) -> (PathBuf, PathBuf) {
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
}

/// Blobs written but not yet durable. Call [`BlobBatch::sync`] before
/// committing any metadata that names them.
///
/// Each blob is flushed to the drive when written (a plain `fsync`, cheap);
/// `sync` then flushes the directories that changed and waits for one full
/// flush of the drive cache for the whole batch. On macOS a full flush
/// (`F_FULLFSYNC`) costs about 3 ms, so one per batch, not one per blob, is
/// what makes a large import fast. Batches that sync at the same time share
/// one full flush, so many small uploads are fast too. A batch that only
/// keeps blobs that are durable already syncs with no flush.
#[must_use = "blobs in a batch are not durable until `sync`"]
pub struct BlobBatch<'a> {
    blobs: &'a Blobs,
    dirs: BTreeSet<PathBuf>,
    /// The blobs this batch renamed into place.
    written: Vec<BlobId>,
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
        match self.keep(id)? {
            BlobStatus::Valid => return Ok(id),
            // The rename below replaces it in one step.
            BlobStatus::Corrupt => tracing::warn!(blob = %id, "rewriting a corrupt blob file"),
            BlobStatus::Missing => {}
        }
        let (dir, path) = self.blobs.location(id);
        let compressed = zstd::bulk::compress(raw, 3)?;
        // Write a temp file and flush it before the rename, so the name never
        // points at data that is not on the drive.
        let mut tmp = tempfile::NamedTempFile::new_in(&self.blobs.tmp)?;
        tmp.write_all(&compressed)?;
        fsync(tmp.as_file())?;
        fs::create_dir_all(&dir)?;
        // Counted before the rename (see `Blobs::unsynced`).
        *self.blobs.unsynced().entry(id).or_default() += 1;
        self.written.push(id);
        tmp.persist(&path).map_err(|e| e.error)?;
        self.dirs.insert(dir);
        Ok(id)
    }

    /// Checks a stored blob and, if it is intact, keeps it: `sync` makes it
    /// durable too. Use it before committing metadata that names a blob this
    /// batch did not write. Most stored blobs are durable already, and then
    /// keeping one costs no flush.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the file exists but cannot be read.
    pub fn keep(&mut self, id: BlobId) -> Result<BlobStatus> {
        let status = self.blobs.verify(id)?;
        // Checked after the file is read (see `Blobs::unsynced`).
        if status == BlobStatus::Valid && self.blobs.unsynced().contains_key(&id) {
            // Another batch wrote it and its flush has not ended. Do not skip
            // it: this batch waits for a full flush that starts later.
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
        if self.dirs.is_empty() {
            return Ok(());
        }
        for dir in &self.dirs {
            fsync_dir(dir)?;
        }
        self.blobs.full_flush()?;
        // Only now are the blobs this batch wrote durable.
        let mut unsynced = self.blobs.unsynced();
        for id in &self.written {
            if let Some(batches) = unsynced.get_mut(id) {
                *batches -= 1;
                if *batches == 0 {
                    unsynced.remove(id);
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::*;

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
        let before = blobs.full_flushes();
        let running = Barrier::new(2);
        let ids: Vec<BlobId> = thread::scope(|s| {
            // A slow first flush, as on a busy drive, so the writers arrive
            // while it runs whatever the speed of the test machine's drive.
            let slow = s.spawn(|| {
                blobs.group_flush.run(|| {
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
        let flushes = blobs.full_flushes() - before - 1;
        assert!(
            flushes <= WRITERS / 4,
            "{WRITERS} writers ran {flushes} full flushes"
        );
        for id in ids {
            assert_eq!(blobs.verify(id).unwrap(), BlobStatus::Valid);
        }
    }

    #[test]
    fn a_keep_of_a_durable_blob_runs_no_flush() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        // One full flush at open makes every blob stored before it durable.
        assert_eq!(blobs.full_flushes(), 1);
        let id = blobs.put(b"stored").unwrap();
        let before = blobs.full_flushes();
        assert_eq!(blobs.put(b"stored").unwrap(), id);
        let mut batch = blobs.batch();
        assert_eq!(batch.keep(id).unwrap(), BlobStatus::Valid);
        batch.sync().unwrap();
        assert_eq!(blobs.full_flushes(), before);
    }

    #[test]
    fn a_keep_of_a_blob_not_synced_yet_waits_for_a_flush() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        let flushes_to_put = |raw: &[u8]| {
            let before = blobs.full_flushes();
            blobs.put(raw).unwrap();
            blobs.full_flushes() - before
        };
        // Written by a batch that has not synced yet.
        let mut first = blobs.batch();
        let id = first.put(b"new").unwrap();
        assert_eq!(flushes_to_put(b"new"), 1);

        // A second batch writes it again, over a damaged file. The first
        // batch's sync does not make the second one's file durable.
        fs::write(blobs.location(id).1, b"torn").unwrap();
        let mut second = blobs.batch();
        second.put(b"new").unwrap();
        first.sync().unwrap();
        assert_eq!(flushes_to_put(b"new"), 1);
        second.sync().unwrap();
        assert_eq!(flushes_to_put(b"new"), 0);

        // A batch dropped without a sync leaves its blob not durable.
        let mut dropped = blobs.batch();
        dropped.put(b"dropped").unwrap();
        drop(dropped);
        assert_eq!(flushes_to_put(b"dropped"), 1);
    }

    #[test]
    fn concurrent_puts_of_a_new_blob_all_wait_for_its_flush() {
        const PUTS: usize = 8;
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        let path = blobs.location(BlobId::of(b"new")).1;
        let running = Barrier::new(2);
        thread::scope(|s| {
            // A slow flush runs while the puts arrive, so each put must wait
            // for the flush after it.
            let slow = s.spawn(|| {
                blobs.group_flush.run(|| {
                    running.wait();
                    thread::sleep(Duration::from_millis(200));
                    Ok(())
                })
            });
            running.wait();
            let slow_flush = blobs.group_flush.lock().started;
            let put = || {
                blobs.put(b"new").unwrap();
                blobs.full_flushes()
            };
            let first = s.spawn(put);
            // The other puts find the file that the first one renamed. They
            // start once the first one waits for its flush.
            while !path.exists() {
                thread::yield_now();
            }
            thread::sleep(Duration::from_millis(20));
            let rest: Vec<_> = (1..PUTS).map(|_| s.spawn(put)).collect();
            for put in std::iter::once(first).chain(rest) {
                let seen = put.join().unwrap();
                assert!(seen > slow_flush, "a put returned before its flush");
            }
            slow.join().unwrap().unwrap();
        });
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
}
