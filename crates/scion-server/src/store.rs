//! Durable state: `SQLite` for metadata and trees, a folder of zstd files for
//! blobs.
//!
//! ```text
//! <data>/lock                      locked by the one process that uses <data>
//! <data>/scion.db                  metadata and trees (see migrations/)
//! <data>/objects/blob/ab/cd..      one zstd file per blob, named by ID
//! <data>/tmp/                      staging for atomic blob writes and uploads
//! ```

use std::collections::BTreeSet;
use std::fs;
use std::io::{self, ErrorKind, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use rusqlite::{Connection, TransactionBehavior};
use rustix::fs::FlockOperation;
use rustix::io::Errno;
use scion_core::{BlobHasher, BlobId};

use crate::error::{Error, Result};

/// The schema version this build makes and reads.
pub(crate) const SCHEMA_VERSION: u32 = 3;

/// Schema migrations in order. Migration `n`, from 1, moves the database from
/// version `n - 1` to version `n`. A released migration never changes.
const MIGRATIONS: [&str; SCHEMA_VERSION as usize] = [
    include_str!("../migrations/0001_init.sql"),
    include_str!("../migrations/0002_land_retention.sql"),
    include_str!("../migrations/0003_derived_values.sql"),
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

/// Handle to one data directory. Cheap to clone.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

struct Inner {
    // SHORTCUT: one connection behind a mutex, so reads wait for writes.
    // Upgrade: a pool of read-only connections (WAL allows parallel readers).
    db: Mutex<Connection>,
    /// Commits made so far. It grows under the `db` lock, right after each
    /// commit.
    committed: AtomicU64,
    /// How many of those commits a finished full flush covers.
    flushed: AtomicU64,
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
        migrate(&db, SCHEMA_VERSION)?;
        Ok(Self {
            inner: Arc::new(Inner {
                db: Mutex::new(db),
                committed: AtomicU64::new(0),
                flushed: AtomicU64::new(0),
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

    /// Commits made so far.
    #[cfg(test)]
    pub(crate) fn commits(&self) -> u64 {
        self.inner.committed.load(Ordering::Acquire)
    }

    /// Runs `f` on the database, outside any explicit transaction. Returns
    /// once every commit that `f` could see is durable, so a reply never
    /// shows a commit that a power loss can undo.
    ///
    /// # Errors
    ///
    /// Returns what `f` returns, or an error from [`Store::flush`].
    pub fn read<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let (result, seen) = {
            let db = self.lock();
            // Commits are counted under this lock, so `seen` counts every
            // commit that `f` could see.
            (f(&db), self.inner.committed.load(Ordering::Acquire))
        };
        // An error is an answer too. The flush runs outside the lock.
        if seen > self.inner.flushed.load(Ordering::Acquire) {
            self.flush()?;
        }
        result
    }

    /// Like [`Store::read`], but returns before what `f` saw is durable. Use
    /// it only inside a write that calls [`Store::flush`] before it replies.
    ///
    /// # Errors
    ///
    /// Returns what `f` returns.
    pub fn read_unflushed<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
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
        self.inner.committed.fetch_add(1, Ordering::Release);
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
        // The full flush starts after this load, so it covers these commits.
        let covers = self.inner.committed.load(Ordering::Acquire);
        // Concurrent calls share one flush, made outside the database lock.
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

/// Runs the migrations from the database's version up to `target`. Only
/// tests stop below [`SCHEMA_VERSION`], to make an older database.
pub(crate) fn migrate(db: &Connection, target: u32) -> Result<()> {
    let version: u32 = db.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(Error::Corrupt(format!(
            "database schema version {version} is newer than this build ({SCHEMA_VERSION})"
        )));
    }
    // Each migration runs in its own transaction, with the version it sets.
    for (to, sql) in (1..=target).zip(MIGRATIONS).skip(version as usize) {
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
    /// Calls to [`Blobs::verify`] so far.
    #[cfg(test)]
    verifies: std::sync::atomic::AtomicU64,
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
        // A crash can leave the temp file of a blob write that was never
        // renamed. No blob name points at it, and the data directory lock
        // keeps out every other writer, so it can go.
        let mut removed = 0;
        for entry in fs::read_dir(&tmp)? {
            fs::remove_file(entry?.path())?;
            removed += 1;
        }
        if removed > 0 {
            tracing::info!(removed, "removed temp files that a crash left behind");
        }
        Ok(Self {
            root,
            tmp,
            group_flush: GroupFlush::default(),
            #[cfg(test)]
            verifies: std::sync::atomic::AtomicU64::default(),
        })
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

    /// Starts an upload: content that arrives in parts.
    ///
    /// # Errors
    ///
    /// Fails if the spool file cannot be made.
    pub fn upload(&self) -> Result<Upload> {
        Ok(Upload {
            spool: tempfile::tempfile_in(&self.tmp)?,
            hasher: BlobHasher::default(),
            len: 0,
        })
    }

    /// Stores a finished upload durably, like [`Blobs::put`]. The content
    /// streams from the spool file through the compressor, so it is never all
    /// in memory. Safe to repeat.
    ///
    /// # Errors
    ///
    /// Fails if a read or write fails.
    pub fn put_upload(&self, upload: Upload) -> Result<BlobId> {
        let Upload {
            mut spool,
            hasher,
            len,
        } = upload;
        let id = hasher.finalize();
        spool.rewind()?;
        let read_bytes = zstd::zstd_safe::CCtx::in_size();
        let mut batch = self.batch();
        batch.write(id, len, io::BufReader::with_capacity(read_bytes, spool))?;
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

    /// Opens a blob's file.
    fn open_file(&self, id: BlobId) -> Result<fs::File> {
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
            BlobStatus::Valid => return Ok(()),
            // The rename below replaces it in one step.
            BlobStatus::Corrupt => tracing::warn!(blob = %id, "rewriting a corrupt blob file"),
            BlobStatus::Missing => {}
        }
        let (dir, path) = self.blobs.location(id);
        // Write a temp file and flush it before the rename, so the name never
        // points at data that is not on the drive. The frame header holds the
        // size, for `Blobs::size`. Compression fails if `raw` does not give
        // exactly `len` bytes.
        let mut tmp = tempfile::NamedTempFile::new_in(&self.blobs.tmp)?;
        zstd::stream::compress_sized_into(raw, &mut tmp, 3, len as u64)?;
        fsync(tmp.as_file())?;
        fs::create_dir_all(&dir)?;
        tmp.persist(&path).map_err(|e| e.error)?;
        self.dirs.insert(dir);
        Ok(())
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
        if self.dirs.is_empty() {
            return Ok(());
        }
        for dir in &self.dirs {
            fsync_dir(dir)?;
        }
        self.blobs.full_flush()?;
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
        fs::read_dir(&blobs.tmp).unwrap().count()
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
                .put_upload(fill(&blobs, parts.iter().copied()).unwrap())
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
        let id = blobs.put_upload(upload).unwrap();
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
        // A crash after the compressed file is written, before its rename.
        let mut torn = tempfile::NamedTempFile::new_in(&store.blobs().tmp).unwrap();
        torn.write_all(&zstd::bulk::compress(content, 3).unwrap())
            .unwrap();
        torn.keep().unwrap();
        assert_eq!(staged(store.blobs()), 1, "the spool file has no name");
        drop(store);

        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.blobs().verify(id).unwrap(), BlobStatus::Missing);
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
                        // All store at once, so their writes overlap.
                        ready.wait();
                        blobs.put_upload(upload)
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

    fn create_repo(c: &Connection) -> Result<()> {
        crate::db::create_repo(c, "main").map(drop)
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
}
