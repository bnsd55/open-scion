//! Where a tier keeps objects: in memory ([`MemoryStore`], `scion-cache`) or
//! on disk ([`DiskStore`], `scion-daemon`). Both keep at most a set number of
//! object bytes, and drop the least recently used objects first.
//!
//! A store holds each object in the form a tier keeps it (see [`Key`]): a
//! tree as its `TreeV1` bytes, a blob as one zstd frame. Its byte limit
//! counts those bytes. A store only holds objects that were checked against
//! their IDs. The disk store checks each object again when it reads it back.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, ErrorKind, Read as _, Seek as _, SeekFrom, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, FileExt as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError, TryLockError};
use std::time::SystemTime;

use hashlink::LruCache;
use rustix::fs::FlockOperation;
use scion_core::{BlobId, TreeId};
use scion_proto::Bytes;
use scion_proto::object::MAX_ENCODED_BYTES;

use crate::Key;

/// Object storage for a tier. Calls may block; the tier makes them on a
/// blocking thread.
pub trait Store: Send + Sync + 'static {
    /// The size of a stored object in its stored form, without reading it.
    /// `None` if it is not stored. Must be quick: a tier calls it while it
    /// holds its table of fetches.
    ///
    /// # Errors
    ///
    /// I/O errors.
    fn size(&self, key: Key) -> io::Result<Option<usize>>;

    /// The stored form of an object whose size [`Store::size`] gave as
    /// `size`. Returns at most `size` bytes. `None` if it is not stored, or
    /// if the stored copy is bad (it is then dropped).
    ///
    /// # Errors
    ///
    /// I/O errors.
    fn get(&self, key: Key, size: usize) -> io::Result<Option<Bytes>>;

    /// Stores an object in its stored form. The caller has checked `data`
    /// against `key`. An object larger than the store's limit is not kept.
    ///
    /// # Errors
    ///
    /// I/O errors.
    fn put(&self, key: Key, data: &Bytes) -> io::Result<()>;
}

/// Objects by key, least recently used first, each with its size.
struct Lru<V> {
    entries: LruCache<Key, (V, usize)>,
    /// Sum of the sizes in `entries`.
    bytes: usize,
}

impl<V> Lru<V> {
    fn new() -> Self {
        Self {
            entries: LruCache::new_unbounded(),
            bytes: 0,
        }
    }

    /// Looks up `key` and marks it as just used.
    fn get(&mut self, key: Key) -> Option<&(V, usize)> {
        self.entries.get(&key)
    }

    /// Adds or replaces `key`, as the most recently used.
    fn insert(&mut self, key: Key, value: V, size: usize) {
        if let Some((_, old)) = self.entries.insert(key, (value, size)) {
            self.bytes -= old;
        }
        self.bytes += size;
    }

    /// Drops the least recently used entries until the total is at most
    /// `limit`. Returns the keys it dropped.
    fn trim(&mut self, limit: usize) -> Vec<Key> {
        let mut dropped = Vec::new();
        while self.bytes > limit {
            let Some((key, (_, size))) = self.entries.remove_lru() else {
                break;
            };
            self.bytes -= size;
            dropped.push(key);
        }
        dropped
    }

    /// Removes `key`. Returns whether it was there.
    fn remove(&mut self, key: Key) -> bool {
        let Some((_, size)) = self.entries.remove(&key) else {
            return false;
        };
        self.bytes -= size;
        true
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Nothing panics while the lock is held.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Objects in memory, up to a byte limit. Blobs stay packed, as on disk, so
/// the limit holds more of them and a client that accepts zstd gets them
/// without work.
pub struct MemoryStore {
    limit: usize,
    lru: Mutex<Lru<Bytes>>,
}

impl MemoryStore {
    /// An empty store that keeps at most `limit` bytes of objects. Per-object
    /// overhead is not counted.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            lru: Mutex::new(Lru::new()),
        }
    }
}

impl Store for MemoryStore {
    fn size(&self, key: Key) -> io::Result<Option<usize>> {
        Ok(lock(&self.lru).get(key).map(|&(_, size)| size))
    }

    /// The stored buffer itself, not a copy.
    fn get(&self, key: Key, size: usize) -> io::Result<Option<Bytes>> {
        let data = lock(&self.lru).get(key).map(|(data, _)| data.clone());
        Ok(data.filter(|data| data.len() <= size))
    }

    fn put(&self, key: Key, data: &Bytes) -> io::Result<()> {
        if data.len() <= self.limit {
            let mut lru = lock(&self.lru);
            lru.insert(key, data.clone(), data.len());
            lru.trim(self.limit);
        }
        Ok(())
    }
}

/// The name of the index file in the data directory (see [`DiskStore`]).
const INDEX: &str = "index";

/// The start of the index file, with its format version. The header goes on
/// with the state byte ([`CLEAN`] or [`DIRTY`]) and the BLAKE3 hash of the
/// records that the last full write wrote. A cut file fails the hash too.
const MAGIC: &[u8; 8] = b"scnidx1\n";

/// Bytes before the first record.
const HEADER: usize = MAGIC.len() + 1 + 32;

/// The index file names every object file, and is on disk: a clean stop
/// wrote it, and its records are the ones that its header hashes.
const CLEAN: u8 = b'c';

/// The index file may lag the object files: the store is open, or was when
/// it stopped.
const DIRTY: u8 = b'd';

/// Bytes of one record: an op, a 32-byte ID and a size.
const RECORD: usize = 1 + 32 + 4;

/// Bytes of records that wait in memory before they are appended to the
/// index file: 1,024 records. While another call writes, more wait.
const FLUSH_BYTES: usize = 1024 * RECORD;

/// Once the index file would hold more records than twice the objects plus
/// this, it is written again with one record per object.
const SLACK_RECORDS: usize = 1 << 16;

// A record's size field holds the largest stored form.
const _: () = assert!(MAX_ENCODED_BYTES <= u32::MAX as usize);

/// Appends the record that `key` is stored with `size` bytes and was just
/// used (`Some`), or that it was dropped (`None`).
fn push_record(out: &mut Vec<u8>, key: Key, size: Option<usize>) {
    let kind = match key {
        Key::Tree(_) => b'T',
        Key::Blob(_) => b'B',
    };
    // A size that does not fit reads back as damage.
    let (op, size) = match size {
        Some(size) => (kind, u32::try_from(size).unwrap_or(u32::MAX)),
        None => (kind.to_ascii_lowercase(), 0),
    };
    out.push(op);
    out.extend_from_slice(&key.id());
    out.extend_from_slice(&size.to_le_bytes());
}

/// Reads one record: an object stored with `Some(size)` bytes and just used,
/// or one dropped (`None`). `None` overall if the bytes are not a record.
fn parse_record(record: &[u8; RECORD]) -> Option<(Key, Option<usize>)> {
    let [op, id @ .., s0, s1, s2, s3] = *record;
    let key = match op.to_ascii_uppercase() {
        b'T' => Key::Tree(TreeId::from_bytes(id)),
        b'B' => Key::Blob(BlobId::from_bytes(id)),
        _ => return None,
    };
    if op.is_ascii_lowercase() {
        return Some((key, None));
    }
    let size = u32::from_le_bytes([s0, s1, s2, s3]) as usize;
    (size <= MAX_ENCODED_BYTES).then_some((key, Some(size)))
}

/// One record per object of `lru`, least recently used first.
fn rows(lru: &Lru<()>) -> Vec<u8> {
    let mut rows = Vec::with_capacity(lru.entries.len() * RECORD);
    for (&key, &((), size)) in &lru.entries {
        push_record(&mut rows, key, Some(size));
    }
    rows
}

/// What a start found in the index file.
#[derive(Debug, PartialEq)]
enum Found {
    /// A clean file of this many records: the start trusts it.
    Clean(usize),
    /// Why the start scans every object file.
    Scan(&'static str),
}

/// Reads the index file in `dir`: its objects, least recently used first,
/// and whether the start may trust it. A damaged file gives what its records
/// before the damage give.
fn read_index(dir: &Path) -> io::Result<(Lru<()>, Found)> {
    let mut lru = Lru::new();
    let bytes = match fs::read(dir.join(INDEX)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return Ok((lru, Found::Scan("there is no index file")));
        }
        Err(e) => return Err(e),
    };
    let header = bytes
        .split_first_chunk::<HEADER>()
        .and_then(|(header, body)| {
            let (magic, rest) = header.split_first_chunk::<8>()?;
            let ([state], hash) = rest.split_first_chunk::<1>()?;
            (magic == MAGIC).then_some((*state, hash, body))
        });
    let Some((state, hash, body)) = header else {
        return Ok((lru, Found::Scan("the index file has an unknown header")));
    };
    let (records, torn) = body.as_chunks::<RECORD>();
    let mut found = if state != CLEAN {
        Found::Scan("the index file is dirty: the last run did not stop cleanly")
    } else if !torn.is_empty() || blake3::hash(body).as_bytes() != hash {
        Found::Scan("the clean index file does not match its header")
    } else {
        Found::Clean(records.len())
    };
    for record in records {
        match parse_record(record) {
            Some((key, Some(size))) => lru.insert(key, (), size),
            Some((key, None)) => {
                lru.remove(key);
            }
            None => {
                found = Found::Scan("the index file is damaged");
                break;
            }
        }
    }
    Ok((lru, found))
}

/// A plain `fsync`: data reaches the drive, but on macOS not past its cache
/// (`File::sync_all` is a full cache flush there). Used before a full flush,
/// which takes it the rest of the way.
fn fsync(dir: &Path) -> io::Result<()> {
    rustix::fs::fsync(fs::File::open(dir)?).map_err(Into::into)
}

/// Writes an index file of `state` with the records `rows`, in place of the
/// one in `dir`. With `sync`, the file and its name are past the drive's
/// cache when it returns.
fn write_index(dir: &Path, tmp: &Path, state: u8, rows: &[u8], sync: bool) -> io::Result<Log> {
    let mut file = tempfile::NamedTempFile::new_in(tmp)?;
    file.write_all(MAGIC)?;
    file.write_all(&[state])?;
    file.write_all(blake3::hash(rows).as_bytes())?;
    file.write_all(rows)?;
    if sync {
        file.as_file().sync_all()?;
    }
    let file = file.persist(dir.join(INDEX)).map_err(|e| e.error)?;
    if sync {
        fs::File::open(dir)?.sync_all()?;
    }
    let records = rows.len() / RECORD;
    Ok(Log { file, records })
}

/// Opens the clean index file in `dir`, of `records` records, to append to
/// it. First marks it [`DIRTY`], past the drive's cache, so a crash from here
/// on is seen.
fn reopen_index(dir: &Path, records: usize) -> io::Result<Log> {
    let mut file = fs::OpenOptions::new().write(true).open(dir.join(INDEX))?;
    file.write_all_at(&[DIRTY], MAGIC.len() as u64)?;
    file.sync_all()?;
    file.seek(SeekFrom::End(0))?;
    Ok(Log { file, records })
}

/// Every object file under `objects`, with when it was written and its size.
/// Removes a file larger than any object.
fn scan(objects: &Path) -> io::Result<HashMap<Key, (SystemTime, usize)>> {
    let mut found = HashMap::new();
    for kind in ["tree", "blob"] {
        let fans = match fs::read_dir(objects.join(kind)) {
            Ok(fans) => fans,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for fan in fans {
            let fan = fan?;
            if !fan.file_type()?.is_dir() {
                continue;
            }
            for file in fs::read_dir(fan.path())? {
                let file = file?;
                let hex = format!(
                    "{}{}",
                    fan.file_name().to_string_lossy(),
                    file.file_name().to_string_lossy()
                );
                let key = match kind {
                    "tree" => hex.parse().map(Key::Tree),
                    _ => hex.parse().map(Key::Blob),
                };
                let meta = file.metadata()?;
                // Larger than any object: never a good copy.
                if meta.len() > MAX_ENCODED_BYTES as u64 {
                    ignore_missing(fs::remove_file(file.path()))?;
                    continue;
                }
                if let Ok(key) = key
                    && meta.is_file()
                {
                    let size = usize::try_from(meta.len()).unwrap_or(usize::MAX);
                    found.insert(key, (meta.modified()?, size));
                }
            }
        }
    }
    Ok(found)
}

/// Makes `index` agree with the object `files`, each with when it was
/// written and its size. Keeps the order of `index` for the files that it
/// names, and leaves out what it names without a file. The files that it
/// does not name come last, as the most recently used, oldest write first:
/// a crash came before their records reached the index file. Sizes are
/// those of the files.
fn reconcile(index: Lru<()>, mut files: HashMap<Key, (SystemTime, usize)>) -> Lru<()> {
    let mut lru = Lru::new();
    for (key, _) in index.entries {
        if let Some((_, size)) = files.remove(&key) {
            lru.insert(key, (), size);
        }
    }
    let mut newer: Vec<_> = files.into_iter().collect();
    newer.sort_by_key(|&(_, (modified, _))| modified);
    for (key, (_, size)) in newer {
        lru.insert(key, (), size);
    }
    lru
}

/// What the disk store's index lock guards. Every change to `lru` goes
/// through these methods, which note it in `pending`, so reading the index
/// file's records in order gives `lru`.
struct Disk {
    /// The stored objects, least recently used first.
    lru: Lru<()>,
    /// Records that the index file does not have yet, oldest first.
    pending: Vec<u8>,
    /// The file of a dropped object could not leave its name, so a file may
    /// be in place that `lru` does not name. The index file that a stop
    /// writes is then not clean.
    stray: bool,
    /// The directories that a dropped file left since the start. A stop
    /// syncs them before it writes a clean index file, so no dropped file
    /// comes back after a crash.
    left: HashSet<PathBuf>,
}

impl Disk {
    /// The size of `key`, which is marked as just used.
    fn touch(&mut self, key: Key) -> Option<usize> {
        let size = self.lru.get(key).map(|&((), size)| size)?;
        push_record(&mut self.pending, key, Some(size));
        Some(size)
    }

    /// Adds or replaces `key`, stored with `size` bytes, as just used.
    fn insert(&mut self, key: Key, size: usize) {
        self.lru.insert(key, (), size);
        push_record(&mut self.pending, key, Some(size));
    }

    /// Removes `key`, if it is there.
    fn remove(&mut self, key: Key) {
        if self.lru.remove(key) {
            push_record(&mut self.pending, key, None);
        }
    }

    /// Drops the least recently used objects until the total is at most
    /// `limit`. Returns their keys.
    fn trim(&mut self, limit: usize) -> Vec<Key> {
        let keys = self.lru.trim(limit);
        for &key in &keys {
            push_record(&mut self.pending, key, None);
        }
        keys
    }
}

/// The index file, written up to its end, and how many records it holds.
struct Log {
    file: fs::File,
    records: usize,
}

/// Objects on disk, one file per object, named by its ID, up to a byte limit:
///
/// ```text
/// <data>/lock                   locked by the one process that uses <data>
/// <data>/index                  the stored objects, in order of use
/// <data>/objects/tree/ab/cd..   a tree's TreeV1 bytes
/// <data>/objects/blob/ab/cd..   one zstd frame of a blob's content
/// <data>/tmp/                   staging for atomic writes, emptied at start
/// ```
///
/// A write goes to a temp file and is renamed into place, so a reader never
/// sees half a file. Files are not synced: a crash can leave a bad file, and
/// the hash check on read catches it. The limit counts object bytes, not the
/// disk blocks that hold them. Every rename and unlink of an object file
/// happens under the index lock, so the index and the files agree.
///
/// The index is in memory, and in `<data>/index` so that a start need not
/// open every object directory. The file is a header (see [`MAGIC`]) and
/// records of [`RECORD`] bytes: an op, an ID and a size (`u32`,
/// little-endian). `T` or `B` says that a tree or a blob of that size is
/// stored and was just used, `t` or `b` that it was dropped. Read in order,
/// the records give the stored objects, least recently used first. Records
/// wait in memory, and are appended in batches of [`FLUSH_BYTES`]. When the
/// file would hold far more records than objects (see [`SLACK_RECORDS`]),
/// it is written again with one record per object.
///
/// A stop syncs the directories that dropped files left, then writes the
/// file again, one record per object, marked [`CLEAN`], with the hash of its
/// records in the header, past the drive's cache. A start trusts a clean
/// file whose records match that hash, for which objects are stored
/// and in what order, and opens no object directory. At open it marks the
/// file [`DIRTY`], past the drive's cache. After a crash the file is dirty:
/// it may lack its last records, so it may name a file that is gone, or
/// miss a new one. The start then scans every object file, as with no
/// index, and keeps the order of the file for the objects that it names. So
/// a crash loses only what was used last, never an object. No start trusts
/// the file for an object's bytes: [`Store::get`] checks them on every read.
///
/// A daemon build from before the index does not know the file. Run between
/// a clean stop and the next start, it leaves a clean file that is stale:
/// remove `<data>/index` after such a run.
// SHORTCUT: the whole index is also in memory, a start reads all of it, and
// writing the file again copies every record under the index lock, which
// `Store::size` needs under the tier's fetch table: every fetch start waits.
// After a crash, a start scans every object file. Memory and these pauses
// grow with the number of objects. Upgrade (issue #115): an index on disk
// that is read and changed in place.
pub struct DiskStore {
    dir: PathBuf,
    objects: PathBuf,
    tmp: PathBuf,
    limit: usize,
    disk: Mutex<Disk>,
    /// Set with `disk` held once it has [`FLUSH_BYTES`] of records, so a read
    /// takes that lock only when there is a batch to write.
    flush_due: AtomicBool,
    /// Taken with `disk` held, by the one call that writes the index file,
    /// and let go of after the write. So records reach the file in order,
    /// and no write to it holds `disk`. `None` when the last write failed:
    /// the next one writes the whole file again.
    log: Mutex<Option<Log>>,
    /// Names files that the limit drops, in `tmp/`.
    dropped: AtomicU64,
    _lock: fs::File,
}

impl DiskStore {
    /// Opens the store in `dir`, keeping at most `limit` bytes of objects.
    /// Creates `dir` with mode 0700 if it is missing. Refuses a `dir` that
    /// other users can reach (not a real directory owned by this user and
    /// closed to others): only this user may read the objects or place files
    /// there. If the stored objects add up to more than `limit`, the least
    /// recently used are dropped.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::PermissionDenied`] if `dir` is not private,
    /// [`ErrorKind::WouldBlock`] if another process uses `dir`, or I/O errors.
    pub fn open(dir: &Path, limit: usize) -> io::Result<Self> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        // The checked directory, so no symlink in `dir` is followed again.
        let dir = &crate::check_private_dir(dir)?;
        let lock_file = lock_dir(dir)?;
        let objects = dir.join("objects");
        let tmp = dir.join("tmp");
        // Temp files left by a crash. The lock keeps other processes out.
        ignore_missing(fs::remove_dir_all(&tmp))?;
        fs::create_dir_all(&tmp)?;
        fs::create_dir_all(&objects)?;
        let (index, found) = read_index(dir)?;
        let lru = match found {
            Found::Clean(_) => index,
            Found::Scan(_) => reconcile(index, scan(&objects)?),
        };
        let store = Self {
            dir: dir.clone(),
            objects,
            tmp,
            limit,
            disk: Mutex::new(Disk {
                lru,
                pending: Vec::new(),
                stray: false,
                left: HashSet::new(),
            }),
            flush_due: AtomicBool::new(false),
            log: Mutex::new(None),
            dropped: AtomicU64::default(),
            _lock: lock_file,
        };
        // Drops first: they free room for the index file.
        let mut disk = lock(&store.disk);
        let mut dropped = Vec::new();
        let evicted = store.evict(&mut disk, &mut dropped);
        let written = match found {
            // A clean file holds one record per object: a stop wrote it so.
            Found::Clean(records) => reopen_index(dir, records),
            Found::Scan(_) => {
                disk.pending.clear();
                write_index(dir, &store.tmp, DIRTY, &rows(&disk.lru), true)
            }
        };
        match written {
            Ok(log) => *lock(&store.log) = Some(log),
            // A clean file must not stay: a crash would leave it trusted.
            Err(e) => {
                tracing::warn!(error = %e, "cannot write the index file; removing it");
                ignore_missing(fs::remove_file(dir.join(INDEX)))?;
                fs::File::open(dir)?.sync_all()?;
            }
        }
        let (objects, bytes) = (disk.lru.entries.len(), disk.lru.bytes);
        drop(disk);
        remove_all(dropped)?;
        evicted?;
        match found {
            Found::Clean(_) => {
                tracing::info!(
                    objects,
                    bytes,
                    "opened the disk store from its clean index file"
                );
            }
            Found::Scan(why) => {
                tracing::info!(
                    objects,
                    bytes,
                    why,
                    "opened the disk store by a scan of every file"
                );
            }
        }
        Ok(store)
    }

    /// Whether the store holds no object.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        lock(&self.disk).lru.entries.is_empty()
    }

    fn path(&self, key: Key) -> PathBuf {
        let (kind, hex) = match key {
            Key::Tree(id) => ("tree", id.to_string()),
            Key::Blob(id) => ("blob", id.to_string()),
        };
        self.objects.join(kind).join(&hex[..2]).join(&hex[2..])
    }

    /// Records a stored file in the index as just used. The caller holds the
    /// index lock. Then drops the objects over the limit (see
    /// [`DiskStore::evict`]).
    fn add(
        &self,
        disk: &mut Disk,
        key: Key,
        size: usize,
        dropped: &mut Vec<PathBuf>,
    ) -> io::Result<()> {
        disk.insert(key, size);
        self.evict(disk, dropped)
    }

    /// Drops the least recently used objects until the index holds at most
    /// the limit. The caller holds the index lock. Their files leave their
    /// names now (see [`DiskStore::take_out`]) and go into `dropped`, for the
    /// caller to remove once it has let go of the lock. Every file is tried;
    /// the first error is returned at the end.
    fn evict(&self, disk: &mut Disk, dropped: &mut Vec<PathBuf>) -> io::Result<()> {
        let mut first = None;
        for key in disk.trim(self.limit) {
            match self.take_out(disk, key) {
                Ok(file) => dropped.extend(file),
                Err(e) => {
                    first.get_or_insert(e);
                }
            }
        }
        if disk.pending.len() >= FLUSH_BYTES {
            self.flush_due.store(true, Ordering::Relaxed);
        }
        first.map_or(Ok(()), Err)
    }

    /// Moves the file of `key`, just left out of the index, out of its name,
    /// into `tmp/`, under the index lock, so that a later put of the same
    /// object keeps its new file. The caller removes the returned file once
    /// it has let go of the lock: a rename is quick, but removing a large
    /// file may not be. `None` if there is no file. `disk` notes the
    /// directory that the file left, or, on an error, that the file may stay
    /// in place.
    fn take_out(&self, disk: &mut Disk, key: Key) -> io::Result<Option<PathBuf>> {
        let n = self.dropped.fetch_add(1, Ordering::Relaxed);
        let to = self.tmp.join(format!("dropped-{n}"));
        let from = self.path(key);
        match fs::rename(&from, &to) {
            Ok(()) => {
                if let Some(fan) = from.parent() {
                    disk.left.insert(fan.to_owned());
                }
                Ok(Some(to))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => {
                disk.stray = true;
                Err(e)
            }
        }
    }

    /// Appends the waiting records to the index file once there are
    /// [`FLUSH_BYTES`] of them, or writes the whole file again once it would
    /// grow past [`SLACK_RECORDS`]. Only what a crash keeps depends on this,
    /// so an error is logged, not returned.
    fn flush(&self) {
        if !self.flush_due.load(Ordering::Relaxed) {
            return;
        }
        let mut disk = lock(&self.disk);
        if disk.pending.len() < FLUSH_BYTES {
            return;
        }
        let mut log = match self.log.try_lock() {
            Ok(log) => log,
            Err(TryLockError::Poisoned(e)) => e.into_inner(),
            // Another call writes. It, or a later one, takes these too.
            Err(TryLockError::WouldBlock) => return,
        };
        self.flush_due.store(false, Ordering::Relaxed);
        let batch = std::mem::replace(&mut disk.pending, Vec::with_capacity(FLUSH_BYTES));
        let most = 2 * disk.lru.entries.len() + SLACK_RECORDS;
        let written = match log.as_mut() {
            Some(open) if open.records + batch.len() / RECORD <= most => {
                drop(disk);
                let written = open.file.write_all(&batch);
                open.records += batch.len() / RECORD;
                written
            }
            _ => {
                let rows = rows(&disk.lru);
                drop(disk);
                write_index(&self.dir, &self.tmp, DIRTY, &rows, false).map(|new| *log = Some(new))
            }
        };
        if let Err(e) = written {
            // The file may hold part of a record now.
            *log = None;
            tracing::warn!(error = %e, "cannot write the index file; a crash now loses what was used last");
        }
    }
}

impl Drop for DiskStore {
    /// Syncs the directories that dropped files left, then writes the index
    /// file again, one record per object, past the drive's cache. If it names
    /// every object file, it is clean, and the next start scans none.
    fn drop(&mut self) {
        let disk = self.disk.get_mut().unwrap_or_else(PoisonError::into_inner);
        let synced = disk
            .left
            .iter()
            .chain([&self.tmp])
            .try_for_each(|dir| fsync(dir));
        if let Err(e) = &synced {
            tracing::warn!(error = %e, "cannot sync the directories of dropped files");
        }
        let state = if disk.stray || synced.is_err() {
            DIRTY
        } else {
            CLEAN
        };
        let (objects, rows) = (disk.lru.entries.len(), rows(&disk.lru));
        match write_index(&self.dir, &self.tmp, state, &rows, true) {
            Ok(_) => tracing::info!(objects, clean = state == CLEAN, "wrote the index file"),
            Err(e) => tracing::warn!(
                error = %e,
                "cannot write the index file; the next start scans every object file"
            ),
        }
    }
}

impl Store for DiskStore {
    fn size(&self, key: Key) -> io::Result<Option<usize>> {
        let mut disk = lock(&self.disk);
        let size = disk.touch(key);
        if disk.pending.len() >= FLUSH_BYTES {
            self.flush_due.store(true, Ordering::Relaxed);
        }
        Ok(size)
    }

    fn get(&self, key: Key, size: usize) -> io::Result<Option<Bytes>> {
        self.flush();
        let path = self.path(key);
        let mut data = Vec::with_capacity(size + 1);
        match fs::File::open(&path) {
            // One byte more than `size`, to see that the file is longer.
            Ok(file) => file.take(size as u64 + 1).read_to_end(&mut data)?,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let mut disk = lock(&self.disk);
                // A put may have renamed the file into place since.
                if !path.exists() {
                    disk.remove(key);
                }
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let checked = if data.len() == size {
            key.check_stored(&data)
        } else {
            Err(format!("{} bytes, not {size}", data.len()))
        };
        if let Err(why) = checked {
            tracing::warn!(%key, %why, "dropping a stored copy that does not match its ID");
            let mut disk = lock(&self.disk);
            disk.remove(key);
            let file = self.take_out(&mut disk, key);
            drop(disk);
            remove_all(file?.into_iter().collect())?;
            return Ok(None);
        }
        Ok(Some(Bytes::from(data)))
    }

    fn put(&self, key: Key, data: &Bytes) -> io::Result<()> {
        if data.len() > self.limit {
            return Ok(());
        }
        self.flush();
        let path = self.path(key);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let mut tmp = tempfile::NamedTempFile::new_in(&self.tmp)?;
        tmp.write_all(data)?;
        // Closed here, outside the lock; only the rename is under it.
        let tmp = tmp.into_temp_path();
        let mut disk = lock(&self.disk);
        // The rename replaces a bad copy, if there is one, in one step.
        tmp.persist(&path).map_err(|e| e.error)?;
        let mut dropped = Vec::new();
        let added = self.add(&mut disk, key, data.len(), &mut dropped);
        drop(disk);
        remove_all(dropped)?;
        added
    }
}

/// Removes files that the limit dropped.
fn remove_all(files: Vec<PathBuf>) -> io::Result<()> {
    for file in files {
        ignore_missing(fs::remove_file(file))?;
    }
    Ok(())
}

/// `Ok` if `result` failed only because the path is missing.
fn ignore_missing(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// Takes an exclusive lock on `<dir>/lock` without waiting. The lock lasts as
/// long as the returned file is open.
fn lock_dir(dir: &Path) -> io::Result<fs::File> {
    let path = dir.join("lock");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)?;
    match rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(file),
        Err(rustix::io::Errno::WOULDBLOCK) => Err(io::Error::new(
            ErrorKind::WouldBlock,
            format!("{} is in use by another daemon", dir.display()),
        )),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use scion_core::{BlobId, Tree};
    use scion_proto::object;

    use super::*;

    /// A blob's key and stored form: one zstd frame of `data`.
    fn blob(data: &[u8]) -> (Key, Bytes) {
        let frame = object::pack(data).unwrap();
        (Key::Blob(BlobId::of(data)), Bytes::from(frame))
    }

    #[test]
    fn the_memory_store_drops_the_least_recently_used_at_its_limit() {
        let (a, b, c) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"cccc"));
        // Each stored form is `n` bytes. Room for two, not three.
        let n = a.1.len();
        let store = MemoryStore::new(2 * n + n / 2);
        store.put(a.0, &a.1).unwrap();
        store.put(b.0, &b.1).unwrap();
        // Reading `a` makes `b` the least recently used.
        assert_eq!(store.get(a.0, n).unwrap(), Some(a.1.clone()));
        // Three are over the limit: `b` goes.
        store.put(c.0, &c.1).unwrap();
        assert_eq!(store.size(a.0).unwrap(), Some(n));
        assert_eq!(store.size(b.0).unwrap(), None);
        assert_eq!(store.size(c.0).unwrap(), Some(n));
        assert_eq!(lock(&store.lru).bytes, 2 * n);

        // A second put of the same object counts it once.
        store.put(c.0, &c.1).unwrap();
        assert_eq!(lock(&store.lru).bytes, 2 * n);

        // An object larger than the whole limit is not kept, and drops nothing.
        let big = (
            Key::Blob(BlobId::of(b"big")),
            Bytes::from(vec![b'x'; 3 * n]),
        );
        store.put(big.0, &big.1).unwrap();
        assert_eq!(store.size(big.0).unwrap(), None);
        assert_eq!(store.size(a.0).unwrap(), Some(n));
    }

    /// Total size of the object files under `dir`.
    fn bytes_on_disk(dir: &Path) -> usize {
        let mut total = 0;
        let mut dirs = vec![dir.join("objects")];
        while let Some(dir) = dirs.pop() {
            for entry in fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let meta = entry.metadata().unwrap();
                if meta.is_dir() {
                    dirs.push(entry.path());
                } else {
                    total += usize::try_from(meta.len()).unwrap();
                }
            }
        }
        total
    }

    #[test]
    fn the_disk_store_stays_under_its_limit_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b, c) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"cccc"));
        let n = a.1.len();
        let limit = 2 * n + n / 2;
        {
            let store = DiskStore::open(&data, limit).unwrap();
            // One process per data directory.
            let err = DiskStore::open(&data, limit).err().unwrap();
            assert_eq!(err.kind(), ErrorKind::WouldBlock);
            store.put(a.0, &a.1).unwrap();
            store.put(b.0, &b.1).unwrap();
            // Reading `a` makes `b` the least recently used.
            assert_eq!(store.size(a.0).unwrap(), Some(n));
            store.put(c.0, &c.1).unwrap();
            assert_eq!(store.size(b.0).unwrap(), None);
            assert!(!store.path(b.0).exists());
            assert_eq!(bytes_on_disk(&data), 2 * n);
            // A temp file left by a crash.
            fs::write(store.tmp.join("torn"), b"aa").unwrap();
        }

        // A restart finds what is on disk, and empties tmp/.
        let store = DiskStore::open(&data, limit).unwrap();
        assert_eq!(store.get(a.0, n).unwrap(), Some(a.1.clone()));
        assert_eq!(store.get(c.0, n).unwrap(), Some(c.1.clone()));
        assert_eq!(store.size(b.0).unwrap(), None);
        assert_eq!(fs::read_dir(&store.tmp).unwrap().count(), 0);
        drop(store);

        // A lower limit drops objects at start.
        let store = DiskStore::open(&data, n).unwrap();
        assert_eq!(bytes_on_disk(&data), n);
        let kept = [a.0, c.0].map(|key| store.size(key).unwrap());
        assert_eq!(kept.iter().flatten().count(), 1, "{kept:?}");
    }

    #[test]
    fn the_disk_store_drops_bad_copies() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        let mode = fs::metadata(&data).unwrap().permissions();
        assert_eq!(
            std::os::unix::fs::PermissionsExt::mode(&mode) & 0o777,
            0o700
        );

        let (blob_key, frame) = blob(b"hello\n");
        let n = frame.len();
        let (tree_bytes, tree_id) = Tree::default().encode_with_id();
        let tree_key = Key::Tree(tree_id);
        store.put(blob_key, &frame).unwrap();
        store
            .put(tree_key, &Bytes::from(tree_bytes.clone()))
            .unwrap();
        assert_eq!(store.size(blob_key).unwrap(), Some(n));
        assert_eq!(store.get(blob_key, n).unwrap(), Some(frame.clone()));
        let tree_size = tree_bytes.len();
        assert_eq!(
            store.get(tree_key, tree_size).unwrap(),
            Some(tree_bytes.into())
        );
        let (missing, _) = blob(b"never stored");
        assert_eq!(store.size(missing).unwrap(), None);
        assert_eq!(store.get(missing, 12).unwrap(), None);

        // A file that is not this blob is never returned, and is removed:
        // the raw content, a frame of other content, or a frame whose header
        // gives another size.
        let mut lying = frame.to_vec();
        // The one-byte content size field of a small single-segment frame.
        lying[5] += 1;
        assert_eq!(object::frame_size(&lying), Ok(7));
        for bad in [
            b"hello\n".to_vec(),
            object::pack(b"hellO\n").unwrap(),
            lying,
        ] {
            fs::write(store.path(blob_key), &bad).unwrap();
            store
                .add(&mut lock(&store.disk), blob_key, bad.len(), &mut Vec::new())
                .unwrap();
            assert_eq!(store.get(blob_key, bad.len()).unwrap(), None);
            assert_eq!(store.size(blob_key).unwrap(), None);
            assert!(!store.path(blob_key).exists());
            // Moved out under the lock, then removed.
            assert_eq!(fs::read_dir(&store.tmp).unwrap().count(), 0);
        }
        // A put replaces a bad copy.
        fs::write(store.path(blob_key), b"torn").unwrap();
        store.put(blob_key, &frame).unwrap();
        assert_eq!(store.get(blob_key, n).unwrap(), Some(frame.clone()));
        // A file longer than its recorded size is not read past it.
        fs::write(store.path(blob_key), [&frame[..], b" and more"].concat()).unwrap();
        assert_eq!(store.get(blob_key, n).unwrap(), None);
    }

    #[test]
    fn a_put_of_an_object_that_was_just_dropped_keeps_its_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (blob(b"aaaa"), blob(b"bbbb"));
        let n = a.1.len();
        // Room for one object.
        let store = DiskStore::open(&dir.path().join("data"), n).unwrap();
        store.put(a.0, &a.1).unwrap();
        // `b` comes in and drops `a`, whose file leaves its name at once.
        let mut dropped = Vec::new();
        store
            .add(&mut lock(&store.disk), b.0, n, &mut dropped)
            .unwrap();
        assert_eq!(dropped.len(), 1);
        assert!(!store.path(a.0).exists());
        // `a` is put again before the dropped file is removed. Removing it
        // then does not touch the new file.
        store.put(a.0, &a.1).unwrap();
        remove_all(dropped).unwrap();
        assert_eq!(store.get(a.0, n).unwrap(), Some(a.1.clone()));
        assert_eq!(fs::read_dir(&store.tmp).unwrap().count(), 0);
    }

    #[test]
    fn a_file_that_cannot_leave_does_not_keep_the_others_in_place() {
        use std::os::unix::fs::PermissionsExt as _;

        // Root may rename out of a read-only directory.
        if rustix::process::geteuid().as_raw() == 0 {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b) = (blob(b"aaaa"), blob(b"bbbb"));
        let n = a.1.len();
        let store = DiskStore::open(&data, 2 * n).unwrap();
        store.put(a.0, &a.1).unwrap();
        store.put(b.0, &b.1).unwrap();
        let fan_out = |key: Key| store.path(key).parent().unwrap().to_path_buf();
        assert_ne!(fan_out(a.0), fan_out(b.0));
        // The file of `a` cannot leave its directory.
        let mode = |m| fs::Permissions::from_mode(m);
        fs::set_permissions(fan_out(a.0), mode(0o500)).unwrap();

        // An object of `2n` bytes drops both: `a` fails, `b` still leaves.
        let big = Key::Blob(BlobId::of(b"big"));
        let mut dropped = Vec::new();
        let added = store.add(&mut lock(&store.disk), big, 2 * n, &mut dropped);
        fs::set_permissions(fan_out(a.0), mode(0o700)).unwrap();
        assert!(added.is_err());
        assert_eq!(dropped.len(), 1);
        assert!(!store.path(b.0).exists());
        remove_all(dropped).unwrap();

        // The file of `a` is still there, and the index does not name it: the
        // stop does not claim that the index names every file.
        drop(store);
        assert_eq!(index_state(&data), DIRTY);
        let store = DiskStore::open(&data, 2 * n).unwrap();
        assert_eq!(store.size(a.0).unwrap(), Some(n));
    }

    /// The index file in `data`.
    fn index_bytes(data: &Path) -> Vec<u8> {
        fs::read(data.join(INDEX)).unwrap()
    }

    /// The state byte of the index file in `data`.
    fn index_state(data: &Path) -> u8 {
        index_bytes(data)[MAGIC.len()]
    }

    /// Writes `index` as the index file in `data`. A copy of the file taken
    /// while a store is open, put back after it stopped, is what a crash at
    /// that moment leaves.
    fn put_back(data: &Path, index: &[u8]) {
        fs::write(data.join(INDEX), index).unwrap();
    }

    /// The stored objects, least recently used first.
    fn order(store: &DiskStore) -> Vec<Key> {
        let disk = lock(&store.disk);
        disk.lru.entries.iter().map(|(&key, _)| key).collect()
    }

    /// Sets the write time of the file of each key, oldest first, so a scan
    /// would order them so. A start that scans no file ignores it.
    fn set_written_in_order(store: &DiskStore, keys: &[Key]) {
        let start = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1 << 30);
        for (i, &key) in (0..).zip(keys) {
            let file = fs::File::options()
                .write(true)
                .open(store.path(key))
                .unwrap();
            let when = start + std::time::Duration::from_secs(i);
            file.set_modified(when).unwrap();
        }
    }

    #[test]
    fn a_start_after_a_clean_stop_reads_the_index_and_scans_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, stray) = (blob(b"aaaa"), blob(b"not in the index"));
        let stray_path = {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            store.put(a.0, &a.1).unwrap();
            store.path(stray.0)
        };
        assert_eq!(index_state(&data), CLEAN);
        // A good object file that the index does not name: only a scan finds
        // it.
        fs::create_dir_all(stray_path.parent().unwrap()).unwrap();
        fs::write(&stray_path, &stray.1).unwrap();
        {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            assert_eq!(store.size(a.0).unwrap(), Some(a.1.len()));
            assert_eq!(store.size(stray.0).unwrap(), None);
            // Dirty from the open on: from then on the file may lag.
            assert_eq!(index_state(&data), DIRTY);
        }

        // A start after a crash scans, and finds it.
        let mut index = index_bytes(&data);
        index[MAGIC.len()] = DIRTY;
        put_back(&data, &index);
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        let n = stray.1.len();
        assert_eq!(store.size(stray.0).unwrap(), Some(n));
        assert_eq!(store.get(stray.0, n).unwrap(), Some(stray.1.clone()));
    }

    #[test]
    fn the_order_of_use_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b, c, d) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"cccc"), blob(b"dddd"));
        let len = a.1.len();
        // Room for three.
        let limit = 3 * len + len / 2;
        {
            let store = DiskStore::open(&data, limit).unwrap();
            for (key, bytes) in [&a, &b, &c] {
                store.put(*key, bytes).unwrap();
            }
            set_written_in_order(&store, &[a.0, b.0, c.0]);
            // `a` is read last, so `b` is the least recently used.
            assert_eq!(store.size(a.0).unwrap(), Some(len));
            assert_eq!(store.get(a.0, len).unwrap(), Some(a.1.clone()));
        }

        let store = DiskStore::open(&data, limit).unwrap();
        assert_eq!(order(&store), [b.0, c.0, a.0]);
        // A fourth object drops `b`, not `a`, the oldest write.
        store.put(d.0, &d.1).unwrap();
        assert_eq!(store.size(b.0).unwrap(), None);
        for key in [a.0, c.0, d.0] {
            assert_eq!(store.size(key).unwrap(), Some(len));
        }
    }

    #[test]
    fn a_start_after_a_crash_makes_the_index_agree_with_the_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b, c) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"cccc"));
        let n = a.1.len();
        // Room for two.
        let limit = 2 * n + n / 2;
        {
            let store = DiskStore::open(&data, limit).unwrap();
            store.put(a.0, &a.1).unwrap();
            store.put(b.0, &b.1).unwrap();
        }
        let mut crashed = {
            let store = DiskStore::open(&data, limit).unwrap();
            let index = index_bytes(&data);
            // `c` drops `a`, then `b` is used. These records never reach the
            // file.
            store.put(c.0, &c.1).unwrap();
            assert!(!store.path(a.0).exists());
            assert_eq!(store.size(b.0).unwrap(), Some(n));
            // By write time alone, `c` would come first.
            set_written_in_order(&store, &[c.0, b.0]);
            index
        };
        // The crash also tore the record it was writing.
        crashed.extend_from_slice(b"B1234");
        put_back(&data, &crashed);

        // The index names `a`, which has no file, and not `c`, which has one.
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        // `b` keeps its place from the index; `c`, which the index does not
        // name, comes after it.
        assert_eq!(order(&store), [b.0, c.0]);
        assert_eq!(lock(&store.disk).lru.bytes, bytes_on_disk(&data));
        assert_eq!(store.get(c.0, n).unwrap(), Some(c.1.clone()));
    }

    #[test]
    fn a_dirty_file_gives_the_order_of_its_records_before_a_bad_one() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b, c) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"cccc"));
        let header = {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            for (key, bytes) in [&a, &b, &c] {
                store.put(*key, bytes).unwrap();
            }
            set_written_in_order(&store, &[a.0, b.0, c.0]);
            index_bytes(&data)[..HEADER].to_vec()
        };
        // Dirty: `c`, a record that is not one, `b`, `a`.
        let mut index = header;
        index[MAGIC.len()] = DIRTY;
        push_record(&mut index, c.0, Some(c.1.len()));
        index.extend_from_slice(&[b'X'; RECORD]);
        push_record(&mut index, b.0, Some(b.1.len()));
        push_record(&mut index, a.0, Some(a.1.len()));
        put_back(&data, &index);

        // `c` from the index; the rest by write time.
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        assert_eq!(order(&store), [c.0, a.0, b.0]);
    }

    #[test]
    fn a_clean_file_that_does_not_match_its_header_is_not_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b, stray) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"not in the index"));
        let stray_path = {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            store.put(a.0, &a.1).unwrap();
            store.put(b.0, &b.1).unwrap();
            store.path(stray.0)
        };
        let good = index_bytes(&data);
        assert_eq!(good[MAGIC.len()], CLEAN);
        // An object file that the index does not name: only a scan finds it.
        fs::create_dir_all(stray_path.parent().unwrap()).unwrap();
        fs::write(&stray_path, &stray.1).unwrap();
        let cut = good[..good.len() - RECORD].to_vec();
        let mut resized = good.clone();
        // The size field of the last record.
        *resized.last_mut().unwrap() ^= 1;
        let mut version = good.clone();
        version[..MAGIC.len()].copy_from_slice(b"scnidx0\n");
        for (case, index, scans) in [
            ("as written", &good, false),
            ("cut at a record", &cut, true),
            ("a record changed", &resized, true),
            ("another version", &version, true),
        ] {
            put_back(&data, index);
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            let found = store.size(stray.0).unwrap().is_some();
            assert_eq!(found, scans, "{case}");
            assert_eq!(store.size(b.0).unwrap(), Some(b.1.len()), "{case}");
        }
    }

    #[test]
    fn an_index_file_that_cannot_be_marked_dirty_is_removed() {
        use std::os::unix::fs::PermissionsExt as _;

        // Root may write a read-only file.
        if rustix::process::geteuid().as_raw() == 0 {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let a = blob(b"aaaa");
        DiskStore::open(&data, 1 << 20)
            .unwrap()
            .put(a.0, &a.1)
            .unwrap();
        let index = data.join(INDEX);
        fs::set_permissions(&index, fs::Permissions::from_mode(0o400)).unwrap();
        // The start goes on without a clean file that a crash would leave.
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        assert!(!index.exists());
        assert_eq!(store.get(a.0, a.1.len()).unwrap(), Some(a.1.clone()));
        drop(store);
        assert_eq!(index_state(&data), CLEAN);
    }

    #[test]
    fn the_index_file_gets_each_batch_and_is_written_again_before_it_grows_large() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b) = (blob(b"aaaa"), blob(b"bbbb"));
        let n = a.1.len();
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        store.put(a.0, &a.1).unwrap();
        store.put(b.0, &b.1).unwrap();
        set_written_in_order(&store, &[a.0, b.0]);
        // Uses of `a`: three times the slack, in batches. A read writes a
        // batch that is full.
        let (mut largest, mut first) = (0, None);
        let batch = FLUSH_BYTES / RECORD;
        for _ in 0..3 * SLACK_RECORDS / batch {
            for _ in 0..batch {
                store.size(a.0).unwrap();
            }
            assert_eq!(store.get(a.0, n).unwrap(), Some(a.1.clone()));
            largest = largest.max(fs::metadata(data.join(INDEX)).unwrap().len());
            first.get_or_insert_with(|| index_bytes(&data));
        }
        // Written again with one record per object, so it never held all of
        // the records.
        let most = HEADER + (2 * 2 + SLACK_RECORDS) * RECORD + FLUSH_BYTES;
        assert!(largest <= most as u64, "{largest} bytes");

        // A crash after the first batch, or now, keeps that `a` was used
        // last.
        let crashes = [first.unwrap(), index_bytes(&data)];
        drop(store);
        for crashed in crashes {
            put_back(&data, &crashed);
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            assert_eq!(order(&store), [b.0, a.0]);
        }
    }

    #[test]
    fn a_torn_file_found_at_start_is_dropped_when_read() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (key, bytes) = blob(b"a whole object\n");
        let path = {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            store.put(key, &bytes).unwrap();
            store.path(key)
        };
        // A crash of the machine left the first bytes only. A clean index
        // gives the size of the object, a scan that of the file: neither is
        // trusted for the bytes.
        let torn = &bytes[..bytes.len() - 1];
        for (scan, size) in [(false, bytes.len()), (true, torn.len())] {
            fs::write(&path, torn).unwrap();
            if scan {
                fs::remove_file(data.join(INDEX)).unwrap();
            }
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            assert_eq!(store.size(key).unwrap(), Some(size));
            assert_eq!(store.get(key, size).unwrap(), None);
            assert_eq!(store.size(key).unwrap(), None);
            assert!(!path.exists());
            assert_eq!(lock(&store.disk).lru.bytes, 0);
        }
    }

    #[test]
    fn a_data_directory_that_others_can_reach_is_refused() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let open = dir.path().join("open");
        fs::create_dir(&open).unwrap();
        fs::set_permissions(&open, fs::Permissions::from_mode(0o777)).unwrap();
        let err = DiskStore::open(&open, 1 << 20).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err}");
        assert!(!open.join("objects").exists());

        // A symlink to a private directory is refused too.
        let private = dir.path().join("private");
        fs::create_dir(&private).unwrap();
        fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&private, &link).unwrap();
        let err = DiskStore::open(&link, 1 << 20).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err}");

        // A directory owned by someone else: the root directory, unless the
        // test runs as root.
        if rustix::process::geteuid().as_raw() != 0 {
            let err = DiskStore::open(Path::new("/"), 1 << 20).err().unwrap();
            assert!(err.to_string().contains("owned by another user"), "{err}");
        }

        // A private directory under one that others can write.
        let err = DiskStore::open(&open.join("data"), 1 << 20).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err}");
        assert!(err.to_string().contains("/open can be written"), "{err}");
    }
}
