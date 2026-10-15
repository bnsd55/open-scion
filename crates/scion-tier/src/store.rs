//! Where a tier keeps objects: in memory ([`MemoryStore`], `scion-cache`) or
//! on disk ([`DiskStore`], `scion-daemon`). Both keep at most a set number of
//! object bytes, and drop the least recently used objects first.
//!
//! A store holds each object in the form a tier keeps it (see [`Key`]): a
//! tree as its `TreeV1` bytes, a blob as one zstd frame. Its byte limit
//! counts those bytes. A store only holds objects that were checked against
//! their IDs. The disk store checks each object again when it reads it back.

use std::fs;
use std::io::{self, ErrorKind, Read as _, Write as _};
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use hashlink::LruCache;
use rustix::fs::FlockOperation;
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
    fn get(&self, key: Key, size: usize) -> io::Result<Option<Vec<u8>>>;

    /// Stores an object in its stored form. The caller has checked `data`
    /// against `key`. An object larger than the store's limit is not kept.
    ///
    /// # Errors
    ///
    /// I/O errors.
    fn put(&self, key: Key, data: &Arc<Vec<u8>>) -> io::Result<()>;
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

    /// Adds or replaces `key`, then drops the least recently used entries
    /// until the total is at most `limit`. Returns the keys it dropped.
    fn insert(&mut self, key: Key, value: V, size: usize, limit: usize) -> Vec<Key> {
        if let Some((_, old)) = self.entries.insert(key, (value, size)) {
            self.bytes -= old;
        }
        self.bytes += size;
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

    fn remove(&mut self, key: Key) {
        if let Some((_, size)) = self.entries.remove(&key) {
            self.bytes -= size;
        }
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
    lru: Mutex<Lru<Arc<Vec<u8>>>>,
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

    fn get(&self, key: Key, size: usize) -> io::Result<Option<Vec<u8>>> {
        let data = lock(&self.lru).get(key).map(|(data, _)| Arc::clone(data));
        // Copied outside the lock.
        Ok(data
            .filter(|data| data.len() <= size)
            .map(|data| data.to_vec()))
    }

    fn put(&self, key: Key, data: &Arc<Vec<u8>>) -> io::Result<()> {
        if data.len() <= self.limit {
            lock(&self.lru).insert(key, Arc::clone(data), data.len(), self.limit);
        }
        Ok(())
    }
}

/// Objects on disk, one file per object, named by its ID, up to a byte limit:
///
/// ```text
/// <data>/lock                   locked by the one process that uses <data>
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
// SHORTCUT: the recency index lives in memory and is rebuilt by a scan of
// every file at start, oldest write first. Memory and start time grow with
// the number of objects, and a restart forgets which objects were read last.
// Upgrade (issue #63): a persistent index.
pub struct DiskStore {
    objects: PathBuf,
    tmp: PathBuf,
    limit: usize,
    lru: Mutex<Lru<()>>,
    _lock: fs::File,
}

impl DiskStore {
    /// Opens the store in `dir`, keeping at most `limit` bytes of objects.
    /// Creates `dir` with mode 0700 if it is missing. Refuses a `dir` that
    /// other users can reach (not a real directory owned by this user and
    /// closed to others): only this user may read the objects or place files
    /// there. If the stored objects add
    /// up to more than `limit`, the oldest are dropped.
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
        crate::check_private_dir(dir)?;
        let lock_file = lock_dir(dir)?;
        let objects = dir.join("objects");
        let tmp = dir.join("tmp");
        // Temp files left by a crash. The lock keeps other processes out.
        ignore_missing(fs::remove_dir_all(&tmp))?;
        fs::create_dir_all(&tmp)?;
        fs::create_dir_all(&objects)?;
        let store = Self {
            objects,
            tmp,
            limit,
            lru: Mutex::new(Lru::new()),
            _lock: lock_file,
        };
        let mut found = store.scan()?;
        found.sort_by_key(|&(modified, ..)| modified);
        let mut lru = lock(&store.lru);
        for (_, key, size) in found {
            store.add(&mut lru, key, size)?;
        }
        drop(lru);
        Ok(store)
    }

    fn path(&self, key: Key) -> PathBuf {
        let (kind, hex) = match key {
            Key::Tree(id) => ("tree", id.to_string()),
            Key::Blob(id) => ("blob", id.to_string()),
        };
        self.objects.join(kind).join(&hex[..2]).join(&hex[2..])
    }

    /// Every stored object, with when it was written and its size.
    fn scan(&self) -> io::Result<Vec<(std::time::SystemTime, Key, usize)>> {
        let mut found = Vec::new();
        for kind in ["tree", "blob"] {
            let fans = match fs::read_dir(self.objects.join(kind)) {
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
                        found.push((meta.modified()?, key, size));
                    }
                }
            }
        }
        Ok(found)
    }

    /// Records a stored file in the index, then removes the files that the
    /// limit drops. The caller holds the index lock.
    fn add(&self, lru: &mut Lru<()>, key: Key, size: usize) -> io::Result<()> {
        for key in lru.insert(key, (), size, self.limit) {
            ignore_missing(fs::remove_file(self.path(key)))?;
        }
        Ok(())
    }
}

impl Store for DiskStore {
    fn size(&self, key: Key) -> io::Result<Option<usize>> {
        Ok(lock(&self.lru).get(key).map(|&((), size)| size))
    }

    fn get(&self, key: Key, size: usize) -> io::Result<Option<Vec<u8>>> {
        let path = self.path(key);
        let mut data = Vec::with_capacity(size + 1);
        match fs::File::open(&path) {
            // One byte more than `size`, to see that the file is longer.
            Ok(file) => file.take(size as u64 + 1).read_to_end(&mut data)?,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let mut lru = lock(&self.lru);
                // A put may have renamed the file into place since.
                if !path.exists() {
                    lru.remove(key);
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
            let mut lru = lock(&self.lru);
            lru.remove(key);
            ignore_missing(fs::remove_file(&path))?;
            return Ok(None);
        }
        Ok(Some(data))
    }

    fn put(&self, key: Key, data: &Arc<Vec<u8>>) -> io::Result<()> {
        if data.len() > self.limit {
            return Ok(());
        }
        let path = self.path(key);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let mut tmp = tempfile::NamedTempFile::new_in(&self.tmp)?;
        tmp.write_all(data)?;
        // Closed here, outside the lock; only the rename is under it.
        let tmp = tmp.into_temp_path();
        let mut lru = lock(&self.lru);
        // The rename replaces a bad copy, if there is one, in one step.
        tmp.persist(&path).map_err(|e| e.error)?;
        self.add(&mut lru, key, data.len())
    }
}

/// `Ok` if `result` failed only because the path is missing.
fn ignore_missing(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// Takes an exclusive lock on `<dir>/lock` without waiting. The lock lasts as
/// long as the returned file is open. The server locks its data directory
/// the same way (`scion-server/src/store.rs`).
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
    fn blob(data: &[u8]) -> (Key, Arc<Vec<u8>>) {
        let frame = object::pack(data).unwrap();
        (Key::Blob(BlobId::of(data)), Arc::new(frame))
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
        assert_eq!(store.get(a.0, n).unwrap().as_ref(), Some(&*a.1));
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
        let big = (Key::Blob(BlobId::of(b"big")), Arc::new(vec![b'x'; 3 * n]));
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
        assert_eq!(store.get(a.0, n).unwrap(), Some(a.1.to_vec()));
        assert_eq!(store.get(c.0, n).unwrap(), Some(c.1.to_vec()));
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
        store.put(tree_key, &Arc::new(tree_bytes.clone())).unwrap();
        assert_eq!(store.size(blob_key).unwrap(), Some(n));
        assert_eq!(store.get(blob_key, n).unwrap(), Some(frame.to_vec()));
        let tree_size = tree_bytes.len();
        assert_eq!(store.get(tree_key, tree_size).unwrap(), Some(tree_bytes));
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
                .add(&mut lock(&store.lru), blob_key, bad.len())
                .unwrap();
            assert_eq!(store.get(blob_key, bad.len()).unwrap(), None);
            assert_eq!(store.size(blob_key).unwrap(), None);
            assert!(!store.path(blob_key).exists());
        }
        // A put replaces a bad copy.
        fs::write(store.path(blob_key), b"torn").unwrap();
        store.put(blob_key, &frame).unwrap();
        assert_eq!(store.get(blob_key, n).unwrap(), Some(frame.to_vec()));
        // A file longer than its recorded size is not read past it.
        fs::write(store.path(blob_key), [&frame[..], b" and more"].concat()).unwrap();
        assert_eq!(store.get(blob_key, n).unwrap(), None);
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
        // A crash left the first bytes only.
        fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();

        let store = DiskStore::open(&data, 1 << 20).unwrap();
        let torn = bytes.len() - 1;
        assert_eq!(store.size(key).unwrap(), Some(torn));
        assert_eq!(store.get(key, torn).unwrap(), None);
        assert_eq!(store.size(key).unwrap(), None);
        assert!(!path.exists());
        assert_eq!(lock(&store.lru).bytes, 0);
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
    }
}
