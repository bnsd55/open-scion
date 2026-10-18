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
use std::io::{self, ErrorKind, Read as _, Write as _};

use rusqlite::OptionalExtension as _;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
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

    /// Notes that `key` went into a reply for `repo`. The tier calls it for
    /// each object that it sends, after the call's checks. Must be quick, as
    /// [`Store::size`]. Only a store that makes snapshots per repo keeps it.
    fn served(&self, _repo: &str, _key: Key) {}
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
    memory: Mutex<Memory>,
    /// It records some repo: [`Store::served`] takes the lock.
    records: bool,
}

/// What the memory store's lock guards.
struct Memory {
    lru: Lru<Bytes>,
    /// For each repo that the store makes snapshots of, the keys that went
    /// into its replies, least recently first. Only keys that `lru` holds: a
    /// key leaves when the store drops its object.
    repos: HashMap<String, LruCache<Key, ()>>,
}

impl MemoryStore {
    /// An empty store that keeps at most `limit` bytes of objects. Per-object
    /// overhead is not counted.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            memory: Mutex::new(Memory {
                lru: Lru::new(),
                repos: HashMap::new(),
            }),
            records: false,
        }
    }

    /// This store, which also notes the keys that go into replies for each
    /// of `repos`, for [`MemoryStore::snapshot`]. A noted key costs about 80
    /// bytes per repo, besides the object.
    #[must_use]
    pub fn recording(mut self, repos: &[String]) -> Self {
        let memory = self
            .memory
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        for repo in repos {
            memory.repos.insert(repo.clone(), LruCache::new_unbounded());
        }
        self.records = !memory.repos.is_empty();
        self
    }

    /// The objects that went into replies for `repo`: the most recently used
    /// ones, up to `limit` bytes, least recently used first, as the store
    /// holds them. Empty for a repo that the store does not record.
    #[must_use]
    pub fn snapshot(&self, repo: &str, limit: usize) -> Vec<(Key, Bytes)> {
        let memory = lock(&self.memory);
        let mut objects = Vec::new();
        let mut bytes = 0;
        for (key, ()) in memory.repos.get(repo).into_iter().flatten().rev() {
            let Some((data, size)) = memory.lru.entries.peek(key) else {
                continue;
            };
            if bytes + size > limit {
                break;
            }
            bytes += size;
            objects.push((*key, data.clone()));
        }
        objects.reverse();
        objects
    }
}

impl Store for MemoryStore {
    fn size(&self, key: Key) -> io::Result<Option<usize>> {
        Ok(lock(&self.memory).lru.get(key).map(|&(_, size)| size))
    }

    /// The stored buffer itself, not a copy.
    fn get(&self, key: Key, size: usize) -> io::Result<Option<Bytes>> {
        let data = lock(&self.memory)
            .lru
            .get(key)
            .map(|(data, _)| data.clone());
        Ok(data.filter(|data| data.len() <= size))
    }

    fn put(&self, key: Key, data: &Bytes) -> io::Result<()> {
        if data.len() <= self.limit {
            let mut memory = lock(&self.memory);
            memory.lru.insert(key, data.clone(), data.len());
            for key in memory.lru.trim(self.limit) {
                for keys in memory.repos.values_mut() {
                    keys.remove(&key);
                }
            }
        }
        Ok(())
    }

    fn served(&self, repo: &str, key: Key) {
        if !self.records {
            return;
        }
        let mut memory = lock(&self.memory);
        let Memory { lru, repos } = &mut *memory;
        if let Some(keys) = repos.get_mut(repo)
            && lru.entries.contains_key(&key)
        {
            keys.insert(key, ());
        }
    }
}

/// The name of the index database in the data directory (see [`DiskStore`]).
const INDEX: &str = "index.db";

/// The name of the index file of the format before this one. A start imports
/// it and removes it. Only its reader is kept.
const OLD_INDEX: &str = "index";

/// The start of the old index file, with its format version. The header goes
/// on with the state byte ([`CLEAN`] or [`DIRTY`]) and the BLAKE3 hash of the
/// records that the last full write wrote. A cut file fails the hash too.
const MAGIC: &[u8; 8] = b"scnidx1\n";

/// Bytes before the first record of the old format.
const HEADER: usize = MAGIC.len() + 1 + 32;

/// The old index file names every object file, and is on disk: a clean stop
/// wrote it, and its records are the ones that its header hashes.
const CLEAN: u8 = b'c';

/// The old index file may lag the object files: the store was open when it
/// stopped. Only the tests write it.
#[cfg(test)]
const DIRTY: u8 = b'd';

/// Bytes of one record: an op, a 32-byte ID and a size. The record is the
/// shared form of an index row and a snapshot entry.
pub(crate) const RECORD: usize = 1 + 32 + 4;

// A record's size field holds the largest stored form.
const _: () = assert!(MAX_ENCODED_BYTES <= u32::MAX as usize);

/// Row versions that wait in memory before one transaction commits them:
/// 1,024 of them. More wait while another call commits.
const FLUSH_RECORDS: usize = 1024;

/// Bytes of new objects that one batch may hold. A put commits at once, so
/// this bounds only what a failed commit may strand.
const FLUSH_NEW_BYTES: usize = 64 << 20;

/// Files that the walk of a start that may have stranded some holds for the
/// store, which adopts them as it flushes. The walk waits while the queue is
/// full, so the store never holds more than this.
const WALK_QUEUE: usize = 256;

/// Appends the record that `key` is stored with `size` bytes and was just
/// used (`Some`), or that it was dropped (`None`).
pub(crate) fn push_record(out: &mut Vec<u8>, key: Key, size: Option<usize>) {
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
pub(crate) fn parse_record(record: &[u8; RECORD]) -> Option<(Key, Option<usize>)> {
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

/// What a start found in the old index file.
#[derive(Debug, PartialEq)]
enum Found {
    /// A clean file of this many records: the start trusts it.
    Clean(usize),
    /// Why the start scans every object file.
    Scan(&'static str),
}

/// Reads the old index file in `dir`: its objects, least recently used
/// first, and whether the start may trust it. A damaged file gives what its
/// records before the damage give.
fn read_old_index(dir: &Path) -> io::Result<(Lru<()>, Found)> {
    let mut lru = Lru::new();
    let bytes = match fs::read(dir.join(OLD_INDEX)) {
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

/// The column of the `kind` of a key.
fn kind_of(key: Key) -> i64 {
    match key {
        Key::Tree(_) => 0,
        Key::Blob(_) => 1,
    }
}

/// The key of a `kind` column and an ID, or `None` if they are not one.
fn key_of(kind: i64, id: &[u8]) -> Option<Key> {
    let id: [u8; 32] = id.try_into().ok()?;
    match kind {
        0 => Some(Key::Tree(TreeId::from_bytes(id))),
        1 => Some(Key::Blob(BlobId::from_bytes(id))),
        _ => None,
    }
}

/// The path of the file of `key` under `objects`.
fn object_path(objects: &Path, key: Key) -> PathBuf {
    let (kind, hex) = match key {
        Key::Tree(id) => ("tree", id.to_string()),
        Key::Blob(id) => ("blob", id.to_string()),
    };
    objects.join(kind).join(&hex[..2]).join(&hex[2..])
}

/// Makes an error of the index db an I/O error.
fn index_error(error: rusqlite::Error) -> io::Error {
    io::Error::other(error)
}

/// One change of one object that the db does not have yet.
enum Op {
    /// The object was used again.
    Use,
    /// The object is stored with `size` bytes.
    Put,
    /// The object left the index.
    Remove,
}

struct Version {
    op: Op,
    key: Key,
    /// For [`Op::Put`]: the size of the stored form.
    size: usize,
    used_at: u64,
    seq: u64,
}

/// The state of the index that the db does not have yet, and the counters
/// that mirror it. Every change goes through these methods, so a batch
/// commit leaves the db, the mirrors and `newest` agreed.
struct Disk {
    /// The db. Only the index lock uses it, so it needs no lock of its own.
    conn: rusqlite::Connection,
    /// Row versions that the db does not have yet, oldest first.
    pending: Vec<Version>,
    /// Bytes of new objects in `pending`.
    pending_bytes: usize,
    /// The latest state in `pending` of a key: its size, or gone. Cleared
    /// when a batch commits, so a settled key is read from the db.
    newest: HashMap<Key, Option<(u64, usize)>>,
    /// The meta counters, `pending` included.
    objects: u64,
    bytes: u64,
    next_used: u64,
    next_seq: u64,
    /// Set once a batch is due, so the next put or get commits it.
    due: bool,
    /// The directories that dropped files left, to sync before the last
    /// commit of a stop, so no dropped file comes back after a crash.
    left: HashSet<PathBuf>,
    /// Versions of the last commit, and rows that the last trim iterated:
    /// for the tests of the bounds.
    #[cfg(test)]
    committed: usize,
    #[cfg(test)]
    iterated: usize,
}

impl Disk {
    /// Opens the db at `path`, and makes its schema. The file may be new.
    fn open(path: &Path) -> io::Result<Self> {
        let conn = rusqlite::Connection::open(path).map_err(index_error)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(index_error)?;
        // WAL, and nothing synced per commit: what a batch keeps is what a
        // process crash keeps, which is what the old index file kept. The
        // page cache is capped, so the memory does not grow with objects.
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(index_error)?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(index_error)?;
        conn.pragma_update(None, "cache_size", -8_000)
            .map_err(index_error)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta (
                k TEXT PRIMARY KEY,
                v INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS objects (
                kind INTEGER NOT NULL,
                id BLOB NOT NULL,
                size INTEGER NOT NULL,
                used_at INTEGER NOT NULL,
                seq INTEGER NOT NULL,
                PRIMARY KEY (kind, id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS objects_used ON objects (used_at);",
        )
        .map_err(index_error)?;
        Ok(Self {
            conn,
            pending: Vec::new(),
            pending_bytes: 0,
            newest: HashMap::new(),
            objects: 0,
            bytes: 0,
            next_used: 0,
            next_seq: 1,
            due: false,
            left: HashSet::new(),
            #[cfg(test)]
            committed: 0,
            #[cfg(test)]
            iterated: 0,
        })
    }

    /// The meta counters, or `None` if the db has none and a start must
    /// build the rows from the old file or the object files.
    fn read_meta(&self) -> io::Result<Option<Meta>> {
        let rows = self
            .conn
            .prepare_cached("SELECT k, v FROM meta")
            .map_err(index_error)?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(index_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(index_error)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let get = |name: &str| -> u64 {
            rows.iter()
                .find(|(k, _)| k == name)
                .map_or(0, |(_, v)| u64::try_from(*v).unwrap_or(0))
        };
        Ok(Some(Meta {
            objects: get("objects"),
            bytes: get("bytes"),
            next_used: get("next_used"),
            next_seq: get("next_seq"),
            clean: get("clean") != 0,
            walk_pending: get("walk_pending") != 0,
        }))
    }

    /// Sets one meta flag.
    fn set_flag(&mut self, name: &str, value: bool) -> io::Result<()> {
        self.conn
            .prepare_cached(
                "INSERT INTO meta(k, v) VALUES(?1, ?2) ON CONFLICT(k) DO UPDATE SET v = ?2",
            )
            .map_err(index_error)?
            .execute(rusqlite::params![name, i64::from(value)])
            .map_err(index_error)?;
        Ok(())
    }

    /// Sets one meta flag, and logs a failure: a flag that is lost changes
    /// what the next start walks, so its loss must be visible.
    fn set_flag_logged(&mut self, name: &str, value: bool) {
        if let Err(error) = self.set_flag(name, value) {
            // No repo on this line: the store sits below the repos, and the
            // call whose work failed logs the repo and the caller.
            tracing::warn!(
                flag = name,
                value,
                %error,
                "cannot write the index flag; the next start may not walk"
            );
        }
    }

    /// The size that the db gives for `key`, if any. The batch does not
    /// name it: the callers check `newest` first.
    fn settled_size(&self, key: Key) -> io::Result<Option<usize>> {
        let size = self
            .conn
            .query_row(
                "SELECT size FROM objects WHERE kind = ?1 AND id = ?2",
                rusqlite::params![kind_of(key), key.id()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(index_error)?;
        Ok(size.map(|size| usize::try_from(size).unwrap_or(0)))
    }

    /// Notes one more version of `key`, and keeps the mirrors.
    fn push(&mut self, op: Op, key: Key, size: usize, used_at: u64) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.pending.push(Version {
            op,
            key,
            size,
            used_at,
            seq,
        });
        if self.pending.len() >= FLUSH_RECORDS || self.pending_bytes >= FLUSH_NEW_BYTES {
            self.due = true;
        }
    }

    /// Notes that `key` was just used, and bumps its place. The batch has
    /// the last word on it; the db on the settled ones.
    fn touch(&mut self, key: Key) -> io::Result<Option<usize>> {
        if let Some(state) = self.newest.get(&key).copied() {
            let Some((_, size)) = state else {
                return Ok(None);
            };
            let used_at = self.next_used;
            self.next_used += 1;
            self.newest.insert(key, Some((used_at, size)));
            self.push(Op::Use, key, size, used_at);
            return Ok(Some(size));
        }
        let Some(size) = self.settled_size(key)? else {
            return Ok(None);
        };
        let used_at = self.next_used;
        self.next_used += 1;
        self.newest.insert(key, Some((used_at, size)));
        self.push(Op::Use, key, size, used_at);
        Ok(Some(size))
    }

    /// The state of `key`: its size, or gone. The batch has the last word;
    /// the db on the settled ones.
    fn state(&self, key: Key) -> io::Result<Option<(u64, usize)>> {
        if let Some(state) = self.newest.get(&key) {
            return Ok(*state);
        }
        let Some(size) = self.settled_size(key)? else {
            return Ok(None);
        };
        Ok(Some((0, size)))
    }

    /// Adds or replaces `key`, stored with `size` bytes, as just used.
    fn insert(&mut self, key: Key, size: usize) {
        let old = self.state(key).unwrap_or_default();
        match old {
            Some((_, old_size)) => self.bytes -= old_size as u64,
            None => self.objects += 1,
        }
        self.bytes += size as u64;
        let used_at = self.next_used;
        self.next_used += 1;
        self.newest.insert(key, Some((used_at, size)));
        self.pending_bytes += size;
        self.push(Op::Put, key, size, used_at);
    }

    /// Adds `key` as the least recently used: what the walk adopted, which
    /// leaves first.
    fn insert_oldest(&mut self, key: Key, size: usize) {
        let old = self.state(key).unwrap_or_default();
        match old {
            Some((_, old_size)) => self.bytes -= old_size as u64,
            None => self.objects += 1,
        }
        self.bytes += size as u64;
        self.newest.insert(key, Some((0, size)));
        self.pending_bytes += size;
        self.push(Op::Put, key, size, 0);
    }

    /// Removes `key`, if it is there.
    fn remove(&mut self, key: Key) {
        let old = self.state(key).unwrap_or_default();
        let Some((_, size)) = old else {
            return;
        };
        self.objects -= 1;
        self.bytes -= size as u64;
        self.newest.insert(key, None);
        self.push(Op::Remove, key, 0, 0);
    }

    /// Commits `pending` in one transaction, with the counters and the mark
    /// that makes its rows settled: every put of the batch renamed its file
    /// before the flush that commits it ran, so every row at or below the
    /// mark has its file. When the transaction cannot land, it rolls back:
    /// the db keeps its rows and counters, the batch goes, and the counters
    /// are read back from the db, so the next batch writes true ones.
    fn commit(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        let count = self.pending.len();
        let mark = self.next_seq - 1;
        let committed = self.commit_batch(mark);
        if committed.is_ok() {
            self.pending = Vec::new();
            self.pending_bytes = 0;
            self.newest.clear();
            #[cfg(test)]
            {
                self.committed = count;
            }
        } else {
            self.pending.clear();
            self.pending_bytes = 0;
            self.newest.clear();
            let counters = self.conn.query_row(
                "SELECT (SELECT COUNT(*) FROM objects),
                        COALESCE((SELECT SUM(size) FROM objects), 0),
                        COALESCE((SELECT v FROM meta WHERE k = 'next_used'), 0),
                        COALESCE((SELECT v FROM meta WHERE k = 'next_seq'), 0)
                ",
                [],
                |row| {
                    Ok((
                        u64::try_from(row.get::<_, i64>(0).unwrap_or(0)).unwrap_or(0),
                        u64::try_from(row.get::<_, i64>(1).unwrap_or(0)).unwrap_or(0),
                        u64::try_from(row.get::<_, i64>(2).unwrap_or(0)).unwrap_or(0),
                        u64::try_from(row.get::<_, i64>(3).unwrap_or(0)).unwrap_or(0),
                    ))
                },
            );
            if let Ok((objects, bytes, next_used, next_seq)) = counters {
                self.objects = objects;
                self.bytes = bytes;
                self.next_used = next_used;
                self.next_seq = next_seq;
            }
        }
        committed
    }

    /// The transaction of [`Disk::commit`]: every row version, then the
    /// counters and the mark. One transaction: all of it lands, or none.
    fn commit_batch(&mut self, mark: u64) -> io::Result<()> {
        let tx = self.conn.transaction().map_err(index_error)?;
        // Sizes and counters of stored bytes, far below i64::MAX.
        #[allow(clippy::cast_possible_wrap)]
        for version in &self.pending {
            let kind = kind_of(version.key);
            let id = version.key.id();
            match version.op {
                Op::Use => {
                    tx.execute(
                        "UPDATE objects SET used_at = ?3, seq = ?4
                         WHERE kind = ?1 AND id = ?2",
                        rusqlite::params![kind, id, version.used_at as i64, version.seq as i64],
                    )
                    .map_err(index_error)?;
                }
                Op::Put => {
                    tx.execute(
                        "INSERT INTO objects(kind, id, size, used_at, seq)
                         VALUES(?1, ?2, ?3, ?4, ?5)
                         ON CONFLICT(kind, id) DO UPDATE
                         SET size = ?3, used_at = ?4, seq = ?5",
                        rusqlite::params![
                            kind,
                            id,
                            version.size as i64,
                            version.used_at as i64,
                            version.seq as i64
                        ],
                    )
                    .map_err(index_error)?;
                }
                Op::Remove => {
                    tx.execute(
                        "DELETE FROM objects WHERE kind = ?1 AND id = ?2",
                        rusqlite::params![kind, id],
                    )
                    .map_err(index_error)?;
                }
            }
        }
        // The counters count stored bytes, far below i64::MAX.
        #[allow(clippy::cast_possible_wrap)]
        for (name, value) in [
            ("objects", self.objects as i64),
            ("bytes", self.bytes as i64),
            ("next_used", self.next_used as i64),
            ("next_seq", self.next_seq as i64),
            ("mark", mark as i64),
        ] {
            tx.execute(
                "INSERT INTO meta(k, v) VALUES(?1, ?2)
                 ON CONFLICT(k) DO UPDATE SET v = ?2",
                rusqlite::params![name, value],
            )
            .map_err(index_error)?;
        }
        tx.commit().map_err(index_error)
    }

    /// Makes the db durable, so a crash keeps the committed batches.
    fn checkpoint(&mut self) -> io::Result<()> {
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(index_error)
    }

    /// Writes the rows of `lru` into the fresh db, in its order, with the
    /// meta counters. The rows are settled: `mark` covers them all, for the
    /// build saw every file, or the old file named them.
    fn import(&mut self, lru: &Lru<()>) -> io::Result<()> {
        // The sizes and positions of stored bytes, far below i64::MAX.
        #[allow(clippy::cast_possible_wrap)]
        let rows: Vec<(Key, i64, i64)> = (0u64..)
            .zip(&lru.entries)
            .map(|(used_at, (&key, &((), size)))| (key, size as i64, used_at as i64))
            .collect();
        let tx = self.conn.transaction().map_err(index_error)?;
        for (key, size, used_at) in &rows {
            tx.execute(
                "INSERT INTO objects(kind, id, size, used_at, seq)
                 VALUES(?1, ?2, ?3, ?4, 0)",
                rusqlite::params![kind_of(*key), key.id(), *size, *used_at],
            )
            .map_err(index_error)?;
        }
        #[allow(clippy::cast_possible_wrap)]
        for (name, value) in [
            ("objects", lru.entries.len() as i64),
            ("bytes", lru.bytes as i64),
            ("next_used", lru.entries.len() as i64),
            ("next_seq", 1),
            ("mark", 0),
            ("clean", 0),
            ("walk_pending", 0),
        ] {
            tx.execute(
                "INSERT INTO meta(k, v) VALUES(?1, ?2) ON CONFLICT(k) DO UPDATE SET v = ?2",
                rusqlite::params![name, value],
            )
            .map_err(index_error)?;
        }
        tx.commit().map_err(index_error)?;
        self.objects = lru.entries.len() as u64;
        self.bytes = lru.bytes as u64;
        self.next_used = lru.entries.len() as u64;
        self.next_seq = 1;
        Ok(())
    }

    /// Drops the least recently used objects until the total is at most
    /// `limit`. Returns their keys. The batch commits first, so the order
    /// of the db is the order of everything, and only the dropped prefix is
    /// read.
    fn trim(&mut self, limit: usize) -> io::Result<Vec<Key>> {
        self.commit()?;
        if self.bytes <= limit as u64 {
            return Ok(Vec::new());
        }
        let mut stmt = self
            .conn
            .prepare_cached("SELECT kind, id, size FROM objects ORDER BY used_at")
            .map_err(index_error)?;
        let mut rows = stmt.query([]).map_err(index_error)?;
        let mut total = self.bytes;
        let mut dropped = Vec::new();
        while total > limit as u64 {
            let Some(row) = rows.next().map_err(index_error)? else {
                break;
            };
            let kind: i64 = row.get(0).map_err(index_error)?;
            let id: Vec<u8> = row.get(1).map_err(index_error)?;
            let size: i64 = row.get(2).map_err(index_error)?;
            total -= u64::try_from(size).unwrap_or(0);
            dropped.push(key_of(kind, &id));
        }
        drop(rows);
        drop(stmt);
        #[cfg(test)]
        {
            self.iterated = dropped.len();
        }
        let mut keys = Vec::with_capacity(dropped.len());
        for key in dropped.into_iter().flatten() {
            self.remove(key);
            keys.push(key);
        }
        Ok(keys)
    }

    /// The stored objects, least recently used first.
    #[cfg(test)]
    fn order(&mut self) -> io::Result<Vec<Key>> {
        self.commit()?;
        let mut stmt = self
            .conn
            .prepare_cached("SELECT kind, id FROM objects ORDER BY used_at")
            .map_err(index_error)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(index_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(index_error)?;
        Ok(rows
            .into_iter()
            .filter_map(|(kind, id)| key_of(kind, &id))
            .collect())
    }
}

/// The meta counters, as a start found them.
struct Meta {
    objects: u64,
    bytes: u64,
    next_used: u64,
    next_seq: u64,
    /// The last stop committed everything and synced the directories of
    /// dropped files: the start may trust the rows.
    clean: bool,
    /// A walk of the object directories is still owed: an earlier one did
    /// not finish, or the last stop was not clean.
    walk_pending: bool,
}

/// What the walk of a start shares with the store: the files that it found
/// without a row, and how it is doing.
struct WalkState {
    queue: Mutex<std::collections::VecDeque<(Key, usize)>>,
    room: Condvar,
    stop: AtomicBool,
    /// Files queued so far, and their bytes.
    found: AtomicU64,
    bytes: AtomicU64,
    /// The walk went over every object directory.
    done: AtomicBool,
}

impl WalkState {
    /// Hands one file to the store, waiting while its queue is full, so the
    /// walk never runs ahead of it.
    fn send(&self, key: Key, size: usize) -> bool {
        let mut queue = lock(&self.queue);
        loop {
            if self.stop.load(Ordering::Relaxed) {
                return false;
            }
            if queue.len() < WALK_QUEUE {
                queue.push_back((key, size));
                return true;
            }
            let (queued, state) = self
                .room
                .wait_timeout(queue, std::time::Duration::from_millis(100))
                .unwrap_or_else(PoisonError::into_inner);
            queue = queued;
            if state.timed_out() && self.stop.load(Ordering::Relaxed) {
                return false;
            }
        }
    }
}

/// Walks the object directories, and hands every file that has no row to the
/// store, which adopts it, as the least recently used, as it flushes. Runs
/// once after a start that may have stranded files, and stops when it is
/// told to, which a stop of the store does. The store clears the flag that
/// asked for the walk, once the walk is done and nothing has been stranded
/// since.
fn walk_objects(db_path: &Path, objects: &Path, walk: &WalkState) {
    let db = match rusqlite::Connection::open(db_path) {
        Ok(db) => db,
        Err(error) => {
            // No repo on this line: the store sits below the repos, and the
            // call whose work failed logs the repo and the caller.
            tracing::warn!(%error, "the walk cannot open the index; the files stay uncounted");
            walk.done.store(true, Ordering::Relaxed);
            return;
        }
    };
    let _ = db.busy_timeout(std::time::Duration::from_secs(5));
    let mut files = 0u64;
    let mut bytes = 0u64;
    'walk: for kind in ["tree", "blob"] {
        let kind_dir = objects.join(kind);
        let Ok(fans) = fs::read_dir(&kind_dir) else {
            continue;
        };
        for fan in fans.flatten() {
            if !fan.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            let Ok(entries) = fs::read_dir(fan.path()) else {
                continue;
            };
            for file in entries.flatten() {
                if walk.stop.load(Ordering::Relaxed) {
                    break 'walk;
                }
                let hex = format!(
                    "{}{}",
                    fan.file_name().to_string_lossy(),
                    file.file_name().to_string_lossy()
                );
                let key = match kind {
                    "tree" => hex.parse().map(Key::Tree),
                    _ => hex.parse().map(Key::Blob),
                };
                let Ok(key) = key else {
                    continue;
                };
                let known = db
                    .query_row(
                        "SELECT 1 FROM objects WHERE kind = ?1 AND id = ?2",
                        rusqlite::params![kind_of(key), key.id()],
                        |_| Ok(()),
                    )
                    .optional()
                    .ok()
                    .flatten()
                    .is_some();
                if known {
                    continue;
                }
                let Ok(meta) = file.metadata() else {
                    continue;
                };
                let Ok(size) = usize::try_from(meta.len()) else {
                    continue;
                };
                // Larger than any object: never a good copy.
                if size > MAX_ENCODED_BYTES {
                    let _ = ignore_missing(fs::remove_file(file.path()));
                    continue;
                }
                if !walk.send(key, size) {
                    break 'walk;
                }
                files += 1;
                bytes += size as u64;
                walk.found.store(files, Ordering::Relaxed);
                walk.bytes.store(bytes, Ordering::Relaxed);
            }
        }
    }
    walk.done.store(true, Ordering::Relaxed);
    tracing::info!(files, bytes, "the walk of the object directories is done");
}

/// A plain `fsync`: data reaches the drive, but on macOS not past its cache
/// (`File::sync_all` is a full cache flush there). Used for the directories
/// that dropped files left, before the last commit of a stop.
fn fsync(dir: &Path) -> io::Result<()> {
    rustix::fs::fsync(fs::File::open(dir)?).map_err(Into::into)
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

/// Objects on disk, one file per object, named by its ID, up to a byte limit:
///
/// ```text
/// <data>/lock                   locked by the one process that uses <data>
/// <data>/index.db               the index: which objects, their sizes, and
///                               the order of their use
/// <data>/index.db-wal           what the last batches added (SQLite's)
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
/// The index is a SQLite database (through the workspace `rusqlite`, as the
/// server's `lands` table), so that it is read and changed in place: memory
/// does not grow with the number of objects, and no change copies all of
/// them. A table `objects(kind, id, size, used_at, seq)` holds one row per
/// object; `used_at` is a counter of uses, and its order is the order of
/// use, least recently used first. A table `meta` holds the counters: the
/// totals, the next values of the counters, `mark`, and the flags `clean`
/// and `walk_pending`.
///
/// Changes wait in memory as row versions, and one transaction commits
/// them, with the counters and `mark` set past them: a put renames its file
/// before the flush that commits its row, so every row at or below `mark`
/// has its file. A batch is due at 1,024 versions or 64 MiB of new bytes
/// (`FLUSH_RECORDS`, `FLUSH_NEW_BYTES`); a put commits at once, at the
/// eviction that follows it. With SQLite's WAL and `synchronous = NORMAL`,
/// a committed batch survives a crash of the process, and a crash of the
/// machine loses at most the last batches, as the old index file did. A
/// commit that failed drops its batch: the files of its puts have no rows
/// then, and a walk finds them.
///
/// A stop commits what is left, syncs the directories that dropped files
/// left, sets `clean`, and truncates the write-ahead log. A start trusts
/// the rows and opens no object directory; it marks `clean` no at once, for
/// from then on a crash may strand files. A crash strands a file when its
/// row did not reach a committed batch: the file is there, the index does
/// not name it, and the limit does not count it. The walk of a start that
/// is owed one (see `walk_objects`) finds them: it hands every file that
/// has no row to the store, which adopts it as the least recently used, so
/// the limit counts it again. The walk runs on its own thread, at the pace
/// that the store drains, and a stop of the store stops it. A start that
/// scanned the object directories, or imported the old index file, owes no
/// walk: it saw every file.
///
/// A store of the format before this one has `<data>/index`: a start
/// imports its records, in their order of use, and removes the file. A
/// dirty or damaged one is not trusted: the start scans instead. A db that
/// cannot be read is moved aside, and the start scans. No start trusts the
/// index for an object's bytes: [`Store::get`] checks them on every read.
pub struct DiskStore {
    dir: PathBuf,
    objects: PathBuf,
    tmp: PathBuf,
    limit: usize,
    /// The index, its batch, and the counters that mirror it.
    disk: Mutex<Disk>,
    /// The walk that this start spawned, when it is owed one: its found
    /// files wait for the store to adopt them.
    walk: Mutex<Option<Arc<WalkState>>>,
    /// Names files that the limit drops, in `tmp/`.
    dropped: AtomicU64,
    /// Something may be on disk without a row since the last walk saw it:
    /// a rename that failed, or a batch that did not commit. A walk that
    /// ran to the end clears the flag that asked for it only while this is
    /// false, so a debt is never dropped.
    stray: AtomicBool,
    /// Held for as long as the store lives, so only this process uses the
    /// data directory. Taken by a simulated crash, so a restart can lock.
    /// Never read: the lock lasts while the file is open.
    #[allow(dead_code)]
    dir_lock: Option<fs::File>,
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
        let db_path = dir.join(INDEX);
        let (mut disk, meta) = Self::open_disk(&db_path)?;
        let mut walk_owed = false;
        if let Some(meta) = meta {
            // The rows are trusted: every one of them has its file, in
            // the order that `used_at` gives.
            disk.objects = meta.objects;
            disk.bytes = meta.bytes;
            disk.next_used = meta.next_used;
            disk.next_seq = meta.next_seq;
            match disk.set_flag("clean", false) {
                // The last stop was clean, and its walk finished: no walk.
                Ok(()) => walk_owed = !meta.clean || meta.walk_pending,
                // A crash would look like a clean stop, so the walk owes
                // what the rows may miss.
                Err(error) => {
                    // No repo on this line: the store sits below the repos, and the
                    // call whose work failed logs the repo and the caller.
                    tracing::warn!(
                        %error,
                        "cannot mark the index unclean; the start walks the object directories"
                    );
                    walk_owed = true;
                }
            }
            tracing::info!(
                objects = disk.objects,
                bytes = disk.bytes,
                "opened the disk store from its index"
            );
        } else {
            // Build the rows: from the old index file, or the files.
            let (lru, found) = read_old_index(dir)?;
            let (lru, how) = match found {
                Found::Clean(_) => (
                    lru,
                    "opened the disk store from the index file of the previous format",
                ),
                Found::Scan(reason) => {
                    tracing::info!(reason, "scanning every object file");
                    (
                        reconcile(lru, scan(&objects)?),
                        "opened the disk store by a scan of every file",
                    )
                }
            };
            disk.import(&lru)?;
            // The import saw every file, or the old file named them, so
            // nothing is stranded.
            ignore_missing(fs::remove_file(dir.join(OLD_INDEX)))?;
            tracing::info!(objects = disk.objects, bytes = disk.bytes, how);
        }
        let store = Self {
            dir: dir.clone(),
            objects,
            tmp,
            limit,
            disk: Mutex::new(disk),
            walk: Mutex::new(None),
            dropped: AtomicU64::default(),
            stray: AtomicBool::new(false),
            dir_lock: Some(lock_file),
        };
        // Drops first: they free room.
        let mut disk = lock(&store.disk);
        let mut dropped = Vec::new();
        let evicted = store.evict(&mut disk, &mut dropped);
        drop(disk);
        remove_all(dropped)?;
        evicted?;
        if walk_owed {
            store.spawn_walk();
        }
        Ok(store)
    }

    /// Opens the index db and reads its meta, before the run is marked
    /// unclean, so a start sees the flags as the last stop left them. A db
    /// that cannot be read or written is moved aside, and a new one is
    /// built from the old file or the object files: the object files are
    /// the truth, so a damaged index costs at most a scan.
    fn open_disk(db_path: &Path) -> io::Result<(Disk, Option<Meta>)> {
        match Self::try_open_disk(db_path) {
            Ok(opened) => Ok(opened),
            Err(error) => {
                // No repo on this line: the store sits below the repos, and the
                // call whose work failed logs the repo and the caller.
                tracing::warn!(%error, "cannot use the index; starting a new one");
                let name = db_path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                let parent = db_path.parent().unwrap_or(Path::new("."));
                for suffix in ["", "-wal", "-shm"] {
                    let from = parent.join(format!("{name}{suffix}"));
                    let to = parent.join(format!("{name}.damaged{suffix}"));
                    ignore_missing(fs::rename(&from, &to))?;
                }
                Self::try_open_disk(db_path)
            }
        }
    }

    /// Opens the db and reads its meta, before the run is marked unclean,
    /// so a start sees the flags as the last stop left them.
    fn try_open_disk(db_path: &Path) -> io::Result<(Disk, Option<Meta>)> {
        let disk = Disk::open(db_path)?;
        let meta = disk.read_meta()?;
        Ok((disk, meta))
    }

    /// Spawns the walk of the object directories, which hands the store
    /// every file that has no row.
    fn spawn_walk(&self) {
        let state = Arc::new(WalkState {
            queue: Mutex::new(std::collections::VecDeque::new()),
            room: Condvar::new(),
            stop: AtomicBool::new(false),
            found: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            done: AtomicBool::new(false),
        });
        let db_path = self.dir.join(INDEX);
        let objects = self.objects.clone();
        let thread_state = Arc::clone(&state);
        let spawned = std::thread::Builder::new()
            .name("index walk".into())
            .spawn(move || walk_objects(&db_path, &objects, &thread_state));
        match spawned {
            Ok(_) => *lock(&self.walk) = Some(state),
            Err(error) => {
                // No repo on this line: the store sits below the repos, and the
                // call whose work failed logs the repo and the caller.
                tracing::warn!(
                    %error,
                    "cannot spawn the walk; files that a crash stranded stay uncounted"
                );
            }
        }
    }

    /// Adopts the files that the walk found into `disk`: inserts them as
    /// the least recently used, so the limit counts them again. A file that
    /// the limit cannot keep is removed. The walk waits while its queue is
    /// full, so this never holds more than [`WALK_QUEUE`] files.
    fn adopt_walk(walk: Option<&Arc<WalkState>>, limit: usize, objects: &Path, disk: &mut Disk) {
        let Some(walk) = walk else {
            return;
        };
        let mut took = 0;
        loop {
            let next = lock(&walk.queue).pop_front();
            let Some((key, size)) = next else {
                break;
            };
            took += 1;
            if size > limit {
                let _ = ignore_missing(fs::remove_file(object_path(objects, key)));
                continue;
            }
            disk.insert_oldest(key, size);
        }
        if took > 0 {
            walk.room.notify_all();
            // The batch that counts them is due.
            disk.due = true;
        }
    }

    /// Whether the store holds no object.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        lock(&self.disk).objects == 0
    }

    fn path(&self, key: Key) -> PathBuf {
        object_path(&self.objects, key)
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
        for key in disk.trim(self.limit)? {
            match self.take_out(disk, key) {
                Ok(file) => dropped.extend(file),
                Err(e) => {
                    first.get_or_insert(e);
                }
            }
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
            // The file stays in place, and its row leaves at the next
            // commit: a walk finds it, this one or the next. The flag
            // survives the stop, so the next start walks if this one is
            // over.
            Err(e) => {
                self.stray.store(true, Ordering::Relaxed);
                disk.set_flag_logged("walk_pending", true);
                Err(e)
            }
        }
    }

    /// Adopts what the walk found, and commits the batch of row versions
    /// once it is due. Only what a crash keeps depends on this, so an error
    /// is logged, not returned.
    fn flush(&self) {
        let mut disk = lock(&self.disk);
        let walk = lock(&self.walk).clone();
        Self::adopt_walk(walk.as_ref(), self.limit, &self.objects, &mut disk);
        if walk
            .as_ref()
            .is_some_and(|w| w.done.load(Ordering::Relaxed))
        {
            *lock(&self.walk) = None;
            // The walk went over every directory. It owes nothing more
            // unless the run stranded a file or lost a batch since.
            if !self.stray.load(Ordering::Relaxed) {
                disk.set_flag_logged("walk_pending", false);
                self.stray.store(false, Ordering::Relaxed);
            }
        }
        if !disk.due {
            return;
        }
        disk.due = false;
        if disk.pending.len() < FLUSH_RECORDS && disk.pending_bytes < FLUSH_NEW_BYTES {
            return;
        }
        if let Err(error) = disk.commit() {
            // The batch is lost whole: the db has none of it. The files of
            // its puts have no rows now; the walk of the next start finds
            // them.
            self.stray.store(true, Ordering::Relaxed);
            disk.set_flag_logged("walk_pending", true);
            // No repo on this line: the store sits below the repos, and the
            // call whose work failed logs the repo and the caller.
            tracing::warn!(
                %error,
                "cannot commit the index; a crash now loses what was used last"
            );
        }
    }

    /// Simulates a crash of the process: no stop-time commit, no clean
    /// flag, and the lock released, as a killed process leaves them. What
    /// the index does not have yet stays what it is.
    #[cfg(test)]
    fn crash(mut self) {
        self.dir_lock.take();
        std::mem::forget(self);
    }

    /// How the walk of this start is doing: the files it found, their
    /// bytes, and whether it went over every directory.
    #[cfg(test)]
    fn walk_state(&self) -> Option<(u64, u64, bool)> {
        let walk = lock(&self.walk).clone()?;
        Some((
            walk.found.load(Ordering::Relaxed),
            walk.bytes.load(Ordering::Relaxed),
            walk.done.load(Ordering::Relaxed),
        ))
    }
}

impl Drop for DiskStore {
    /// Stops the walk, adopts what it found, commits what is left, and
    /// makes it durable, with the directories of dropped files synced
    /// first. The stop is clean when the commit and the syncs worked: the
    /// next start then trusts the rows, and walks only if the walk of this
    /// one did not finish.
    fn drop(&mut self) {
        let walk = lock(&self.walk).clone();
        if let Some(walk) = walk.as_ref() {
            walk.stop.store(true, Ordering::Relaxed);
            walk.room.notify_all();
        }
        let disk = self.disk.get_mut().unwrap_or_else(PoisonError::into_inner);
        Self::adopt_walk(walk.as_ref(), self.limit, &self.objects, disk);
        let synced = disk
            .left
            .iter()
            .chain([&self.tmp])
            .try_for_each(|dir| fsync(dir));
        if let Err(e) = &synced {
            // No repo on this line: the store sits below the repos, and the
            // call whose work failed logs the repo and the caller.
            tracing::warn!(error = %e, "cannot sync the directories of dropped files");
        }
        let committed = disk.commit();
        if let Err(error) = &committed {
            tracing::warn!(
                error = %error,
                "cannot commit the index; the next start walks the object directories"
            );
        }
        // Not clean when the run stranded something whose flag write may
        // have failed: the next start then walks.
        let clean = synced.is_ok() && committed.is_ok() && !self.stray.load(Ordering::Relaxed);
        disk.set_flag_logged("clean", clean);
        if committed.is_ok()
            && let Err(error) = disk.checkpoint()
        {
            tracing::warn!(
                %error,
                "cannot checkpoint the index; the next start reads the write-ahead log"
            );
        }
        tracing::info!(
            objects = disk.objects,
            bytes = disk.bytes,
            clean,
            "stopped the disk store"
        );
    }
}

impl Store for DiskStore {
    fn size(&self, key: Key) -> io::Result<Option<usize>> {
        let size = lock(&self.disk).touch(key)?;
        self.flush();
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
        if added.is_err() {
            // The file is in place, but the index may not name it: a walk
            // finds it, this one or the next.
            self.stray.store(true, Ordering::Relaxed);
            lock(&self.disk).set_flag_logged("walk_pending", true);
        }
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
    use std::time::{Duration, Instant};

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
        assert_eq!(lock(&store.memory).lru.bytes, 2 * n);

        // A second put of the same object counts it once.
        store.put(c.0, &c.1).unwrap();
        assert_eq!(lock(&store.memory).lru.bytes, 2 * n);

        // An object larger than the whole limit is not kept, and drops nothing.
        let big = (
            Key::Blob(BlobId::of(b"big")),
            Bytes::from(vec![b'x'; 3 * n]),
        );
        store.put(big.0, &big.1).unwrap();
        assert_eq!(store.size(big.0).unwrap(), None);
        assert_eq!(store.size(a.0).unwrap(), Some(n));
    }

    #[test]
    fn the_memory_store_notes_per_repo_only_what_it_holds() {
        let (a, b, c) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"cccc"));
        let n = a.1.len();
        // Room for two.
        let store = MemoryStore::new(2 * n + n / 2).recording(&["r".into()]);
        let noted = |limit| -> Vec<Key> {
            let snapshot = store.snapshot("r", limit);
            snapshot.into_iter().map(|(key, _)| key).collect()
        };
        // Not stored: not noted.
        store.served("r", a.0);
        assert_eq!(noted(usize::MAX), []);
        store.put(a.0, &a.1).unwrap();
        store.put(b.0, &b.1).unwrap();
        store.served("r", a.0);
        // A repo that the store does not record.
        store.served("q", b.0);
        store.served("r", b.0);
        store.served("r", a.0);
        // Least recently used first. A limit keeps the most recent.
        assert_eq!(noted(usize::MAX), [b.0, a.0]);
        assert_eq!(noted(n), [a.0]);
        assert_eq!(store.snapshot("r", n), std::slice::from_ref(&a));
        assert_eq!(store.snapshot("q", usize::MAX), []);

        // `c` drops `a`, the least recently used in the store; its key
        // leaves the record too.
        store.put(c.0, &c.1).unwrap();
        assert_eq!(noted(usize::MAX), [b.0]);
        assert_eq!(lock(&store.memory).repos["r"].len(), 1);
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
    fn a_file_that_cannot_leave_is_adopted_by_the_next_walk() {
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

        // The file of `a` is still there, and the index does not name it.
        // The stop is clean, but the store owes a walk for the file it
        // could not take out.
        drop(store);
        let store = DiskStore::open(&data, 2 * n).unwrap();
        until(|| store.walk_state().is_some_and(|(_, _, done)| done));
        store.flush();
        assert_eq!(store.size(a.0).unwrap(), Some(n));
    }

    /// The state byte of the old index file in `data`.
    fn old_state(data: &Path) -> u8 {
        fs::read(data.join(OLD_INDEX)).unwrap()[MAGIC.len()]
    }

    /// Writes the old index file of the previous format into `data`.
    fn put_old_back(data: &Path, index: &[u8]) {
        fs::write(data.join(OLD_INDEX), index).unwrap();
    }

    /// Removes the index db and its write-ahead log.
    fn remove_db(data: &Path) {
        for suffix in ["", "-wal", "-shm"] {
            ignore_missing(fs::remove_file(data.join(format!("{INDEX}{suffix}")))).unwrap();
        }
    }

    /// The stored objects, least recently used first.
    fn order(store: &DiskStore) -> Vec<Key> {
        lock(&store.disk).order().unwrap()
    }

    /// Waits until `f` holds, so a test asserts on state, not on timing.
    fn until(f: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !f() {
            assert!(Instant::now() < deadline, "the state never came");
            std::thread::sleep(Duration::from_millis(5));
        }
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
    fn a_start_after_a_clean_stop_trusts_the_index_and_a_crash_walks() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, stray) = (blob(b"aaaa"), blob(b"not in the index"));
        let stray_path = {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            store.put(a.0, &a.1).unwrap();
            store.path(stray.0)
        };
        assert!(clean_flag(&data));
        // A good object file that the index does not name: a start that
        // trusts its rows does not find it, and owes no walk.
        fs::create_dir_all(stray_path.parent().unwrap()).unwrap();
        fs::write(&stray_path, &stray.1).unwrap();
        {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            assert_eq!(store.size(a.0).unwrap(), Some(a.1.len()));
            assert_eq!(store.size(stray.0).unwrap(), None);
            assert!(store.walk_state().is_none());
        }

        // A crash: the store never stopped cleanly, and a file it renamed
        // never reached a committed batch. The next start walks, and
        // adopts the stray, and nothing else.
        {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            store.put(a.0, &a.1).unwrap();
            store.crash();
        }
        fs::create_dir_all(stray_path.parent().unwrap()).unwrap();
        fs::write(&stray_path, &stray.1).unwrap();
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        until(|| store.walk_state().is_some_and(|(_, _, done)| done));
        let (found, bytes, _) = store.walk_state().unwrap();
        store.flush();
        let n = stray.1.len();
        assert_eq!(store.size(stray.0).unwrap(), Some(n));
        assert_eq!(store.get(stray.0, n).unwrap(), Some(stray.1.clone()));
        assert_eq!(found, 1, "only the stray is adopted");
        assert_eq!(bytes, n as u64);
        drop(store);
        // The next start owes no walk: the walk finished, and the stop was
        // clean.
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        assert!(store.walk_state().is_none());
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
    fn a_crash_keeps_what_was_committed_and_loses_only_its_last_uses() {
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
        {
            // The crash loses what was not committed: `c` dropped `a` and
            // its put was committed at once; the use of `b` was not.
            let store = DiskStore::open(&data, limit).unwrap();
            store.put(c.0, &c.1).unwrap();
            assert!(!store.path(a.0).exists());
            assert_eq!(store.size(b.0).unwrap(), Some(n));
            store.crash();
        }
        // The remove of `a` was in the batch that the crash lost, and
        // without it the store is over its limit: the start drops `a`
        // again, whose file the crash had already moved out of its name.
        let store = DiskStore::open(&data, limit).unwrap();
        assert_eq!(order(&store), [b.0, c.0]);
        assert_eq!(store.size(a.0).unwrap(), None);
        assert_eq!(
            usize::try_from(lock(&store.disk).bytes).unwrap(),
            bytes_on_disk(&data)
        );
        assert_eq!(store.get(c.0, n).unwrap(), Some(c.1.clone()));
    }

    /// Writes an old-format index file with `state` and the records `rows`.
    fn old_index(state: u8, rows: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(state);
        out.extend_from_slice(blake3::hash(rows).as_bytes());
        out.extend_from_slice(rows);
        out
    }

    #[test]
    fn the_index_file_of_the_previous_format_is_imported_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b, c) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"cccc"));
        {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            for (key, bytes) in [&a, &b, &c] {
                store.put(*key, bytes).unwrap();
            }
            set_written_in_order(&store, &[a.0, b.0, c.0]);
        }
        // A clean file of the previous format: `b`, `c`, `a`, least
        // recently used first.
        remove_db(&data);
        let mut rows = Vec::new();
        push_record(&mut rows, b.0, Some(b.1.len()));
        push_record(&mut rows, c.0, Some(c.1.len()));
        push_record(&mut rows, a.0, Some(a.1.len()));
        put_old_back(&data, &old_index(CLEAN, &rows));
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        // The order of the file is the order of use of the new index.
        assert_eq!(order(&store), [b.0, c.0, a.0]);
        assert!(!data.join(OLD_INDEX).exists());
        assert_eq!(store.size(c.0).unwrap(), Some(c.1.len()));
    }

    #[test]
    fn a_dirty_file_of_the_previous_format_gives_its_order_before_the_scan() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b, c) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"cccc"));
        {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            for (key, bytes) in [&a, &b, &c] {
                store.put(*key, bytes).unwrap();
            }
            set_written_in_order(&store, &[a.0, b.0, c.0]);
        }
        remove_db(&data);
        // Dirty: `c`, a record that is not one, `b`, `a`.
        let mut rows = Vec::new();
        push_record(&mut rows, c.0, Some(c.1.len()));
        rows.extend_from_slice(&[b'X'; RECORD]);
        push_record(&mut rows, b.0, Some(b.1.len()));
        push_record(&mut rows, a.0, Some(a.1.len()));
        put_old_back(&data, &old_index(DIRTY, &rows));
        assert_eq!(old_state(&data), DIRTY);

        // `c` keeps its place from the file; the rest by write time.
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        assert_eq!(order(&store), [c.0, a.0, b.0]);
    }

    #[test]
    fn a_damaged_file_of_the_previous_format_is_not_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b, stray) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"not in the index"));
        let stray_path = {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            store.put(a.0, &a.1).unwrap();
            store.put(b.0, &b.1).unwrap();
            store.path(stray.0)
        };
        // An object file that the old index does not name: only a scan
        // finds it.
        fs::create_dir_all(stray_path.parent().unwrap()).unwrap();
        fs::write(&stray_path, &stray.1).unwrap();
        let mut rows = Vec::new();
        push_record(&mut rows, a.0, Some(a.1.len()));
        push_record(&mut rows, b.0, Some(b.1.len()));
        let good = old_index(CLEAN, &rows);
        let cut = good[..good.len() - RECORD].to_vec();
        let mut changed = good.clone();
        // The size field of the last record.
        *changed.last_mut().unwrap() ^= 1;
        let mut version = good.clone();
        version[..MAGIC.len()].copy_from_slice(b"scnidx0\n");
        for (case, index, scans) in [
            ("as written", &good, false),
            ("cut at a record", &cut, true),
            ("a record changed", &changed, true),
            ("another version", &version, true),
        ] {
            remove_db(&data);
            put_old_back(&data, index);
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            let found = store.size(stray.0).unwrap().is_some();
            assert_eq!(found, scans, "{case}");
            assert_eq!(store.size(b.0).unwrap(), Some(b.1.len()), "{case}");
        }
    }

    #[test]
    fn a_damaged_index_db_is_set_aside_and_the_files_are_scanned() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, stray) = (blob(b"aaaa"), blob(b"not in the index"));
        let stray_path = {
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            store.put(a.0, &a.1).unwrap();
            store.path(stray.0)
        };
        fs::create_dir_all(stray_path.parent().unwrap()).unwrap();
        fs::write(&stray_path, &stray.1).unwrap();
        // A db that is not a database: the start moves it aside and scans.
        remove_db(&data);
        fs::write(data.join(INDEX), b"not a database at all").unwrap();
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        assert_eq!(store.size(a.0).unwrap(), Some(a.1.len()));
        let n = stray.1.len();
        assert_eq!(store.size(stray.0).unwrap(), Some(n));
        assert!(data.join(format!("{INDEX}.damaged")).exists());
    }

    #[test]
    fn an_index_that_cannot_be_read_is_set_aside() {
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
        let db = data.join(INDEX);
        fs::set_permissions(&db, fs::Permissions::from_mode(0o400)).unwrap();
        // The start goes on with a new index: the files are the truth.
        let store = DiskStore::open(&data, 1 << 20).unwrap();
        assert_eq!(store.get(a.0, a.1.len()).unwrap(), Some(a.1.clone()));
        assert_eq!(store.size(a.0).unwrap(), Some(a.1.len()));
    }

    #[test]
    fn the_memory_of_the_index_stays_bounded_as_objects_grow() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let store = DiskStore::open(&data, 1 << 30).unwrap();
        // The rows of the db grow with the objects; the batch that waits in
        // memory does not: every put commits at once, and the uses of a
        // batch are bounded.
        let key = blob(b"once").0;
        store.put(key, &Bytes::from(vec![b'x'; 8])).unwrap();
        let pages = |store: &DiskStore| {
            lock(&store.disk)
                .conn
                .query_row("PRAGMA page_count", [], |row| row.get::<_, i64>(0))
                .unwrap()
        };
        let mut before = pages(&store);
        let mut biggest_batch = 0;
        for i in 0..3 * FLUSH_RECORDS + 100 {
            store.size(key).unwrap();
            let disk = lock(&store.disk);
            assert!(disk.pending.len() <= FLUSH_RECORDS, "{i} uses wait");
            assert!(disk.newest.len() <= FLUSH_RECORDS, "{i} keys wait");
            biggest_batch = biggest_batch.max(disk.committed);
            drop(disk);
            if i % FLUSH_RECORDS == FLUSH_RECORDS - 1 {
                before = before.max(pages(&store));
            }
        }
        // One batch of uses, not one version per object of the store.
        assert!(biggest_batch <= FLUSH_RECORDS, "{biggest_batch}");
        // The db grew on disk while the memory stayed bounded.
        assert!(pages(&store) > before || before > 0);
    }

    #[test]
    fn an_eviction_reads_only_what_it_drops() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b) = (blob(b"aaaa"), blob(b"bbbb"));
        let n = a.1.len();
        // Room for eight.
        let store = DiskStore::open(&data, 8 * n + n / 2).unwrap();
        for i in 0..8 {
            let key = Key::Blob(BlobId::of(format!("full{i}").as_bytes()));
            store.put(key, &a.1).unwrap();
        }
        // Every put past the limit drops one object, and reads one row of
        // the index: not the eight that stay.
        for i in 0..100 {
            store.put(b.0, &a.1).unwrap();
            let disk = lock(&store.disk);
            assert!(
                disk.iterated <= 2,
                "the {}th eviction read {} rows",
                i,
                disk.iterated
            );
            assert_eq!(disk.objects, 8);
            assert_eq!(usize::try_from(disk.bytes).unwrap(), 8 * n);
            drop(disk);
        }
        assert_eq!(store.size(b.0).unwrap(), Some(n));
        drop(store);
        let store = DiskStore::open(&data, 8 * n + n / 2).unwrap();
        assert_eq!(lock(&store.disk).objects, 8);
    }

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn a_walk_adopts_the_files_that_a_crash_left_and_the_limit_counts_them() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b, c, d, e) = (
            blob(b"aaaa"),
            blob(b"bbbb"),
            blob(b"cccc"),
            blob(b"dddd"),
            blob(b"eeee"),
        );
        let n = a.1.len();
        // Room for two.
        let limit = 2 * n + n / 2;
        {
            // The crash: a stop never came, so the next start walks.
            let store = DiskStore::open(&data, limit).unwrap();
            store.put(a.0, &a.1).unwrap();
            store.put(b.0, &b.1).unwrap();
            // Their last uses are committed, so the files that the walk
            // adopts are older than every row: they leave first.
            store.size(a.0).unwrap();
            store.size(b.0).unwrap();
            lock(&store.disk).commit().unwrap();
            store.crash();
        }
        // The files of `c` and `d` were renamed into place, and their rows
        // never reached a committed batch.
        for (key, bytes) in [&c, &d] {
            let path = object_path(&data.join("objects"), *key);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, bytes).unwrap();
        }
        let store = DiskStore::open(&data, limit).unwrap();
        until(|| store.walk_state().is_some_and(|(_, _, done)| done));
        // The walk adopted the two, and counted their bytes.
        let (found, bytes, _) = store.walk_state().unwrap();
        store.flush();
        lock(&store.disk).commit().unwrap();
        assert_eq!(found, 2, "{found} files adopted");
        assert_eq!(bytes, (c.1.len() + d.1.len()) as u64);
        assert_eq!(db_size(&data, c.0), Some(c.1.len()));
        assert_eq!(db_size(&data, d.0), Some(d.1.len()));
        assert_eq!(bytes_on_disk(&data), 4 * n);

        // The adopted are the least recently used, so `e` comes in and
        // `c`, then `d`, then `a` leave, until the limit holds again.
        store.put(e.0, &e.1).unwrap();
        assert_eq!(store.size(c.0).unwrap(), None);
        assert_eq!(store.size(d.0).unwrap(), None);
        assert_eq!(store.size(a.0).unwrap(), None);
        assert!(!store.path(c.0).exists());
        assert!(!store.path(d.0).exists());
        assert_eq!(bytes_on_disk(&data), 2 * n);
        assert_eq!(store.size(b.0).unwrap(), Some(n));
        assert_eq!(store.size(e.0).unwrap(), Some(n));
        drop(store);

        // And what the walk counted stays counted over a restart.
        let store = DiskStore::open(&data, limit).unwrap();
        assert_eq!(usize::try_from(lock(&store.disk).bytes).unwrap(), 2 * n);
        assert_eq!(bytes_on_disk(&data), 2 * n);
    }

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn a_commit_that_fails_keeps_the_rows_the_truth() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let (a, b, c) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"cccc"));
        let n = a.1.len();
        let store = DiskStore::open(&data, 2 * n).unwrap();
        store.put(a.0, &a.1).unwrap();
        store.put(b.0, &b.1).unwrap();

        // The index cannot take a write.
        lock(&store.disk)
            .conn
            .execute_batch("PRAGMA query_only = ON")
            .unwrap();

        // The put's file lands, its row does not: the batch is lost whole,
        // and the counters are the db's again.
        assert!(store.put(c.0, &c.1).is_err());
        assert_eq!(store.size(c.0).unwrap(), None);

        // The index writes again: the next batch commits counters that the
        // rows say too, and drops in the order of use, `a` and not `b`.
        lock(&store.disk)
            .conn
            .execute_batch("PRAGMA query_only = OFF")
            .unwrap();
        let d = blob(b"dddd");
        store.put(d.0, &d.1).unwrap();
        assert_eq!(store.size(b.0).unwrap(), Some(n));
        assert_eq!(usize::try_from(lock(&store.disk).bytes).unwrap(), 2 * n);
        drop(store);

        // The stop was not clean: the store owed a walk, though it could
        // not write the flag that says so.
        let store = DiskStore::open(&data, 2 * n).unwrap();
        until(|| store.walk_state().is_some_and(|(_, _, done)| done));
        let (found, bytes, _) = store.walk_state().unwrap();
        store.flush();
        lock(&store.disk).commit().unwrap();
        assert_eq!(found, 1, "{found} files adopted");
        assert_eq!(bytes, n as u64);
        assert_eq!(db_size(&data, c.0), Some(c.1.len()));
        drop(store);

        // The walk paid the debt: the next start is clean, and owes no walk.
        // The store is over its limit again, so the least recently used
        // goes: `c`, which the walk brought back last as the oldest.
        let store = DiskStore::open(&data, 2 * n).unwrap();
        assert!(store.walk_state().is_none());
        assert_eq!(store.size(c.0).unwrap(), None);
        assert_eq!(store.size(b.0).unwrap(), Some(n));
        assert_eq!(usize::try_from(lock(&store.disk).bytes).unwrap(), 2 * n);
        assert_eq!(bytes_on_disk(&data), 2 * n);
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
        // A crash of the machine left the first bytes only. A trusted index
        // gives the size of the object, a scan that of the file: neither is
        // trusted for the bytes.
        let torn = &bytes[..bytes.len() - 1];
        for (scan, size) in [(false, bytes.len()), (true, torn.len())] {
            fs::write(&path, torn).unwrap();
            if scan {
                remove_db(&data);
            }
            let store = DiskStore::open(&data, 1 << 20).unwrap();
            assert_eq!(store.size(key).unwrap(), Some(size));
            assert_eq!(store.get(key, size).unwrap(), None);
            assert_eq!(store.size(key).unwrap(), None);
            assert!(!path.exists());
            assert_eq!(lock(&store.disk).bytes, 0);
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

    /// The size that the index db in `data` gives for `key`, without using
    /// the store, so no use is marked.
    fn db_size(data: &Path, key: Key) -> Option<usize> {
        rusqlite::Connection::open(data.join(INDEX))
            .unwrap()
            .query_row(
                "SELECT size FROM objects WHERE kind = ?1 AND id = ?2",
                rusqlite::params![kind_of(key), key.id()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .unwrap()
            .map(|size| usize::try_from(size).unwrap())
    }

    /// The `clean` flag of the index db in `data`.
    fn clean_flag(data: &Path) -> bool {
        rusqlite::Connection::open_with_flags(
            data.join(INDEX),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap()
        .query_row("SELECT v FROM meta WHERE k = 'clean'", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap()
            != 0
    }
}
