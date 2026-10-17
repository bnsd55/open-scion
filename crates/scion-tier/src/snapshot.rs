//! Snapshots of the objects that a cache sent for one repo, so that a cold
//! daemon can start with them instead of fetching them all
//! ([#59](https://github.com/bnsd55/open-scion/issues/59)).
//!
//! A snapshot is one file: [`MAGIC`], the repo name (its length as a `u16`,
//! little-endian, then its bytes), then one entry per object, least recently
//! used first. An entry is a record as in the disk store's index (an op `T`
//! or `B`, the ID, the size as a `u32`, little-endian), then the object in
//! its stored form: a tree's `TreeV1` bytes, a blob's zstd frame. A reader
//! trusts none of it: it checks each object against its ID before it stores
//! the object.

use std::fmt::Write as _;
use std::fs;
use std::io::{self, BufRead as _, BufReader, BufWriter, ErrorKind, Read as _, Write as _};
use std::path::Path;

use scion_proto::{Bytes, check};

use crate::Key;
use crate::store::{DiskStore, RECORD, Store as _, parse_record, push_record};

/// The start of a snapshot file.
pub const MAGIC: &[u8; 9] = b"scnsnap1\n";

/// The file name of `repo`'s snapshot: `REPO.scnsnap`, with each byte other
/// than an ASCII letter, a digit, `-`, `_` or a `.` after the first byte
/// written as `%XX`. So any repo name gives one plain name in the snapshot
/// directory: `team/app` gives `team%2Fapp.scnsnap`.
#[must_use]
pub fn file_name(repo: &str) -> String {
    let mut name = String::with_capacity(repo.len() + 8);
    for (i, byte) in repo.bytes().enumerate() {
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' || (byte == b'.' && i > 0) {
            name.push(char::from(byte));
        } else {
            // Writing to a `String` does not fail.
            let _ = write!(name, "%{byte:02X}");
        }
    }
    name + ".scnsnap"
}

/// Writes a snapshot of `repo` that holds `objects`, least recently used
/// first, at `path`. Writes a temp file next to it (mode 0600), syncs it,
/// and renames it over `path`, so a reader never sees half a file. Returns
/// the bytes of the objects.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`] if `repo` is longer than 65,535 bytes, or
/// I/O errors.
pub fn write(path: &Path, repo: &str, objects: &[(Key, Bytes)]) -> io::Result<usize> {
    let name = u16::try_from(repo.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "the repo name is too long"))?;
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let mut out = BufWriter::new(tempfile::NamedTempFile::new_in(dir)?);
    out.write_all(MAGIC)?;
    out.write_all(&name.to_le_bytes())?;
    out.write_all(repo.as_bytes())?;
    let mut record = Vec::with_capacity(RECORD);
    let mut bytes = 0;
    for (key, data) in objects {
        record.clear();
        push_record(&mut record, *key, Some(data.len()));
        out.write_all(&record)?;
        out.write_all(data)?;
        bytes += data.len();
    }
    let file = out.into_inner().map_err(io::IntoInnerError::into_error)?;
    // A plain `fsync`, as the store's data syncs: on macOS not past the
    // drive's cache. A lost tail is a torn snapshot: the objects before it
    // still seed a cold daemon.
    rustix::fs::fsync(file.as_file()).map_err(io::Error::from)?;
    file.persist(path).map_err(|e| e.error)?;
    Ok(bytes)
}

/// What [`seed`] did.
#[derive(Debug)]
pub struct Seeded {
    /// The repo that the snapshot names.
    pub repo: String,
    /// Objects that matched their IDs, handed to the store.
    pub stored: usize,
    /// Their bytes, in stored form.
    pub bytes: usize,
    /// Objects whose bytes are not the object that their ID names. Not
    /// stored.
    pub skipped: usize,
    /// The file ended in the middle of an entry, or held something that is
    /// not one. The objects before it count.
    pub damaged: bool,
}

/// Seeds `store` from the snapshot at `path`, if the store holds no object.
/// Checks each object against its ID, as a fetch from upstream does, and
/// stores only those that match, through the store's limit. Holds one object
/// at a time. `None` if the store holds objects: a warm daemon keeps what it
/// has, and the file is not opened.
///
/// # Errors
///
/// [`ErrorKind::InvalidData`] if the file is not a snapshot, or I/O errors,
/// such as [`ErrorKind::NotFound`] for a missing file.
pub fn seed(store: &DiskStore, path: &Path) -> io::Result<Option<Seeded>> {
    if !store.is_empty() {
        return Ok(None);
    }
    let not_snapshot = |why: &str| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("{} is not a snapshot: {why}", path.display()),
        )
    };
    let mut file = BufReader::new(fs::File::open(path)?);
    let mut header = [0; MAGIC.len() + 2];
    if !fill(&mut file, &mut header)? || !header.starts_with(MAGIC) {
        return Err(not_snapshot("it does not start as one"));
    }
    let len = usize::from(u16::from_le_bytes([
        header[MAGIC.len()],
        header[MAGIC.len() + 1],
    ]));
    let mut name = vec![0; len];
    if !fill(&mut file, &mut name)? {
        return Err(not_snapshot("it ends in its repo name"));
    }
    let repo = String::from_utf8(name)
        .ok()
        .filter(|repo| check::name("repo", repo).is_ok())
        .ok_or_else(|| not_snapshot("its repo name is not valid"))?;
    let mut seeded = Seeded {
        repo,
        stored: 0,
        bytes: 0,
        skipped: 0,
        damaged: false,
    };
    let mut record = [0; RECORD];
    while !file.fill_buf()?.is_empty() {
        // A size over the largest object is not a record.
        let entry = fill(&mut file, &mut record)?
            .then(|| parse_record(&record))
            .flatten();
        let Some((key, Some(size))) = entry else {
            seeded.damaged = true;
            break;
        };
        // Grows as it reads, so a size that the file does not hold costs
        // only what the file holds.
        let mut data = Vec::new();
        file.by_ref().take(size as u64).read_to_end(&mut data)?;
        if data.len() != size {
            seeded.damaged = true;
            break;
        }
        if let Err(why) = key.check_stored(&data) {
            tracing::debug!(%key, %why, "a snapshot object does not match its ID");
            seeded.skipped += 1;
            continue;
        }
        store.put(key, &Bytes::from(data))?;
        seeded.stored += 1;
        seeded.bytes += size;
    }
    Ok(Some(seeded))
}

/// Fills `buf` from `file`. `false` if the file ends first.
fn fill(file: &mut impl io::Read, buf: &mut [u8]) -> io::Result<bool> {
    match file.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use scion_core::{BlobId, Entry, FileEntry, FileState, PathElem, Tree};
    use scion_proto::object;
    use scion_proto::ticket::{self, now};
    use scion_proto::v1::objects_server::Objects as _;
    use scion_proto::v1::{Encoding, GetObjectsRequest};
    use tokio_stream::StreamExt as _;
    use tonic::Request;

    use super::*;
    use crate::{DAEMON_UPSTREAM_TIMEOUT, MemoryStore, Store, Tier};

    /// A blob's key and stored form.
    fn blob(data: &[u8]) -> (Key, Bytes) {
        (
            Key::Blob(BlobId::of(data)),
            Bytes::from(object::pack(data).unwrap()),
        )
    }

    /// A tree, two blobs, and a snapshot of them in `dir`, written by a
    /// cache that sent them for repo `r` in that order. The tree reaches
    /// both blobs at `one` and `two`, so a ticket for it serves them.
    fn snapshot_in(dir: &Path) -> (Vec<(Key, Bytes)>, std::path::PathBuf, [u8; 32]) {
        let entries = [("one", b"one\n".as_slice()), ("two", b"two\n")].map(|(name, data)| {
            (
                PathElem::new(name.as_bytes().to_vec()).unwrap(),
                Entry::File(FileEntry::new(
                    FileState::Regular {
                        content: BlobId::of(data),
                        executable: false,
                    },
                    data.len() as u64,
                )),
            )
        });
        let (tree, id) = Tree {
            entries: entries.into_iter().collect(),
        }
        .encode_with_id();
        let objects = vec![
            (Key::Tree(id), Bytes::from(tree)),
            blob(b"one\n"),
            blob(b"two\n"),
        ];
        let cache = MemoryStore::new(1 << 20).recording(&["r".into()]);
        for (key, data) in &objects {
            cache.put(*key, data).unwrap();
            cache.served("r", *key);
        }
        let path = dir.join("r.scnsnap");
        let bytes = write(&path, "r", &cache.snapshot("r", 1 << 20)).unwrap();
        assert_eq!(bytes, objects.iter().map(|(_, data)| data.len()).sum());
        (objects, path, *id.as_bytes())
    }

    #[test]
    fn any_repo_name_gives_one_plain_file_name() {
        assert_eq!(file_name("main"), "main.scnsnap");
        assert_eq!(file_name("team/app"), "team%2Fapp.scnsnap");
        assert_eq!(file_name("../x"), "%2E.%2Fx.scnsnap");
        assert_eq!(file_name("a%b é"), "a%25b%20%C3%A9.scnsnap");
    }

    #[test]
    fn a_cold_store_starts_with_the_snapshot_and_a_warm_one_ignores_it() {
        let dir = tempfile::tempdir().unwrap();
        let (objects, path, _) = snapshot_in(dir.path());
        let size = |i: usize| objects[i].1.len();
        // Room for the last two only: the least recently used drops first.
        let limit = size(1) + size(2);
        let store = DiskStore::open(&dir.path().join("data"), limit).unwrap();
        let seeded = seed(&store, &path).unwrap().unwrap();
        assert_eq!(seeded.repo, "r");
        assert_eq!((seeded.stored, seeded.skipped), (3, 0));
        assert!(!seeded.damaged);
        assert_eq!(store.size(objects[0].0).unwrap(), None);
        for (key, data) in &objects[1..] {
            assert_eq!(store.get(*key, data.len()).unwrap(), Some(data.clone()));
        }

        // A warm store does not open the file.
        let missing = dir.path().join("missing.scnsnap");
        assert!(seed(&store, &missing).unwrap().is_none());
        let cold = DiskStore::open(&dir.path().join("cold"), 1 << 20).unwrap();
        let err = seed(&cold, &missing).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn a_bad_object_is_skipped_and_a_torn_file_keeps_the_objects_before_the_tear() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, c) = (blob(b"aaaa"), blob(b"bbbb"), blob(b"cccc"));
        // `b`'s ID with bytes that are another blob.
        let lie = (b.0, blob(b"not b").1);
        let path = dir.path().join("r.scnsnap");
        write(&path, "r", &[a.clone(), lie, c.clone()]).unwrap();
        let store = DiskStore::open(&dir.path().join("one"), 1 << 20).unwrap();
        let seeded = seed(&store, &path).unwrap().unwrap();
        assert_eq!((seeded.stored, seeded.skipped), (2, 1));
        assert!(!seeded.damaged);
        assert_eq!(store.size(b.0).unwrap(), None);
        assert_eq!(store.get(c.0, c.1.len()).unwrap(), Some(c.1.clone()));

        // Cut in the middle of the last object.
        let mut torn = fs::read(&path).unwrap();
        torn.truncate(torn.len() - 2);
        fs::write(&path, torn).unwrap();
        let store = DiskStore::open(&dir.path().join("two"), 1 << 20).unwrap();
        let seeded = seed(&store, &path).unwrap().unwrap();
        assert_eq!((seeded.stored, seeded.skipped), (1, 1));
        assert!(seeded.damaged);
        assert_eq!(store.get(a.0, a.1.len()).unwrap(), Some(a.1.clone()));
        assert_eq!(store.size(c.0).unwrap(), None);

        // Not a snapshot at all.
        fs::write(&path, b"scnidx1\nc").unwrap();
        let store = DiskStore::open(&dir.path().join("three"), 1 << 20).unwrap();
        let err = seed(&store, &path).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData, "{err}");
    }

    #[tokio::test]
    async fn a_seeded_daemon_serves_the_snapshot_without_its_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let (objects, path, root) = snapshot_in(dir.path());
        let store = DiskStore::open(&dir.path().join("data"), 1 << 20).unwrap();
        seed(&store, &path).unwrap().unwrap();
        // Nothing listens on the discard port.
        let key = ed25519_dalek::SigningKey::from_bytes(&[6; 32]);
        let tier = Tier::new(
            store,
            "http://127.0.0.1:9",
            "t",
            DAEMON_UPSTREAM_TIMEOUT,
            None,
            vec![key.verifying_key()],
        )
        .unwrap();
        // A ticket for the seeded root tree; its walk finds everything in
        // the store, so the daemon serves without its upstream.
        let ticket = ticket::sign(&key, "r", "test", root, now()).unwrap();
        let paths = [b"one".to_vec(), b"two".to_vec()];
        let request = GetObjectsRequest {
            refs: objects[1..]
                .iter()
                .zip(paths)
                .map(|((key, _), path)| key.to_ref(&path))
                .collect(),
            accept: vec![Encoding::Zstd.into()],
            ticket,
        };
        let reply = tier.get_objects(Request::new(request)).await.unwrap();
        let parts: Vec<_> = reply.into_inner().collect().await;
        assert_eq!(parts.len(), objects.len() - 1);
        for (part, (key, data)) in parts.into_iter().zip(&objects[1..]) {
            let part = part.unwrap();
            assert_eq!(part.id, key.id());
            assert_eq!(part.data, *data);
        }
        assert_eq!(tier.upstream_calls(), 0);
    }
}
