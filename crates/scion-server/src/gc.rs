//! `scion-server gc <data>`: removes the blob files and the `repo_blobs`
//! rows that nothing names any more, over the crate's internal metadata
//! trait, so one collector serves every backend. Its queries are methods
//! of that trait; its files are the object folder, which every backend
//! keeps on disk.
//!
//! A blob file is shared by content across repos, so what is garbage is a
//! question with two levels:
//!
//! - a `repo_blobs` row is garbage when no stored changeset of its repo
//!   puts its blob, and the blob file is older than the grace period;
//! - a blob file is garbage when no `repo_blobs` row of any repo names it,
//!   and it is older than the grace period.
//!
//! The grace period keeps a blob that a client uploaded and is about to
//! name with a `CreateChangeset`: its row and file are younger than the
//! grace, so the collector leaves them. `PutBlob` of a blob that is already
//! stored keeps the copy and touches its file, so the grace counts from the
//! last upload, not the first.
//!
//! Rows go before files, the mirror of every writer's order (the file is
//! durable first, then the row names it): a crash mid-GC leaves orphan
//! files only, never a row that names a missing file. `fsck` checks that
//! invariant, and the next run of `gc` removes the orphan files.
//!
//! It runs beside a live server without a race:
//!
//! - The live set comes from one batched read walk, like `fsck`'s.
//! - Each repo's dead rows go in one short write transaction. Inside that
//!   transaction the collector re-reads the repo's changesets that
//!   committed after the snapshot walk and adds their puts to the live
//!   set, so a changeset that committed after the snapshot pins the old
//!   blobs that it puts. The server has one writer, so nothing commits
//!   during the transaction: a `CreateChangeset` either committed before
//!   it (and is seen), or runs after it (finds no row, refuses, and the
//!   client uploads again).
//! - The sweep unlinks inside short write transactions of
//!   [`SWEEP_BATCH`] files each: the row check and the unlink share the
//!   write lock, so no writer can add a row between them - and a writer
//!   need not rewrite the file to add one, so a check outside the lock is
//!   not enough. The mtime re-stat stays as a second guard. An unlink is
//!   not rolled back by the transaction; a crash after it and before the
//!   commit is fine, because the file had no row.
//!
//! A reader that is between its row check and its file open when the sweep
//! unlinks sees the same window that `fsck` reads through; a read of a
//! blob whose row is gone is refused. GC never runs inside `serve` in this
//! version: an operator runs it, from cron or by hand.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use scion_core::{BlobId, Changeset, ChangesetId};

use crate::backend::Backend;
use crate::db;
use crate::meta::{Meta as _, MetaTx as _, RepoChangeset, RepoId};
use std::str::FromStr as _;

/// Rows to read per batch, as `fsck` reads.
pub const BATCH: usize = 4096;

/// Files to check and unlink per write transaction of the sweep, so a
/// writer waits milliseconds, not the whole sweep.
pub const SWEEP_BATCH: usize = 1000;

/// The dead-row candidates one prune transaction deletes, at most: one
/// transaction holds the collector's key, so its length bounds how long
/// every writer waits behind it.
const PRUNE_BATCH: usize = 256;

/// The time one prune transaction or sweep batch may hold the collector's
/// key before the run says so in the log. The row cap bounds the hold;
/// this clock only reports the batches that ran past it.
const BATCH_BUDGET: Duration = Duration::from_millis(100);

/// How long between two progress lines of a long run: a collector over
/// millions of rows stays silent for minutes otherwise, and an operator
/// tailing the log cannot tell a slow walk from a hung one.
const PROGRESS_LOG: Duration = Duration::from_secs(10);

/// The grace period, when `--grace-secs` is not given: seven days.
pub const DEFAULT_GRACE_SECS: u64 = 7 * 24 * 60 * 60;

/// A stage of the collection, reported between two batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// The live-set walk over the stored changesets.
    LiveSet,
    /// The dead-row prune of one repo.
    RepoRows,
    /// The sweep over the blob files.
    BlobFiles,
}

/// What one run collected.
#[derive(Debug, Default)]
pub struct Report {
    /// The repos whose rows were looked at.
    pub repos: u64,
    /// The `repo_blobs` rows removed.
    pub rows_pruned: u64,
    /// The rows that were dead but whose blob was younger than the grace,
    /// so they were left.
    pub rows_young: u64,
    /// The blob files unlinked.
    pub files_removed: u64,
    /// The files that no row names but whose mtime was younger than the
    /// grace, so they were left for a later run.
    pub files_young: u64,
}

impl Report {
    /// Whether the run collected nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows_pruned == 0 && self.files_removed == 0
    }
}

/// Collects what nothing names in the data directory of `store`.
///
/// `between` runs between two batches of the live-set walk, after each
/// repo's prune, and before the file sweep; a test uses it to write while
/// the collector reads. With `dry_run`, nothing is removed and the report
/// says what a real run would collect.
///
/// # Errors
///
/// Errors of the database or the file system.
pub fn run(
    store: &Backend,
    grace: Duration,
    dry_run: bool,
    between: &mut dyn FnMut(Stage),
) -> crate::error::Result<Report> {
    let started = std::time::Instant::now();
    let mut report = Report::default();
    let live = live_set(store, between)?;
    // One read names the repos for the start line and the prune; prune
    // rows walks them right after.
    let repos = store.read(|c| c.repos())?;
    tracing::info!(
        repos = u64::try_from(repos.len()).unwrap_or(u64::MAX),
        grace_secs = grace.as_secs(),
        dry_run,
        "the collector walks the live set"
    );
    prune_rows(store, &live, repos, grace, dry_run, between, &mut report)?;
    sweep_files(store, grace, dry_run, between, &mut report)?;
    tracing::info!(
        repos = report.repos,
        rows_pruned = report.rows_pruned,
        files_removed = report.files_removed,
        rows_young = report.rows_young,
        files_young = report.files_young,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "the collector ended"
    );
    Ok(report)
}

/// The blobs that each repo's stored changesets put, read in batches.
///
/// A batch runs in one short read transaction, so a live server keeps
/// writing; changesets that commit later are picked up when their repo's
/// rows are pruned.
fn live_set(store: &Backend, between: &mut dyn FnMut(Stage)) -> crate::error::Result<LiveSet> {
    let mut live = LiveSet::default();
    let mut last: Option<(RepoId, Vec<u8>)> = None;
    loop {
        let (from_repo, from_cs) = last.clone().unwrap_or((RepoId(0), Vec::new()));
        let rows = store.read(|c| c.repo_changesets_after(from_repo, from_cs.as_slice(), BATCH))?;
        let count = rows.len();
        let next = rows.last().map(|row| (row.0, row.1.clone()));
        for RepoChangeset(repo, cs, body) in rows {
            let (cs, _) = Changeset::decode_with_id(&body)
                .map_err(|e| crate::Error::Corrupt(format!("changeset {}: {e}", cs_string(&cs))))?;
            live.note(repo, &cs);
        }
        last = next;
        between(Stage::LiveSet);
        if count < BATCH {
            break;
        }
    }
    Ok(live)
}

/// The live sets of every repo, and the changesets that were read.
#[derive(Default)]
struct LiveSet {
    /// The blobs that each repo's stored changesets put.
    live: BTreeMap<RepoId, BTreeSet<BlobId>>,
    /// The changesets that the snapshot walk read, per repo. The prune of a
    /// repo re-reads its changesets and decodes only the new ones.
    seen: HashMap<RepoId, BTreeSet<ChangesetId>>,
}

impl LiveSet {
    /// Records the puts of `cs` as live for its repo.
    fn note(&mut self, repo: RepoId, cs: &Changeset) {
        let blobs = self.live.entry(repo).or_default();
        for (_, blob) in db::puts(cs) {
            blobs.insert(blob);
        }
        self.seen.entry(repo).or_default().insert(cs.id());
    }
}

/// Prunes the dead `repo_blobs` rows of each repo, in chunks of at most
/// `PRUNE_BATCH` rows, one transaction per chunk.
fn prune_rows(
    store: &Backend,
    live: &LiveSet,
    repos: Vec<(RepoId, String)>,
    grace: Duration,
    dry_run: bool,
    between: &mut dyn FnMut(Stage),
    report: &mut Report,
) -> crate::error::Result<()> {
    // Every repo, not only the ones with changesets: a repo whose rows all
    // came from uploads gets its prune too.
    report.repos = u64::try_from(repos.len()).unwrap_or(u64::MAX);
    let total = report.repos;
    let mut done = 0u64;
    let mut logged = std::time::Instant::now();
    for (repo, name) in repos {
        // The candidate scan reads rows and files: it runs in a read
        // transaction, so it does not hold the collector's key while it
        // stats the files.
        let dead = store.read(|c| {
            let live_of_repo = live.live.get(&repo);
            // The rows dead per the snapshot, and old enough. The row's age
            // is its file's: every writer makes the file durable first, and
            // names it right after. A row whose file is gone is dead at any
            // age: it grants nothing, and a later upload makes it again.
            let mut dead = Vec::new();
            for blob in c.repo_blob_ids(repo)? {
                let Ok(blob) = BlobId::try_from(blob.as_slice()) else {
                    continue; // fsck reports the bad ID
                };
                if live_of_repo.is_some_and(|live| live.contains(&blob)) {
                    continue;
                }
                match file_age(store, blob)? {
                    Some(age) if age < grace => {
                        report.rows_young += 1;
                    }
                    _ => dead.push(blob),
                }
            }
            Ok(dead)
        })?;
        // The collector's key (Postgres) and the one write connection
        // (SQLite) already order the collector's delete transactions
        // against every changeset transaction: a writer that began first
        // commits before a delete chunk begins, and a writer that begins
        // after the chunk waits for it.
        // One transaction per chunk of at most PRUNE_BATCH rows: a
        // transaction holds the collector's key from its begin to its
        // commit, so its length is how long every writer waits. A
        // changeset that committed after the snapshot walk pins the blobs
        // that it puts: the re-read inside each chunk runs after the key
        // was taken, so it sees every changeset that committed while this
        // transaction waited for one.
        // SHORTCUT: the re-read loads every changeset ID of the repo once
        // per chunk, so a repo with many changesets pays that walk per
        // chunk. Fine at 256 rows a chunk; if a chunk ever runs past its
        // budget in the log, load the IDs once per repo per sweep and
        // share them across chunks, at the cost of a wider pin window.
        let mut seen = live.seen.get(&repo).cloned().unwrap_or_default();
        for chunk in dead.chunks(PRUNE_BATCH) {
            let started = std::time::Instant::now();
            store.write(|c| {
                // The changesets this repo gained since the snapshot walk:
                // their IDs, collected under the write lock the deletes
                // share.
                let mut unseen = Vec::new();
                for cs in c.repo_changeset_ids(repo)? {
                    let Ok(cs) = ChangesetId::try_from(cs.as_slice()) else {
                        continue; // fsck reports the bad ID
                    };
                    if seen.insert(cs) {
                        unseen.push(cs.as_bytes().to_vec());
                    }
                }
                // The puts of every new changeset, bodies in chunks of
                // [`DERIVED_BATCH`]: one statement per 1,000 ids instead
                // of one per changeset. A missing body is skipped, as the
                // walk reads it: `fsck` reports it.
                // SHORTCUT: 1,000 bodies per statement; memory is 1,000 x
                // the largest body in a chunk (an imported initial commit
                // can hold MAX_CHANGES changes). Upgrade path if that
                // matters: chunk by bytes (octet_length(body)) or a
                // cursor with a fetch size.
                let mut fresh = BTreeSet::new();
                for ids in unseen.chunks(crate::meta::DERIVED_BATCH) {
                    for (id, body) in c.stored_changeset_bodies(ids)? {
                        let Ok(cs) = ChangesetId::try_from(id.as_slice()) else {
                            continue; // fsck reports the bad ID
                        };
                        let (cs, _) = Changeset::decode_with_id(&body)
                            .map_err(|e| crate::Error::Corrupt(format!("changeset {cs}: {e}")))?;
                        for (_, blob) in db::puts(&cs) {
                            fresh.insert(blob);
                        }
                    }
                }
                // The chunk's dead rows leave in one statement.
                let gone: Vec<Vec<u8>> = chunk
                    .iter()
                    .filter(|blob| !fresh.contains(*blob))
                    .map(|blob| blob.as_bytes().to_vec())
                    .collect();
                if !dry_run && !gone.is_empty() {
                    c.delete_repo_blobs(repo, &gone)?;
                }
                report.rows_pruned += u64::try_from(gone.len()).unwrap_or(u64::MAX);
                Ok(())
            })?;
            tracing::info!(
                repo = %name,
                rows = chunk.len(),
                elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "the collector pruned a batch of rows"
            );
            if started.elapsed() > BATCH_BUDGET {
                tracing::warn!(
                    rows = chunk.len(),
                    elapsed_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                    "the collector's prune batch ran past its budget"
                );
            }
            between(Stage::RepoRows);
        }
        done += 1;
        // One progress line at most every `PROGRESS_LOG`, with the repos
        // done and the total: the counts live here, where the walk runs,
        // so the callback stays as it is.
        if logged.elapsed() >= PROGRESS_LOG {
            logged = std::time::Instant::now();
            tracing::info!(done, total, "the collector's progress over the repos");
        }
    }
    Ok(())
}

/// Unlinks the blob files that no row names, oldest first.
fn sweep_files(
    store: &Backend,
    grace: Duration,
    dry_run: bool,
    between: &mut dyn FnMut(Stage),
    report: &mut Report,
) -> crate::error::Result<()> {
    let mut candidates = Vec::new();
    for (path, listed) in blob_files(store)? {
        let Some(id) = blob_id_of(&path) else {
            continue; // not a blob name: fsck reports it, the sweep leaves it
        };
        let Some(age) = file_age(store, id)? else {
            continue; // gone when listed: a collector that runs twice
        };
        if age < grace {
            report.files_young += 1;
            continue;
        }
        candidates.push((path, listed, id));
    }
    // The listing is done; a test writes between it and the sweep.
    between(Stage::BlobFiles);
    // The sweep's row check and unlink run inside a write transaction, so
    // no writer can add a row between the two: on the data directory the
    // one write connection orders the sweep against the changeset, and on
    // a shared database every write transaction holds the collector's key
    // from its begin (`Pg::commit`), the way the SQLite one-write-
    // connection store serializes. The mtime re-stat stays as a second
    // guard. An unlink is not rolled back by the transaction, and a crash
    // after it and before the commit is fine: the file had no row.
    candidates.sort_by(|a, b| a.2.as_bytes().cmp(b.2.as_bytes()));
    for batch in candidates.chunks(SWEEP_BATCH) {
        let started = std::time::Instant::now();
        store.write(|c| {
            for (path, listed, id) in batch {
                // The directory's flock orders this unlink against a put
                // of the same blob in another process: a touch or a
                // rename that lands first changes the mtime, so the
                // re-stat below skips the file, and an unlink that lands
                // first makes the put's keep see a missing file and
                // write anew. The metadata transaction orders the row
                // check; the file work itself needs this lock.
                let _dir = store.blobs().dir_flock(*id)?;
                if c.any_repo_names(id.as_bytes())? {
                    continue;
                }
                if file_mtime(path)? != Some(*listed) {
                    continue;
                }
                if !dry_run {
                    std::fs::remove_file(path)?;
                }
                report.files_removed += 1;
            }
            Ok(())
        })?;
        tracing::info!(
            files = batch.len(),
            removed = report.files_removed,
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "the collector swept a batch of files"
        );
    }
    Ok(())
}

/// Every file under the blob root, with the mtime that the listing saw.
fn blob_files(store: &Backend) -> crate::error::Result<Vec<(PathBuf, SystemTime)>> {
    let root = store.blobs().root().to_path_buf();
    let mut files = Vec::new();
    let dirs = match std::fs::read_dir(&root) {
        Ok(dirs) => dirs,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(files),
        Err(e) => return Err(e.into()),
    };
    for dir in dirs {
        let dir = dir?;
        if !dir.path().is_dir() {
            continue;
        }
        for file in std::fs::read_dir(dir.path())? {
            let file = file?;
            let path = file.path();
            if path.is_file() {
                let mtime = file.metadata()?.modified()?;
                files.push((path, mtime));
            }
        }
    }
    Ok(files)
}

/// The blob ID that a blob file's name holds.
fn blob_id_of(path: &Path) -> Option<BlobId> {
    let name = path.file_name()?.to_string_lossy().into_owned();
    let fan = path.parent()?.file_name()?.to_string_lossy().into_owned();
    BlobId::from_str(&format!("{fan}{name}")).ok()
}

/// The age of a blob file, or `None` when it is not there.
fn file_age(store: &Backend, id: BlobId) -> crate::error::Result<Option<Duration>> {
    let (_, path) = store.blobs().location(id);
    Ok(file_mtime(&path)?.map(|mtime| {
        SystemTime::now()
            .duration_since(mtime)
            .unwrap_or(Duration::ZERO)
    }))
}

/// The mtime of `path`, or `None` when it is not there.
fn file_mtime(path: &Path) -> std::io::Result<Option<SystemTime>> {
    match std::fs::metadata(path) {
        Ok(meta) => Ok(meta.modified().ok()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Prints a changeset ID for an error message.
fn cs_string(cs: &[u8]) -> String {
    ChangesetId::try_from(cs).map_or_else(|_| "unknown".to_owned(), |id| id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Caller;
    use crate::backend::Backend;
    use crate::db::SqliteTx;
    use crate::deadline::Deadline;
    use crate::land::{LandRequest, Lands};
    use crate::store::Store;
    use scion_core::{FileState, IdentityTime, RepoPath};
    use std::time::Instant;
    use tempfile::TempDir;

    const REPO: &str = "gc";
    /// Older than the default grace of seven days.
    const OLD: Duration = Duration::from_secs(8 * 24 * 60 * 60);

    /// A changeset on `parent` that puts `files`.
    fn cs(parent: Option<ChangesetId>, files: &[(&str, FileState)]) -> Changeset {
        Changeset::new(
            parent.into_iter().collect(),
            IdentityTime::new(
                b"Ada".to_vec(),
                b"ada@example.com".to_vec(),
                1_700_000_000,
                0,
            )
            .expect("identity fits the format"),
            None,
            b"gc\n".to_vec(),
            files
                .iter()
                .map(|(path, state)| (RepoPath::new(*path).unwrap(), Some(*state)))
                .collect(),
        )
        .expect("changeset fits the format")
    }

    /// A file state whose content is `text`.
    fn file_of(store: &Store, text: &[u8]) -> FileState {
        FileState::Regular {
            content: store.blobs().put(text).unwrap(),
            executable: false,
        }
    }

    /// Stores `cs` in the repo and moves `main` to it, as a land does.
    fn land(store: &Store, cs: &Changeset) -> ChangesetId {
        store
            .write(|c| {
                let mut tx = SqliteTx(c);
                let repo = tx.create_repo(REPO)?;
                let (id, _) = crate::tree::store_changeset(
                    &mut tx,
                    repo,
                    cs,
                    &mut crate::tree::lens_of(&Backend::sqlite(store.clone())),
                )?;
                let version = tx.get_ref(repo, "main")?.map(|r| r.version);
                assert!(tx.cas_ref(repo, "main", version, id)?);
                Ok(id)
            })
            .unwrap()
    }

    /// A store with one repo and no changesets.
    fn store() -> (TempDir, Store) {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store
            .write(|c| {
                let mut tx = SqliteTx(c);
                tx.create_repo(REPO)?;
                Ok(())
            })
            .unwrap();
        (dir, store)
    }

    /// Puts `text` as a blob file and names it from the repo, without a
    /// changeset: the state after a `PutBlob`.
    fn uploaded(store: &Store, text: &[u8]) -> BlobId {
        let id = store.blobs().put(text).unwrap();
        store
            .write(|c| {
                let mut tx = SqliteTx(c);
                let repo = tx.repo_id(REPO)?;
                tx.add_repo_blob(repo, id)
            })
            .unwrap();
        id
    }

    /// Makes the blob file of `id` look `age` old.
    fn age_file(store: &Backend, id: BlobId, age: Duration) {
        let (_, path) = store.blobs().location(id);
        let file = std::fs::File::options().write(true).open(&path).unwrap();
        file.set_modified(SystemTime::now() - age).unwrap();
    }

    /// The mtime of the blob file of `id`.
    fn mtime_of(store: &Store, id: BlobId) -> SystemTime {
        let (_, path) = store.blobs().location(id);
        std::fs::metadata(&path).unwrap().modified().unwrap()
    }

    /// Whether `repo` names `blob`.
    fn named(store: &Store, blob: BlobId) -> bool {
        store.read(|c| c.any_repo_names(blob.as_bytes())).unwrap()
    }

    /// One run with the default grace, through the stages.
    fn collect(store: &Backend, grace: Duration, between: &mut dyn FnMut(Stage)) -> Report {
        run(store, grace, false, between).unwrap()
    }

    #[test]
    fn gc_removes_what_nothing_names() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"orphan\n");
        age_file(&Backend::sqlite(store.clone()), x, OLD);
        let report = collect(
            &Backend::sqlite(store.clone()),
            Duration::from_secs(DEFAULT_GRACE_SECS),
            &mut |_| {},
        );
        assert_eq!(report.rows_pruned, 1, "{report:?}");
        assert_eq!(report.files_removed, 1, "{report:?}");
        assert!(!named(&store, x));
        assert!(matches!(
            store.blobs().verify(x).unwrap(),
            crate::store::BlobStatus::Missing
        ));
    }

    #[test]
    fn gc_keeps_what_a_changeset_puts() {
        let (_dir, store) = store();
        let x = store.blobs().put(b"kept\n").unwrap();
        land(&store, &cs(None, &[("a.txt", file_of(&store, b"kept\n"))]));
        age_file(&Backend::sqlite(store.clone()), x, OLD);
        let report = collect(
            &Backend::sqlite(store.clone()),
            Duration::from_secs(DEFAULT_GRACE_SECS),
            &mut |_| {},
        );
        assert_eq!(report.rows_pruned, 0, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert!(named(&store, x));
    }

    #[test]
    fn gc_keeps_a_fresh_upload() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"fresh\n");
        let report = collect(
            &Backend::sqlite(store.clone()),
            Duration::from_secs(DEFAULT_GRACE_SECS),
            &mut |_| {},
        );
        assert_eq!(report.rows_young, 1, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert!(named(&store, x));
    }

    #[test]
    fn a_blob_one_repo_names_survives_another_repos_dead_row() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"shared\n");
        // A second repo uploads the same content and never names it in a
        // changeset.
        store
            .write(|c| {
                let mut tx = SqliteTx(c);
                let other = tx.create_repo("other")?;
                tx.add_repo_blob(other, x)
            })
            .unwrap();
        land(
            &store,
            &cs(None, &[("a.txt", file_of(&store, b"shared\n"))]),
        );
        age_file(&Backend::sqlite(store.clone()), x, OLD);
        let report = collect(
            &Backend::sqlite(store.clone()),
            Duration::from_secs(DEFAULT_GRACE_SECS),
            &mut |_| {},
        );
        assert_eq!(report.rows_pruned, 1, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert!(named(&store, x));
    }

    #[test]
    fn a_changeset_after_the_snapshot_pins_its_blobs() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"pinned\n");
        age_file(&Backend::sqlite(store.clone()), x, OLD);
        let mut injected = false;
        let report = {
            let store = &store;
            let mut between = |stage: Stage| {
                if stage == Stage::LiveSet && !injected {
                    injected = true;
                    // A changeset commits after the snapshot walk and names
                    // the old blob `x` without uploading it again, as a
                    // `CreateChangeset` of an old upload does. The prune
                    // that follows must see it.
                    let state = FileState::Regular {
                        content: x,
                        executable: false,
                    };
                    land(store, &cs(None, &[("x.txt", state)]));
                }
            };
            collect(
                &Backend::sqlite(store.clone()),
                Duration::from_secs(DEFAULT_GRACE_SECS),
                &mut between,
            )
        };
        assert!(injected);
        assert_eq!(report.rows_pruned, 0, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert!(named(&store, x));
    }

    #[test]
    fn a_put_that_keeps_the_copy_restarts_the_grace() {
        let (_dir, store) = store();
        let text = b"again\n";
        let x = uploaded(&store, text);
        age_file(&Backend::sqlite(store.clone()), x, OLD);
        let before = mtime_of(&store, x);
        std::thread::sleep(Duration::from_millis(20));
        // The second upload of the same content keeps the stored copy.
        assert_eq!(store.blobs().put(text).unwrap(), x);
        let after = mtime_of(&store, x);
        assert!(after > before, "the keep must touch the file: {before:?}");
        // The touched file is younger than the grace, so the run keeps it.
        let report = collect(
            &Backend::sqlite(store.clone()),
            Duration::from_secs(DEFAULT_GRACE_SECS),
            &mut |_| {},
        );
        assert_eq!(report.rows_pruned, 0, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert!(named(&store, x));
    }

    #[test]
    fn a_pending_lands_changesets_are_live() {
        let (_dir, store) = store();
        let root = land(&store, &cs(None, &[("a.txt", file_of(&store, b"one\n"))]));
        let tip = land(
            &store,
            &cs(Some(root), &[("b.txt", file_of(&store, b"two\n"))]),
        );
        // A land whose deadline has passed stays pending: its changeset is
        // stored, so its puts are already live.
        let lands = Lands::new(None, None);
        let req = LandRequest {
            repo: REPO.to_owned(),
            changeset: tip,
            target_ref: "main".into(),
            request_key: "late".into(),
            deadline: Deadline::new(Instant::now()),
        };
        let err = lands
            .land(&Backend::sqlite(store.clone()), &Caller("t".into()), &req)
            .unwrap_err();
        assert!(matches!(err, crate::Error::DeadlineExceeded(_)), "{err:?}");
        // Every stored blob is old, and nothing new is orphaned.
        let report = collect(&Backend::sqlite(store.clone()), Duration::ZERO, &mut |_| {});
        assert_eq!(report.rows_pruned, 0, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
    }

    #[test]
    fn rows_go_before_files_so_a_crash_leaves_no_missing_file() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"crash\n");
        age_file(&Backend::sqlite(store.clone()), x, OLD);
        // What a crash between the two stages leaves: the rows are gone,
        // the file is an orphan.
        store
            .write(|c| {
                let repo = c.repo_id(REPO)?;
                c.delete_repo_blob(repo, x.as_bytes())
            })
            .unwrap();
        assert!(!named(&store, x));
        // No row names a missing file: the fsck invariant holds.
        store
            .read(|c| {
                for (repo, blob) in c.repo_blob_rows_after(RepoId(0), &[], usize::MAX)? {
                    let id = BlobId::try_from(blob.as_slice()).unwrap();
                    let (_, path) = store.blobs().location(id);
                    assert!(path.exists(), "row (repo {}) names a missing file", repo.0);
                }
                Ok(())
            })
            .unwrap();
        // The next run removes the orphan file.
        let report = collect(&Backend::sqlite(store.clone()), Duration::ZERO, &mut |_| {});
        assert_eq!(report.files_removed, 1, "{report:?}");
    }

    #[test]
    fn a_fresh_orphan_file_survives_one_gc() {
        let (_dir, store) = store();
        let x = store.blobs().put(b"later\n").unwrap();
        let report = collect(
            &Backend::sqlite(store.clone()),
            Duration::from_secs(DEFAULT_GRACE_SECS),
            &mut |_| {},
        );
        assert_eq!(report.files_young, 1, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        // Once it is old, the next run removes it.
        age_file(&Backend::sqlite(store.clone()), x, OLD);
        let report = collect(
            &Backend::sqlite(store.clone()),
            Duration::from_secs(DEFAULT_GRACE_SECS),
            &mut |_| {},
        );
        assert_eq!(report.files_removed, 1, "{report:?}");
    }

    /// The sweep of a file another process is writing waits for the
    /// process's flock of the file's fan-out directory: it never unlinks
    /// a file under a put that is about to rename or touch it, and a
    /// touch that landed under the lock makes the sweep skip the file.
    /// A build without the flock unlinks the aged orphan right away.
    #[test]
    fn a_sweep_of_a_file_another_process_writes_waits_for_its_flock() {
        let (dir, store) = store();
        // A second process beside the collector, the way a serving server
        // and `scion-server gc` run.
        let other = Store::open_shared(dir.path()).unwrap();
        let x = store.blobs().put(b"raced\n").unwrap();
        age_file(&Backend::sqlite(store.clone()), x, OLD);

        // The put of the other process holds the lock while it works on
        // the file. Every wait below is bounded.
        let held = other.blobs().dir_flock(x).unwrap();
        let listed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let listed2 = listed.clone();
        let (done, done_rx) = std::sync::mpsc::channel();
        let backend = Backend::sqlite(store.clone());
        std::thread::spawn(move || {
            let report = collect(
                &backend,
                Duration::from_secs(DEFAULT_GRACE_SECS),
                &mut |stage: Stage| {
                    if stage == Stage::BlobFiles {
                        listed2.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                },
            );
            let _ = done.send(report);
        });
        let started = std::time::Instant::now();
        while !listed.load(std::sync::atomic::Ordering::SeqCst) {
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "gc never listed"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        // The sweep listed the file and reached its re-stat and unlink.
        // With the flock it waits; without it the file is gone by now.
        std::thread::sleep(Duration::from_millis(150));
        let (_, path) = store.blobs().location(x);
        assert!(path.exists(), "the sweep unlinked a file a put was writing");
        // The put's touch lands under the lock, so its mtime is young and
        // the sweep that follows skips the file.
        let file = std::fs::File::options().write(true).open(&path).unwrap();
        file.set_modified(SystemTime::now()).unwrap();
        drop(held);
        let report = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("gc never finished");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert!(path.exists());
        assert_eq!(
            store.blobs().verify(x).unwrap(),
            crate::store::BlobStatus::Valid
        );
    }

    #[test]
    fn a_row_added_between_the_listing_and_the_sweep_keeps_its_file() {
        let (_dir, store) = store();
        let x = store.blobs().put(b"listed\n").unwrap();
        age_file(&Backend::sqlite(store.clone()), x, OLD);
        let mut injected = false;
        let report = {
            let mut between = |stage: Stage| {
                if stage == Stage::BlobFiles && !injected {
                    injected = true;
                    // A changeset commits after the listing and names the
                    // listed blob. `insert_changeset` adds its row without
                    // writing the file: the blob is already stored.
                    let state = FileState::Regular {
                        content: x,
                        executable: false,
                    };
                    land(&store, &cs(None, &[("x.txt", state)]));
                }
            };
            collect(
                &Backend::sqlite(store.clone()),
                Duration::from_secs(DEFAULT_GRACE_SECS),
                &mut between,
            )
        };
        assert!(injected);
        assert_eq!(report.files_removed, 0, "{report:?}");
        // The file is there, and fsck finds nothing.
        assert_eq!(
            store.blobs().verify(x).unwrap(),
            crate::store::BlobStatus::Valid
        );
        let check = crate::fsck::run(&Backend::sqlite(store.clone()), 1, &mut |_| {}).unwrap();
        assert!(check.is_clean(), "{check:?}");
    }

    #[test]
    fn a_dry_run_removes_nothing() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"dry\n");
        age_file(&Backend::sqlite(store.clone()), x, OLD);
        let mut removed = 0;
        let mut between = |stage: Stage| {
            if stage == Stage::BlobFiles {
                removed += 1;
            }
        };
        let report = run(
            &Backend::sqlite(store.clone()),
            Duration::from_secs(DEFAULT_GRACE_SECS),
            true,
            &mut between,
        )
        .unwrap();
        // The report says what the stages would collect. In a real run the
        // rows go first, and the sweep of the same run would then see no
        // row; a dry run keeps the rows, so the sweep keeps the file too.
        assert_eq!(report.rows_pruned, 1, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        // Nothing was removed: the row and the file are still there.
        assert!(named(&store, x));
        assert_eq!(
            store.blobs().verify(x).unwrap(),
            crate::store::BlobStatus::Valid
        );
        // The real run after it collects both.
        let real = collect(
            &Backend::sqlite(store.clone()),
            Duration::from_secs(DEFAULT_GRACE_SECS),
            &mut |_| {},
        );
        assert_eq!(real.rows_pruned, 1, "{real:?}");
        assert_eq!(real.files_removed, 1, "{real:?}");
    }

    #[test]
    fn a_store_that_never_holds_blobs_collects_nothing() {
        let (_dir, store) = store();
        let report = collect(
            &Backend::sqlite(store.clone()),
            Duration::from_secs(DEFAULT_GRACE_SECS),
            &mut |_| {},
        );
        assert_eq!(report.repos, 1, "{report:?}");
        assert_eq!(report.rows_pruned, 0, "{report:?}");
        assert_eq!(report.rows_young, 0, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert_eq!(report.files_young, 0, "{report:?}");
    }

    /// A changeset on no parent that puts `blob`.
    fn cs_put(blob: BlobId) -> Changeset {
        cs(
            None,
            &[(
                "pinned.txt",
                FileState::Regular {
                    content: blob,
                    executable: false,
                },
            )],
        )
    }

    /// A changeset that names the blob commits between the collector's
    /// snapshot and its prune: the re-read inside the prune sees it, and
    /// the blob's row and file survive.
    #[test]
    fn a_changeset_that_commits_while_gc_runs_keeps_its_blob() {
        let dir = TempDir::new().unwrap();
        let store = Backend::open_test_shared(dir.path()).unwrap();
        let repo = store.write(|c| c.create_repo(REPO)).unwrap();
        let b = store.blobs().put(b"pinned\n").unwrap();
        store.write(|c| c.add_repo_blob(repo, b)).unwrap();
        age_file(&store, b, OLD);
        let committed = std::sync::atomic::AtomicBool::new(false);
        let report = collect(&store, Duration::from_secs(1), &mut |stage| {
            if stage == Stage::LiveSet && !committed.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                // The changeset commits after the snapshot and before
                // the prune, the way a client that lands mid-gc does.
                store
                    .write(|c| {
                        c.insert_changeset(repo, &cs_put(b))?;
                        c.add_repo_blob(repo, b)
                    })
                    .unwrap();
            }
        });
        assert_eq!(report.rows_pruned, 0, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert!(
            store.read(|c| c.any_repo_names(b.as_bytes())).unwrap(),
            "the row vanished"
        );
        let (_, path) = store.blobs().location(b);
        assert!(path.exists(), "the file vanished");
    }

    /// The interleaving the lock exists for: a changeset transaction holds
    /// the blob's lock while the collector prunes; the collector waits, its
    /// re-read then sees the changeset, and the file survives.
    #[test]
    fn the_collector_waits_for_a_changeset_that_holds_the_blob_lock() {
        if !Backend::test_is_postgres() {
            eprintln!("skip on sqlite: the lock is the write lock itself");
            return;
        }
        // The collector side opens the way the `gc` CLI does - shared,
        // with no one-server lock - and the writer the way a server
        // does, with the per-schema lock. One data directory, as they
        // share in production: the store UUID pairs it with the schema.
        let dir = TempDir::new().unwrap();
        let gc_store = Backend::open_test_shared(dir.path()).unwrap();
        let writer = Backend::open_test(dir.path()).unwrap();
        let repo = gc_store.write(|c| c.create_repo(REPO)).unwrap();
        let b = gc_store.blobs().put(b"locked\n").unwrap();
        writer.write(|c| c.add_repo_blob(repo, b)).unwrap();
        age_file(&gc_store, b, OLD);
        let cs = cs_put(b);

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let writer_thread = std::thread::spawn(move || {
            writer
                .write(|c| {
                    // The transaction took the collector's key at its
                    // begin (`Pg::commit`): the collector's batches wait
                    // for it from here to its commit.
                    started_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                    c.insert_changeset(repo, &cs)?;
                    c.add_repo_blob(repo, b)?;
                    Ok(())
                })
                .unwrap();
        });
        started_rx
            .recv()
            .expect("the changeset transaction took the key");
        // The snapshot runs; the prune then waits for the changeset.
        let (live_tx, live_rx) = std::sync::mpsc::channel();
        let gc_thread = {
            let gc_store = gc_store.clone();
            std::thread::spawn(move || {
                run(&gc_store, Duration::ZERO, false, &mut |stage| {
                    if stage == Stage::LiveSet {
                        let _ = live_tx.send(());
                    }
                })
                .unwrap()
            })
        };
        live_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("the collector did not reach its snapshot");
        go_tx.send(()).expect("the changeset transaction is alive");
        writer_thread.join().unwrap();
        let report = gc_thread.join().unwrap();
        assert_eq!(report.rows_pruned, 0, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert!(
            gc_store.read(|c| c.any_repo_names(b.as_bytes())).unwrap(),
            "the row vanished"
        );
        let (_, path) = gc_store.blobs().location(b);
        assert!(path.exists(), "the file was removed");
    }

    /// A prune bigger than one chunk yields the collector's key between
    /// chunks: the hook fires once per chunk, so a run over more than
    /// `PRUNE_BATCH` dead rows holds the key twice, never once for all of
    /// them. The count, not a clock, is the proof: each transaction
    /// deletes at most `PRUNE_BATCH` rows.
    #[test]
    fn a_prune_bigger_than_a_chunk_yields_the_key_between_chunks() {
        let dir = TempDir::new().unwrap();
        let store = Backend::open_test_shared(dir.path()).unwrap();
        let repo = store.write(|c| c.create_repo(REPO)).unwrap();
        let blobs: Vec<BlobId> = (0..PRUNE_BATCH + 2)
            .map(|n| {
                let blob = store
                    .blobs()
                    .put(format!("chunk {n}\n").as_bytes())
                    .unwrap();
                store.write(|c| c.add_repo_blob(repo, blob)).unwrap();
                blob
            })
            .collect();
        for blob in &blobs {
            age_file(&store, *blob, OLD);
        }
        let chunks = std::cell::Cell::new(0u32);
        let report = collect(&store, Duration::from_secs(1), &mut |stage| {
            if stage == Stage::RepoRows {
                chunks.set(chunks.get() + 1);
            }
        });
        assert_eq!(chunks.get(), 2, "the prune held the key once for every row");
        assert_eq!(report.rows_pruned, blobs.len() as u64, "{report:?}");
        assert_eq!(report.files_removed, blobs.len() as u64, "{report:?}");
    }

    use crate::write::{PARK_CREATE, PARK_PUT, create_changeset, put_blob};

    /// Waits until a writer parked in one of the write parks, named by
    /// the marker file of `id`.
    fn wait_parked(hooks: &Path, id: BlobId) {
        let marker = hooks.join(format!("parked-{id}"));
        let give_up = std::time::Instant::now() + Duration::from_secs(10);
        while !marker.exists() {
            assert!(
                std::time::Instant::now() < give_up,
                "the writer never parked"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The collector takes the row and the file while a `create_changeset`
    /// sits parked between its file check and its write transaction. The
    /// commit may not name a blob whose file the collector removed: the
    /// create refuses (the client uploads again), or it commits and the
    /// named blob reads back. A commit that names a file the sweep
    /// unlinked loses the file's bytes for every reader of the path.
    #[test]
    fn a_create_over_a_collected_blob_does_not_name_its_lost_file() {
        let dir = TempDir::new().unwrap();
        let store = Backend::open_test_shared(dir.path()).unwrap();
        let repo = store.write(|c| c.create_repo(REPO)).unwrap();
        let text = b"named\n";
        let upload = store.blobs().upload().unwrap().append(text).unwrap();
        let b = put_blob(&store, repo, upload).unwrap();
        age_file(&store, b, OLD);
        let root = store
            .write(|c| c.insert_changeset(repo, &cs(None, &[])))
            .unwrap();
        let mut changes = std::collections::BTreeMap::new();
        changes.insert(
            RepoPath::new("f").unwrap(),
            Some(FileState::Regular {
                content: b,
                executable: false,
            }),
        );
        let bytes = Changeset::new(
            vec![root],
            IdentityTime::new(
                b"Ada".to_vec(),
                b"ada@example.com".to_vec(),
                1_700_000_000,
                0,
            )
            .expect("identity fits the format"),
            None,
            b"write\n".to_vec(),
            changes,
        )
        .expect("changeset fits the format")
        .encode();

        // The create parks after its file check; the collector then takes
        // the row and the file the check saw.
        let hooks = dir.path().join("hooks");
        std::fs::create_dir_all(&hooks).unwrap();
        PARK_CREATE.arm(&hooks);
        std::fs::write(hooks.join(format!("park-{b}")), b"").unwrap();
        let writer = store.clone();
        let created = std::thread::spawn(move || {
            create_changeset(&writer, REPO, &bytes).map_err(|e| e.to_string())
        });
        wait_parked(&hooks, b);
        let report = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
        assert_eq!(report.rows_pruned, 1, "{report:?}");
        assert_eq!(report.files_removed, 1, "{report:?}");

        std::fs::remove_file(hooks.join(format!("park-{b}"))).unwrap();
        let answer = created.join().expect("the create's thread answers");
        PARK_CREATE.lift();
        match answer {
            // The refuse sends the client to upload again: nothing names
            // the file the collector removed.
            Err(why) => assert!(why.contains("is not stored"), "{why}"),
            // A commit may only name blobs that read back.
            Ok(_) => assert_eq!(
                store.blobs().get(b).unwrap(),
                text,
                "the named blob reads back"
            ),
        }
    }

    /// The collector takes the row while a `PutBlob` sits parked after
    /// its member read, and takes the file too. The reply must still
    /// leave the row behind: a `PutBlob` that answers ok stores the blob
    /// for the repo, and the next changeset may name it.
    #[test]
    fn a_put_blob_that_answers_ok_names_the_blob_over_a_concurrent_gc() {
        let dir = TempDir::new().unwrap();
        let store = Backend::open_test_shared(dir.path()).unwrap();
        let repo = store.write(|c| c.create_repo(REPO)).unwrap();
        let upload = store.blobs().upload().unwrap().append(b"kept\n").unwrap();
        let b = put_blob(&store, repo, upload).unwrap();
        age_file(&store, b, OLD);

        // The PutBlob parks after its member read said true; the
        // collector then prunes the row and unlinks the file.
        let hooks = dir.path().join("hooks");
        std::fs::create_dir_all(&hooks).unwrap();
        PARK_PUT.arm(&hooks);
        std::fs::write(hooks.join(format!("park-{b}")), b"").unwrap();
        let writer = store.clone();
        let put_thread = std::thread::spawn(move || {
            let upload = writer.blobs().upload().unwrap().append(b"kept\n").unwrap();
            put_blob(&writer, repo, upload).map_err(|e| e.to_string())
        });
        wait_parked(&hooks, b);
        let report = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
        assert_eq!(report.rows_pruned, 1, "{report:?}");
        assert_eq!(report.files_removed, 1, "{report:?}");

        std::fs::remove_file(hooks.join(format!("park-{b}"))).unwrap();
        let answer = put_thread.join().expect("the PutBlob's thread answers");
        PARK_PUT.lift();
        answer.expect("the PutBlob answers ok");
        assert!(
            store.read(|c| c.any_repo_names(b.as_bytes())).unwrap(),
            "the row of a PutBlob that answered ok vanished"
        );
        assert_eq!(
            store.blobs().get(b).unwrap(),
            b"kept\n",
            "the blob reads back"
        );
    }
}
