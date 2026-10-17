//! `scion-server gc <data>`: removes the blob files and the `repo_blobs`
//! rows that nothing names any more. It belongs to the SQLite backend: its
//! SQL lives in `db.rs`, and another backend brings its own collector.
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

use crate::db;
use crate::store::Store;
use std::str::FromStr as _;

/// Rows to read per batch, as `fsck` reads.
pub const BATCH: usize = 4096;

/// Files to check and unlink per write transaction of the sweep, so a
/// writer waits milliseconds, not the whole sweep.
pub const SWEEP_BATCH: usize = 1000;

/// The grace period, when `--grace-secs` is not given: seven days.
pub const DEFAULT_GRACE_SECS: u64 = 7 * 24 * 60 * 60;

/// A stage of the collection, reported between two batches or repos.
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
    store: &Store,
    grace: Duration,
    dry_run: bool,
    between: &mut dyn FnMut(Stage),
) -> crate::error::Result<Report> {
    let mut report = Report::default();
    let live = live_set(store, between)?;
    prune_rows(store, &live, grace, dry_run, between, &mut report)?;
    sweep_files(store, grace, dry_run, between, &mut report)?;
    Ok(report)
}

/// The blobs that each repo's stored changesets put, read in batches.
///
/// A batch runs in one short read transaction, so a live server keeps
/// writing; changesets that commit later are picked up when their repo's
/// rows are pruned.
fn live_set(store: &Store, between: &mut dyn FnMut(Stage)) -> crate::error::Result<LiveSet> {
    let mut live = LiveSet::default();
    let mut last: Option<(i64, Vec<u8>)> = None;
    loop {
        let (from_repo, from_cs) = last.clone().unwrap_or((0, Vec::new()));
        let rows = store.read(|c| db::gc_changesets(c, from_repo, from_cs.as_slice(), BATCH))?;
        let count = rows.len();
        let next = rows.last().map(|(repo, cs, _)| (*repo, cs.clone()));
        for (repo, cs, body) in rows {
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
    live: BTreeMap<i64, BTreeSet<BlobId>>,
    /// The changesets that the snapshot walk read, per repo. The prune of a
    /// repo re-reads its changesets and decodes only the new ones.
    seen: HashMap<i64, BTreeSet<ChangesetId>>,
}

impl LiveSet {
    /// Records the puts of `cs` as live for its repo.
    fn note(&mut self, repo: i64, cs: &Changeset) {
        let blobs = self.live.entry(repo).or_default();
        for (_, blob) in db::puts(cs) {
            blobs.insert(blob);
        }
        self.seen.entry(repo).or_default().insert(cs.id());
    }
}

/// Prunes the dead `repo_blobs` rows of each repo, one transaction per repo.
fn prune_rows(
    store: &Store,
    live: &LiveSet,
    grace: Duration,
    dry_run: bool,
    between: &mut dyn FnMut(Stage),
    report: &mut Report,
) -> crate::error::Result<()> {
    // Every repo, not only the ones with changesets: a repo whose rows all
    // came from uploads gets its prune too.
    let repos = store.read(db::gc_repos)?;
    report.repos = u64::try_from(repos.len()).unwrap_or(u64::MAX);
    for repo in repos {
        store.write(|c| {
            // A changeset that committed after the snapshot walk pins the
            // blobs that it puts, whatever their age. The transaction sees
            // every commit before it, and the one writer of the server
            // commits nothing while it runs.
            let live_of_repo = live.live.get(&repo);
            let mut seen = live.seen.get(&repo).cloned().unwrap_or_default();
            let mut fresh = BTreeSet::new();
            for cs in db::gc_repo_changesets(c, repo)? {
                let Ok(cs) = ChangesetId::try_from(cs.as_slice()) else {
                    continue; // fsck reports the bad ID
                };
                if seen.insert(cs) {
                    let Some(body) = db::gc_changeset_body(c, cs.as_bytes())? else {
                        continue;
                    };
                    let (cs, _) = Changeset::decode_with_id(&body)
                        .map_err(|e| crate::Error::Corrupt(format!("changeset {cs}: {e}")))?;
                    for (_, blob) in db::puts(&cs) {
                        fresh.insert(blob);
                    }
                }
            }
            for blob in db::gc_repo_blobs(c, repo)? {
                let Ok(blob) = BlobId::try_from(blob.as_slice()) else {
                    continue; // fsck reports the bad ID
                };
                if live_of_repo.is_some_and(|live| live.contains(&blob)) || fresh.contains(&blob) {
                    continue;
                }
                // The row's age is its file's: every writer makes the file
                // durable first, and names it right after. A row whose file
                // is gone is dead at any age: it grants nothing, and a
                // later upload makes it again.
                match file_age(store, blob)? {
                    Some(age) if age < grace => {
                        report.rows_young += 1;
                    }
                    _ => {
                        if !dry_run {
                            db::gc_delete_repo_blob(c, repo, blob.as_bytes())?;
                        }
                        report.rows_pruned += 1;
                    }
                }
            }
            Ok(())
        })?;
        between(Stage::RepoRows);
    }
    Ok(())
}

/// Unlinks the blob files that no row names, oldest first.
fn sweep_files(
    store: &Store,
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
    // The unlink runs inside a write transaction with the row check, so no
    // writer can add a row between the two: the server has one writer, and
    // it waits for the transaction. The mtime re-stat stays as a second
    // guard. An unlink is not rolled back by the transaction, and a crash
    // after it and before the commit is fine: the file had no row.
    for batch in candidates.chunks(SWEEP_BATCH) {
        store.write(|c| {
            for (path, listed, id) in batch {
                if db::gc_any_repo_blob(c, id.as_bytes())? {
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
    }
    Ok(())
}

/// Every file under the blob root, with the mtime that the listing saw.
fn blob_files(store: &Store) -> crate::error::Result<Vec<(PathBuf, SystemTime)>> {
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
fn file_age(store: &Store, id: BlobId) -> crate::error::Result<Option<Duration>> {
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
    use crate::db::SqliteTx;
    use crate::deadline::Deadline;
    use crate::land::{LandRequest, Lands};
    use crate::meta::MetaTx as _;
    use scion_core::{FileState, IdentityTime, RepoPath};
    use std::time::Instant;
    use tempfile::TempDir;

    const REPO: &str = "gc";
    /// Older than the default grace of seven days.
    const OLD: Duration = Duration::from_secs(8 * 24 * 60 * 60);

    /// A changeset on `parent` that puts `files`.
    fn cs(parent: Option<ChangesetId>, files: &[(&str, FileState)]) -> Changeset {
        Changeset {
            parents: parent.into_iter().collect(),
            author: IdentityTime {
                name: b"Ada".to_vec(),
                email: b"ada@example.com".to_vec(),
                unix_secs: 1_700_000_000,
                tz_offset_minutes: 0,
            },
            committer: None,
            message: b"gc\n".to_vec(),
            changes: files
                .iter()
                .map(|(path, state)| (RepoPath::new(*path).unwrap(), Some(*state)))
                .collect(),
        }
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
                    &mut crate::tree::lens_of(store),
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
    fn age_file(store: &Store, id: BlobId, age: Duration) {
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
        store
            .read(|c| db::gc_any_repo_blob(c, blob.as_bytes()))
            .unwrap()
    }

    /// One run with the default grace, through the stages.
    fn collect(store: &Store, grace: Duration, between: &mut dyn FnMut(Stage)) -> Report {
        run(store, grace, false, between).unwrap()
    }

    #[test]
    fn gc_removes_what_nothing_names() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"orphan\n");
        age_file(&store, x, OLD);
        let report = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
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
        age_file(&store, x, OLD);
        let report = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
        assert_eq!(report.rows_pruned, 0, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert!(named(&store, x));
    }

    #[test]
    fn gc_keeps_a_fresh_upload() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"fresh\n");
        let report = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
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
        age_file(&store, x, OLD);
        let report = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
        assert_eq!(report.rows_pruned, 1, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert!(named(&store, x));
    }

    #[test]
    fn a_changeset_after_the_snapshot_pins_its_blobs() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"pinned\n");
        age_file(&store, x, OLD);
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
            collect(store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut between)
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
        age_file(&store, x, OLD);
        let before = mtime_of(&store, x);
        std::thread::sleep(Duration::from_millis(20));
        // The second upload of the same content keeps the stored copy.
        assert_eq!(store.blobs().put(text).unwrap(), x);
        let after = mtime_of(&store, x);
        assert!(after > before, "the keep must touch the file: {before:?}");
        // The touched file is younger than the grace, so the run keeps it.
        let report = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
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
        let err = lands.land(&store, &Caller("t".into()), &req).unwrap_err();
        assert!(matches!(err, crate::Error::DeadlineExceeded(_)), "{err:?}");
        // Every stored blob is old, and nothing new is orphaned.
        let report = collect(&store, Duration::ZERO, &mut |_| {});
        assert_eq!(report.rows_pruned, 0, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
    }

    #[test]
    fn rows_go_before_files_so_a_crash_leaves_no_missing_file() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"crash\n");
        age_file(&store, x, OLD);
        // What a crash between the two stages leaves: the rows are gone,
        // the file is an orphan.
        store
            .write(|c| {
                let mut tx = SqliteTx(c);
                let repo = tx.repo_id(REPO)?;
                db::gc_delete_repo_blob(&mut tx, repo.0, x.as_bytes())
            })
            .unwrap();
        assert!(!named(&store, x));
        // No row names a missing file: the fsck invariant holds.
        store
            .read(|c| {
                let mut tx = SqliteTx(c);
                for (repo, blob) in db::fsck_repo_blobs(&mut tx, 0, &[], usize::MAX)? {
                    let id = BlobId::try_from(blob.as_slice()).unwrap();
                    let (_, path) = store.blobs().location(id);
                    assert!(path.exists(), "row (repo {repo}) names a missing file");
                }
                Ok(())
            })
            .unwrap();
        // The next run removes the orphan file.
        let report = collect(&store, Duration::ZERO, &mut |_| {});
        assert_eq!(report.files_removed, 1, "{report:?}");
    }

    #[test]
    fn a_fresh_orphan_file_survives_one_gc() {
        let (_dir, store) = store();
        let x = store.blobs().put(b"later\n").unwrap();
        let report = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
        assert_eq!(report.files_young, 1, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        // Once it is old, the next run removes it.
        age_file(&store, x, OLD);
        let report = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
        assert_eq!(report.files_removed, 1, "{report:?}");
    }

    #[test]
    fn a_row_added_between_the_listing_and_the_sweep_keeps_its_file() {
        let (_dir, store) = store();
        let x = store.blobs().put(b"listed\n").unwrap();
        age_file(&store, x, OLD);
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
                &store,
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
        let check = crate::fsck::run(&store, 1, &mut |_| {}).unwrap();
        assert!(check.is_clean(), "{check:?}");
    }

    #[test]
    fn a_dry_run_removes_nothing() {
        let (_dir, store) = store();
        let x = uploaded(&store, b"dry\n");
        age_file(&store, x, OLD);
        let mut removed = 0;
        let mut between = |stage: Stage| {
            if stage == Stage::BlobFiles {
                removed += 1;
            }
        };
        let report = run(
            &store,
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
        let real = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
        assert_eq!(real.rows_pruned, 1, "{real:?}");
        assert_eq!(real.files_removed, 1, "{real:?}");
    }

    #[test]
    fn a_store_that_never_holds_blobs_collects_nothing() {
        let (_dir, store) = store();
        let report = collect(&store, Duration::from_secs(DEFAULT_GRACE_SECS), &mut |_| {});
        assert_eq!(report.repos, 1, "{report:?}");
        assert_eq!(report.rows_pruned, 0, "{report:?}");
        assert_eq!(report.rows_young, 0, "{report:?}");
        assert_eq!(report.files_removed, 0, "{report:?}");
        assert_eq!(report.files_young, 0, "{report:?}");
    }
}
