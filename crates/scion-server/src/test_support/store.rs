//! `store.rs`'s test hooks, as its own child module: `store.rs`
//! declares this file with `#[path]`, so everything here keeps the
//! privacy of its module unchanged, and the file sits under
//! `src/test_support/` for the coverage exclusion.

#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use super::{BlobId, Blobs, Result, Store};

/// A step the tests run at a counted batch boundary of the import, with
/// the count of batches done so far.
#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
pub type AfterBatch = Box<dyn FnMut(usize) + Send>;

/// The import's test hooks, the same state on both backends: the count of
/// busy retries, so a test proves the retry ran instead of only that the
/// import survived, and the step the tests run after each batch commits,
/// so a test that must act at a counted batch boundary runs there,
/// instead of racing the batch's end.
#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default)]
pub struct ImportHooks {
    busy_retries: AtomicU64,
    after_batch: Mutex<Option<AfterBatch>>,
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
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
    /// done so far. The mutex is held across the step: a test step may
    /// block for as long as it likes (the busy tests hold the database for
    /// seconds), but nothing else can set or clear the step meanwhile, and
    /// a step that calls back into `set_after_batch` deadlocks.
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

/// The write park of the test that races two processes onto one blob
/// ([#268]). With `SCION_TEST_PARK_WRITES` in the environment, a writer
/// whose `keep` found the blob missing parks while the park file of that
/// blob's ID exists next to the data directory. The spawned server picks
/// the variable up at start, so a test parks the writer of a real
/// process; the park file is the release.
#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default)]
pub struct WritePark {
    enabled: bool,
    root: PathBuf,
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
impl WritePark {
    /// Arms the park from the environment, over the data root.
    pub(crate) fn from_env(root: &Path) -> Self {
        Self {
            enabled: std::env::var_os("SCION_TEST_PARK_WRITES").is_some(),
            root: root.to_owned(),
        }
    }

    /// Parks the caller while the park file of `id` exists. The writer
    /// holds its stripe all the while, so no same-blob write of this
    /// process moves either. A marker file says the writer is inside the
    /// park: a stat of the park file can come from the kernel's cache,
    /// so the test cannot see the park in the ops trace. The ceiling
    /// bounds the park of the good code: a park nobody lifts fails the
    /// holder, not the whole store.
    pub fn park(&self, id: &BlobId) {
        if !self.enabled {
            return;
        }
        let hooks = self.root.join("hooks");
        let file = hooks.join(format!("park-{id}"));
        let marker = hooks.join(format!("parked-{id}"));
        let _ = fs::write(&marker, b"");
        // A cold CI import of one commit took about 30 s; the ceiling
        // sits well above it, and the test lifts the park long before.
        let give_up = std::time::Instant::now() + Duration::from_secs(180);
        while file.exists() {
            assert!(
                std::time::Instant::now() < give_up,
                "the park of {id} was never lifted"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let _ = fs::remove_file(&marker);
    }
}

/// One durability step of the blob path, recorded for the order test.
#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurabilityStep {
    /// A temp file for a blob was written.
    TmpWrite(BlobId),
    /// The temp file's bytes were flushed to the drive.
    FileFsync(BlobId),
    /// The temp file took the blob's name.
    Rename(BlobId),
    /// A stored file was flushed to the drive, on the keep path.
    StoredFileFsync(BlobId),
    /// A directory that changed was flushed.
    DirFsync(PathBuf),
    /// A full flush of the drive cache ran for a batch.
    FullFlush,
    /// A batch's `sync` answered its caller.
    Answer,
}

/// The ordered log of [`DurabilityStep`]s behind the test-hooks seam.
#[doc(hidden)]
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default)]
pub struct DurabilityLog {
    steps: std::sync::Mutex<Vec<DurabilityStep>>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl DurabilityLog {
    /// Records one step.
    pub(super) fn record(&self, step: DurabilityStep) {
        self.steps
            .lock()
            .expect("the durability log's lock")
            .push(step);
    }

    /// The steps recorded so far, oldest first.
    ///
    /// # Panics
    ///
    /// Panics if the log's lock is poisoned.
    #[must_use]
    pub fn steps(&self) -> Vec<DurabilityStep> {
        self.steps
            .lock()
            .expect("the durability log's lock")
            .clone()
    }
}

/// The rename of a temp file onto the blob's name, with its step recorded
/// in one call.
#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn persist_logged(
    log: &DurabilityLog,
    id: BlobId,
    tmp: tempfile::NamedTempFile,
    path: &Path,
) -> Result<()> {
    tmp.persist(path).map_err(|e| e.error)?;
    log.record(DurabilityStep::Rename(id));
    Ok(())
}

impl Store {
    /// The durability steps this store's blob path ran, oldest first,
    /// for the order test under the test-hooks seam.
    #[doc(hidden)]
    #[must_use]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn durability_steps(&self) -> Vec<DurabilityStep> {
        self.inner.blobs.log.steps()
    }
    /// Commits made so far.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn commits(&self) -> u64 {
        self.inner.committed.load(Ordering::Acquire)
    }
    /// The commit count a completed flush last covered: every commit
    /// below it is durable.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn flushed_upto(&self) -> u64 {
        self.inner.flushed.load(Ordering::Acquire)
    }
    /// Sets how many of the next commits fail as a busy database. Tests of
    /// the import's busy retry use it: the failing transaction ran and
    /// rolled back, so the retry re-runs it whole.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fail_next_commits(&self, n: u32) {
        self.inner
            .fail_commits
            .store(u64::from(n), Ordering::Relaxed);
    }
    /// Sets how long the write connection waits for a busy database. Tests
    /// use it to make a busy lock fail fast, so a retry is deterministic.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_busy_timeout(&self, timeout: Duration) {
        let _ = self.lock().busy_timeout(timeout);
    }
    /// The import's test hooks and busy-retry count.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn hooks(&self) -> &ImportHooks {
        &self.inner.hooks
    }
    /// Caps the database file at its current size plus `extra` pages, for
    /// the out-of-space tests: the next write that must grow the file
    /// fails with the real disk-full.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn cap_database_pages(&self, extra: i64) {
        let db = self.lock();
        let pages: i64 = db
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap_or(0);
        db.pragma_update(None, "max_page_count", pages + extra)
            .unwrap();
    }
    /// Lifts the page cap again, back to the largest file SQLite allows.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn release_database_pages(&self) {
        let db = self.lock();
        db.pragma_update(None, "max_page_count", 0x7FFF_FFFFi64)
            .unwrap();
    }
    /// Write transactions run so far.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn txes(&self) -> u64 {
        self.inner.txes.load(Ordering::Relaxed)
    }
}

impl Blobs {
    /// How many fsyncs passed the gate so far: a test reads it to see
    /// that a kept file was fsynced, file and directory both.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fsync_count(&self) -> u64 {
        self.fsync_count.load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Arms the hook that fails an fsync of this backend: it lets the
    /// next `n` fsyncs through, fails the one after with EIO, and lets
    /// the rest through. Zero arms it for the very next fsync. The count
    /// runs over the file fsyncs and the directory fsyncs both, in the
    /// order the writes make them.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fail_fsyncs_after(&self, n: usize) {
        self.fail_fsyncs
            .store(n, std::sync::atomic::Ordering::SeqCst);
    }
    /// Arms the hook: it lets the next `n` file reads through, fails the
    /// one after with EIO, the disk's answer for a failing device, and
    /// lets the rest through. Zero arms it for the very next read.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fail_reads_after(&self, n: usize) {
        self.fail_reads
            .store(n, std::sync::atomic::Ordering::SeqCst);
    }
    /// Arms the hook: it lets the next `n` file writes through, fails the
    /// one after with the disk's answer, and lets the rest through. Zero
    /// arms it for the very next write. Calling it again rearms it; the
    /// start ([`UNARMED`]) never arms it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fail_writes_after(&self, n: usize) {
        self.fail_writes_after_errno(n, 28);
    }
    /// Arms the write hook with a chosen errno instead of ENOSPC: 5 is
    /// EIO, 30 is EROFS (a read-only mount). The same one-shot shape as
    /// [`Blobs::fail_writes_after`].
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fail_writes_after_errno(&self, n: usize, errno: u8) {
        self.fail_writes_errno
            .store(errno, std::sync::atomic::Ordering::SeqCst);
        self.fail_writes
            .store(n, std::sync::atomic::Ordering::SeqCst);
    }
    /// Calls to [`Blobs::verify`] so far.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn verifies(&self) -> u64 {
        self.verifies.load(std::sync::atomic::Ordering::Relaxed)
    }
}
