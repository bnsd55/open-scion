//! The audit log: an append-only record of who did what, with no
//! secrets in it.
//!
//! Every line is JSON (`v` names the schema version), written by one
//! thread that owns the file. Callers hand lines to a bounded channel;
//! the writer drains the queue, writes and fsyncs once per drain, and
//! wakes each caller when its own lines are durable: evidence first,
//! so a state change whose audit line is not durable does not happen.
//!
//! The file opens with the same discipline as the key files: the
//! parent is checked, the open carries `O_NOFOLLOW`, and a file that
//! is not a plain 0600 file of the user or root is refused.
//!
//! What is never in a line: token bytes, token digests, and blob or
//! tree digests (a digest is a dictionary key to contents).

use std::collections::HashMap;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jiff::Timestamp;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch};

/// The schema version of every line.
const VERSION: u64 = 1;
/// The bound of the line queue. A caller that fills it waits, which
/// is the backpressure: the audit never drops a line to keep up.
const QUEUE: usize = 1024;
/// How often the refusal meter flushes its counts into one line per
/// peer.
const METER_FLUSH: Duration = Duration::from_secs(10);
/// The most peers the refusal meter tracks at once. Past the cap, a
/// new peer folds into the shared `"*"` bucket, so refused calls from
/// many strangers cannot grow the memory or the line count.
const METER_PEERS_CAP: usize = 1024;

/// One audit line: the kind of record, what happened, and the fields
/// that name the objects. Rendered with the shared fields (`v`, `ts`,
/// `event`, `outcome`) and merged with the caller's.
#[allow(unreachable_pub)]
#[allow(clippy::struct_field_names)]
pub struct Event {
    event: &'static str,
    outcome: &'static str,
    fields: Value,
}

impl Event {
    /// A line of kind `event`, with outcome and the event's own fields.
    #[must_use]
    pub fn new(event: &'static str, outcome: &'static str, fields: Value) -> Self {
        Self {
            event,
            outcome,
            fields,
        }
    }

    fn render(&self) -> String {
        let mut line = json!({
            "v": VERSION,
            "ts": Timestamp::now().to_string(),
            "event": self.event,
            "outcome": self.outcome,
        });
        if let (Value::Object(line), Value::Object(extra)) = (&mut line, &self.fields) {
            for (k, v) in extra {
                line.insert(k.clone(), v.clone());
            }
        }
        let mut rendered = line.to_string();
        rendered.push('\n');
        rendered
    }
}

/// The audit log of one server. Cloned handles share the one writer.
/// The audit log of one server. Cloned handles share the one writer.
#[allow(unreachable_pub)]
#[derive(Clone, Debug)]
pub struct Audit {
    tx: Option<mpsc::Sender<Msg>>,
    ready: watch::Receiver<bool>,
    /// The test hook: how many of the next appends fail, as an EIO or
    /// an ENOSPC would, without touching the file. Behind
    /// `test_support`, on the handle, so parallel tests cannot fail
    /// each other's appends through a shared counter.
    /// The test hook: how many of the next appends fail, as an EIO or
    /// ENOSPC would, without touching the file. Read only by the
    /// test-hooks seam.
    #[cfg_attr(not(feature = "test-hooks"), allow(dead_code))]
    faults: Arc<AtomicU64>,
    /// How many directory fsyncs the opens of this audit did: the
    /// audit file's own fsync makes its bytes durable, not its entry
    /// in the parent, so the open syncs the parent. Counted for the
    /// test that proves the create path does it.
    #[cfg_attr(not(test), allow(dead_code))]
    dir_fsyncs: Arc<AtomicU64>,
    /// The readiness sender of the off handle, held so its watch never
    /// closes: a closed watch would flip readiness at once.
    _ready_tx: Option<watch::Sender<bool>>,
}

struct Msg {
    line: String,
    done: oneshot::Sender<io::Result<()>>,
}

impl Audit {
    /// Opens (or creates) the audit file at `path` and starts its
    /// writer. The file is mode 0600, opened for append, and never a
    /// symlink; the parent directory must be private.
    ///
    /// # Errors
    ///
    /// The parent is not a private directory, or the file exists with
    /// the wrong kind, owner, or mode, or the open fails.
    pub fn open(path: &Path) -> io::Result<Audit> {
        let dir_fsyncs = Arc::new(AtomicU64::new(0));
        let file = checked_open(path, Some(&dir_fsyncs))?;
        let (tx, rx) = mpsc::channel(QUEUE);
        let (ready_tx, ready_rx) = watch::channel(true);
        let path = path.to_path_buf();
        let faults = Arc::new(AtomicU64::new(0));
        std::thread::Builder::new().name("audit".into()).spawn({
            let faults = Arc::clone(&faults);
            move || writer(rx, file, path, ready_tx, faults)
        })?;
        Ok(Self {
            tx: Some(tx),
            ready: ready_rx,
            faults,
            dir_fsyncs,
            _ready_tx: None,
        })
    }

    /// The directory fsyncs this audit's opens did. Only the
    /// in-module tests read it.
    #[cfg(test)]
    fn dir_syncs(&self) -> u64 {
        self.dir_fsyncs.load(Ordering::Relaxed)
    }

    /// A handle for tests that serve without an audit file: every
    /// record succeeds and nothing is written.
    #[cfg(feature = "test-hooks")]
    #[allow(unreachable_pub)]
    #[must_use]
    pub fn off() -> Audit {
        let (ready_tx, ready) = watch::channel(true);
        Self {
            tx: None,
            ready,
            faults: Arc::new(AtomicU64::new(0)),
            dir_fsyncs: Arc::new(AtomicU64::new(0)),
            _ready_tx: Some(ready_tx),
        }
    }

    /// Makes the next `n` appends of THIS audit fail, as an EIO or an
    /// ENOSPC would, without touching the file. Behind the test-hooks
    /// feature, and a method on the handle, so parallel tests fail
    /// only their own audit.
    ///
    /// # Errors
    ///
    /// Never, but a fallible signature keeps the seam uniform.
    #[cfg(feature = "test-hooks")]
    #[allow(unreachable_pub)]
    pub fn fail_next_appends(&self, n: u32) -> io::Result<()> {
        self.faults.fetch_add(u64::from(n), Ordering::Relaxed);
        Ok(())
    }

    /// Records one line, and returns once it is durable on the disk.
    /// With the off handle, the line is dropped and the call succeeds.
    ///
    /// # Errors
    ///
    /// The line was not made durable: the append failed, or the writer
    /// is gone. A caller that guards a state change must refuse it.
    pub async fn record(&self, event: Event) -> io::Result<()> {
        let Some(tx) = &self.tx else {
            return Ok(());
        };
        let (done, ack) = oneshot::channel();
        tx.send(Msg {
            line: event.render(),
            done,
        })
        .await
        .map_err(|_| queue_closed())?;
        ack.await.map_err(|_| queue_closed())?
    }

    /// The readiness of the audit: false once a line was lost. It
    /// latches: a recovered drive does not restore trust in a log that
    /// has a hole, so readiness stays off until a restart.
    #[must_use]
    pub fn ready(&self) -> watch::Receiver<bool> {
        self.ready.clone()
    }
}

fn queue_closed() -> io::Error {
    io::Error::other("the audit writer is gone")
}

/// Opens the audit file with the checks of a secret file, for append.
fn checked_open(
    path: &Path,
    dir_syncs: Option<&AtomicU64>,
) -> io::Result<std::fs::File> {
    let me = rustix::process::getuid().as_raw();
    // The walk resolves and checks every directory above the file, so
    // no other user can swap it on the way; the open's `O_NOFOLLOW`
    // then refuses a symlink at the file itself.
    let resolved = scion_proto::runtime::auth::check_ancestors(path, me)?;
    let file = rustix::fs::open(
        &resolved,
        rustix::fs::OFlags::RDWR
            | rustix::fs::OFlags::APPEND
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )
    .map_err(io::Error::from)?;
    let stat = rustix::fs::fstat(&file).map_err(io::Error::from)?;
    // Plain mode bits, no platform raw-mode conversions: a regular
    // file of the user or root, mode 0600.
    let is_file = stat.st_mode & 0o170_000 == 0o100_000;
    let owned = stat.st_uid == me || stat.st_uid == 0;
    let mode = stat.st_mode & 0o777;
    if !is_file || !owned || mode != 0o600 {
        drop(file);
        return Err(io::Error::other(
            "the audit file must be a plain 0600 file of the user or root",
        ));
    }
    // The file's fsync makes its bytes durable, not its entry in the
    // parent: a crash right after the first start would lose the file
    // itself. Sync the directory once per open.
    if let Some(parent) = resolved.parent() {
        let dir = rustix::fs::open(
            parent,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(io::Error::from)?;
        let dir = std::fs::File::from(dir);
        dir.sync_data()?;
        if let Some(counted) = dir_syncs {
            counted.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(file.into())
}

/// The writer thread: one owner of the file. It takes its file, path,
/// readiness and faults by value because it is the only owner there is.
#[allow(clippy::needless_pass_by_value)]
fn writer(
    #[allow(clippy::needless_pass_by_value)] // the thread owns them
    mut rx: mpsc::Receiver<Msg>,
    mut file: std::fs::File,
    path: PathBuf,
    ready: watch::Sender<bool>,
    // The test hook: how many of the next appends fail, as an EIO or
    // ENOSPC would, without touching the file. Read only by the
    // test-hooks seam.
    #[cfg_attr(not(feature = "test-hooks"), allow(dead_code))] faults: Arc<AtomicU64>,
) {
    while let Some(first) = rx.blocking_recv() {
        let mut batch = Vec::with_capacity(16);
        batch.push(first);
        while batch.len() < QUEUE {
            match rx.try_recv() {
                Ok(msg) => batch.push(msg),
                Err(_) => break,
            }
        }
        match append(&mut file, &batch, &faults) {
            Ok(()) => {
                for msg in batch {
                    let _ = msg.done.send(Ok(()));
                }
            }
            Err(why) => {
                tracing::error!(
                    path = %path.display(),
                    reason = %why,
                    "audit lines are not durable: refusing the calls they guard"
                );
                let _ = ready.send_if_modified(|ready| {
                    let changed = *ready;
                    *ready = false;
                    changed
                });
                for msg in batch {
                    let _ = msg.done.send(Err(io::Error::other(format!(
                        "the audit line was not made durable: {why}"
                    ))));
                }
            }
        }
    }
}

fn append(file: &mut std::fs::File, batch: &[Msg], faults: &AtomicU64) -> io::Result<()> {
    if faults.load(Ordering::Relaxed) > 0 {
        faults.fetch_sub(1, Ordering::Relaxed);
        return Err(io::Error::other(
            "the audit append was made to fail by the test hook",
        ));
    }
    let mut bytes = Vec::new();
    for msg in batch {
        bytes.extend_from_slice(msg.line.as_bytes());
    }
    let mut written = 0;
    while written < bytes.len() {
        written += file.write(&bytes[written..])?;
    }
    file.sync_data()
}

/// Counts refused authentication per peer, and flushes one line per
/// peer per interval: a stranger who spams bad tokens makes counted
/// lines, not one line per call, and past the peer cap all strangers
/// share one bucket.
#[derive(Default)]
pub(crate) struct Meter {
    counts: Mutex<HashMap<String, u64>>,
}

impl Meter {
    pub(crate) fn count(&self, peer: &str) {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = if counts.contains_key(peer) || counts.len() < METER_PEERS_CAP {
            peer.to_string()
        } else {
            "*".to_string()
        };
        *counts.entry(key).or_insert(0) += 1;
    }

    async fn flush(&self, audit: &Audit) {
        let taken: HashMap<String, u64> = std::mem::take(
            &mut *self
                .counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for (peer, count) in taken {
            let line = Event::new("auth", "refused", json!({ "peer": peer, "count": count }));
            if let Err(why) = audit.record(line).await {
                tracing::warn!(reason = %why, "the refused-auth count was not made durable");
            }
        }
    }
}

/// Flushes the meter every [`METER_FLUSH`], and once more at stop.
pub(crate) async fn meter_task(meter: Arc<Meter>, audit: Audit, mut stop: watch::Receiver<()>) {
    let mut ticks = tokio::time::interval(METER_FLUSH);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticks.tick() => meter.flush(&audit).await,
            _ = stop.changed() => {
                meter.flush(&audit).await;
                return;
            }
        }
    }
}

/// The bearer-token check of [`Auth`], counting every refusal into the
/// meter. The principal of an accepted call rides through unchanged.
#[derive(Clone)]
pub(crate) struct AuditedAuth {
    inner: scion_proto::runtime::auth::Auth,
    meter: Arc<Meter>,
}

impl AuditedAuth {
    pub(crate) fn new(inner: scion_proto::runtime::auth::Auth, meter: Arc<Meter>) -> Self {
        Self { inner, meter }
    }

    fn peer_of(request: &tonic::Request<()>) -> String {
        // The connection's handle is the only peer a call still has
        // below the router: the connect info of the request.
        request
            .extensions()
            .get::<scion_proto::runtime::listen::Stalled>()
            .and_then(scion_proto::runtime::listen::Stalled::peer)
            .map(|addr| addr.to_string())
            .unwrap_or_default()
    }
}

impl tonic::service::Interceptor for AuditedAuth {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let peer = Self::peer_of(&request);
        match self.inner.call(request) {
            Err(status) => {
                self.meter.count(&peer);
                Err(status)
            }
            ok => ok,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_lines(path: &Path) -> Vec<Value> {
        let bytes = std::fs::read(path).unwrap();
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[tokio::test]
    async fn a_line_carries_the_schema_version_and_a_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let audit = Audit::open(&path).unwrap();
        audit
            .record(Event::new("admin", "ok", json!({ "what": "start" })))
            .await
            .unwrap();
        let lines = read_lines(&path);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["v"], 1);
        assert_eq!(lines[0]["event"], "admin");
        assert_eq!(lines[0]["outcome"], "ok");
        assert_eq!(lines[0]["what"], "start");
        lines[0]["ts"]
            .as_str()
            .unwrap()
            .parse::<Timestamp>()
            .expect("the timestamp is RFC 3339");
    }

    #[tokio::test]
    async fn a_batch_is_one_durable_append_per_caller() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let audit = Audit::open(&path).unwrap();
        let (a, b) = tokio::join!(
            audit.record(Event::new("write", "ok", json!({}))),
            audit.record(Event::new("ref", "ok", json!({})))
        );
        a.unwrap();
        b.unwrap();
        assert_eq!(read_lines(&path).len(), 2);
    }

    #[tokio::test]
    async fn a_failing_append_refuses_the_caller_and_latches_readiness() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let audit = Audit::open(&path).unwrap();
        audit.fail_next_appends(1).unwrap();
        let why = audit
            .record(Event::new("write", "ok", json!({})))
            .await
            .unwrap_err();
        assert!(why.to_string().contains("test hook"), "{why:?}");
        assert!(!*audit.ready().borrow());
        // A later append may succeed on the file, but readiness stays off.
        audit
            .record(Event::new("write", "ok", json!({})))
            .await
            .unwrap();
        assert!(!*audit.ready().borrow());
    }

    #[tokio::test]
    async fn the_meter_folds_many_refusals_into_one_counted_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let audit = Audit::open(&path).unwrap();
        let meter = Arc::new(Meter::default());
        for _ in 0..500 {
            meter.count("127.0.0.1:1");
        }
        meter.count("127.0.0.1:2");
        meter.flush(&audit).await;
        let lines = read_lines(&path);
        assert_eq!(lines.len(), 2);
        for line in &lines {
            assert_eq!(line["event"], "auth");
            assert_eq!(line["outcome"], "refused");
        }
        // The flush iterates a hash map, so the order of the lines is
        // not the order of the counts.
        let mut counts: Vec<u64> = lines
            .iter()
            .map(|line| line["count"].as_u64().unwrap())
            .collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![1, 500]);
    }

    #[tokio::test]
    async fn strangers_past_the_peer_cap_share_one_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let audit = Audit::open(&path).unwrap();
        let meter = Arc::new(Meter::default());
        for i in 0..(METER_PEERS_CAP + 10) {
            meter.count(&format!("peer-{i}"));
        }
        meter.flush(&audit).await;
        let lines = read_lines(&path);
        assert_eq!(lines.len(), METER_PEERS_CAP + 1);
        let shared: Vec<&Value> = lines.iter().filter(|line| line["peer"] == "*").collect();
        assert_eq!(shared.len(), 1, "one bucket holds every stranger");
        assert_eq!(shared[0]["count"], 10);
    }

    #[test]
    fn every_open_syncs_the_parent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let audit = Audit::open(&path).unwrap();
        assert_eq!(audit.dir_syncs(), 1, "the create syncs the parent");
        drop(audit);
        let audit = Audit::open(&path).unwrap();
        assert_eq!(audit.dir_syncs(), 1, "a reopen syncs the parent again");
    }

    #[test]
    fn a_symlink_at_the_audit_path_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("real.log");
        std::fs::write(&target, b"").unwrap();
        let link = dir.path().join("audit.log");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let why = Audit::open(&link).unwrap_err();
        assert!(
            why.to_string().to_lowercase().contains("link")
                || why.raw_os_error() == Some(rustix::io::Errno::LOOP.raw_os_error()),
            "{why:?}"
        );
    }

    #[test]
    fn a_preexisting_file_with_the_wrong_mode_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        std::fs::write(&path, b"").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let why = Audit::open(&path).unwrap_err();
        assert!(why.to_string().contains("0600"), "{why:?}");
    }

    #[test]
    fn an_audit_path_in_a_shared_parent_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("shared");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o777)).unwrap();
        let why = Audit::open(&parent.join("audit.log")).unwrap_err();
        assert!(why.to_string().contains("written by others"), "{why:?}");
    }
}
