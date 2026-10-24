//! A migration that runs leaves its story in the log: one line at the
//! start (`from_version`, `to_version`), one line per step
//! (`from_version`, `to_version`, `elapsed_ms`), one line at the end,
//! and nothing when nothing migrates. An operator watching an upgrade reads these; this
//! test parses the fields the way the guide's §4 promises them, on both
//! backends.

use scion_server::Backend;
use scion_server::test_support::backend::schema_version;
use std::fs;
use std::sync::{Arc, Mutex};

/// The version this build's SQLite store climbs to, from the build, not
/// from a literal the next migration turns stale.
const SQLITE_TOP: u32 = schema_version::SQLITE;

/// The version this build's Postgres schema climbs to.
const POSTGRES_TOP: u32 = schema_version::POSTGRES;

/// The log of the test binary, into this one buffer. One global
/// subscriber, set once: a per-thread default cannot serve a test whose
/// events another test's first open has already poisoned the callsite
/// cache for (a callsite's interest is computed once, against whatever
/// dispatch first evaluated it — a thread-local default that was not
/// there at that moment means the events stay off for everyone).
static LOG: std::sync::LazyLock<Captured> = std::sync::LazyLock::new(Captured::default);

impl Default for Captured {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }
}

#[derive(Clone)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Installs the one global subscriber; the first test that runs wins
/// the `Once`, and every test reads the same buffer from its own mark.
fn init_log() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::level_filters::LevelFilter::TRACE)
            .with_writer(Captured::clone(&*LOG))
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
    });
}

/// The buffer's length at a moment: a test marks before it acts, and
/// reads only what came after.
fn mark() -> usize {
    LOG.0.lock().unwrap().len()
}

fn since(mark: usize) -> String {
    String::from_utf8(LOG.0.lock().unwrap()[mark..].to_vec()).unwrap()
}

/// The lines that name a message, with the values of `field` in order.
fn fields(log: &str, message: &str, field: &str) -> Vec<u32> {
    log.lines()
        .filter(|line| line.contains(message))
        .filter_map(|line| {
            let at = line.find(field)? + field.len();
            let rest = &line[at..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().ok()
        })
        .collect()
}

/// A SQLite data directory at schema version 7, as a build of that
/// version left it: migrations 1 to 7 applied, `user_version` at 7.
fn schema_7_data(data: &std::path::Path) {
    fs::create_dir_all(data).unwrap();
    let c = rusqlite::Connection::open(data.join("scion.db")).unwrap();
    c.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
    for sql in [
        include_str!("../migrations/0001_init.sql"),
        include_str!("../migrations/0002_land_retention.sql"),
        include_str!("../migrations/0003_derived_values.sql"),
        include_str!("../migrations/0004_repo_blobs.sql"),
        include_str!("../migrations/0005_path_history.sql"),
        include_str!("../migrations/0006_land_created_at.sql"),
        include_str!("../migrations/0007_tree_sizes.sql"),
    ] {
        c.execute_batch(sql).unwrap();
    }
    c.execute_batch("PRAGMA user_version = 7;").unwrap();
}

#[test]
fn an_open_logs_what_migrated_and_nothing_when_nothing_did() {
    // The quiet phase asserts the SQLite store's own log; the Postgres
    // parity job covers its climbing story in the fresh-database test.
    if Backend::test_is_postgres() {
        eprintln!("skip on postgres: the quiet phase is the SQLite log");
        return;
    }
    init_log();
    // A fresh store's first open says it created the schema, and no
    // migration line: nothing was there for a migration to move, and a
    // line that says it migrated would be false.
    let dir = tempfile::tempdir().unwrap();
    let fresh_at = mark();
    {
        let store = Backend::open_test(dir.path()).unwrap();
        drop(store);
    }
    let fresh = since(fresh_at);
    assert_eq!(
        fields(&fresh, "the metadata schema was created", "to_version="),
        vec![SQLITE_TOP],
        "a fresh store says what it created; the log was:\n{fresh}"
    );
    assert!(
        !fresh.contains("migrat"),
        "a fresh store's story is a creation, not a migration: {fresh}"
    );
    assert!(
        !fresh.contains("removed temp folders"),
        "a fresh store's tmp held nothing to sweep: {fresh}"
    );

    // An open at the current schema says nothing: read the log around a
    // second open of the same directory.
    let quiet_at = mark();
    let store = Backend::open_test(dir.path()).unwrap();
    drop(store);
    let quiet = since(quiet_at);
    assert!(
        !quiet.contains("migrat"),
        "an open at the current schema says nothing: {quiet}"
    );

    // A leftover temp folder is still reported, for what it is: the
    // folder's process is gone - a crash's or a one-shot run's. The
    // sweep cannot tell which, and the line does not claim to.
    let tmp = dir.path().join("tmp");
    std::fs::create_dir(tmp.join("dead-folder")).unwrap();
    let leftover_at = mark();
    let store = Backend::open_test(dir.path()).unwrap();
    drop(store);
    let leftover = since(leftover_at);
    assert!(
        leftover.contains("removed temp folders whose process is gone"),
        "a folder whose process is gone is reported: {leftover}"
    );
    assert!(
        !tmp.join("dead-folder").exists(),
        "the leftover's folder is gone"
    );

    // An open of an older store tells the whole story: from version,
    // one line per step, and the end.
    let dir = tempfile::tempdir().unwrap();
    schema_7_data(&dir.path().join("data"));
    let at = mark();
    let store = Backend::open_test(&dir.path().join("data")).unwrap();
    drop(store);
    let log = since(at);

    // The start line names the whole climb.
    assert_eq!(
        fields(&log, "migrating the metadata schema", "from_version="),
        vec![7],
        "one start line, from version 7; the log was:\n{log}"
    );
    assert_eq!(
        fields(&log, "migrating the metadata schema", "to_version="),
        vec![SQLITE_TOP],
        "the start line climbs to this build's schema"
    );
    // One line per step, in order, each with a time.
    assert_eq!(
        fields(
            &log,
            "the metadata schema migrated one step",
            "from_version="
        ),
        (7..SQLITE_TOP).collect::<Vec<_>>(),
        "the steps climb the fixture's 7 to this build's schema; the log was:\n{log}"
    );
    assert_eq!(
        fields(&log, "the metadata schema migrated one step", "to_version="),
        (8..=SQLITE_TOP).collect::<Vec<_>>()
    );
    assert_eq!(
        fields(&log, "the metadata schema migrated one step", "elapsed_ms=").len(),
        (7..SQLITE_TOP).len(),
        "every step line carries its time"
    );
    // The end line closes the story.
    assert_eq!(
        fields(&log, "the metadata migration is done", "from_version="),
        vec![7]
    );
    assert_eq!(
        fields(&log, "the metadata migration is done", "to_version="),
        vec![SQLITE_TOP]
    );
}

/// The Postgres leg: a fresh database says it created the schema - the
/// same honest one-liner as SQLite's - and nothing about migrating.
/// Skips itself without the test backend's environment, as the parity
/// job sets it.
#[test]
fn a_postgres_open_of_a_fresh_database_logs_its_creation() {
    init_log();
    if std::env::var("SCION_TEST_BACKEND").as_deref() != Ok("postgres") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let at = mark();
    let store = Backend::open_test(dir.path()).unwrap();
    drop(store);
    let log = since(at);

    assert_eq!(
        fields(&log, "the metadata schema was created", "to_version="),
        vec![POSTGRES_TOP],
        "a fresh database says what it created; the log was:\n{log}"
    );
    assert!(
        !log.contains("migrat"),
        "a fresh database's story is a creation, not a migration: {log}"
    );
}
