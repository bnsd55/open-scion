#![forbid(unsafe_code)]
//! The `scion-server` binary: `import`, `serve` and `derive`.

use std::io::IsTerminal;
use std::io::Write as _;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use scion_proto::runtime::listen::{self, Limits};
use scion_proto::runtime::metrics::{loopback_or_allow, serve as metrics_serve};
use scion_proto::runtime::tls::ServerTls;
use scion_server::{ImportOptions, import_git};

use scion_server::Metrics;
use scion_server::Quotas;
use scion_server::{Backend, Error, ServeOptions, archive, fsck, gc};
use tracing_subscriber::EnvFilter;

/// The scion server: import, serve and derive.
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The data directory. Holds the SQLite database and the objects.
    #[arg(long, env = "SCION_DATA", default_value = "scion-data")]
    data: PathBuf,
    /// Serve metadata from Postgres at this URL instead of the SQLite
    /// database of the data directory. The URL holds the credentials, so it
    /// is never logged. Prefer `--postgres-url-file` or `SCION_POSTGRES_URL`:
    /// a URL on the command line is readable by every local account (`ps`).
    #[arg(long, value_name = "URL")]
    postgres_url: Option<String>,
    /// File with the Postgres URL, read like the access file: the owner
    /// alone may read it, and a symlink is refused. Give `--postgres-url`
    /// or `--postgres-url-file`, not both.
    #[arg(
        long,
        conflicts_with = "postgres_url",
        env = "SCION_POSTGRES_URL_FILE",
        value_name = "FILE"
    )]
    postgres_url_file: Option<PathBuf>,
    /// Verify the certificate of the Postgres server with this CA (PEM).
    /// Required when `--postgres-url` names a host that is not local.
    #[arg(long, requires = "postgres_url", value_name = "FILE")]
    postgres_ca: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

/// The flags of `serve`, as clap parses them.
#[derive(clap::Args)]
struct ServeArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:7878")]
    listen: SocketAddr,
    /// Bearer token that clients must send. Its principal, `default`,
    /// may read and write every repo. Give `--token`, `--token-file` or
    /// `--access`, not two. With this, the per-principal quotas are off
    /// unless a flag sets one: every client is the same principal, so a
    /// cap would be a global one.
    #[arg(long, value_name = "TOKEN")]
    token: Option<String>,
    /// File with the bearer token of `--token`, read like the access
    /// file: the owner alone may read it, and a symlink is refused.
    #[arg(long, conflicts_with_all = ["token", "access"], value_name = "FILE")]
    token_file: Option<PathBuf>,
    /// Access file: tokens with principals, and which repos and paths
    /// each principal may read and write. Give `--token` or `--access`,
    /// not both.
    #[arg(long, value_name = "FILE")]
    access: Option<PathBuf>,
    /// Most requests one principal may hold at once. With `--access`
    /// the default is 64; with `--token` the check is off.
    #[arg(long)]
    max_principal_requests: Option<NonZeroUsize>,
    /// Most upload bytes one principal may hold in flight. With
    /// `--access` the default is 536870912 (512 MiB); with `--token`
    /// the check is off.
    #[arg(long, value_name = "BYTES")]
    max_principal_upload_bytes: Option<NonZeroUsize>,
    /// Most pending lands one principal may hold at once. With
    /// `--access` the default is 64; with `--token` the check is off.
    #[arg(long)]
    max_principal_pending_lands: Option<NonZeroUsize>,
    /// A land that has been pending for longer than this many seconds is
    /// rejected by the sweep, so a restart cannot leave one pending
    /// forever. The default is 86400 (24 hours).
    #[arg(
        long,
        default_value_t = 24 * 60 * 60,
        value_parser = clap::value_parser!(i64).range(1..),
        allow_negative_numbers = true
    )]
    land_stale_secs: i64,
    /// Allow a non-loopback address. Without TLS, traffic, including the
    /// token, is not encrypted, so use this only on a trusted network.
    /// With `--tls-cert` and `--tls-key`, a non-loopback address is
    /// allowed without this flag.
    #[arg(long)]
    insecure_listen: bool,
    /// Serve TLS with this certificate (PEM), together with `--tls-key`.
    /// Then a non-loopback address needs no `--insecure-listen`.
    #[arg(long, requires = "tls_key", value_name = "FILE")]
    tls_cert: Option<PathBuf>,
    /// The key of `--tls-cert` (PEM).
    #[arg(long, requires = "tls_cert", value_name = "FILE")]
    tls_key: Option<PathBuf>,
    /// Require a client certificate (mTLS) that this CA (PEM) signed.
    #[arg(long, requires = "tls_cert", value_name = "FILE")]
    tls_client_ca: Option<PathBuf>,
    /// Most connections served at once. More clients wait until one closes.
    #[arg(long, default_value_t = listen::DEFAULT_CONNECTIONS)]
    max_connections: NonZeroUsize,
    /// Most calls (HTTP/2 streams) that one connection may have open at once.
    #[arg(long, default_value_t = listen::DEFAULT_STREAMS)]
    max_streams: NonZeroU32,
    /// File with the Ed25519 seed (64 hex characters) that signs the
    /// tickets of `GetRootTree` replies, from `ticket-key`. Without it,
    /// no object is readable: a read needs a ticket.
    #[arg(long, value_name = "FILE")]
    ticket_key: Option<PathBuf>,
    /// File with the previous ticket key (64 hex characters of its
    /// public key), whose tickets stay valid during a rotation.
    #[arg(long, value_name = "FILE", requires = "ticket_key")]
    ticket_key_old: Option<PathBuf>,
    /// Most bytes that the stalled replies of one connection may hold. A
    /// connection over it is closed with them.
    #[arg(long, default_value_t = listen::DEFAULT_STALLED_BYTES)]
    max_stalled_bytes: NonZeroU64,
    /// Serve Prometheus metrics over plain HTTP on this address. Off by
    /// default. Follows the listen rule: a non-loopback address needs
    /// `--insecure-listen`, because the metrics travel unencrypted.
    #[arg(long, value_name = "ADDR")]
    metrics_listen: Option<SocketAddr>,
    /// Where the audit log goes. The default is `audit.log` in the data
    /// directory. Every server records one: there is no way to turn the
    /// audit off.
    #[arg(long, value_name = "FILE")]
    audit_file: Option<PathBuf>,
    /// How long the in-flight calls have to finish once the first signal
    /// starts the drain. Past it the calls are cut, the log names the
    /// count, and the process exits non-zero, so a supervisor restarts it.
    /// A second signal always cuts at once.
    #[arg(long, default_value_t = listen::DEFAULT_DRAIN_TIMEOUT.as_secs())]
    drain_timeout_secs: u64,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)] // SHORTCUT: `Serve` holds its flags by value, 309 bytes against `Import`'s 109: 20 bytes over the lint. Box the TLS paths if a variant grows further.
enum Command {
    /// Serve the gRPC API.
    Serve(ServeArgs),
    /// Print the BLAKE3 digest of a token, as a `token` line of an access
    /// file wants it. The file holds digests, never tokens.
    HashToken {
        /// The token to digest. Give `--token` (deprecated: it sits in
        /// the process list), `--token-file`, `SCION_TOKEN`, or the
        /// token on stdin.
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
        /// File with the token to digest, read like the access file.
        #[arg(long, conflicts_with = "token", value_name = "FILE")]
        token_file: Option<PathBuf>,
    },
    /// Generate an Ed25519 key for the tickets of object reads: writes the
    /// secret seed to `--out`, and prints the public key as 64 hex
    /// characters, for `--ticket-key-old` and the tiers to verify with.
    TicketKey {
        /// File to write the secret seed to, as 64 hex characters.
        #[arg(long, value_name = "FILE")]
        out: PathBuf,
        /// Where the audit log goes. The default is `audit.log` in the
        /// data directory: the same file the server appends to, so one
        /// file holds the whole history of a data directory.
        #[arg(long, value_name = "FILE")]
        audit_file: Option<PathBuf>,
    },
    /// Import a local git repository, one way. Safe to repeat. It can run
    /// while the server runs.
    Import {
        /// Scion repository to import into. Created if missing.
        #[arg(long, default_value = "main")]
        repo: String,
        /// Git revision to import.
        #[arg(long, default_value = "HEAD")]
        rev: String,
        /// Scion ref to point at the imported tip.
        #[arg(long = "ref", default_value = "main")]
        target_ref: String,
        /// How many ordinary commits to check against git, besides every root
        /// and merge.
        #[arg(long, default_value_t = 100)]
        verify: usize,
        /// Path to the git repository.
        git_dir: PathBuf,
        /// Where the audit log goes. The default is `audit.log` in the
        /// data directory: the same file the server appends to, so one
        /// file holds the whole history of a data directory.
        #[arg(long, value_name = "FILE")]
        audit_file: Option<PathBuf>,
    },
    /// Check a data directory and report the damage it finds. Read-only: it
    /// changes nothing, and it can run while the server runs. It reads in
    /// short batches, and each check looks at the state at the moment it
    /// looks, so a clean report means "no damage seen", not a snapshot.
    /// One line per problem: kind, ID, path. Exit 0 when it saw no damage,
    /// 1 when it did, 2 when it could not run.
    Fsck,
    /// Export a whole repository, or the whole store, to a versioned
    /// logical archive: a directory of files that `verify` checks and
    /// `restore` replays into an empty store. The format is
    /// docs/archive-format.md. The metadata sections are one read
    /// snapshot, so it is safe beside a live server.
    Export {
        /// The name of one repository to export.
        #[arg(long)]
        repo: Option<String>,
        /// Export every repository of the store.
        #[arg(long)]
        all: bool,
        /// The archive directory to write. It must not exist.
        #[arg(long)]
        out: PathBuf,
    },
    /// Check an archive against its format, without the store's code: it
    /// re-hashes every blob file and every changeset body, and checks
    /// that every ref, parent, and land input is backed. One line per
    /// damage. Exit 0 when it saw none, 1 when it did, 2 when it could
    /// not run.
    Verify {
        /// The archive directory to check.
        #[arg(long)]
        archive: PathBuf,
    },
    /// Replay an archive into an EMPTY data directory: the blob files go
    /// through the store's own write path, so every object ID carries
    /// over; the changesets replay in generation order; the refs go back
    /// with the versions the export read; the pending lands are recorded
    /// again, so a retry with the same request key resumes.
    Restore {
        /// The archive directory to replay.
        #[arg(long)]
        archive: PathBuf,
        /// Where the audit log goes. The default is `audit.log` in the
        /// new data directory: the restored store's own audit file, so
        /// the restore's lines open its history.
        #[arg(long, value_name = "FILE")]
        audit_file: Option<PathBuf>,
    },
    /// Collect the blob files and the rows that nothing names any more: a
    /// row whose blob no stored changeset of its repo puts, and a file that
    /// no repo's row names, both older than the grace period. Safe beside a
    /// live server; safe to repeat. Rows go before files, so a crash leaves
    /// orphan files only, which the next run removes.
    Gc {
        /// How old a blob file must be before the collector may remove it:
        /// an upload that a client is about to name is younger than this.
        #[arg(long, default_value_t = gc::DEFAULT_GRACE_SECS)]
        grace_secs: u64,
        /// Where the audit log goes. The default is `audit.log` in the
        /// data directory: the same file the server appends to, so one
        /// file holds the whole history of a data directory.
        #[arg(long, value_name = "FILE")]
        audit_file: Option<PathBuf>,
        /// Report what a real run would collect, and remove nothing.
        #[arg(long)]
        dry_run: bool,
        /// Write this run's report as a Prometheus text file, in the
        /// format of the node exporter's textfile collector, so the
        /// machine's scrape can carry what the collector did: the
        /// collector runs as its own process, so the server's /metrics
        /// cannot show it. Written only when the run succeeds, atomically
        /// (temp file, fsync, rename), so a reader never sees half a file
        /// and a failed run leaves the previous file, whose timestamp
        /// then goes stale.
        #[arg(long, value_name = "PATH")]
        metrics_file: Option<PathBuf>,
    },
    /// Derive one kind of derived data for the history of a ref, in bounded
    /// steps, and remove the rows of its older versions. Safe to repeat.
    Derive {
        /// The kind to derive.
        #[arg(long)]
        kind: KindName,
        /// Scion repository.
        #[arg(long, default_value = "main")]
        repo: String,
        /// Ref whose history is derived: its first-parent history for
        /// `root_tree` and `path_history`, every ancestor for
        /// `commit_graph`.
        #[arg(long = "ref", default_value = "main")]
        target_ref: String,
        /// Where the audit log goes. The default is `audit.log` in the
        /// data directory: the same file the server appends to, so one
        /// file holds the whole history of a data directory.
        #[arg(long, value_name = "FILE")]
        audit_file: Option<PathBuf>,
    },
}

/// The kinds of derived data that `derive` can fill.
#[derive(Clone, Copy, ValueEnum)]
enum KindName {
    /// The root tree of each changeset.
    #[value(name = "root_tree")]
    RootTree,
    /// The commit-graph index of each changeset.
    #[value(name = "commit_graph")]
    CommitGraph,
    /// The path history of each changeset.
    #[value(name = "path_history")]
    PathHistory,
}

/// Runs `export` and prints the manifest's counts. Exits 0, or 2 when
/// the export could not run.
fn archive_export(store: &Backend, data: &Path, repo: Option<&str>, all: bool, out: &Path) -> ! {
    match archive::export(store, data, repo, all, out) {
        Ok(manifest) => {
            let counts = &manifest["counts"];
            println!(
                "export: {} repos, {} changesets, {} refs, {} lands, {} blobs",
                counts["repos"],
                counts["changesets"],
                counts["refs"],
                counts["pending_lands"],
                counts["blobs"],
            );
            std::process::exit(0)
        }
        Err(e) => {
            eprintln!("scion-server export: {e}");
            std::process::exit(2);
        }
    }
}

/// Runs `fsck` against `store`, prints one line per problem, and exits:
/// 0 when it saw no damage, 1 when it did, 2 when it could not run.
/// The exits are `std::process::exit`, which skips destructors: safe
/// here, because nothing this function holds needs dropping — the
/// store's lock is released by the kernel at exit, and the report is
/// plain data.
fn fsck_command(store: &Backend) -> anyhow::Result<()> {
    let report = match fsck::run(store, fsck::BATCH, &mut |_| {}) {
        Ok(report) => report,
        Err(e) => {
            eprintln!("scion-server fsck: {e}");
            std::process::exit(2);
        }
    };
    for problem in &report.problems {
        println!("{problem}");
    }
    for (kind, version) in &report.old {
        println!("derived-old {kind} v{version}");
    }
    println!(
        "fsck: {} problems, {} derived rows of unknown versions",
        report.problems.len(),
        report.old.len()
    );
    if report.is_clean() {
        std::process::exit(0);
    }
    std::process::exit(1);
}

/// Opens the audit log a CLI subcommand records its admin lines in:
/// the same file, checks, and writer the server uses, so the CLI and a
/// running server append whole lines to one file. A run that cannot
/// record refuses before it changes anything.
/// D25: a missing data directory is refused, not created. One rule for
/// every subcommand that touches a store; `restore` creates its own
/// target and never sees this.
fn refuse_missing_data_dir(data: &Path) -> anyhow::Result<()> {
    if data.is_dir() {
        return Ok(());
    }
    if data.exists() {
        return Err(anyhow::anyhow!("{} is not a directory", data.display()));
    }
    Err(anyhow::anyhow!(
        "{} does not exist; create it or pass --data",
        data.display()
    ))
}

fn cli_audit(data: &Path, audit_file: Option<&Path>) -> anyhow::Result<scion_server::Audit> {
    let path = audit_file.map_or_else(|| data.join("audit.log"), Path::to_path_buf);
    scion_server::Audit::open(&path)
        .with_context(|| format!("cannot open audit log {}", path.display()))
}

/// The class label of an error, for an admin line's `why`: the class,
/// never the message, which can carry a path.
fn error_class(e: &Error) -> &'static str {
    match e {
        Error::Storage(storage) => match storage.class() {
            scion_server::StorageClass::Busy => "busy",
            scion_server::StorageClass::DiskFull => "disk_full",
            _ => "failed",
        },
        _ => "failed",
    }
}

/// Whether an audit write failed because the disk is out of space: the
/// collector is the remedy for exactly this, so a gc must run anyway.
fn is_disk_full(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
    )
}

/// Writes the intent line a disk-full audit deferred: the run freed the
/// space, so the line goes in now, marked with why it was late. An
/// append that still fails ends the process with the run's own status:
/// the report is already on stdout, and there is nothing left to record
/// it with.
async fn write_deferred_intent(audit: &scion_server::Audit, dry_run: bool, run_ok: bool) {
    let mut what = serde_json::json!({ "what": "gc", "dry_run": dry_run });
    what["deferred"] = serde_json::json!("disk_full");
    if let Err(why) = audit
        .record(scion_server::Event::new("admin", "intent", what))
        .await
    {
        eprintln!("scion-server gc: the run's intent line is still not recorded: {why}");
        std::process::exit(i32::from(!run_ok));
    }
}

/// Checks an archive against its format and prints one line per damage.
/// Exits 2 when it could not run, 1 when it saw damage.
fn verify_command(archive: &Path) -> ! {
    let damages = match archive::verify(archive) {
        Ok(damages) => damages,
        Err(e) => {
            eprintln!("scion-server verify: {e:#}");
            std::process::exit(2);
        }
    };
    for line in &damages {
        println!("{line}");
    }
    println!("verify: {} damages", damages.len());
    std::process::exit(i32::from(!damages.is_empty()));
}

/// Records the line for a schema migration this process's open made:
/// one `admin`/`ok` line with the versions it moved. The migration ran
/// inside the store's open, before the audit could open, so the record
/// goes in as soon as the audit exists. A fresh open creates the schema
/// and reports no migration; an open that migrates nothing writes
/// nothing.
async fn record_migrated(audit: &scion_server::Audit, store: &Backend) -> anyhow::Result<()> {
    if let Some((from, to)) = store.migrated() {
        audit
            .record(scion_server::Event::new(
                "admin",
                "ok",
                serde_json::json!({ "what": "migrate", "from": from, "to": to }),
            ))
            .await
            .with_context(|| "cannot record the migration in the audit log".to_owned())?;
    }
    Ok(())
}

/// Records the `admin`/`failed` line of a run that did not finish: the
/// error's class, never its message, which can carry a path. A line that
/// itself fails to become durable is logged, not fatal: the run's own
/// error is what the caller reports.
async fn record_admin_failed(audit: &scion_server::Audit, what: &str, why: &str) {
    let fields = serde_json::json!({ "what": what, "why": why });
    if let Err(record_why) = audit
        .record(scion_server::Event::new("admin", "failed", fields))
        .await
    {
        tracing::error!(reason = %record_why, "a failed run's audit line was not made durable");
    }
}

/// Runs `import` and prints what it stored: one admin intent before the
/// import and one result after it, in the data directory's audit log.
async fn import_command(
    audit: scion_server::Audit,
    store: Backend,
    repo: String,
    git_dir: PathBuf,
    opts: ImportOptions,
) -> anyhow::Result<()> {
    let what = serde_json::json!({ "what": "import", "repo": repo });
    audit
        .record(scion_server::Event::new("admin", "intent", what.clone()))
        .await
        .with_context(|| "cannot record the import intent in the audit log".to_owned())?;
    let repo_for_run = repo.clone();
    let run = move || import_git(&store, &repo_for_run, &git_dir, &opts);
    let report = match tokio::task::spawn_blocking(run).await {
        Ok(Ok(report)) => report,
        Ok(Err(e)) => {
            record_admin_failed(&audit, "import", error_class(&e)).await;
            return Err(e.into());
        }
        Err(e) => {
            record_admin_failed(&audit, "import", "failed").await;
            return Err(e.into());
        }
    };
    let fields = serde_json::json!({
        "what": "import",
        "repo": repo,
        "commits": report.commits,
        "commits_imported": report.imported,
        "verified": report.verified,
        "refs_moved": report.refs_moved,
    });
    audit
        .record(scion_server::Event::new("admin", "ok", fields))
        .await
        .with_context(|| "cannot record the import result in the audit log".to_owned())?;
    println!(
        "imported {} of {} commits, verified {}, tip {}",
        report.imported, report.commits, report.verified, report.tip
    );
    Ok(())
}

/// Runs `restore` and prints what it replayed: one admin intent before
/// the replay and one result after it, in the restored store's own audit
/// file. Exits 2 when the restore could not finish. The replay blocks
/// this task, as it always has.
async fn restore_command(
    audit: scion_server::Audit,
    data: &Path,
    archive: &Path,
    postgres_url: Option<&str>,
    postgres_ca: Option<&Path>,
) -> anyhow::Result<()> {
    let what = serde_json::json!({ "what": "restore" });
    audit
        .record(scion_server::Event::new("admin", "intent", what.clone()))
        .await
        .with_context(|| "cannot record the restore intent in the audit log".to_owned())?;
    let report = match archive::restore(archive, data, postgres_url, postgres_ca) {
        Ok(report) => report,
        Err(e) => {
            record_admin_failed(&audit, "restore", error_class(&e)).await;
            eprintln!("scion-server restore: {e:#}");
            std::process::exit(2);
        }
    };
    let fields = serde_json::json!({
        "what": "restore",
        "repos": report.repos.len(),
        "refs": report.refs,
        "lands": report.lands,
    });
    audit
        .record(scion_server::Event::new("admin", "ok", fields))
        .await
        .with_context(|| "cannot record the restore result in the audit log".to_owned())?;
    for name in &report.repos {
        println!("{name}");
    }
    println!(
        "restore: {} repos, {} refs, {} lands",
        report.repos.len(),
        report.refs,
        report.lands
    );
    Ok(())
}

/// Runs `gc` against `store` and prints what it collected. Exits 2 when it
/// could not run.
async fn gc_command(
    audit: scion_server::Audit,
    store: &Backend,
    grace: Duration,
    dry_run: bool,
    metrics_file: Option<&Path>,
) -> anyhow::Result<()> {
    // The run's intent, before anything moves: a crash after it leaves
    // an intent with no result, which tells the auditor a run started.
    let what = serde_json::json!({ "what": "gc", "dry_run": dry_run });
    // The collector is the runbook's cure for the disk it writes its own
    // line to: an intent append that fails because the disk is full must
    // not stop the run. The deferred intent goes in after the run freed
    // the space, marked with why it was late. Every other audit failure
    // still refuses before any change.
    let deferred = match audit
        .record(scion_server::Event::new("admin", "intent", what.clone()))
        .await
    {
        Ok(()) => false,
        Err(e) if is_disk_full(&e) => true,
        Err(e) => {
            return Err(anyhow::Error::new(e))
                .with_context(|| "cannot record the gc intent in the audit log".to_owned());
        }
    };
    let started = std::time::Instant::now();
    let report = match gc::run(store, grace, dry_run, &mut |_| {}) {
        Ok(report) => report,
        Err(e) => {
            // One result line for the intent, with the error's class and
            // never its message: a storage message can carry a path.
            let fields = serde_json::json!({
                "what": "gc",
                "dry_run": dry_run,
                "why": error_class(&e),
            });
            if deferred {
                write_deferred_intent(&audit, dry_run, false).await;
            }
            if let Err(record_why) = audit
                .record(scion_server::Event::new("admin", "failed", fields))
                .await
            {
                tracing::error!(reason = %record_why, "a failed gc's audit line was not made durable");
            }
            eprintln!("scion-server gc: {e}");
            std::process::exit(2);
        }
    };
    let fields = serde_json::json!({
        "what": "gc",
        "dry_run": dry_run,
        "repos": report.repos,
        "rows_pruned": report.rows_pruned,
        "files_removed": report.files_removed,
        "rows_young": report.rows_young,
        "files_young": report.files_young,
    });
    // The run's report goes out first: on a full disk the deferred
    // lines may still fail, and the operator reads the stdout either
    // way. The deferred intent goes in before the result, so the file
    // reads in the order the run happened.
    println!(
        "gc: {} repos, {} rows pruned, {} files removed{}",
        report.repos,
        report.rows_pruned,
        report.files_removed,
        if dry_run { " (dry run)" } else { "" },
    );
    if deferred {
        write_deferred_intent(&audit, dry_run, true).await;
    }
    audit
        .record(scion_server::Event::new("admin", "ok", fields))
        .await
        .with_context(|| "cannot record the gc result in the audit log".to_owned())?;
    if let Some(path) = metrics_file {
        write_gc_metrics_file(path, &report, started.elapsed(), dry_run);
    }
    Ok(())
}

/// Writes the metrics file of `scion-server gc --metrics-file`: the
/// gauges of one finished run, in the Prometheus text format the node
/// exporter's textfile collector reads. A failed run never touches the
/// file, so its timestamp goes stale and an alert on it fires. One file
/// holds one run: each run rewrites the whole file, so its four series
/// always describe that run, never a mix of two runs; the label that
/// says which is on every series.
fn write_gc_metrics_file(path: &Path, report: &gc::Report, took: Duration, dry_run: bool) {
    let dry = if dry_run { "true" } else { "false" };
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let text = format!(
        "# HELP scion_gc_last_success_timestamp_seconds When the collector last finished a run, in seconds since the Unix epoch.\n\
         # TYPE scion_gc_last_success_timestamp_seconds gauge\n\
         scion_gc_last_success_timestamp_seconds{{dry_run=\"{dry}\"}} {unix}\n\
         # HELP scion_gc_duration_seconds How long the last finished run took, in seconds.\n\
         # TYPE scion_gc_duration_seconds gauge\n\
         scion_gc_duration_seconds{{dry_run=\"{dry}\"}} {}\n\
         # HELP scion_gc_rows_pruned Repo blob rows the last finished run pruned.\n\
         # TYPE scion_gc_rows_pruned gauge\n\
         scion_gc_rows_pruned{{dry_run=\"{dry}\"}} {}\n\
         # HELP scion_gc_files_removed Blob files the last finished run removed.\n\
         # TYPE scion_gc_files_removed gauge\n\
         scion_gc_files_removed{{dry_run=\"{dry}\"}} {}\n",
        took.as_secs_f64(),
        report.rows_pruned,
        report.files_removed,
    );
    // Write to a temp file beside the target, fsync it, then rename: a
    // reader of PATH never sees half a file, and a crash leaves the old
    // file whole. The temp name carries the pid: no lock spans a whole
    // gc run (the collector's key is per write transaction), so two runs
    // on one --metrics-file must not share a temp.
    let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
    let write = || -> std::io::Result<()> {
        use std::io::Write as _;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)
    };
    if let Err(e) = write() {
        // The run itself succeeded, so the command stays 0: the stale
        // timestamp of the old file is what the alert on the folder sees.
        eprintln!(
            "scion-server gc: cannot write the metrics file {}: {e}",
            path.display()
        );
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    // Logs go to stderr: stdout carries the output of `ticket-key` and
    // `hash-token`, which callers capture with $(...).
    // Color only when stderr is a terminal: a file or a pipe is a log
    // to keep, and escape bytes in it are noise on every line.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_ansi(std::io::stderr().is_terminal())
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    // Every `serve` flag is checked before the data directory opens: a bad
    // flag must not take the data directory's one-server lock, and must
    // not create a file in an empty directory.
    let flags = match &cli.command {
        Command::Serve(args) => Some(serve_flags(args)?),
        _ => None,
    };
    // `hash-token`, `verify`, and `restore` need no data directory (verify
    // reads only the archive; restore makes a new one). An import does not
    // land, so it can run beside the server. `derive` removes rows of older
    // versions of a kind, which a running server of an older build may
    // read, so it opens as the server.
    // The Postgres URL of the flags: the file form wins (the two cannot
    // be given together), the argv form warns, the env is quiet.
    let postgres_url = postgres_url_of(&cli)?;
    // `verify` and `restore` run without a store: verify reads only the
    // archive, and restore makes a data directory of its own. Handle them
    // before the store opens, so a restore's empty target stays empty of
    // one, and an archive check opens nothing at all.
    match cli.command {
        Command::Verify { archive } => verify_command(&archive),
        Command::Restore {
            archive,
            audit_file,
        } => {
            // The restore's audit file is the restored store's own: the
            // data directory may not exist yet, and its one file rides
            // the publication rename into place. A run that cannot
            // record refuses before it changes anything.
            scion_proto::runtime::auth::create_private_dir_all(&cli.data)
                .with_context(|| format!("cannot create {}", cli.data.display()))?;
            let audit = cli_audit(&cli.data, audit_file.as_deref())?;
            return restore_command(
                audit,
                &cli.data,
                &archive,
                cli.postgres_url.as_deref(),
                cli.postgres_ca.as_deref(),
            )
            .await;
        }
        _ => {}
    }
    // D25: a missing data directory is refused, not created. Every
    // subcommand that touches a store or writes into the directory must
    // find it there; `restore` is the one command whose job is to
    // create one, `verify` reads only the archive, and `hash-token`
    // touches no store. A silent `./scion-data` in whatever directory a
    // run happened to start in has bitten once already: ticket-key
    // wrote an audit log into the cwd's default, where CI's ACL
    // refused it.
    if let Err(refusal) = if matches!(
        cli.command,
        Command::Verify { .. } | Command::Restore { .. } | Command::HashToken { .. }
    ) {
        Ok(())
    } else {
        refuse_missing_data_dir(&cli.data)
    } {
        if matches!(cli.command, Command::Fsck) {
            // fsck's exit table promises 2 for "could not run"
            // (docs/stability.md, 1.x): the refusal is that case.
            eprintln!("scion-server fsck: {refusal}");
            std::process::exit(2);
        }
        return Err(refusal);
    }
    let store = match cli.command {
        Command::Import { .. } | Command::Gc { .. } | Command::Export { .. } => {
            Backend::open_shared(
                &cli.data,
                postgres_url.as_deref(),
                cli.postgres_ca.as_deref(),
            )
        }
        Command::HashToken { .. } => return hash_token(),
        Command::TicketKey { out, audit_file } => {
            // The key file lives wherever the operator says; the audit
            // log lives in the data directory, as for every other
            // command. A run that cannot record refuses to make the key.
            scion_proto::runtime::auth::create_private_dir_all(&cli.data)
                .with_context(|| format!("cannot create {}", cli.data.display()))?;
            let audit = cli_audit(&cli.data, audit_file.as_deref())?;
            return ticket_key_command(audit, &out).await;
        }
        // `fsck` cannot run when the store cannot open, and its table
        // says that is 2, not the generic open failure's 1.
        Command::Fsck => match Backend::open_shared(
            &cli.data,
            postgres_url.as_deref(),
            cli.postgres_ca.as_deref(),
        ) {
            Ok(store) => Ok(store),
            Err(e) => {
                eprintln!("scion-server fsck: {e}");
                // `exit` skips destructors: safe on this path, because
                // nothing was opened yet — the store refused to open,
                // so there is no lock to release and nothing buffered.
                std::process::exit(2);
            }
        },
        // `verify` and `restore` ended the function above: verify exits,
        // and restore returns.
        Command::Verify { .. } | Command::Restore { .. } => {
            unreachable!("handled before the store opens")
        }
        Command::Serve { .. } | Command::Derive { .. } => Backend::open(
            &cli.data,
            postgres_url.as_deref(),
            cli.postgres_ca.as_deref(),
        ),
    }
    .with_context(|| format!("cannot open data directory {}", cli.data.display()))?;
    match cli.command {
        Command::Serve { .. } => {
            let flags = flags.expect("a serve command validated its flags");
            // The audit opens after the store: the data directory must be
            // empty at its first open, and the default audit file lives
            // in it. A server that cannot record refuses to run.
            let audit_path = flags
                .audit_file
                .clone()
                .unwrap_or_else(|| cli.data.join("audit.log"));
            let audit = scion_server::Audit::open(&audit_path)
                .with_context(|| format!("cannot open audit log {}", audit_path.display()))?;
            // The migration predates the start, so its line goes first.
            record_migrated(&audit, &store).await?;
            audit
                .record(scion_server::Event::new(
                    "admin",
                    "ok",
                    serde_json::json!({
                        "what": "start",
                        "version": env!("CARGO_PKG_VERSION"),
                        "tls": flags.server_tls.is_some(),
                        "mtls": flags.mtls,
                        "insecure_listen": flags.insecure_listen,
                        "access_tokens": flags.access.token_count(),
                        "ticket_keys": flags.tickets.as_ref().map_or(0, |keys| keys.for_verify().len()),
                    }),
                ))
                .await
                .with_context(|| format!("cannot record the audit start line in {}", audit_path.display()))?;
            serve(
                store,
                flags.listen,
                flags.access,
                ServeConfig {
                    data: cli.data.clone(),
                    quotas: flags.caps.quotas,
                    pending_lands: flags.caps.pending_lands,
                    land_stale_secs: flags.land_stale_secs,
                    server_tls: flags.server_tls,
                    max_connections: flags.max_connections,
                    max_streams: flags.max_streams,
                    max_stalled_bytes: flags.max_stalled_bytes,
                    metrics_listen: flags.metrics_listen,
                    drain_timeout: Duration::from_secs(flags.drain_timeout_secs),
                    audit,
                },
                flags.tickets,
            )
            .await?;
        }
        Command::HashToken { .. }
        | Command::TicketKey { .. }
        | Command::Verify { .. }
        | Command::Restore { .. } => {
            unreachable!("handled before the store opens")
        }
        Command::Export { repo, all, out } => {
            archive_export(&store, &cli.data, repo.as_deref(), all, &out);
        }
        Command::Fsck => {
            return fsck_command(&store);
        }
        Command::Gc {
            grace_secs,
            dry_run,
            audit_file,
            metrics_file,
        } => {
            let audit = cli_audit(&cli.data, audit_file.as_deref())?;
            record_migrated(&audit, &store).await?;
            gc_command(
                audit,
                &store,
                Duration::from_secs(grace_secs),
                dry_run,
                metrics_file.as_deref(),
            )
            .await?;
        }
        Command::Import {
            repo,
            rev,
            target_ref,
            verify,
            git_dir,
            audit_file,
        } => {
            let audit = cli_audit(&cli.data, audit_file.as_deref())?;
            record_migrated(&audit, &store).await?;
            let opts = ImportOptions {
                rev,
                target_ref,
                verify_sample: verify,
            };
            import_command(audit, store, repo, git_dir, opts).await?;
        }
        Command::Derive {
            kind,
            repo,
            target_ref,
            audit_file,
        } => {
            let audit = cli_audit(&cli.data, audit_file.as_deref())?;
            record_migrated(&audit, &store).await?;
            let kind_name = match kind {
                KindName::RootTree => "root_tree",
                KindName::CommitGraph => "commit_graph",
                KindName::PathHistory => "path_history",
            };
            let what = serde_json::json!({ "what": "derive", "kind": kind_name });
            audit
                .record(scion_server::Event::new("admin", "intent", what.clone()))
                .await
                .with_context(|| "cannot record the derive intent in the audit log".to_owned())?;
            let run = move || {
                let tip = store.resolve_ref(&repo, &target_ref)?;
                let report = match kind {
                    KindName::RootTree => scion_server::backfill_root_tree(&store, &repo, tip)?,
                    KindName::CommitGraph => {
                        scion_server::backfill_commit_graph(&store, &repo, tip)?
                    }
                    KindName::PathHistory => {
                        scion_server::backfill_path_history(&store, &repo, tip)?
                    }
                };
                Ok::<_, Error>((report, tip))
            };
            let (report, tip) = match tokio::task::spawn_blocking(run).await {
                Ok(Ok(pair)) => pair,
                Ok(Err(e)) => {
                    record_admin_failed(&audit, "derive", error_class(&e)).await;
                    return Err(e.into());
                }
                Err(e) => {
                    record_admin_failed(&audit, "derive", "failed").await;
                    return Err(e.into());
                }
            };
            let fields = serde_json::json!({
                "what": "derive",
                "kind": kind_name,
                "written": report.written,
                "removed": report.removed,
            });
            audit
                .record(scion_server::Event::new("admin", "ok", fields))
                .await
                .with_context(|| "cannot record the derive result in the audit log".to_owned())?;
            println!(
                "derived {} for the history of {tip}; older versions are removed",
                report.name
            );
        }
    }
    Ok(())
}

/// The TLS of a `serve` command: none, or the certificate, key and client CA
/// of the flags. Clap requires the certificate and the key together, and the
/// client CA on a certificate.
fn server_tls(
    cert: Option<&Path>,
    key: Option<&Path>,
    client_ca: Option<&Path>,
) -> anyhow::Result<Option<ServerTls>> {
    match (cert, key) {
        (None, None) => Ok(None),
        (Some(cert), Some(key)) => {
            let tls = ServerTls::load(cert, key, client_ca)?;
            Ok(Some(tls))
        }
        _ => unreachable!("clap requires --tls-cert and --tls-key together"),
    }
}

/// The three per-principal caps, as the flags set them. With one principal
/// (the `--token` mode) a per-principal cap is a global one, so each cap is
/// off unless a flag sets it; with an access file the defaults apply.
struct Caps {
    quotas: Quotas,
    pending_lands: Option<usize>,
}

fn caps(
    single: bool,
    requests: Option<NonZeroUsize>,
    upload_bytes: Option<NonZeroUsize>,
    pending_lands: Option<NonZeroUsize>,
) -> Caps {
    let with_default = |flag: Option<NonZeroUsize>, default: usize| {
        if single {
            flag.map(NonZeroUsize::get)
        } else {
            Some(flag.map_or(default, NonZeroUsize::get))
        }
    };
    Caps {
        quotas: Quotas::new(
            with_default(requests, 64),
            with_default(upload_bytes, 512 << 20),
        ),
        pending_lands: with_default(pending_lands, 64),
    }
}

/// What a `serve` command serves with, besides the data directory and the
/// access rules.
struct ServeConfig {
    quotas: Quotas,
    pending_lands: Option<usize>,
    land_stale_secs: i64,
    server_tls: Option<ServerTls>,
    max_connections: NonZeroUsize,
    max_streams: NonZeroU32,
    max_stalled_bytes: NonZeroU64,
    metrics_listen: Option<SocketAddr>,
    data: std::path::PathBuf,
    /// How long the drain waits for the in-flight calls before it cuts
    /// them (`--drain-timeout-secs`).
    drain_timeout: std::time::Duration,
    /// The audit log, opened by the caller, which records its start
    /// line before the server serves.
    audit: scion_server::Audit,
}

/// The validated flags of a `serve` command: everything `serve` needs is
/// checked here, before the data directory opens, so a bad flag never takes
/// the data directory's one-server lock and never creates a file.
struct ServeFlags {
    listen: SocketAddr,
    access: scion_server::Access,
    caps: Caps,
    land_stale_secs: i64,
    server_tls: Option<ServerTls>,
    max_connections: NonZeroUsize,
    max_streams: NonZeroU32,
    max_stalled_bytes: NonZeroU64,
    metrics_listen: Option<SocketAddr>,
    tickets: Option<Arc<scion_server::Keys>>,
    drain_timeout_secs: u64,
    /// The audit log the operator asked for, if any. The default path
    /// is the data directory's, which only the caller knows.
    audit_file: Option<PathBuf>,
    /// The listen and TLS posture, for the audit's start line.
    insecure_listen: bool,
    mtls: bool,
}

/// Checks every `serve` flag that a parse alone does not settle: the rules
/// of `--token` against `--access`, the TLS files, the ticket keys, and the
/// listen rule (a non-loopback address needs TLS or `--insecure-listen`).
/// Checks every `serve` flag that a parse alone does not settle: the rules
/// of `--token` against `--access`, the TLS files, the ticket keys, and the
/// listen rule (a non-loopback address needs TLS or `--insecure-listen`).
fn serve_flags(args: &ServeArgs) -> anyhow::Result<ServeFlags> {
    // The token of the flags: the file form, the env, or the argv form,
    // which warns. The second reply says whether the mode is one token:
    // with one principal, a per-principal cap is a global one, so the
    // quotas are off unless a flag sets one.
    let token = match (&args.token, &args.token_file) {
        (Some(_), Some(_)) => unreachable!("clap refuses --token with --token-file"),
        (Some(_), None) => {
            warn_argv_secret("--token", "--token-file (or SCION_TOKEN)");
            args.token.clone()
        }
        (None, Some(file)) => Some(secret_file(file)?),
        (None, None) => std::env::var("SCION_TOKEN").ok().filter(|t| !t.is_empty()),
    };
    let (access, single) = load_access(token, None, args.access.clone())?;
    let caps = caps(
        single,
        args.max_principal_requests,
        args.max_principal_upload_bytes,
        args.max_principal_pending_lands,
    );
    let server_tls = server_tls(
        args.tls_cert.as_deref(),
        args.tls_key.as_deref(),
        args.tls_client_ca.as_deref(),
    )?;
    let tickets = load_tickets(args.ticket_key.as_deref(), args.ticket_key_old.as_deref())?;
    anyhow::ensure!(
        args.listen.ip().is_loopback() || args.insecure_listen || server_tls.is_some(),
        "{} is not a loopback address; without TLS the traffic and the \
         token would travel unencrypted. Pass --tls-cert and --tls-key, or \
         --insecure-listen to allow it.",
        args.listen
    );
    if let Some(metrics_listen) = args.metrics_listen {
        let checked = loopback_or_allow(metrics_listen, args.insecure_listen);
        anyhow::ensure!(checked.is_ok(), "{}", checked.unwrap_err());
    }
    Ok(ServeFlags {
        listen: args.listen,
        access,
        caps,
        land_stale_secs: args.land_stale_secs,
        server_tls,
        max_connections: args.max_connections,
        max_streams: args.max_streams,
        max_stalled_bytes: args.max_stalled_bytes,
        metrics_listen: args.metrics_listen,
        tickets,
        drain_timeout_secs: args.drain_timeout_secs,
        audit_file: args.audit_file.clone(),
        insecure_listen: args.insecure_listen,
        mtls: args.tls_client_ca.is_some(),
    })
}

/// Serves the gRPC API on `listen`, with the TLS files of the flags, until
/// the caller stops it.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn serve(
    store: Backend,
    listen: SocketAddr,
    access: scion_server::Access,
    config: ServeConfig,
    tickets: Option<Arc<scion_server::Keys>>,
) -> anyhow::Result<()> {
    // The registry always counts; only its plain-HTTP export is optional.
    // The metrics listener has no token, so unlike the gRPC listener, TLS
    // does not excuse a non-loopback address: `--insecure-listen` must allow
    // it.
    let metrics = Arc::new(Metrics::default());
    // One signal task ends the gRPC listener and the metrics export alike.
    // A second signal while the drain runs ends it at once: the in-flight
    // calls are cut, the log says so, and the process exits non-zero, so
    // the operator does not need kill -9.
    let (end_tx, end_rx) = tokio::sync::watch::channel(());
    tokio::spawn(async move {
        scion_proto::runtime::metrics::shutdown_signal().await;
        let _ = end_tx.send(());
        scion_proto::runtime::metrics::shutdown_signal().await;
        tracing::warn!("a second signal ended the drain; the in-flight calls were cut");
        std::process::exit(1);
    });
    // The drain bound: when the first signal starts the drain, the
    // in-flight calls have this long to finish. On expiry the calls are
    // cut, the log names the count, and the process exits non-zero. The
    // second signal above stays the operator's at-once cut.
    scion_proto::runtime::metrics::spawn_drain_bound(
        end_rx.clone(),
        config.drain_timeout,
        Arc::clone(&metrics),
    );
    if let Some(metrics_listen) = config.metrics_listen {
        let address = metrics_listen.to_string();
        let metrics_listener = tokio::net::TcpListener::bind(metrics_listen)
            .await
            .with_context(|| format!("cannot listen on {address}"))?;
        let metrics_addr = metrics_listener.local_addr()?;
        tracing::info!(listen = %metrics_addr, "serving metrics");
        let mut stopped = end_rx.clone();
        tokio::spawn(metrics_serve(
            metrics_listener,
            metrics.clone(),
            async move {
                let _ = stopped.changed().await;
            },
        ));
    }
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot listen on {listen}"))?;
    // The bound address, so port 0 shows the port that the OS chose.
    let listen = listener.local_addr()?;
    tracing::info!(%listen, "serving");

    Box::pin(scion_server::serve(
        store,
        listener,
        access,
        ServeOptions {
            data: config.data,
            limits: Limits {
                connections: config.max_connections,
                streams: config.max_streams,
                stalled_bytes: config.max_stalled_bytes,
            },
            quotas: config.quotas,
            pending_lands: config.pending_lands,
            land_stale_secs: config.land_stale_secs,
            tls: config.server_tls,
            metrics,
            audit: config.audit,
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        tickets,
        async move {
            let mut end_rx = end_rx;
            let _ = end_rx.changed().await;
        },
    ))
    .await?;
    Ok(())
}

/// The stderr warning of a secret that arrived on the command line: ps
/// and `/proc/$$/cmdline` show it to every local account, and it lands
/// in shell history. The argv form stays for now (the owner's D8: warn
/// first, remove in a later minor); the replacement reads a file that
/// the checked read of the access file protects.
fn argv_secret_warning(flag: &str, replacement: &str) -> String {
    format!(
        "warning: {flag} put a secret on the command line, which ps and \
         /proc/$$/cmdline show to every local account, and which lands in \
         shell history; use {replacement} instead (the argv form is \
         deprecated and goes away in a later release)"
    )
}

fn warn_argv_secret(flag: &str, replacement: &str) {
    eprintln!("{}", argv_secret_warning(flag, replacement));
}

/// A secret from a file the checked read protects, trimmed: the owner
/// alone may read it, a symlink is refused, and the directories above
/// it are checked.
fn secret_file(file: &Path) -> anyhow::Result<String> {
    let bytes = scion_proto::runtime::auth::read_secret_file(file)
        .map_err(|why| scion_proto::runtime::auth::checked_read_error(file, why))?;
    let text =
        String::from_utf8(bytes).map_err(|_| anyhow::anyhow!("{} is not UTF-8", file.display()))?;
    let token = text.trim();
    anyhow::ensure!(!token.is_empty(), "{} holds no secret", file.display());
    Ok(token.to_owned())
}

/// The Postgres URL of the CLI: the file form wins (clap refuses both
/// at once), the argv form warns, the env is quiet.
fn postgres_url_of(cli: &Cli) -> anyhow::Result<Option<String>> {
    if let Some(file) = &cli.postgres_url_file {
        return Ok(Some(secret_file(file)?));
    }
    if let Some(url) = &cli.postgres_url {
        warn_argv_secret(
            "--postgres-url",
            "--postgres-url-file (or SCION_POSTGRES_URL)",
        );
        return Ok(Some(url.clone()));
    }
    Ok(std::env::var("SCION_POSTGRES_URL")
        .ok()
        .filter(|u| !u.is_empty()))
}

/// The rules of `serve`: exactly one of `--token` (one principal with
/// every right), `--token-file` (the same principal, from a checked
/// file) and `--access` (a file of tokens and rules).
fn load_access(
    token: Option<String>,
    token_file: Option<PathBuf>,
    access: Option<PathBuf>,
) -> anyhow::Result<(scion_server::Access, bool)> {
    let given = [token.is_some(), token_file.is_some(), access.is_some()]
        .into_iter()
        .filter(|given| *given)
        .count();
    anyhow::ensure!(
        given == 1,
        "give --token, --token-file or --access, not two"
    );
    if let Some(file) = token_file {
        let token = secret_file(&file)?;
        return Ok((scion_server::Access::single(&token), true));
    }
    if let Some(token) = token {
        anyhow::ensure!(!token.is_empty(), "--token must not be empty");
        return Ok((scion_server::Access::single(&token), true));
    }
    let file = access.expect("exactly one source is given");
    // The access file holds bearer tokens: read through one checked
    // descriptor, the way the ticket keys are read.
    let bytes = scion_proto::runtime::auth::read_secret_file(&file)
        .map_err(|why| scion_proto::runtime::auth::checked_read_error(&file, why))?;
    let text =
        String::from_utf8(bytes).map_err(|_| anyhow::anyhow!("{} is not UTF-8", file.display()))?;
    let access = scion_server::Access::parse(&text)
        .map_err(|why| anyhow::anyhow!("bad access file {}: {why}", file.display()))?;
    Ok((access, false))
}

/// The ticket keys of `serve`: a signing key, and the public key that a
/// rotation keeps alive. Both files hold 64 hex characters.
fn load_tickets(
    seed: Option<&Path>,
    old: Option<&Path>,
) -> anyhow::Result<Option<Arc<scion_server::Keys>>> {
    let Some(seed) = seed else {
        anyhow::ensure!(old.is_none(), "--ticket-key-old needs --ticket-key");
        return Ok(None);
    };
    let keys = scion_server::Keys::load(
        &read_hex(seed, true)?,
        &match old {
            Some(file) => vec![read_hex(file, false)?],
            None => Vec::new(),
        },
    )
    .map_err(|why| anyhow::anyhow!("{why}"))?;
    Ok(Some(Arc::new(keys)))
}

/// The file's trimmed text, as a key wants it.
///
/// A seed must be readable by its owner alone: it mints permissions to
/// anyone who can read it. A public key is not a secret, so anyone may
/// read it; but group or others must not be able to WRITE it, because a
/// changed key breaks every tier that verifies against the old one, and a
/// new one mints nothing good. Both are read through one checked
/// descriptor, by [`read_secret_file`] and [`read_shared_file`]: the
/// directories above are checked, the file is opened once with
/// `O_NOFOLLOW`, and kind, owner, and mode run on the open descriptor the
/// bytes are read from.
fn read_hex(file: &Path, secret: bool) -> anyhow::Result<String> {
    let bytes = if secret {
        scion_proto::runtime::auth::read_secret_file(file)
    } else {
        scion_proto::runtime::auth::read_shared_file(file)
    }
    .map_err(|why| scion_proto::runtime::auth::checked_read_error(file, why))?;
    let text =
        String::from_utf8(bytes).map_err(|_| anyhow::anyhow!("{} is not UTF-8", file.display()))?;
    Ok(text.trim().to_owned())
}

/// Writes a fresh ticket key to `--out` and prints its public key. The
/// file is created, never replaced: a second run to the same path fails,
/// because silently changing the key breaks every tier that was given the
/// old public half. The file is 0600: the seed mints permissions.
/// Makes the ticket key and records its admin pair: one intent before
/// the key exists and one ok after it is on the disk. The lines carry
/// the what and nothing else: never the seed, the key bytes, or the key
/// file's path. Exits with the run's own status when the line after the
/// key still fails.
async fn ticket_key_command(audit: scion_server::Audit, out: &Path) -> anyhow::Result<()> {
    audit
        .record(scion_server::Event::new(
            "admin",
            "intent",
            serde_json::json!({ "what": "ticket-key" }),
        ))
        .await
        .with_context(|| "cannot record the ticket-key intent in the audit log".to_owned())?;
    let made = match make_key_file(out) {
        Ok(keys) => keys,
        Err(e) => {
            record_admin_failed(&audit, "ticket-key", "failed").await;
            return Err(e)
                .with_context(|| format!("cannot write the ticket key to {}", out.display()));
        }
    };
    audit
        .record(scion_server::Event::new(
            "admin",
            "ok",
            serde_json::json!({ "what": "ticket-key" }),
        ))
        .await
        .with_context(|| "cannot record the ticket-key result in the audit log".to_owned())?;
    println!("{}", made.public_hex());
    Ok(())
}

/// Generates the key and writes the seed to a new private file. Returns
/// the key, or the error the caller records and reports.
fn make_key_file(out: &Path) -> std::result::Result<scion_server::Keys, anyhow::Error> {
    let keys = scion_server::Keys::generate();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(out)
        .with_context(|| {
            format!(
                "cannot create {} (it exists already? the key must not change under the tiers)",
                out.display()
            )
        })?;
    file.write_all(format!("{}\n", keys.seed_hex()).as_bytes())
        .with_context(|| format!("cannot write {}", out.display()))?;
    Ok(keys)
}

/// Prints the BLAKE3 digest of the token of `hash-token`.
fn hash_token() -> anyhow::Result<()> {
    let Cli { command, .. } = Cli::parse();
    let Command::HashToken { token, token_file } = command else {
        unreachable!("hash_token handles only the hash-token command")
    };
    // The token's source: the checked file, the env, the argv form (it
    // warns), or stdin, which keeps the token out of the process list
    // and out of the shell history alike.
    let token = match (&token, &token_file) {
        (Some(_), Some(_)) => unreachable!("clap refuses --token with --token-file"),
        (None, Some(file)) => secret_file(file)?,
        (Some(_), None) => {
            warn_argv_secret("--token", "--token-file, SCION_TOKEN or stdin");
            token.expect("the argv form is given")
        }
        (None, None) => match std::env::var("SCION_TOKEN") {
            Ok(env_token) if !env_token.is_empty() => env_token,
            _ => {
                use std::io::Read as _;
                let mut from_stdin = String::new();
                std::io::stdin()
                    .read_to_string(&mut from_stdin)
                    .context("cannot read the token from stdin")?;
                let from_stdin = from_stdin.trim().to_owned();
                anyhow::ensure!(
                    !from_stdin.is_empty(),
                    "give --token, --token-file, SCION_TOKEN or a token on stdin"
                );
                from_stdin
            }
        },
    };
    println!(
        "{}",
        blake3::Hash::from(scion_proto::runtime::auth::token_digest(&token)).to_hex()
    );
    Ok(())
}

#[cfg(test)]
mod key_file_tests {
    use super::*;

    fn write_key(dir: &Path, name: &str, mode: u32, text: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let file = dir.join(name);
        std::fs::write(&file, text).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();
        file
    }

    #[test]
    fn a_private_seed_reads_back_trimmed() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_key(dir.path(), "seed", 0o600, "  abcd \n");
        assert_eq!(read_hex(&file, true).unwrap(), "abcd");
    }

    #[test]
    fn a_public_key_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_key(dir.path(), "old", 0o644, "abcd\n");
        assert_eq!(read_hex(&file, false).unwrap(), "abcd");
    }

    #[test]
    fn a_group_readable_seed_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_key(dir.path(), "seed", 0o640, "abcd");
        let err = read_hex(&file, true).unwrap_err().to_string();
        assert!(err.contains("read by group or others"), "{err}");
    }

    #[test]
    fn a_group_writable_public_key_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_key(dir.path(), "old", 0o664, "abcd");
        let err = read_hex(&file, false).unwrap_err().to_string();
        assert!(err.contains("written by group or others"), "{err}");
    }

    /// The final component is opened with `O_NOFOLLOW`, so a symlink dies
    /// at the open even when its target has a perfect mode and owner:
    /// the checks must hold of the file at the checked path, not of
    /// whichever file the path names when the read happens.
    #[test]
    fn a_symlinked_key_is_refused_with_a_perfect_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = write_key(dir.path(), "real", 0o600, "abcd");
        let link = dir.path().join("seed");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let err = read_hex(&link, true).unwrap_err().to_string();
        assert!(err.contains("symlink"), "{err}");
    }
}

/// The access file of `--access` holds bearer tokens: the same checked
/// read as the keys ([`read_secret_file`]), so a file that group or
/// others can read is refused, not silently taken.
#[cfg(test)]
mod access_file_tests {
    use super::*;

    fn write_access(dir: &Path, name: &str, mode: u32, text: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let file = dir.join(name);
        std::fs::write(&file, text).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();
        file
    }

    #[test]
    fn an_access_file_the_group_or_others_can_read_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_access(dir.path(), "access", 0o644, "# rules\n");
        let err = load_access(None, None, Some(file)).unwrap_err().to_string();
        assert!(err.contains("read by group or others"), "{err}");
    }

    #[test]
    fn a_private_access_file_parses() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_access(dir.path(), "access", 0o600, "# rules\n");
        let (_, single) = load_access(None, None, Some(file)).unwrap();
        assert!(!single);
    }

    #[test]
    fn a_symlinked_access_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let target = write_access(dir.path(), "real", 0o600, "# rules\n");
        let link = dir.path().join("access");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let err = load_access(None, None, Some(link)).unwrap_err().to_string();
        assert!(err.contains("symlink"), "{err}");
    }
}
