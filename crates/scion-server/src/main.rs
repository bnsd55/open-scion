#![forbid(unsafe_code)]
//! The `scion-server` binary: `import`, `serve` and `derive`.

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
use scion_server::{Backend, Error, ServeOptions, fsck, gc};
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
    /// is never logged.
    #[arg(
        long,
        env = "SCION_POSTGRES_URL",
        hide_env_values = true,
        value_name = "URL"
    )]
    postgres_url: Option<String>,
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
    /// may read and write every repo. Give `--token` or `--access`, not
    /// both. With this, the per-principal quotas are off unless a flag
    /// sets one: every client is the same principal, so a cap would be
    /// a global one.
    #[arg(long, env = "SCION_TOKEN", hide_env_values = true)]
    token: Option<String>,
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
        /// The token to digest.
        #[arg(long, env = "SCION_TOKEN", hide_env_values = true)]
        token: String,
    },
    /// Generate an Ed25519 key for the tickets of object reads: writes the
    /// secret seed to `--out`, and prints the public key as 64 hex
    /// characters, for `--ticket-key-old` and the tiers to verify with.
    TicketKey {
        /// File to write the secret seed to, as 64 hex characters.
        #[arg(long, value_name = "FILE")]
        out: PathBuf,
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
    },
    /// Check a data directory and report the damage it finds. Read-only: it
    /// changes nothing, and it can run while the server runs. It reads in
    /// short batches, and each check looks at the state at the moment it
    /// looks, so a clean report means "no damage seen", not a snapshot.
    /// One line per problem: kind, ID, path. Exit 0 when it saw no damage,
    /// 1 when it did, 2 when it could not run.
    Fsck,
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

/// Runs `fsck` against `store`, prints one line per problem, and exits:
/// 0 when it saw no damage, 1 when it did, 2 when it could not run.
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

/// Runs `gc` against `store` and prints what it collected. Exits 2 when it
/// could not run.
fn gc_command(store: &Backend, grace: Duration, dry_run: bool, metrics_file: Option<&Path>) {
    let started = std::time::Instant::now();
    let report = match gc::run(store, grace, dry_run, &mut |_| {}) {
        Ok(report) => report,
        Err(e) => {
            eprintln!("scion-server gc: {e}");
            std::process::exit(2);
        }
    };
    println!(
        "gc: {} repos, {} rows pruned, {} files removed{}",
        report.repos,
        report.rows_pruned,
        report.files_removed,
        if dry_run { " (dry run)" } else { "" },
    );
    if let Some(path) = metrics_file {
        write_gc_metrics_file(path, &report, started.elapsed(), dry_run);
    }
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
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
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
    // `hash-token` needs no data directory. An import does not land, so it
    // can run beside the server. `derive` removes rows of older versions of a
    // kind, which a running server of an older build may read, so it opens as
    // the server.
    let store = match cli.command {
        Command::Import { .. } | Command::Fsck | Command::Gc { .. } => Backend::open_shared(
            &cli.data,
            cli.postgres_url.as_deref(),
            cli.postgres_ca.as_deref(),
        ),
        Command::HashToken { .. } => return hash_token(),
        Command::TicketKey { .. } => return write_ticket_key(),
        Command::Serve { .. } | Command::Derive { .. } => Backend::open(
            &cli.data,
            cli.postgres_url.as_deref(),
            cli.postgres_ca.as_deref(),
        ),
    }
    .with_context(|| format!("cannot open data directory {}", cli.data.display()))?;
    match cli.command {
        Command::Serve { .. } => {
            let flags = flags.expect("a serve command validated its flags");
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
                },
                flags.tickets,
            )
            .await?;
        }
        Command::HashToken { .. } | Command::TicketKey { .. } => {
            unreachable!("handled before the store opens")
        }
        Command::Fsck => {
            return fsck_command(&store);
        }
        Command::Gc {
            grace_secs,
            dry_run,
            metrics_file,
        } => {
            gc_command(
                &store,
                Duration::from_secs(grace_secs),
                dry_run,
                metrics_file.as_deref(),
            );
        }
        Command::Import {
            repo,
            rev,
            target_ref,
            verify,
            git_dir,
        } => {
            let opts = ImportOptions {
                rev,
                target_ref,
                verify_sample: verify,
            };
            let report =
                tokio::task::spawn_blocking(move || import_git(&store, &repo, &git_dir, &opts))
                    .await??;
            println!(
                "imported {} of {} commits, verified {}, tip {}",
                report.imported, report.commits, report.verified, report.tip
            );
        }
        Command::Derive {
            kind,
            repo,
            target_ref,
        } => {
            let (name, tip) = tokio::task::spawn_blocking(move || {
                let tip = store.resolve_ref(&repo, &target_ref)?;
                let name = match kind {
                    KindName::RootTree => scion_server::backfill_root_tree(&store, &repo, tip)?,
                    KindName::CommitGraph => {
                        scion_server::backfill_commit_graph(&store, &repo, tip)?
                    }
                    KindName::PathHistory => {
                        scion_server::backfill_path_history(&store, &repo, tip)?
                    }
                };
                Ok::<_, Error>((name, tip))
            })
            .await??;
            println!("derived {name} for the history of {tip}; older versions are removed");
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
}

/// Checks every `serve` flag that a parse alone does not settle: the rules
/// of `--token` against `--access`, the TLS files, the ticket keys, and the
/// listen rule (a non-loopback address needs TLS or `--insecure-listen`).
/// Checks every `serve` flag that a parse alone does not settle: the rules
/// of `--token` against `--access`, the TLS files, the ticket keys, and the
/// listen rule (a non-loopback address needs TLS or `--insecure-listen`).
fn serve_flags(args: &ServeArgs) -> anyhow::Result<ServeFlags> {
    // The second reply says whether the mode is one token: with one
    // principal, a per-principal cap is a global one, so the quotas
    // are off unless a flag sets one.
    let (access, single) = load_access(args.token.clone(), args.access.clone())?;
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

/// The rules of `serve`: exactly one of `--token` (one principal with every
/// right) and `--access` (a file of tokens and rules).
fn load_access(
    token: Option<String>,
    file: Option<PathBuf>,
) -> anyhow::Result<(scion_server::Access, bool)> {
    match (token, file) {
        (Some(_), Some(_)) => anyhow::bail!("give --token or --access, not both"),
        (Some(token), None) => {
            anyhow::ensure!(!token.is_empty(), "--token must not be empty");
            Ok((scion_server::Access::single(&token), true))
        }
        (None, Some(file)) => {
            // The access file holds bearer tokens: read through one checked
            // descriptor, the way the ticket keys are read.
            let bytes = scion_proto::runtime::auth::read_secret_file(&file)
                .map_err(|why| scion_proto::runtime::auth::checked_read_error(&file, why))?;
            let text = String::from_utf8(bytes)
                .map_err(|_| anyhow::anyhow!("{} is not UTF-8", file.display()))?;
            let access = scion_server::Access::parse(&text)
                .map_err(|why| anyhow::anyhow!("bad access file {}: {why}", file.display()))?;
            Ok((access, false))
        }
        (None, None) => anyhow::bail!("give --token or --access"),
    }
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
fn write_ticket_key() -> anyhow::Result<()> {
    let Cli { command, .. } = Cli::parse();
    let Command::TicketKey { out } = command else {
        unreachable!("write_ticket_key handles only the ticket-key command")
    };
    let keys = scion_server::Keys::generate();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&out)
        .with_context(|| {
            format!(
                "cannot create {} (it exists already? the key must not change under the tiers)",
                out.display()
            )
        })?;
    file.write_all(format!("{}\n", keys.seed_hex()).as_bytes())
        .with_context(|| format!("cannot write {}", out.display()))?;
    println!("{}", keys.public_hex());
    Ok(())
}

/// Prints the BLAKE3 digest of the token of `hash-token`.
fn hash_token() -> anyhow::Result<()> {
    let Cli { command, .. } = Cli::parse();
    let Command::HashToken { token } = command else {
        unreachable!("hash_token handles only the hash-token command")
    };
    anyhow::ensure!(!token.is_empty(), "--token must not be empty");
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
        let err = load_access(None, Some(file)).unwrap_err().to_string();
        assert!(err.contains("read by group or others"), "{err}");
    }

    #[test]
    fn a_private_access_file_parses() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_access(dir.path(), "access", 0o600, "# rules\n");
        let (_, single) = load_access(None, Some(file)).unwrap();
        assert!(!single);
    }

    #[test]
    fn a_symlinked_access_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let target = write_access(dir.path(), "real", 0o600, "# rules\n");
        let link = dir.path().join("access");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let err = load_access(None, Some(link)).unwrap_err().to_string();
        assert!(err.contains("symlink"), "{err}");
    }
}
