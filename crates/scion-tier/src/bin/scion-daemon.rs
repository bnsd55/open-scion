#![forbid(unsafe_code)]
//! `scion-daemon`: the node tier. Stores objects on disk for every job on the
//! node, serves them over a Unix socket, and fetches misses from its upstream,
//! a `scion-cache`.

use std::io::IsTerminal;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context as _;
use clap::Parser;
use scion_proto::runtime::listen::bind_unix;
use scion_proto::runtime::listen::{self, Limits};
use scion_proto::runtime::tls::ClientTls;
use scion_tier::{DAEMON_UPSTREAM_TIMEOUT, DiskStore, Tier, snapshot};
use tokio::signal::unix::{SignalKind, signal};
use tokio_stream::wrappers::UnixListenerStream;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about = "Node daemon for the Scion version control system")]
struct Cli {
    /// Data directory for stored objects. Created with mode 0700 if missing;
    /// refused if other users can reach it.
    #[arg(long, env = "SCION_DAEMON_DATA", default_value = "scion-daemon-data")]
    data: PathBuf,
    /// Unix socket to listen on, with mode 0600, in a directory that only
    /// this user can reach [default: daemon.sock in the data directory].
    #[arg(long)]
    socket: Option<PathBuf>,
    /// Most object bytes to keep on disk.
    #[arg(long, default_value_t = 10 << 30)]
    max_bytes: usize,
    /// Where to fetch misses: a scion-cache.
    #[arg(long, default_value = "http://127.0.0.1:7879")]
    upstream: String,
    /// Trust this CA (PEM) for the https upstream.
    #[arg(long, value_name = "FILE")]
    tls_ca: Option<PathBuf>,
    /// Present this client certificate (PEM) to the upstream, for mTLS,
    /// together with `--tls-client-key`.
    #[arg(long, requires = "tls_client_key", value_name = "FILE")]
    tls_client_cert: Option<PathBuf>,
    /// The key of `--tls-client-cert` (PEM).
    #[arg(long, requires = "tls_client_cert", value_name = "FILE")]
    tls_client_key: Option<PathBuf>,
    /// Bearer token that clients must send. Give `--token-file`, or
    /// `SCION_TOKEN`; the argv form sits in the process list and warns.
    #[arg(long, value_name = "TOKEN")]
    token: Option<String>,
    /// File with the bearer token of `--token`, read like the access
    /// file: the owner alone may read it, and a symlink is refused.
    #[arg(long, conflicts_with = "token", value_name = "FILE")]
    token_file: Option<PathBuf>,
    /// Bearer token to send upstream. Give `--upstream-token-file`, or
    /// `SCION_UPSTREAM_TOKEN`; the argv form sits in the process list
    /// and warns.
    #[arg(long, value_name = "TOKEN")]
    upstream_token: Option<String>,
    /// File with the bearer token of `--upstream-token`, read like the
    /// access file.
    #[arg(long, conflicts_with = "upstream_token", value_name = "FILE")]
    upstream_token_file: Option<PathBuf>,
    /// Most connections served at once. More clients wait until one closes.
    #[arg(long, default_value_t = listen::DEFAULT_CONNECTIONS)]
    max_connections: NonZeroUsize,
    /// Most calls (HTTP/2 streams) that one connection may have open at once.
    #[arg(long, default_value_t = listen::DEFAULT_STREAMS)]
    max_streams: NonZeroU32,
    /// A snapshot from scion-cache (`--snapshot`) to start from. Read only
    /// if the store holds no object: each object is checked against its ID
    /// and stored before the daemon serves. Repeat for more. Seeding one is
    /// refused without `--seed-full-repo`.
    #[arg(long, value_name = "FILE")]
    seed: Vec<PathBuf>,
    /// Say that every `--seed` snapshot is a full-repo credential: whoever
    /// has the file can read its whole repo, so every user of this daemon
    /// may read that repo whole. A snapshot without this say-so is refused.
    #[arg(long, requires = "seed")]
    seed_full_repo: bool,
    /// Most bytes that the stalled replies of one connection may hold. A
    /// connection over it is closed with them.
    #[arg(long, default_value_t = listen::DEFAULT_STALLED_BYTES)]
    max_stalled_bytes: NonZeroU64,
}

/// The two tokens of a tier: what it asks for, and what it sends
/// upstream. Each resolves through the checked file, the env, or the
/// argv form, which warns.
fn resolve_tokens(cli: &Cli) -> anyhow::Result<(String, String)> {
    let token = secret_of(
        cli.token.as_deref(),
        cli.token_file.as_deref(),
        "--token",
        "--token-file",
        "SCION_TOKEN",
    )?;
    let upstream = secret_of(
        cli.upstream_token.as_deref(),
        cli.upstream_token_file.as_deref(),
        "--upstream-token",
        "--upstream-token-file",
        "SCION_UPSTREAM_TOKEN",
    )?;
    Ok((token, upstream))
}

/// A secret that the flags may carry three ways: the checked file, the
/// env, or the argv form, which warns that the process list shows it.
/// The warning names the replacement, per the deprecation rule: warn
/// first, remove in a later minor.
fn secret_of(
    token: Option<&str>,
    file: Option<&std::path::Path>,
    flag: &str,
    file_flag: &str,
    env: &str,
) -> anyhow::Result<String> {
    match (token, file) {
        (Some(_), Some(_)) => unreachable!("clap refuses {flag} with {file_flag}"),
        (Some(_), None) => {
            eprintln!(
                "warning: {flag} put a secret on the command line, which ps and \
                 /proc/$$/cmdline show to every local account; use {file_flag} \
                 (or {env}) instead (the argv form is deprecated and goes away \
                 in a later release)"
            );
            Ok(token.expect("the argv form is given").to_owned())
        }
        (None, Some(file)) => {
            let bytes = scion_proto::runtime::auth::read_secret_file(file)
                .map_err(|why| scion_proto::runtime::auth::checked_read_error(file, why))?;
            let text = String::from_utf8(bytes)
                .map_err(|_| anyhow::anyhow!("{} is not UTF-8", file.display()))?;
            let token = text.trim();
            anyhow::ensure!(!token.is_empty(), "{} holds no token", file.display());
            Ok(token.to_owned())
        }
        (None, None) => {
            let from_env = std::env::var(env).unwrap_or_default();
            anyhow::ensure!(!from_env.is_empty(), "give {flag}, {file_flag} or {env}");
            Ok(from_env)
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs go to stderr: stdout may carry command output that callers
    // capture with $(...).
    // Color only when stderr is a terminal: a file or a pipe is a log
    // to keep, and escape bytes in it are noise on every line.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_ansi(std::io::stderr().is_terminal())
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let (token, upstream) = resolve_tokens(&cli)?;
    anyhow::ensure!(
        cli.seed.is_empty() || cli.seed_full_repo,
        "refusing to seed without --seed-full-repo: a snapshot is a \
         full-repo credential, it holds every object of its repo, so whoever \
         has the file can read what a folder rule would hide from a partial \
         reader; pass --seed-full-repo when every user of this daemon may \
         read the repo whole"
    );
    let store = DiskStore::open(&cli.data, cli.max_bytes)
        .with_context(|| format!("cannot open data directory {}", cli.data.display()))?;
    let client_tls = ClientTls::load(
        cli.tls_ca.as_deref(),
        cli.tls_client_cert.as_deref(),
        cli.tls_client_key.as_deref(),
    )?;
    for path in &cli.seed {
        seed(&store, path);
    }
    let tier = Tier::new(
        store,
        &cli.upstream,
        &upstream,
        DAEMON_UPSTREAM_TIMEOUT,
        Some(&client_tls),
        // A daemon does not check tickets: it forwards them, and its
        // upstream does the checking. Only the walk gates what it serves.
        Vec::new(),
    )?;
    let socket = cli.socket.unwrap_or_else(|| cli.data.join("daemon.sock"));
    let listener = bind_unix(&socket)
        .await
        .with_context(|| format!("cannot listen on {}", socket.display()))?;
    // Where the socket is, with its directory resolved. `--socket` may go
    // through a symlink that another user can repoint before the removal
    // below, which then must not follow it.
    let socket = listener
        .local_addr()?
        .as_pathname()
        .context("the socket has no path")?
        .to_owned();
    // SIGTERM too, as service managers send it: a clean stop writes the
    // store's index, so the next start reads it instead of every file.
    let mut terminate = signal(SignalKind::terminate()).context("cannot listen for SIGTERM")?;
    tracing::info!(socket = %socket.display(), upstream = %cli.upstream, "serving");
    let limits = Limits {
        connections: cli.max_connections,
        streams: cli.max_streams,
        stalled_bytes: cli.max_stalled_bytes,
    };
    Box::pin(tier.serve(
        &token,
        UnixListenerStream::new(listener),
        limits,
        None,
        async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            tracing::info!("stopping");
        },
    ))
    .await?;
    // A failure leaves a stale socket file, which is harmless: the next
    // open checks whether a listener is behind it and removes it then
    // (bind_unix).
    std::fs::remove_file(&socket).ok();
    // The store writes its index when the last task that holds the tier ends,
    // as the runtime stops after this.
    Ok(())
}

/// Seeds a cold `store` from the snapshot at `path`. A seed only saves
/// fetches, so a failure is logged, and the daemon starts as it is.
fn seed(store: &DiskStore, path: &std::path::Path) {
    let started = Instant::now();
    match snapshot::seed(store, path) {
        Ok(Some(seeded)) => {
            let ms = started.elapsed().as_millis();
            let snapshot::Seeded {
                repo,
                stored,
                bytes,
                skipped,
                damaged,
            } = seeded;
            tracing::info!(%repo, stored, bytes, skipped, damaged, ms, "seeded the store from a snapshot");
            if skipped > 0 || damaged {
                tracing::warn!(path = %path.display(), skipped, damaged, "the snapshot holds bad objects or is torn; they were not stored");
            }
        }
        Ok(None) => {
            tracing::info!(path = %path.display(), "the store holds objects; the snapshot is not read");
        }
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "cannot seed the store; it starts cold");
        }
    }
}
