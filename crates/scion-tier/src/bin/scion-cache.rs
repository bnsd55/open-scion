//! `scion-cache`: a regional cache tier. Serves the `Objects` calls from
//! memory and fetches misses from its upstream, the server.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::os::unix::fs::DirBuilderExt as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use clap::Parser;
use scion_proto::runtime::check;
use scion_proto::runtime::listen::{self, Limits};
use scion_proto::runtime::tls::{ClientTls, ServerTls};
use scion_tier::{CACHE_UPSTREAM_TIMEOUT, MemoryStore, Tier, snapshot};
use tokio::time::MissedTickBehavior;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about = "Regional cache for the Scion version control system")]
struct Cli {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:7879")]
    listen: SocketAddr,
    /// Allow a non-loopback address. Without TLS, traffic, including the
    /// token, is not encrypted, so use this only on a trusted network. With
    /// `--tls-cert` and `--tls-key`, a non-loopback address is allowed
    /// without this.
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
    /// Where to fetch misses: the server.
    #[arg(long, default_value = "http://127.0.0.1:7878")]
    upstream: String,
    /// Most object bytes to keep in memory.
    #[arg(long, default_value_t = 1 << 30)]
    max_bytes: usize,
    /// Bearer token that clients must send.
    #[arg(long, env = "SCION_TOKEN", hide_env_values = true)]
    token: String,
    /// Bearer token to send upstream.
    #[arg(long, env = "SCION_UPSTREAM_TOKEN", hide_env_values = true)]
    upstream_token: String,
    /// The public keys (64 hex characters, from `scion-server ticket-key`)
    /// that a ticket of a call must verify against. One is enough; a
    /// second keeps the old key alive during a rotation. Without one, no
    /// object is served.
    #[arg(long, value_name = "HEX")]
    ticket_public_key: Vec<String>,
    /// Most connections served at once. More clients wait until one closes.
    #[arg(long, default_value_t = listen::DEFAULT_CONNECTIONS)]
    max_connections: NonZeroUsize,
    /// Most calls (HTTP/2 streams) that one connection may have open at once.
    #[arg(long, default_value_t = listen::DEFAULT_STREAMS)]
    max_streams: NonZeroU32,
    /// Directory for snapshots, for daemons to start from (`scion-daemon
    /// --seed`): one file per --snapshot-repo, DIR/REPO.scnsnap (a byte
    /// that is not a letter, a digit, '-', '_' or '.' as %XX). Created with
    /// mode 0700 if missing.
    #[arg(long, value_name = "DIR", requires = "snapshot_repo")]
    snapshot: Option<PathBuf>,
    /// A repo to write snapshots of: the objects that went into its
    /// replies, most recently used first. Repeat for more. Give a node only
    /// repos that ALL its users may read WHOLE: a snapshot holds every
    /// object of the repo by ID, so a daemon seeded with it serves paths
    /// that folder read rules hide.
    #[arg(long, value_name = "REPO", requires = "snapshot")]
    snapshot_repo: Vec<String>,
    /// Most object bytes in one snapshot.
    #[arg(long, default_value_t = 256 << 20)]
    snapshot_bytes: usize,
    /// Seconds between snapshots. The first comes one interval after start.
    #[arg(long, value_name = "SECS", default_value_t = 600,
          value_parser = clap::value_parser!(u64).range(1..))]
    snapshot_every: u64,
    /// Most bytes that the stalled replies of one connection may hold. A
    /// connection over it is closed with them.
    #[arg(long, default_value_t = listen::DEFAULT_STALLED_BYTES)]
    max_stalled_bytes: NonZeroU64,
    /// Serve Prometheus metrics over plain HTTP on this address. Off by
    /// default. Follows the listen rule: a non-loopback address needs
    /// `--insecure-listen`, because the metrics travel unencrypted.
    #[arg(long, value_name = "ADDR")]
    metrics_listen: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs go to stderr: stdout may carry command output that callers
    // capture with $(...).
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    anyhow::ensure!(!cli.token.is_empty(), "--token must not be empty");
    let server_tls = match (cli.tls_cert.as_deref(), cli.tls_key.as_deref()) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some(ServerTls::load(cert, key, cli.tls_client_ca.as_deref())?),
        _ => unreachable!("clap requires --tls-cert and --tls-key together"),
    };
    anyhow::ensure!(
        cli.listen.ip().is_loopback() || cli.insecure_listen || server_tls.is_some(),
        "{} is not a loopback address; without TLS the traffic and the token would \
         travel unencrypted. Pass --tls-cert and --tls-key, or --insecure-listen to \
         allow it.",
        cli.listen
    );
    let client_tls = ClientTls::load(
        cli.tls_ca.as_deref(),
        cli.tls_client_cert.as_deref(),
        cli.tls_client_key.as_deref(),
    )?;
    for repo in &cli.snapshot_repo {
        check::name("--snapshot-repo", repo).map_err(anyhow::Error::msg)?;
    }
    let store = MemoryStore::new(cli.max_bytes).recording(&cli.snapshot_repo);
    let tier = Tier::new(
        store,
        &cli.upstream,
        &cli.upstream_token,
        CACHE_UPSTREAM_TIMEOUT,
        Some(&client_tls),
        ticket_keys(&cli.ticket_public_key)?,
    )?;
    if let Some(dir) = cli.snapshot {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .with_context(|| format!("cannot make {}", dir.display()))?;
        let every = Duration::from_secs(cli.snapshot_every);
        tokio::spawn(snapshots(
            tier.clone(),
            dir,
            cli.snapshot_repo,
            cli.snapshot_bytes,
            every,
        ));
    }
    // One signal task ends the Objects listener and the metrics export
    // alike.
    let (end_tx, end_rx) = tokio::sync::watch::channel(());
    tokio::spawn(async move {
        scion_proto::runtime::metrics::shutdown_signal().await;
        let _ = end_tx.send(());
    });
    if let Some(metrics_listen) = cli.metrics_listen {
        let address = metrics_listen.to_string();
        let checked =
            scion_proto::runtime::metrics::loopback_or_allow(metrics_listen, cli.insecure_listen);
        anyhow::ensure!(checked.is_ok(), "{}", checked.unwrap_err());
        let metrics_listener = tokio::net::TcpListener::bind(metrics_listen)
            .await
            .with_context(|| format!("cannot listen on {address}"))?;
        let metrics_addr = metrics_listener.local_addr()?;
        tracing::info!(listen = %metrics_addr, "serving metrics");
        let metrics = tier.metrics();
        let mut stopped = end_rx.clone();
        tokio::spawn(scion_proto::runtime::metrics::serve(
            metrics_listener,
            metrics,
            async move {
                let _ = stopped.changed().await;
            },
        ));
    }
    let listener = tokio::net::TcpListener::bind(cli.listen)
        .await
        .with_context(|| format!("cannot listen on {}", cli.listen))?;
    tracing::info!(listen = %cli.listen, upstream = %cli.upstream, "serving");
    let limits = Limits {
        connections: cli.max_connections,
        streams: cli.max_streams,
        stalled_bytes: cli.max_stalled_bytes,
    };
    Box::pin(tier.serve(
        &cli.token,
        listen::tcp(listener),
        limits,
        server_tls,
        async move {
            let mut end_rx = end_rx;
            let _ = end_rx.changed().await;
        },
    ))
    .await?;
    Ok(())
}

/// Every `every`, writes a snapshot of each of `repos` into `dir`: the
/// objects that went into its replies, the most recently used up to `bytes`.
/// A repo with none keeps its last snapshot.
async fn snapshots(
    tier: Tier<MemoryStore>,
    dir: PathBuf,
    repos: Vec<String>,
    bytes: usize,
    every: Duration,
) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        for repo in &repos {
            let objects = tier.store().snapshot(repo, bytes);
            if objects.is_empty() {
                continue;
            }
            let path = dir.join(snapshot::file_name(repo));
            let (count, started) = (objects.len(), Instant::now());
            let task = {
                let (path, repo) = (path.clone(), repo.clone());
                tokio::task::spawn_blocking(move || snapshot::write(&path, &repo, &objects))
            };
            match task.await {
                Ok(Ok(written)) => tracing::info!(
                    %repo,
                    objects = count,
                    bytes = written,
                    ms = started.elapsed().as_millis(),
                    "wrote a snapshot"
                ),
                Ok(Err(e)) => {
                    tracing::warn!(%repo, path = %path.display(), error = %e, "cannot write a snapshot");
                }
                Err(e) => tracing::warn!(%repo, error = %e, "cannot write a snapshot"),
            }
        }
    }
}

/// The keys that a ticket of a call must verify against: at most two, the
/// second one from a rotation.
fn ticket_keys(hex: &[String]) -> anyhow::Result<Vec<ed25519_dalek::VerifyingKey>> {
    anyhow::ensure!(hex.len() <= 2, "give at most two --ticket-key");
    hex.iter()
        .map(|key| {
            let bytes = scion_tier::key_from_hex(key)
                .ok_or_else(|| anyhow::anyhow!("--ticket-key must be 64 hex characters"))?;
            ed25519_dalek::VerifyingKey::from_bytes(&bytes)
                .map_err(|why| anyhow::anyhow!("bad --ticket-key: {why}"))
        })
        .collect()
}
