//! `scion-cache`: a regional cache tier. Serves the `Objects` calls from
//! memory and fetches misses from its upstream, the server.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::os::unix::fs::DirBuilderExt as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use clap::Parser;
use scion_proto::check;
use scion_proto::listen::{self, Limits};
use scion_tier::{CACHE_UPSTREAM_TIMEOUT, MemoryStore, Tier, snapshot};
use tokio::time::MissedTickBehavior;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about = "Regional cache for the Scion version control system")]
struct Cli {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:7879")]
    listen: SocketAddr,
    /// Allow a non-loopback address. Traffic, including the token, is not
    /// encrypted, so use this only on a trusted network.
    #[arg(long)]
    insecure_listen: bool,
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
    /// snapshots of repos that all its users may read.
    #[arg(long, value_name = "REPO", requires = "snapshot")]
    snapshot_repo: Vec<String>,
    /// Most object bytes in one snapshot.
    #[arg(long, default_value_t = 256 << 20)]
    snapshot_bytes: usize,
    /// Seconds between snapshots. The first comes one interval after start.
    #[arg(long, value_name = "SECS", default_value_t = 600,
          value_parser = clap::value_parser!(u64).range(1..))]
    snapshot_every: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();
    anyhow::ensure!(!cli.token.is_empty(), "--token must not be empty");
    // SHORTCUT: no TLS. Upgrade (issue #14): tonic TLS or mTLS before any
    // deployment beyond one machine.
    anyhow::ensure!(
        cli.listen.ip().is_loopback() || cli.insecure_listen,
        "{} is not a loopback address; traffic and the token would travel \
         unencrypted. Pass --insecure-listen to allow it.",
        cli.listen
    );
    for repo in &cli.snapshot_repo {
        check::name("--snapshot-repo", repo).map_err(anyhow::Error::msg)?;
    }
    let store = MemoryStore::new(cli.max_bytes).recording(&cli.snapshot_repo);
    let tier = Tier::new(
        store,
        &cli.upstream,
        &cli.upstream_token,
        CACHE_UPSTREAM_TIMEOUT,
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
    let listener = tokio::net::TcpListener::bind(cli.listen)
        .await
        .with_context(|| format!("cannot listen on {}", cli.listen))?;
    tracing::info!(listen = %cli.listen, upstream = %cli.upstream, "serving");
    let limits = Limits {
        connections: cli.max_connections,
        streams: cli.max_streams,
    };
    tier.serve(&cli.token, listen::tcp(listener), limits, async {
        tokio::signal::ctrl_c().await.ok();
    })
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
