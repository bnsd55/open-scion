//! `scion-daemon`: the node tier. Stores objects on disk for every job on the
//! node, serves them over a Unix socket, and fetches misses from its upstream,
//! a `scion-cache`.

use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;

use anyhow::Context as _;
use clap::Parser;
use scion_proto::listen::{self, Limits};
use scion_tier::{DAEMON_UPSTREAM_TIMEOUT, DiskStore, Tier, bind_unix};
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();
    anyhow::ensure!(!cli.token.is_empty(), "--token must not be empty");
    let store = DiskStore::open(&cli.data, cli.max_bytes)
        .with_context(|| format!("cannot open data directory {}", cli.data.display()))?;
    let tier = Tier::new(
        store,
        &cli.upstream,
        &cli.upstream_token,
        DAEMON_UPSTREAM_TIMEOUT,
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
    tracing::info!(socket = %socket.display(), upstream = %cli.upstream, "serving");
    let limits = Limits {
        connections: cli.max_connections,
        streams: cli.max_streams,
    };
    tier.serve(
        &cli.token,
        UnixListenerStream::new(listener),
        limits,
        async {
            tokio::signal::ctrl_c().await.ok();
        },
    )
    .await?;
    std::fs::remove_file(&socket).ok();
    Ok(())
}
