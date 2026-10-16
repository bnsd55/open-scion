//! `scion-cache`: a regional cache tier. Serves the `Objects` calls from
//! memory and fetches misses from its upstream, the server.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};

use anyhow::Context as _;
use clap::Parser;
use scion_proto::listen::{self, Limits};
use scion_tier::{CACHE_UPSTREAM_TIMEOUT, MemoryStore, Tier};
use tokio_stream::wrappers::TcpListenerStream;
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
    let tier = Tier::new(
        MemoryStore::new(cli.max_bytes),
        &cli.upstream,
        &cli.upstream_token,
        CACHE_UPSTREAM_TIMEOUT,
    )?;
    let listener = tokio::net::TcpListener::bind(cli.listen)
        .await
        .with_context(|| format!("cannot listen on {}", cli.listen))?;
    tracing::info!(listen = %cli.listen, upstream = %cli.upstream, "serving");
    let limits = Limits {
        connections: cli.max_connections,
        streams: cli.max_streams,
    };
    tier.serve(
        &cli.token,
        TcpListenerStream::new(listener),
        limits,
        async {
            tokio::signal::ctrl_c().await.ok();
        },
    )
    .await?;
    Ok(())
}
