//! `scion-cache`: a regional cache tier. Serves the `Objects` calls from
//! memory and fetches misses from its upstream, the server.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;

use anyhow::Context as _;
use clap::Parser;
use scion_proto::listen::{self, Limits};
use scion_proto::tls::{ClientTls, ServerTls};
use scion_tier::{CACHE_UPSTREAM_TIMEOUT, MemoryStore, Tier};
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
    let tier = Tier::new(
        MemoryStore::new(cli.max_bytes),
        &cli.upstream,
        &cli.upstream_token,
        CACHE_UPSTREAM_TIMEOUT,
        Some(&client_tls),
    )?;
    let listener = tokio::net::TcpListener::bind(cli.listen)
        .await
        .with_context(|| format!("cannot listen on {}", cli.listen))?;
    tracing::info!(listen = %cli.listen, upstream = %cli.upstream, "serving");
    let limits = Limits {
        connections: cli.max_connections,
        streams: cli.max_streams,
    };
    Box::pin(tier.serve(
        &cli.token,
        listen::tcp(listener),
        limits,
        server_tls.as_ref(),
        async {
            tokio::signal::ctrl_c().await.ok();
        },
    ))
    .await?;
    Ok(())
}
