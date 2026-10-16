//! `scion-server`: serve the Scion API, import a git repository, or derive
//! derived data ahead of reads.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use scion_proto::listen::{self, Limits};
use scion_proto::tls::ServerTls;
use scion_server::derived::{self, Kind};
use scion_server::graph::CommitGraph;
use scion_server::import::{ImportOptions, import_git};
use scion_server::tree::RootTree;
use scion_server::{Error, Store, db, read, service};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about = "Server for the Scion version control system")]
struct Cli {
    /// Data directory. Created if missing.
    #[arg(long, env = "SCION_DATA", default_value = "scion-data", global = true)]
    data: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve the gRPC API.
    Serve {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:7878")]
        listen: SocketAddr,
        /// Bearer token that clients must send.
        #[arg(long, env = "SCION_TOKEN", hide_env_values = true)]
        token: String,
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
        /// `root_tree`, every ancestor for `commit_graph`.
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();
    // An import does not land, so it can run beside the server. `derive`
    // removes rows of older versions of a kind, which a running server of an
    // older build may read, so it opens as the server.
    let open = match cli.command {
        Command::Import { .. } => Store::open_shared,
        Command::Serve { .. } | Command::Derive { .. } => Store::open,
    };
    let store = open(&cli.data)
        .with_context(|| format!("cannot open data directory {}", cli.data.display()))?;
    match cli.command {
        Command::Serve {
            listen,
            token,
            insecure_listen,
            tls_cert,
            tls_key,
            tls_client_ca,
            max_connections,
            max_streams,
        } => {
            anyhow::ensure!(!token.is_empty(), "--token must not be empty");
            let server_tls =
                server_tls(tls_cert.as_deref(), tls_key.as_deref(), tls_client_ca.as_deref())?;
            anyhow::ensure!(
                listen.ip().is_loopback() || insecure_listen || server_tls.is_some(),
                "{listen} is not a loopback address; without TLS the traffic and the \
                 token would travel unencrypted. Pass --tls-cert and --tls-key, or \
                 --insecure-listen to allow it."
            );
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .with_context(|| format!("cannot listen on {listen}"))?;
            tracing::info!(%listen, "serving");
            let limits = Limits {
                connections: max_connections,
                streams: max_streams,
            };
            Box::pin(service::serve(
                store,
                listener,
                &token,
                limits,
                server_tls.as_ref(),
                async {
                    tokio::signal::ctrl_c().await.ok();
                },
            ))
            .await?;
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
                let tip = read::resolve_ref(&store, &repo, &target_ref)?;
                let repo_id = store.read(|c| db::repo_id(c, &repo))?;
                let name = match kind {
                    KindName::RootTree => {
                        derived::backfill::<RootTree>(&store, repo_id, tip)?;
                        RootTree::NAME
                    }
                    KindName::CommitGraph => {
                        derived::backfill::<CommitGraph>(&store, repo_id, tip)?;
                        CommitGraph::NAME
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
