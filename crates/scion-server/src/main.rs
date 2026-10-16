//! `scion-server`: serve the Scion API, import a git repository, or derive
//! derived data ahead of reads.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use scion_proto::listen::{self, Limits};
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
        /// Allow a non-loopback address. Traffic, including the token, is not
        /// encrypted, so use this only on a trusted network.
        #[arg(long)]
        insecure_listen: bool,
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
            max_connections,
            max_streams,
        } => {
            anyhow::ensure!(!token.is_empty(), "--token must not be empty");
            // SHORTCUT: no TLS. Upgrade: tonic TLS or mTLS before any
            // deployment beyond one machine.
            anyhow::ensure!(
                listen.ip().is_loopback() || insecure_listen,
                "{listen} is not a loopback address; traffic and the token would \
                 travel unencrypted. Pass --insecure-listen to allow it."
            );
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .with_context(|| format!("cannot listen on {listen}"))?;
            // The bound address, so port 0 shows the port that the OS chose.
            let listen = listener.local_addr()?;
            tracing::info!(%listen, "serving");
            let limits = Limits {
                connections: max_connections,
                streams: max_streams,
            };
            service::serve(store, listener, &token, limits, async {
                tokio::signal::ctrl_c().await.ok();
            })
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
