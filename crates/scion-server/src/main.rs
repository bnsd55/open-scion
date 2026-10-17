//! The `scion-server` binary: `import`, `serve` and `derive`.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use scion_proto::listen::{self, Limits};
use scion_proto::tls::ServerTls;
use scion_server::derived::{self, Kind};
use scion_server::graph::CommitGraph;
use scion_server::history::PathHistory;
use scion_server::import::{ImportOptions, import_git};
use scion_server::meta::MetaTx as _;
use scion_server::quotas::Quotas;
use scion_server::tree::RootTree;
use scion_server::{Error, Store, fsck, read, service};
use tracing_subscriber::EnvFilter;

/// The scion server: import, serve and derive.
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The data directory. Holds the SQLite database and the objects.
    #[arg(long, env = "SCION_DATA", default_value = "scion-data")]
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
        max_principal_requests: Option<usize>,
        /// Most upload bytes one principal may hold in flight. With
        /// `--access` the default is 536870912 (512 MiB); with `--token`
        /// the check is off.
        #[arg(long, value_name = "BYTES")]
        max_principal_upload_bytes: Option<usize>,
        /// Most pending lands one principal may hold at once. With
        /// `--access` the default is 64; with `--token` the check is off.
        #[arg(long)]
        max_principal_pending_lands: Option<usize>,
        /// A land that has been pending for longer than this many seconds is
        /// rejected by the sweep, so a restart cannot leave one pending
        /// forever. The default is 86400 (24 hours).
        #[arg(long, default_value_t = 24 * 60 * 60)]
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
        /// Most bytes that the stalled replies of one connection may hold. A
        /// connection over it is closed with them.
        #[arg(long, default_value_t = listen::DEFAULT_STALLED_BYTES)]
        max_stalled_bytes: NonZeroU64,
    },
    /// Print the BLAKE3 digest of a token, as a `token` line of an access
    /// file wants it. The file holds digests, never tokens.
    HashToken {
        #[arg(long, env = "SCION_TOKEN", hide_env_values = true)]
        token: String,
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
fn fsck_command(store: &Store) -> anyhow::Result<()> {
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

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();
    // `hash-token` needs no data directory. An import does not land, so it
    // can run beside the server. `derive` removes rows of older versions of a
    // kind, which a running server of an older build may read, so it opens as
    // the server.
    let open = match cli.command {
        Command::Import { .. } | Command::Fsck => Store::open_shared,
        Command::Serve { .. } | Command::Derive { .. } => Store::open,
        Command::HashToken { .. } => return hash_token(),
    };
    let store = open(&cli.data)
        .with_context(|| format!("cannot open data directory {}", cli.data.display()))?;
    match cli.command {
        Command::Serve {
            listen,
            token,
            access,
            max_principal_requests,
            max_principal_upload_bytes,
            max_principal_pending_lands,
            land_stale_secs,
            insecure_listen,
            tls_cert,
            tls_key,
            tls_client_ca,
            max_connections,
            max_streams,
            max_stalled_bytes,
        } => {
            // The second reply says whether the mode is one token: with one
            // principal, a per-principal cap is a global one, so the quotas
            // are off unless a flag sets one.
            let (access, single) = load_access(token, access)?;
            let caps = caps(
                single,
                max_principal_requests,
                max_principal_upload_bytes,
                max_principal_pending_lands,
            );
            let server_tls = server_tls(
                tls_cert.as_deref(),
                tls_key.as_deref(),
                tls_client_ca.as_deref(),
            )?;
            serve(
                store,
                listen,
                access,
                ServeConfig {
                    insecure_listen,
                    quotas: caps.quotas,
                    pending_lands: caps.pending_lands,
                    land_stale_secs,
                    server_tls,
                    max_connections,
                    max_streams,
                    max_stalled_bytes,
                },
            )
            .await?;
        }
        Command::HashToken { .. } => unreachable!("handled before the store opens"),
        Command::Fsck => return fsck_command(&store),
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
                let repo_id = store.read(|c| c.repo_id(&repo))?;
                let name = match kind {
                    KindName::RootTree => {
                        derived::backfill::<RootTree>(&store, repo_id, tip)?;
                        RootTree::NAME
                    }
                    KindName::CommitGraph => {
                        derived::backfill::<CommitGraph>(&store, repo_id, tip)?;
                        CommitGraph::NAME
                    }
                    KindName::PathHistory => {
                        derived::backfill::<PathHistory>(&store, repo_id, tip)?;
                        PathHistory::NAME
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
    requests: Option<usize>,
    upload_bytes: Option<usize>,
    pending_lands: Option<usize>,
) -> Caps {
    let with_default = |flag: Option<usize>, default: usize| {
        if single {
            flag
        } else {
            Some(flag.unwrap_or(default))
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
    insecure_listen: bool,
    quotas: Quotas,
    pending_lands: Option<usize>,
    land_stale_secs: i64,
    server_tls: Option<ServerTls>,
    max_connections: NonZeroUsize,
    max_streams: NonZeroU32,
    max_stalled_bytes: NonZeroU64,
}

/// Serves the gRPC API on `listen`, with the TLS files of the flags, until
/// the caller stops it.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn serve(
    store: Store,
    listen: SocketAddr,
    access: scion_server::access::Access,
    config: ServeConfig,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        listen.ip().is_loopback() || config.insecure_listen || config.server_tls.is_some(),
        "{listen} is not a loopback address; without TLS the traffic and the \
         token would travel unencrypted. Pass --tls-cert and --tls-key, or \
         --insecure-listen to allow it."
    );
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot listen on {listen}"))?;
    // The bound address, so port 0 shows the port that the OS chose.
    let listen = listener.local_addr()?;
    tracing::info!(%listen, "serving");

    Box::pin(service::serve(
        store,
        listener,
        access,
        service::ServeOptions {
            limits: Limits {
                connections: config.max_connections,
                streams: config.max_streams,
                stalled_bytes: config.max_stalled_bytes,
            },
            quotas: config.quotas,
            pending_lands: config.pending_lands,
            land_stale_secs: config.land_stale_secs,
            tls: config.server_tls,
        },
        async {
            tokio::signal::ctrl_c().await.ok();
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
) -> anyhow::Result<(scion_server::access::Access, bool)> {
    match (token, file) {
        (Some(_), Some(_)) => anyhow::bail!("give --token or --access, not both"),
        (Some(token), None) => {
            anyhow::ensure!(!token.is_empty(), "--token must not be empty");
            Ok((scion_server::access::Access::single(&token), true))
        }
        (None, Some(file)) => {
            let text = std::fs::read_to_string(&file)
                .with_context(|| format!("cannot read {}", file.display()))?;
            let access = scion_server::access::Access::parse(&text)
                .map_err(|why| anyhow::anyhow!("bad access file {}: {why}", file.display()))?;
            Ok((access, false))
        }
        (None, None) => anyhow::bail!("give --token or --access"),
    }
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
        blake3::Hash::from(scion_proto::auth::token_digest(&token)).to_hex()
    );
    Ok(())
}
