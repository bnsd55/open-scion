//! `scion-server`: serve the Scion API, import a git repository, or derive
//! derived data ahead of reads.

use std::io::Write as _;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use scion_proto::listen::{self, Limits};
use scion_proto::tls::ServerTls;
use scion_server::derived::{self, Kind};
use scion_server::graph::CommitGraph;
use scion_server::history::PathHistory;
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
        /// Bearer token that clients must send. Its principal, `default`,
        /// may read and write every repo. Give `--token` or `--access`, not
        /// both.
        #[arg(long, env = "SCION_TOKEN", hide_env_values = true)]
        token: Option<String>,
        /// Access file: tokens with principals, and which repos and paths
        /// each principal may read and write. Give `--token` or `--access`,
        /// not both.
        #[arg(long, value_name = "FILE")]
        access: Option<PathBuf>,
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
    },
    /// Print the BLAKE3 digest of a token, as a `token` line of an access
    /// file wants it. The file holds digests, never tokens.
    HashToken {
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
    // `hash-token` needs no data directory. An import does not land, so it
    // can run beside the server. `derive` removes rows of older versions of a
    // kind, which a running server of an older build may read, so it opens as
    // the server.
    let open = match cli.command {
        Command::Import { .. } => Store::open_shared,
        Command::Serve { .. } | Command::Derive { .. } => Store::open,
        Command::HashToken { .. } => return hash_token(),
        Command::TicketKey { .. } => return write_ticket_key(),
    };
    let store = open(&cli.data)
        .with_context(|| format!("cannot open data directory {}", cli.data.display()))?;
    match cli.command {
        Command::Serve {
            listen,
            token,
            access,
            insecure_listen,
            tls_cert,
            tls_key,
            tls_client_ca,
            max_connections,
            max_streams,
            max_stalled_bytes,
            ticket_key,
            ticket_key_old,
        } => {
            let server_tls = server_tls(
                tls_cert.as_deref(),
                tls_key.as_deref(),
                tls_client_ca.as_deref(),
            )?;
            let tickets = load_tickets(ticket_key.as_deref(), ticket_key_old.as_deref())?;
            serve(
                store,
                listen,
                load_access(token, access)?,
                insecure_listen,
                server_tls,
                tickets,
                Limits {
                    connections: max_connections,
                    streams: max_streams,
                    stalled_bytes: max_stalled_bytes,
                },
            )
            .await?;
        }
        Command::HashToken { .. } | Command::TicketKey { .. } => {
            unreachable!("handled before the store opens")
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

/// Serves the gRPC API on `listen`, with the TLS files of the flags, until
/// the caller stops it.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn serve(
    store: Store,
    listen: SocketAddr,
    access: scion_server::access::Access,
    insecure_listen: bool,
    server_tls: Option<ServerTls>,
    tickets: Option<Arc<scion_server::tickets::Keys>>,
    limits: Limits,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        listen.ip().is_loopback() || insecure_listen || server_tls.is_some(),
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
        limits,
        server_tls,
        tickets,
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
) -> anyhow::Result<scion_server::access::Access> {
    match (token, file) {
        (Some(_), Some(_)) => anyhow::bail!("give --token or --access, not both"),
        (Some(token), None) => {
            anyhow::ensure!(!token.is_empty(), "--token must not be empty");
            Ok(scion_server::access::Access::single(&token))
        }
        (None, Some(file)) => {
            let text = std::fs::read_to_string(&file)
                .with_context(|| format!("cannot read {}", file.display()))?;
            scion_server::access::Access::parse(&text)
                .map_err(|why| anyhow::anyhow!("bad access file {}: {why}", file.display()))
        }
        (None, None) => anyhow::bail!("give --token or --access"),
    }
}

/// The ticket keys of `serve`: a signing key, and the public key that a
/// rotation keeps alive. Both files hold 64 hex characters.
fn load_tickets(
    seed: Option<&Path>,
    old: Option<&Path>,
) -> anyhow::Result<Option<Arc<scion_server::tickets::Keys>>> {
    let Some(seed) = seed else {
        anyhow::ensure!(old.is_none(), "--ticket-key-old needs --ticket-key");
        return Ok(None);
    };
    let keys = scion_server::tickets::Keys::load(
        &read_hex(seed)?,
        &match old {
            Some(file) => vec![read_hex(file)?],
            None => Vec::new(),
        },
    )
    .map_err(|why| anyhow::anyhow!("{why}"))?;
    Ok(Some(Arc::new(keys)))
}

/// The file's trimmed text, as a key wants it. The file must be readable
/// by its owner alone: a seed that group or others can read mints
/// permissions to anyone who can read it.
fn read_hex(file: &Path) -> anyhow::Result<String> {
    let meta = std::fs::symlink_metadata(file)
        .with_context(|| format!("cannot read {}", file.display()))?;
    anyhow::ensure!(
        meta.is_file(),
        "{} is not a file (a symlink is refused too)",
        file.display()
    );
    anyhow::ensure!(
        meta.mode().trailing_zeros() >= 6,
        "{} can be read by group or others (fix: chmod 600 {})",
        file.display(),
        file.display()
    );
    let me = rustix::process::geteuid().as_raw();
    scion_proto::auth::check_ancestors(file, me)
        .with_context(|| format!("{} sits in a directory that is not private", file.display()))?;
    let text =
        std::fs::read_to_string(file).with_context(|| format!("cannot read {}", file.display()))?;
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
    let keys = scion_server::tickets::Keys::generate();
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
        blake3::Hash::from(scion_proto::auth::token_digest(&token)).to_hex()
    );
    Ok(())
}
