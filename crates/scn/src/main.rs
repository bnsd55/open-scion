//! `scn`: command-line client for a Scion server.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use scion_core::RepoPath;

use open_scion::client::Ctx;

#[derive(Parser)]
#[command(version, about = "Client for the Scion version control system")]
struct Cli {
    /// Server address: `http://HOST:PORT`, or `unix:///PATH` for a
    /// scion-daemon on this node.
    #[arg(
        long,
        env = "SCION_SERVER",
        default_value = "http://127.0.0.1:7878",
        global = true
    )]
    server: String,
    /// Bearer token for the server.
    #[arg(long, env = "SCION_TOKEN", hide_env_values = true)]
    token: String,
    /// Address of a scion-cache or scion-daemon that serves `GetObjects`
    /// with a ticket: `http://HOST:PORT` or `unix:///PATH`. Without it,
    /// object reads go to `--server`, which also has them.
    #[arg(long, env = "SCION_OBJECTS", global = true)]
    objects: Option<String>,
    /// Trust this CA (PEM) for an https server.
    #[arg(long, env = "SCION_TLS_CA", global = true, value_name = "FILE")]
    tls_ca: Option<PathBuf>,
    /// Repository name.
    #[arg(long, env = "SCION_REPO", default_value = "main", global = true)]
    repo: String,
    /// Print machine-readable JSON.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print a file: `scn cat main:src/lib.rs`.
    Cat {
        /// `REV:PATH`. REV is a ref name or a changeset ID.
        spec: String,
    },
    /// List a directory: `scn ls main:src`, or `scn ls main` for the root.
    Ls {
        /// `REV[:DIR]`.
        spec: String,
    },
    /// Show first-parent history, newest first.
    Log {
        /// Ref name or changeset ID.
        #[arg(default_value = "main")]
        rev: String,
        /// Show only the changesets that change this file, or anything
        /// below this directory.
        #[arg(long, value_parser = open_scion::client::parse_repo_path)]
        path: Option<RepoPath>,
        /// How many changesets to show.
        #[arg(short = 'n', long, default_value_t = 10)]
        limit: u32,
    },
    /// Show the changeset that last changed each line of a file, on
    /// first-parent history: `scn blame main:src/lib.rs`.
    Blame {
        /// `REV:PATH`.
        spec: String,
    },
    /// Show one changeset and its changed paths.
    Show {
        /// Ref name or changeset ID.
        rev: String,
    },
    /// Write the files of a changeset into a new directory:
    /// `scn checkout main --sparse src --into ./work`.
    Checkout(open_scion::cmd::checkout::CheckoutArgs),
    /// Serve a ref as a filesystem that reads files on demand:
    /// `scn mount ./view main`.
    Mount(open_scion::cmd::mount::MountArgs),
    /// Stop a mount of `scn mount`.
    Unmount(open_scion::cmd::mount::UnmountArgs),
    /// Create a changeset on the server. Moves no ref; use `scn land` next.
    Commit(open_scion::cmd::commit::CommitArgs),
    /// Rebase a changeset onto a ref and move the ref.
    Land(open_scion::cmd::land::LandArgs),
    /// Many agents commit and land at the same time; print throughput.
    Bench(open_scion::cmd::bench::BenchArgs),
    /// Print the best common ancestors of two revisions, over all parents,
    /// as `git merge-base --all`.
    MergeBase {
        /// Ref name or changeset ID.
        a: String,
        /// Ref name or changeset ID.
        b: String,
    },
    /// Exit 0 if ANCESTOR is DESCENDANT or one of its ancestors, 1 if not.
    IsAncestor {
        /// Ref name or changeset ID.
        ancestor: String,
        /// Ref name or changeset ID.
        descendant: String,
    },
    /// Print the paths whose file differs between two revisions, as
    /// `git diff --name-status`. A rename prints as `R`, a tab, and
    /// `old -> new`.
    Diff {
        /// The old revision: a ref name or a changeset ID.
        old: String,
        /// The new revision.
        new: String,
        /// Only this file, or the files below this directory.
        #[arg(long, value_parser = open_scion::client::parse_repo_path)]
        path: Option<RepoPath>,
    },
}

/// The exit code of an error. 1 is the "no" of `scn is-ancestor`, so an
/// error is 2, as for a bad command line.
const ERROR_EXIT: u8 = 2;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("scn: {e:#}");
            ExitCode::from(ERROR_EXIT)
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<ExitCode> {
    let mut ctx = Ctx::connect(
        &cli.server,
        cli.objects.as_deref(),
        &cli.token,
        cli.tls_ca.as_deref(),
        cli.repo,
        cli.json,
    )
    .await?;
    let done = match cli.command {
        Command::Cat { spec } => open_scion::cmd::read::cat(&mut ctx, &spec).await,
        Command::Ls { spec } => open_scion::cmd::read::ls(&mut ctx, &spec).await,
        Command::Log { rev, path, limit } => {
            open_scion::cmd::read::log(&mut ctx, &rev, path.as_ref(), limit).await
        }
        Command::Blame { spec } => open_scion::cmd::blame::blame(&mut ctx, &spec).await,
        Command::Show { rev } => open_scion::cmd::read::show(&mut ctx, &rev).await,
        Command::Checkout(args) => open_scion::cmd::checkout::checkout(&mut ctx, args).await,
        Command::Mount(args) => open_scion::cmd::mount::mount(&mut ctx, args).await,
        Command::Unmount(args) => open_scion::cmd::mount::unmount(&mut ctx, args).await,
        Command::Commit(args) => open_scion::cmd::commit::commit(&mut ctx, args).await,
        Command::Land(args) => open_scion::cmd::land::land(&mut ctx, args).await,
        Command::Bench(args) => open_scion::cmd::bench::bench(&ctx, args).await,
        Command::MergeBase { a, b } => open_scion::cmd::query::merge_base(&mut ctx, &a, &b).await,
        Command::IsAncestor {
            ancestor,
            descendant,
        } => {
            let yes = open_scion::cmd::query::is_ancestor(&mut ctx, &ancestor, &descendant).await?;
            // 0 for yes, 1 for no.
            return Ok(ExitCode::from(u8::from(!yes)));
        }
        Command::Diff { old, new, path } => {
            open_scion::cmd::query::diff(&mut ctx, &old, &new, path.as_ref()).await
        }
    };
    done.map(|()| ExitCode::SUCCESS)
}
