//! `scn`: command-line client for a Scion server.

mod client;
mod cmd;
mod materialize;

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use scion_core::RepoPath;

use crate::client::Ctx;

#[derive(Parser)]
#[command(version, about = "Client for the Scion version control system")]
struct Cli {
    /// Server address.
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
        #[arg(long, value_parser = cmd::checkout::parse_dir)]
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
    Checkout(cmd::checkout::CheckoutArgs),
    /// Create a changeset on the server. Moves no ref; use `scn land` next.
    Commit(cmd::commit::CommitArgs),
    /// Rebase a changeset onto a ref and move the ref.
    Land(cmd::land::LandArgs),
    /// Many agents commit and land at the same time; print throughput.
    Bench(cmd::bench::BenchArgs),
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("scn: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let mut ctx = Ctx::connect(&cli.server, &cli.token, cli.repo, cli.json).await?;
    match cli.command {
        Command::Cat { spec } => cmd::read::cat(&mut ctx, &spec).await,
        Command::Ls { spec } => cmd::read::ls(&mut ctx, &spec).await,
        Command::Log { rev, path, limit } => {
            cmd::read::log(&mut ctx, &rev, path.as_ref(), limit).await
        }
        Command::Blame { spec } => cmd::blame::blame(&mut ctx, &spec).await,
        Command::Show { rev } => cmd::read::show(&mut ctx, &rev).await,
        Command::Checkout(args) => cmd::checkout::checkout(&mut ctx, args).await,
        Command::Commit(args) => cmd::commit::commit(&mut ctx, args).await,
        Command::Land(args) => cmd::land::land(&mut ctx, args).await,
        Command::Bench(args) => cmd::bench::bench(&ctx, args).await,
    }
}
