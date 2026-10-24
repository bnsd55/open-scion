#![forbid(unsafe_code)]
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
    /// Bearer token for the server. Pass it in `SCION_TOKEN`, not here:
    /// the command line is readable by every local user (`ps`), the
    /// environment of your own process is not.
    #[arg(long, value_name = "TOKEN")]
    token: Option<String>,
    /// File with the bearer token, read with the checks of a secret
    /// file: the owner alone may read it, and a symlink is refused.
    #[arg(long, conflicts_with = "token", value_name = "FILE")]
    token_file: Option<PathBuf>,
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
            let err: &(dyn std::error::Error + 'static) = e.as_ref();
            let text = chain_text(err);
            eprintln!("scn: {text}");
            if let Some(hint) = hint(&text) {
                eprintln!("check: {hint}");
            }
            ExitCode::from(ERROR_EXIT)
        }
    }
}

/// One advice line for the failures a first-time user hits: what the
/// error names, and what to check next. The order matters: a TLS
/// failure is also a transport error, so the certificate check comes
/// first.
fn hint(text: &str) -> Option<&'static str> {
    if text.contains("invalid peer certificate") {
        Some("--tls-ca must hold the CA (PEM) that signed the certificate of the server you trust")
    } else if text.contains("valid authentication credentials") || text.contains("wrong token") {
        Some("compare --token (or SCION_TOKEN) with the token the server was given")
    } else if text.contains("cannot connect") {
        Some("is a scion server or tier running at that address?")
    } else {
        None
    }
}

/// The text of one error and its causes, joined with `: `, each part once.
///
/// tonic's transport errors end their Display with their source's text,
/// and `{:#}` printed that source again: `tcp connect error` twice, `bad
/// URL, scheme is missing` twice. A cause whose text the message so far
/// already ends with adds nothing new, so print only the rest of it.
fn chain_text(error: &(dyn std::error::Error + 'static)) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(err) = source {
        let text = err.to_string();
        let (have, add) = (out.as_bytes(), text.as_bytes());
        // The longest suffix of `out` that is a prefix of `text`.
        let mut overlap = 0;
        for k in (1..=have.len().min(add.len())).rev() {
            if have[have.len() - k..] == add[..k] {
                overlap = k;
                break;
            }
        }
        let rest = &text[overlap..];
        if overlap == 0 {
            out.push_str(": ");
            out.push_str(&text);
        } else if rest.starts_with(": ") || rest.is_empty() {
            out.push_str(rest);
        } else {
            // The overlap ended inside a word. Print the whole cause, so
            // the parts stay whole.
            out.push_str(": ");
            out.push_str(&text);
        }
        source = err.source();
    }
    out
}

async fn run(cli: Cli) -> anyhow::Result<ExitCode> {
    // A build without the writable-mount feature has no write path at
    // all. The refusal fires before anything connects, so it names the
    // build, not the syntax and not the server.
    #[cfg(not(feature = "unstable-writable-mount"))]
    match &cli.command {
        Command::Mount(args) if args.writable_requested() => anyhow::bail!(
            "this build has no writable mount: it was built without the \
             unstable-writable-mount feature"
        ),
        Command::Commit(args) if args.from_mount() => anyhow::bail!(
            "this build has no writable mount: it was built without the \
             unstable-writable-mount feature"
        ),
        _ => {}
    }
    // The token's source: the checked file, the env, or the argv form,
    // which warns that the process list shows it.
    let token = match (&cli.token, &cli.token_file) {
        (Some(_), Some(_)) => unreachable!("clap refuses --token with --token-file"),
        (Some(_), None) => {
            eprintln!(
                "warning: --token put a secret on the command line, which ps and \
                 /proc/$$/cmdline show to every local account; use --token-file \
                 (or SCION_TOKEN) instead (the argv form is deprecated and goes \
                 away in a later release)"
            );
            cli.token.clone().expect("the argv form is given")
        }
        (None, Some(file)) => {
            let bytes = scion_proto::runtime::auth::read_secret_file(file)
                .map_err(|why| scion_proto::runtime::auth::checked_read_error(file, why))?;
            let text = String::from_utf8(bytes)
                .map_err(|_| anyhow::anyhow!("{} is not UTF-8", file.display()))?;
            let token = text.trim();
            anyhow::ensure!(!token.is_empty(), "{} holds no token", file.display());
            token.to_owned()
        }
        (None, None) => {
            let from_env = std::env::var("SCION_TOKEN").unwrap_or_default();
            anyhow::ensure!(
                !from_env.is_empty(),
                "give SCION_TOKEN, --token-file or --token"
            );
            from_env
        }
    };
    let mut ctx = Ctx::connect(
        &cli.server,
        cli.objects.as_deref(),
        &token,
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

#[cfg(test)]
mod tests {
    use super::{chain_text, hint};

    /// A cause whose Display ends with its source's text, as tonic's
    /// transport errors have.
    #[derive(Debug)]
    struct Doubled(std::io::Error);

    impl Doubled {
        fn of(text: &str) -> Self {
            Self(std::io::Error::other(text))
        }
    }

    impl std::fmt::Display for Doubled {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "outer: {}", self.0)
        }
    }

    impl std::error::Error for Doubled {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    /// A cause the message does not name at all, as most errors have.
    #[derive(Debug)]
    struct Unrelated(std::io::Error);

    impl std::fmt::Display for Unrelated {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "outer text")
        }
    }

    impl std::error::Error for Unrelated {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn a_tls_failure_is_also_a_connect_failure_and_the_certificate_hint_wins() {
        let text = "cannot connect to http://127.0.0.1:7932: transport error: tcp connect error: invalid peer certificate: UnknownIssuer";
        let got = hint(text).unwrap();
        assert!(got.contains("--tls-ca"), "{got}");
    }

    #[test]
    fn a_cause_the_message_already_names_prints_once() {
        let err = Doubled::of("scheme is missing");
        assert_eq!(chain_text(&err), "outer: scheme is missing");
    }

    #[test]
    fn a_cause_that_extends_the_last_part_prints_its_rest() {
        let err = Doubled::of("tcp connect error: refused");
        assert_eq!(chain_text(&err), "outer: tcp connect error: refused");
    }

    #[test]
    fn a_cause_that_names_something_new_prints_whole() {
        let err = Unrelated(std::io::Error::other("something else"));
        assert_eq!(chain_text(&err), "outer text: something else");
    }

    #[tokio::test]
    async fn a_real_bad_url_prints_its_parts_once() {
        // `Endpoint::from_shared` takes it; the connect rejects a URL
        // with no scheme, as `scn --server no-scheme` sees.
        let err = tonic::transport::Endpoint::from_shared(String::from("no-scheme"))
            .unwrap()
            .connect()
            .await
            .unwrap_err();
        let err: &(dyn std::error::Error + 'static) = &err;
        let text = chain_text(err);
        assert_eq!(text.matches("scheme is missing").count(), 1, "{text}");
    }

    #[tokio::test]
    async fn a_real_refused_connection_prints_its_parts_once() {
        let err = tonic::transport::Endpoint::from_shared(String::from("http://127.0.0.1:1"))
            .unwrap()
            .connect()
            .await
            .unwrap_err();
        let err: &(dyn std::error::Error + 'static) = &err;
        let text = chain_text(err);
        assert_eq!(text.matches("tcp connect error").count(), 1, "{text}");
        assert!(text.contains("Connection refused"), "{text}");
    }
}
