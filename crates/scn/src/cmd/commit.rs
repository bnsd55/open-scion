//! `commit`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result, bail, ensure};
use scion_core::{Changeset, FileState, IdentityTime, RepoPath};

use crate::client::{Ctx, now_unix_secs};

/// Arguments of `scn commit`.
#[derive(clap::Args)]
pub struct CommitArgs {
    /// Base: ref name or changeset ID.
    #[arg(long, default_value = "main")]
    base: String,
    /// Commit message.
    #[arg(short, long)]
    message: String,
    /// `PATH=FILE`: put the content of local FILE at PATH. Repeatable.
    #[arg(long = "put", value_name = "PATH=FILE", value_parser = parse_put)]
    puts: Vec<(String, PathBuf)>,
    /// Mark the put files executable.
    #[arg(long)]
    executable: bool,
    /// PATH to delete. Repeatable.
    #[arg(long = "delete", value_name = "PATH")]
    deletes: Vec<String>,
    /// Author as `Name <email>`.
    #[arg(long, env = "SCION_AUTHOR", default_value = "scn <scn@localhost>")]
    author: String,
    /// Take the changes from the overlay of a `scn mount` on DIR, instead
    /// of --put and --delete.
    #[arg(long = "from-mount", value_name = "DIR", conflicts_with_all = ["puts", "deletes", "executable"])]
    from_mount: Option<PathBuf>,
}

fn parse_put(spec: &str) -> Result<(String, PathBuf), String> {
    let (path, file) = spec
        .split_once('=')
        .ok_or_else(|| format!("{spec:?}: expected PATH=FILE"))?;
    Ok((path.to_owned(), PathBuf::from(file)))
}

/// What to commit for one path: a file, a symlink, or a deletion.
enum Edit {
    /// The local file replaces the path, or adds it.
    Put { file: PathBuf, exec: bool },
    /// The symlink names these bytes.
    Link(Vec<u8>),
    /// The path of the base is deleted.
    Delete,
}

/// Uploads the files, creates the changeset, and prints its ID.
pub async fn commit(ctx: &mut Ctx, args: CommitArgs) -> Result<()> {
    let (name, email) = parse_author(&args.author)?;
    let edits = match &args.from_mount {
        None => arg_edits(&args)?,
        Some(dir) => {
            let record = crate::cmd::mount::session_for_dir(dir)
                .with_context(|| format!("cannot commit from the mount on {}", dir.display()))?;
            let mut edits = BTreeMap::new();
            for (path, change) in crate::cmd::mount::overlay_changes(&record)? {
                let edit = match change {
                    crate::vfs::Change::Put { exec } => {
                        let file = record
                            .overlay
                            .join(crate::vfs::CHANGES)
                            .join(String::from_utf8_lossy(path.as_bytes()).as_ref());
                        Edit::Put { file, exec }
                    }
                    crate::vfs::Change::PutLink(target) => Edit::Link(target),
                    crate::vfs::Change::Delete => Edit::Delete,
                };
                ensure!(!edits.contains_key(&path), "{path:?} changes twice");
                edits.insert(path, edit);
            }
            edits
        }
    };

    let parent = ctx.resolve(&args.base).await?;
    let mut changes = BTreeMap::new();
    for (path, edit) in edits {
        let state = match edit {
            Edit::Delete => None,
            Edit::Link(target) => {
                // SHORTCUT: reads each target whole, so memory caps the
                // length. Upgrade: stream chunks.
                let content = ctx
                    .put_blob(target)
                    .await
                    .context("cannot upload a symlink target")?;
                Some(FileState::Symlink { target: content })
            }
            Edit::Put { file, exec } => {
                // SHORTCUT: reads each file whole, so memory caps the file
                // size (the server caps it at 100 MiB). Upgrade: stream
                // chunks from the file.
                let data = std::fs::read(&file)
                    .with_context(|| format!("cannot read {}", file.display()))?;
                let content = ctx
                    .put_blob(data)
                    .await
                    .with_context(|| format!("cannot upload {}", file.display()))?;
                Some(FileState::Regular {
                    content,
                    executable: exec,
                })
            }
        };
        changes.insert(path, state);
    }
    let changeset = Changeset {
        parents: vec![parent],
        author: IdentityTime {
            name,
            email,
            unix_secs: now_unix_secs()?,
            // SHORTCUT: always UTC, since std cannot read the local time zone.
            // Upgrade: a time zone crate.
            tz_offset_minutes: 0,
        },
        committer: None,
        message: args.message.into_bytes(),
        changes,
    };
    let id = ctx.create_changeset(&changeset).await?;
    if ctx.json {
        println!("{}", serde_json::json!({ "changeset": id.to_string() }));
    } else {
        println!("{id}");
    }
    Ok(())
}

/// The edits of the command line: --put and --delete.
fn arg_edits(args: &CommitArgs) -> Result<BTreeMap<RepoPath, Edit>> {
    let mut edits = BTreeMap::new();
    let puts = args.puts.iter().map(|(path, file)| (path, Some(file)));
    for (path, file) in puts.chain(args.deletes.iter().map(|path| (path, None))) {
        let path = RepoPath::new(path.as_str()).with_context(|| format!("bad path {path:?}"))?;
        ensure!(
            !edits.contains_key(&path),
            "{path:?} is given more than once"
        );
        let edit = match file {
            None => Edit::Delete,
            Some(file) => Edit::Put {
                file: file.clone(),
                exec: args.executable,
            },
        };
        edits.insert(path, edit);
    }
    Ok(edits)
}

/// Splits `Name <email>` into name and email bytes.
fn parse_author(author: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let well_formed = |part: &str| !part.is_empty() && !part.contains(['<', '>', '\n']);
    match author
        .strip_suffix('>')
        .and_then(|rest| rest.split_once(" <"))
    {
        Some((name, email)) if well_formed(name) && well_formed(email) => {
            Ok((name.as_bytes().to_vec(), email.as_bytes().to_vec()))
        }
        _ => bail!("--author {author:?}: expected \"Name <email>\""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn author_splits_name_and_email() {
        let (name, email) = parse_author("Ada Lovelace <ada@example.com>").unwrap();
        assert_eq!(name, b"Ada Lovelace");
        assert_eq!(email, b"ada@example.com");
    }

    #[test]
    fn malformed_author_is_rejected() {
        for bad in [
            "Ada",
            "ada@example.com",
            "<ada@example.com>",
            "Ada <>",
            "Ada <ada@example.com",
            "Ada<ada@example.com>",
            "Ada <ada@example.com> x",
            "Ada <a <b>",
            "Ada\nEve <ada@example.com>",
        ] {
            assert!(parse_author(bad).is_err(), "{bad:?} was accepted");
        }
    }
}
