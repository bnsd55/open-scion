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
}

fn parse_put(spec: &str) -> Result<(String, PathBuf), String> {
    let (path, file) = spec
        .split_once('=')
        .ok_or_else(|| format!("{spec:?}: expected PATH=FILE"))?;
    Ok((path.to_owned(), PathBuf::from(file)))
}

/// Uploads the files, creates the changeset, and prints its ID.
pub async fn commit(ctx: &mut Ctx, args: CommitArgs) -> Result<()> {
    let (name, email) = parse_author(&args.author)?;
    // Each path and the local file to put there, or `None` to delete it.
    let mut edits = BTreeMap::new();
    let puts = args.puts.iter().map(|(path, file)| (path, Some(file)));
    for (path, file) in puts.chain(args.deletes.iter().map(|path| (path, None))) {
        let path = RepoPath::new(path.as_str()).with_context(|| format!("bad path {path:?}"))?;
        ensure!(
            !edits.contains_key(&path),
            "{path:?} is given more than once"
        );
        edits.insert(path, file);
    }

    let parent = ctx.resolve(&args.base).await?;
    let mut changes = BTreeMap::new();
    for (path, file) in edits {
        let state = match file {
            None => None,
            Some(file) => {
                // SHORTCUT: reads each file whole, so memory caps the file
                // size (the server caps it at 100 MiB). Upgrade: stream
                // chunks from the file.
                let data = std::fs::read(file)
                    .with_context(|| format!("cannot read {}", file.display()))?;
                let content = ctx
                    .put_blob(data)
                    .await
                    .with_context(|| format!("cannot upload {}", file.display()))?;
                Some(FileState::Regular {
                    content,
                    executable: args.executable,
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
