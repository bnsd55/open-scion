//! `cat`, `ls`, `log`, `show`.

use std::io::Write as _;

use anyhow::{Context, Result, bail};
use scion_core::{
    Changeset, ChangesetId, Entry, FileEntry, FileState, IdentityTime, RepoPath, Tree,
};
use scion_proto::v1::{
    GetChangesetRequest, ListDirRequest, LogRequest, ReadFileRequest, ReadFileResponse,
};
use serde_json::{Value, json};
use tonic::{Status, Streaming};

use crate::client::{Ctx, MAX_JOINED_REPLY_BYTES, id_from, lossy, split_spec};

/// Prints a file's raw bytes. The content is printed as it is, also with
/// `--json`.
pub async fn cat(ctx: &mut Ctx, spec: &str) -> Result<()> {
    let (rev, path) = split_spec(spec)?;
    let path = path.with_context(|| format!("{spec:?}: expected REV:PATH"))?;
    let changeset_id = ctx.resolve(rev).await?;
    let mut stream = read_stream(ctx, changeset_id, &path)
        .await
        .with_context(|| format!("cannot read {spec}"))?;
    let mut stdout = std::io::stdout();
    while let Some(chunk) = stream
        .message()
        .await
        .with_context(|| format!("cannot read {spec}"))?
    {
        stdout.write_all(&chunk.data)?;
    }
    stdout.flush()?;
    Ok(())
}

/// Starts streaming the content of the file at `path` in a changeset.
pub(crate) async fn read_stream(
    ctx: &mut Ctx,
    changeset_id: ChangesetId,
    path: &RepoPath,
) -> Result<Streaming<ReadFileResponse>, Status> {
    let reply = ctx
        .client
        .read_file(ReadFileRequest {
            repo: ctx.repo.clone(),
            changeset_id: changeset_id.as_bytes().to_vec(),
            path: path.as_bytes().to_vec(),
        })
        .await?;
    Ok(reply.into_inner())
}

/// Joins the parts of one streamed reply into the bytes of its object.
/// Stops with an error once the joined bytes pass `limit`, so a broken or
/// hostile server cannot stream until the client runs out of memory.
async fn join_parts<S, T, F>(reply: &mut S, field: F, limit: usize) -> Result<Vec<u8>>
where
    S: tokio_stream::Stream<Item = Result<T, Status>> + Unpin,
    F: Fn(&T) -> &[u8],
{
    let mut bytes = Vec::new();
    while let Some(part) = tokio_stream::StreamExt::next(reply).await.transpose()? {
        bytes.extend_from_slice(field(&part));
        anyhow::ensure!(
            bytes.len() <= limit,
            "the server sent more than {limit} bytes for one object"
        );
    }
    Ok(bytes)
}

/// Lists a directory, one entry per line: git-style mode, kind, name.
/// Directories end in `/`.
pub async fn ls(ctx: &mut Ctx, spec: &str) -> Result<()> {
    let (rev, path) = split_spec(spec)?;
    let changeset_id = ctx.resolve(rev).await?;
    let mut reply = ctx
        .client
        .list_dir(ListDirRequest {
            repo: ctx.repo.clone(),
            changeset_id: changeset_id.as_bytes().to_vec(),
            path: path.map(|p| p.as_bytes().to_vec()).unwrap_or_default(),
        })
        .await
        .with_context(|| format!("cannot list {spec}"))?
        .into_inner();
    let tree_bytes = join_parts(&mut reply, |part| &part.tree, MAX_JOINED_REPLY_BYTES)
        .await
        .with_context(|| format!("cannot list {spec}"))?;
    let tree = Tree::decode(&tree_bytes).context("server sent a bad tree")?;
    if ctx.json {
        let entries: Vec<Value> = tree
            .entries
            .iter()
            .map(|(name, entry)| {
                let mut value = entry_json(entry);
                value["name"] = lossy(name.as_bytes()).into();
                value
            })
            .collect();
        println!("{}", Value::from(entries));
        return Ok(());
    }
    for (name, entry) in &tree.entries {
        let (mode, kind) = describe(entry);
        let slash = if matches!(entry, Entry::Dir(_)) {
            "/"
        } else {
            ""
        };
        println!("{mode} {kind:<7} {}{slash}", lossy(name.as_bytes()));
    }
    Ok(())
}

/// Prints first-parent history, newest first: short ID, author, subject.
/// With a `path`, only the changesets that change it. Asks again from where
/// each reply stopped until it has `limit` changesets or the history ends.
pub async fn log(ctx: &mut Ctx, rev: &str, path: Option<&RepoPath>, limit: u32) -> Result<()> {
    let path = path.map(|p| p.as_bytes().to_vec()).unwrap_or_default();
    let mut next = Some(ctx.resolve(rev).await?);
    let mut entries = Vec::new();
    while let Some(from) = next.filter(|_| entries.len() < limit as usize) {
        let reply = ctx
            .client
            .log(LogRequest {
                repo: ctx.repo.clone(),
                changeset_id: from.as_bytes().to_vec(),
                limit: limit - u32::try_from(entries.len())?,
                path: path.clone(),
            })
            .await
            .with_context(|| format!("cannot read the log of {rev}"))?
            .into_inner();
        for entry in &reply.entries {
            let id = id_from(&entry.changeset_id)?;
            entries.push((id, decode_changeset(id, &entry.changeset)?));
        }
        next = opt_id(&reply.next_changeset_id)?;
    }
    if ctx.json {
        let entries: Vec<Value> = entries
            .iter()
            .map(|(id, cs)| changeset_json(*id, cs))
            .collect();
        println!("{}", Value::from(entries));
        return Ok(());
    }
    for (id, cs) in &entries {
        let subject = cs.message.split(|&b| b == b'\n').next().unwrap_or_default();
        println!(
            "{} {} {}",
            &id.to_string()[..12],
            lossy(&cs.author.name),
            lossy(subject)
        );
    }
    Ok(())
}

/// Prints one changeset: ID, parents, author, committer, message and the
/// changed paths.
pub async fn show(ctx: &mut Ctx, rev: &str) -> Result<()> {
    let id = ctx.resolve(rev).await?;
    let mut reply = ctx
        .client
        .get_changeset(GetChangesetRequest {
            repo: ctx.repo.clone(),
            changeset_id: id.as_bytes().to_vec(),
        })
        .await
        .with_context(|| format!("cannot read changeset {rev}"))?
        .into_inner();
    let cs_bytes = join_parts(&mut reply, |part| &part.changeset, MAX_JOINED_REPLY_BYTES)
        .await
        .with_context(|| format!("cannot read changeset {rev}"))?;
    let cs = decode_changeset(id, &cs_bytes)?;
    if ctx.json {
        let mut value = changeset_json(id, &cs);
        value["changes"] = cs
            .changes
            .iter()
            .map(|(path, state)| {
                let mut change = match state {
                    Some(state) => {
                        let mut put = entry_json(&Entry::File(FileEntry::new(*state, 0)));
                        put["op"] = "put".into();
                        put
                    }
                    None => json!({ "op": "delete" }),
                };
                change["path"] = lossy(path.as_bytes()).into();
                change
            })
            .collect();
        println!("{value}");
        return Ok(());
    }
    println!("changeset {id}");
    for parent in &cs.parents {
        println!("parent    {parent}");
    }
    println!("author    {}", identity(&cs.author));
    if let Some(committer) = &cs.committer {
        println!("committer {}", identity(committer));
    }
    println!();
    for line in lossy(&cs.message).lines() {
        println!("    {line}");
    }
    println!();
    for (path, state) in &cs.changes {
        let op = if state.is_some() { "put   " } else { "delete" };
        println!("{op} {}", lossy(path.as_bytes()));
    }
    Ok(())
}

/// An optional changeset ID from a reply: empty means `None`.
pub(crate) fn opt_id(bytes: &[u8]) -> Result<Option<ChangesetId>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        id_from(bytes).map(Some)
    }
}

/// Decodes a changeset and checks it against the ID it was asked for.
fn decode_changeset(id: ChangesetId, bytes: &[u8]) -> Result<Changeset> {
    let cs = Changeset::decode(bytes)
        .with_context(|| format!("server sent a bad changeset for {id}"))?;
    if cs.id() != id {
        bail!("server sent changeset {} for {id}", cs.id());
    }
    Ok(cs)
}

/// A git-style mode and a kind name.
fn describe(entry: &Entry) -> (&'static str, &'static str) {
    match entry {
        Entry::Dir(_) => ("040000", "dir"),
        Entry::File(file) => match file.state() {
            FileState::Regular { executable, .. } if executable => ("100755", "file"),
            FileState::Regular { .. } => ("100644", "file"),
            FileState::Symlink { .. } => ("120000", "symlink"),
            FileState::Gitlink { .. } => ("160000", "gitlink"),
        },
    }
}

/// `Name <email> unix-seconds +hhmm`, as in a git commit.
fn identity(who: &IdentityTime) -> String {
    let sign = if who.tz_offset_minutes < 0 { '-' } else { '+' };
    let minutes = who.tz_offset_minutes.unsigned_abs();
    format!(
        "{} <{}> {} {sign}{:02}{:02}",
        lossy(&who.name),
        lossy(&who.email),
        who.unix_secs,
        minutes / 60,
        minutes % 60
    )
}

pub(crate) fn identity_json(who: &IdentityTime) -> Value {
    json!({
        "name": lossy(&who.name),
        "email": lossy(&who.email),
        "unix_secs": who.unix_secs,
        "tz_offset_minutes": who.tz_offset_minutes,
    })
}

fn changeset_json(id: ChangesetId, cs: &Changeset) -> Value {
    json!({
        "id": id.to_string(),
        "parents": cs.parents.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "author": identity_json(&cs.author),
        "committer": cs.committer.as_ref().map(identity_json),
        "message": lossy(&cs.message),
    })
}

/// An entry as JSON: kind, ID, and the executable flag of a regular file.
pub(crate) fn entry_json(entry: &Entry) -> Value {
    let id = match entry {
        Entry::Dir(id) => id.to_string(),
        Entry::File(file) => match file.state() {
            FileState::Regular { content: blob, .. } | FileState::Symlink { target: blob } => {
                blob.to_string()
            }
            FileState::Gitlink { commit } => commit.to_string(),
        },
    };
    let mut value = json!({ "kind": describe(entry).1, "id": id });
    if let Entry::File(file) = entry
        && let FileState::Regular { executable, .. } = file.state()
    {
        value["executable"] = executable.into();
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_proto::v1::{GetChangesetResponse, ListDirResponse};

    /// A stream of `parts` parts of `part_len` bytes.
    fn parts_of(
        parts: usize,
        part_len: usize,
    ) -> impl tokio_stream::Stream<Item = Result<ListDirResponse, Status>> {
        tokio_stream::iter((0..parts).map(move |_| {
            Ok(ListDirResponse {
                tree: vec![b'x'; part_len],
            })
        }))
    }

    #[tokio::test]
    async fn a_reply_past_the_bound_ends_with_an_error() {
        let mut reply = parts_of(4, 10);
        let err = join_parts(&mut reply, |p| &p.tree, 25)
            .await
            .expect_err("over the bound");
        assert!(err.to_string().contains("more than 25 bytes"), "{err:?}");
    }

    #[tokio::test]
    async fn a_reply_at_the_bound_joins_whole() {
        let mut reply = parts_of(2, 10);
        let bytes = join_parts(&mut reply, |p| &p.tree, 20).await.unwrap();
        assert_eq!(bytes.len(), 20);
        let mut reply = tokio_stream::iter(Vec::<Result<GetChangesetResponse, Status>>::new());
        assert_eq!(
            join_parts(&mut reply, |p| &p.changeset, 20).await.unwrap(),
            Vec::<u8>::new()
        );
    }
}
