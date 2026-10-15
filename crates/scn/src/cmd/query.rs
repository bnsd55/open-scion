//! `merge-base`, `is-ancestor`, `diff`.

use anyhow::{Context, Result, bail};
use scion_core::{ChangesetId, Entry, FileState, GitOid, RepoPath};
use scion_proto::v1::{self, DiffRequest, FileKind, IsAncestorRequest, MergeBaseRequest};
use serde_json::{Value, json};

use crate::client::{Ctx, id_from, lossy};
use crate::cmd::read::entry_json;

/// Prints the best common ancestors of two revisions, one per line, as
/// `git merge-base --all`. Usually one; criss-cross merges can give several.
/// Prints nothing when the histories share no changeset.
pub async fn merge_base(ctx: &mut Ctx, a: &str, b: &str) -> Result<()> {
    let a_id = ctx.resolve(a).await?;
    let b_id = ctx.resolve(b).await?;
    let reply = ctx
        .client
        .merge_base(MergeBaseRequest {
            repo: ctx.repo.clone(),
            a_changeset_id: a_id.as_bytes().to_vec(),
            b_changeset_id: b_id.as_bytes().to_vec(),
        })
        .await
        .with_context(|| format!("cannot find the merge base of {a} and {b}"))?
        .into_inner();
    let bases = reply
        .changeset_ids
        .iter()
        .map(|id| id_from::<ChangesetId>(id))
        .collect::<Result<Vec<_>>>()?;
    if ctx.json {
        let bases: Vec<String> = bases.iter().map(ToString::to_string).collect();
        println!("{}", Value::from(bases));
        return Ok(());
    }
    for id in bases {
        println!("{id}");
    }
    Ok(())
}

/// Whether `ancestor` is `descendant` or one of its ancestors. Prints
/// nothing, except with `--json`; the exit code gives the answer.
pub async fn is_ancestor(ctx: &mut Ctx, ancestor: &str, descendant: &str) -> Result<bool> {
    let ancestor_id = ctx.resolve(ancestor).await?;
    let descendant_id = ctx.resolve(descendant).await?;
    let reply = ctx
        .client
        .is_ancestor(IsAncestorRequest {
            repo: ctx.repo.clone(),
            ancestor_id: ancestor_id.as_bytes().to_vec(),
            descendant_id: descendant_id.as_bytes().to_vec(),
        })
        .await
        .with_context(|| format!("cannot check whether {ancestor} is an ancestor of {descendant}"))?
        .into_inner();
    if ctx.json {
        println!("{}", json!({ "is_ancestor": reply.is_ancestor }));
    }
    Ok(reply.is_ancestor)
}

/// Prints the paths whose file differs between two revisions, one per line:
/// a status letter as `git diff --name-status` (`A`dded, `D`eleted,
/// `M`odified, `T` for a kind change), a tab, the path. A reply that stops
/// at the server's bound names the last path it holds, so this asks again
/// after it until the diff is complete.
pub async fn diff(ctx: &mut Ctx, old: &str, new: &str, path: Option<&RepoPath>) -> Result<()> {
    let old_id = ctx.resolve(old).await?;
    let new_id = ctx.resolve(new).await?;
    let path = path.map(|p| p.as_bytes().to_vec()).unwrap_or_default();
    let mut entries = Vec::new();
    let mut after = Vec::new();
    loop {
        let reply = ctx
            .client
            .diff(DiffRequest {
                repo: ctx.repo.clone(),
                old_changeset_id: old_id.as_bytes().to_vec(),
                new_changeset_id: new_id.as_bytes().to_vec(),
                path: path.clone(),
                after: after.clone(),
            })
            .await
            .with_context(|| format!("cannot diff {old} and {new}"))?
            .into_inner();
        for entry in reply.entries {
            entries.push((entry.path, file_state(entry.old)?, file_state(entry.new)?));
        }
        if reply.next_after.is_empty() {
            break;
        }
        if reply.next_after == after {
            bail!("server sent the same place to continue");
        }
        after = reply.next_after;
    }
    if ctx.json {
        let entries: Vec<Value> = entries
            .iter()
            .map(|(path, old, new)| {
                let side = |state: &Option<FileState>| state.map(|s| entry_json(&Entry::File(s)));
                json!({
                    "path": lossy(path),
                    "status": status(*old, *new).to_string(),
                    "old": side(old),
                    "new": side(new),
                })
            })
            .collect();
        println!("{}", Value::from(entries));
        return Ok(());
    }
    for (path, old, new) in &entries {
        println!("{}\t{}", status(*old, *new), lossy(path));
    }
    Ok(())
}

/// The `git diff --name-status` letter for a change.
fn status(old: Option<FileState>, new: Option<FileState>) -> char {
    let kind = |state: FileState| std::mem::discriminant(&state);
    match (old, new) {
        (None, _) => 'A',
        (_, None) => 'D',
        (Some(old), Some(new)) if kind(old) != kind(new) => 'T',
        _ => 'M',
    }
}

/// A file state from a reply. Unset means no file.
fn file_state(state: Option<v1::FileState>) -> Result<Option<FileState>> {
    let Some(state) = state else {
        return Ok(None);
    };
    let state = match state.kind() {
        FileKind::Regular => FileState::Regular {
            content: id_from(&state.id)?,
            executable: state.executable,
        },
        FileKind::Symlink => FileState::Symlink {
            target: id_from(&state.id)?,
        },
        FileKind::Gitlink => FileState::Gitlink {
            commit: match state.id.len() {
                20 => GitOid::Sha1(id_from(&state.id)?),
                _ => GitOid::Sha256(id_from(&state.id)?),
            },
        },
        FileKind::Unspecified => bail!("server sent a file state with no kind"),
    };
    Ok(Some(state))
}
