//! `merge-base`, `is-ancestor`, `diff`.

use std::collections::HashMap;
use std::io::{BufWriter, Write as _};

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
/// `M`odified, `T` for a kind change), a tab, the path. A rename prints once,
/// as `R`, a tab, and `old -> new`, where the new path would print. A reply
/// that stops at the server's bound names the last path it holds, so this
/// asks again after it until the diff is complete. Text prints each reply as
/// it arrives; JSON is one array, printed after the last reply.
pub async fn diff(ctx: &mut Ctx, old: &str, new: &str, path: Option<&RepoPath>) -> Result<()> {
    let old_id = ctx.resolve(old).await?;
    let new_id = ctx.resolve(new).await?;
    let path = path.map(|p| p.as_bytes().to_vec()).unwrap_or_default();
    let mut json_entries = Vec::new();
    // The two halves of each rename: the deleted ones by path, and the added
    // ones. They can come in different replies, so they are matched at the
    // end.
    let mut deletes = HashMap::new();
    let mut adds = Vec::new();
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
        let mut out = BufWriter::new(std::io::stdout().lock());
        for entry in reply.entries {
            let (old, new) = (file_state(entry.old)?, file_state(entry.new)?);
            let side = |state: Option<FileState>| state.map(|s| entry_json(&Entry::File(s)));
            // The deleted half of a rename prints with the added half.
            if !entry.rename_to.is_empty() {
                deletes.insert(entry.path, (entry.rename_to, side(old)));
                continue;
            }
            let path = lossy(&entry.path);
            let renamed = !entry.rename_from.is_empty();
            if renamed {
                adds.push((entry.rename_from.clone(), entry.path.clone()));
            }
            let status = if renamed { 'R' } else { status(old, new) };
            if ctx.json {
                let mut value = json!({
                    "path": path,
                    "status": status.to_string(),
                    "old": side(old),
                    "new": side(new),
                });
                if renamed {
                    value["old_path"] = lossy(&entry.rename_from).into();
                    value["similarity"] = entry.similarity.into();
                }
                json_entries.push((entry.rename_from, value));
            } else if renamed {
                writeln!(out, "R\t{} -> {path}", lossy(&entry.rename_from))?;
            } else {
                writeln!(out, "{status}\t{path}")?;
            }
        }
        out.flush()?;
        drop(out);
        if reply.next_after.is_empty() {
            break;
        }
        if reply.next_after == after {
            bail!("server sent the same place to continue");
        }
        after = reply.next_after;
    }
    let mut olds = match_renames(deletes, adds)?;
    if ctx.json {
        let values = json_entries.into_iter().map(|(from, mut value)| {
            if let Some(old) = olds.remove(&from) {
                value["old"] = old.into();
            }
            value
        });
        println!("{}", Value::from_iter(values));
    }
    Ok(())
}

/// The old file of each rename, by its old path. Matches the two halves of
/// each rename: `deletes`, each deleted half by its path, with the new path
/// and the old file, and `adds`, each added half as its old and new path.
///
/// # Errors
///
/// A half with no other half: the server sent a bad diff.
fn match_renames(
    mut deletes: HashMap<Vec<u8>, (Vec<u8>, Option<Value>)>,
    adds: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<HashMap<Vec<u8>, Option<Value>>> {
    let mut olds = HashMap::new();
    for (from, to) in adds {
        match deletes.remove(&from) {
            Some((rename_to, old)) if rename_to == to => olds.insert(from, old),
            _ => bail!(
                "server sent a rename from {} to {}, but no delete there",
                lossy(&from),
                lossy(&to)
            ),
        };
    }
    if let Some((from, (to, _))) = deletes.into_iter().next() {
        bail!(
            "server sent a delete of {} as a rename to {}, but no rename there",
            lossy(&from),
            lossy(&to)
        );
    }
    Ok(olds)
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

#[cfg(test)]
mod tests {
    use super::*;

    type Delete = (Vec<u8>, (Vec<u8>, Option<Value>));

    /// A rename from `from` to `to`: its deleted half, and its added half.
    fn halves(from: &str, to: &str) -> (Delete, (Vec<u8>, Vec<u8>)) {
        let delete = (from.into(), (to.into(), Some(json!(from))));
        (delete, (from.into(), to.into()))
    }

    #[test]
    fn a_rename_needs_both_halves() {
        let (delete, add) = halves("old", "new");
        let (other_delete, other_add) = halves("old", "other");
        let olds = match_renames(HashMap::from([delete.clone()]), vec![add.clone()]).unwrap();
        assert_eq!(olds, HashMap::from([(b"old".to_vec(), Some(json!("old")))]));
        for (deletes, adds) in [
            (vec![delete.clone()], vec![]),
            (vec![], vec![add.clone()]),
            (vec![delete], vec![other_add]),
            (vec![other_delete], vec![add]),
        ] {
            let found = match_renames(deletes.into_iter().collect(), adds);
            assert!(found.is_err(), "{found:?}");
        }
    }
}
