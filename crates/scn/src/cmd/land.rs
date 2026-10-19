//! `land`.

use crate::client::{Ctx, id_from, lossy};
use crate::outln;
use anyhow::{Context, Result, anyhow};
use scion_core::ChangesetId;
use scion_proto::v1::{LandRequest, LandResponse, LandState};
use serde_json::json;

/// Arguments of `scn land`.
#[derive(clap::Args)]
pub struct LandArgs {
    /// Changeset ID.
    changeset: ChangesetId,
    /// Ref to move.
    #[arg(long, default_value = "main")]
    onto: String,
    /// Retry key. Defaults to the changeset ID and ref, so a retry of the
    /// same land is safe.
    #[arg(long)]
    request_id: Option<String>,
}

/// Lands a changeset and prints the outcome. Fails unless it landed, so a
/// script or an agent can branch on the exit code.
pub async fn land(ctx: &mut Ctx, args: LandArgs) -> Result<()> {
    let LandArgs {
        changeset: id,
        onto,
        request_id,
    } = args;
    let request_key = request_id.unwrap_or_else(|| format!("{id}:{onto}"));
    let reply = ctx
        .client
        .land(LandRequest {
            repo: ctx.repo.clone(),
            changeset_id: id.as_bytes().to_vec(),
            target_ref: onto,
            request_key,
        })
        .await
        .context("land failed")?
        .into_inner();
    let state = reply.state();
    print_outcome(ctx.json, &reply)?;
    match state {
        LandState::Landed => Ok(()),
        // print_outcome already named the detail, so the error carries
        // the state alone: the text prints once.
        state => Err(land_error(state, &reply)),
    }
}

/// The error for a land that did not land.
fn land_error(state: LandState, reply: &LandResponse) -> anyhow::Error {
    match state {
        LandState::Conflict => anyhow!("land conflicted"),
        LandState::Rejected => anyhow!("land rejected"),
        LandState::Pending | LandState::Unspecified => {
            anyhow!("land {} did not finish", reply.land_id)
        }
        LandState::Landed => anyhow!("land landed but was not accepted"),
    }
}

fn print_outcome(json: bool, reply: &LandResponse) -> Result<()> {
    let result = if reply.result_changeset_id.is_empty() {
        None
    } else {
        Some(id_from::<ChangesetId>(&reply.result_changeset_id)?)
    };
    let paths: Vec<String> = reply
        .conflict_paths
        .iter()
        .map(|p| lossy(p).into_owned())
        .collect();
    if json {
        let out = json!({
            "land_id": reply.land_id,
            "state": reply.state().as_str_name(),
            "result": result.map(|id| id.to_string()),
            "detail": reply.detail,
            "conflict_paths": paths,
        });
        outln!("{out}");
        return Ok(());
    }
    match (reply.state(), result) {
        (LandState::Landed, Some(id)) => outln!("landed {id}"),
        (LandState::Conflict, _) => {
            outln!("conflict: {}", reply.detail);
            for path in &paths {
                outln!("  {path}");
            }
            outln!("re-read those paths on the target, redo the change, commit again");
        }
        (state, _) => outln!("{}: {}", state.as_str_name(), reply.detail),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conflict_reply() -> LandResponse {
        LandResponse {
            land_id: String::from("l1"),
            state: LandState::Conflict as i32,
            result_changeset_id: Vec::new(),
            detail: String::from("1 path(s) changed on \"main\" after the base"),
            conflict_paths: vec![b"same.txt".to_vec()],
        }
    }

    #[test]
    fn the_error_of_a_conflict_names_the_state_once() {
        let reply = conflict_reply();
        let text = land_error(LandState::Conflict, &reply).to_string();
        assert_eq!(text, "land conflicted");
        assert!(!text.contains("same.txt"), "{text}");
        assert!(!text.contains("path(s)"), "{text}");
    }

    #[test]
    fn the_error_of_a_rejection_names_the_state_once() {
        let mut reply = conflict_reply();
        reply.state = LandState::Rejected as i32;
        let text = land_error(LandState::Rejected, &reply).to_string();
        assert_eq!(text, "land rejected");
        assert!(!text.contains("path(s)"), "{text}");
    }
}
