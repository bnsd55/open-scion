//! `blame`.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use scion_core::{ChangesetId, IdentityTime};
use scion_proto::v1::BlameRequest;
use serde_json::{Value, json};

use crate::client::{Ctx, id_from, lossy, split_spec};
use crate::cmd::read::{identity_json, opt_id, read_stream};

/// Prints, for each line of a file, the changeset that last changed it on
/// first-parent history. A reply that stops at the server's bound names
/// where to continue, so this asks again there until every line has a
/// changeset.
pub async fn blame(ctx: &mut Ctx, spec: &str) -> Result<()> {
    let (rev, path) = split_spec(spec)?;
    let path = path.with_context(|| format!("{spec:?}: expected REV:PATH"))?;
    let changeset_id = ctx.resolve(rev).await?;
    let mut at = changeset_id;
    let mut authors = HashMap::new();
    // For each line of the file: its changeset once known, and until then
    // its line in the file at `at`. Empty before the first reply.
    let mut lines: Vec<(Option<ChangesetId>, u32)> = Vec::new();
    loop {
        let reply = ctx
            .client
            .blame(BlameRequest {
                repo: ctx.repo.clone(),
                changeset_id: at.as_bytes().to_vec(),
                path: path.as_bytes().to_vec(),
            })
            .await
            .with_context(|| format!("cannot blame {spec}"))?
            .into_inner();
        for c in reply.changesets {
            let author = IdentityTime {
                name: c.author_name,
                email: c.author_email,
                unix_secs: c.author_unix_secs,
                tz_offset_minutes: c.author_tz_offset_minutes,
            };
            authors.insert(id_from::<ChangesetId>(&c.changeset_id)?, author);
        }
        // The source of each line of the file at `at`.
        let mut here = Vec::new();
        for range in &reply.ranges {
            if range.start_line as usize != here.len() + 1 {
                bail!("server sent blame ranges out of order");
            }
            let changeset = opt_id(&range.changeset_id)?;
            here.extend(
                (0..range.line_count).map(|n| (changeset, range.origin_line.saturating_add(n))),
            );
        }
        if lines.is_empty() {
            lines = here;
        } else {
            for line in lines.iter_mut().filter(|(source, _)| source.is_none()) {
                *line = *line
                    .1
                    .checked_sub(1)
                    .and_then(|i| here.get(i as usize))
                    .context("server sent a blame shorter than the file")?;
            }
        }
        if lines.iter().all(|(source, _)| source.is_some()) {
            break;
        }
        let next = opt_id(&reply.next_changeset_id)?.context("server sent no place to continue")?;
        if next == at {
            bail!("server sent the same place to continue");
        }
        at = next;
    }
    let lines: Vec<(ChangesetId, u32)> = lines
        .into_iter()
        .filter_map(|(source, origin)| Some((source?, origin)))
        .collect();
    if ctx.json {
        println!("{}", Value::from(ranges_json(&lines, &authors)));
        return Ok(());
    }
    let mut stream = read_stream(ctx, changeset_id, &path)
        .await
        .with_context(|| format!("cannot read {spec}"))?;
    let mut text = Vec::new();
    while let Some(chunk) = stream
        .message()
        .await
        .with_context(|| format!("cannot read {spec}"))?
    {
        text.extend(chunk.data);
    }
    let width = lines.len().to_string().len();
    for (n, ((id, _), text)) in lines
        .iter()
        .zip(text.split_inclusive(|&b| b == b'\n'))
        .enumerate()
    {
        let author = authors.get(id);
        let name = author.map(|a| lossy(&a.name)).unwrap_or_default();
        let secs = author.map_or(0, |a| a.unix_secs);
        let text = lossy(text);
        println!(
            "{} ({name} {secs} {:>width$}) {}",
            &id.to_string()[..12],
            n + 1,
            text.strip_suffix('\n').unwrap_or(&text)
        );
    }
    Ok(())
}

/// Lines grouped into ranges, as JSON: consecutive lines from the same
/// changeset that are also consecutive there.
fn ranges_json(
    lines: &[(ChangesetId, u32)],
    authors: &HashMap<ChangesetId, IdentityTime>,
) -> Vec<Value> {
    let mut ranges: Vec<(usize, ChangesetId, u32, u32)> = Vec::new();
    for (n, &(id, origin)) in lines.iter().enumerate() {
        match ranges.last_mut() {
            Some((_, last, start, count))
                if *last == id && start.checked_add(*count) == Some(origin) =>
            {
                *count += 1;
            }
            _ => ranges.push((n + 1, id, origin, 1)),
        }
    }
    ranges
        .into_iter()
        .map(|(start_line, id, origin_line, line_count)| {
            json!({
                "start_line": start_line,
                "line_count": line_count,
                "changeset": id.to_string(),
                "origin_line": origin_line,
                "author": authors.get(&id).map(identity_json),
            })
        })
        .collect()
}
