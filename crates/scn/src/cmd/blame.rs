//! `blame`.

use anyhow::{Context, Result, bail};
use scion_core::{ChangesetId, IdentityTime};
use scion_proto::v1::BlameRequest;
use serde_json::{Value, json};
use std::collections::HashMap;

use crate::client::{Ctx, id_from, lossy, put_json_bytes, split_spec};
use crate::cmd::read::{identity_json, opt_id, read_stream};
use crate::outln;

/// The author of each changeset, and the path of the file there.
type Sources = HashMap<ChangesetId, (IdentityTime, Vec<u8>)>;

/// Prints, for each line of a file, the changeset that last changed it on
/// first-parent history, across renames. A reply that stops at the server's
/// bound names where to continue, so this asks again there until every line
/// has a changeset.
/// One call's reply, with its parts joined: the ranges in order, and the
/// cursor of the last part, which is where the walk continues.
struct Page {
    ranges: Vec<scion_proto::v1::BlameRange>,
    cursor: Vec<u8>,
}

/// Asks one call and joins its parts: each part holds a group of ranges
/// with the changesets its ranges name; the last part's `next_cursor` is
/// where the walk continues.
async fn blame_call(
    ctx: &mut Ctx,
    spec: &str,
    changeset_id: &ChangesetId,
    path: &scion_core::RepoPath,
    cursor: &[u8],
    sources: &mut Sources,
) -> Result<Page> {
    let mut pages = ctx
        .client
        .blame(BlameRequest {
            repo: ctx.repo.clone(),
            changeset_id: changeset_id.as_bytes().to_vec(),
            path: path.as_bytes().to_vec(),
            cursor: cursor.to_vec(),
        })
        .await
        .with_context(|| format!("cannot blame {spec}"))?
        .into_inner();
    let mut ranges = Vec::new();
    let mut next = Vec::new();
    loop {
        let page = pages
            .message()
            .await
            .with_context(|| format!("cannot blame {spec}"))?;
        let Some(page) = page else { break };
        for c in page.changesets {
            let author = IdentityTime::new(
                c.author_name,
                c.author_email,
                c.author_unix_secs,
                c.author_tz_offset_minutes,
            )
            .expect("identity fits the format");
            sources.insert(id_from::<ChangesetId>(&c.changeset_id)?, (author, c.path));
        }
        ranges.extend(page.ranges);
        next = page.next_cursor;
    }
    Ok(Page {
        ranges,
        cursor: next,
    })
}

/// Prints, for each line of the file at `spec` (`REV:PATH`), the changeset
/// that last changed it on first-parent history, across renames.
pub async fn blame(ctx: &mut Ctx, spec: &str) -> Result<()> {
    let (rev, path) = split_spec(spec)?;
    let path = path.with_context(|| format!("{spec:?}: expected REV:PATH"))?;
    let changeset_id = ctx.resolve(rev).await?;
    // The cursor of the next call, opaque: sent back unchanged with the
    // same changeset and path. Empty starts the first call.
    let mut cursor = Vec::new();
    let mut sources = Sources::new();
    // For each line of the file: its changeset once known, and until then
    // its line in the file at `at`. Empty before the first reply.
    let mut lines: Vec<(Option<ChangesetId>, u32)> = Vec::new();
    loop {
        let page = blame_call(ctx, spec, &changeset_id, &path, &cursor, &mut sources).await?;
        let reply_cursor = page.cursor;
        let ranges = page.ranges;
        // The source of each line of the window the reply covers. The
        // ranges cover it in order, so the first range names its first
        // line. An empty file has none, and no cursor to go on with.
        let at_line = match ranges.first() {
            Some(r) => r.start_line as usize - 1,
            None if reply_cursor.is_empty() => break,
            None => bail!("server sent no blame ranges"),
        };
        let mut here = Vec::new();
        for range in &ranges {
            if range.start_line as usize != at_line + here.len() + 1 {
                bail!("server sent blame ranges out of order");
            }
            let changeset = opt_id(&range.changeset_id)?;
            here.extend(
                (0..range.line_count).map(|n| (changeset, range.origin_line.saturating_add(n))),
            );
        }
        if at_line == lines.len() {
            // A window the client has not seen: it follows the last one.
            lines.extend(here);
        } else {
            // The same window again, from a call that went deeper into the
            // history. Its lines map to their sources.
            if at_line > lines.len() {
                bail!("server skipped lines of the file");
            }
            for (line, source) in lines
                .iter_mut()
                .enumerate()
                .take(at_line + here.len())
                .skip(at_line)
            {
                if source.0.is_some() {
                    continue;
                }
                *source = *here
                    .get(line - at_line)
                    .context("server sent a blame shorter than the file")?;
            }
        }
        if reply_cursor.is_empty() {
            break;
        }
        if reply_cursor == cursor {
            bail!("server sent the same cursor again");
        }
        cursor = reply_cursor;
    }
    let lines: Vec<(ChangesetId, u32)> = lines
        .into_iter()
        .filter_map(|(source, origin)| Some((source?, origin)))
        .collect();
    if ctx.json {
        outln!("{}", Value::from(ranges_json(&lines, &sources)));
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
    print_text(&lines, &sources, path.as_bytes(), &text)?;
    Ok(())
}

/// Prints each line of `text`, the file at `path`, after its changeset, its
/// author and time, and its line number. As in git, each line also shows
/// the path of its source if any line came from another path.
fn print_text(
    lines: &[(ChangesetId, u32)],
    sources: &Sources,
    path: &[u8],
    text: &[u8],
) -> Result<()> {
    let width = lines.len().to_string().len();
    let path_width = if sources.values().any(|(_, p)| p != path) {
        sources
            .values()
            .map(|(_, p)| lossy(p).chars().count())
            .max()
    } else {
        None
    };
    for (n, ((id, _), text)) in lines
        .iter()
        .zip(text.split_inclusive(|&b| b == b'\n'))
        .enumerate()
    {
        let source = sources.get(id);
        let name = source.map(|(a, _)| lossy(a.name())).unwrap_or_default();
        let secs = source.map_or(0, |(a, _)| a.unix_secs());
        let source_path = match (source, path_width) {
            (Some((_, p)), Some(path_width)) => format!(" {:<path_width$}", lossy(p)),
            _ => String::new(),
        };
        let text = lossy(text);
        outln!(
            "{}{source_path} ({name} {secs} {:>width$}) {}",
            id,
            n + 1,
            text.strip_suffix('\n').unwrap_or(&text)
        );
    }
    Ok(())
}

/// Lines grouped into ranges, as JSON: consecutive lines from the same
/// changeset that are also consecutive there.
fn ranges_json(lines: &[(ChangesetId, u32)], sources: &Sources) -> Vec<Value> {
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
            let source = sources.get(&id);
            let mut value = json!({
                "start_line": start_line,
                "line_count": line_count,
                "changeset": id.to_string(),
                "path": source.map(|(_, path)| lossy(path)),
                "origin_line": origin_line,
                "author": source.map(|(author, _)| identity_json(author)),
            });
            if let Some((_, path)) = &source {
                put_json_bytes(&mut value, "path", path);
            }
            value
        })
        .collect()
}
