//! Blame: which changeset last changed each line of a file, on first-parent
//! history.
//!
//! Computed on each call. Walk first parents to each changeset that changed
//! the file, diff its version with the version in its first parent, and give
//! the added lines to that changeset. Stop when every line has a changeset,
//! or at a bound.

use gix::diff::blob::sources::byte_lines;
use gix::diff::blob::{Algorithm, Diff, InternedInput};
use scion_core::{BlobId, ChangesetId, Entry, FileState, IdentityTime, RepoPath};

use crate::db::{self, RepoId};
use crate::error::{Error, Result};
use crate::read::{self, MAX_SCAN};
use crate::store::Store;
use crate::tree;

/// Largest file blame reads, in bytes. Bounds the memory and diff time of
/// one version, and the lines in a reply.
// SHORTCUT: a larger file is refused, and an older version over the limit
// counts as absent, so its lines go to the changeset that replaced it.
// Upgrade: blame from derived data, so a call does not diff whole versions.
pub const MAX_BLAME_BYTES: usize = 1 << 20;

/// Most versions of a file one call diffs.
// SHORTCUT: a file with many versions costs one call per 100 versions.
// Upgrade: blame as derived data, kept per file version.
pub const MAX_BLAME_VERSIONS: usize = 100;

/// Consecutive lines with the same source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlameRange {
    /// The first line, counting from 1, in the blamed file.
    pub start_line: u32,
    /// How many lines.
    pub line_count: u32,
    /// The changeset that last changed these lines. `None` when the call
    /// stopped first: see [`Blame::next`].
    pub changeset: Option<ChangesetId>,
    /// The first line, counting from 1, in the file at `changeset`, or at
    /// [`Blame::next`] when `changeset` is `None`.
    pub origin_line: u32,
}

/// The blame of one file.
#[derive(Debug)]
pub struct Blame {
    /// Ranges in file order. Together they cover every line once.
    pub ranges: Vec<BlameRange>,
    /// The author of each changeset that a range names, once each.
    pub authors: Vec<(ChangesetId, IdentityTime)>,
    /// Set when the call stopped at a bound before every line had a
    /// changeset. Blame the same path at this changeset to continue.
    pub next: Option<ChangesetId>,
}

/// Blames the file at `path` in `cs`. Diffs at most [`MAX_BLAME_VERSIONS`]
/// versions and checks at most [`MAX_SCAN`] changesets.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo, the changeset or the file does not
/// exist. [`Error::InvalidArgument`] if the path is not a regular file, is
/// larger than [`MAX_BLAME_BYTES`], or is binary (has a NUL byte).
pub fn blame(store: &Store, repo: &str, cs: ChangesetId, path: &RepoPath) -> Result<Blame> {
    let repo = store.read(|c| db::repo_id(c, repo))?;
    let (mut blob, mut text) = text_at(store, repo, cs, path)??;
    // For each line of the blamed file: its changeset once known, and until
    // then its line in `text`, the version being explained.
    let mut lines: Vec<(Option<ChangesetId>, usize)> = (0..byte_lines(&text).count())
        .map(|at| (None, at))
        .collect();
    let mut authors = Vec::new();
    let mut next = Some(cs);
    let mut budget = MAX_SCAN;
    for _ in 0..MAX_BLAME_VERSIONS {
        if lines.iter().all(|(source, _)| source.is_some()) {
            break;
        }
        let Some((id, changeset)) =
            store.read(|c| read::next_change(c, repo, Some(path), &mut next, &mut budget))?
        else {
            break;
        };
        let puts_text = matches!(
            changeset.changes.get(path),
            Some(Some(FileState::Regular { content, .. })) if *content == blob
        );
        if !puts_text {
            return Err(Error::Corrupt(format!(
                "changeset {id} does not put the blamed version of {path:?}"
            )));
        }
        // The version before `id`. If nothing is there, or no text blame can
        // diff, `id` added every line.
        let older = match next {
            Some(parent) => text_at(store, repo, parent, path)?.ok(),
            None => None,
        };
        let kept = match &older {
            Some((_, older_text)) => kept_lines(older_text, &text),
            None => vec![None; byte_lines(&text).count()],
        };
        let mut named = false;
        for (source, at) in lines.iter_mut().filter(|(source, _)| source.is_none()) {
            if let Some(older_at) = kept[*at] {
                *at = older_at;
            } else {
                *source = Some(id);
                named = true;
            }
        }
        if named {
            authors.push((id, changeset.author));
        }
        if let Some(older) = older {
            (blob, text) = older;
        }
    }
    if lines.iter().all(|(source, _)| source.is_some()) {
        next = None;
    } else if next.is_none() {
        return Err(Error::Corrupt(format!(
            "no changeset on the history of {cs} adds {path:?}"
        )));
    }
    Ok(Blame {
        ranges: ranges(&lines),
        authors,
        next,
    })
}

/// The regular file at `path` in `cs`, if blame can diff it. The inner
/// error says why not: nothing is there ([`Error::NotFound`]), or it is not
/// a regular text file of at most [`MAX_BLAME_BYTES`]
/// ([`Error::InvalidArgument`]).
///
/// # Errors
///
/// The outer error is a failure to read.
fn text_at(
    store: &Store,
    repo: RepoId,
    cs: ChangesetId,
    path: &RepoPath,
) -> Result<Result<(BlobId, Vec<u8>)>> {
    let root = tree::root_tree(store, repo, cs)?;
    let blob = match store.read(|c| tree::lookup(c, root, path))? {
        Some(Entry::File(FileState::Regular { content, .. })) => content,
        Some(_) => {
            return Ok(Err(Error::InvalidArgument(format!(
                "{path:?} is not a regular file"
            ))));
        }
        None => return Ok(Err(Error::NotFound(format!("path {path:?}")))),
    };
    if store.blobs().size(blob)? > MAX_BLAME_BYTES {
        return Ok(Err(Error::InvalidArgument(format!(
            "{path:?} is larger than {MAX_BLAME_BYTES} bytes"
        ))));
    }
    let text = store.blobs().get(blob)?;
    if text.contains(&0) {
        return Ok(Err(Error::InvalidArgument(format!("{path:?} is binary"))));
    }
    Ok(Ok((blob, text)))
}

/// For each line of `new`: its line in `old` if the diff keeps it, `None` if
/// `new` adds it. A Myers diff with git's indent heuristic, as `git blame`
/// uses by default.
fn kept_lines(old: &[u8], new: &[u8]) -> Vec<Option<usize>> {
    let input = InternedInput::new(old, new);
    let mut diff = Diff::compute(Algorithm::Myers, &input);
    diff.postprocess_lines(&input);
    // Kept lines are in the same order on both sides.
    let mut old_line = 0;
    (0..input.after.len())
        .map(|line| {
            if diff.is_added(line_u32(line)) {
                return None;
            }
            while diff.is_removed(line_u32(old_line)) {
                old_line += 1;
            }
            old_line += 1;
            Some(old_line - 1)
        })
        .collect()
}

/// Groups lines, in file order, into ranges: the same source and
/// consecutive lines there.
fn ranges(lines: &[(Option<ChangesetId>, usize)]) -> Vec<BlameRange> {
    let mut ranges: Vec<BlameRange> = Vec::new();
    for (line, &(changeset, at)) in lines.iter().enumerate() {
        let origin_line = line_u32(at + 1);
        match ranges.last_mut() {
            Some(range)
                if range.changeset == changeset
                    && range.origin_line + range.line_count == origin_line =>
            {
                range.line_count += 1;
            }
            _ => ranges.push(BlameRange {
                start_line: line_u32(line + 1),
                line_count: 1,
                changeset,
                origin_line,
            }),
        }
    }
    ranges
}

/// A line number as a `u32`. A file of at most [`MAX_BLAME_BYTES`] has
/// far fewer lines than `u32::MAX`.
fn line_u32(line: usize) -> u32 {
    u32::try_from(line).expect("a blamed file has fewer than 2^32 lines")
}
