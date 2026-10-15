//! Queries on the shared tables: repos, changesets, refs, trees and derived
//! data. Tables owned by one module (lands, git import) keep their SQL there.
//!
//! Each function takes a `&Connection`, so it runs the same inside or outside
//! a transaction (a `Transaction` derefs to a `Connection`).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use rusqlite::{Connection, OptionalExtension, params};
use scion_core::{Changeset, ChangesetId, Tree, TreeId};

use crate::error::{Error, Result};

/// Row ID of a repository.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RepoId(pub i64);

/// Where a ref points, and the version to compare-and-swap against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefState {
    /// The changeset the ref points to.
    pub target: ChangesetId,
    /// Bumped on every move.
    pub version: i64,
}

/// Creates a repository, or returns the existing one with that name.
///
/// # Errors
///
/// Database errors.
pub fn create_repo(c: &Connection, name: &str) -> Result<RepoId> {
    c.prepare_cached("INSERT OR IGNORE INTO repos (name) VALUES (?1)")?
        .execute([name])?;
    repo_id(c, name)
}

/// Looks up a repository by name.
///
/// # Errors
///
/// [`Error::NotFound`] if no repository has that name.
pub fn repo_id(c: &Connection, name: &str) -> Result<RepoId> {
    c.prepare_cached("SELECT repo_id FROM repos WHERE name = ?1")?
        .query_row([name], |row| row.get(0).map(RepoId))
        .optional()?
        .ok_or_else(|| Error::NotFound(format!("repo {name:?}")))
}

/// Stores a changeset and adds it to `repo`. Every parent must already be in
/// `repo`. Safe to repeat.
///
/// # Errors
///
/// [`Error::NotFound`] if a parent is not in `repo`.
pub fn insert_changeset(c: &Connection, repo: RepoId, cs: &Changeset) -> Result<ChangesetId> {
    let (body, id) = cs.encode_with_id();
    let mut parent_gen = 0;
    for parent in &cs.parents {
        parent_gen = parent_gen.max(generation(c, repo, *parent)?);
    }
    c.prepare_cached("INSERT OR IGNORE INTO changesets (id, gen, body) VALUES (?1, ?2, ?3)")?
        .execute(params![id.as_bytes(), parent_gen + 1, body])?;
    for (n, parent) in (0_i64..).zip(&cs.parents) {
        c.prepare_cached(
            "INSERT OR IGNORE INTO changeset_parents (cs, n, parent) VALUES (?1, ?2, ?3)",
        )?
        .execute(params![id.as_bytes(), n, parent.as_bytes()])?;
    }
    c.prepare_cached("INSERT OR IGNORE INTO repo_changesets (repo_id, cs) VALUES (?1, ?2)")?
        .execute(params![repo.0, id.as_bytes()])?;
    Ok(id)
}

/// Reads a changeset that `repo` may see and checks it against its ID.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have it, [`Error::Corrupt`] if it
/// does not decode or match.
pub fn get_changeset(c: &Connection, repo: RepoId, id: ChangesetId) -> Result<Changeset> {
    let body: Vec<u8> = c
        .query_row(
            "SELECT body FROM changesets JOIN repo_changesets ON cs = id
             WHERE repo_id = ?1 AND id = ?2",
            params![repo.0, id.as_bytes()],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| Error::NotFound(format!("changeset {id}")))?;
    let (cs, actual) = Changeset::decode_with_id(&body)
        .map_err(|e| Error::Corrupt(format!("changeset {id}: {e}")))?;
    if actual != id {
        return Err(Error::Corrupt(format!(
            "changeset {id} does not match its ID"
        )));
    }
    Ok(cs)
}

/// Whether `repo` may see changeset `id`.
///
/// # Errors
///
/// Database errors.
pub fn has_changeset(c: &Connection, repo: RepoId, id: ChangesetId) -> Result<bool> {
    Ok(
        c.prepare_cached("SELECT 1 FROM repo_changesets WHERE repo_id = ?1 AND cs = ?2")?
            .exists(params![repo.0, id.as_bytes()])?,
    )
}

/// The generation of a changeset that `repo` may see: 1 for a root, else 1 +
/// the highest parent generation.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have it.
pub fn generation(c: &Connection, repo: RepoId, id: ChangesetId) -> Result<i64> {
    c.prepare_cached(
        "SELECT gen FROM changesets JOIN repo_changesets ON cs = id
         WHERE repo_id = ?1 AND id = ?2",
    )?
    .query_row(params![repo.0, id.as_bytes()], |row| row.get(0))
    .optional()?
    .ok_or_else(|| Error::NotFound(format!("changeset {id}")))
}

/// The first parent of a changeset, or `None` for a root.
///
/// # Errors
///
/// Database errors.
pub fn first_parent(c: &Connection, id: ChangesetId) -> Result<Option<ChangesetId>> {
    let parent: Option<Vec<u8>> = c
        .query_row(
            "SELECT parent FROM changeset_parents WHERE cs = ?1 AND n = 0",
            [id.as_bytes()],
            |row| row.get(0),
        )
        .optional()?;
    parent.map(|bytes| to_id(&bytes)).transpose()
}

/// Every parent of changeset `id`, whose generation is `id_gen`, in order,
/// with its generation. The parents of a changeset in a repo are in that
/// repo too ([`insert_changeset`]). The graph walks rely on each parent's
/// generation being below its child's, so this checks it.
///
/// # Errors
///
/// [`Error::Corrupt`] if a parent's generation is not below `id_gen`.
fn parents(c: &Connection, id: ChangesetId, id_gen: i64) -> Result<Vec<(ChangesetId, i64)>> {
    let mut parents = c.prepare_cached(
        "SELECT parent, gen FROM changeset_parents JOIN changesets ON id = parent
         WHERE cs = ?1 ORDER BY n",
    )?;
    let rows = parents.query_map([id.as_bytes()], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
    })?;
    rows.map(|row| {
        let (parent, parent_gen) = row?;
        let parent = to_id(&parent)?;
        if parent_gen >= id_gen {
            return Err(Error::Corrupt(format!(
                "changeset {id} has generation {id_gen}, its parent {parent} {parent_gen}"
            )));
        }
        Ok((parent, parent_gen))
    })
    .collect()
}

/// Whether `ancestor` is `descendant` or reachable from it through parents.
///
/// Walks every parent, not only first parents. A parent's generation is
/// always below its child's, so the walk never goes below the generation of
/// `ancestor`, and visits each changeset at most once. It reads the parents
/// of at most `max_visits` changesets.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have either changeset.
/// [`Error::ResourceExhausted`] if the walk needs more than `max_visits`
/// changesets.
pub fn is_ancestor(
    c: &Connection,
    repo: RepoId,
    ancestor: ChangesetId,
    descendant: ChangesetId,
    max_visits: usize,
) -> Result<bool> {
    let floor = generation(c, repo, ancestor)?;
    let mut seen = HashSet::new();
    let mut stack = vec![(descendant, generation(c, repo, descendant)?)];
    while let Some((id, id_gen)) = stack.pop() {
        if id == ancestor {
            return Ok(true);
        }
        if id_gen <= floor || !seen.insert(id) {
            continue;
        }
        if seen.len() > max_visits {
            return Err(too_deep(max_visits));
        }
        stack.extend(parents(c, id, id_gen)?);
    }
    Ok(false)
}

/// Reached from `a`.
const FROM_A: u8 = 1;
/// Reached from `b`.
const FROM_B: u8 = 2;
/// Reached from a common ancestor already found, so not a best one.
const STALE: u8 = 4;

/// The best common ancestors of `a` and `b`, over all parents: every common
/// ancestor that is not an ancestor of another common ancestor. Highest
/// generation first, then lowest ID.
///
/// The walk takes changesets from a queue, highest generation first, and
/// marks each with the sides that reach it. A parent's generation is below
/// its child's, so a changeset leaves the queue only after every
/// descendant the walk reached, with all its marks. One marked from both
/// sides and not stale is a best common ancestor, and its ancestors become
/// stale. The walk ends when no queued changeset that is not stale is
/// marked from `a`, or none from `b`: no other best common ancestor can
/// follow. It takes at most `max_visits` changesets from the queue.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have either changeset.
/// [`Error::ResourceExhausted`] if the walk needs more than `max_visits`
/// changesets.
pub fn merge_bases(
    c: &Connection,
    repo: RepoId,
    a: ChangesetId,
    b: ChangesetId,
    max_visits: usize,
) -> Result<Vec<ChangesetId>> {
    let mut marks: HashMap<ChangesetId, u8> = HashMap::new();
    let mut queue = BinaryHeap::new();
    for (id, side) in [(a, FROM_A), (b, FROM_B)] {
        let mark = marks.entry(id).or_insert(0);
        if *mark == 0 {
            queue.push((generation(c, repo, id)?, Reverse(id)));
        }
        *mark |= side;
    }
    // Queued changesets that are not stale, marked from `a` and from `b`:
    // one each to start, also when `a` is `b`.
    let mut live = [1, 1];
    let mut bases = Vec::new();
    let mut taken = 0;
    while let Some((id_gen, Reverse(id))) = queue.pop() {
        if live.contains(&0) {
            break;
        }
        let mut mark = marks[&id];
        tally(&mut live, mark, false);
        taken += 1;
        if taken > max_visits {
            return Err(too_deep(max_visits));
        }
        if mark == FROM_A | FROM_B {
            bases.push(id);
            mark |= STALE;
        }
        for (parent, parent_gen) in parents(c, id, id_gen)? {
            let old = marks.get(&parent).copied();
            let new = old.unwrap_or(0) | mark;
            if old == Some(new) {
                continue;
            }
            match old {
                // Still queued: its generation is below the one taken.
                Some(old) => tally(&mut live, old, false),
                None => queue.push((parent_gen, Reverse(parent))),
            }
            tally(&mut live, new, true);
            marks.insert(parent, new);
        }
    }
    Ok(bases)
}

/// Adds a queued changeset's marks to `live`, or removes them. A stale
/// changeset counts for neither side.
fn tally(live: &mut [usize; 2], mark: u8, add: bool) {
    if mark & STALE != 0 {
        return;
    }
    for (count, side) in live.iter_mut().zip([FROM_A, FROM_B]) {
        if mark & side != 0 {
            if add {
                *count += 1;
            } else {
                *count -= 1;
            }
        }
    }
}

/// The error of a graph walk that reached its bound.
fn too_deep(max_visits: usize) -> Error {
    Error::ResourceExhausted(format!(
        "the history is too deep: the answer needs more than {max_visits} changesets"
    ))
}

/// Where a ref points, if it exists.
///
/// # Errors
///
/// Database errors.
pub fn get_ref(c: &Connection, repo: RepoId, name: &str) -> Result<Option<RefState>> {
    c.prepare_cached("SELECT target, version FROM refs WHERE repo_id = ?1 AND name = ?2")?
        .query_row(params![repo.0, name], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get(1)?))
        })
        .optional()?
        .map(|(target, version)| {
            Ok(RefState {
                target: to_id(&target)?,
                version,
            })
        })
        .transpose()
}

/// Moves a ref if its version is still `expected`. `None` creates the ref
/// and expects it not to exist. Returns whether the ref moved.
///
/// # Errors
///
/// Database errors, or [`Error::NotFound`] if `repo` does not have `target`.
pub fn cas_ref(
    c: &Connection,
    repo: RepoId,
    name: &str,
    expected: Option<i64>,
    target: ChangesetId,
) -> Result<bool> {
    generation(c, repo, target)?;
    let changed = match expected {
        None => c.prepare_cached("INSERT OR IGNORE INTO refs (repo_id, name, target, version) VALUES (?1, ?2, ?3, 1)")?.execute(
            params![repo.0, name, target.as_bytes()],
        )?,
        Some(version) => c.prepare_cached("UPDATE refs SET target = ?3, version = version + 1
             WHERE repo_id = ?1 AND name = ?2 AND version = ?4")?.execute(
            params![repo.0, name, target.as_bytes(), version],
        )?,
    };
    Ok(changed == 1)
}

/// The stored bytes of a derived value, if computed.
///
/// # Errors
///
/// Database errors.
pub fn get_derived(
    c: &Connection,
    kind: &str,
    version: u32,
    cs: ChangesetId,
) -> Result<Option<Vec<u8>>> {
    Ok(
        c.prepare_cached("SELECT value FROM derived WHERE kind = ?1 AND version = ?2 AND cs = ?3")?
            .query_row(params![kind, version, cs.as_bytes()], |row| row.get(0))
            .optional()?,
    )
}

/// Records a derived value. A second write must agree with the first:
/// derivation is deterministic, so a mismatch means corruption.
///
/// # Errors
///
/// [`Error::Corrupt`] if a different value is already stored.
pub fn put_derived(
    c: &Connection,
    kind: &str,
    version: u32,
    cs: ChangesetId,
    value: &[u8],
) -> Result<()> {
    let inserted = c
        .prepare_cached(
            "INSERT OR IGNORE INTO derived (kind, version, cs, value) VALUES (?1, ?2, ?3, ?4)",
        )?
        .execute(params![kind, version, cs.as_bytes(), value])?;
    if inserted == 0 && get_derived(c, kind, version, cs)?.as_deref() != Some(value) {
        return Err(Error::Corrupt(format!(
            "derived {kind} v{version} for {cs} differs from the stored value"
        )));
    }
    Ok(())
}

/// Stores a tree. Safe to repeat.
///
/// # Errors
///
/// Database errors.
pub fn put_tree(c: &Connection, tree: &Tree) -> Result<TreeId> {
    let (body, id) = tree.encode_with_id();
    c.prepare_cached("INSERT OR IGNORE INTO trees (id, body) VALUES (?1, ?2)")?
        .execute(params![id.as_bytes(), body])?;
    Ok(id)
}

/// Reads a tree and checks it against its ID.
///
/// # Errors
///
/// [`Error::NotFound`] if absent, [`Error::Corrupt`] if it does not decode or
/// match.
pub fn get_tree(c: &Connection, id: TreeId) -> Result<Tree> {
    load_tree(c, id).map(|(tree, _)| tree)
}

/// Reads a tree's stored `TreeV1` bytes and checks them against its ID. The
/// strict decoder accepts only canonical bytes, so they are sent as they are.
///
/// # Errors
///
/// As [`get_tree`].
pub fn get_tree_body(c: &Connection, id: TreeId) -> Result<Vec<u8>> {
    load_tree(c, id).map(|(_, body)| body)
}

/// The length of a tree's stored `TreeV1` bytes, without reading them.
///
/// # Errors
///
/// [`Error::NotFound`] if absent.
pub fn tree_size(c: &Connection, id: TreeId) -> Result<usize> {
    let size: i64 = c
        .prepare_cached("SELECT length(body) FROM trees WHERE id = ?1")?
        .query_row([id.as_bytes()], |row| row.get(0))
        .optional()?
        .ok_or_else(|| Error::NotFound(format!("tree {id}")))?;
    usize::try_from(size).map_err(|_| Error::Corrupt(format!("tree {id}: length {size}")))
}

fn load_tree(c: &Connection, id: TreeId) -> Result<(Tree, Vec<u8>)> {
    let body: Vec<u8> = c
        .prepare_cached("SELECT body FROM trees WHERE id = ?1")?
        .query_row([id.as_bytes()], |row| row.get(0))
        .optional()?
        .ok_or_else(|| Error::NotFound(format!("tree {id}")))?;
    let (tree, actual) =
        Tree::decode_with_id(&body).map_err(|e| Error::Corrupt(format!("tree {id}: {e}")))?;
    if actual != id {
        return Err(Error::Corrupt(format!("tree {id} does not match its ID")));
    }
    Ok((tree, body))
}

fn to_id(bytes: &[u8]) -> Result<ChangesetId> {
    ChangesetId::try_from(bytes).map_err(|e| Error::Corrupt(format!("stored changeset ID: {e}")))
}
