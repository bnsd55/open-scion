//! Queries on the shared tables: repos, changesets, the blobs of each repo,
//! refs, trees and derived data. Tables owned by one module (lands, git
//! import) keep their SQL there.
//!
//! Each function takes a `&Connection`, so it runs the same inside or outside
//! a transaction (a `Transaction` derefs to a `Connection`).

use std::collections::HashSet;

use rusqlite::{Connection, OptionalExtension, params};
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath, Tree, TreeId};

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
/// `repo`. Safe to repeat. [`tree::store_changeset`](crate::tree::store_changeset)
/// also adds the blobs that it names to `repo`.
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

/// The puts of a changeset's changes: each path and its blob, in path
/// order, with repeats. A gitlink names no blob, whatever its bytes.
pub(crate) fn puts(cs: &Changeset) -> impl Iterator<Item = (&RepoPath, BlobId)> + '_ {
    cs.changes
        .iter()
        .filter_map(|(path, state)| Some((path, state.as_ref()?.blob()?)))
}

/// Adds `blob` to the blobs that `repo` may name. Returns whether it is new.
///
/// # Errors
///
/// Database errors.
pub fn add_repo_blob(c: &Connection, repo: RepoId, blob: BlobId) -> Result<bool> {
    let added = c
        .prepare_cached("INSERT OR IGNORE INTO repo_blobs (repo_id, blob) VALUES (?1, ?2)")?
        .execute(params![repo.0, blob.as_bytes()])?;
    Ok(added > 0)
}

/// Whether `repo` may name `blob`: it was uploaded to `repo`, or a changeset
/// of `repo` names it. Whether the blob is stored is another question.
///
/// # Errors
///
/// Database errors.
pub fn repo_has_blob(c: &Connection, repo: RepoId, blob: BlobId) -> Result<bool> {
    Ok(
        c.prepare_cached("SELECT 1 FROM repo_blobs WHERE repo_id = ?1 AND blob = ?2")?
            .exists(params![repo.0, blob.as_bytes()])?,
    )
}

/// Changesets read between two progress lines of [`fill_repo_blobs`].
const FILL_LOG_EVERY: u64 = 10_000;

/// The upgrade to schema version 4: adds the blobs that each repo's
/// changesets name to `repo_blobs`. A changeset lists every path that differs
/// from its first parent, so the puts of all the changesets of a repo name
/// every blob that the repo's trees name. Reads each changeset once, inside
/// the migration's one transaction, and logs its progress. Returns the
/// changesets read and the rows added.
///
/// # Errors
///
/// [`Error::Corrupt`] if a changeset does not decode or match its ID.
/// Database errors.
pub(crate) fn fill_repo_blobs(c: &Connection) -> Result<(u64, u64)> {
    let total: u64 = c
        .query_row(
            "SELECT count(*) FROM repo_changesets JOIN changesets ON cs = id",
            [],
            |row| row.get::<_, i64>(0),
        )?
        .cast_unsigned();
    tracing::info!(changesets = total, "schema 4: adding each repo's blobs");
    let mut select =
        c.prepare("SELECT repo_id, id, body FROM repo_changesets JOIN changesets ON cs = id")?;
    let mut rows = select.query([])?;
    let (mut read, mut added) = (0, 0);
    while let Some(row) = rows.next()? {
        let repo = RepoId(row.get(0)?);
        let id = ChangesetId::from_bytes(row.get(1)?);
        let body: Vec<u8> = row.get(2)?;
        let (cs, actual) = Changeset::decode_with_id(&body)
            .map_err(|e| Error::Corrupt(format!("changeset {id}: {e}")))?;
        if actual != id {
            return Err(Error::Corrupt(format!(
                "changeset {id} does not match its ID"
            )));
        }
        for (_, blob) in puts(&cs) {
            added += u64::from(add_repo_blob(c, repo, blob)?);
        }
        read += 1;
        if read % FILL_LOG_EVERY == 0 {
            tracing::info!(
                changesets = read,
                of = total,
                rows = added,
                "schema 4: adding each repo's blobs"
            );
        }
    }
    tracing::info!(
        changesets = read,
        rows = added,
        "schema 4: added each repo's blobs"
    );
    Ok((read, added))
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
        .prepare_cached("SELECT parent FROM changeset_parents WHERE cs = ?1 AND n = 0")?
        .query_row([id.as_bytes()], |row| row.get(0))
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
pub(crate) fn parents(
    c: &Connection,
    id: ChangesetId,
    id_gen: i64,
) -> Result<Vec<(ChangesetId, i64)>> {
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

/// Deletes up to `limit` derived values of `kind` with a version below
/// `version`, and returns how many. The primary key starts with (kind,
/// version), so this reads only the rows it deletes.
///
/// # Errors
///
/// Database errors.
pub fn delete_derived_below(
    c: &Connection,
    kind: &str,
    version: u32,
    limit: usize,
) -> Result<usize> {
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    Ok(c.prepare_cached(
        "DELETE FROM derived WHERE (kind, version, cs) IN (
             SELECT kind, version, cs FROM derived
             WHERE kind = ?1 AND version < ?2 LIMIT ?3)",
    )?
    .execute(params![kind, version, limit])?)
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

/// Commit graphs for tests.
#[cfg(test)]
pub(crate) mod testing {
    use proptest::prelude::{Strategy, any};
    use proptest::sample::Index;
    use scion_core::{BlobId, FileState, IdentityTime, RepoPath};

    use super::*;

    /// Stores a graph of changesets in `repo` with [`insert_changeset`], so
    /// with no derived rows. Changeset `i` has the parents `parents[i]`, in
    /// order, each an earlier changeset, and puts the file `f{i}`. Returns
    /// the IDs.
    pub(crate) fn insert_graph(
        c: &Connection,
        repo: RepoId,
        parents: &[Vec<usize>],
    ) -> Result<Vec<ChangesetId>> {
        let mut ids: Vec<ChangesetId> = Vec::new();
        for (i, of) in parents.iter().enumerate() {
            let path = format!("f{i}");
            let state = FileState::Regular {
                content: BlobId::of(path.as_bytes()),
                executable: false,
            };
            let cs = Changeset {
                parents: of.iter().map(|&p| ids[p]).collect(),
                author: IdentityTime {
                    name: b"a".to_vec(),
                    email: b"a@x".to_vec(),
                    unix_secs: 0,
                    tz_offset_minutes: 0,
                },
                committer: None,
                message: path.clone().into_bytes(),
                changes: [(RepoPath::new(path).unwrap(), Some(state))].into(),
            };
            ids.push(insert_changeset(c, repo, &cs)?);
        }
        Ok(ids)
    }

    /// Graphs of 1 to `max` changesets for [`insert_graph`]. Most have a
    /// first parent a few changesets back, so first-parent histories are
    /// long. Some have no parent, and some have up to two more parents
    /// anywhere before them, so there are several roots, merges and
    /// criss-crosses.
    pub(crate) fn graphs(max: usize) -> impl Strategy<Value = Vec<Vec<usize>>> {
        let node = (
            proptest::option::weighted(0.9, 1..4_usize),
            proptest::collection::vec(any::<Index>(), 0..3),
        );
        proptest::collection::vec(node, 1..=max).prop_map(|nodes| {
            (0..)
                .zip(nodes)
                .map(|(i, (back, others))| {
                    let mut parents: Vec<usize> = Vec::new();
                    if i > 0 {
                        let first = back.map(|back| i - back.min(i));
                        for p in first.into_iter().chain(others.iter().map(|o| o.index(i))) {
                            if !parents.contains(&p) {
                                parents.push(p);
                            }
                        }
                    }
                    parents
                })
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use scion_core::{FileState, GitOid, IdentityTime, RepoPath};

    use super::*;
    use crate::Store;
    use crate::store::migrate;

    fn changeset(parents: Vec<ChangesetId>, changes: &[(&str, Option<FileState>)]) -> Changeset {
        Changeset {
            parents,
            author: IdentityTime {
                name: b"a".to_vec(),
                email: b"a@x".to_vec(),
                unix_secs: 0,
                tz_offset_minutes: 0,
            },
            committer: None,
            message: Vec::new(),
            changes: changes
                .iter()
                .map(|(path, state)| (RepoPath::new(*path).unwrap(), *state))
                .collect(),
        }
    }

    fn file(content: &str) -> FileState {
        FileState::Regular {
            content: BlobId::of(content.as_bytes()),
            executable: false,
        }
    }

    #[test]
    fn the_upgrade_to_version_4_adds_the_blobs_of_each_repo() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, c) = {
            let db = Connection::open(dir.path().join("scion.db")).unwrap();
            migrate(&db, 3).unwrap();
            let insert = |repo, cs: &Changeset| insert_changeset(&db, repo, cs).unwrap();
            let [a, b, c] = ["a", "b", "c"].map(|name| create_repo(&db, name).unwrap());
            // Repo a: a root, a child that deletes a file, and a branch that
            // a merge joins without its file.
            let root = changeset(vec![], &[("x", Some(file("x"))), ("y", Some(file("y")))]);
            let root_id = insert(a, &root);
            let child = insert(
                a,
                &changeset(vec![root_id], &[("x", None), ("z", Some(file("z")))]),
            );
            let side = insert(a, &changeset(vec![root_id], &[("s", Some(file("s")))]));
            insert(a, &changeset(vec![child, side], &[]));
            // Repo b: the same root, a symlink and a submodule whose
            // commit bytes are repo a's "z" blob. A gitlink names no blob,
            // so repo b gains nothing for it, whatever the 32 bytes are.
            insert(b, &root);
            let link = FileState::Symlink {
                target: BlobId::of(b"t"),
            };
            let module = FileState::Gitlink {
                commit: GitOid::Sha256(*BlobId::of(b"z").as_bytes()),
            };
            insert(
                b,
                &changeset(vec![root_id], &[("l", Some(link)), ("m", Some(module))]),
            );
            // Repo c: nothing.
            (a, b, c)
        };

        let store = Store::open(dir.path()).unwrap();
        let rows: Vec<(i64, [u8; 32])> = store
            .read(|db| {
                let mut select = db.prepare("SELECT repo_id, blob FROM repo_blobs")?;
                let rows = select.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
                Ok(rows.collect::<rusqlite::Result<_>>()?)
            })
            .unwrap();
        let mut got: BTreeMap<i64, BTreeSet<BlobId>> = BTreeMap::new();
        for (repo, blob) in rows {
            got.entry(repo)
                .or_default()
                .insert(BlobId::from_bytes(blob));
        }
        let blobs = |names: &[&str]| names.iter().map(|n| BlobId::of(n.as_bytes())).collect();
        let want = BTreeMap::from([
            (a.0, blobs(&["x", "y", "z", "s"])),
            (b.0, blobs(&["x", "y", "t"])),
        ]);
        assert_eq!(got, want);
        assert!(!got.contains_key(&c.0));
    }
}
