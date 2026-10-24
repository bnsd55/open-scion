//! The SQLite queries on the shared tables: repos, changesets, the blobs
//! of each repo, refs, trees and derived data. They are the methods of
//! [`MetaTx`] that this module implements for
//! [`SqliteTx`]. Tables owned by one module (lands, git import) keep their
//! SQL there, in impls of the same trait.
//!
//! Each query takes the transaction, so it runs the same inside or outside
//! a write transaction. Tests that hold a `Connection` reach the queries
//! through [`SqliteTx`], which derefs to the connection.

use std::collections::HashMap;
#[cfg(any(test, feature = "test-hooks"))]
use std::collections::HashSet;
use std::ops::Deref;

use gix::ObjectId;

use crate::auth::Caller;
use rusqlite::{Connection, OptionalExtension, params};
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath, Tree, TreeId};

use crate::error::{Error, Result};
use crate::history::NodeId;
use crate::import::SHA1;
use crate::land::{
    EXPIRE_PER_LAND, LAND_RETENTION_SECS, LandOutcome, LandRequest, LandState, decode_paths,
    encode_paths,
};
use crate::meta::{FsckDerivedRow, MetaTx, RefState, RepoChangeset, RepoId, StaleLand};

#[cfg(test)]
thread_local! {
    /// Changesets that [`get_changeset`] read on this thread. Tests use it
    /// to bound the reads of a query.
    pub(crate) static CHANGESET_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-hooks"))]
#[path = "test_support/db.rs"]
pub(crate) mod test_hooks;

/// The transaction handle of the SQLite [`Store`](crate::store::Store): a
/// reader, or the write connection inside one transaction. The queries of
/// the shared tables are its [`MetaTx`] methods.
///
/// `pub` with the unreachable-pub allow: [`Meta::Tx`] names it, and the
/// trait re-exports through `test_support::meta` (the #333 seam), so the
/// type must stay as visible as the trait that carries it.
#[allow(unreachable_pub)]
pub struct SqliteTx<'a>(pub(crate) &'a Connection);

impl Deref for SqliteTx<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.0
    }
}

/// The error of a repo that does not exist. A caller that may not read a
/// repo gets it too (`access::Access::check_read`).
pub(crate) fn no_repo(name: &str) -> Error {
    Error::NotFound(format!("repo {name:?}"))
}

/// The puts of a changeset's changes: each path and its blob, in path
/// order, with repeats. A gitlink names no blob, whatever its bytes.
pub(crate) fn puts(cs: &Changeset) -> impl Iterator<Item = (&RepoPath, BlobId)> + '_ {
    cs.changes()
        .iter()
        .filter_map(|(path, state)| Some((path, state.as_ref()?.blob()?)))
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
        )
        .map_err(crate::error::from_sqlite)?
        .cast_unsigned();
    tracing::info!(changesets = total, "schema 4: adding each repo's blobs");
    let mut select = c
        .prepare("SELECT repo_id, id, body FROM repo_changesets JOIN changesets ON cs = id")
        .map_err(crate::error::from_sqlite)?;
    let mut rows = select.query([]).map_err(crate::error::from_sqlite)?;
    let (mut read, mut added) = (0, 0);
    while let Some(row) = rows.next().map_err(crate::error::from_sqlite)? {
        let repo = RepoId(row.get(0).map_err(crate::error::from_sqlite)?);
        let id = ChangesetId::from_bytes(row.get(1).map_err(crate::error::from_sqlite)?);
        let body: Vec<u8> = row.get(2).map_err(crate::error::from_sqlite)?;
        let (cs, actual) = Changeset::decode_with_id(&body)
            .map_err(|e| Error::Corrupt(format!("changeset {id}: {e}")))?;
        if actual != id {
            return Err(Error::Corrupt(format!(
                "changeset {id} does not match its ID"
            )));
        }
        for (_, blob) in puts(&cs) {
            added += u64::from(SqliteTx(c).add_repo_blob(repo, blob)?);
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

/// Reads a tree's rows and checks them against its ID.
fn load_tree(c: &Connection, id: TreeId) -> Result<(Tree, Vec<u8>)> {
    let body: Vec<u8> = c
        .prepare_cached("SELECT body FROM trees WHERE id = ?1")
        .map_err(crate::error::from_sqlite)?
        .query_row([id.as_bytes()], |row| row.get(0))
        .optional()
        .map_err(crate::error::from_sqlite)?
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

impl MetaTx for SqliteTx<'_> {
    /// Creates a repository, or returns the existing one with that name.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn create_repo(&mut self, name: &str) -> Result<RepoId> {
        self.0
            .prepare_cached("INSERT OR IGNORE INTO repos (name) VALUES (?1)")
            .map_err(crate::error::from_sqlite)?
            .execute([name])
            .map_err(crate::error::from_sqlite)?;
        self.repo_id(name)
    }

    /// Looks up a repository by name.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if no repository has that name.
    fn repo_id(&mut self, name: &str) -> Result<RepoId> {
        self.0
            .prepare_cached("SELECT repo_id FROM repos WHERE name = ?1")
            .map_err(crate::error::from_sqlite)?
            .query_row([name], |row| row.get(0).map(RepoId))
            .optional()
            .map_err(crate::error::from_sqlite)?
            .ok_or_else(|| no_repo(name))
    }

    /// Stores a changeset and adds it to `repo`. Every parent must already be in
    /// `repo`. Safe to repeat. [`tree::store_changeset`](crate::tree::store_changeset)
    /// also adds the blobs that it names to `repo`.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if a parent is not in `repo`.
    /// Turns the importer's changeset batching on or off. SQLite writes
    /// every row as its statement anyway (one write connection, so a
    /// batch buys no round trips), so this holds nothing.
    fn set_changeset_batching(&mut self, _on: bool) {}

    fn preload_derived_chain(
        &mut self,
        _kind: &str,
        _version: u32,
        _start: ChangesetId,
        _limit: i64,
    ) -> Result<()> {
        Ok(())
    }

    fn changeset_batching(&self) -> bool {
        false
    }

    /// Writes what the batching held back: nothing, on SQLite.
    ///
    /// # Errors
    ///
    /// None.
    fn flush_changeset_rows(&mut self, _repo: RepoId) -> Result<()> {
        Ok(())
    }

    fn insert_changeset(&mut self, repo: RepoId, cs: &Changeset) -> Result<ChangesetId> {
        let (body, id) = cs.encode_with_id();
        let mut parent_gen = 0;
        for parent in cs.parents() {
            parent_gen = parent_gen.max(self.generation(repo, *parent)?);
        }
        self.0
            .prepare_cached("INSERT OR IGNORE INTO changesets (id, gen, body) VALUES (?1, ?2, ?3)")
            .map_err(crate::error::from_sqlite)?
            .execute(params![id.as_bytes(), parent_gen + 1, body])
            .map_err(crate::error::from_sqlite)?;
        for (n, parent) in (0_i64..).zip(cs.parents()) {
            self.0
                .prepare_cached(
                    "INSERT OR IGNORE INTO changeset_parents (cs, n, parent) VALUES (?1, ?2, ?3)",
                )
                .map_err(crate::error::from_sqlite)?
                .execute(params![id.as_bytes(), n, parent.as_bytes()])
                .map_err(crate::error::from_sqlite)?;
        }
        self.0
            .prepare_cached("INSERT OR IGNORE INTO repo_changesets (repo_id, cs) VALUES (?1, ?2)")
            .map_err(crate::error::from_sqlite)?
            .execute(params![repo.0, id.as_bytes()])
            .map_err(crate::error::from_sqlite)?;
        Ok(id)
    }

    /// Adds `blob` to the blobs that `repo` may name. Returns whether it is new.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn add_repo_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        let added = self
            .0
            .prepare_cached("INSERT OR IGNORE INTO repo_blobs (repo_id, blob) VALUES (?1, ?2)")
            .map_err(crate::error::from_sqlite)?
            .execute(params![repo.0, blob.as_bytes()])
            .map_err(crate::error::from_sqlite)?;
        Ok(added > 0)
    }

    /// Whether `repo` may name `blob`: it was uploaded to `repo`, or a changeset
    /// of `repo` names it. Whether the blob is stored is another question.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn repo_has_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        self.0
            .prepare_cached("SELECT 1 FROM repo_blobs WHERE repo_id = ?1 AND blob = ?2")
            .map_err(crate::error::from_sqlite)?
            .exists(params![repo.0, blob.as_bytes()])
            .map_err(crate::error::from_sqlite)
    }

    /// The blobs of the list that the repo does not have. A loop over
    /// [`repo_has_blob`](Self::repo_has_blob) is fine here: the check is
    /// local, so a list of any length costs no round trips.
    fn repo_missing_blobs(&mut self, repo: RepoId, blobs: &[BlobId]) -> Result<Vec<BlobId>> {
        let mut missing = Vec::new();
        for blob in blobs {
            if !self.repo_has_blob(repo, *blob)? {
                missing.push(*blob);
            }
        }
        Ok(missing)
    }

    /// Reads a changeset that `repo` may see and checks it against its ID.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not have it, [`Error::Corrupt`] if it
    /// does not decode or match.
    fn get_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<Changeset> {
        #[cfg(test)]
        CHANGESET_READS.set(CHANGESET_READS.get() + 1);
        let body: Vec<u8> = self
            .0
            .query_row(
                "SELECT body FROM changesets JOIN repo_changesets ON cs = id
                 WHERE repo_id = ?1 AND id = ?2",
                params![repo.0, id.as_bytes()],
                |row| row.get(0),
            )
            .optional()
            .map_err(crate::error::from_sqlite)?
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
    fn has_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<bool> {
        self.0
            .prepare_cached("SELECT 1 FROM repo_changesets WHERE repo_id = ?1 AND cs = ?2")
            .map_err(crate::error::from_sqlite)?
            .exists(params![repo.0, id.as_bytes()])
            .map_err(crate::error::from_sqlite)
    }

    /// One page of the first-parent chain, one statement: the recursive
    /// term follows the indexed first-parent rows (cs, n = 0), and the
    /// join carries the generation and the body. Every fetched changeset
    /// is an ancestor of `start`, so the repo check rides the start row,
    /// as `get_changeset` checks it.
    fn first_parent_page(
        &mut self,
        repo: RepoId,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, i64, Vec<u8>)>> {
        let mut stmt = self
            .0
            .prepare_cached(
                "WITH RECURSIVE walk(id, depth) AS (
                     SELECT id, 0 FROM changesets JOIN repo_changesets ON cs = id
                     WHERE repo_id = ?1 AND id = ?2
                     UNION ALL
                     SELECT cp.parent, w.depth + 1 FROM walk w
                     JOIN changeset_parents cp ON cp.cs = w.id AND cp.n = 0
                     WHERE w.depth < ?3
                 )
                 SELECT c.id, c.gen, c.body FROM walk
                 JOIN changesets c ON c.id = walk.id
                 ORDER BY walk.depth",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(params![repo.0, start.as_bytes(), limit], |row| {
                let cs: Vec<u8> = row.get(0)?;
                let generation: i64 = row.get(1)?;
                let body: Vec<u8> = row.get(2)?;
                Ok((cs, generation, body))
            })
            .map_err(crate::error::from_sqlite)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)?;
        if rows.is_empty() {
            return Err(Error::NotFound(format!("changeset {start}")));
        }
        rows.into_iter()
            .map(|(cs, generation, body)| {
                let id = ChangesetId::try_from(cs.as_slice()).map_err(|_| {
                    Error::Corrupt("a changeset row's id is not a changeset id".to_owned())
                })?;
                Ok((id, generation, body))
            })
            .collect()
    }

    /// The generation of a changeset that `repo` may see: 1 for a root, else 1 +
    /// the highest parent generation.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not have it.
    fn generation(&mut self, repo: RepoId, id: ChangesetId) -> Result<i64> {
        self.0
            .prepare_cached(
                "SELECT gen FROM changesets JOIN repo_changesets ON cs = id
             WHERE repo_id = ?1 AND id = ?2",
            )
            .map_err(crate::error::from_sqlite)?
            .query_row(params![repo.0, id.as_bytes()], |row| row.get(0))
            .optional()
            .map_err(crate::error::from_sqlite)?
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")))
    }

    /// The generation of a changeset, in any repo. For derived data: the caller
    /// reaches `id` from a changeset whose repo it checked.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if no changeset has this ID.
    fn generation_of(&mut self, id: ChangesetId) -> Result<i64> {
        self.0
            .prepare_cached("SELECT gen FROM changesets WHERE id = ?1")
            .map_err(crate::error::from_sqlite)?
            .query_row([id.as_bytes()], |row| row.get(0))
            .optional()
            .map_err(crate::error::from_sqlite)?
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")))
    }

    /// The first parent of a changeset, or `None` for a root.
    ///
    /// # Errors
    ///
    /// Database errors.
    #[cfg(any(test, feature = "test-hooks"))]
    #[cfg(any(test, feature = "test-hooks"))]
    fn first_parent(&mut self, id: ChangesetId) -> Result<Option<ChangesetId>> {
        let parent: Option<Vec<u8>> = self
            .0
            .prepare_cached("SELECT parent FROM changeset_parents WHERE cs = ?1 AND n = 0")
            .map_err(crate::error::from_sqlite)?
            .query_row([id.as_bytes()], |row| row.get(0))
            .optional()
            .map_err(crate::error::from_sqlite)?;
        parent.map(|bytes| to_id(&bytes)).transpose()
    }

    /// Every parent of changeset `id`, whose generation is `id_gen`, in order,
    /// with its generation. The parents of a changeset in a repo are in that
    /// repo too ([`MetaTx::insert_changeset`]). The graph walks rely on each parent's
    /// generation being below its child's, so this checks it.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if a parent's generation is not below `id_gen`.
    fn parents(&mut self, id: ChangesetId, id_gen: i64) -> Result<Vec<(ChangesetId, i64)>> {
        let mut parents = self
            .0
            .prepare_cached(
                "SELECT parent, gen FROM changeset_parents JOIN changesets ON id = parent
             WHERE cs = ?1 ORDER BY n",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = parents
            .query_map([id.as_bytes()], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(crate::error::from_sqlite)?;
        rows.map(|row| {
            let (parent, parent_gen) = row.map_err(crate::error::from_sqlite)?;
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

    fn parents_many(
        &mut self,
        merges: &[(ChangesetId, i64)],
    ) -> Result<Vec<Vec<(ChangesetId, i64)>>> {
        // Local, no round trip: one prepared statement over the batch.
        merges
            .iter()
            .map(|&(id, id_gen)| self.parents(id, id_gen))
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
    #[cfg(any(test, feature = "test-hooks"))]
    #[cfg(any(test, feature = "test-hooks"))]
    fn is_ancestor(
        &mut self,
        repo: RepoId,
        ancestor: ChangesetId,
        descendant: ChangesetId,
        max_visits: usize,
    ) -> Result<bool> {
        let floor = self.generation(repo, ancestor)?;
        let mut seen = HashSet::new();
        let mut stack = vec![(descendant, self.generation(repo, descendant)?)];
        while let Some((id, id_gen)) = stack.pop() {
            if id == ancestor {
                return Ok(true);
            }
            if id_gen <= floor || !seen.insert(id) {
                continue;
            }
            if seen.len() > max_visits {
                return Err(test_hooks::too_deep(max_visits));
            }
            stack.extend(self.parents(id, id_gen)?);
        }
        Ok(false)
    }

    /// Where a ref points, if it exists.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn get_ref(&mut self, repo: RepoId, name: &str) -> Result<Option<RefState>> {
        self.0
            .prepare_cached("SELECT target, version FROM refs WHERE repo_id = ?1 AND name = ?2")
            .map_err(crate::error::from_sqlite)?
            .query_row(params![repo.0, name], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get(1)?))
            })
            .optional()
            .map_err(crate::error::from_sqlite)?
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
    fn cas_ref(
        &mut self,
        repo: RepoId,
        name: &str,
        expected: Option<i64>,
        target: ChangesetId,
    ) -> Result<bool> {
        self.generation(repo, target)?;
        let changed = match expected {
            None => self.0.prepare_cached("INSERT OR IGNORE INTO refs (repo_id, name, target, version) VALUES (?1, ?2, ?3, 1)").map_err(crate::error::from_sqlite)?.execute(
                params![repo.0, name, target.as_bytes()],
            ).map_err(crate::error::from_sqlite)?,
            Some(version) => self.0.prepare_cached("UPDATE refs SET target = ?3, version = version + 1
                 WHERE repo_id = ?1 AND name = ?2 AND version = ?4").map_err(crate::error::from_sqlite)?.execute(
                params![repo.0, name, target.as_bytes(), version],
            ).map_err(crate::error::from_sqlite)?,
        };
        Ok(changed == 1)
    }

    fn restore_ref(&mut self, repo: RepoId, name: &str, target: &[u8], version: i64) -> Result<()> {
        self.0
            .prepare_cached(
                "INSERT OR REPLACE INTO refs (repo_id, name, target, version) \
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .map_err(crate::error::from_sqlite)?
            .execute(params![repo.0, name, target, version])
            .map_err(crate::error::from_sqlite)?;
        Ok(())
    }

    /// The stored bytes of a derived value, if computed.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn get_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
    ) -> Result<Option<Vec<u8>>> {
        self.0
            .prepare_cached(
                "SELECT value FROM derived WHERE kind = ?1 AND version = ?2 AND cs = ?3",
            )
            .map_err(crate::error::from_sqlite)?
            .query_row(params![kind, version, cs.as_bytes()], |row| row.get(0))
            .optional()
            .map_err(crate::error::from_sqlite)
    }

    fn get_derived_batch(
        &mut self,
        kind: &str,
        version: u32,
        ids: &[ChangesetId],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        // One prepared statement reused over the batch, inside the caller's
        // one transaction: a land between two ids of the batch cannot mix
        // two states.
        let mut stmt = self
            .0
            .prepare_cached(
                "SELECT value FROM derived WHERE kind = ?1 AND version = ?2 AND cs = ?3",
            )
            .map_err(crate::error::from_sqlite)?;
        ids.iter()
            .map(|cs| {
                stmt.query_row(params![kind, version, cs.as_bytes()], |row| row.get(0))
                    .optional()
                    .map_err(crate::error::from_sqlite)
            })
            .collect()
    }

    fn get_derived_chain(
        &mut self,
        kind: &str,
        version: u32,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, Vec<u8>)>> {
        // One statement for the whole chain: the recursive term follows the
        // indexed first-parent rows (cs, n = 0), and the join keeps the
        // rows that carry a stored value. Every fetched changeset is an
        // ancestor of `start`, so no repo filter is needed. The reply is
        // in walk order: newest first, from the asked node, the way
        // first_parent_page orders.
        let mut stmt = self
            .0
            .prepare_cached(
                "WITH RECURSIVE walk(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT cp.parent, w.depth + 1
                 FROM walk w
                 JOIN changeset_parents cp ON cp.cs = w.id AND cp.n = 0
                 WHERE w.depth < ?2
             )
             SELECT d.cs, d.value FROM walk
             JOIN derived d ON d.cs = walk.id
             WHERE d.kind = ?3 AND d.version = ?4
             ORDER BY walk.depth",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(params![start.as_bytes(), limit, kind, version], |row| {
                let cs: Vec<u8> = row.get(0)?;
                let value: Vec<u8> = row.get(1)?;
                Ok((cs, value))
            })
            .map_err(crate::error::from_sqlite)?;
        rows.map(|row| {
            let (cs, value) = row.map_err(crate::error::from_sqlite)?;
            Ok((
                ChangesetId::try_from(cs.as_slice()).map_err(|_| {
                    Error::Corrupt("a derived row's changeset is not a changeset id".to_owned())
                })?,
                value,
            ))
        })
        .collect()
    }

    /// Records a derived value. A second write must agree with the first:
    /// derivation is deterministic, so a mismatch means corruption.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if a different value is already stored.
    fn put_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
        value: &[u8],
    ) -> Result<()> {
        let inserted = self
            .0
            .prepare_cached(
                "INSERT OR IGNORE INTO derived (kind, version, cs, value) VALUES (?1, ?2, ?3, ?4)",
            )
            .map_err(crate::error::from_sqlite)?
            .execute(params![kind, version, cs.as_bytes(), value])
            .map_err(crate::error::from_sqlite)?;
        if inserted == 0 && self.get_derived(kind, version, cs)?.as_deref() != Some(value) {
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
    fn delete_derived_below(&mut self, kind: &str, version: u32, limit: usize) -> Result<usize> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.0
            .prepare_cached(
                "DELETE FROM derived WHERE (kind, version, cs) IN (
                 SELECT kind, version, cs FROM derived
                 WHERE kind = ?1 AND version < ?2 LIMIT ?3)",
            )
            .map_err(crate::error::from_sqlite)?
            .execute(params![kind, version, limit])
            .map_err(crate::error::from_sqlite)
    }

    /// Stores a tree. Safe to repeat.
    ///
    fn get_view_tree(
        &mut self,
        repo: RepoId,
        digest: &[u8; 32],
        version: u32,
        tree: TreeId,
        ctx: [u8; 32],
    ) -> Result<Option<TreeId>> {
        let bytes: Option<Vec<u8>> = self
            .0
            .prepare_cached(
                "SELECT view FROM view_trees \
                 WHERE repo_id = ?1 AND digest = ?2 AND version = ?3 AND tree = ?4 AND ctx = ?5",
            )
            .map_err(crate::error::from_sqlite)?
            .query_row(
                params![repo.0, digest, version, tree.as_bytes(), ctx],
                |row| row.get(0),
            )
            .optional()
            .map_err(crate::error::from_sqlite)?;
        bytes
            .map(|b| TreeId::try_from(b.as_slice()).map_err(|_| Error::Corrupt("bad view".into())))
            .transpose()
    }

    fn put_view_tree(
        &mut self,
        repo: RepoId,
        digest: &[u8; 32],
        version: u32,
        tree: TreeId,
        ctx: [u8; 32],
        view: TreeId,
    ) -> Result<()> {
        let inserted = self
            .0
            .prepare_cached(
                "INSERT OR IGNORE INTO view_trees \
                 (repo_id, digest, version, tree, ctx, view) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .map_err(crate::error::from_sqlite)?
            .execute(params![
                repo.0,
                digest,
                version,
                tree.as_bytes(),
                ctx,
                view.as_bytes()
            ])
            .map_err(crate::error::from_sqlite)?;
        if inserted == 0 && self.get_view_tree(repo, digest, version, tree, ctx)? != Some(view) {
            return Err(Error::Corrupt(format!(
                "the view of tree {tree} under its context differs from the stored value"
            )));
        }
        Ok(())
    }

    fn delete_stale_view_trees(
        &mut self,
        repo: RepoId,
        live: &[[u8; 32]],
        version: u32,
        limit: usize,
    ) -> Result<usize> {
        // Rows of an older projection version are dead, whoever used them.
        // The LIMIT lives in the subquery: this SQLite build has no
        // UPDATE_DELETE_LIMIT, as the derived-row expiry shows.
        let bound = i64::try_from(limit).map_err(|_| Error::InvalidArgument("limit".into()))?;
        let mut removed = self
            .0
            .prepare_cached(
                "DELETE FROM view_trees WHERE (repo_id, digest, version, tree, ctx) IN (
                 SELECT repo_id, digest, version, tree, ctx FROM view_trees
                 WHERE repo_id = ?1 AND version < ?2 LIMIT ?3)",
            )
            .map_err(crate::error::from_sqlite)?
            .execute(params![repo.0, version, bound])
            .map_err(crate::error::from_sqlite)?;
        // Rows of a digest that no grant of this repo hashes to any more,
        // as far as this server's access file knows. All binds positional:
        // a numbered list would share its indexes with the columns. `live`
        // is never empty here: the deriving caller's digest is among the
        // digests of its access snapshot (the assert in `view_root`), and
        // a caller that cannot prove that has no business sweeping.
        if removed < limit && !live.is_empty() {
            let marks = std::iter::repeat_n("?", live.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "DELETE FROM view_trees WHERE (repo_id, digest, version, tree, ctx) IN (
                 SELECT repo_id, digest, version, tree, ctx FROM view_trees
                 WHERE repo_id = ? AND version = ? AND digest NOT IN ({marks})
                 LIMIT ?)"
            );
            let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                vec![Box::new(repo.0), Box::new(version)];
            params.extend(
                live.iter()
                    .map(|d| Box::new(*d) as Box<dyn rusqlite::ToSql>),
            );
            params.push(Box::new(bound - i64::try_from(removed).unwrap_or(bound)));
            let refs: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(std::convert::AsRef::as_ref).collect();
            removed += self
                .prepare_cached(&sql)
                .map_err(crate::error::from_sqlite)?
                .execute(refs.as_slice())
                .map_err(crate::error::from_sqlite)?;
        }
        Ok(removed)
    }

    /// # Errors
    ///
    /// Database errors.
    fn put_tree(&mut self, tree: &Tree) -> Result<TreeId> {
        let (body, id) = tree.encode_with_id();
        self.0
            .prepare_cached("INSERT OR IGNORE INTO trees (id, body) VALUES (?1, ?2)")
            .map_err(crate::error::from_sqlite)?
            .execute(params![id.as_bytes(), body])
            .map_err(crate::error::from_sqlite)?;
        Ok(id)
    }

    /// Reads a tree and checks it against its ID.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent, [`Error::Corrupt`] if it does not decode or
    /// match.
    fn get_tree(&mut self, id: TreeId) -> Result<Tree> {
        load_tree(self.0, id).map(|(tree, _)| tree)
    }

    /// The stored bytes of the trees the ids name, in the order asked.
    /// SQLite has one write connection: the existing per-id read, in a
    /// loop, is the batch.
    fn get_trees(&mut self, ids: &[TreeId]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut bodies = Vec::with_capacity(ids.len());
        let mut stmt = self
            .0
            .prepare_cached("SELECT body FROM trees WHERE id = ?1")
            .map_err(crate::error::from_sqlite)?;
        for id in ids {
            let body = stmt
                .query_row([id.as_bytes()], |row| row.get(0))
                .optional()
                .map_err(crate::error::from_sqlite)?;
            bodies.push(body);
        }
        Ok(bodies)
    }

    /// Reads a tree's stored `TreeV1` bytes and checks them against its ID. The
    /// strict decoder accepts only canonical bytes, so they are sent as they are.
    ///
    /// # Errors
    ///
    /// As [`MetaTx::get_tree`].
    fn get_tree_body(&mut self, id: TreeId) -> Result<Vec<u8>> {
        load_tree(self.0, id).map(|(_, body)| body)
    }

    /// The length of a tree's stored `TreeV1` bytes, without reading them.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent.
    fn tree_size(&mut self, id: TreeId) -> Result<usize> {
        let size: i64 = self
            .0
            .prepare_cached("SELECT length(body) FROM trees WHERE id = ?1")
            .map_err(crate::error::from_sqlite)?
            .query_row([id.as_bytes()], |row| row.get(0))
            .optional()
            .map_err(crate::error::from_sqlite)?
            .ok_or_else(|| Error::NotFound(format!("tree {id}")))?;
        usize::try_from(size).map_err(|_| Error::Corrupt(format!("tree {id}: length {size}")))
    }

    /// Inserts a land, or finds the land for the same request key. A land to a
    /// ref that does not exist is recorded as rejected at once, so only real
    /// refs ever get a lock slot in [`Lands`](crate::land::Lands). A new land also expires old
    /// ones.
    fn record_land(
        &mut self,
        caller: &Caller,
        req: &LandRequest,
        pending_limit: Option<usize>,
    ) -> Result<(RepoId, LandOutcome)> {
        let repo = self.repo_id(&req.repo)?;
        self.generation(repo, req.changeset)?;
        let existing = self
            .0
            .query_row(
                "SELECT land_id, input_cs, target_ref FROM lands
             WHERE repo_id = ?1 AND caller = ?2 AND request_key = ?3",
                params![repo.0, caller.0, req.request_key],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(crate::error::from_sqlite)?;
        let land_id = if let Some((land_id, input, target)) = existing {
            if input != req.changeset.as_bytes() || target != req.target_ref {
                return Err(Error::InvalidArgument(format!(
                    "request key {:?} was already used for a different land",
                    req.request_key
                )));
            }
            land_id
        } else {
            let (state, detail) = if self.get_ref(repo, &req.target_ref)?.is_some() {
                (LandState::Pending, String::new())
            } else {
                let detail = format!("ref {:?} does not exist", req.target_ref);
                (LandState::Rejected, detail)
            };
            if let Some(limit) = pending_limit {
                // The pending-lands quota counts new pending rows. A retry
                // of an existing key resumes its land and adds no row, so a
                // client can always finish what it started.
                let pending = self
                    .0
                    .prepare_cached(
                        "SELECT count(*) FROM lands WHERE caller = ?1 AND state = 'pending'",
                    )
                    .map_err(crate::error::from_sqlite)?
                    .query_row([caller.0.as_str()], |row| row.get::<_, i64>(0))
                    .map_err(crate::error::from_sqlite)?;
                if pending >= i64::try_from(limit).unwrap_or(i64::MAX) {
                    return Err(Error::ResourceExhausted(format!(
                        "principal {:?} has its {limit} lands pending \
                         (--max-principal-pending-lands {limit}); wait for one to finish",
                        caller.0
                    )));
                }
            }
            self.0
                .prepare_cached(
                    "INSERT INTO lands
                 (repo_id, caller, request_key, target_ref, input_cs, state, detail, finished_at, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7,
                 CASE ?6 WHEN 'pending' THEN NULL ELSE unixepoch() END, unixepoch())",
                ).map_err(crate::error::from_sqlite)?
                .execute(params![
                    repo.0,
                    caller.0,
                    req.request_key,
                    req.target_ref,
                    req.changeset.as_bytes(),
                    state.as_str(),
                    detail
                ]).map_err(crate::error::from_sqlite)?;
            let land_id = self.0.last_insert_rowid();
            self.expire_lands()?;
            land_id
        };
        Ok((repo, self.land_outcome(repo, caller, land_id)?))
    }

    /// The pending lands that started more than `stale_secs` ago.
    fn stale_lands(&mut self, stale_secs: i64) -> Result<Vec<StaleLand>> {
        let mut stmt = self
            .0
            .prepare_cached(
                "SELECT l.repo_id, l.land_id, l.target_ref, l.caller, l.request_key, r.name
                 FROM lands l JOIN repos r ON r.repo_id = l.repo_id
                 WHERE l.state = 'pending' AND l.created_at > 0
                     AND l.created_at < unixepoch() - ?1",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(params![stale_secs], |row| {
                Ok(StaleLand {
                    repo: RepoId(row.get(0)?),
                    land_id: row.get(1)?,
                    target_ref: row.get(2)?,
                    caller: row.get(3)?,
                    request_key: row.get(4)?,
                    repo_name: row.get(5)?,
                })
            })
            .map_err(crate::error::from_sqlite)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)?;
        Ok(rows)
    }

    /// Rejects a land that is still pending; 1 if it changed, else 0.
    fn reject_stale_land(&mut self, land_id: i64, detail: String) -> Result<u64> {
        let changed = self
            .0
            .prepare_cached(
                "UPDATE lands SET state = 'rejected', detail = ?1, finished_at = unixepoch()
                 WHERE land_id = ?2 AND state = 'pending'",
            )
            .map_err(crate::error::from_sqlite)?
            .execute(params![detail, land_id])
            .map_err(crate::error::from_sqlite)?;
        Ok(u64::try_from(changed).unwrap_or(0))
    }

    /// Deletes up to `EXPIRE_PER_LAND` lands that finished more than
    /// `LAND_RETENTION_SECS` ago, oldest first. A pending land has no
    /// `finished_at`, so it never expires. It runs in the transaction that
    /// records a new land, so the flush before that land's reply covers it.
    ///
    /// The newest land always stays. `land_id` has no AUTOINCREMENT: a new
    /// row gets the highest ID plus one. So keeping the newest row means a
    /// land ID is never given out twice.
    fn expire_lands(&mut self) -> Result<()> {
        let deleted = self
            .0
            .prepare_cached(
                "DELETE FROM lands WHERE land_id IN (
                     SELECT land_id FROM lands
                     WHERE finished_at < unixepoch() - ?1
                         AND land_id < (SELECT max(land_id) FROM lands)
                     ORDER BY finished_at LIMIT ?2)",
            )
            .map_err(crate::error::from_sqlite)?
            .execute(params![LAND_RETENTION_SECS, EXPIRE_PER_LAND])
            .map_err(crate::error::from_sqlite)?;
        if deleted > 0 {
            tracing::debug!(deleted, "expired finished lands");
        }
        Ok(())
    }

    /// A land that `caller` started in `repo`.
    fn land_outcome(&mut self, repo: RepoId, caller: &Caller, land_id: i64) -> Result<LandOutcome> {
        let (state, result, detail, paths) = self
            .0
            .query_row(
                "SELECT state, result_cs, detail, conflict_paths FROM lands
                 WHERE repo_id = ?1 AND land_id = ?2 AND caller = ?3",
                params![repo.0, land_id, caller.0],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(crate::error::from_sqlite)?
            .ok_or_else(|| Error::NotFound(format!("land {land_id}")))?;
        let result = result
            .map(|bytes| ChangesetId::try_from(bytes.as_slice()))
            .transpose()
            .map_err(|e| Error::Corrupt(format!("land {land_id} result: {e}")))?;
        Ok(LandOutcome {
            moved: false,
            land_id,
            state: LandState::parse(&state)?,
            result,
            previous: None,
            detail,
            conflict_paths: decode_paths(&paths)
                .ok_or_else(|| Error::Corrupt(format!("land {land_id} conflict paths")))?,
        })
    }

    /// Moves a pending land to its final state and returns the outcome.
    fn set_land_state(
        &mut self,
        repo: RepoId,
        land_id: i64,
        state: LandState,
        result: Option<ChangesetId>,
        detail: String,
        conflict_paths: Vec<RepoPath>,
    ) -> Result<LandOutcome> {
        let updated = self
            .0
            .prepare_cached(
                "UPDATE lands SET state = ?3, result_cs = ?4, detail = ?5,
                     conflict_paths = ?6, finished_at = unixepoch()
                 WHERE repo_id = ?1 AND land_id = ?2 AND state = 'pending'",
            )
            .map_err(crate::error::from_sqlite)?
            .execute(params![
                repo.0,
                land_id,
                state.as_str(),
                result.as_ref().map(ChangesetId::as_bytes),
                detail,
                encode_paths(&conflict_paths)
            ])
            .map_err(crate::error::from_sqlite)?;
        if updated != 1 {
            return Err(Error::Corrupt(format!(
                "land {land_id} is no longer pending"
            )));
        }
        Ok(LandOutcome {
            moved: false,
            land_id,
            state,
            result,
            previous: None,
            detail,
            conflict_paths,
        })
    }

    /// Records that a git commit became the changeset. Safe to repeat.
    fn put_git_import(&mut self, repo: RepoId, commit: ObjectId, cs: ChangesetId) -> Result<()> {
        self.0
            .prepare_cached(
                "INSERT OR IGNORE INTO git_import (repo_id, git_algo, git_oid, cs)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .map_err(crate::error::from_sqlite)?
            .execute(params![repo.0, SHA1, commit.as_slice(), cs.as_bytes()])
            .map_err(crate::error::from_sqlite)?;
        Ok(())
    }

    /// Records that the git commits became the changesets. SQLite has
    /// one write connection: the existing per-row statement, in a loop.
    fn put_git_imports(&mut self, repo: RepoId, pairs: &[(ObjectId, ChangesetId)]) -> Result<()> {
        for (oid, cs) in pairs {
            self.put_git_import(repo, *oid, *cs)?;
        }
        Ok(())
    }

    /// Every git commit already imported into `repo`.
    fn imported_commits(&mut self, repo: RepoId) -> Result<HashMap<ObjectId, ChangesetId>> {
        let mut stmt = self
            .0
            .prepare("SELECT git_oid, cs FROM git_import WHERE repo_id = ?1 AND git_algo = ?2")
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(params![repo.0, SHA1], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(crate::error::from_sqlite)?;
        let mut out = HashMap::new();
        for row in rows {
            let (oid, cs) = row.map_err(crate::error::from_sqlite)?;
            let corrupt = || Error::Corrupt("git_import row".to_owned());
            let oid = ObjectId::try_from(oid.as_slice()).map_err(|_| corrupt())?;
            let cs = ChangesetId::try_from(cs.as_slice()).map_err(|_| corrupt())?;
            out.insert(oid, cs);
        }
        Ok(out)
    }

    /// Stores a history node under its content address. Safe to repeat.
    /// Returns whether the row was new.
    fn put_history_node(&mut self, id: NodeId, body: Vec<u8>) -> Result<(NodeId, bool)> {
        let stored = self
            .0
            .prepare_cached("INSERT OR IGNORE INTO history_nodes (id, body) VALUES (?1, ?2)")
            .map_err(crate::error::from_sqlite)?
            .execute(params![&id.0[..], body])
            .map_err(crate::error::from_sqlite)?;
        Ok((id, stored > 0))
    }

    /// Reads a history node and checks it against its ID. Also returns the
    /// length of its stored bytes.
    fn get_history_node(&mut self, id: NodeId) -> Result<(Vec<u8>, usize)> {
        let body: Vec<u8> = self
            .0
            .prepare_cached("SELECT body FROM history_nodes WHERE id = ?1")
            .map_err(crate::error::from_sqlite)?
            .query_row([&id.0[..]], |row| row.get(0))
            .optional()
            .map_err(crate::error::from_sqlite)?
            .ok_or_else(|| Error::Corrupt(format!("history node {id} is missing")))?;
        if NodeId::of(&body) != id {
            return Err(Error::Corrupt(format!(
                "history node {id} does not match its ID"
            )));
        }
        let len = body.len();
        Ok((body, len))
    }

    /// Every repo with its name, in ID order.
    fn repos(&mut self) -> Result<Vec<(RepoId, String)>> {
        let mut stmt = self
            .0
            .prepare_cached("SELECT repo_id, name FROM repos ORDER BY repo_id")
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((RepoId(row.get::<_, i64>(0)?), row.get::<_, String>(1)?))
            })
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    /// The `(repo, changeset ID, body)` of the stored changesets after
    /// `(repo, cs)`, in that order, at most `limit`.
    fn repo_changesets_after(
        &mut self,
        from_repo: RepoId,
        from_cs: &[u8],
        limit: usize,
    ) -> Result<Vec<RepoChangeset>> {
        let mut stmt = self
            .0
            .prepare_cached(
                "SELECT rc.repo_id, rc.cs, ch.body
             FROM repo_changesets rc JOIN changesets ch ON ch.id = rc.cs
             WHERE (rc.repo_id, rc.cs) > (?1, ?2)
             ORDER BY rc.repo_id, rc.cs LIMIT ?3",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(
                params![
                    from_repo.0,
                    from_cs,
                    i64::try_from(limit).unwrap_or(i64::MAX)
                ],
                |row| Ok(RepoChangeset(RepoId(row.get(0)?), row.get(1)?, row.get(2)?)),
            )
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    /// The changeset IDs that `repo` holds, all of them.
    fn repo_changeset_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>> {
        let mut stmt = self
            .0
            .prepare_cached("SELECT cs FROM repo_changesets WHERE repo_id = ?1 ORDER BY cs")
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(params![repo.0], |row| row.get(0))
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    fn repo_changesets_with_bodies(
        &mut self,
        repo: RepoId,
    ) -> Result<Vec<(Vec<u8>, i64, Vec<u8>)>> {
        let mut stmt = self
            .0
            .prepare_cached(
                "SELECT c.id, c.gen, c.body FROM repo_changesets r \
                 JOIN changesets c ON c.id = r.cs WHERE r.repo_id = ?1 ORDER BY c.gen, c.id",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(params![repo.0], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    fn list_refs(&mut self, repo: RepoId) -> Result<Vec<(String, Vec<u8>, i64)>> {
        let mut stmt = self
            .0
            .prepare_cached(
                "SELECT name, target, version FROM refs WHERE repo_id = ?1 ORDER BY name",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(params![repo.0], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    fn pending_lands(&mut self, repo: RepoId) -> Result<Vec<crate::meta::PendingLand>> {
        let mut stmt = self
            .0
            .prepare_cached(
                "SELECT caller, request_key, target_ref, input_cs FROM lands \
                 WHERE repo_id = ?1 AND state = 'pending' ORDER BY land_id",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(params![repo.0], |row| {
                Ok(crate::meta::PendingLand {
                    caller: row.get(0)?,
                    request_key: row.get(1)?,
                    target_ref: row.get(2)?,
                    input_cs: row.get(3)?,
                })
            })
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    /// The bodies of the changesets the ids name, as `(id, body)` for
    /// every id that has a stored body. SQLite has one write connection:
    /// the existing per-id statement, in a loop, is the batch.
    fn stored_changeset_bodies(&mut self, ids: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut rows = Vec::new();
        let mut stmt = self
            .0
            .prepare_cached("SELECT body FROM changesets WHERE id = ?1")
            .map_err(crate::error::from_sqlite)?;
        for id in ids {
            if let Some(body) = stmt
                .query_row(params![id], |row| row.get(0))
                .optional()
                .map_err(crate::error::from_sqlite)?
            {
                rows.push((id.clone(), body));
            }
        }
        Ok(rows)
    }

    /// The stored length of every id's body. SQLite holds bodies as
    /// BLOBs, so `length` counts bytes.
    fn stored_changeset_body_lengths(&mut self, ids: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, i64)>> {
        let mut rows = Vec::new();
        let mut stmt = self
            .0
            .prepare_cached("SELECT id, length(body) FROM changesets WHERE id = ?1")
            .map_err(crate::error::from_sqlite)?;
        for id in ids {
            if let Some(len) = stmt
                .query_row(params![id], |row| row.get(1))
                .optional()
                .map_err(crate::error::from_sqlite)?
            {
                rows.push((id.clone(), len));
            }
        }
        Ok(rows)
    }

    /// The blobs that `repo` may name, all of them.
    fn repo_blob_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>> {
        let mut stmt = self
            .0
            .prepare_cached("SELECT blob FROM repo_blobs WHERE repo_id = ?1 ORDER BY blob")
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(params![repo.0], |row| row.get(0))
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    /// Removes the row that names `blob` for `repo`.
    fn delete_repo_blob(&mut self, repo: RepoId, blob: &[u8]) -> Result<()> {
        self.0
            .prepare_cached("DELETE FROM repo_blobs WHERE repo_id = ?1 AND blob = ?2")
            .map_err(crate::error::from_sqlite)?
            .execute(params![repo.0, blob])
            .map_err(crate::error::from_sqlite)?;
        Ok(())
    }

    /// Removes the rows that name any of `blobs` for `repo`. SQLite has
    /// one write connection: the existing per-row statement, in a loop.
    fn delete_repo_blobs(&mut self, repo: RepoId, blobs: &[Vec<u8>]) -> Result<()> {
        for blob in blobs {
            self.delete_repo_blob(repo, blob)?;
        }
        Ok(())
    }

    /// Whether any repo names `blob`. `repo_blobs` has no index on `blob`
    /// alone, so the query drives from `repos` and looks the pair up in the
    /// `(repo_id, blob)` primary key of each repo.
    fn any_repo_names(&mut self, blob: &[u8]) -> Result<bool> {
        self.0
            .prepare_cached(
                "SELECT 1 FROM repos r
                 WHERE EXISTS (SELECT 1 FROM repo_blobs rb
                               WHERE rb.repo_id = r.repo_id AND rb.blob = ?1)
                 LIMIT 1",
            )
            .map_err(crate::error::from_sqlite)?
            .exists(params![blob])
            .map_err(crate::error::from_sqlite)
    }

    /// The `(changeset ID, body)` of the changesets after `from`, in ID
    /// order, at most `limit`.
    fn changeset_bodies_after(
        &mut self,
        from: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut stmt = self
            .0
            .prepare_cached("SELECT id, body FROM changesets WHERE id > ?1 ORDER BY id LIMIT ?2")
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(
                params![from, i64::try_from(limit).unwrap_or(i64::MAX)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    /// The `(tree ID, body)` of the trees after `from`, in ID order, at
    /// most `limit`.
    fn tree_bodies_after(&mut self, from: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut stmt = self
            .0
            .prepare_cached("SELECT id, body FROM trees WHERE id > ?1 ORDER BY id LIMIT ?2")
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(
                params![from, i64::try_from(limit).unwrap_or(i64::MAX)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    /// The `(node ID, body)` of the history nodes after `from`, in ID
    /// order, at most `limit`.
    fn history_node_bodies_after(
        &mut self,
        from: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut stmt = self
            .0
            .prepare_cached("SELECT id, body FROM history_nodes WHERE id > ?1 ORDER BY id LIMIT ?2")
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(
                params![from, i64::try_from(limit).unwrap_or(i64::MAX)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    /// The `repo_blobs` rows after `(repo, blob)`, at most `limit`.
    fn repo_blob_rows_after(
        &mut self,
        from_repo: RepoId,
        from_blob: &[u8],
        limit: usize,
    ) -> Result<Vec<(RepoId, Vec<u8>)>> {
        let mut stmt = self
            .0
            .prepare_cached(
                "SELECT repo_id, blob FROM repo_blobs
             WHERE (repo_id, blob) > (?1, ?2) ORDER BY repo_id, blob LIMIT ?3",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(
                params![
                    from_repo.0,
                    from_blob,
                    i64::try_from(limit).unwrap_or(i64::MAX)
                ],
                |row| Ok((RepoId(row.get(0)?), row.get(1)?)),
            )
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    /// The refs whose target no `repo_changesets` row of their repo holds.
    fn dangling_refs(&mut self) -> Result<Vec<(RepoId, String, Vec<u8>)>> {
        let mut stmt = self
            .0
            .prepare_cached(
                "SELECT repo_id, name, target FROM refs r
             WHERE NOT EXISTS (
                 SELECT 1 FROM repo_changesets rc
                 WHERE rc.repo_id = r.repo_id AND rc.cs = r.target
             )",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((RepoId(row.get(0)?), row.get(1)?, row.get(2)?))
            })
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    /// Every land whose input or result changeset is not stored, as
    /// `(land, changeset, which column)`.
    fn lands_with_missing_changesets(&mut self) -> Result<Vec<(i64, Vec<u8>, &'static str)>> {
        let mut out = Vec::new();
        for (column, when) in [
            ("input_cs", "1 = 1"),
            ("result_cs", "result_cs IS NOT NULL"),
        ] {
            let sql = format!(
                "SELECT land_id, {column} FROM lands l
                 WHERE {when} AND NOT EXISTS (
                     SELECT 1 FROM changesets c WHERE c.id = l.{column}
                 )"
            );
            let mut stmt = self.0.prepare(&sql).map_err(crate::error::from_sqlite)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(crate::error::from_sqlite)?;
            for row in rows {
                let (land, cs) = row.map_err(crate::error::from_sqlite)?;
                out.push((land, cs, column));
            }
        }
        Ok(out)
    }

    /// The derived rows after `(kind, version, cs)`, at most `limit`, with
    /// whether their changeset is stored.
    fn derived_rows_after(
        &mut self,
        kind: &str,
        version: u32,
        cs: &[u8],
        limit: usize,
    ) -> Result<Vec<FsckDerivedRow>> {
        let mut stmt = self
            .0
            .prepare_cached(
                "SELECT d.kind, d.version, d.cs, d.value,
                    EXISTS (SELECT 1 FROM changesets c WHERE c.id = d.cs)
             FROM derived d
             WHERE (d.kind, d.version, d.cs) > (?1, ?2, ?3)
             ORDER BY d.kind, d.version, d.cs LIMIT ?4",
            )
            .map_err(crate::error::from_sqlite)?;
        let rows = stmt
            .query_map(
                params![kind, version, cs, i64::try_from(limit).unwrap_or(i64::MAX)],
                |row| {
                    Ok(FsckDerivedRow(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .map_err(crate::error::from_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::error::from_sqlite)
    }

    fn clear_restore_mark(&mut self) -> Result<()> {
        // SQLite has no restore staging mark: the mark is Postgres's.
        Ok(())
    }
}
/// Commit graphs for tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::BTreeMap;

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
        insert_each(c, repo, parents, |i| {
            let path = format!("f{i}");
            let state = FileState::Regular {
                content: BlobId::of(path.as_bytes()),
                executable: false,
            };
            let message = path.clone().into_bytes();
            (
                message,
                [(RepoPath::new(path).unwrap(), Some(state))].into(),
            )
        })
    }

    /// Stores a graph as [`insert_graph`] does, where changeset `i` has the
    /// message and the changes that `make(i)` returns.
    fn insert_each(
        c: &Connection,
        repo: RepoId,
        parents: &[Vec<usize>],
        make: impl Fn(usize) -> (Vec<u8>, BTreeMap<RepoPath, Option<FileState>>),
    ) -> Result<Vec<ChangesetId>> {
        let mut ids: Vec<ChangesetId> = Vec::new();
        for (i, of) in parents.iter().enumerate() {
            let (message, changes) = make(i);
            let cs = Changeset::new(
                of.iter().map(|&p| ids[p]).collect(),
                IdentityTime::new(b"a".to_vec(), b"a@x".to_vec(), 0, 0)
                    .expect("identity fits the format"),
                None,
                message,
                changes,
            )
            .expect("changeset fits the format");
            ids.push(SqliteTx(c).insert_changeset(repo, &cs)?);
        }
        Ok(ids)
    }

    /// Paths that [`histories`] change. Some are files and directories of
    /// others, so a path is a file at one time and a directory at another,
    /// or both in one changeset.
    pub(crate) const PATHS: [&str; 8] = ["a", "b", "a/b", "a/c", "b/a", "a/b/c", "a/b/d", "c/a/b"];

    /// The changes of one changeset of [`histories`]: an index in
    /// [`PATHS`], and whether it puts the file there or deletes it.
    pub(crate) type Edits = Vec<(usize, bool)>;

    /// Graphs as [`graphs`] makes them, with random puts and deletes of
    /// [`PATHS`] in each changeset, or none. A change need not apply to a
    /// tree, for example a delete of a missing file, so only kinds that read
    /// the changes and not the trees can derive these histories.
    pub(crate) fn histories(max: usize) -> impl Strategy<Value = (Vec<Vec<usize>>, Vec<Edits>)> {
        graphs(max).prop_flat_map(|parents| {
            let edit = (0..PATHS.len(), any::<bool>());
            let edits =
                proptest::collection::vec(proptest::collection::vec(edit, 0..4), parents.len());
            (proptest::strategy::Just(parents), edits)
        })
    }

    /// Stores a history of [`histories`] as [`insert_graph`] does, where
    /// changeset `i` makes the changes `edits[i]`. Returns the IDs.
    pub(crate) fn insert_history(
        c: &Connection,
        repo: RepoId,
        parents: &[Vec<usize>],
        edits: &[Edits],
    ) -> Result<Vec<ChangesetId>> {
        insert_each(c, repo, parents, |i| {
            let changes = edits[i]
                .iter()
                .map(|&(path, put)| {
                    let state = put.then(|| FileState::Regular {
                        content: BlobId::of(i.to_string().as_bytes()),
                        executable: false,
                    });
                    (RepoPath::new(PATHS[path]).unwrap(), state)
                })
                .collect();
            (i.to_string().into_bytes(), changes)
        })
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
    use crate::store::migrate;
    use crate::test_support::store::Store;

    fn changeset(parents: Vec<ChangesetId>, changes: &[(&str, Option<FileState>)]) -> Changeset {
        Changeset::new(
            parents,
            IdentityTime::new(b"a".to_vec(), b"a@x".to_vec(), 0, 0)
                .expect("identity fits the format"),
            None,
            Vec::new(),
            changes
                .iter()
                .map(|(path, state)| (RepoPath::new(*path).unwrap(), *state))
                .collect(),
        )
        .expect("changeset fits the format")
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
            let mut tx = SqliteTx(&db);
            let [a, b, c] = ["a", "b", "c"].map(|name| tx.create_repo(name).unwrap());
            let mut insert = |repo, cs: &Changeset| tx.insert_changeset(repo, cs).unwrap();
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
                let mut select = db
                    .prepare("SELECT repo_id, blob FROM repo_blobs")
                    .map_err(crate::error::from_sqlite)?;
                let rows = select
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(crate::error::from_sqlite)?;
                rows.collect::<rusqlite::Result<_>>()
                    .map_err(crate::error::from_sqlite)
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
