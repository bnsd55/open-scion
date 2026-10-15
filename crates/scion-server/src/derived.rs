//! Derived data: values computed from changesets, not part of them.
//!
//! One mechanism for every kind. A [`Kind`] has a name, a version, and a
//! function from a changeset and its first parent's value to a value. A
//! value is computed when it is first needed and stored by (kind, version,
//! changeset). A kind that must always be ready, such as the root tree,
//! also stores its value when the changeset is written.
//!
//! A long history without values is derived in steps of at most [`STEP`]
//! changesets. Each step is its own transaction, so other requests run
//! between steps, and a crash loses at most one step.
//!
//! Each version has its own rows. So a version bump makes the old rows
//! unused, and nothing rewrites them: the new version derives its values
//! again. Each step also removes some rows of older versions of its kind.

use std::ops::ControlFlow;

use rusqlite::Connection;
use scion_core::{Changeset, ChangesetId};

use crate::db::{self, RepoId};
use crate::error::{Error, Result};
use crate::store::Store;

/// Most changesets that one step walks or derives, and most rows of older
/// versions that one step removes. A step holds the database lock, so this
/// bounds how long other requests wait. Tests use a small step, so that a
/// short history takes several steps.
pub const STEP: usize = if cfg!(test) { 2 } else { 100 };

/// How many steps derive `changesets` changesets.
#[cfg(test)]
pub(crate) fn steps(changesets: usize) -> u64 {
    changesets.div_ceil(STEP) as u64
}

/// One kind of derived data.
pub trait Kind {
    /// The kind's name in the `derived` table.
    const NAME: &'static str;
    /// Bump it when [`Kind::derive`] gives a different answer. Rows of other
    /// versions are never read. Rows of older versions are removed.
    const VERSION: u32;
    /// The value for one changeset.
    type Value;

    /// The value for changeset `id`, from the changeset and from the value
    /// for its first parent (`None` for a root). Writes that it makes, such
    /// as trees, go in the caller's transaction.
    ///
    /// # Errors
    ///
    /// Database errors, or [`Error::Corrupt`] if the stored changeset does
    /// not fit its parent's value.
    // SHORTCUT: only the first parent's value, because a changeset stores
    // only its changes against its first parent. A kind over the full graph,
    // such as a generation number, does not fit. Upgrade: pass every
    // parent's value and derive ancestors in generation order
    // (https://github.com/bnsd55/open-scion/issues/76).
    fn derive(
        c: &Connection,
        id: ChangesetId,
        changeset: &Changeset,
        parent: Option<Self::Value>,
    ) -> Result<Self::Value>;

    /// The stored bytes of a value.
    fn encode(value: &Self::Value) -> Vec<u8>;

    /// The value in stored bytes, or `None` if they are not a value.
    fn decode(bytes: &[u8]) -> Option<Self::Value>;
}

/// The value of `K` for a changeset that `repo` may see, derived as
/// [`fill`] does if needed. Returns once the value, and what it names, is
/// durable.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. Errors of
/// [`Kind::derive`], or of [`Store::flush`].
pub fn get<K: Kind>(store: &Store, repo: RepoId, cs: ChangesetId) -> Result<K::Value> {
    let known = store.read(|c| {
        require(c, repo, cs)?;
        load::<K>(c, cs)
    })?;
    if let Some(value) = known {
        return Ok(value);
    }
    // A value can name rows that the derivation wrote, such as trees, and
    // the caller can send it to a client. So it waits for one full flush,
    // shared with concurrent commits. An error is an answer too, so it
    // waits as well: the steps before the error committed.
    let value = fill::<K>(store, repo, cs);
    store.flush()?;
    value
}

/// Derives every missing value of `K` on the first-parent history of `cs`,
/// in steps, and returns the value for `cs`. Returns before the values are
/// durable: call [`Store::flush`] before replying with anything they name.
///
/// First it walks first parents back to the nearest changeset with a stored
/// value, through at most [`STEP`] changesets per hold of the database
/// lock. Then it derives forward, oldest first, at most [`STEP`] changesets
/// per transaction. Each of these transactions also removes up to [`STEP`]
/// rows of older versions of `K`. So an error or a crash keeps every step
/// before it, and the next call goes on from there.
///
/// The walk keeps the changesets without a value in memory, 32 bytes each,
/// so that it visits each one once.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. Errors of
/// [`Kind::derive`].
pub fn fill<K: Kind>(store: &Store, repo: RepoId, cs: ChangesetId) -> Result<K::Value> {
    let (pending, value) = walk_all::<K>(store, repo, cs)?;
    derive_steps::<K>(store, repo, &pending, value)?
        .ok_or_else(|| Error::NotFound(format!("changeset {cs}")))
}

/// Fills `K` for `cs` and its first-parent history as [`fill`] does, then
/// removes every row of an older version of `K`, at most [`STEP`] rows per
/// transaction. Returns once all of it is durable.
///
/// # Errors
///
/// As [`fill`] and [`Store::flush`].
pub fn backfill<K: Kind>(store: &Store, repo: RepoId, cs: ChangesetId) -> Result<()> {
    fill::<K>(store, repo, cs)?;
    // A short step means that no older row is left.
    while store.commit(remove_old::<K>)? == STEP {}
    store.flush()
}

/// Derives the value of `K` for `cs` inside the caller's transaction, and
/// stores every value it derives. It walks and derives as [`fill`] does,
/// but with no bound, in one transaction.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. Errors of
/// [`Kind::derive`].
pub fn derive<K: Kind>(c: &Connection, repo: RepoId, cs: ChangesetId) -> Result<K::Value> {
    require(c, repo, cs)?;
    let mut pending = Vec::new();
    let ControlFlow::Break(value) = walk::<K>(c, cs, usize::MAX, &mut pending)? else {
        unreachable!("a walk with no limit ends");
    };
    pending.reverse();
    derive_chain::<K>(c, repo, &pending, value)?
        .ok_or_else(|| Error::NotFound(format!("changeset {cs}")))
}

/// Stores the value of `K` for `cs`. A second write must agree with the
/// first.
///
/// # Errors
///
/// [`Error::Corrupt`] if a different value is already stored.
pub fn put<K: Kind>(c: &Connection, cs: ChangesetId, value: &K::Value) -> Result<()> {
    db::put_derived(c, K::NAME, K::VERSION, cs, &K::encode(value))
}

/// The walk of [`fill`]: the changesets from `cs` back to the nearest one
/// with a stored value, oldest first, and that value (`None` past a root).
fn walk_all<K: Kind>(
    store: &Store,
    repo: RepoId,
    cs: ChangesetId,
) -> Result<(Vec<ChangesetId>, Option<K::Value>)> {
    let mut pending = Vec::new();
    let mut next = cs;
    let value = loop {
        // Rows are keyed by changeset alone, so check the repo: a stored
        // value must not leak to a repo that cannot see the changeset.
        let step = store.read_unflushed(|c| {
            require(c, repo, next)?;
            walk::<K>(c, next, STEP, &mut pending)
        })?;
        match step {
            ControlFlow::Continue(id) => next = id,
            ControlFlow::Break(value) => break value,
        }
    };
    pending.reverse();
    Ok((pending, value))
}

/// Follows first parents from `from` through at most `limit` changesets,
/// and pushes each one without a stored value onto `pending`. Breaks with
/// the stored value where the walk ends (`None` past a root), or continues
/// with the changeset where the next walk starts.
fn walk<K: Kind>(
    c: &Connection,
    from: ChangesetId,
    limit: usize,
    pending: &mut Vec<ChangesetId>,
) -> Result<ControlFlow<Option<K::Value>, ChangesetId>> {
    let mut next = Some(from);
    for _ in 0..limit {
        let Some(id) = next else { break };
        if let Some(value) = load::<K>(c, id)? {
            return Ok(ControlFlow::Break(Some(value)));
        }
        pending.push(id);
        next = db::first_parent(c, id)?;
    }
    Ok(next.map_or(ControlFlow::Break(None), ControlFlow::Continue))
}

/// The derivation of [`fill`]: derives `pending`, oldest first, from
/// `value`, the value for the first one's parent, at most [`STEP`]
/// changesets per transaction. Returns the value for the last one.
fn derive_steps<K: Kind>(
    store: &Store,
    repo: RepoId,
    pending: &[ChangesetId],
    mut value: Option<K::Value>,
) -> Result<Option<K::Value>> {
    for step in pending.chunks(STEP) {
        value = store.commit(|c| {
            let value = derive_chain::<K>(c, repo, step, value)?;
            remove_old::<K>(c)?;
            Ok(value)
        })?;
    }
    Ok(value)
}

/// Derives and stores the value of `K` for each of `ids`, oldest first, from
/// `value`, the value for the first one's parent. A value that is already
/// stored, for example by a concurrent [`fill`], is read, not derived again.
fn derive_chain<K: Kind>(
    c: &Connection,
    repo: RepoId,
    ids: &[ChangesetId],
    mut value: Option<K::Value>,
) -> Result<Option<K::Value>> {
    for &id in ids {
        if let Some(known) = load::<K>(c, id)? {
            value = Some(known);
            continue;
        }
        let changeset = db::get_changeset(c, repo, id)?;
        let next = K::derive(c, id, &changeset, value)?;
        put::<K>(c, id, &next)?;
        value = Some(next);
    }
    Ok(value)
}

/// Removes up to [`STEP`] rows of older versions of `K` in the caller's
/// transaction, and returns how many. Each step of [`fill`] calls it, and
/// so does each write of an eager kind, so old rows go away as new ones
/// come. Rows of a newer version stay: a newer build that ran on this data
/// may run again, and it reads them.
///
/// # Errors
///
/// Database errors.
pub fn remove_old<K: Kind>(c: &Connection) -> Result<usize> {
    let removed = db::delete_derived_below(c, K::NAME, K::VERSION, STEP)?;
    if removed > 0 {
        tracing::debug!(
            kind = K::NAME,
            removed,
            "removed derived rows of older versions"
        );
    }
    Ok(removed)
}

fn load<K: Kind>(c: &Connection, cs: ChangesetId) -> Result<Option<K::Value>> {
    db::get_derived(c, K::NAME, K::VERSION, cs)?
        .map(|bytes| {
            K::decode(&bytes).ok_or_else(|| {
                Error::Corrupt(format!("derived {} v{} for {cs}", K::NAME, K::VERSION))
            })
        })
        .transpose()
}

fn require(c: &Connection, repo: RepoId, cs: ChangesetId) -> Result<()> {
    if db::has_changeset(c, repo, cs)? {
        Ok(())
    } else {
        Err(Error::NotFound(format!("changeset {cs}")))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use scion_core::{BlobId, FileState, IdentityTime, RepoPath};

    use super::*;
    use crate::store::{FSYNC_LEAVES_DRIVE_CACHE, SCHEMA_VERSION, migrate};
    use crate::tree::{self, RootTree};

    thread_local! {
        /// Calls to a test kind's `derive` on this thread.
        static DERIVES: Cell<usize> = const { Cell::new(0) };
        /// A changeset whose derivation fails on this thread.
        static FAIL_AT: Cell<Option<ChangesetId>> = const { Cell::new(None) };
    }

    /// A test kind: how many paths the first-parent history has changed, up
    /// to and including this changeset. Not stored at write time, and its
    /// value is not 32 bytes. Version 2 gives a new answer: it counts puts
    /// only.
    struct ChangedPaths<const VERSION: u32>;

    impl<const V: u32> Kind for ChangedPaths<V> {
        const NAME: &'static str = "changed_paths";
        const VERSION: u32 = V;
        type Value = u64;

        fn derive(
            _: &Connection,
            id: ChangesetId,
            cs: &Changeset,
            parent: Option<u64>,
        ) -> Result<u64> {
            if FAIL_AT.get() == Some(id) {
                return Err(Error::Corrupt(format!("injected failure at {id}")));
            }
            DERIVES.set(DERIVES.get() + 1);
            let counted = cs
                .changes
                .values()
                .filter(|state| V == 1 || state.is_some());
            Ok(parent.unwrap_or(0) + counted.count() as u64)
        }

        fn encode(count: &u64) -> Vec<u8> {
            count.to_le_bytes().to_vec()
        }

        fn decode(bytes: &[u8]) -> Option<u64> {
            bytes.try_into().ok().map(u64::from_le_bytes)
        }
    }

    type ChangedPathsV1 = ChangedPaths<1>;
    type ChangedPathsV2 = ChangedPaths<2>;

    /// The changes of one changeset: `(path, true)` puts a file, `(path,
    /// false)` deletes it.
    type Edits = &'static [(&'static str, bool)];

    /// A first-parent chain. Changed paths: 2, 4, 5. Puts: 2, 3, 4.
    const HISTORY: [Edits; 3] = [
        &[("a", true), ("b", true)],
        &[("a", false), ("c", true)],
        &[("d", true)],
    ];

    /// A first-parent chain of `n` changesets that put and delete `a` in
    /// turn. Changed paths: 1, 2, 3, ... Puts: 1, 1, 2, 2, ...
    fn chain(n: usize) -> Vec<Edits> {
        const PUT: Edits = &[("a", true)];
        const DELETE: Edits = &[("a", false)];
        (0..n)
            .map(|i| if i % 2 == 0 { PUT } else { DELETE })
            .collect()
    }

    /// A new store with `history` in repo `r`, as [`store_chain`] stores it.
    fn store_with(history: &[Edits]) -> (tempfile::TempDir, Store, RepoId, Vec<ChangesetId>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (repo, ids) = store.write(|c| store_chain(c, history)).unwrap();
        (dir, store, repo, ids)
    }

    /// Stores `history` in a new repo `r` through the one write path, which
    /// stores root trees and nothing else. Returns the IDs, oldest first.
    fn store_chain(c: &Connection, history: &[Edits]) -> Result<(RepoId, Vec<ChangesetId>)> {
        let repo = db::create_repo(c, "r")?;
        let mut ids: Vec<ChangesetId> = Vec::new();
        for edits in history {
            let changes = edits
                .iter()
                .map(|&(path, put)| {
                    let state = put.then(|| FileState::Regular {
                        content: BlobId::of(path.as_bytes()),
                        executable: false,
                    });
                    (RepoPath::new(path).unwrap(), state)
                })
                .collect();
            let cs = Changeset {
                parents: ids.last().copied().into_iter().collect(),
                author: IdentityTime {
                    name: b"a".to_vec(),
                    email: b"a@x".to_vec(),
                    unix_secs: 0,
                    tz_offset_minutes: 0,
                },
                committer: None,
                message: vec![],
                changes,
            };
            ids.push(tree::store_changeset(c, repo, &cs)?.0);
        }
        Ok((repo, ids))
    }

    /// Every `derived` row, as (kind, version, changeset, value).
    fn rows(c: &Connection) -> Vec<(String, u32, Vec<u8>, Vec<u8>)> {
        let mut stmt = c
            .prepare("SELECT kind, version, cs, value FROM derived ORDER BY kind, version, cs")
            .unwrap();
        stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    }

    /// The rows of the test kind at `version`, as (changeset, value).
    fn kind_rows(store: &Store, version: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        let all = store.read(|c| Ok(rows(c))).unwrap();
        all.into_iter()
            .filter(|(kind, v, ..)| kind == ChangedPathsV1::NAME && *v == version)
            .map(|(_, _, cs, value)| (cs, value))
            .collect()
    }

    #[test]
    fn a_new_kind_is_derived_on_first_read_and_stored() {
        let (_dir, store, repo, ids) = store_with(&HISTORY);
        assert!(kind_rows(&store, 1).is_empty(), "nothing derived yet");

        DERIVES.set(0);
        let start = store.blobs().full_flushes();
        let flushes = || store.blobs().full_flushes() - start;
        assert_eq!(get::<ChangedPathsV1>(&store, repo, ids[2]).unwrap(), 5);
        assert_eq!(DERIVES.get(), 3, "each changeset once, from the root");
        // None where the commit's own fsync flushes the drive cache.
        let one = u64::from(FSYNC_LEAVES_DRIVE_CACHE);
        assert_eq!(flushes(), one, "durable before get returns");
        let counts: Vec<u64> = ids
            .iter()
            .map(|&cs| get::<ChangedPathsV1>(&store, repo, cs).unwrap())
            .collect();
        assert_eq!(counts, [2, 4, 5]);
        assert_eq!(DERIVES.get(), 3, "later reads use the stored values");
        assert_eq!(flushes(), one, "and run no flush");
        let stored = kind_rows(&store, 1);
        assert_eq!(stored.len(), 3);
        assert!(stored.iter().all(|(_, value)| value.len() == 8));

        // A stored value does not leak to a repo without the changeset.
        let other = store.write(|c| db::create_repo(c, "other")).unwrap();
        let tip = ids[2];
        assert!(matches!(
            get::<ChangedPathsV1>(&store, other, tip),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            store.commit(|c| derive::<ChangedPathsV1>(c, other, tip)),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn a_version_bump_derives_every_value_again() {
        let (_dir, store, repo, ids) = store_with(&HISTORY);
        let tip = ids[2];
        assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), 5);

        DERIVES.set(0);
        assert_eq!(get::<ChangedPathsV2>(&store, repo, tip).unwrap(), 4);
        assert_eq!(DERIVES.get(), 3, "version 2 derives every value again");
        assert_eq!(kind_rows(&store, 2).len(), 3);
        assert!(
            kind_rows(&store, 1).is_empty(),
            "its steps removed version 1"
        );
    }

    #[test]
    fn a_version_2_database_upgrades_and_keeps_its_derived_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (repo, ids, before) = {
            let c = Connection::open(dir.path().join("scion.db")).unwrap();
            migrate(&c, 2).unwrap();
            let (repo, ids) = store_chain(&c, &HISTORY).unwrap();
            (repo, ids, rows(&c))
        };
        // The rows that version 2 holds: a root tree for each changeset.
        // Their kind and version must stay, or every root tree is derived
        // again.
        assert_eq!(before.len(), 3);
        assert!(
            before
                .iter()
                .all(|(kind, v, ..)| kind == "root_tree" && *v == 1)
        );

        let store = Store::open(dir.path()).unwrap();
        let version: u32 = store
            .read(|c| Ok(c.pragma_query_value(None, "user_version", |row| row.get(0))?))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        assert_eq!(store.read(|c| Ok(rows(c))).unwrap(), before);
        // Each root tree is read from its copied row.
        for &cs in &ids {
            let tree = tree::root_tree(&store, repo, cs).unwrap();
            let row = (
                RootTree::NAME.to_owned(),
                RootTree::VERSION,
                cs.as_bytes().to_vec(),
                tree.as_bytes().to_vec(),
            );
            assert!(before.contains(&row), "{cs}");
        }
        // The table now holds a value that is not 32 bytes.
        assert_eq!(get::<ChangedPathsV1>(&store, repo, ids[2]).unwrap(), 5);
    }

    /// How many root tree rows the store holds.
    fn root_trees(store: &Store) -> usize {
        let all = store.read(|c| Ok(rows(c))).unwrap();
        all.iter()
            .filter(|(kind, ..)| kind == RootTree::NAME)
            .count()
    }

    #[test]
    fn a_long_history_is_derived_in_steps() {
        let (_dir, store, repo, ids) = store_with(&chain(7));
        let tip = ids[6];

        // One step of the walk goes through at most STEP changesets.
        let mut pending = Vec::new();
        let step = store
            .read(|c| walk::<ChangedPathsV1>(c, tip, STEP, &mut pending))
            .unwrap();
        assert_eq!(step, ControlFlow::Continue(ids[6 - STEP]));
        let newest: Vec<ChangesetId> = ids.iter().rev().take(STEP).copied().collect();
        assert_eq!(pending, newest);

        DERIVES.set(0);
        let commits = store.commits();
        assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), 7);
        assert_eq!(DERIVES.get(), 7, "each changeset once");
        assert_eq!(store.commits() - commits, steps(7), "one per step");
        // Each step went on from the value of the step before it.
        for (count, &cs) in (1..).zip(&ids) {
            assert_eq!(get::<ChangedPathsV1>(&store, repo, cs).unwrap(), count);
        }
        assert_eq!(DERIVES.get(), 7);
    }

    #[test]
    fn an_error_in_a_step_keeps_the_steps_before_it() {
        let (_dir, store, repo, ids) = store_with(&chain(7));
        let tip = ids[6];
        // The third step fails, as a crash after the second commit would.
        // The error waits until the steps before it are durable.
        FAIL_AT.set(Some(ids[2 * STEP]));
        let flushes = store.blobs().full_flushes();
        let failed = get::<ChangedPathsV1>(&store, repo, tip);
        FAIL_AT.set(None);
        assert!(matches!(failed, Err(Error::Corrupt(_))), "{failed:?}");
        let one = u64::from(FSYNC_LEAVES_DRIVE_CACHE);
        assert_eq!(store.blobs().full_flushes() - flushes, one);

        // Only the rows of the first two steps, each with its right value.
        let mut kept: Vec<(Vec<u8>, Vec<u8>)> = (1_u64..)
            .zip(&ids[..2 * STEP])
            .map(|(count, cs)| (cs.as_bytes().to_vec(), count.to_le_bytes().to_vec()))
            .collect();
        kept.sort();
        assert_eq!(kind_rows(&store, 1), kept);

        DERIVES.set(0);
        assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), 7);
        assert_eq!(DERIVES.get(), 3, "the next read derives only the rest");
    }

    #[test]
    fn a_step_reads_the_values_that_another_fill_stored() {
        let (_dir, store, repo, ids) = store_with(&chain(5));
        let tip = ids[4];
        // One fill walks. Another fill derives the history before the first
        // one's steps run.
        let (pending, value) = walk_all::<ChangedPathsV1>(&store, repo, tip).unwrap();
        assert_eq!((pending.len(), value), (5, None));
        DERIVES.set(0);
        assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), 5);
        assert_eq!(DERIVES.get(), 5);

        let late = derive_steps::<ChangedPathsV1>(&store, repo, &pending, value).unwrap();
        assert_eq!(late, Some(5));
        assert_eq!(DERIVES.get(), 5, "the late steps derive nothing again");
    }

    #[test]
    fn a_step_removes_rows_of_older_versions_only() {
        let (_dir, store, repo, ids) = store_with(&chain(7));
        assert_eq!(get::<ChangedPathsV1>(&store, repo, ids[6]).unwrap(), 7);
        assert_eq!(kind_rows(&store, 1).len(), 7);
        assert_eq!(root_trees(&store), 7);

        // The first step of version 2 derives two values and removes two
        // rows of version 1.
        assert_eq!(get::<ChangedPathsV2>(&store, repo, ids[1]).unwrap(), 1);
        assert_eq!(kind_rows(&store, 2).len(), STEP);
        assert_eq!(kind_rows(&store, 1).len(), 7 - STEP);

        // Version 1 removes neither its own rows nor the rows of version 2,
        // which is newer. No step removes the rows of another kind.
        assert_eq!(store.commit(remove_old::<ChangedPathsV1>).unwrap(), 0);
        assert_eq!(kind_rows(&store, 1).len(), 7 - STEP);
        assert_eq!(kind_rows(&store, 2).len(), STEP);
        assert_eq!(root_trees(&store), 7);
    }

    #[test]
    fn a_backfill_derives_a_history_and_removes_every_older_row() {
        let (_dir, store, repo, ids) = store_with(&chain(7));
        let tip = ids[6];
        backfill::<ChangedPathsV2>(&store, repo, tip).unwrap();
        assert_eq!(kind_rows(&store, 2).len(), 7);
        // A build with version 1 derives its own rows and keeps version 2.
        assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), 7);
        assert_eq!(kind_rows(&store, 1).len(), 7);
        assert_eq!(kind_rows(&store, 2).len(), 7);

        // Back on version 2, nothing is left to derive. The backfill removes
        // every row of version 1, at most STEP per transaction, and returns
        // once that is durable.
        DERIVES.set(0);
        let commits = store.commits();
        let flushes = store.blobs().full_flushes();
        backfill::<ChangedPathsV2>(&store, repo, tip).unwrap();
        assert_eq!(DERIVES.get(), 0);
        assert!(kind_rows(&store, 1).is_empty(), "version 1 is removed");
        assert_eq!(store.commits() - commits, steps(7));
        let one = u64::from(FSYNC_LEAVES_DRIVE_CACHE);
        assert_eq!(store.blobs().full_flushes() - flushes, one);
        assert_eq!(kind_rows(&store, 2).len(), 7);
        assert_eq!(root_trees(&store), 7);
        assert_eq!(get::<ChangedPathsV2>(&store, repo, tip).unwrap(), 4);
    }

    #[test]
    fn a_write_removes_root_trees_of_older_versions() {
        let (_dir, store, repo, ids) = store_with(&chain(7));
        let older = "INSERT INTO derived (kind, version, cs, value)
                     SELECT kind, 0, cs, value FROM derived WHERE kind = ?1";
        let added = store
            .write(|c| Ok(c.execute(older, [RootTree::NAME])?))
            .unwrap();
        assert_eq!(added, 7);

        let cs = Changeset {
            parents: vec![ids[6]],
            ..store.read(|c| db::get_changeset(c, repo, ids[0])).unwrap()
        };
        store
            .write(|c| tree::store_changeset(c, repo, &cs))
            .unwrap();
        // The new row of version 1, and seven rows of version 0 less one
        // step.
        assert_eq!(root_trees(&store), 7 + 1 + 7 - STEP);
    }
}
