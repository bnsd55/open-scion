//! Derived data: values computed from changesets, not part of them.
//!
//! One mechanism for every kind. A [`Kind`] has a name, a version, and a
//! function from a changeset and its first parent's value to a value. A
//! value is computed when it is first needed and stored by (kind, version,
//! changeset). A kind that must always be ready, such as the root tree,
//! also stores its value when the changeset is written.
//!
//! Each version has its own rows. So a version bump makes the old rows
//! unused, and nothing rewrites them: the new version derives its values
//! again, on demand. The old rows stay, and nothing removes them yet
//! (<https://github.com/bnsd55/open-scion/issues/71>).

use rusqlite::Connection;
use scion_core::{Changeset, ChangesetId};

use crate::db::{self, RepoId};
use crate::error::{Error, Result};
use crate::store::Store;

/// One kind of derived data.
pub trait Kind {
    /// The kind's name in the `derived` table.
    const NAME: &'static str;
    /// Bump it when [`Kind::derive`] gives a different answer. Rows of other
    /// versions are never read.
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

/// The value of `K` for a changeset that `repo` may see, derived if needed.
/// Returns once the value, and what it names, is durable.
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
    match known {
        Some(value) => Ok(value),
        // A value can name rows that the derivation wrote, such as trees,
        // and the caller can send it to a client. So it waits for one full
        // flush, shared with concurrent commits. `derive` and `put` never
        // flush: inside a write, the write's own flush covers them.
        None => store.write(|tx| derive::<K>(tx, repo, cs)),
    }
}

/// Derives the value of `K` for `cs` inside the caller's transaction, and
/// stores every value it derives.
///
/// Walks first parents back to the nearest changeset with a stored value,
/// then derives forward, oldest first. No recursion, so a long underived
/// history is safe.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. Errors of
/// [`Kind::derive`].
// SHORTCUT: one transaction derives every missing value on the chain, and
// holds the database lock while it runs. So the first read after a new kind
// or a version bump derives the whole history at once. Upgrade: a backfill
// that derives a bounded number of changesets per transaction
// (https://github.com/bnsd55/open-scion/issues/71).
pub fn derive<K: Kind>(c: &Connection, repo: RepoId, cs: ChangesetId) -> Result<K::Value> {
    // Rows are keyed by changeset alone, so check the repo here: a stored
    // value must not leak to a repo that cannot see the changeset.
    require(c, repo, cs)?;
    let mut pending = Vec::new();
    let mut cur = Some(cs);
    let mut value = None;
    while let Some(id) = cur {
        if let Some(known) = load::<K>(c, id)? {
            value = Some(known);
            break;
        }
        pending.push(id);
        cur = db::first_parent(c, id)?;
    }
    for id in pending.into_iter().rev() {
        let changeset = db::get_changeset(c, repo, id)?;
        let next = K::derive(c, id, &changeset, value)?;
        put::<K>(c, id, &next)?;
        value = Some(next);
    }
    value.ok_or_else(|| Error::NotFound(format!("changeset {cs}")))
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
    use crate::tree;

    thread_local! {
        /// Calls to a test kind's `derive` on this thread.
        static DERIVES: Cell<usize> = const { Cell::new(0) };
    }

    /// A test kind: how many paths the first-parent history has changed, up
    /// to and including this changeset. Not stored at write time, and its
    /// value is not 32 bytes.
    struct ChangedPaths;

    impl Kind for ChangedPaths {
        const NAME: &'static str = "changed_paths";
        const VERSION: u32 = 1;
        type Value = u64;

        fn derive(
            _: &Connection,
            _: ChangesetId,
            cs: &Changeset,
            parent: Option<u64>,
        ) -> Result<u64> {
            DERIVES.set(DERIVES.get() + 1);
            Ok(parent.unwrap_or(0) + cs.changes.len() as u64)
        }

        fn encode(count: &u64) -> Vec<u8> {
            count.to_le_bytes().to_vec()
        }

        fn decode(bytes: &[u8]) -> Option<u64> {
            bytes.try_into().ok().map(u64::from_le_bytes)
        }
    }

    /// Version 2 of the test kind, with a new answer: it counts puts only.
    struct PutPaths;

    impl Kind for PutPaths {
        const NAME: &'static str = ChangedPaths::NAME;
        const VERSION: u32 = 2;
        type Value = u64;

        fn derive(
            _: &Connection,
            _: ChangesetId,
            cs: &Changeset,
            parent: Option<u64>,
        ) -> Result<u64> {
            DERIVES.set(DERIVES.get() + 1);
            let puts = cs.changes.values().filter(|state| state.is_some()).count();
            Ok(parent.unwrap_or(0) + puts as u64)
        }

        fn encode(count: &u64) -> Vec<u8> {
            ChangedPaths::encode(count)
        }

        fn decode(bytes: &[u8]) -> Option<u64> {
            ChangedPaths::decode(bytes)
        }
    }

    /// A first-parent chain: `(path, true)` puts a file, `(path, false)`
    /// deletes it. Changed paths: 2, 4, 5. Puts: 2, 3, 4.
    const HISTORY: [&[(&str, bool)]; 3] = [
        &[("a", true), ("b", true)],
        &[("a", false), ("c", true)],
        &[("d", true)],
    ];

    /// Stores `HISTORY` in a new repo `r` through the one write path, which
    /// stores root trees and nothing else. Returns the IDs, oldest first.
    fn store_history(c: &Connection) -> Result<(RepoId, Vec<ChangesetId>)> {
        let repo = db::create_repo(c, "r")?;
        let mut ids: Vec<ChangesetId> = Vec::new();
        for edits in HISTORY {
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
            .filter(|(kind, v, ..)| kind == ChangedPaths::NAME && *v == version)
            .map(|(_, _, cs, value)| (cs, value))
            .collect()
    }

    #[test]
    fn a_new_kind_is_derived_on_first_read_and_stored() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (repo, ids) = store.write(store_history).unwrap();
        assert!(kind_rows(&store, 1).is_empty(), "nothing derived yet");

        DERIVES.set(0);
        assert_eq!(get::<ChangedPaths>(&store, repo, ids[2]).unwrap(), 5);
        assert_eq!(DERIVES.get(), 3, "each changeset once, from the root");
        let counts: Vec<u64> = ids
            .iter()
            .map(|&cs| get::<ChangedPaths>(&store, repo, cs).unwrap())
            .collect();
        assert_eq!(counts, [2, 4, 5]);
        assert_eq!(DERIVES.get(), 3, "later reads use the stored values");
        let stored = kind_rows(&store, 1);
        assert_eq!(stored.len(), 3);
        assert!(stored.iter().all(|(_, value)| value.len() == 8));

        // A stored value does not leak to a repo without the changeset.
        let other = store.write(|c| db::create_repo(c, "other")).unwrap();
        let tip = ids[2];
        assert!(matches!(
            get::<ChangedPaths>(&store, other, tip),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            store.commit(|c| derive::<ChangedPaths>(c, other, tip)),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn a_version_bump_derives_again_and_leaves_old_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (repo, ids) = store.write(store_history).unwrap();
        let tip = ids[2];
        assert_eq!(get::<ChangedPaths>(&store, repo, tip).unwrap(), 5);
        let v1 = kind_rows(&store, 1);

        DERIVES.set(0);
        assert_eq!(get::<PutPaths>(&store, repo, tip).unwrap(), 4);
        assert_eq!(DERIVES.get(), 3, "version 2 derives every value again");
        assert_eq!(kind_rows(&store, 2).len(), 3);
        assert_eq!(kind_rows(&store, 1), v1, "version 1 rows are not rewritten");
        assert_eq!(get::<ChangedPaths>(&store, repo, tip).unwrap(), 5);
        assert_eq!(DERIVES.get(), 3, "and version 1 still reads them");
    }

    #[test]
    fn a_version_2_database_upgrades_and_keeps_its_derived_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (repo, ids, before) = {
            let c = Connection::open(dir.path().join("scion.db")).unwrap();
            let v1 = include_str!("../migrations/0001_init.sql");
            let v2 = include_str!("../migrations/0002_land_retention.sql");
            c.execute_batch(&format!(
                "BEGIN; {v1} {v2} PRAGMA user_version = 2; COMMIT;"
            ))
            .unwrap();
            let (repo, ids) = store_history(&c).unwrap();
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
        assert_eq!(version, 3);
        assert_eq!(store.read(|c| Ok(rows(c))).unwrap(), before);
        // Each root tree is read from its copied row.
        for &cs in &ids {
            let tree = tree::root_tree(&store, repo, cs).unwrap();
            let row = (
                tree::RootTree::NAME.to_owned(),
                tree::RootTree::VERSION,
                cs.as_bytes().to_vec(),
                tree.as_bytes().to_vec(),
            );
            assert!(before.contains(&row), "{cs}");
        }
        // The table now holds a value that is not 32 bytes.
        assert_eq!(get::<ChangedPaths>(&store, repo, ids[2]).unwrap(), 5);
    }
}
