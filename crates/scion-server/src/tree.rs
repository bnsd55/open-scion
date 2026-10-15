//! Snapshots: apply a changeset to a tree, look up paths, and derive the
//! root tree of any changeset.
//!
//! Every function takes a `&Connection`. Callers that write pass a
//! transaction, so all trees from one apply or derivation commit together.

use std::collections::BTreeMap;

use rusqlite::Connection;
use scion_core::{Changeset, ChangesetId, Entry, FileState, PathElem, RepoPath, Tree, TreeId};

use crate::db::{self, RepoId};
use crate::error::{ApplyError, Error, Result};
use crate::store::Store;

/// Derived-data kind for the root tree of a changeset.
const ROOT_TREE: &str = "root_tree";
const ROOT_TREE_VERSION: u32 = 1;

/// Applies `changes` to `base` (`None` is the empty tree), stores the new
/// trees, and returns the new root.
///
/// All deletes run before all puts, so a changeset can turn a file into a
/// directory or back. Directories left empty are removed.
///
/// # Errors
///
/// [`Error::Apply`] if a change does not fit the tree. Database errors
/// otherwise.
pub fn apply(
    c: &Connection,
    base: Option<TreeId>,
    changes: &BTreeMap<RepoPath, Option<FileState>>,
) -> Result<TreeId> {
    let mut root = base.map_or_else(|| Dir::Loaded(BTreeMap::new()), Dir::Stored);
    for (path, _) in changes.iter().filter(|(_, change)| change.is_none()) {
        root.delete(c, path, &parts(path))?;
    }
    for (path, state) in changes {
        if let Some(state) = state {
            root.put(c, path, &parts(path), 0, *state)?;
        }
    }
    root.store(c)
}

/// Finds the entry at `path` under `root`.
///
/// # Errors
///
/// Database errors.
pub fn lookup(c: &Connection, root: TreeId, path: &RepoPath) -> Result<Option<Entry>> {
    let mut entry = Entry::Dir(root);
    for name in parts(path) {
        let Entry::Dir(id) = entry else {
            return Ok(None);
        };
        match db::get_tree(c, id)?.entries.remove(&name) {
            Some(next) => entry = next,
            None => return Ok(None),
        }
    }
    Ok(Some(entry))
}

/// The root tree of a changeset that `repo` may see, deriving it if needed.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. [`Error::Corrupt`] if a
/// stored changeset does not apply to its parent tree.
pub fn root_tree(store: &Store, repo: RepoId, cs: ChangesetId) -> Result<TreeId> {
    let known = store.read(|c| {
        require(c, repo, cs)?;
        db::get_derived(c, ROOT_TREE, ROOT_TREE_VERSION, cs)
    })?;
    match known {
        Some(tree) => Ok(TreeId::from_bytes(tree)),
        // Derived data can be derived again, so a read does not wait for a
        // flush; a later write's flush covers it.
        None => store.commit(|tx| derive_root_tree(tx, repo, cs)),
    }
}

/// Derives the root tree of `cs` inside the caller's transaction.
///
/// Walks first parents back to the nearest changeset with a known root tree,
/// then applies changes forward, oldest first. No recursion, so a long
/// underived history is safe.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. [`Error::Corrupt`] if a
/// stored changeset does not apply to its parent tree.
pub fn derive_root_tree(c: &Connection, repo: RepoId, cs: ChangesetId) -> Result<TreeId> {
    // Derived data is keyed by changeset alone, so check the repo here: a
    // cached tree must not leak to a repo that cannot see the changeset.
    require(c, repo, cs)?;
    let mut pending = Vec::new();
    let mut cur = Some(cs);
    let mut base = None;
    while let Some(id) = cur {
        if let Some(tree) = db::get_derived(c, ROOT_TREE, ROOT_TREE_VERSION, id)? {
            base = Some(TreeId::from_bytes(tree));
            break;
        }
        pending.push(id);
        cur = db::first_parent(c, id)?;
    }
    for id in pending.into_iter().rev() {
        let changeset = db::get_changeset(c, repo, id)?;
        let tree = apply(c, base, &changeset.changes).map_err(|e| match e {
            Error::Apply(e) => Error::Corrupt(format!("stored changeset {id} does not apply: {e}")),
            other => other,
        })?;
        db::put_derived(c, ROOT_TREE, ROOT_TREE_VERSION, id, *tree.as_bytes())?;
        base = Some(tree);
    }
    base.ok_or_else(|| Error::NotFound(format!("changeset {cs}")))
}

/// Stores a changeset in `repo` with its root tree: the first parent's tree
/// plus the changes. Every parent must be in `repo`. Safe to repeat. This is
/// the one write path for changesets, used by commits, lands and imports.
///
/// # Errors
///
/// [`Error::Apply`] if the changes do not apply to the first parent's tree;
/// callers decide whether that is the client's fault or corruption.
/// [`Error::NotFound`] if a parent is not in `repo`.
pub fn store_changeset(
    c: &Connection,
    repo: RepoId,
    cs: &Changeset,
) -> Result<(ChangesetId, TreeId)> {
    let base = cs
        .parents
        .first()
        .map(|&parent| derive_root_tree(c, repo, parent))
        .transpose()?;
    let tree = apply(c, base, &cs.changes)?;
    let id = db::insert_changeset(c, repo, cs)?;
    db::put_derived(c, ROOT_TREE, ROOT_TREE_VERSION, id, *tree.as_bytes())?;
    Ok((id, tree))
}

fn require(c: &Connection, repo: RepoId, cs: ChangesetId) -> Result<()> {
    if db::has_changeset(c, repo, cs)? {
        Ok(())
    } else {
        Err(Error::NotFound(format!("changeset {cs}")))
    }
}

fn parts(path: &RepoPath) -> Vec<PathElem> {
    path.as_bytes()
        .split(|&b| b == b'/')
        .map(|part| PathElem::new(part).expect("a RepoPath part is a valid name"))
        .collect()
}

fn prefix(parts: &[PathElem]) -> RepoPath {
    let bytes: Vec<&[u8]> = parts.iter().map(PathElem::as_bytes).collect();
    RepoPath::new(bytes.join(&b'/')).expect("a prefix of a RepoPath is a RepoPath")
}

/// A directory being edited. Only directories on changed paths are loaded.
enum Dir {
    Stored(TreeId),
    Loaded(BTreeMap<PathElem, Node>),
}

enum Node {
    File(FileState),
    Dir(Dir),
}

impl Dir {
    fn entries(&mut self, c: &Connection) -> Result<&mut BTreeMap<PathElem, Node>> {
        if let Self::Stored(id) = *self {
            let entries = db::get_tree(c, id)?
                .entries
                .into_iter()
                .map(|(name, entry)| {
                    let node = match entry {
                        Entry::File(state) => Node::File(state),
                        Entry::Dir(id) => Node::Dir(Self::Stored(id)),
                    };
                    (name, node)
                })
                .collect();
            *self = Self::Loaded(entries);
        }
        match self {
            Self::Loaded(entries) => Ok(entries),
            Self::Stored(_) => unreachable!("loaded above"),
        }
    }

    /// Deletes the file at `parts`. Returns whether this directory is now empty.
    fn delete(&mut self, c: &Connection, path: &RepoPath, parts: &[PathElem]) -> Result<bool> {
        let entries = self.entries(c)?;
        let (name, rest) = parts.split_first().expect("a path has at least one part");
        let remove = match (entries.get_mut(name), rest.is_empty()) {
            (Some(Node::File(_)), true) => true,
            (Some(Node::Dir(_)), true) => return Err(ApplyError::NotAFile(path.clone()).into()),
            (Some(Node::Dir(child)), false) => child.delete(c, path, rest)?,
            (Some(Node::File(_)) | None, _) => {
                return Err(ApplyError::DeleteMissing(path.clone()).into());
            }
        };
        if remove {
            entries.remove(name);
        }
        Ok(entries.is_empty())
    }

    fn put(
        &mut self,
        c: &Connection,
        path: &RepoPath,
        parts: &[PathElem],
        depth: usize,
        state: FileState,
    ) -> Result<()> {
        let entries = self.entries(c)?;
        let name = &parts[depth];
        if depth + 1 == parts.len() {
            if matches!(entries.get(name), Some(Node::Dir(_))) {
                return Err(ApplyError::DirInTheWay(path.clone()).into());
            }
            entries.insert(name.clone(), Node::File(state));
            return Ok(());
        }
        let child = entries
            .entry(name.clone())
            .or_insert_with(|| Node::Dir(Self::Loaded(BTreeMap::new())));
        match child {
            Node::Dir(dir) => dir.put(c, path, parts, depth + 1, state),
            Node::File(_) => Err(ApplyError::FileInTheWay {
                path: path.clone(),
                file: prefix(&parts[..=depth]),
            }
            .into()),
        }
    }

    /// Stores every loaded directory, bottom up, and returns this one's ID.
    fn store(self, c: &Connection) -> Result<TreeId> {
        match self {
            Self::Stored(id) => Ok(id),
            Self::Loaded(entries) => {
                let mut tree = Tree::default();
                for (name, node) in entries {
                    let entry = match node {
                        Node::File(state) => Entry::File(state),
                        Node::Dir(dir) => Entry::Dir(dir.store(c)?),
                    };
                    tree.entries.insert(name, entry);
                }
                db::put_tree(c, &tree)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_core::{BlobId, Changeset, IdentityTime};

    /// `(path, Some(content))` puts a regular file; `(path, None)` deletes.
    type Edits<'a> = &'a [(&'a str, Option<&'a str>)];

    fn regular(content: &str) -> FileState {
        FileState::Regular {
            content: BlobId::of(content.as_bytes()),
            executable: false,
        }
    }

    fn changes(edits: Edits<'_>) -> BTreeMap<RepoPath, Option<FileState>> {
        edits
            .iter()
            .map(|(p, content)| (RepoPath::new(*p).unwrap(), content.map(regular)))
            .collect()
    }

    fn path(p: &str) -> RepoPath {
        RepoPath::new(p).unwrap()
    }

    /// Runs `f` in one write transaction on a fresh store.
    fn with_db<T>(f: impl FnOnce(&Connection) -> Result<T>) -> T {
        let dir = tempfile::tempdir().unwrap();
        Store::open(dir.path()).unwrap().write(f).unwrap()
    }

    fn files(c: &Connection, root: TreeId) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![(String::new(), root)];
        while let Some((prefix, id)) = stack.pop() {
            for (name, entry) in db::get_tree(c, id).unwrap().entries {
                let path = format!("{prefix}{}", String::from_utf8_lossy(name.as_bytes()));
                match entry {
                    Entry::File(_) => out.push(path),
                    Entry::Dir(child) => stack.push((format!("{path}/"), child)),
                }
            }
        }
        out.sort();
        out
    }

    #[test]
    fn puts_and_deletes_build_the_expected_tree() {
        with_db(|c| {
            let t1 = apply(
                c,
                None,
                &changes(&[("a/b/c", Some("1")), ("a/d", Some("2")), ("e", Some("3"))]),
            )?;
            assert_eq!(files(c, t1), ["a/b/c", "a/d", "e"]);
            let t2 = apply(
                c,
                Some(t1),
                &changes(&[("a/b/c", None), ("f/g", Some("4"))]),
            )?;
            assert_eq!(files(c, t2), ["a/d", "e", "f/g"], "empty a/b is pruned");
            assert_eq!(
                lookup(c, t2, &path("a/d"))?,
                Some(Entry::File(regular("2")))
            );
            assert_eq!(lookup(c, t2, &path("a/b"))?, None);
            assert_eq!(lookup(c, t2, &path("e/x"))?, None, "a file has no children");
            Ok(())
        });
    }

    #[test]
    fn same_content_gives_same_tree_id() {
        with_db(|c| {
            let direct = apply(c, None, &changes(&[("x/y", Some("1"))]))?;
            let t = apply(c, None, &changes(&[("x/y", Some("1")), ("z", Some("2"))]))?;
            let via_delete = apply(c, Some(t), &changes(&[("z", None)]))?;
            assert_eq!(direct, via_delete);
            Ok(())
        });
    }

    #[test]
    fn file_and_directory_swap_in_one_changeset() {
        with_db(|c| {
            let t = apply(c, None, &changes(&[("a", Some("1"))]))?;
            let t = apply(c, Some(t), &changes(&[("a", None), ("a/b", Some("2"))]))?;
            assert_eq!(files(c, t), ["a/b"]);
            let t = apply(c, Some(t), &changes(&[("a", Some("3")), ("a/b", None)]))?;
            assert_eq!(files(c, t), ["a"]);
            Ok(())
        });
    }

    #[test]
    fn structural_conflicts_are_rejected() {
        with_db(|c| {
            let t = apply(c, None, &changes(&[("a", Some("1")), ("d/x", Some("2"))]))?;
            let cases: [(Edits<'_>, ApplyError); 6] = [
                (
                    &[("a/b", Some("x"))],
                    ApplyError::FileInTheWay {
                        path: path("a/b"),
                        file: path("a"),
                    },
                ),
                (&[("d", Some("x"))], ApplyError::DirInTheWay(path("d"))),
                (&[("nope", None)], ApplyError::DeleteMissing(path("nope"))),
                (&[("a/b", None)], ApplyError::DeleteMissing(path("a/b"))),
                (&[("d", None)], ApplyError::NotAFile(path("d"))),
                (
                    &[("n", Some("x")), ("n/m", Some("y"))],
                    ApplyError::FileInTheWay {
                        path: path("n/m"),
                        file: path("n"),
                    },
                ),
            ];
            for (edits, expected) in cases {
                match apply(c, Some(t), &changes(edits)) {
                    Err(Error::Apply(e)) => assert_eq!(e, expected),
                    other => panic!("expected {expected:?}, got {other:?}"),
                }
            }
            Ok(())
        });
    }

    /// A changeset applied to a flat path → content map, with the same rules:
    /// deletes first, then puts in path order. `None` means the changeset
    /// does not apply.
    fn apply_flat(
        files: &BTreeMap<String, u8>,
        changes: &BTreeMap<String, Option<u8>>,
    ) -> Option<BTreeMap<String, u8>> {
        let mut files = files.clone();
        for (path, _) in changes.iter().filter(|(_, c)| c.is_none()) {
            files.remove(path)?;
        }
        for (path, content) in changes {
            let Some(content) = content else { continue };
            let file_above = path
                .match_indices('/')
                .any(|(i, _)| files.contains_key(&path[..i]));
            let dir_here = files.keys().any(|f| f.starts_with(&format!("{path}/")));
            if file_above || dir_here {
                return None;
            }
            files.insert(path.clone(), *content);
        }
        Some(files)
    }

    fn snapshot(c: &Connection, root: TreeId) -> BTreeMap<String, u8> {
        files(c, root)
            .into_iter()
            .map(|path| {
                let Some(Entry::File(FileState::Regular { content, .. })) =
                    lookup(c, root, &RepoPath::new(path.as_str()).unwrap()).unwrap()
                else {
                    panic!("{path} is not a regular file");
                };
                let byte = (0..=u8::MAX)
                    .find(|b| BlobId::of(&[*b]) == content)
                    .expect("test content is one byte");
                (path, byte)
            })
            .collect()
    }

    proptest::proptest! {
        /// `apply` agrees with the flat model on every random edit sequence:
        /// the same files with the same content, and the same refusals.
        #[test]
        fn apply_matches_a_flat_model(
            history in proptest::collection::vec(
                proptest::collection::btree_map(
                    proptest::sample::select(
                        ["a", "b", "a/b", "a/c", "b/a", "a/b/c", "a/b/d", "c/a/b"].as_slice(),
                    ),
                    proptest::option::of(0u8..4),
                    1..5,
                ),
                1..6,
            )
        ) {
            with_db(|c| {
                let mut model = BTreeMap::new();
                let mut tree = None;
                for edits in &history {
                    let changes = edits
                        .iter()
                        .map(|(p, content)| {
                            let state = content.map(|b| FileState::Regular {
                                content: BlobId::of(&[b]),
                                executable: false,
                            });
                            (RepoPath::new(*p).unwrap(), state)
                        })
                        .collect();
                    let flat_changes = edits.iter().map(|(p, c)| ((*p).to_owned(), *c)).collect();
                    match (apply_flat(&model, &flat_changes), apply(c, tree, &changes)) {
                        (Some(next), Ok(next_tree)) => {
                            assert_eq!(snapshot(c, next_tree), next);
                            model = next;
                            tree = Some(next_tree);
                        }
                        (None, Err(Error::Apply(_))) => {}
                        (flat, real) => panic!("model {flat:?} but apply {real:?} for {edits:?}"),
                    }
                }
                Ok(())
            });
        }
    }

    /// Root trees of a fixed history, stored when written and derived when
    /// read, are pinned IDs. So a change to derivation that changes a tree
    /// fails here.
    #[test]
    fn root_trees_match_golden_ids() {
        let history: [Edits<'_>; 3] = [
            &[("a/b/c", Some("1")), ("a/d", Some("2")), ("e", Some("3"))],
            &[("a/b/c", None), ("a/b", Some("4")), ("f/g/h", Some("5"))],
            &[("e", None), ("a/d", Some("6")), ("f/g/h", None)],
        ];
        let golden = [
            "512cf237e5e62fc7d013ee67bd701857d637c2c7b3c60897ee9f57d4f63212b5",
            "8e7be185763f5990a6b48bca674e5766e80aa28a5769f46c530d8e4de2b1c8be",
            "27d510c1cac510175ac54737191dab75dbc4e3de391ee9c3b56233d74a70124c",
        ];
        for stored in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path()).unwrap();
            let (repo, ids) = store
                .write(|c| {
                    let repo = db::create_repo(c, "r")?;
                    let mut ids: Vec<ChangesetId> = Vec::new();
                    for edits in history {
                        let cs = Changeset {
                            parents: ids.last().copied().into_iter().collect(),
                            author: author(),
                            committer: None,
                            message: vec![],
                            changes: changes(edits),
                        };
                        ids.push(if stored {
                            store_changeset(c, repo, &cs)?.0
                        } else {
                            db::insert_changeset(c, repo, &cs)?
                        });
                    }
                    Ok((repo, ids))
                })
                .unwrap();
            let trees: Vec<String> = ids
                .iter()
                .map(|&cs| root_tree(&store, repo, cs).unwrap().to_string())
                .collect();
            assert_eq!(trees, golden, "stored when written: {stored}");
        }
    }

    fn author() -> IdentityTime {
        IdentityTime {
            name: b"a".to_vec(),
            email: b"a@x".to_vec(),
            unix_secs: 0,
            tz_offset_minutes: 0,
        }
    }

    #[test]
    fn root_tree_derives_a_long_chain_and_caches_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let author = author();
        let (repo, tip) = store
            .write(|c| {
                let repo = db::create_repo(c, "r")?;
                let mut parent: Option<ChangesetId> = None;
                for i in 0..2000 {
                    let cs = Changeset {
                        parents: parent.into_iter().collect(),
                        author: author.clone(),
                        committer: None,
                        message: vec![],
                        changes: changes(&[(&format!("f{}", i % 7), Some(&i.to_string()))]),
                    };
                    parent = Some(db::insert_changeset(c, repo, &cs)?);
                }
                Ok((repo, parent.unwrap()))
            })
            .unwrap();
        let tree = root_tree(&store, repo, tip).unwrap();
        assert_eq!(store.read(|c| Ok(files(c, tree))).unwrap().len(), 7);
        assert_eq!(root_tree(&store, repo, tip).unwrap(), tree);
    }
}
