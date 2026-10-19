//! Snapshots: apply a changeset to a tree, look up paths, and derive the
//! root tree of any changeset.
//!
//! Every function takes a `&mut dyn MetaTx`. Callers that write pass the
//! transaction of their write, so all trees from one apply or derivation
//! commit together.

use std::collections::{BTreeMap, HashMap};

use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileEntry, FileState, PathElem, RepoPath, Tree, TreeId,
};

use crate::backend::Backend;
use crate::db;
use crate::deadline::Deadline;
use crate::derived::{self, Kind};
use crate::error::{ApplyError, Error, Result};
use crate::graph::CommitGraph;
use crate::history::PathHistory;
use crate::meta::{MetaTx, RepoId};

/// The byte length of a blob, as the tree entries carry it. The store reads
/// it from the blob's frame header without unpacking it.
pub type BlobLens<'a> = &'a mut dyn FnMut(BlobId) -> Result<u64>;

/// The lens of the store's blobs, from each frame header, for the callers
/// that hold the store and no pre-read lengths, such as tests and the
/// in-transaction derive of `store_changeset`.
pub fn lens_of(store: &Backend) -> impl FnMut(BlobId) -> Result<u64> + '_ {
    |blob| Ok(store.blobs().size(blob)? as u64)
}

/// A lens that answers 0 for everything. For tests that build trees from
/// blobs they never store: the trees are only compared to themselves.
#[cfg(test)]
pub(crate) fn lens_none() -> impl FnMut(BlobId) -> Result<u64> {
    |_| Ok(0)
}

/// The lengths of the blobs that `puts` names, read from their frame
/// headers. Call it before a write transaction, so the one write
/// connection does no file I/O. A blob that is not stored fails here,
/// before any write.
///
/// # Errors
///
/// [`Error::NotFound`] if a blob is not stored.
pub fn lens_of_puts(
    store: &Backend,
    puts: impl Iterator<Item = BlobId>,
) -> Result<HashMap<BlobId, u64>> {
    let mut lens = HashMap::new();
    for blob in puts {
        if lens.contains_key(&blob) {
            continue;
        }
        lens.insert(blob, store.blobs().size(blob)? as u64);
    }
    Ok(lens)
}

/// A lens over lengths that a caller read before its transaction.
pub fn lens_from<S: std::hash::BuildHasher>(
    map: &HashMap<BlobId, u64, S>,
) -> impl FnMut(BlobId) -> Result<u64> + '_ {
    move |blob| {
        map.get(&blob)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("blob {blob}")))
    }
}

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
    c: &mut dyn MetaTx,
    base: Option<TreeId>,
    changes: &BTreeMap<RepoPath, Option<FileState>>,
    lens: BlobLens<'_>,
) -> Result<TreeId> {
    let mut root = base.map_or_else(|| Dir::Loaded(BTreeMap::new()), Dir::Stored);
    for (path, _) in changes.iter().filter(|(_, change)| change.is_none()) {
        root.delete(c, path, &parts(path))?;
    }
    for (path, state) in changes {
        if let Some(state) = state {
            // A gitlink names no blob, so it carries no length; its entry
            // ends at the commit bytes.
            let entry = match state.blob() {
                Some(blob) => FileEntry::new(*state, lens(blob)?),
                None => FileEntry::new(*state, 0),
            };
            root.put(c, path, &parts(path), 0, entry)?;
        }
    }
    root.store(c)
}

/// Finds the entry at `path` under `root`.
///
/// # Errors
///
/// Database errors.
pub fn lookup(c: &mut dyn MetaTx, root: TreeId, path: &RepoPath) -> Result<Option<Entry>> {
    let mut entry = Entry::Dir(root);
    for name in parts(path) {
        let Entry::Dir(id) = entry else {
            return Ok(None);
        };
        match c.get_tree(id)?.entries().clone().remove(&name) {
            Some(next) => entry = next,
            None => return Ok(None),
        }
    }
    Ok(Some(entry))
}

/// The root tree of a changeset's full snapshot: its first parent's root
/// tree plus its changes.
pub struct RootTree;

/// Derives the root tree of `cs`, and removes the rows of older
/// versions. Returns the kind's name for the `derive` command's report.
///
/// # Errors
///
/// Errors of the `derived` fill, and of the final flush.
pub fn backfill_root_tree(store: &Backend, repo: &str, cs: ChangesetId) -> Result<&'static str> {
    crate::derived::backfill::<RootTree>(store, repo, cs)
}

impl Kind for RootTree {
    const NAME: &'static str = "root_tree";
    const VERSION: u32 = 1;
    const NEEDS_LENS: bool = true;
    type Value = TreeId;

    fn derive(
        c: &mut dyn MetaTx,
        id: ChangesetId,
        changeset: &Changeset,
        parents: Vec<TreeId>,
        lens: BlobLens<'_>,
    ) -> Result<TreeId> {
        apply(c, parents.first().copied(), changeset.changes(), lens).map_err(|e| match e {
            Error::Apply(e) => Error::Corrupt(format!("stored changeset {id} does not apply: {e}")),
            other => other,
        })
    }

    fn encode(tree: &TreeId) -> Vec<u8> {
        tree.as_bytes().to_vec()
    }

    fn decode(bytes: &[u8]) -> Option<TreeId> {
        TreeId::try_from(bytes).ok()
    }
}

/// The root tree of a changeset that `repo` may see, deriving it if needed,
/// until `deadline`.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. [`Error::Corrupt`] if a
/// stored changeset does not apply to its parent tree.
/// [`Error::DeadlineExceeded`] once `deadline` has passed.
pub fn root_tree(
    store: &Backend,
    repo: RepoId,
    cs: ChangesetId,
    deadline: &Deadline,
) -> Result<TreeId> {
    derived::get_by::<RootTree>(store, repo, cs, deadline)
}

/// [`root_tree`] with no deadline: the caller has no request to serve, so
/// nothing bounds the fill in time. For the `fsck` command and tests.
///
/// # Errors
///
/// As [`root_tree`], without [`Error::DeadlineExceeded`].
pub fn root_tree_unbounded(store: &Backend, repo: RepoId, cs: ChangesetId) -> Result<TreeId> {
    derived::get_unbounded::<RootTree>(store, repo, cs)
}

/// Derives the root tree of `cs` in steps if it is missing, as the
/// `derived` mechanism fills a kind. Returns before it is durable.
///
/// # Errors
///
/// As [`derive_root_tree`].
pub fn fill_root_tree(store: &Backend, repo: RepoId, cs: ChangesetId) -> Result<TreeId> {
    derived::fill::<RootTree>(store, repo, cs)
}

/// Derives the root tree of `cs` inside the caller's transaction, as the
/// `derived` mechanism computes a kind's value.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. [`Error::Corrupt`] if a
/// stored changeset does not apply to its parent tree.
pub fn derive_root_tree(
    c: &mut dyn MetaTx,
    repo: RepoId,
    cs: ChangesetId,
    lens: BlobLens<'_>,
) -> Result<TreeId> {
    derived::derive::<RootTree>(c, repo, cs, lens)
}

/// Stores a changeset in `repo` with its root tree: the first parent's tree
/// plus the changes. Every parent must be in `repo`. Safe to repeat. This is
/// the one write path for changesets, used by commits, lands and imports.
///
/// If the first parent has no root tree, for example after a version bump,
/// this derives it in the caller's transaction, with no bound, and holds
/// the write connection while it runs. So a request calls [`fill_root_tree`]
/// on the first parent before its transaction, and so does git import, for
/// each first parent from before its batch.
///
/// Like each fill step of the `derived` mechanism, it also removes a few
/// root trees of older versions.
///
/// It also stores the changeset's node in the commit graph, if each parent
/// has one, and its path history, if its first parent has one, which the
/// `derived` mechanism's on-write hook does for every kind.
///
/// It adds each blob that the changeset names to the blobs of `repo`, so a
/// blob that a changeset of `repo` names always belongs to `repo`, also
/// after an import, which uploads nothing.
///
/// # Errors
///
/// [`Error::Apply`] if the changes do not apply to the first parent's tree;
/// callers decide whether that is the client's fault or corruption.
/// [`Error::NotFound`] if a parent is not in `repo`.
pub fn store_changeset(
    c: &mut dyn MetaTx,
    repo: RepoId,
    cs: &Changeset,
    lens: BlobLens<'_>,
) -> Result<(ChangesetId, TreeId)> {
    let base = cs
        .parents()
        .first()
        .map(|&parent| derive_root_tree(c, repo, parent, lens))
        .transpose()?;
    let tree = apply(c, base, cs.changes(), lens)?;
    let id = c.insert_changeset(repo, cs)?;
    for (_, blob) in db::puts(cs) {
        c.add_repo_blob(repo, blob)?;
    }
    derived::put::<RootTree>(c, id, &tree)?;
    derived::remove_old::<RootTree>(c)?;
    derived::on_write::<CommitGraph>(c, id, cs, lens)?;
    derived::on_write::<PathHistory>(c, id, cs, lens)?;
    Ok((id, tree))
}

/// The parts of a path, as tree entry names.
pub(crate) fn parts(path: &RepoPath) -> Vec<PathElem> {
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
    File(FileEntry),
    Dir(Dir),
}

impl Dir {
    fn entries(&mut self, c: &mut dyn MetaTx) -> Result<&mut BTreeMap<PathElem, Node>> {
        if let Self::Stored(id) = *self {
            let entries = c
                .get_tree(id)?
                .entries()
                .clone()
                .into_iter()
                .map(|(name, entry)| {
                    let node = match entry {
                        Entry::File(file) => Node::File(file),
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
    fn delete(&mut self, c: &mut dyn MetaTx, path: &RepoPath, parts: &[PathElem]) -> Result<bool> {
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
        c: &mut dyn MetaTx,
        path: &RepoPath,
        parts: &[PathElem],
        depth: usize,
        file: FileEntry,
    ) -> Result<()> {
        let entries = self.entries(c)?;
        let name = &parts[depth];
        if depth + 1 == parts.len() {
            if matches!(entries.get(name), Some(Node::Dir(_))) {
                return Err(ApplyError::DirInTheWay(path.clone()).into());
            }
            entries.insert(name.clone(), Node::File(file));
            return Ok(());
        }
        let child = entries
            .entry(name.clone())
            .or_insert_with(|| Node::Dir(Self::Loaded(BTreeMap::new())));
        match child {
            Node::Dir(dir) => dir.put(c, path, parts, depth + 1, file),
            Node::File(_) => Err(ApplyError::FileInTheWay {
                path: path.clone(),
                file: prefix(&parts[..=depth]),
            }
            .into()),
        }
    }

    /// Stores every loaded directory, bottom up, and returns this one's ID.
    // SHORTCUT: a change rewrites each directory on its paths whole, so a
    // write in a directory of n entries stores about n x (name + 38) bytes:
    // 500 KB for 10,000 names of 12 bytes. Upgrade: directories cut into
    // chunks, a new tree format, so a write stores only the chunks it changes
    // (https://github.com/bnsd55/open-scion/issues/114).
    fn store(self, c: &mut dyn MetaTx) -> Result<TreeId> {
        match self {
            Self::Stored(id) => Ok(id),
            Self::Loaded(entries) => {
                let mut loaded = std::collections::BTreeMap::new();
                for (name, node) in entries {
                    let entry = match node {
                        Node::File(file) => Entry::File(file),
                        Node::Dir(dir) => Entry::Dir(dir.store(c)?),
                    };
                    loaded.insert(name, entry);
                }
                let tree = Tree::new(loaded).expect("loaded entries fit the format");
                c.put_tree(&tree)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::db::SqliteTx;
    use crate::meta::Meta as _;
    use crate::store::Store;
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
    fn with_db<T>(f: impl FnOnce(&mut SqliteTx<'_>) -> Result<T>) -> T {
        let dir = tempfile::tempdir().unwrap();
        Store::open(dir.path()).unwrap().write(f).unwrap()
    }

    fn files(c: &mut dyn MetaTx, root: TreeId) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![(String::new(), root)];
        while let Some((prefix, id)) = stack.pop() {
            for (name, entry) in c.get_tree(id).unwrap().entries() {
                let path = format!("{prefix}{}", String::from_utf8_lossy(name.as_bytes()));
                match entry {
                    Entry::File(_) => out.push(path),
                    Entry::Dir(child) => stack.push((format!("{path}/"), *child)),
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
                &mut lens_none(),
            )?;
            assert_eq!(files(c, t1), ["a/b/c", "a/d", "e"]);
            let t2 = apply(
                c,
                Some(t1),
                &changes(&[("a/b/c", None), ("f/g", Some("4"))]),
                &mut lens_none(),
            )?;
            assert_eq!(files(c, t2), ["a/d", "e", "f/g"], "empty a/b is pruned");
            assert_eq!(
                lookup(c, t2, &path("a/d"))?,
                Some(Entry::File(FileEntry::new(regular("2"), 0)))
            );
            assert_eq!(lookup(c, t2, &path("a/b"))?, None);
            assert_eq!(lookup(c, t2, &path("e/x"))?, None, "a file has no children");
            Ok(())
        });
    }

    #[test]
    fn same_content_gives_same_tree_id() {
        with_db(|c| {
            let direct = apply(c, None, &changes(&[("x/y", Some("1"))]), &mut lens_none())?;
            let t = apply(
                c,
                None,
                &changes(&[("x/y", Some("1")), ("z", Some("2"))]),
                &mut lens_none(),
            )?;
            let via_delete = apply(c, Some(t), &changes(&[("z", None)]), &mut lens_none())?;
            assert_eq!(direct, via_delete);
            Ok(())
        });
    }

    #[test]
    fn file_and_directory_swap_in_one_changeset() {
        with_db(|c| {
            let t = apply(c, None, &changes(&[("a", Some("1"))]), &mut lens_none())?;
            let t = apply(
                c,
                Some(t),
                &changes(&[("a", None), ("a/b", Some("2"))]),
                &mut lens_none(),
            )?;
            assert_eq!(files(c, t), ["a/b"]);
            let t = apply(
                c,
                Some(t),
                &changes(&[("a", Some("3")), ("a/b", None)]),
                &mut lens_none(),
            )?;
            assert_eq!(files(c, t), ["a"]);
            Ok(())
        });
    }

    #[test]
    fn structural_conflicts_are_rejected() {
        with_db(|c| {
            let t = apply(
                c,
                None,
                &changes(&[("a", Some("1")), ("d/x", Some("2"))]),
                &mut lens_none(),
            )?;
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
                match apply(c, Some(t), &changes(edits), &mut lens_none()) {
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

    fn snapshot(c: &mut dyn MetaTx, root: TreeId) -> BTreeMap<String, u8> {
        files(c, root)
            .into_iter()
            .map(|path| {
                let Some(Entry::File(file)) =
                    lookup(c, root, &RepoPath::new(path.as_str()).unwrap()).unwrap()
                else {
                    panic!("{path} is not a regular file");
                };
                let FileState::Regular { content, .. } = file.state() else {
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
                    match (
                        apply_flat(&model, &flat_changes),
                        apply(c, tree, &changes, &mut lens_none()),
                    ) {
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
            "2fcd3277f226b4342d77cb088de545ac630a6f1289d51f9464aba202e6cd6fda",
            "b50d00563540613f21dc430f76afedd5da998681d321db7d3f5ef990a7f3a90c",
            "b4717ab9e5bbd8b20c844a4236fededbea08e83cedbd871731fa9e948a0cbac8",
        ];
        for stored in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
            // The chain's six puts name blobs "1".."6"; their files feed the
            // real lens, so stored and re-derived trees carry true lengths.
            for c in ["1", "2", "3", "4", "5", "6"] {
                store.blobs().put(c.as_bytes()).unwrap();
            }
            let (repo, ids) = store
                .write(|c| {
                    let repo = c.create_repo("r")?;
                    let mut ids: Vec<ChangesetId> = Vec::new();
                    for edits in history {
                        let cs = changeset(ids.last().copied(), edits);
                        ids.push(if stored {
                            store_changeset(c, repo, &cs, &mut lens_of(&store))?.0
                        } else {
                            c.insert_changeset(repo, &cs)?
                        });
                    }
                    Ok((repo, ids))
                })
                .unwrap();
            let trees: Vec<String> = ids
                .iter()
                .map(|&cs| root_tree_unbounded(&store, repo, cs).unwrap().to_string())
                .collect();
            assert_eq!(trees, golden, "stored when written: {stored}");
        }
    }

    fn changeset(parent: Option<ChangesetId>, edits: Edits<'_>) -> Changeset {
        Changeset::new(
            parent.into_iter().collect(),
            IdentityTime::new(b"a".to_vec(), b"a@x".to_vec(), 0, 0)
                .expect("identity fits the format"),
            None,
            vec![],
            changes(edits),
        )
        .expect("changeset fits the format")
    }

    /// A cold root tree and a past deadline: the read refuses instead of
    /// deriving the history first. On main, where the derive ran without
    /// the deadline, `read_file` returned the file's bytes.
    #[test]
    fn a_read_with_a_past_deadline_derives_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
        store.blobs().put(b"1").unwrap();
        // A changeset with no root tree row yet.
        let cs = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                c.insert_changeset(repo, &changeset(None, &[("a/b", Some("1"))]))
            })
            .unwrap();
        let past = crate::deadline::Deadline::new(std::time::Instant::now());
        let got = crate::read::read_file(
            &store,
            "r",
            cs,
            &scion_core::RepoPath::new("a/b").unwrap(),
            &past,
        );
        assert!(matches!(got, Err(Error::DeadlineExceeded(_))), "{got:?}");
        let cold = store.read(|c| Ok(derived::load::<RootTree>(c, cs).unwrap()));
        assert!(cold.unwrap().is_none(), "the read derived a root tree");
    }

    /// `GetRootTree` sends a tree ID that the client then fetches. So a tree
    /// derived during the read is durable before the reply, or a power loss
    /// could lose a tree that the client was told about.
    #[test]
    fn get_root_tree_flushes_once_after_it_derives() {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
        store.blobs().put(b"1").unwrap();
        // A changeset with no root tree row yet.
        let cs = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                c.insert_changeset(repo, &changeset(None, &[("a/b", Some("1"))]))
            })
            .unwrap();
        let get_root_tree = || {
            let before = store.blobs().full_flushes();
            let deadline = crate::deadline::Deadline::new(
                std::time::Instant::now() + crate::deadline::CALL_LIMIT,
            );
            let tree = crate::read::root_tree(&store, "r", cs, &deadline).unwrap();
            (tree, store.blobs().full_flushes() - before)
        };
        let (tree, flushes) = get_root_tree();
        // None where the commit's own fsync flushes the drive cache.
        let want = u64::from(crate::store::FSYNC_LEAVES_DRIVE_CACHE);
        assert_eq!(flushes, want, "the derivation is durable before the reply");
        assert_eq!(get_root_tree(), (tree, 0), "a stored tree needs no flush");
    }

    #[test]
    fn root_tree_derives_a_long_chain_and_caches_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
        let author = IdentityTime::new(b"a".to_vec(), b"a@x".to_vec(), 0, 0)
            .expect("identity fits the format");
        // Every put of the chain names its index as the blob; their files
        // feed the real lens that reads the chain's trees with.
        for i in 0..2000 {
            store.blobs().put(i.to_string().as_bytes()).unwrap();
        }
        let (repo, tip) = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                let mut parent: Option<ChangesetId> = None;
                for i in 0..2000 {
                    let cs = Changeset::new(
                        parent.into_iter().collect(),
                        author.clone(),
                        None,
                        vec![],
                        changes(&[(&format!("f{}", i % 7), Some(&i.to_string()))]),
                    )
                    .expect("changeset fits the format");
                    parent = Some(c.insert_changeset(repo, &cs)?);
                }
                Ok((repo, parent.unwrap()))
            })
            .unwrap();
        let tree = root_tree_unbounded(&store, repo, tip).unwrap();
        assert_eq!(store.read(|c| Ok(files(c, tree))).unwrap().len(), 7);
        assert_eq!(root_tree_unbounded(&store, repo, tip).unwrap(), tree);
    }
}
