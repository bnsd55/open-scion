//! Path history: for each changeset and path, the newest changeset on its
//! first-parent history that changed the path.
//!
//! Kind `path_history` gives each changeset a history tree. It has the shape
//! of a directory tree, but it holds every path that a changeset on the
//! first-parent history put or deleted, also after a delete. Each entry
//! holds:
//!
//! - `last`: the newest changeset, at or below this one on its first-parent
//!   history, that changed the path or a path below it, with its generation,
//! - `own`: the generation of the newest one that put or deleted a file at
//!   the path itself,
//! - `below`: the node of the paths below it, if any changed.
//!
//! A changeset's tree is its first parent's tree, with the entries of its
//! changed paths and of the directories above them set to the changeset. So
//! a changeset writes one node per directory on its changed paths, as a root
//! tree does, and shares every other node with its first parent. A node is
//! one directory level, stored in `history_nodes` by the hash of its bytes.
//!
//! Two queries read it, each one node per path part:
//!
//! - `Log` with a path ([`newest_change`]): the newest change of a path
//!   from a changeset is `last` at the path in that changeset's tree. The
//!   change before it is `last` in the tree of that change's first parent.
//! - The conflict check of a land on an old base ([`touched_after`]).
//!   Generations strictly increase along first parents, so on that history
//!   a path changed after the base exactly when a generation in its entry is
//!   above the base's.

use std::collections::{BTreeMap, HashMap, btree_map, hash_map};
use std::fmt;

use rusqlite::{Connection, OptionalExtension, params};
use scion_core::{Changeset, ChangesetId, FileState, PathElem, RepoPath};

use crate::db;
use crate::deadline::Deadline;
use crate::derived::{self, Kind};
use crate::error::{Error, Result};
use crate::tree;

/// Hashed before a node's bytes, so a node ID is never the ID of another
/// kind of data.
const DOMAIN: &[u8] = b"scion\0history\0v1\0";
/// Entry flag: `own` follows.
const OWN: u8 = 1;
/// Entry flag: `below` follows.
const BELOW: u8 = 2;

#[cfg(test)]
thread_local! {
    /// Nodes read on this thread. Tests use it to bound the reads of a
    /// query.
    pub(crate) static NODE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The ID of a history node: the hash of its bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId([u8; 32]);

impl NodeId {
    fn of(body: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(DOMAIN);
        hasher.update(body);
        Self(*hasher.finalize().as_bytes())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&blake3::Hash::from_bytes(self.0).to_hex())
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({self})")
    }
}

/// A changeset and its generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Stamp {
    id: ChangesetId,
    generation: i64,
}

/// One entry of a history node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    /// The newest changeset that changed this path or a path below it.
    last: Stamp,
    /// The generation of the newest changeset that put or deleted a file at
    /// this path.
    own: Option<i64>,
    /// The node of the paths below this one, if any changed.
    below: Option<NodeId>,
}

/// One directory level of a history tree, by entry name.
type Node = BTreeMap<PathElem, Entry>;

/// The history tree of a changeset, as the ID of its root node.
pub struct PathHistory;

impl Kind for PathHistory {
    const NAME: &'static str = "path_history";
    const VERSION: u32 = 1;
    type Value = NodeId;

    fn derive(
        c: &Connection,
        id: ChangesetId,
        changeset: &Changeset,
        parents: Vec<NodeId>,
    ) -> Result<NodeId> {
        let by = Stamp {
            id,
            generation: db::generation_of(c, id)?,
        };
        apply(c, parents.first().copied(), by, &changeset.changes)
    }

    fn encode(root: &NodeId) -> Vec<u8> {
        root.0.to_vec()
    }

    fn decode(bytes: &[u8]) -> Option<NodeId> {
        bytes.try_into().ok().map(NodeId)
    }
}

/// The tree of `base` (`None` is the empty tree) with the entry of each
/// changed path, and of each directory above it, stamped with `by`. Stores
/// the new nodes and returns the new root.
fn apply(
    c: &Connection,
    base: Option<NodeId>,
    by: Stamp,
    changes: &BTreeMap<RepoPath, Option<FileState>>,
) -> Result<NodeId> {
    if let (Some(base), true) = (base, changes.is_empty()) {
        return Ok(base);
    }
    let mut root = Edit::load(c, base)?;
    for path in changes.keys() {
        root.stamp(c, &tree::parts(path), by)?;
    }
    root.store(c)
}

/// A node being edited: its entries, and the nodes below them that are
/// being edited too. Only the nodes on changed paths are loaded.
struct Edit {
    entries: Node,
    below: BTreeMap<PathElem, Edit>,
}

impl Edit {
    fn load(c: &Connection, id: Option<NodeId>) -> Result<Self> {
        Ok(Self {
            entries: match id {
                Some(id) => get_node(c, id)?.0,
                None => Node::new(),
            },
            below: BTreeMap::new(),
        })
    }

    /// Stamps the entry at `parts` with `by`, and each entry above it.
    fn stamp(&mut self, c: &Connection, parts: &[PathElem], by: Stamp) -> Result<()> {
        let (name, rest) = parts.split_first().expect("a path has at least one part");
        let entry = self.entries.entry(name.clone()).or_insert(Entry {
            last: by,
            own: None,
            below: None,
        });
        entry.last = by;
        if rest.is_empty() {
            entry.own = Some(by.generation);
            return Ok(());
        }
        let stored = entry.below;
        let edit = match self.below.entry(name.clone()) {
            btree_map::Entry::Occupied(edit) => edit.into_mut(),
            btree_map::Entry::Vacant(edit) => edit.insert(Self::load(c, stored)?),
        };
        edit.stamp(c, rest, by)
    }

    /// Stores every edited node, bottom up, and returns this one's ID.
    fn store(mut self, c: &Connection) -> Result<NodeId> {
        for (name, edit) in self.below {
            let below = edit.store(c)?;
            let entry = self
                .entries
                .get_mut(&name)
                .expect("an edited node has an entry");
            entry.below = Some(below);
        }
        put_node(c, &self.entries)
    }
}

/// What a history tree holds for one path.
#[derive(Default)]
struct Hit {
    /// The newest changeset that changed the path or a path below it.
    last: Option<Stamp>,
    /// The newest generation that put or deleted a file at a directory
    /// above the path, or 0.
    above: i64,
}

/// The nodes that one call has read, so that it reads each one once, and
/// how many bytes they hold.
#[derive(Default)]
pub(crate) struct Nodes {
    nodes: HashMap<NodeId, Node>,
    bytes: usize,
}

impl Nodes {
    /// The stored bytes of the nodes read so far.
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    fn get(&mut self, c: &Connection, id: NodeId) -> Result<&Node> {
        Ok(match self.nodes.entry(id) {
            hash_map::Entry::Occupied(node) => node.into_mut(),
            hash_map::Entry::Vacant(slot) => {
                let (node, bytes) = get_node(c, id)?;
                self.bytes += bytes;
                slot.insert(node)
            }
        })
    }
}

/// What the tree at `root` holds for `path`. Reads at most one node per
/// part of `path`, and none that `nodes` holds.
fn lookup(c: &Connection, nodes: &mut Nodes, root: NodeId, path: &RepoPath) -> Result<Hit> {
    let parts = tree::parts(path);
    let (name, dirs) = parts.split_last().expect("a path has at least one part");
    let mut hit = Hit::default();
    let mut node = root;
    for dir in dirs {
        let Some(entry) = nodes.get(c, node)?.get(dir).copied() else {
            return Ok(hit);
        };
        hit.above = hit.above.max(entry.own.unwrap_or(0));
        let Some(below) = entry.below else {
            return Ok(hit);
        };
        node = below;
    }
    hit.last = nodes.get(c, node)?.get(name).map(|entry| entry.last);
    Ok(hit)
}

/// The newest changeset at or below `from`, on its first-parent history,
/// that changes `path`: the file, or anything below it if it is a
/// directory. This is the rule of [`crate::read::changes_path`]. `from` has
/// a value of [`PathHistory`]. Reads at most one node per part of `path`,
/// and none that `nodes` holds.
///
/// # Errors
///
/// [`Error::Corrupt`] if `from` has no value, or a node is missing.
pub(crate) fn newest_change(
    c: &Connection,
    nodes: &mut Nodes,
    from: ChangesetId,
    path: &RepoPath,
) -> Result<Option<ChangesetId>> {
    let root = derived::load::<PathHistory>(c, from)?
        .ok_or_else(|| Error::Corrupt(format!("path history: no value for {from}")))?;
    let hit = lookup(c, nodes, root, path)?;
    Ok(hit.last.map(|last| last.id))
}

/// The paths of `paths` that a changeset newer than `base_gen` touched, on
/// the first-parent history whose tree is `root`: the same path, a path
/// below it, or a file at a directory above it. This is the rule of the
/// land's path index. Reads each node on the paths at most once.
///
/// # Errors
///
/// [`Error::Corrupt`] if a node is missing. [`Error::DeadlineExceeded`]
/// once `deadline` has passed.
pub(crate) fn touched_after<'p>(
    c: &Connection,
    root: NodeId,
    paths: impl IntoIterator<Item = &'p RepoPath>,
    base_gen: i64,
    deadline: &Deadline,
) -> Result<Vec<RepoPath>> {
    let mut nodes = Nodes::default();
    let mut touched = Vec::new();
    for path in paths {
        deadline.check()?;
        let hit = lookup(c, &mut nodes, root, path)?;
        if hit.above > base_gen || hit.last.is_some_and(|last| last.generation > base_gen) {
            touched.push(path.clone());
        }
    }
    Ok(touched)
}

/// Stores a node. Safe to repeat.
// SHORTCUT: a node holds every name that its directory ever had on the
// first-parent history, and a change rewrites each node on its paths whole.
// Ceiling: a write in a directory stores name + 51 to 83 bytes for each name
// it ever had: 630 KB for 10,000 names of 12 bytes. Upgrade: nodes cut into
// chunks, so a write stores only the chunks it changes, as for trees
// (https://github.com/bnsd55/open-scion/issues/114).
fn put_node(c: &Connection, node: &Node) -> Result<NodeId> {
    let body = encode_node(node);
    let id = NodeId::of(&body);
    c.prepare_cached("INSERT OR IGNORE INTO history_nodes (id, body) VALUES (?1, ?2)")?
        .execute(params![&id.0[..], body])?;
    Ok(id)
}

/// Reads a node and checks it against its ID. Also returns the length of
/// its stored bytes.
fn get_node(c: &Connection, id: NodeId) -> Result<(Node, usize)> {
    #[cfg(test)]
    NODE_READS.set(NODE_READS.get() + 1);
    let body: Vec<u8> = c
        .prepare_cached("SELECT body FROM history_nodes WHERE id = ?1")?
        .query_row([&id.0[..]], |row| row.get(0))
        .optional()?
        .ok_or_else(|| Error::Corrupt(format!("history node {id} is missing")))?;
    if NodeId::of(&body) != id {
        return Err(Error::Corrupt(format!(
            "history node {id} does not match its ID"
        )));
    }
    let node = decode_node(&body)
        .ok_or_else(|| Error::Corrupt(format!("history node {id} does not decode")))?;
    Ok((node, body.len()))
}

/// The bytes of a node: its entries in name order, each
/// `u16le(len(name)) || name || u8(flags) || last ID || i64le(last
/// generation) || [i64le(own)] || [below]`. Flag 1 says that `own` follows,
/// flag 2 that `below` follows.
fn encode_node(node: &Node) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, entry) in node {
        let name = name.as_bytes();
        let len = u16::try_from(name.len()).expect("a name is at most 4,096 bytes");
        out.extend(len.to_le_bytes());
        out.extend(name);
        let own = if entry.own.is_some() { OWN } else { 0 };
        let below = if entry.below.is_some() { BELOW } else { 0 };
        out.push(own | below);
        out.extend(entry.last.id.as_bytes());
        out.extend(entry.last.generation.to_le_bytes());
        if let Some(own) = entry.own {
            out.extend(own.to_le_bytes());
        }
        if let Some(below) = entry.below {
            out.extend(below.0);
        }
    }
    out
}

/// The node in `bytes`, or `None` if they are not the bytes of one.
fn decode_node(mut bytes: &[u8]) -> Option<Node> {
    let int = |bytes: &[u8]| Some(i64::from_le_bytes(bytes.try_into().ok()?));
    let mut node = Node::new();
    while !bytes.is_empty() {
        let len = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().ok()?);
        let name = PathElem::new(take(&mut bytes, usize::from(len))?).ok()?;
        let flags = take(&mut bytes, 1)?[0];
        // An entry is there because the path or a path below it changed.
        if flags == 0 || flags & !(OWN | BELOW) != 0 {
            return None;
        }
        let last = Stamp {
            id: ChangesetId::try_from(take(&mut bytes, 32)?).ok()?,
            generation: int(take(&mut bytes, 8)?)?,
        };
        let own = match flags & OWN {
            0 => None,
            _ => Some(int(take(&mut bytes, 8)?)?),
        };
        let below = match flags & BELOW {
            0 => None,
            _ => Some(NodeId(take(&mut bytes, 32)?.try_into().ok()?)),
        };
        if node.last_key_value().is_some_and(|(prev, _)| *prev >= name) {
            return None;
        }
        node.insert(name, Entry { last, own, below });
    }
    Some(node)
}

/// The first `n` bytes of `bytes`, which then start after them.
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    let (head, rest) = bytes.split_at_checked(n)?;
    *bytes = rest;
    Some(head)
}

#[cfg(test)]
mod tests {
    use scion_core::{BlobId, FileState, IdentityTime};

    use super::*;
    use crate::db::{RepoId, testing};
    use crate::read;
    use crate::store::Store;
    use crate::write;

    fn path(p: &str) -> RepoPath {
        RepoPath::new(p).unwrap()
    }

    fn later() -> Deadline {
        Deadline::new(std::time::Instant::now() + std::time::Duration::from_secs(600))
    }

    /// A changeset that puts a file with `blob`, or deletes it, at each path.
    fn changeset(parent: Option<ChangesetId>, edits: &[(&str, Option<BlobId>)]) -> Changeset {
        Changeset {
            parents: parent.into_iter().collect(),
            author: IdentityTime {
                name: b"a".to_vec(),
                email: b"a@x".to_vec(),
                unix_secs: 0,
                tz_offset_minutes: 0,
            },
            committer: None,
            message: vec![],
            changes: edits
                .iter()
                .map(|&(p, blob)| {
                    let state = blob.map(|content| FileState::Regular {
                        content,
                        executable: false,
                    });
                    (path(p), state)
                })
                .collect(),
        }
    }

    /// A new store with repo `r`.
    fn open() -> (tempfile::TempDir, Store, RepoId) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let repo = store.write(|c| db::create_repo(c, "r")).unwrap();
        (dir, store, repo)
    }

    /// The changes of one changeset of a chain: puts (`true`) and deletes.
    type Edits<'a> = &'a [(&'a str, bool)];

    /// Stores a first-parent chain in `repo`, from `parent` on, through the
    /// one write path (`stored`) or with no derived data. Changeset `i`
    /// makes the changes `history[i]`. Returns the IDs, oldest first.
    fn store_chain(
        store: &Store,
        repo: RepoId,
        parent: Option<ChangesetId>,
        history: &[Edits<'_>],
        stored: bool,
    ) -> Vec<ChangesetId> {
        store
            .write(|c| {
                let mut ids: Vec<ChangesetId> = Vec::new();
                for (i, edits) in history.iter().enumerate() {
                    let blob = BlobId::of(i.to_string().as_bytes());
                    let edits: Vec<_> = edits
                        .iter()
                        .map(|&(p, put)| (p, put.then_some(blob)))
                        .collect();
                    let cs = changeset(ids.last().copied().or(parent), &edits);
                    ids.push(if stored {
                        tree::store_changeset(c, repo, &cs)?.0
                    } else {
                        db::insert_changeset(c, repo, &cs)?
                    });
                }
                Ok(ids)
            })
            .unwrap()
    }

    /// Every changeset of a path log from `from`, at most `limit` per call.
    fn path_log(store: &Store, from: ChangesetId, p: &str, limit: usize) -> Vec<ChangesetId> {
        let mut ids = Vec::new();
        let mut next = Some(from);
        while let Some(from) = next {
            let page = read::log_page(store, "r", from, Some(&path(p)), limit, &later()).unwrap();
            // Small directories never reach the bound of node bytes.
            assert!(
                page.entries.len() == limit.min(read::MAX_SCAN) || page.next.is_none(),
                "a page is full unless the history ends"
            );
            ids.extend(page.entries.into_iter().map(|(id, _)| id));
            next = page.next;
        }
        ids
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

        /// On any history, with merges, deletes, adds again, and paths that
        /// are files at one time and directories at another, a path log
        /// gives what a walk of first parents finds, in full pages.
        #[test]
        fn a_path_log_matches_a_walk_of_first_parents(
            (parents, edits) in testing::histories(24),
            limit in 1..4_usize,
        ) {
            let (_dir, store, repo) = open();
            // In one transaction: a commit per step would take most of the
            // time.
            let ids = store
                .write(|c| {
                    let ids = testing::insert_history(c, repo, &parents, &edits)?;
                    for &tip in &ids {
                        derived::derive::<PathHistory>(c, repo, tip)?;
                    }
                    Ok(ids)
                })
                .unwrap();
            let queried = testing::PATHS.into_iter().chain(["c", "c/a", "a/b/c/d", "z"]);
            for p in queried {
                for &tip in &ids {
                    let mut walked = Vec::new();
                    let mut next = Some(tip);
                    while let Some(id) = next {
                        let cs = store.read(|c| db::get_changeset(c, repo, id)).unwrap();
                        if read::changes_path(&cs, &path(p)) {
                            walked.push(id);
                        }
                        next = cs.parents.first().copied();
                    }
                    proptest::prop_assert_eq!(path_log(&store, tip, p, limit), walked, "{}", p);
                }
            }
        }
    }

    #[test]
    fn a_path_log_reads_only_the_changesets_that_change_the_path() {
        let (_dir, store, repo) = open();
        // Every third changeset changes `d/f`, 1,001 of them; the others
        // change `g`.
        let history: Vec<Edits<'_>> = (0..=3 * read::MAX_SCAN)
            .map(|i| {
                if i % 3 == 0 {
                    &[("d/f", true)][..]
                } else {
                    &[("g", true)][..]
                }
            })
            .collect();
        let ids = store_chain(&store, repo, None, &history, true);
        let tip = *ids.last().unwrap();
        let changed: Vec<ChangesetId> = ids.iter().copied().step_by(3).rev().collect();

        db::CHANGESET_READS.set(0);
        NODE_READS.set(0);
        let page = read::log_page(&store, "r", tip, Some(&path("d/f")), usize::MAX, &later());
        let page = page.unwrap();
        // One call returns a full page, past 3,000 changesets.
        let found: Vec<ChangesetId> = page.entries.iter().map(|(id, _)| *id).collect();
        assert_eq!(found, changed[..read::MAX_SCAN]);
        assert_eq!(page.next, Some(ids[2]), "the parent of the last entry");
        // One changeset per entry, and one node per part of the path.
        assert_eq!(db::CHANGESET_READS.get(), read::MAX_SCAN);
        assert_eq!(NODE_READS.get(), 2 * read::MAX_SCAN);

        let rest = read::log_page(
            &store,
            "r",
            ids[2],
            Some(&path("d/f")),
            usize::MAX,
            &later(),
        );
        let rest = rest.unwrap();
        let found: Vec<ChangesetId> = rest.entries.iter().map(|(id, _)| *id).collect();
        assert_eq!(found, [ids[0]]);
        assert_eq!(rest.next, None);
    }

    #[test]
    fn a_path_log_stops_at_its_node_bytes_after_one_entry() {
        let (_dir, store, repo) = open();
        // `d` holds 10,000 files, so its node is larger than the bound. Then
        // three changesets change `d/f0`.
        let wide: Vec<String> = (0..10_000).map(|i| format!("d/f{i}")).collect();
        let first: Vec<(&str, bool)> = wide.iter().map(|p| (p.as_str(), true)).collect();
        let mut history: Vec<Edits<'_>> = vec![&first];
        history.extend([&[("d/f0", true)][..]; 3]);
        let ids = store_chain(&store, repo, None, &history, true);
        let page_of = |from| {
            let page = read::log_page(&store, "r", from, Some(&path("d/f0")), 10, &later());
            let page = page.unwrap();
            let found: Vec<ChangesetId> = page.entries.iter().map(|(id, _)| *id).collect();
            (found, page.next)
        };
        // Each page holds one entry, and says where to go on.
        assert_eq!(page_of(ids[3]), (vec![ids[3]], Some(ids[2])));
        assert_eq!(page_of(ids[2]), (vec![ids[2]], Some(ids[1])));
        assert_eq!(page_of(ids[0]), (vec![ids[0]], None));
        // A file in a small directory fills its page.
        let small = store_chain(&store, repo, Some(ids[3]), &[&[("e", true)][..]; 3], true);
        let page = read::log_page(&store, "r", small[2], Some(&path("e")), 10, &later());
        assert_eq!(page.unwrap().entries.len(), 3);
    }

    #[test]
    fn a_draft_changeset_has_a_path_history() {
        let (_dir, store, repo) = open();
        let main = store_chain(&store, repo, None, &[&[("a", true)], &[("b", true)]], true);
        let blob = store.blobs().put(b"draft").unwrap();
        // A changeset that no ref points to gets its path history when it is
        // written, because its parent has one.
        let draft = changeset(Some(main[1]), &[("a", Some(blob))]);
        let draft = write::create_changeset(&store, "r", &draft.encode()).unwrap();
        assert!(
            store
                .read(|c| derived::load::<PathHistory>(c, draft))
                .unwrap()
                .is_some()
        );
        assert_eq!(path_log(&store, draft, "a", 10), [draft, main[0]]);

        // On a history from before path history, the first path log derives
        // it, in steps, for the draft's first-parent history only.
        let old = store_chain(
            &store,
            repo,
            None,
            &[&[("x", true)], &[("y", true)], &[("x", false)]],
            false,
        );
        let draft = changeset(Some(old[2]), &[("y", Some(blob))]);
        let draft = write::create_changeset(&store, "r", &draft.encode()).unwrap();
        let stored = |cs| store.read(|c| derived::load::<PathHistory>(c, cs)).unwrap();
        assert!(stored(draft).is_none(), "its parent has none");
        let commits = store.commits();
        assert_eq!(path_log(&store, draft, "x", 10), [old[2], old[0]]);
        assert_eq!(path_log(&store, draft, "y", 10), [draft, old[1]]);
        assert_eq!(store.commits() - commits, derived::steps(4));
        assert!(old.iter().all(|&cs| stored(cs).is_some()));
    }

    /// The root nodes of a fixed history are pinned, both stored when
    /// written and derived when read. So a change to the node bytes, which
    /// needs a version bump, fails here.
    #[test]
    fn path_histories_match_golden_ids() {
        let history: [Edits<'_>; 4] = [
            &[("a/b/c", true), ("a/d", true), ("e", true)],
            &[("a/b/c", false), ("a/b", true), ("f/g/h", true)],
            &[],
            &[("e", false), ("a/b", false), ("a/b/c", true)],
        ];
        // The empty changeset keeps its parent's tree.
        let golden = [
            "275f0ac4048df304db51095dc775de19d520a5c4ed585bd6550e8724904c0b62",
            "d6cf99672ed34457a9c8f2954e970f85458389f07ad3e886c00940096308cc08",
            "d6cf99672ed34457a9c8f2954e970f85458389f07ad3e886c00940096308cc08",
            "791e35308cf1c2618cd46241bbbecc9f0e99fedfacd088b9fd62e6c5cb01b5e6",
        ];
        for stored in [true, false] {
            let (_dir, store, repo) = open();
            let ids = store_chain(&store, repo, None, &history, stored);
            let roots: Vec<String> = ids
                .iter()
                .map(|&cs| {
                    derived::get::<PathHistory>(&store, repo, cs)
                        .unwrap()
                        .to_string()
                })
                .collect();
            assert_eq!(roots, golden, "stored when written: {stored}");
        }
    }

    #[test]
    fn a_node_decodes_only_from_its_own_bytes() {
        let (_dir, store, repo) = open();
        let history: [Edits<'_>; 2] = [
            &[("a/b", true), ("c", true)],
            &[("a/b", false), ("a", true)],
        ];
        let ids = store_chain(&store, repo, None, &history, true);
        let root = store
            .read(|c| derived::load::<PathHistory>(c, ids[1]))
            .unwrap()
            .unwrap();
        let (node, _) = store.read(|c| get_node(c, root)).unwrap();
        let bytes = encode_node(&node);
        assert_eq!(decode_node(&bytes), Some(node.clone()));
        let a = node[&PathElem::new("a").unwrap()];
        assert_eq!(a.last.id, ids[1]);
        assert_eq!((a.own, a.below.is_some()), (Some(2), true));
        // An entry with no flag, two entries out of order, and a cut entry.
        let mut no_flag = bytes.clone();
        no_flag[3] = 0;
        assert_eq!(decode_node(&no_flag), None);
        let cut = encode_node(&node.clone().into_iter().take(1).collect());
        let mut swapped = bytes[cut.len()..].to_vec();
        swapped.extend(&cut);
        assert_eq!(decode_node(&swapped), None);
        assert_eq!(decode_node(&bytes[..bytes.len() - 1]), None);
    }
}
