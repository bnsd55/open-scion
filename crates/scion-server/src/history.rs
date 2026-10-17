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
use std::ops::Bound::{self, Unbounded};

use scion_core::{Changeset, ChangesetId, FileState, PathElem, RepoPath};

use crate::deadline::Deadline;
use crate::derived::{self, Kind};
use crate::error::{Error, Result};
use crate::meta::MetaTx;
use crate::tree;

/// Hashed before a node's bytes, so a node ID is never the ID of another
/// kind of data.
const DOMAIN: &[u8] = b"scion\0history\0v2\0";
/// First byte of a stored node: its entries.
const LEAF: u8 = 0;
/// First byte of a stored node: an index of chunks of one level, each
/// `first name` to the chunk that holds it.
const INDEX: u8 = 1;
/// Entry flag: `own` follows.
const OWN: u8 = 1;
/// Entry flag: `below` follows.
const BELOW: u8 = 2;

/// How wide one stored node row may be, and how often a name cuts. A
/// level above `max_bytes` is cut into chunks: a name ends its chunk when
/// a hash of that name alone hits 1 in `1 << bits`, so the mean chunk
/// holds `1 << bits` entries, about 8 KiB of rows at a typical entry of
/// ~90 bytes, and a chunk that reaches `max_bytes` is cut anyway. A cut
/// hangs on one name, so the same names cut the same way whatever the
/// edit order.
#[derive(Clone, Copy)]
struct Cut {
    max_bytes: usize,
    bits: u32,
}

/// The limits of production.
const PROD_CUT: Cut = Cut {
    max_bytes: 64 << 10,
    bits: 7,
};

/// The bounds in use. Tests build their own `Cut` to reach deep stacks
/// and rare cuts with small trees.
const CUT: Cut = Cut {
    max_bytes: if cfg!(test) { 1024 } else { PROD_CUT.max_bytes },
    bits: PROD_CUT.bits,
};

#[cfg(test)]
thread_local! {
    /// Nodes read on this thread. Tests use it to bound the reads of a
    /// query.
    pub(crate) static NODE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Bytes stored in `history_nodes` on this thread. Tests use it to
    /// bound the cost of a change.
    pub(crate) static NODE_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The ID of a history node: the hash of its bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub(crate) [u8; 32]);

impl NodeId {
    /// The ID of `body`: the hash of its bytes.
    pub(crate) fn of(body: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(DOMAIN);
        hasher.update(body);
        Self(*hasher.finalize().as_bytes())
    }
}

impl NodeId {
    /// The bytes of the ID.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Whether `body` decodes as a history node.
pub(crate) fn node_decodes(body: &[u8]) -> bool {
    let Some((kind, payload)) = body.split_first() else {
        return false;
    };
    match *kind {
        LEAF => decode_node(payload).is_some(),
        INDEX => decode_index(payload).is_some(),
        _ => false,
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
    const VERSION: u32 = 2;
    type Value = NodeId;

    fn derive(
        c: &mut dyn MetaTx,
        id: ChangesetId,
        changeset: &Changeset,
        parents: Vec<NodeId>,
    ) -> Result<NodeId> {
        let by = Stamp {
            id,
            generation: c.generation_of(id)?,
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
    c: &mut dyn MetaTx,
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
    root.store(c, &CUT)
}

/// A node being edited. A small level is one `leaf`; a large one is an
/// `index` of chunks, of which only the ones on changed paths are loaded
/// into `chunks`. `below` holds the edited levels below it.
struct Edit {
    leaf: Option<Node>,
    index: Option<BTreeMap<PathElem, NodeId>>,
    chunks: BTreeMap<PathElem, Node>,
    below: BTreeMap<PathElem, Edit>,
}

impl Edit {
    fn load(c: &mut dyn MetaTx, id: Option<NodeId>) -> Result<Self> {
        Ok(match id {
            None => Self {
                leaf: Some(Node::new()),
                index: None,
                chunks: BTreeMap::new(),
                below: BTreeMap::new(),
            },
            Some(id) => Self::stored(c, id)?,
        })
    }

    /// Loads the level at `id`: a leaf, or its whole chunk index,
    /// flattened to the leaf chunks it names.
    // SHORTCUT: the edit holds the whole index of a chunked level, so a
    // write in a directory of 10M names re-encodes an index of about
    // 2.7 MB. Upgrade: keep the index stacked in the edit, and edit one
    // index chunk per level of the stack.
    fn stored(c: &mut dyn MetaTx, id: NodeId) -> Result<Self> {
        match get_row(c, id)?.0 {
            Row::Leaf(node) => Ok(Self {
                leaf: Some(node),
                index: None,
                chunks: BTreeMap::new(),
                below: BTreeMap::new(),
            }),
            Row::Chunks(index) => Ok(Self::chunked(flatten(c, index)?)),
        }
    }

    /// A chunked level whose index rows are flattened to the leaf chunks
    /// they name.
    fn chunked(index: BTreeMap<PathElem, NodeId>) -> Self {
        Self {
            leaf: None,
            index: Some(index),
            chunks: BTreeMap::new(),
            below: BTreeMap::new(),
        }
    }

    /// The entries that hold `name`: the whole level, or the chunk of a
    fn entries(&mut self, c: &mut dyn MetaTx, name: &PathElem) -> Result<&mut Node> {
        let Some(index) = &self.index else {
            return Ok(self.leaf.as_mut().expect("a level is leaf or chunked"));
        };
        if index.is_empty() {
            // An empty level: everything below it is new.
            self.leaf = Some(Node::new());
            self.index = None;
            return Ok(self.leaf.as_mut().expect("just set"));
        }
        // The chunk whose range holds the name, or the first one.
        let first = index
            .range((Unbounded, Bound::Included(name.to_owned())))
            .next_back()
            .or_else(|| index.iter().next())
            .map(|(first, _)| first.clone())
            .expect("an index is not empty");
        if !self.chunks.contains_key(&first) {
            let node = leaf_of(c, index[&first])?;
            self.chunks.insert(first.clone(), node);
        }
        Ok(self.chunks.get_mut(&first).expect("loaded above"))
    }

    /// Stamps the entry at `parts` with `by`, and each entry above it.
    fn stamp(&mut self, c: &mut dyn MetaTx, parts: &[PathElem], by: Stamp) -> Result<()> {
        let (name, rest) = parts.split_first().expect("a path has at least one part");
        let entries = self.entries(c, name)?;
        let entry = entries.entry(name.clone()).or_insert(Entry {
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

    /// Stores every edited level, bottom up, and returns this one's ID. A
    /// chunked level stores each chunk it edited and rebuilds its index;
    /// rows whose bytes did not change keep their IDs.
    fn store(mut self, c: &mut dyn MetaTx, cut: &Cut) -> Result<NodeId> {
        for (name, edit) in std::mem::take(&mut self.below) {
            let below = edit.store(c, cut)?;
            let entries = self.entries_now(&name)?;
            let entry = entries.get_mut(&name).expect("an edited node has an entry");
            entry.below = Some(below);
        }
        if let Some(mut index) = self.index.take() {
            for (first, node) in std::mem::take(&mut self.chunks) {
                index.remove(&first);
                index.extend(put_level(c, &node, cut)?);
            }
            return level_root(c, index, cut);
        }
        let leaf = self.leaf.expect("a level is leaf or chunked");
        let index = put_level(c, &leaf, cut)?;
        level_root(c, index, cut)
    }

    /// The entries that `store` edits, without reading: they are already
    /// loaded.
    fn entries_now(&mut self, name: &PathElem) -> Result<&mut Node> {
        if let Some(index) = &self.index {
            let first = index
                .range((Unbounded, Bound::Included(name.to_owned())))
                .next_back()
                .or_else(|| index.iter().next())
                .map(|(first, _)| first.clone())
                .ok_or_else(|| Error::Corrupt("history: no edited chunk".into()))?;
            return match self.chunks.get_mut(&first) {
                Some(entries) => Ok(entries),
                None => Err(Error::Corrupt("history: no edited chunk".into())),
            };
        }
        self.leaf
            .as_mut()
            .ok_or_else(|| Error::Corrupt("history: no level".into()))
    }
}

/// The pairs of the leaf chunks that the index rows at `top` name, down
/// the stack of index levels.
fn flatten(
    c: &mut dyn MetaTx,
    top: BTreeMap<PathElem, NodeId>,
) -> Result<BTreeMap<PathElem, NodeId>> {
    let mut flat = BTreeMap::new();
    let mut work: Vec<(PathElem, NodeId)> = top.into_iter().collect();
    while let Some((first, id)) = work.pop() {
        match get_row(c, id)?.0 {
            Row::Leaf(_) => {
                flat.insert(first, id);
            }
            Row::Chunks(sub) => work.extend(sub),
        }
    }
    Ok(flat)
}

/// Reads the leaf chunk that the edit's index names. The edit holds
/// flattened IDs, which are always leaves, so a row that is an index is
/// corrupt data.
fn leaf_of(c: &mut dyn MetaTx, id: NodeId) -> Result<Node> {
    match get_row(c, id)?.0 {
        Row::Leaf(node) => Ok(node),
        Row::Chunks(_) => Err(Error::Corrupt(format!(
            "history chunk {id} is an index where a leaf is expected"
        ))),
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

/// The rows that one call has read, so that it reads each one once, and
/// how many bytes they hold.
#[derive(Default)]
pub(crate) struct Nodes {
    rows: HashMap<NodeId, Row>,
    bytes: usize,
}

impl Nodes {
    /// The stored bytes of the rows read so far.
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    fn row(&mut self, c: &mut dyn MetaTx, id: NodeId) -> Result<&Row> {
        Ok(match self.rows.entry(id) {
            hash_map::Entry::Occupied(row) => row.into_mut(),
            hash_map::Entry::Vacant(slot) => {
                let (row, bytes) = get_row(c, id)?;
                self.bytes += bytes;
                slot.insert(row)
            }
        })
    }

    /// The entry that the level at `id` holds for `name`: down its stacked
    /// indexes, in the chunk whose range holds the name. Reads each row
    /// once.
    fn entry(&mut self, c: &mut dyn MetaTx, id: NodeId, name: &PathElem) -> Result<Option<Entry>> {
        let mut id = id;
        loop {
            let next = match self.row(c, id)? {
                Row::Leaf(node) => return Ok(node.get(name).copied()),
                Row::Chunks(index) => {
                    // The chunk whose range holds the name, or the first
                    // one.
                    index
                        .range((Unbounded, Bound::Included(name.to_owned())))
                        .next_back()
                        .or_else(|| index.iter().next())
                        .map(|(_, id)| *id)
                }
            };
            let Some(next) = next else {
                return Ok(None);
            };
            id = next;
        }
    }
}

/// What the tree at `root` holds for `path`. Reads at most one node per
/// part of `path`, and none that `nodes` holds.
fn lookup(c: &mut dyn MetaTx, nodes: &mut Nodes, root: NodeId, path: &RepoPath) -> Result<Hit> {
    let parts = tree::parts(path);
    let (name, dirs) = parts.split_last().expect("a path has at least one part");
    let mut hit = Hit::default();
    let mut node = root;
    for dir in dirs {
        let Some(entry) = nodes.entry(c, node, dir)? else {
            return Ok(hit);
        };
        hit.above = hit.above.max(entry.own.unwrap_or(0));
        let Some(below) = entry.below else {
            return Ok(hit);
        };
        node = below;
    }
    hit.last = nodes.entry(c, node, name)?.map(|entry| entry.last);
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
    c: &mut dyn MetaTx,
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
    c: &mut dyn MetaTx,
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

/// Stores the entries of one level, cut into chunks when they pass
/// `cut.max_bytes`, and returns the index of its chunks: the first name
/// of each chunk and the chunk's ID. A small level is one leaf, so one
/// pair.
fn put_level(c: &mut dyn MetaTx, node: &Node, cut: &Cut) -> Result<BTreeMap<PathElem, NodeId>> {
    let body = encode_node(node);
    if node.is_empty() {
        return Ok(BTreeMap::new());
    }
    if body.len() <= cut.max_bytes {
        let id = put_row(c, LEAF, &body)?;
        let first = node.first_key_value().expect("a level is not empty").0;
        return Ok(BTreeMap::from([(first.clone(), id)]));
    }
    let mut index = BTreeMap::new();
    for chunk in split(node.iter(), cut) {
        let node: Node = chunk.into_iter().collect();
        let id = put_row(c, LEAF, &encode_node(&node))?;
        let first = node.first_key_value().expect("a chunk is not empty").0;
        index.insert(first.clone(), id);
    }
    Ok(index)
}

/// Builds the index of a level's chunks, itself cut into chunks when it
/// passes `cut.max_bytes`, and returns the ID of its top node.
fn index_node(c: &mut dyn MetaTx, index: &BTreeMap<PathElem, NodeId>, cut: &Cut) -> Result<NodeId> {
    let body = encode_index(index);
    if body.len() <= cut.max_bytes {
        return put_row(c, INDEX, &body);
    }
    let mut upper = BTreeMap::new();
    for chunk in split(index.iter(), cut) {
        let node: BTreeMap<PathElem, NodeId> = chunk.into_iter().collect();
        let id = put_row(c, INDEX, &encode_index(&node))?;
        let first = node.first_key_value().expect("a chunk is not empty").0;
        upper.insert(first.clone(), id);
    }
    level_root(c, upper, cut)
}

/// The ID of a level whose chunks `index` names: the one leaf, or the top
/// index row.
fn level_root(c: &mut dyn MetaTx, index: BTreeMap<PathElem, NodeId>, cut: &Cut) -> Result<NodeId> {
    if index.len() == 1 {
        return Ok(index.into_values().next().expect("one pair"));
    }
    index_node(c, &index, cut)
}

/// Stores one row of kind `kind`. Safe to repeat. Counts the bytes that
/// actually landed in the table, so that a test can bound the cost of a
/// change: a row whose bytes are already there is stored again for free.
fn put_row(c: &mut dyn MetaTx, kind: u8, payload: &[u8]) -> Result<NodeId> {
    let mut body = Vec::with_capacity(payload.len() + 1);
    body.push(kind);
    body.extend(payload);
    let id = NodeId::of(&body);
    #[cfg(test)]
    let len = body.len();
    #[cfg_attr(not(test), allow(unused_variables))]
    let (_, stored) = c.put_history_node(id, body)?;
    #[cfg(test)]
    if stored {
        NODE_BYTES.with(|n| n.set(n.get() + len));
    }
    Ok(id)
}

/// Reads a row and checks it against its ID. Also returns the length of its
/// stored bytes.
fn get_row(c: &mut dyn MetaTx, id: NodeId) -> Result<(Row, usize)> {
    #[cfg(test)]
    NODE_READS.set(NODE_READS.get() + 1);
    let (body, len) = c.get_history_node(id)?;
    let row = decode_row(&body)
        .ok_or_else(|| Error::Corrupt(format!("history node {id} does not decode")))?;
    Ok((row, len))
}

/// One stored row of a level: a leaf of entries, or an index of the level's
/// chunks.
#[derive(Clone, Debug)]
enum Row {
    Leaf(Node),
    Chunks(BTreeMap<PathElem, NodeId>),
}

/// The bytes of a leaf row: its entries in name order, each
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

/// The bytes of an index row: its chunk entries in name order, each
/// `u16le(len(name)) || name || chunk ID`.
fn encode_index(index: &BTreeMap<PathElem, NodeId>) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, id) in index {
        let name = name.as_bytes();
        let len = u16::try_from(name.len()).expect("a name is at most 4,096 bytes");
        out.extend(len.to_le_bytes());
        out.extend(name);
        out.extend(id.0);
    }
    out
}

/// The index in `bytes`, or `None` if they are not the bytes of one.
fn decode_index(mut bytes: &[u8]) -> Option<BTreeMap<PathElem, NodeId>> {
    let mut index = BTreeMap::new();
    while !bytes.is_empty() {
        let len = u16::from_le_bytes(take(&mut bytes, 2)?.try_into().ok()?);
        let name = PathElem::new(take(&mut bytes, usize::from(len))?).ok()?;
        let id = NodeId(take(&mut bytes, 32)?.try_into().ok()?);
        if index
            .last_key_value()
            .is_some_and(|(prev, _)| *prev >= name)
        {
            return None;
        }
        index.insert(name, id);
    }
    Some(index)
}

/// The row in `body`, whose first byte is its kind.
fn decode_row(body: &[u8]) -> Option<Row> {
    match body.first() {
        Some(&LEAF) => decode_node(&body[1..]).map(Row::Leaf),
        Some(&INDEX) => decode_index(&body[1..]).map(Row::Chunks),
        _ => None,
    }
}

/// The gear hash: one value per byte, from a fixed pseudorandom sequence,
/// so cuts are the same on every machine.
fn gear(byte: u8) -> u64 {
    let mut x = (u64::from(byte)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x
}

/// Splits a sorted list into chunks. A name ends its chunk when a hash of
/// that name alone hits 1 in `1 << cut.bits`, so a cut hangs on one name:
/// neighbours never move it, and the same names cut the same way whatever
/// the edit order. A chunk that reaches `cut.max_bytes` is cut anyway,
/// the rare backstop for names far larger than the mean. Names are
/// unique, so chunks stay sorted and never empty.
fn split<'a, T: Clone + 'a>(
    items: impl Iterator<Item = (&'a PathElem, &'a T)>,
    cut: &Cut,
) -> Vec<Vec<(PathElem, T)>> {
    let mask = (1u64 << cut.bits) - 1;
    let mut chunks: Vec<Vec<(PathElem, T)>> = Vec::new();
    let mut chunk: Vec<(PathElem, T)> = Vec::new();
    let mut bytes = 0usize;
    for (name, item) in items {
        chunk.push((name.clone(), item.clone()));
        bytes += name.as_bytes().len() + 2 + 83;
        let mut hash = 0u64;
        for byte in name.as_bytes() {
            hash = hash.rotate_left(1).wrapping_add(gear(*byte));
        }
        if (bytes >= cut.max_bytes || hash & mask == 0) && !chunk.is_empty() {
            chunks.push(std::mem::take(&mut chunk));
            bytes = 0;
        }
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

/// The first `n` bytes of `bytes`, which then start after them.
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    let (head, rest) = bytes.split_at_checked(n)?;
    *bytes = rest;
    Some(head)
}

#[cfg(test)]
mod tests {
    use crate::db;
    use scion_core::{BlobId, FileState, IdentityTime};

    use super::*;
    use crate::db::testing;
    use crate::meta::RepoId;
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
        let repo = store.write(|c| c.create_repo("r")).unwrap();
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
                        c.insert_changeset(repo, &cs)?
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
                        let cs = store.read(|c| c.get_changeset(repo, id)).unwrap();
                        if read::changes_path(&cs, &path(p)) {
                            walked.push(id);
                        }
                        next = cs.parents.first().copied();
                    }
                    proptest::prop_assert_eq!(path_log(&store, tip, p, limit), walked, "{}", p);
                }
            }
        }

        /// One level built from the same names through two edit orders
        /// gets the same top node: a cut hangs on one name, so the order
        /// of the edits cannot move a boundary. The limits keep the byte
        /// cap out of reach, so only name hashes cut.
        #[test]
        fn the_same_names_cut_the_same_way_in_any_edit_order(
            names in proptest::collection::vec("[a-z]{1,6}", 200..300),
            seed in proptest::num::u64::ANY,
        ) {
            let (_dir, store, _repo) = open();
            let mut names: Vec<String> = names;
            names.sort();
            names.dedup();
            let mut shuffled = names.clone();
            let mut state = seed;
            for i in (1..shuffled.len()).rev() {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let j = (state >> 33) as usize % (i + 1);
                shuffled.swap(i, j);
            }
            // The byte cap stays out of reach of any chunk of a few
            // names, so a cut is a name hash or nothing, while the level
            // of 200 names is over the cap and really is chunked.
            let cut = Cut { max_bytes: 8192, bits: 2 };
            let level = |order: &[String], c: &mut dyn MetaTx| -> Result<NodeId> {
                let mut edit = Edit::load(c, None)?;
                let by = Stamp {
                    id: ChangesetId::from_bytes([7; 32]),
                    generation: 1,
                };
                for name in order {
                    edit.stamp(c, &tree::parts(&path(name)), by)?;
                }
                edit.store(c, &cut)
            };
            let (root, again) = store
                .write(|c| Ok((level(&names, c)?, level(&shuffled, c)?)))
                .unwrap();
            proptest::prop_assert_eq!(root, again);
            // The level really is chunked, so the test means something.
            let row = store.read(|c| Ok(get_row(c, root).unwrap().0)).unwrap();
            assert!(matches!(row, Row::Chunks(_)), "the level is chunked");
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
        // `d` holds 10,000 files, so its level is cut into chunks and its
        // lookup reads an index and a chunk. Then three changesets change
        // `d/f0`, and the first changeset made the directory.
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
        // The lookups of the three changesets share most of their rows, so
        // one page of 10 holds all four entries however wide the directory
        // is. One entry of the old whole-node format cost the whole level
        // of 10,000 names, about 630 KB, over the bound.
        let (found, next) = page_of(ids[3]);
        assert_eq!(found, [ids[3], ids[2], ids[1], ids[0]]);
        assert_eq!(next, None);
        // A file in a small directory fills its page.
        let small = store_chain(&store, repo, Some(ids[3]), &[&[("e", true)][..]; 3], true);
        let page = read::log_page(&store, "r", small[2], Some(&path("e")), 10, &later());
        assert_eq!(page.unwrap().entries.len(), 3);
    }

    #[test]
    fn a_change_in_a_wide_directory_stores_chunks_not_the_level() {
        let (_dir, store, repo) = open();
        // `d` holds 100,000 files. Then one changeset changes `d/f50000`.
        // The nodes it stores are the chunks the change touches, not the
        // 6.3 MB level of every name (https://github.com/bnsd55/open-scion/
        // issues/114).
        let wide: Vec<String> = (0..100_000).map(|i| format!("d/f{i}")).collect();
        let first: Vec<(&str, bool)> = wide.iter().map(|p| (p.as_str(), true)).collect();
        let ids = store_chain(
            &store,
            repo,
            None,
            &[&first[..], &[("d/f50000", true)]],
            true,
        );
        NODE_BYTES.with(|n| n.set(0));
        let _one = store_chain(
            &store,
            repo,
            Some(ids[1]),
            &[&[("d/f50001", true)][..]],
            true,
        );
        let written = NODE_BYTES.get();
        // Two chunks and one index node at most, each under the cap.
        assert!(written <= 3 * CUT.max_bytes, "{written}");
        // The lookup still answers.
        assert_eq!(
            read::log_page(&store, "r", ids[1], Some(&path("d/f50000")), 10, &later())
                .unwrap()
                .entries
                .len(),
            2
        );
    }

    /// A name far wider than the mean chunk still splits: the byte cap
    /// cuts when no name hash hits, and every chunk stays within it.
    #[test]
    fn a_name_wider_than_the_mean_chunk_still_cuts() {
        let (_dir, store, _repo) = open();
        // Twelve names of about 300 bytes, about 385 bytes of entries
        // each. No name hash hits 1 in a million, so every cut is the
        // cap.
        let cut = Cut {
            max_bytes: 1024,
            bits: 20,
        };
        let node: Node = (0..12)
            .map(|i| {
                let name = format!("n{i:02}{}", "x".repeat(300));
                (
                    PathElem::new(name).unwrap(),
                    Entry {
                        last: Stamp {
                            id: ChangesetId::from_bytes([7; 32]),
                            generation: 1,
                        },
                        own: Some(1),
                        below: None,
                    },
                )
            })
            .collect();
        let chunks = split(node.iter(), &cut);
        assert!(chunks.len() >= 4, "{}", chunks.len());
        let mut names: Vec<&[u8]> = Vec::new();
        for chunk in &chunks {
            assert_ne!(chunk.len(), 0);
            let bytes: usize = chunk
                .iter()
                .map(|(name, _)| name.as_bytes().len() + 2 + 83)
                .sum();
            // The cap cuts after the entry that crosses it, so a chunk
            // holds at most one entry over the cap.
            assert!(bytes < cut.max_bytes + 385, "{bytes}");
            names.extend(chunk.iter().map(|(name, _)| name.as_bytes()));
        }
        // The chunks cover every name once, in order.
        assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(names.len(), node.len());
        // A stored level round-trips through its index and its chunks.
        store
            .write(|c| {
                let index = put_level(c, &node, &cut)?;
                assert!(index.len() >= 4, "{}", index.len());
                for (first, id) in &index {
                    let node = leaf_of(c, *id)?;
                    assert!(node.contains_key(first));
                    assert!(!decode_node(&encode_node(&node)).unwrap().is_empty());
                }
                Ok(())
            })
            .unwrap();
    }

    /// Inserting a name into a wide level re-stores only the chunk that
    /// holds it: a cut hangs on one name, so the boundaries of the later
    /// chunks hold. A cut at a byte count would move the boundary after
    /// the insert, and every later chunk would change.
    #[test]
    fn an_inserted_name_moves_no_later_boundary() {
        let (_dir, store, _repo) = open();
        store
            .write(|c| {
                let cut = Cut {
                    max_bytes: 8192,
                    bits: 2,
                };
                let level = |names: &[String], c: &mut dyn MetaTx| -> Result<NodeId> {
                    let mut edit = Edit::load(c, None)?;
                    let by = Stamp {
                        id: ChangesetId::from_bytes([7; 32]),
                        generation: 1,
                    };
                    for name in names {
                        edit.stamp(c, &tree::parts(&path(name)), by)?;
                    }
                    edit.store(c, &cut)
                };
                let mut names: Vec<String> = (0..200).map(|i| format!("n{i:03}")).collect();
                let before = level(&names, c)?;
                names.insert(100, "n0995".into());
                let after = level(&names, c)?;
                // The chunks that the two levels name differ only around
                // the insert.
                let index =
                    |root: NodeId, c: &mut dyn MetaTx| -> Result<BTreeMap<PathElem, NodeId>> {
                        match get_row(c, root)?.0 {
                            Row::Chunks(index) => flatten(c, index),
                            Row::Leaf(_) => Err(Error::Corrupt("the level is chunked".into())),
                        }
                    };
                let before = index(before, c)?;
                let after = index(after, c)?;
                let changed = before
                    .iter()
                    .filter(|(first, id)| after.get(*first).is_some_and(|next| next != *id))
                    .count()
                    // A chunk whose first name moved counts as one gone
                    // and one new.
                    + before.iter().filter(|(first, _)| !after.contains_key(*first)).count()
                    + after.iter().filter(|(first, _)| !before.contains_key(*first)).count();

                assert!(changed <= 2, "{changed} chunks changed");
                Ok(())
            })
            .unwrap();
    }

    /// One name change in a directory of 100,000 names, at the production
    /// limits, stores the changed chunk and the index rows, not the
    /// 6.3 MB level of every name. Measured with the production `Cut`,
    /// the change stores 47,672 bytes: the changed chunk, about 8 KiB,
    /// and the index of the level's 781 chunks, about 33 KiB. The
    /// index is the whole cost of a change and the ceiling of the
    /// `stored` shortcut; keeping it stacked in the edit is the upgrade
    /// of issue 114.
    #[test]
    fn a_change_measured_with_production_limits_stores_its_chunk_and_index() {
        let (_dir, store, _repo) = open();
        store
            .write(|c| {
                let mut edit = Edit::load(c, None)?;
                let by = Stamp {
                    id: ChangesetId::from_bytes([7; 32]),
                    generation: 1,
                };
                for i in 0..100_000 {
                    edit.stamp(c, &tree::parts(&path(&format!("d/f{i}"))), by)?;
                }
                let root = edit.store(c, &PROD_CUT)?;
                NODE_BYTES.with(|n| n.set(0));
                let mut edit = Edit::load(c, Some(root))?;
                let by = Stamp {
                    id: ChangesetId::from_bytes([9; 32]),
                    generation: 2,
                };
                edit.stamp(c, &tree::parts(&path("d/f50001")), by)?;
                edit.store(c, &PROD_CUT)?;
                let written = NODE_BYTES.get();
                // The changed chunk and the index, each under the cap.
                assert!(written <= 2 * PROD_CUT.max_bytes, "{written}");
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn a_draft_changeset_has_a_path_history() {
        let (_dir, store, repo) = open();
        let main = store_chain(&store, repo, None, &[&[("a", true)], &[("b", true)]], true);
        let blob = store.blobs().put(b"draft").unwrap();
        // PutBlob makes the blob a member of the repo; the test mirrors it.
        store.write(|c| c.add_repo_blob(repo, blob)).unwrap();
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
            "5208e7e609f94b249aca6f04fdbcfa6319ccd98e1d732e7639e52b95f3fb7a7d",
            "9fd2ee8a02e3ce4c75fcf0a634c971065117508885573ae6da6fa523e0438843",
            "9fd2ee8a02e3ce4c75fcf0a634c971065117508885573ae6da6fa523e0438843",
            "01a64c51815c59ccaae7f819ec25cb4939b06d4e8f30bbe1a69f2a628e669b14",
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
        let root_row = store.read(|c| get_row(c, root)).unwrap().0;
        let Row::Leaf(node) = root_row else {
            panic!("a small level is one leaf");
        };
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
