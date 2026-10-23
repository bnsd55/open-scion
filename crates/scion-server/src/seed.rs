//! The test seed for a deep, file-less ladder of changesets: one batched
//! write instead of one statement per row, so a test that measures the
//! queries does not measure the seeding.

#![forbid(unsafe_code)]

use std::collections::HashMap;

use scion_core::Changeset;

use crate::derived::{self, Kind as _};
use crate::error::{Error, Result};
use crate::graph::CommitGraph;
use crate::meta::MetaTx;
use crate::tree;

/// Seeds `css` — oldest first, every parent before its children, file-less
/// — as the ladder shape: the first changeset goes through the ordinary
/// write path (it creates the empty tree and its own derived values),
/// and the rest go in through [`MetaTx::bulk_seed`], with their derived
/// values computed in memory from their parents' nodes: the same math
/// `CommitGraph::derive` runs, without a round trip per node. A seed
/// writes file-less changesets only: any tree entry would need the tree
/// rows the bulk path does not build.
///
/// # Errors
///
/// What the database returns, or [`Error::Corrupt`] if a changeset of
/// `css` carries tree entries.
pub fn seed_file_less_ladder(
    c: &mut dyn MetaTx,
    repo: crate::meta::RepoId,
    css: &[Changeset],
) -> Result<Vec<scion_core::ChangesetId>> {
    if let Some(cs) = css.iter().find(|cs| !cs.changes().is_empty()) {
        return Err(Error::Corrupt(format!(
            "the bulk seed writes file-less changesets; {} carries {} entries",
            cs.encode_with_id().1,
            cs.changes().len()
        )));
    }
    let root = tree::store_changeset(c, repo, &css[0], &mut |_| Ok(0))?.0;
    let root_graph = derived::load::<CommitGraph>(c, root)?
        .ok_or_else(|| Error::Corrupt(format!("commit graph: no node for {root}")))?;
    let root_tree = c
        .get_derived("root_tree", 1, root)?
        .ok_or_else(|| Error::Corrupt(format!("root tree: no node for {root}")))?;
    let root_history = c
        .get_derived("path_history", 2, root)?
        .ok_or_else(|| Error::Corrupt(format!("path history: no node for {root}")))?;

    // The nodes, in order: a child needs only its parents' nodes.
    let mut nodes: HashMap<scion_core::ChangesetId, crate::graph::Node> = HashMap::new();
    nodes.insert(root, root_graph);
    let mut derived_rows: Vec<(&'static str, u32, scion_core::ChangesetId, Vec<u8>)> = Vec::new();
    for cs in &css[1..] {
        let id = cs.encode_with_id().1;
        let node = node_of(id, cs, &nodes)?;
        nodes.insert(id, node);
    }
    for cs in &css[1..] {
        let id = cs.encode_with_id().1;
        let node = nodes[&id];
        derived_rows.push((
            "commit_graph",
            CommitGraph::VERSION,
            id,
            CommitGraph::encode(&node),
        ));
        // A file-less changeset applies to its parent's tree and changes
        // no path, so both values are the root's, unchanged.
        derived_rows.push(("root_tree", 1, id, root_tree.clone()));
        derived_rows.push(("path_history", 2, id, root_history.clone()));
    }
    c.bulk_seed(repo, css, &derived_rows)
}

/// The node `CommitGraph::derive` would compute for `cs`, from its
/// parents' nodes alone.
fn node_of(
    id: scion_core::ChangesetId,
    cs: &Changeset,
    nodes: &HashMap<scion_core::ChangesetId, crate::graph::Node>,
) -> Result<crate::graph::Node> {
    use crate::graph::{Link, Node};
    let mut generation = 0;
    for parent in cs.parents() {
        let node = nodes
            .get(parent)
            .ok_or_else(|| Error::Corrupt(format!("commit graph: no node for {parent}")))?;
        generation = generation.max(node.generation);
    }
    let generation = 1 + generation;
    let (Some(_first), Some(&first_id)) = (cs.parents().first(), cs.parents().first()) else {
        let root = Link {
            id,
            generation,
            depth: 0,
        };
        return Ok(Node {
            generation,
            depth: 0,
            parent: root,
            jump: root,
            merge: None,
        });
    };
    let first_node = nodes
        .get(&first_id)
        .ok_or_else(|| Error::Corrupt(format!("commit graph: no node for {first_id}")))?;
    let depth = first_node.depth + 1;
    let parent = Link {
        id: first_id,
        generation: first_node.generation,
        depth: first_node.depth,
    };
    // Myers' rule: if the parent's jump and the jump after it span the
    // same depth, jump over both. Else jump to the parent.
    let after = if first_node.depth == 0 {
        first_node.jump
    } else {
        nodes
            .get(&first_node.jump.id)
            .ok_or_else(|| {
                Error::Corrupt(format!("commit graph: no node for {}", first_node.jump.id))
            })?
            .jump
    };
    let jump = if first_node.depth - first_node.jump.depth == first_node.jump.depth - after.depth {
        after
    } else {
        parent
    };
    let merge = if cs.parents().len() > 1 {
        Some(Link {
            id,
            generation,
            depth,
        })
    } else {
        first_node.merge
    };
    Ok(Node {
        generation,
        depth,
        parent,
        jump,
        merge,
    })
}
