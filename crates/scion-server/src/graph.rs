//! The commit-graph index, and the graph queries that read it.
//!
//! Kind `commit_graph` gives each changeset a [`Node`]: its generation, its
//! first-parent depth, its first parent, a jump to an older changeset on its
//! first-parent history, and the newest merge on that history. The article
//! says that Scion keeps skip lists for the commit graph as derived data.
//! The form of the skip list here is this project's choice: the skew-binary
//! jump pointers of Myers ("An applicative random-access stack", 1983). Each
//! node holds one jump, and the depth that a jump lands on depends only on
//! the depth it starts from. So a walk down a first-parent history to a
//! generation, or to where two such histories meet, reads O(log n) nodes.
//!
//! The kind reads every parent ([`Kind::ALL_PARENTS`]), so each ancestor of
//! a changeset with a node has a node, and the queries can follow any
//! parent:
//!
//! - [`is_ancestor`] searches the first-parent history of the descendant.
//!   Then it searches from the other parents of each merge on that history
//!   that is newer than the ancestor, and so on. It reads O(log n) nodes per
//!   history it searches, and three rows per merge. It never reads a
//!   changeset between two merges.
//! - [`merge_bases`] answers from where the two first-parent histories meet,
//!   when neither has a merge newer than that point. Else it walks the graph
//!   ([`db::merge_bases`]).

use std::collections::HashSet;

use rusqlite::Connection;
use scion_core::{Changeset, ChangesetId};

use crate::db::{self, RepoId};
use crate::derived::{self, Kind};
use crate::error::{Error, Result};
use crate::store::Store;

/// A changeset that a [`Node`] points to, with what a query needs to decide
/// whether to go there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Link {
    /// The changeset.
    pub id: ChangesetId,
    /// Its generation.
    pub generation: i64,
    /// Its first-parent depth.
    pub depth: i64,
}

/// The value of [`CommitGraph`] for one changeset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Node {
    /// 1 for a root, else 1 + the highest generation of its parents.
    pub generation: i64,
    /// How many first parents lead from it to its root: 0 for a root.
    pub depth: i64,
    /// Its first parent. A root points to itself.
    pub parent: Link,
    /// An older changeset on its first-parent history: the skip list's
    /// pointer. A root points to itself.
    pub jump: Link,
    /// The newest changeset on its first-parent history, itself included,
    /// with two or more parents.
    pub merge: Option<Link>,
}

/// The commit-graph index: a [`Node`] for each changeset. A write stores the
/// node of a changeset whose parents have nodes (`tree::store_changeset`).
/// Else the first query that needs it derives it, in steps.
pub struct CommitGraph;

/// Bytes of a stored [`Link`]: ID, generation, depth.
const LINK_BYTES: usize = 32 + 8 + 8;

impl Kind for CommitGraph {
    const NAME: &'static str = "commit_graph";
    const VERSION: u32 = 1;
    const ALL_PARENTS: bool = true;
    type Value = Node;

    fn derive(
        c: &Connection,
        id: ChangesetId,
        changeset: &Changeset,
        parents: Vec<Node>,
    ) -> Result<Node> {
        let generation = 1 + parents.iter().map(|p| p.generation).max().unwrap_or(0);
        let (Some(first), Some(&first_id)) = (parents.first(), changeset.parents.first()) else {
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
        let depth = first.depth + 1;
        let parent = Link {
            id: first_id,
            generation: first.generation,
            depth: first.depth,
        };
        // Myers' rule: if the parent's jump and the jump after it span the
        // same depth, jump over both. Else jump to the parent.
        let after = if first.depth == 0 {
            first.jump
        } else {
            node(c, first.jump.id)?.jump
        };
        let jump = if first.depth - first.jump.depth == first.jump.depth - after.depth {
            after
        } else {
            parent
        };
        let merge = if changeset.parents.len() > 1 {
            Some(Link {
                id,
                generation,
                depth,
            })
        } else {
            first.merge
        };
        Ok(Node {
            generation,
            depth,
            parent,
            jump,
            merge,
        })
    }

    fn encode(node: &Node) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + 3 * LINK_BYTES);
        out.extend(node.generation.to_le_bytes());
        out.extend(node.depth.to_le_bytes());
        for link in [Some(node.parent), Some(node.jump), node.merge]
            .iter()
            .flatten()
        {
            out.extend(link.id.as_bytes());
            out.extend(link.generation.to_le_bytes());
            out.extend(link.depth.to_le_bytes());
        }
        out
    }

    fn decode(bytes: &[u8]) -> Option<Node> {
        let int = |bytes: &[u8]| Some(i64::from_le_bytes(bytes.try_into().ok()?));
        let (head, links) = bytes.split_at_checked(16)?;
        if links.len() % LINK_BYTES != 0 {
            return None;
        }
        let links: Vec<Link> = links
            .chunks(LINK_BYTES)
            .map(|link| {
                Some(Link {
                    id: ChangesetId::try_from(&link[..32]).ok()?,
                    generation: int(&link[32..40])?,
                    depth: int(&link[40..])?,
                })
            })
            .collect::<Option<_>>()?;
        let (parent, jump, merge) = match links[..] {
            [parent, jump] => (parent, jump, None),
            [parent, jump, merge] => (parent, jump, Some(merge)),
            _ => return None,
        };
        Some(Node {
            generation: int(&head[..8])?,
            depth: int(&head[8..])?,
            parent,
            jump,
            merge,
        })
    }
}

/// Whether `ancestor` is `descendant` or reachable from it through parents.
/// Derives the commit graph first if `descendant` has no node.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have either changeset.
/// [`Error::ResourceExhausted`] if the answer needs more than `max_reads`
/// reads of nodes and parents.
pub fn is_ancestor(
    store: &Store,
    repo: RepoId,
    ancestor: ChangesetId,
    descendant: ChangesetId,
    max_reads: usize,
) -> Result<bool> {
    let node = derived::get::<CommitGraph>(store, repo, descendant)?;
    store.read(|c| {
        let floor = db::generation(c, repo, ancestor)?;
        let from = At {
            id: descendant,
            node,
        };
        reaches(&mut Index::new(c, max_reads), from, ancestor, floor)
    })
}

/// The best common ancestors of `a` and `b`, as [`db::merge_bases`] gives
/// them. Derives the commit graph first if `a` or `b` has no node. Reads a
/// few nodes when neither first-parent history has a merge newer than where
/// the two meet. Else walks at most `max_reads` changesets.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have either changeset.
/// [`Error::ResourceExhausted`] if the answer needs more than `max_reads`
/// reads.
pub fn merge_bases(
    store: &Store,
    repo: RepoId,
    a: ChangesetId,
    b: ChangesetId,
    max_reads: usize,
) -> Result<Vec<ChangesetId>> {
    let a_node = derived::get::<CommitGraph>(store, repo, a)?;
    let b_node = derived::get::<CommitGraph>(store, repo, b)?;
    store.read(|c| {
        let (a, b) = (At { id: a, node: a_node }, At { id: b, node: b_node });
        match meeting(&mut Index::new(c, max_reads), a, b)? {
            Some(bases) => Ok(bases),
            None => db::merge_bases(c, repo, a.id, b.id, max_reads),
        }
    })
}

/// Whether `from` is `ancestor`, of generation `floor`, or reaches it
/// through parents.
///
/// A path from `from` to `ancestor` follows first parents all the way, or
/// leaves the first-parent history at a merge newer than `ancestor`,
/// through another parent of the merge. So this searches the first-parent
/// history of `from`, then does the same from each such parent. It takes
/// each merge once.
fn reaches(index: &mut Index, from: At, ancestor: ChangesetId, floor: i64) -> Result<bool> {
    let mut next = vec![from];
    let mut merges = HashSet::new();
    while let Some(from) = next.pop() {
        if index.descend(from, floor, |link| link.generation)?.id == ancestor {
            return Ok(true);
        }
        let mut merge = from.node.merge;
        while let Some(at) = merge {
            // An older merge has older parents only. A merge seen before had
            // the history below it searched then.
            if at.generation <= floor || !merges.insert(at.id) {
                break;
            }
            let parents = index.parents(at)?;
            let Some((&(first, _), others)) = parents.split_first() else {
                return Err(Error::Corrupt(format!(
                    "commit graph: merge {} has no parents",
                    at.id
                )));
            };
            for &(parent, generation) in others {
                if generation >= floor {
                    next.push(index.at(parent)?);
                }
            }
            merge = index.at(first)?.node.merge;
        }
    }
    Ok(false)
}

/// The best common ancestors of `a` and `b` when neither first-parent
/// history has a merge newer than where the two meet: that changeset, or
/// none if the histories do not meet. `None` when one of them has such a
/// merge.
///
/// Without such a merge, the ancestors of `a` are its first-parent history
/// down to the meeting point, then the ancestors of that point, and the same
/// for `b`. The parts above the meeting point are disjoint, so the common
/// ancestors are the ancestors of the meeting point.
fn meeting(index: &mut Index, a: At, b: At) -> Result<Option<Vec<ChangesetId>>> {
    let met = index.meet(a, b)?;
    let floor = met.map_or(0, |at| at.node.generation);
    let newest_merge = |at: At| at.node.merge.map_or(0, |merge| merge.generation);
    if newest_merge(a) > floor || newest_merge(b) > floor {
        return Ok(None);
    }
    Ok(Some(met.map(|at| at.id).into_iter().collect()))
}

/// A changeset and its node.
#[derive(Clone, Copy, Debug)]
struct At {
    id: ChangesetId,
    node: Node,
}

/// Reads nodes and parents for one query, at most `max_reads` of them.
struct Index<'c> {
    c: &'c Connection,
    reads: usize,
    max_reads: usize,
}

impl<'c> Index<'c> {
    fn new(c: &'c Connection, max_reads: usize) -> Self {
        Self {
            c,
            reads: 0,
            max_reads,
        }
    }

    fn count(&mut self) -> Result<()> {
        self.reads += 1;
        if self.reads > self.max_reads {
            return Err(db::too_deep(self.max_reads));
        }
        Ok(())
    }

    /// The node of a changeset that a node points to.
    fn at(&mut self, id: ChangesetId) -> Result<At> {
        self.count()?;
        Ok(At {
            id,
            node: node(self.c, id)?,
        })
    }

    /// Every parent of a merge, in order, with its generation.
    fn parents(&mut self, merge: Link) -> Result<Vec<(ChangesetId, i64)>> {
        self.count()?;
        db::parents(self.c, merge.id, merge.generation)
    }

    /// Walks down the first-parent history of `from` to the oldest changeset
    /// whose `key` is at least `min`, or `from` itself if its parent's is
    /// lower. The key grows along first parents: the generation, or the
    /// depth. Each move takes the jump if it does not pass `min`, else the
    /// parent.
    fn descend(&mut self, mut from: At, min: i64, key: fn(&Link) -> i64) -> Result<At> {
        while from.node.depth > 0 && key(&from.node.parent) >= min {
            let to = if key(&from.node.jump) >= min {
                from.node.jump
            } else {
                from.node.parent
            };
            from = self.at(to.id)?;
        }
        Ok(from)
    }

    /// Where the first-parent histories of `a` and `b` meet: the newest
    /// changeset on both. `None` if they have different roots.
    fn meet(&mut self, a: At, b: At) -> Result<Option<At>> {
        let (deep, shallow) = if a.node.depth >= b.node.depth {
            (a, b)
        } else {
            (b, a)
        };
        let mut a = self.descend(deep, shallow.node.depth, |link| link.depth)?;
        let mut b = shallow;
        // At one depth, the two jumps land on one depth. They land on two
        // changesets exactly while that depth is above the meeting point, so
        // this moves as `descend` does.
        while a.id != b.id {
            if a.node.depth == 0 {
                return Ok(None);
            }
            let (to_a, to_b) = if a.node.jump.id == b.node.jump.id {
                (a.node.parent, b.node.parent)
            } else {
                (a.node.jump, b.node.jump)
            };
            a = self.at(to_a.id)?;
            b = self.at(to_b.id)?;
        }
        Ok(Some(a))
    }
}

/// The stored node of a changeset that a node reaches. It has one: each
/// ancestor of a changeset with a node has one.
fn node(c: &Connection, id: ChangesetId) -> Result<Node> {
    derived::load::<CommitGraph>(c, id)?
        .ok_or_else(|| Error::Corrupt(format!("commit graph: no node for {id}")))
}

#[cfg(test)]
mod tests {
    use scion_core::{BlobId, FileState, IdentityTime, RepoPath};

    use super::*;
    use crate::db::testing;
    use crate::derived::steps;

    /// A new store with the graph `parents` in repo `r`, with no derived
    /// rows, as in a database from before the commit graph.
    fn store_graph(parents: &[Vec<usize>]) -> (tempfile::TempDir, Store, RepoId, Vec<ChangesetId>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (repo, ids) = store
            .write(|c| {
                let repo = db::create_repo(c, "r")?;
                Ok((repo, testing::insert_graph(c, repo, parents)?))
            })
            .unwrap();
        (dir, store, repo, ids)
    }

    /// The depth that a jump from `depth` lands on, by Myers' rule, from the
    /// depths alone.
    fn jump_depth(depth: i64) -> i64 {
        let mut jumps: Vec<i64> = vec![0];
        for parent in 0..depth {
            let jump = jumps[usize::try_from(parent).unwrap()];
            let after = jumps[usize::try_from(jump).unwrap()];
            jumps.push(if parent - jump == jump - after { after } else { parent });
        }
        jumps[usize::try_from(depth).unwrap()]
    }

    /// The first-parent history of each changeset of `parents`, newest
    /// first.
    fn histories(parents: &[Vec<usize>]) -> Vec<Vec<usize>> {
        let mut out: Vec<Vec<usize>> = Vec::new();
        for (i, of) in parents.iter().enumerate() {
            let mut history = vec![i];
            if let Some(&first) = of.first() {
                history.extend(&out[first]);
            }
            out.push(history);
        }
        out
    }

    proptest::proptest! {
        // Each case checks every pair of changesets.
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

        /// On any graph, each node holds what a walk of the graph finds, and
        /// the queries give the answers of the walks with no bound.
        #[test]
        fn queries_match_the_graph_walks(parents in testing::graphs(20)) {
            let (_dir, store, repo, ids) = store_graph(&parents);
            let histories = histories(&parents);
            let nodes: Vec<Node> = ids
                .iter()
                .map(|&id| derived::get::<CommitGraph>(&store, repo, id).unwrap())
                .collect();
            let index_of = |id: ChangesetId| ids.iter().position(|&i| i == id).unwrap();
            for (i, node) in nodes.iter().enumerate() {
                let history = &histories[i];
                let generation = store.read(|c| db::generation(c, repo, ids[i])).unwrap();
                proptest::prop_assert_eq!(node.generation, generation);
                proptest::prop_assert_eq!(usize::try_from(node.depth).unwrap(), history.len() - 1);
                proptest::prop_assert_eq!(index_of(node.parent.id), history[1.min(history.len() - 1)]);
                let jump = jump_depth(node.depth);
                proptest::prop_assert_eq!(node.jump.depth, jump);
                let back = usize::try_from(node.depth - jump).unwrap();
                proptest::prop_assert_eq!(index_of(node.jump.id), history[back]);
                let merge = history.iter().find(|&&h| parents[h].len() > 1);
                proptest::prop_assert_eq!(node.merge.map(|m| index_of(m.id)), merge.copied());
            }
            for x in 0..ids.len() {
                for y in 0..ids.len() {
                    let (a, b) = (ids[x], ids[y]);
                    let walked = store
                        .read(|c| {
                            Ok((
                                db::is_ancestor(c, repo, a, b, usize::MAX)?,
                                db::merge_bases(c, repo, a, b, usize::MAX)?,
                            ))
                        })
                        .unwrap();
                    let ancestry = is_ancestor(&store, repo, a, b, usize::MAX).unwrap();
                    proptest::prop_assert_eq!(ancestry, walked.0, "{} {}", x, y);
                    let bases = merge_bases(&store, repo, a, b, usize::MAX).unwrap();
                    proptest::prop_assert_eq!(bases, walked.1, "{} {}", x, y);

                    // The walks down first-parent histories.
                    let (from, other) = (At { id: b, node: nodes[y] }, At { id: a, node: nodes[x] });
                    let (down, met) = store
                        .read(|c| {
                            let mut index = Index::new(c, usize::MAX);
                            let down = index.descend(from, nodes[x].generation, |l| l.generation)?;
                            Ok((down.id, index.meet(from, other)?.map(|at| at.id)))
                        })
                        .unwrap();
                    let oldest = histories[y]
                        .iter()
                        .take_while(|&&h| nodes[h].generation >= nodes[x].generation)
                        .last()
                        .copied()
                        .unwrap_or(y);
                    proptest::prop_assert_eq!(index_of(down), oldest);
                    let shared = histories[y].iter().find(|h| histories[x].contains(h));
                    proptest::prop_assert_eq!(met.map(index_of), shared.copied());
                }
            }
        }
    }

    /// A first-parent chain of `n` changesets, each node stored as it is
    /// written, and `side`, whose parent is changeset `n / 2`. Returns the
    /// chain and `side`.
    fn deep_chain(store: &Store, n: usize) -> (RepoId, Vec<ChangesetId>, ChangesetId) {
        store
            .write(|c| {
                let repo = db::create_repo(c, "r")?;
                let add = |parent: Option<ChangesetId>, message: String| {
                    let cs = Changeset {
                        parents: parent.into_iter().collect(),
                        author: IdentityTime {
                            name: b"a".to_vec(),
                            email: b"a@x".to_vec(),
                            unix_secs: 0,
                            tz_offset_minutes: 0,
                        },
                        committer: None,
                        message: message.into_bytes(),
                        changes: [(
                            RepoPath::new("f").unwrap(),
                            Some(FileState::Regular {
                                content: BlobId::of(b"f"),
                                executable: false,
                            }),
                        )]
                        .into(),
                    };
                    let id = db::insert_changeset(c, repo, &cs)?;
                    derived::on_write::<CommitGraph>(c, id, &cs)?;
                    Ok::<_, Error>(id)
                };
                let mut ids: Vec<ChangesetId> = Vec::with_capacity(n);
                for i in 0..n {
                    let id = add(ids.last().copied(), i.to_string())?;
                    ids.push(id);
                }
                let side = add(Some(ids[n / 2]), "side".to_owned())?;
                Ok((repo, ids, side))
            })
            .unwrap()
    }

    #[test]
    fn a_deep_history_reads_log_n_nodes() {
        const N: usize = 100_000;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (repo, ids, side) = deep_chain(&store, N);
        let tip = ids[N - 1];
        // Myers bounds a walk down to a depth by about 3 log2 n moves.
        let bound = 3 * (N.ilog2() as usize + 1);

        // Each query counts its reads of nodes and parents.
        let ancestry = |ancestor: ChangesetId, descendant: ChangesetId| {
            store
                .read(|c| {
                    let mut index = Index::new(c, usize::MAX);
                    let from = index.at(descendant)?;
                    let floor = db::generation(c, repo, ancestor)?;
                    Ok((reaches(&mut index, from, ancestor, floor)?, index.reads))
                })
                .unwrap()
        };
        let bases = |a: ChangesetId, b: ChangesetId| {
            store
                .read(|c| {
                    let mut index = Index::new(c, usize::MAX);
                    let (a, b) = (index.at(a)?, index.at(b)?);
                    Ok((meeting(&mut index, a, b)?, index.reads))
                })
                .unwrap()
        };
        let mut most = 0;
        for i in [0, 1, 2, 3, N / 3, N / 2, N - 3, N - 2, N - 1] {
            let (found, reads) = ancestry(ids[i], tip);
            assert!(found, "{i}");
            most = most.max(reads);
            let (met, reads) = bases(ids[i], tip);
            assert_eq!(met, Some(vec![ids[i]]), "{i}");
            most = most.max(reads);
        }
        // A changeset off the history is not an ancestor, and the histories
        // meet where it branched.
        let (found, reads) = ancestry(side, tip);
        assert!(!found);
        most = most.max(reads);
        let (met, reads) = bases(side, tip);
        assert_eq!(met, Some(vec![ids[N / 2]]));
        most = most.max(reads);
        assert!(most <= bound, "{most} reads, more than {bound}");

        // The calls agree, within that bound.
        assert!(is_ancestor(&store, repo, ids[0], tip, bound).unwrap());
        assert!(!is_ancestor(&store, repo, side, tip, bound).unwrap());
        let met = merge_bases(&store, repo, side, tip, bound).unwrap();
        assert_eq!(met, [ids[N / 2]]);
    }

    #[test]
    fn an_old_database_derives_the_graph_in_steps_on_first_query() {
        let parents: Vec<Vec<usize>> = vec![vec![], vec![0], vec![0], vec![1, 2], vec![3], vec![]];
        let (_dir, store, repo, ids) = store_graph(&parents);
        let commits = store.commits();
        assert!(is_ancestor(&store, repo, ids[2], ids[4], usize::MAX).unwrap());
        // Changesets 0 to 4 in steps, and nothing for 5.
        assert_eq!(store.commits() - commits, steps(5));
        let commits = store.commits();
        assert!(!is_ancestor(&store, repo, ids[5], ids[4], usize::MAX).unwrap());
        let met = merge_bases(&store, repo, ids[2], ids[4], usize::MAX).unwrap();
        assert_eq!(met, [ids[2]]);
        assert_eq!(store.commits(), commits, "the next queries derive nothing");
    }

    #[test]
    fn a_search_past_its_bound_fails() {
        // A ladder: each changeset on the first-parent history merges a
        // side changeset that branched from its parent.
        let mut parents: Vec<Vec<usize>> = vec![vec![]];
        for _ in 0..10 {
            let parent = parents.len() - 1;
            parents.push(vec![parent]);
            parents.push(vec![parent, parent + 1]);
        }
        let (_dir, store, repo, ids) = store_graph(&parents);
        let tip = *ids.last().unwrap();
        // Ten merges, three reads each, before the side changeset 1.
        let deep = is_ancestor(&store, repo, ids[1], tip, 20);
        assert!(matches!(deep, Err(Error::ResourceExhausted(_))), "{deep:?}");
        assert!(is_ancestor(&store, repo, ids[1], tip, 40).unwrap());
        // On the first-parent history, it reads no merge.
        assert!(is_ancestor(&store, repo, ids[0], tip, 20).unwrap());
    }
}
