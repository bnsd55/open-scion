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
//!   from both changesets, highest generation first (`Paint`). It jumps
//!   over each run of first parents that no other part of the walk reaches.
//!
//! Neither query has a bound on its reads. Each fails once its [`Deadline`]
//! has passed, and runs in steps of about 1,000 reads (`STEPS`). A step holds
//! one read connection ([`Store::read`]), so writes and other reads run
//! beside it.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use scion_core::{Changeset, ChangesetId};

use crate::deadline::Deadline;
use crate::derived::{self, Kind};
use crate::error::{Error, Result};
use crate::meta::{MetaTx, RepoId};
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
        c: &mut dyn MetaTx,
        id: ChangesetId,
        changeset: &Changeset,
        parents: Vec<Node>,
        _lens: crate::tree::BlobLens<'_>,
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
/// [`Error::DeadlineExceeded`] once `deadline` has passed.
pub fn is_ancestor(
    store: &Store,
    repo: RepoId,
    ancestor: ChangesetId,
    descendant: ChangesetId,
    deadline: &Deadline,
) -> Result<bool> {
    let from = At::get(store, repo, descendant)?;
    let floor = store.read(|c| c.generation(repo, ancestor))?;
    let mut search = Search::new(from, ancestor, floor);
    Ok(in_steps(store, deadline, STEPS, |index| search.next(index))?.0)
}

/// The best common ancestors of `a` and `b`, over all parents: every common
/// ancestor that is not an ancestor of another common ancestor. Highest
/// generation first, then lowest ID. Derives the commit graph first if `a`
/// or `b` has no node. Reads a few nodes when neither first-parent history
/// has a merge newer than where the two meet. Else walks the graph
/// (`Paint`).
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have either changeset.
/// [`Error::DeadlineExceeded`] once `deadline` has passed.
pub fn merge_bases(
    store: &Store,
    repo: RepoId,
    a: ChangesetId,
    b: ChangesetId,
    deadline: &Deadline,
) -> Result<Vec<ChangesetId>> {
    let (a, b) = (At::get(store, repo, a)?, At::get(store, repo, b)?);
    if let Some(bases) = store.read(|c| meeting(&mut Index::new(c, deadline), a, b))? {
        return Ok(bases);
    }
    let mut paint = Paint::new(a, b);
    Ok(in_steps(store, deadline, STEPS, |index| paint.next(index))?.0)
}

/// Whether `base`, of generation `base_gen`, is on the first-parent history
/// of `tip`, `tip` included. `None` if `tip` has no node yet. Walks down
/// that history to `base_gen`, so it reads O(log n) nodes. Unlike the
/// queries above, it runs on the caller's transaction, in one go.
///
/// # Errors
///
/// [`Error::DeadlineExceeded`] once `deadline` has passed.
pub(crate) fn on_first_parents(
    c: &mut dyn MetaTx,
    tip: ChangesetId,
    base: ChangesetId,
    base_gen: i64,
    deadline: &Deadline,
) -> Result<Option<bool>> {
    let Some(node) = derived::load::<CommitGraph>(c, tip)? else {
        return Ok(None);
    };
    let mut index = Index::new(c, deadline);
    let from = At { id: tip, node };
    // Generations strictly increase along first parents, so the oldest
    // changeset at or above `base_gen` is the base exactly when the base is
    // on the history.
    let at = index.descend(from, base_gen, |link| link.generation)?;
    Ok(Some(at.id == base))
}

/// What a query read: nodes and parents, in steps.
#[derive(Clone, Copy, Debug, Default)]
struct Cost {
    reads: usize,
    steps: usize,
}

/// How long a walk holds one read connection: for moves of up to `reads`
/// reads.
#[derive(Clone, Copy, Debug)]
struct Steps {
    reads: usize,
}

/// The steps of the queries. A step ends after the move that reaches 1,000
/// reads, and a move reads at most a few dozen rows: O(log n) nodes, or the
/// parents of a merge.
const STEPS: Steps = Steps { reads: 1000 };

/// Makes the moves of a walk with `next` until it gives the answer, in
/// `steps`. Each step holds one read connection, and the walk gives it back
/// between steps. A changeset's parents and node never change, so a walk
/// goes on in the next step from where it stopped.
fn in_steps<T>(
    store: &Store,
    deadline: &Deadline,
    steps: Steps,
    mut next: impl FnMut(&mut Index) -> Result<Option<T>>,
) -> Result<(T, Cost)> {
    let mut cost = Cost::default();
    let mut seen = 0;
    let result = loop {
        cost.steps += 1;
        let (step, step_seen) = store.read_counted(|c| {
            let mut index = Index::new(c, deadline);
            let answer = loop {
                match next(&mut index) {
                    Ok(None) if index.reads < steps.reads => {}
                    answer => break answer,
                }
            };
            cost.reads += index.reads;
            answer
        });
        seen = seen.max(step_seen);
        if let Some(result) = step.transpose() {
            break result;
        }
    };
    // As `Store::read`: the reply waits until each commit that a step could
    // see is durable, with one flush for all steps.
    store.wait_durable(seen)?;
    let Cost { reads, steps } = cost;
    if let Err(Error::DeadlineExceeded(why)) = &result {
        tracing::info!(reads, steps, %why, "a graph query stopped");
    } else {
        tracing::debug!(reads, steps, "graph query");
    }
    result.map(|answer| (answer, cost))
}

/// The search of [`is_ancestor`] for `ancestor`, of generation `floor`.
///
/// A path from a changeset to `ancestor` follows first parents all the way,
/// or leaves the first-parent history at a merge newer than `ancestor`,
/// through another parent of the merge. So this searches the first-parent
/// history of the descendant, then does the same from each such parent. It
/// takes each merge once.
struct Search {
    ancestor: ChangesetId,
    floor: i64,
    /// Changesets whose first-parent history is still to search.
    next: Vec<At>,
    /// The next merge to take on the history being searched.
    merge: Option<Link>,
    /// The merges taken.
    merges: HashSet<ChangesetId>,
}

impl Search {
    fn new(from: At, ancestor: ChangesetId, floor: i64) -> Self {
        Self {
            ancestor,
            floor,
            next: vec![from],
            merge: None,
            merges: HashSet::new(),
        }
    }

    /// One move: takes the next merge on the history being searched, or
    /// searches the next history down to `floor`. Then `Some(found)` if the
    /// search has ended.
    fn next(&mut self, index: &mut Index) -> Result<Option<bool>> {
        if let Some(at) = self.merge.take() {
            // An older merge has older parents only. A merge seen before had
            // the history below it searched then.
            if at.generation <= self.floor || !self.merges.insert(at.id) {
                return Ok(None);
            }
            let parents = index.parents(at)?;
            let Some((&(first, _), others)) = parents.split_first() else {
                return Err(Error::Corrupt(format!(
                    "commit graph: merge {} has no parents",
                    at.id
                )));
            };
            for &(parent, generation) in others {
                if generation >= self.floor {
                    self.next.push(index.at(parent)?);
                }
            }
            self.merge = index.at(first)?.node.merge;
            return Ok(None);
        }
        let Some(from) = self.next.pop() else {
            return Ok(Some(false));
        };
        if index.descend(from, self.floor, |link| link.generation)?.id == self.ancestor {
            return Ok(Some(true));
        }
        self.merge = from.node.merge;
        Ok(None)
    }
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

/// Reached from `a`.
const FROM_A: u8 = 1;
/// Reached from `b`.
const FROM_B: u8 = 2;
/// Reached from a common ancestor already found, so not a best one.
const STALE: u8 = 4;

/// The walk of [`merge_bases`] from `a` and `b`. It paints the graph as
/// `git merge-base --all` does.
///
/// The walk takes changesets from a queue, highest generation first, and
/// marks each with the sides that reach it. A parent's generation is below
/// its child's, so a changeset leaves the queue only after every descendant
/// the walk reached, with all its marks. One marked from both sides and not
/// stale is a best common ancestor, and its ancestors become stale. The walk
/// ends when no queued changeset that is not stale is marked from `a`, or
/// none from `b`: no other best common ancestor can follow.
///
/// It jumps over runs of first parents. Each changeset that the rest of the
/// walk reaches has a generation of at most `next`, the highest one still
/// queued. So the first parents of a changeset taken, down to generation
/// `next + 1`, get no other mark. The walk would take them one after the
/// other, with the mark of the one taken, and find no base among them. Down
/// to the newest merge on that history, each has one parent. So the walk
/// jumps down to the oldest of them, or to that merge if it comes first,
/// and goes on from its parents.
struct Paint {
    /// The marks of the queued changesets. So the walk holds memory for
    /// its queue, not for the history it walked.
    marks: HashMap<ChangesetId, u8>,
    queue: BinaryHeap<(i64, Reverse<ChangesetId>)>,
    /// Queued changesets that are not stale, marked from `a` and from `b`.
    live: [usize; 2],
    bases: Vec<ChangesetId>,
}

impl Paint {
    fn new(a: At, b: At) -> Self {
        let mut paint = Self {
            marks: HashMap::new(),
            queue: BinaryHeap::new(),
            // One each to start, also when `a` is `b`.
            live: [1, 1],
            bases: Vec::new(),
        };
        for (at, side) in [(a, FROM_A), (b, FROM_B)] {
            let mark = paint.marks.entry(at.id).or_insert(0);
            if *mark == 0 {
                paint.queue.push((at.node.generation, Reverse(at.id)));
            }
            *mark |= side;
        }
        paint
    }

    /// One move: takes the next changeset from the queue. Then the bases if
    /// the walk has ended.
    fn next(&mut self, index: &mut Index) -> Result<Option<Vec<ChangesetId>>> {
        let Some((_, Reverse(id))) = self.queue.pop().filter(|_| !self.live.contains(&0)) else {
            return Ok(Some(std::mem::take(&mut self.bases)));
        };
        // No changeset taken later has it as a parent: its generation is
        // higher. So its mark goes, and the marks are the queue's.
        let mut mark = self
            .marks
            .remove(&id)
            .expect("a queued changeset has a mark");
        tally(&mut self.live, mark, false);
        if mark == FROM_A | FROM_B {
            self.bases.push(id);
            mark |= STALE;
        }
        let next = self.queue.peek().map_or(0, |&(generation, _)| generation);
        let from = index.at(id)?;
        let merge = from.node.merge.map_or(0, |merge| merge.generation);
        let last = index.descend(from, merge.max(next + 1), |link| link.generation)?;
        let parents = match last.node.merge {
            Some(merge) if merge.id == last.id => index.parents(merge)?,
            _ if last.node.depth > 0 => vec![(last.node.parent.id, last.node.parent.generation)],
            _ => Vec::new(),
        };
        for (parent, parent_gen) in parents {
            let old = self.marks.get(&parent).copied();
            let new = old.unwrap_or(0) | mark;
            if old == Some(new) {
                continue;
            }
            match old {
                // Still queued: its generation is below the one taken.
                Some(old) => tally(&mut self.live, old, false),
                None => self.queue.push((parent_gen, Reverse(parent))),
            }
            tally(&mut self.live, new, true);
            self.marks.insert(parent, new);
        }
        Ok(None)
    }
}

/// Adds a queued changeset's marks to `live`, or removes them. A stale
/// changeset counts for neither side.
fn tally(live: &mut [usize; 2], mark: u8, add: bool) {
    if mark & STALE != 0 {
        return;
    }
    for (count, side) in live.iter_mut().zip([FROM_A, FROM_B]) {
        if mark & side != 0 {
            if add {
                *count += 1;
            } else {
                *count -= 1;
            }
        }
    }
}

/// A changeset and its node.
#[derive(Clone, Copy, Debug)]
struct At {
    id: ChangesetId,
    node: Node,
}

impl At {
    /// A changeset of `repo` and its node. Derives the commit graph first if
    /// it has no node.
    fn get(store: &Store, repo: RepoId, id: ChangesetId) -> Result<Self> {
        let node = derived::get::<CommitGraph>(store, repo, id)?;
        Ok(Self { id, node })
    }
}

/// Reads nodes and parents for one step of a query, until its deadline.
struct Index<'a> {
    c: &'a mut dyn MetaTx,
    deadline: &'a Deadline,
    reads: usize,
}

impl<'a> Index<'a> {
    fn new(c: &'a mut dyn MetaTx, deadline: &'a Deadline) -> Self {
        Self {
            c,
            deadline,
            reads: 0,
        }
    }

    /// Counts a read. Fails once the deadline has passed.
    fn count(&mut self) -> Result<()> {
        self.reads += 1;
        self.deadline.check()
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
        self.c.parents(merge.id, merge.generation)
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
fn node(c: &mut dyn MetaTx, id: ChangesetId) -> Result<Node> {
    derived::load::<CommitGraph>(c, id)?
        .ok_or_else(|| Error::Corrupt(format!("commit graph: no node for {id}")))
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use proptest::prelude::{Strategy, any};
    use proptest::sample::Index as Sample;
    use scion_core::{BlobId, FileState, IdentityTime, RepoPath};

    use super::*;
    use crate::db::testing;
    use crate::derived::steps;

    /// Steps of a few reads, so that walks go on across steps.
    const SMALL: Steps = Steps { reads: 4 };

    /// A deadline that a test does not reach.
    fn later() -> Deadline {
        Deadline::new(Instant::now() + Duration::from_secs(600))
    }

    /// A new store with the graph `parents` in repo `r`, with no derived
    /// rows, as in a database from before the commit graph.
    fn store_graph(parents: &[Vec<usize>]) -> (tempfile::TempDir, Store, RepoId, Vec<ChangesetId>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (repo, ids) = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                Ok((repo, testing::insert_graph(c, repo, parents)?))
            })
            .unwrap();
        (dir, store, repo, ids)
    }

    /// As [`store_graph`], then stores each node as a write does.
    fn store_graph_with_nodes(
        parents: &[Vec<usize>],
    ) -> (tempfile::TempDir, Store, RepoId, Vec<ChangesetId>) {
        let (dir, store, repo, ids) = store_graph(parents);
        store
            .write(|c| {
                for &id in &ids {
                    let cs = c.get_changeset(repo, id)?;
                    derived::on_write::<CommitGraph>(
                        c,
                        id,
                        &cs,
                        &mut crate::tree::lens_of(&store),
                    )?;
                }
                Ok(())
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
            jumps.push(if parent - jump == jump - after {
                after
            } else {
                parent
            });
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

    /// The ancestors of each changeset of `parents`, itself included.
    fn ancestors(parents: &[Vec<usize>]) -> Vec<HashSet<usize>> {
        let mut out: Vec<HashSet<usize>> = Vec::new();
        for (i, of) in parents.iter().enumerate() {
            let mut all: HashSet<usize> = of.iter().flat_map(|&p| out[p].clone()).collect();
            all.insert(i);
            out.push(all);
        }
        out
    }

    /// Graphs of [`testing::graphs`] with most merges taken out, so that
    /// first-parent runs are long and the merge-base walk jumps over them.
    fn sparse_graphs(max: usize) -> impl Strategy<Value = Vec<Vec<usize>>> {
        let merges = proptest::collection::vec(proptest::bool::weighted(0.15), max);
        (testing::graphs(max), merges).prop_map(|(mut parents, merges)| {
            for (of, merge) in parents.iter_mut().zip(merges) {
                if !merge {
                    of.truncate(1);
                }
            }
            parents
        })
    }

    /// Pairs of changesets that each case of [`queries_match_the_graph`]
    /// checks, of the up to 400 of a graph. CI runs 2,000 cases.
    const PAIRS: usize = 40;

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

        /// On any graph, each node holds what a walk of the graph finds, and
        /// the queries give the answers that the sets of ancestors define.
        /// A step here takes a few reads, so the walks go on across steps.
        #[test]
        fn queries_match_the_graph(
            parents in proptest::prop_oneof![testing::graphs(20), sparse_graphs(20)],
            pairs in proptest::collection::vec(any::<(Sample, Sample)>(), PAIRS),
        ) {
            let (_dir, store, repo, ids) = store_graph(&parents);
            let histories = histories(&parents);
            let ancestors = ancestors(&parents);
            let nodes: Vec<Node> = ids
                .iter()
                .map(|&id| derived::get::<CommitGraph>(&store, repo, id).unwrap())
                .collect();
            let index_of = |id: ChangesetId| ids.iter().position(|&i| i == id).unwrap();
            for (i, node) in nodes.iter().enumerate() {
                let history = &histories[i];
                let generation = store.read(|c| c.generation(repo, ids[i])).unwrap();
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
            // Ancestry for every pair, in one read: some histories reach the
            // ancestor only through an older merge, and few pairs show it.
            let searched = store
                .read(|c| {
                    let deadline = later();
                    let mut walked = Vec::new();
                    for (x, y) in (0..ids.len()).flat_map(|x| (0..ids.len()).map(move |y| (x, y))) {
                        let (a, b) = (ids[x], ids[y]);
                        walked.push(c.is_ancestor(repo, a, b, usize::MAX)?);
                    }
                    let mut index = Index::new(&mut *c, &deadline);
                    let mut out = Vec::new();
                    for (x, y) in (0..ids.len()).flat_map(|x| (0..ids.len()).map(move |y| (x, y))) {
                        let (a, b) = (ids[x], ids[y]);
                        let mut search = Search::new(At { id: b, node: nodes[y] }, a, nodes[x].generation);
                        let found = loop {
                            if let Some(found) = search.next(&mut index)? {
                                break found;
                            }
                        };
                        out.push((x, y, walked[x * ids.len() + y], found));
                    }
                    Ok(out)
                })
                .unwrap();
            for (x, y, walked, found) in searched {
                let ancestry = ancestors[y].contains(&x);
                proptest::prop_assert_eq!(walked, ancestry, "{} {}", x, y);
                proptest::prop_assert_eq!(found, ancestry, "{} {}", x, y);
            }
            // The rest for a sample of the pairs. The walks here go on
            // across steps.
            for (x, y) in pairs.iter().map(|(x, y)| (x.index(ids.len()), y.index(ids.len()))) {
                let (a, b) = (ids[x], ids[y]);
                // The best common ancestors, by their definition.
                let common: Vec<usize> = (0..ids.len())
                    .filter(|c| ancestors[x].contains(c) && ancestors[y].contains(c))
                    .collect();
                let mut best: Vec<usize> = common
                    .iter()
                    .copied()
                    .filter(|&c| !common.iter().any(|&d| d != c && ancestors[d].contains(&c)))
                    .collect();
                best.sort_by_key(|&c| (Reverse(nodes[c].generation), ids[c]));
                let best: Vec<ChangesetId> = best.into_iter().map(|c| ids[c]).collect();
                let ancestry = ancestors[y].contains(&x);
                let mut search = Search::new(At { id: b, node: nodes[y] }, a, nodes[x].generation);
                let (found, _) =
                    in_steps(&store, &later(), SMALL, |index| search.next(index)).unwrap();
                proptest::prop_assert_eq!(found, ancestry, "{} {}", x, y);
                // `merge_bases` answers from where the first-parent
                // histories meet, or else from the walk. Each that
                // answers is right, and the walk always answers.
                let (from, other) = (At { id: b, node: nodes[y] }, At { id: a, node: nodes[x] });
                let mut paint = Paint::new(other, from);
                let (painted, _) =
                    in_steps(&store, &later(), SMALL, |index| paint.next(index)).unwrap();
                proptest::prop_assert_eq!(&painted, &best, "{} {}", x, y);
                let (bases, down, met) = store
                    .read(|c| {
                        let deadline = later();
                        let mut index = Index::new(c, &deadline);
                        let bases = meeting(&mut index, other, from)?;
                        // The walks down first-parent histories.
                        let down = index.descend(from, nodes[x].generation, |l| l.generation)?;
                        Ok((bases, down.id, index.meet(from, other)?.map(|at| at.id)))
                    })
                    .unwrap();
                if let Some(bases) = bases {
                    proptest::prop_assert_eq!(&bases, &best, "{} {}", x, y);
                }
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

    /// A first-parent chain of `n` changesets, each node stored as it is
    /// written, and `side`, whose parent is changeset `n / 2`. Returns the
    /// chain and `side`.
    fn deep_chain(store: &Store, n: usize) -> (RepoId, Vec<ChangesetId>, ChangesetId) {
        store
            .write(|c| {
                let repo = c.create_repo("r")?;
                let mut add = |parent: Option<ChangesetId>, message: String| {
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
                    let id = c.insert_changeset(repo, &cs)?;
                    derived::on_write::<CommitGraph>(c, id, &cs, &mut crate::tree::lens_of(store))?;
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
        // Deep enough that a walk without the jump reads about a hundred
        // times more than the bound, and small enough to build in under a
        // second. The test counts reads, so its pass does not depend on how
        // busy the machine is.
        const N: usize = 4_096;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (repo, ids, side) = deep_chain(&store, N);
        let tip = ids[N - 1];
        // Myers bounds a walk down to a depth by about 3 log2 n moves.
        let bound = 3 * (N.ilog2() as usize + 1);

        // Each query counts its reads of nodes and parents.
        let ancestry = |ancestor: ChangesetId, descendant: ChangesetId| {
            let floor = store.read(|c| c.generation(repo, ancestor)).unwrap();
            let mut search =
                Search::new(At::get(&store, repo, descendant).unwrap(), ancestor, floor);
            let (found, cost) =
                in_steps(&store, &later(), STEPS, |index| search.next(index)).unwrap();
            (found, cost.reads)
        };
        let bases = |a: ChangesetId, b: ChangesetId| {
            let (a, b) = (
                At::get(&store, repo, a).unwrap(),
                At::get(&store, repo, b).unwrap(),
            );
            store
                .read(|c| {
                    let deadline = later();
                    let mut index = Index::new(&mut *c, &deadline);
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

        // The calls agree.
        assert!(is_ancestor(&store, repo, ids[0], tip, &later()).unwrap());
        assert!(!is_ancestor(&store, repo, side, tip, &later()).unwrap());
        let met = merge_bases(&store, repo, side, tip, &later()).unwrap();
        assert_eq!(met, [ids[N / 2]]);
    }

    #[test]
    fn an_old_database_derives_the_graph_in_steps_on_first_query() {
        let parents: Vec<Vec<usize>> = vec![vec![], vec![0], vec![0], vec![1, 2], vec![3], vec![]];
        let (_dir, store, repo, ids) = store_graph(&parents);
        let commits = store.commits();
        assert!(is_ancestor(&store, repo, ids[2], ids[4], &later()).unwrap());
        // Changesets 0 to 4 in steps, and nothing for 5.
        assert_eq!(store.commits() - commits, steps(5));
        let commits = store.commits();
        assert!(!is_ancestor(&store, repo, ids[5], ids[4], &later()).unwrap());
        let met = merge_bases(&store, repo, ids[2], ids[4], &later()).unwrap();
        assert_eq!(met, [ids[2]]);
        assert_eq!(store.commits(), commits, "the next queries derive nothing");

        // The write derives root trees, whose entries name the graph's
        // blobs "f0".."f5"; their files feed the real lens.
        for i in 0..6 {
            store.blobs().put(format!("f{i}").as_bytes()).unwrap();
        }
        let cs = Changeset {
            parents: vec![ids[4], ids[1]],
            ..store.read(|c| c.get_changeset(repo, ids[4])).unwrap()
        };
        let (merge, _) = store
            .write(|c| {
                crate::tree::store_changeset(c, repo, &cs, &mut crate::tree::lens_of(&store))
            })
            .unwrap();
        let commits = store.commits();
        assert!(is_ancestor(&store, repo, ids[1], merge, &later()).unwrap());
        assert_eq!(store.commits(), commits, "the write stored the node");
    }

    #[test]
    fn a_merge_base_walk_jumps_over_runs_between_merges() {
        // A first-parent history of N changesets. Every 1,000th merges a side
        // changeset that branched from the one before. `side` branched from
        // the 100th, and is not merged.
        const N: usize = 20_000;
        let mut parents: Vec<Vec<usize>> = vec![vec![]];
        let mut history = vec![0];
        for i in 1..N {
            let last = *history.last().unwrap();
            parents.push(vec![last]);
            if i % 1000 == 0 {
                // A merge of the side changeset just pushed.
                parents.push(vec![last, parents.len() - 1]);
            }
            history.push(parents.len() - 1);
        }
        parents.push(vec![history[100]]);
        let (_dir, store, repo, ids) = store_graph_with_nodes(&parents);
        let (side, tip) = (*ids.last().unwrap(), ids[history[N - 1]]);
        let (a, b) = (
            At::get(&store, repo, side).unwrap(),
            At::get(&store, repo, tip).unwrap(),
        );
        let deadline = later();
        let met = store.read(|c| meeting(&mut Index::new(c, &deadline), a, b));
        assert_eq!(met.unwrap(), None, "a merge is newer than where they meet");

        let mut paint = Paint::new(a, b);
        let (bases, cost) = in_steps(&store, &deadline, STEPS, |index| paint.next(index)).unwrap();
        assert_eq!(bases, [ids[history[100]]]);
        // A walk of one changeset at a time takes all 20,000. This one jumps
        // down to each merge in about 3 log2 n reads (Myers), and takes a
        // few changesets around it.
        let move_reads = 3 * (N.ilog2() as usize + 1);
        let bound = (N / 1000 + 1) * (move_reads + 4);
        assert!(cost.reads <= bound, "{cost:?}, more than {bound} reads");
        assert_eq!(
            merge_bases(&store, repo, tip, side, &later()).unwrap(),
            [ids[history[100]]]
        );
    }

    /// A ladder of `merges` merges: each changeset on the first-parent
    /// history merges a side changeset that branched from its parent.
    fn ladder(merges: usize) -> Vec<Vec<usize>> {
        let mut parents: Vec<Vec<usize>> = vec![vec![]];
        for _ in 0..merges {
            let parent = parents.len() - 1;
            parents.push(vec![parent]);
            parents.push(vec![parent, parent + 1]);
        }
        parents
    }

    /// Makes the moves of a walk with `next`, and lets its call go after
    /// `stop` moves. Returns the answer, and the moves made.
    fn walk_until<T>(
        store: &Store,
        stop: usize,
        mut next: impl FnMut(&mut Index) -> Result<Option<T>>,
    ) -> (Result<T>, usize) {
        let deadline = later();
        let mut call = Some(deadline.guard());
        let mut moves = 0;
        let answer = in_steps(store, &deadline, SMALL, |index| {
            if moves == stop {
                drop(call.take());
            }
            moves += 1;
            next(index)
        });
        (answer.map(|(answer, _)| answer), moves)
    }

    #[test]
    fn a_walk_stops_at_its_deadline() {
        // The walks from the tip go through every merge down to `side`.
        let (_dir, store, repo, ids) = store_graph(&ladder(100));
        let (side, tip) = (ids[1], *ids.last().unwrap());
        let passed = Deadline::new(Instant::now());
        let late = is_ancestor(&store, repo, side, tip, &passed);
        assert!(matches!(late, Err(Error::DeadlineExceeded(_))), "{late:?}");
        let late = merge_bases(&store, repo, side, tip, &passed);
        assert!(matches!(late, Err(Error::DeadlineExceeded(_))), "{late:?}");

        // A call that goes in the middle of a walk stops it at its next read.
        let floor = store.read(|c| c.generation(repo, side)).unwrap();
        let search = || Search::new(At::get(&store, repo, tip).unwrap(), side, floor);
        let mut whole = search();
        let (found, all) = walk_until(&store, usize::MAX, |index| whole.next(index));
        assert!(found.unwrap());
        let mut stopped = search();
        let (late, moves) = walk_until(&store, 50, |index| stopped.next(index));
        assert!(matches!(late, Err(Error::DeadlineExceeded(_))), "{late:?}");
        assert!(50 < moves && moves < all, "{moves} of {all} moves");

        let paint = || {
            Paint::new(
                At::get(&store, repo, side).unwrap(),
                At::get(&store, repo, tip).unwrap(),
            )
        };
        let mut whole = paint();
        let (bases, all) = walk_until(&store, usize::MAX, |index| whole.next(index));
        assert_eq!(bases.unwrap(), [side]);
        let mut stopped = paint();
        let (late, moves) = walk_until(&store, 50, |index| stopped.next(index));
        assert!(matches!(late, Err(Error::DeadlineExceeded(_))), "{late:?}");
        assert!(50 < moves && moves < all, "{moves} of {all} moves");
    }

    #[test]
    fn a_walk_blocks_no_write_and_no_read() {
        let (_dir, store, repo, ids) = store_graph_with_nodes(&ladder(20));
        let (side, tip) = (ids[1], *ids.last().unwrap());
        let mut paint = Paint::new(
            At::get(&store, repo, side).unwrap(),
            At::get(&store, repo, tip).unwrap(),
        );
        let mut other = None;
        let (bases, _) = in_steps(&store, &later(), STEPS, |index| {
            // In the first move, a write and a read run to the end while the
            // walk holds its read connection.
            if other.is_none() {
                let (done, finished) = mpsc::channel();
                let store = store.clone();
                other = Some(std::thread::spawn(move || {
                    store.write(|c| c.create_repo("other").map(drop)).unwrap();
                    store.read(|_| Ok(())).unwrap();
                    done.send(()).unwrap();
                }));
                finished
                    .recv_timeout(Duration::from_secs(10))
                    .expect("a write or a read waited for the walk");
            }
            paint.next(index)
        })
        .unwrap();
        other.unwrap().join().unwrap();
        assert_eq!(bases, [side]);
    }

    #[test]
    fn a_walk_waits_for_one_flush_for_all_its_steps() {
        let (_dir, store, repo, ids) = store_graph_with_nodes(&ladder(20));
        let (side, tip) = (ids[1], *ids.last().unwrap());
        let mut paint = Paint::new(
            At::get(&store, repo, side).unwrap(),
            At::get(&store, repo, tip).unwrap(),
        );
        let before = store.blobs().full_flushes();
        let mut commits = 0;
        let (bases, cost) = in_steps(&store, &later(), SMALL, |index| {
            // Each step sees a commit that no flush covers yet.
            if index.reads == 0 {
                commits += 1;
                let name = format!("other {commits}");
                store.commit(|c| c.create_repo(&name).map(drop))?;
            }
            paint.next(index)
        })
        .unwrap();
        assert_eq!(bases, [side]);
        assert!(cost.steps > 1, "{cost:?}");
        // None where the commit's own fsync flushes the drive cache.
        let flushes = store.blobs().full_flushes() - before;
        assert_eq!(flushes, u64::from(crate::store::FSYNC_LEAVES_DRIVE_CACHE));
    }
}
