//! The view of a repo that a partial reader reads: the real trees with the
//! entries that its read rules do not cover removed. A view tree is an
//! ordinary `TreeV1` object, stored in the same tree table and
//! content-addressed by its own ID, so a ticket minted on a view root
//! walks, serves and caches like any other tree ([#121]). A kept directory
//! entry points at the child's view tree when anything below it is hidden,
//! and at the real child tree when the whole subtree is visible; the same
//! real tree under the same context shares one view. A subtree whose view
//! would be empty is hidden: its parent's view omits the entry, the same
//! as a missing one.
//!
//! The root of the view is cached per (repo, view key, changeset) in
//! `view_roots`, where the view key is the rule-set digest bound with the
//! projection version. The derivation is a pure function of the rule set
//! and the real trees, so two servers that derive the same view store the
//! same bytes and agree on the row.
//!
//! What the derivation costs, and what that leaks (accepted in the design):
//! the first read of a rule set walks the real tree, so its latency varies
//! with the repo; later reads of the same key and changeset find the row
//! and skip the walk.
//!
//! SHORTCUT: the store caches the view root only, not each derived tree;
//! a cache miss re-walks the whole tree. File-granular rules over a large
//! flat tree pay the walk once per changeset. Per-tree rows (one per
//! (key, tree, context)) are the upgrade path if that shows in practice.
//! SHORTCUT: rows of old keys (a changed rule set, a bumped projection
//! version) are never removed; [#183] holds the cleanup.

use scion_core::{ChangesetId, Entry, RepoPath, Tree, TreeId};

use crate::deadline::Deadline;
use crate::error::Result;
use crate::meta::{MetaTx, RepoId};
use crate::store::Store;

/// The context of a tree in the projection: the read prefixes that fall
/// below the tree's path, relative to it, in the rule set's canonical form.
type Context = Vec<Vec<u8>>;

/// What a context says about one directory entry.
#[derive(Debug)]
enum Below {
    /// The whole subtree is visible: the real child tree serves as is.
    Whole,
    /// Nothing below the entry is visible: the entry is hidden.
    Nothing,
    /// The visible suffixes below the child.
    Some(Context),
}

/// The context below `name`.
fn below(ctx: &[Vec<u8>], name: &[u8]) -> Below {
    let mut head = name.to_vec();
    head.push(b'/');
    let mut sub = Context::new();
    for prefix in ctx {
        if prefix.as_slice() == name {
            return Below::Whole;
        }
        if let Some(rest) = prefix.strip_prefix(&head[..]) {
            sub.push(rest.to_vec());
        }
    }
    if sub.is_empty() {
        Below::Nothing
    } else {
        Below::Some(sub)
    }
}

/// The subtrees derived so far, by (tree ID, context digest), so shared
/// subtrees are derived once per derivation, and the new view trees it
/// built, for the one short write that stores them.
#[derive(Default)]
struct Memo {
    derived: std::collections::HashMap<(TreeId, [u8; 32]), Option<TreeId>>,
    trees: Vec<Tree>,
}

/// The version of the projection. Bumped when the derivation changes, so
/// views of an old version stop being served: the version is bound into
/// the view key, and the old rows are never read again.
const PROJECTION_VERSION: u8 = 1;

/// The view key: the rule-set digest bound with the projection version.
fn view_key(digest: &[u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[PROJECTION_VERSION]);
    hasher.update(digest);
    *hasher.finalize().as_bytes()
}

/// The view root that `prefixes` derives for `cs`: the ID of the tree that
/// holds exactly what the rule set covers. The first call for a (repo,
/// key, changeset) walks the real tree; the later ones find the row. The
/// walk runs on a read connection under `deadline` and stores nothing;
/// one short write follows with the new trees and the row, so a slow
/// derivation never holds the write lock.
///
/// # Errors
///
/// Store errors, or [`crate::error::Error::DeadlineExceeded`] once
/// `deadline` passes.
pub(crate) fn view_root(
    store: &Store,
    repo: RepoId,
    digest: &[u8; 32],
    cs: ChangesetId,
    prefixes: &[RepoPath],
    deadline: &Deadline,
) -> Result<TreeId> {
    let key = view_key(digest);
    if let Some(cached) = store.read(|c| c.get_view_root(repo, &key, cs))? {
        return Ok(cached);
    }
    let real = crate::tree::root_tree(store, repo, cs)?;
    let context: Vec<Vec<u8>> = prefixes.iter().map(|p| p.as_bytes().to_vec()).collect();
    // The derivation reads real trees only: it holds no write lock, and it
    // gives up when the call that asked for it has gone.
    let mut memo = Memo::default();
    // A rule set with prefixes covers at least one path of the repo, but it
    // may cover only paths that this changeset's tree does not have: then
    // the view is the empty tree, which is a valid root. The root is never
    // dropped.
    let view = if let Some(view) =
        store.read(|c| project(&mut *c, real, &context, &mut memo, deadline))?
    {
        view
    } else {
        let empty = Tree::default();
        let id = empty.encode_with_id().1;
        memo.trees.push(empty);
        id
    };
    // One short write: the new view trees, then the row. Both are safe to
    // repeat; a row that another server stored already must agree, or the
    // derivation is not a pure function and that is a bug.
    store.write(|c| {
        for tree in &memo.trees {
            c.put_tree(tree)?;
        }
        c.put_view_root(repo, &key, cs, view)?;
        Ok(view)
    })
}

/// Projects the tree `id` under `ctx`, the visible suffixes below it.
/// Returns `None` when nothing below is visible, so the caller hides the
/// entry that holds it. Stores nothing: the derived trees wait in `memo`
/// for the caller's one write.
///
/// # Errors
///
/// Store errors, or [`crate::error::Error::DeadlineExceeded`] once
/// `deadline` passes.
fn project(
    c: &mut dyn MetaTx,
    id: TreeId,
    ctx: &Context,
    memo: &mut Memo,
    deadline: &Deadline,
) -> Result<Option<TreeId>> {
    if ctx.is_empty() {
        return Ok(None);
    }
    deadline.check()?;
    let key = (id, context_digest(ctx));
    if let Some(hit) = memo.derived.get(&key) {
        return Ok(*hit);
    }
    let tree = c.get_tree(id)?;
    let mut kept = std::collections::BTreeMap::new();
    for (name, entry) in &tree.entries {
        // A file is visible when a prefix names it exactly; a prefix
        // can name nothing below a file.
        match entry {
            Entry::File(_) => {
                if ctx.iter().any(|prefix| prefix == name.as_bytes()) {
                    kept.insert(name.clone(), *entry);
                }
            }
            Entry::Dir(child) => match below(ctx, name.as_bytes()) {
                Below::Whole => {
                    kept.insert(name.clone(), *entry);
                }
                Below::Nothing => {}
                // The child's view takes its ID; an empty view hides the
                // entry, the same as a missing one.
                Below::Some(sub) => {
                    if let Some(view_id) = project(c, *child, &sub, memo, deadline)? {
                        kept.insert(name.clone(), Entry::Dir(view_id));
                    }
                }
            },
        }
        deadline.check()?;
    }
    let view = if kept.is_empty() {
        None
    } else {
        let view = Tree { entries: kept };
        let id = view.encode_with_id().1;
        memo.trees.push(view);
        Some(id)
    };
    memo.derived.insert(key, view);
    Ok(view)
}

/// The digest of a context, so one derivation memoizes shared subtrees by
/// (tree, context).
fn context_digest(ctx: &Context) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(ctx.len() as u64).to_be_bytes());
    for prefix in ctx {
        hasher.update(&(prefix.len() as u64).to_be_bytes());
        hasher.update(prefix);
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_core::{FileEntry, FileState, PathElem};
    use std::time::{Duration, Instant};

    /// A deadline that no test outlives.
    fn alive() -> Deadline {
        Deadline::new(Instant::now() + Duration::from_secs(600))
    }

    fn file(store: &Store, content: &[u8]) -> Entry {
        let blob = store.blobs().put(content).unwrap();
        let len = u64::try_from(content.len()).unwrap();
        Entry::File(FileEntry::new(
            FileState::Regular {
                content: blob,
                executable: false,
            },
            len,
        ))
    }

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    fn name(raw: &[u8]) -> PathElem {
        PathElem::new(raw.to_vec()).unwrap()
    }

    #[test]
    fn a_view_keeps_visible_files_whole_subtrees_and_drops_hidden_ones() {
        let (_dir, store) = store();
        // /README, /src/public, /src/secret, /docs/note, /ops/only-hidden
        let src = Tree {
            entries: [
                (name(b"public"), file(&store, b"public\n")),
                (name(b"secret"), file(&store, b"secret\n")),
            ]
            .into(),
        };
        let (src_id, root_id) = store
            .write(|c| {
                let src_id = c.put_tree(&src)?;
                let root = Tree {
                    entries: [
                        (name(b"README"), file(&store, b"readme\n")),
                        (name(b"docs"), file(&store, b"note\n")),
                        (name(b"ops"), Entry::Dir(src_id)),
                    ]
                    .into(),
                };
                let root_id = c.put_tree(&root)?;
                Ok((src_id, root_id))
            })
            .unwrap();
        // The rule covers README, docs/note and ops/public.
        let ctx: Context = vec![b"README".to_vec(), b"docs".to_vec(), b"ops/public".to_vec()];
        let mut memo = Memo::default();
        let view = store
            .read(|c| project(&mut *c, root_id, &ctx, &mut memo, &alive()))
            .unwrap()
            .expect("the root keeps a visible path");
        store
            .write(|c| {
                for tree in &memo.trees {
                    c.put_tree(tree)?;
                }
                Ok(())
            })
            .unwrap();
        let shown = store.read(|c| c.get_tree(view)).unwrap();
        let names: Vec<Vec<u8>> = shown
            .entries
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        // `ops` is a view: it holds `public` only, and no `secret`.
        assert_eq!(
            names,
            [b"README".to_vec(), b"docs".to_vec(), b"ops".to_vec()]
        );
        let Entry::Dir(ops) = shown.entries.get(&name(b"ops")).unwrap() else {
            panic!("ops is a directory in the view");
        };
        assert_ne!(*ops, src_id, "the hidden entry changed the subtree");
        let ops_tree = store.write(|c| c.get_tree(*ops)).unwrap();
        let ops_names: Vec<Vec<u8>> = ops_tree
            .entries
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        assert_eq!(ops_names, [b"public".to_vec()]);

        // The same tree with a context that covers nothing is hidden.
        let ctx: Context = vec![b"absent".to_vec()];
        let mut memo = Memo::default();
        let none = store
            .read(|c| project(&mut *c, root_id, &ctx, &mut memo, &alive()))
            .unwrap();
        assert!(none.is_none());

        // A context that covers the whole tree names the tree itself: the
        // caller uses the real ID, and the projection is not asked.
        // (Below::Whole is decided by the parent; see the unit test below.)
    }

    #[test]
    fn a_context_below_a_name_splits_and_wholes() {
        let ctx: Context = vec![b"README".to_vec(), b"src/net".to_vec(), b"src/api".to_vec()];
        match below(&ctx, b"src") {
            Below::Some(sub) => {
                assert_eq!(sub, [b"net".to_vec(), b"api".to_vec()]);
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(below(&ctx, b"README"), Below::Whole));
        assert!(matches!(below(&ctx, b"docs"), Below::Nothing));
        let whole: Context = vec![b"src".to_vec()];
        assert!(matches!(below(&whole, b"src"), Below::Whole));
    }

    #[test]
    fn one_derivation_serves_shared_subtrees_once() {
        let (_dir, store) = store();
        // Two directories with the same content: one visible, one not.
        let inner = Tree {
            entries: [
                (name(b"a"), file(&store, b"a\n")),
                (name(b"b"), file(&store, b"b\n")),
            ]
            .into(),
        };
        let (left, root_id) = store
            .write(|c| {
                let id = c.put_tree(&inner)?;
                let root = Tree {
                    entries: [
                        (name(b"left"), Entry::Dir(id)),
                        (name(b"right"), Entry::Dir(id)),
                    ]
                    .into(),
                };
                let root_id = c.put_tree(&root)?;
                Ok((id, root_id))
            })
            .unwrap();
        // Only `left/a` is visible: the view drops `right` whole and keeps
        // a view of `left` with `a` only. The memo derives the shared tree
        // once per context, and the two contexts differ here.
        let ctx: Context = vec![b"left/a".to_vec()];
        let mut memo = Memo::default();
        let view = store
            .read(|c| project(&mut *c, root_id, &ctx, &mut memo, &alive()))
            .unwrap()
            .unwrap();
        // The same real tree under the other context derives a second,
        // different view: one tree, two views.
        let ctx2: Context = vec![b"right/b".to_vec()];
        let view2 = store
            .read(|c| project(&mut *c, root_id, &ctx2, &mut memo, &alive()))
            .unwrap()
            .unwrap();
        assert_ne!(view, view2, "one real tree, two contexts, two views");
        // The derivation stores nothing: put the derived trees, then the
        // views below are readable.
        store
            .write(|c| {
                for tree in &memo.trees {
                    c.put_tree(tree)?;
                }
                Ok(())
            })
            .unwrap();
        let right_tree = store.read(|c| c.get_tree(view2)).unwrap();
        let right_names: Vec<Vec<u8>> = right_tree
            .entries
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        assert_eq!(right_names, [b"right".to_vec()]);
        let shown = store.write(|c| c.get_tree(view)).unwrap();
        let names: Vec<Vec<u8>> = shown
            .entries
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        assert_eq!(names, [b"left".to_vec()]);
        let Entry::Dir(left_view) = shown.entries.get(&name(b"left")).unwrap() else {
            panic!("left is a directory in the view");
        };
        let left_tree = store.read(|c| c.get_tree(*left_view)).unwrap();
        let kept: Vec<Vec<u8>> = left_tree
            .entries
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        assert_eq!(kept, [b"a".to_vec()]);
        assert_eq!(memo.derived.len(), 4, "root and child, two contexts");
        assert_eq!(memo.trees.len(), 4, "each derived tree waits, unstored");
        let _ = left;
    }

    /// A folder whose whole subtree is visible keeps the real tree ID: the
    /// view of a fully covered directory is the directory itself.
    #[test]
    fn a_fully_visible_folder_keeps_the_real_tree_id() {
        let (_dir, store) = store();
        let docs = Tree {
            entries: [(name(b"note"), file(&store, b"note\n"))].into(),
        };
        let (docs_id, root_id) = store
            .write(|c| {
                let docs_id = c.put_tree(&docs)?;
                let root = Tree {
                    entries: [(name(b"docs"), Entry::Dir(docs_id))].into(),
                };
                Ok((docs_id, c.put_tree(&root)?))
            })
            .unwrap();
        // The rule covers `docs` whole: the view keeps the real `docs` ID.
        let ctx: Context = vec![b"docs".to_vec()];
        let mut memo = Memo::default();
        let view = store
            .read(|c| project(&mut *c, root_id, &ctx, &mut memo, &alive()))
            .unwrap()
            .unwrap();
        let shown = store.read(|c| c.get_tree(view)).unwrap();
        let Entry::Dir(kept) = shown.entries.get(&name(b"docs")).unwrap() else {
            panic!("docs is a directory in the view");
        };
        assert_eq!(*kept, docs_id, "the fully visible folder is unchanged");
    }
}
