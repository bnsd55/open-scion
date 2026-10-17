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
//! The root of the view is cached per (repo, rule-set digest, changeset) in
//! `view_roots`. The derivation is a pure function of the rule set and the
//! real trees, so two servers that derive the same view store the same
//! bytes and agree on the row.
//!
//! What the derivation costs, and what that leaks (accepted in the design):
//! the first read of a rule set walks the real tree, so its latency varies
//! with the repo; later reads of the same digest and changeset find the row
//! and skip the walk.

use scion_core::{ChangesetId, Entry, RepoPath, Tree, TreeId};

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
/// subtrees are derived once per derivation.
#[derive(Default)]
struct Memo(std::collections::HashMap<(TreeId, [u8; 32]), Option<TreeId>>);

/// The view root that `prefixes` derives for `cs`: the ID of the tree that
/// holds exactly what the rule set covers. The first call for a (repo,
/// digest, changeset) walks the real tree; the later ones find the row.
///
/// # Errors
///
/// Store errors.
pub(crate) fn view_root(
    store: &Store,
    repo: RepoId,
    digest: &[u8; 32],
    cs: ChangesetId,
    prefixes: &[RepoPath],
) -> Result<TreeId> {
    let cached = store.write(|c| c.get_view_root(repo, digest, cs))?;
    if let Some(cached) = cached {
        return Ok(cached);
    }
    let real = crate::tree::root_tree(store, repo, cs)?;
    let context: Vec<Vec<u8>> = prefixes.iter().map(|p| p.as_bytes().to_vec()).collect();
    let mut memo = Memo::default();
    // A rule set with prefixes covers at least one path of the repo, but it
    // may cover only paths that this changeset's tree does not have: then
    // the view is the empty tree, which is a valid root. The root is never
    // dropped.
    store.write(|c| {
        let view = match project(&mut *c, real, &context, &mut memo)? {
            Some(view) => view,
            None => c.put_tree(&Tree::default())?,
        };
        c.put_view_root(repo, digest, cs, view)?;
        Ok(view)
    })
}

/// Projects the tree `id` under `ctx`, the visible suffixes below it.
/// Returns `None` when nothing below is visible, so the caller hides the
/// entry that holds it.
///
/// # Errors
///
/// Store errors.
fn project(
    c: &mut dyn MetaTx,
    id: TreeId,
    ctx: &Context,
    memo: &mut Memo,
) -> Result<Option<TreeId>> {
    if ctx.is_empty() {
        return Ok(None);
    }
    let key = (id, context_digest(ctx));
    if let Some(hit) = memo.0.get(&key) {
        return Ok(*hit);
    }
    let tree = c.get_tree(id)?;
    let mut kept = std::collections::BTreeMap::new();
    for (name, entry) in &tree.entries {
        match entry {
            // A file is visible when a prefix names it exactly; a prefix
            // can name nothing below a file.
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
                    if let Some(view_id) = project(c, *child, &sub, memo)? {
                        kept.insert(name.clone(), Entry::Dir(view_id));
                    }
                }
            },
        }
    }
    let view = if kept.is_empty() {
        None
    } else {
        Some(c.put_tree(&Tree { entries: kept })?)
    };
    memo.0.insert(key, view);
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
            .write(|c| project(&mut *c, root_id, &ctx, &mut memo))
            .unwrap()
            .expect("the root keeps a visible path");
        let shown = store.write(|c| c.get_tree(view)).unwrap();
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
            .write(|c| project(&mut *c, root_id, &ctx, &mut memo))
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
            .write(|c| project(&mut *c, root_id, &ctx, &mut memo))
            .unwrap()
            .unwrap();
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
        let left_tree = store.write(|c| c.get_tree(*left_view)).unwrap();
        let kept: Vec<Vec<u8>> = left_tree
            .entries
            .keys()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        assert_eq!(kept, [b"a".to_vec()]);
        assert_eq!(memo.0.len(), 2, "the root and the shared child");
        let _ = left;
    }
}
