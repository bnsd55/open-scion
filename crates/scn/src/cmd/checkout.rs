//! `checkout`.
//!
//! Uses only the `Objects` service, so a cache tier can serve all of it:
//! resolve the revision, get its root tree, fetch one tree level per batch,
//! then fetch the blobs in batches, in path order. Every object is checked
//! against its ID on arrival. Nothing is written until the whole tree is
//! walked and every name has passed the checkout path rules, and then only
//! into a temp directory that becomes the destination by renames (see
//! [`crate::materialize`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use hashlink::LinkedHashSet;
use scion_core::{BlobId, Entry, FileState, PathElem, RepoPath, Tree, TreeId};
use scion_proto::MAX_OBJECT_REFS;
use scion_proto::object::{self, Assembler, Header};
use scion_proto::v1::{Encoding, GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectRef};
use serde_json::json;

use crate::client::{Ctx, Objects, id_from};
use crate::materialize::{Checkout, check_dest, check_name, is_within};

/// Arguments of `scn checkout`.
#[derive(clap::Args)]
pub struct CheckoutArgs {
    /// Ref name or changeset ID.
    rev: String,
    /// Write only this directory and everything below it. Repeat for more.
    #[arg(long, value_name = "DIR", value_parser = parse_dir)]
    sparse: Vec<RepoPath>,
    /// Directory to write into. It must not exist, or be empty. An empty
    /// directory is kept, and must be yours: the checkout is built inside it
    /// and moved up. A new one is built next to it and renamed into place,
    /// so its parent must be writable. The empty one, or the new one's
    /// parent, must not be writable by all users unless it has the sticky
    /// bit (as /tmp has). Members of a group that can write it are trusted.
    #[arg(long, value_name = "PATH")]
    into: PathBuf,
}

fn parse_dir(dir: &str) -> Result<RepoPath> {
    RepoPath::new(dir.trim_end_matches('/')).with_context(|| format!("bad path {dir:?}"))
}

/// Files to write, in path order. Never holds a gitlink.
type Files = BTreeMap<RepoPath, FileState>;

/// Writes the files of a changeset into a new or empty directory, and prints
/// how many it wrote. On failure, or on Ctrl-C, the destination is left as
/// it was.
pub async fn checkout(ctx: &mut Ctx, args: CheckoutArgs) -> Result<()> {
    // Listening for Ctrl-C replaces the default exit, so start it first. On
    // Ctrl-C the checkout is dropped at its next wait, and the drop removes
    // what it wrote. If listening fails, that branch is off and Ctrl-C exits
    // as usual.
    tokio::select! {
        biased;
        Ok(()) = tokio::signal::ctrl_c() => bail!("interrupted: nothing was checked out"),
        done = run(ctx, &args) => done,
    }
}

async fn run(ctx: &mut Ctx, args: &CheckoutArgs) -> Result<()> {
    check_dest(&args.into)?;
    let id = ctx.resolve(&args.rev).await?;
    let reply = ctx
        .objects
        .get_root_tree(GetRootTreeRequest {
            repo: ctx.repo.clone(),
            changeset_id: id.as_bytes().to_vec(),
        })
        .await
        .with_context(|| format!("cannot read the tree of {}", args.rev))?
        .into_inner();
    let root = id_from(&reply.tree_id)?;
    let files = walk(&mut ctx.objects, &ctx.repo, root, &args.sparse).await?;
    let mut dest = Checkout::start(&args.into)?;
    write_files(&mut ctx.objects, &ctx.repo, &mut dest, &files).await?;
    dest.finish()?;
    let count = files.len();
    let into = args.into.display();
    if ctx.json {
        let summary =
            json!({ "changeset": id.to_string(), "files": count, "into": into.to_string() });
        println!("{summary}");
    } else {
        println!("checked out {count} files from {id} into {into}");
    }
    Ok(())
}

/// Walks the tree from `root`, one level per batch, and returns the regular
/// files and symlinks inside the sparse paths (all of them if there are
/// none). Gitlinks are skipped with a note.
///
/// # Errors
///
/// Fails if a name breaks the checkout path rules, if a sparse path is not in
/// the tree, or if a fetch fails. Names that fold together on the target
/// filesystem are caught when written.
async fn walk(
    objects: &mut Objects,
    repo: &str,
    root: TreeId,
    sparse: &[RepoPath],
) -> Result<Files> {
    let mut files = Files::new();
    let mut found = HashSet::new();
    let mut level: Vec<(Option<RepoPath>, TreeId)> = vec![(None, root)];
    while !level.is_empty() {
        let mut trees = HashMap::new();
        let ids = level.iter().map(|(_, id)| *id.as_bytes());
        fetch(objects, repo, ObjectKind::Tree, ids, |id, data| {
            let id = TreeId::from_bytes(id);
            trees.insert(id, decode_tree(id, &data)?);
            Ok(())
        })
        .await?;
        let mut next = Vec::new();
        for (dir, id) in level {
            let tree = trees.get(&id).context("server skipped a tree")?;
            for (name, entry) in &tree.entries {
                let path = join(dir.as_ref(), name)?;
                let bytes = path.as_bytes();
                let inside =
                    sparse.is_empty() || sparse.iter().any(|s| is_within(bytes, s.as_bytes()));
                let keep = match entry {
                    Entry::Dir(_) => {
                        inside || sparse.iter().any(|s| is_within(s.as_bytes(), bytes))
                    }
                    Entry::File(_) => inside,
                };
                if !keep {
                    continue;
                }
                if sparse.contains(&path) {
                    found.insert(path.clone());
                }
                if let Entry::File(FileState::Gitlink { commit }) = entry {
                    eprintln!("note: skipping submodule {path:?} at {commit}");
                    continue;
                }
                if let Err(why) = check_name(name.as_bytes()) {
                    bail!("refusing to write {path:?}: {why}");
                }
                match entry {
                    Entry::Dir(id) => next.push((Some(path), *id)),
                    Entry::File(state) => {
                        files.insert(path, *state);
                    }
                }
            }
        }
        level = next;
    }
    if let Some(missing) = sparse.iter().find(|s| !found.contains(*s)) {
        bail!("sparse path {missing:?} is not in the tree");
    }
    Ok(files)
}

/// Fetches the blobs of `files` in batches, in the order of the first file
/// that uses each, and writes each file as its blob arrives. So the files of
/// a directory are written together, and [`Checkout`] opens it about once: a
/// blob that backs files in several directories writes them all on arrival.
///
/// # Errors
///
/// Fails if a fetch or a write fails.
async fn write_files(
    objects: &mut Objects,
    repo: &str,
    dest: &mut Checkout,
    files: &Files,
) -> Result<()> {
    // A blob may back several files.
    let mut by_blob: HashMap<BlobId, Vec<(&RepoPath, &FileState)>> = HashMap::new();
    for (path, state) in files {
        if let Some(blob) = state.blob() {
            by_blob.entry(blob).or_default().push((path, state));
        }
    }
    let ids = files
        .values()
        .filter_map(FileState::blob)
        .map(|id| *id.as_bytes());
    fetch(objects, repo, ObjectKind::Blob, ids, |id, data| {
        let id = BlobId::from_bytes(id);
        check_blob(id, &data)?;
        for &(path, state) in by_blob.get(&id).into_iter().flatten() {
            match *state {
                FileState::Regular { executable, .. } => dest.file(path, executable, &data)?,
                FileState::Symlink { .. } => dest.symlink(path, &data)?,
                // A gitlink has no blob, so it never gets here.
                FileState::Gitlink { .. } => {}
            }
        }
        Ok(())
    })
    .await
}

/// Fetches objects of one kind, each once and in the order given, and calls
/// `got` with each object's ID and bytes as it arrives, unpacked within the
/// object limit. A reply may stop early; the rest are asked for again.
///
/// # Errors
///
/// Fails if a call fails, if the server sends an object that was not asked
/// for or none of those asked for, if an object's parts or encoding are bad,
/// or if `got` fails.
async fn fetch(
    objects: &mut Objects,
    repo: &str,
    kind: ObjectKind,
    ids: impl IntoIterator<Item = [u8; 32]>,
    mut got: impl FnMut([u8; 32], Vec<u8>) -> Result<()>,
) -> Result<()> {
    let mut want: LinkedHashSet<[u8; 32]> = ids.into_iter().collect();
    while !want.is_empty() {
        let refs = want
            .iter()
            .take(MAX_OBJECT_REFS)
            .map(|id| ObjectRef {
                kind: kind.into(),
                id: id.to_vec(),
            })
            .collect();
        // Blobs come as the zstd files that the server and the daemon store,
        // and are unpacked here, once.
        let mut stream = objects
            .get_objects(GetObjectsRequest {
                repo: repo.to_owned(),
                refs,
                accept: vec![Encoding::Zstd.into()],
            })
            .await
            .context("cannot fetch objects")?
            .into_inner();
        let before = want.len();
        while let Some(first) = stream.message().await.context("cannot fetch objects")? {
            let id: [u8; 32] = id_from(&first.id)?;
            if first.kind != i32::from(kind) || !want.remove(&id) {
                bail!("server sent an object that was not asked for");
            }
            let name = match kind {
                ObjectKind::Tree => format!("tree {}", TreeId::from_bytes(id)),
                _ => format!("blob {}", BlobId::from_bytes(id)),
            };
            let bad = |why| anyhow!("server sent a bad object for {name}: {why}");
            let mut object = Assembler::new(Header::of(&first, true).map_err(bad)?, first);
            while !object.is_done() {
                let part = stream.message().await.context("cannot fetch objects")?;
                let part = part.context("the reply ended in the middle of an object")?;
                object.push(&part).map_err(bad)?;
            }
            let (header, data) = object.finish();
            got(id, object::decode(&header, data).map_err(bad)?)?;
        }
        if want.len() == before {
            bail!("server sent none of the requested objects");
        }
    }
    Ok(())
}

/// Decodes a tree sent for `id`, and checks that it is that tree.
fn decode_tree(id: TreeId, data: &[u8]) -> Result<Tree> {
    let (tree, actual) =
        Tree::decode_with_id(data).with_context(|| format!("server sent a bad tree for {id}"))?;
    if actual != id {
        bail!("server sent tree {actual} for {id}");
    }
    Ok(tree)
}

/// Checks that content sent for blob `id` is that blob.
fn check_blob(id: BlobId, data: &[u8]) -> Result<()> {
    let actual = BlobId::of(data);
    if actual != id {
        bail!("server sent blob {actual} for {id}");
    }
    Ok(())
}

/// `dir/name`, or `name` at the root.
fn join(dir: Option<&RepoPath>, name: &PathElem) -> Result<RepoPath> {
    let bytes = match dir {
        Some(dir) => [dir.as_bytes(), b"/", name.as_bytes()].concat(),
        None => name.as_bytes().to_vec(),
    };
    RepoPath::new(bytes).map_err(|e| anyhow!("a path in {dir:?} is too long or too deep: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn objects_with_the_wrong_bytes_are_refused() {
        let blob = BlobId::of(b"hello\n");
        let tree = Tree {
            entries: [(
                PathElem::new("hello.txt").unwrap(),
                Entry::File(FileState::Regular {
                    content: blob,
                    executable: false,
                }),
            )]
            .into(),
        };
        let (bytes, id) = tree.encode_with_id();
        assert_eq!(decode_tree(id, &bytes).unwrap(), tree);
        check_blob(blob, b"hello\n").unwrap();

        // Valid objects, but not the ones asked for.
        let other = Tree::default();
        assert!(decode_tree(id, &other.encode()).is_err());
        assert!(decode_tree(other.id(), &bytes).is_err());
        assert!(check_blob(blob, b"hello!\n").is_err());
        assert!(check_blob(blob, b"").is_err());
    }

    #[test]
    fn a_sparse_path_selects_itself_and_what_is_below_it() {
        let dir = b"src/a";
        for (path, within) in [
            ("src/a", true),
            ("src/a/b", true),
            ("src/ab", false),
            ("src", false),
            ("srC/a", false),
        ] {
            assert_eq!(is_within(path.as_bytes(), dir), within, "{path}");
        }
    }
}
