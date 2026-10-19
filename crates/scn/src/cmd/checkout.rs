//! `checkout`.
//!
//! Uses only the `Objects` service, so a cache tier can serve all of it:
//! resolve the revision, get its root tree, fetch one tree level per batch,
//! then fetch the blobs in batches, in path order. Every object is checked
//! against its ID on arrival. Nothing is written until the whole tree is
//! walked and every name has passed the checkout path rules, and then only
//! into a temp directory that becomes the destination by renames (see
//! [`crate::materialize`]).

use anyhow::{Context, Result, anyhow, bail};
use hashlink::LinkedHashMap;
use scion_core::{BlobId, Entry, FileEntry, FileState, PathElem, RepoPath, Tree, TreeId};
use scion_proto::MAX_OBJECT_REFS;
use scion_proto::runtime::object::{self, Assembler, Header};
use scion_proto::v1::{Encoding, GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectRef};
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::task::Poll;
use tokio::signal::unix::{Signal, SignalKind, signal};

use crate::client::{Ctx, Objects, id_from};
use crate::materialize::{self, Checkout, check_dest, check_name, is_within};
use crate::outln;

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
type Files = BTreeMap<RepoPath, FileEntry>;

/// Writes the files of a changeset into a new or empty directory, and prints
/// how many it wrote. On failure, or on a signal in `STOP_SIGNALS`, the
/// destination is left as it was.
pub async fn checkout(ctx: &mut Ctx, args: CheckoutArgs) -> Result<()> {
    // Listening replaces the default exit, so start it first. On a signal
    // the checkout is dropped at its next wait, and the drop removes what it
    // wrote.
    let mut signals = listen()?;
    let mut run = Box::pin(run(ctx, &args));
    let stop = tokio::select! {
        biased;
        stop = next_signal(&mut signals) => stop,
        done = &mut run => return done,
    };
    // The removal can take long, and it blocks this thread. A second signal
    // reaches this task on a worker thread of the runtime, and exits at once.
    tokio::spawn(async move {
        let again = next_signal(&mut signals).await;
        let what = match materialize::removing() {
            Some(temp) => format!("stopped removing the checkout: left {}", temp.display()),
            None => "nothing was checked out".to_owned(),
        };
        exit(again, &what);
    });
    drop(run);
    exit(stop, "nothing was checked out")
}

/// A signal and its name.
type Stop = (SignalKind, &'static str);

/// The signals that stop a checkout.
const STOP_SIGNALS: [Stop; 3] = [
    (SignalKind::interrupt(), "SIGINT"),
    (SignalKind::terminate(), "SIGTERM"),
    (SignalKind::hangup(), "SIGHUP"),
];

/// Listens for each of [`STOP_SIGNALS`]. Listening replaces the default exit
/// until the process ends.
fn listen() -> Result<Vec<(Stop, Signal)>> {
    STOP_SIGNALS
        .into_iter()
        .map(|stop @ (kind, name)| {
            let signal = signal(kind).with_context(|| format!("cannot listen for {name}"))?;
            Ok((stop, signal))
        })
        .collect()
}

/// Waits for the next of the `signals`.
async fn next_signal(signals: &mut [(Stop, Signal)]) -> Stop {
    std::future::poll_fn(|cx| {
        for (stop, signal) in signals.iter_mut() {
            if let Poll::Ready(Some(())) = signal.poll_recv(cx) {
                return Poll::Ready(*stop);
            }
        }
        Poll::Pending
    })
    .await
}

/// Prints `scn: interrupted by NAME: what`, and dies by the signal: it
/// restores the default action and raises the signal again. So a shell that
/// runs `scn` stops too, as a bash loop does on Ctrl-C, and reports 128 plus
/// the signal number (not the 2 of other errors). After SIGHUP the terminal
/// can be gone, so a failed print changes nothing.
fn exit((kind, name): Stop, what: &str) -> ! {
    let _ = writeln!(io::stderr(), "scn: interrupted by {name}: {what}");
    let number = kind.as_raw_value();
    // For these signals it returns only if it does not know the signal.
    let _ = signal_hook::low_level::emulate_default_handler(number);
    std::process::exit(128 + number)
}

async fn run(ctx: &mut Ctx, args: &CheckoutArgs) -> Result<()> {
    check_dest(&args.into)?;
    let id = ctx.resolve(&args.rev).await?;
    // The server mints the checkout's ticket: the read permission of the
    // whole walk below this root tree, and the root itself, signed together.
    let reply = ctx
        .roots
        .get_root_tree(GetRootTreeRequest {
            repo: ctx.repo.clone(),
            changeset_id: id.as_bytes().to_vec(),
        })
        .await
        .with_context(|| format!("cannot read the tree of {}", args.rev))?
        .into_inner();
    let root = id_from(&reply.tree_id)?;
    let mut grant = Grant {
        repo: &ctx.repo,
        changeset: *id.as_bytes(),
        ticket: reply.ticket,
    };
    let files = walk(
        &mut ctx.objects,
        &mut ctx.roots,
        &mut grant,
        root,
        &args.sparse,
    )
    .await?;
    let mut dest = Checkout::start(&args.into)?;
    write_files(
        &mut ctx.objects,
        &mut ctx.roots,
        &mut grant,
        &mut dest,
        &files,
    )
    .await?;
    dest.finish()?;
    let count = files.len();
    let into = args.into.display();
    if ctx.json {
        let summary =
            json!({ "changeset": id.to_string(), "files": count, "into": into.to_string() });
        outln!("{summary}");
    } else {
        outln!("checked out {count} files from {id} into {into}");
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
    roots: &mut Objects,
    grant: &mut Grant<'_>,
    root: TreeId,
    sparse: &[RepoPath],
) -> Result<Files> {
    let mut files = Files::new();
    let mut found = HashSet::new();
    // A tree's path is its address in the ticket's walk: empty for the root,
    // and the parent path plus the name for the rest.
    let mut level: Vec<(Option<RepoPath>, TreeId, Vec<u8>)> = vec![(None, root, Vec::new())];
    while !level.is_empty() {
        let mut trees = HashMap::new();
        let ids = level
            .iter()
            .map(|(_, id, path)| (*id.as_bytes(), path.clone()));
        fetch(
            objects,
            roots,
            ObjectKind::Tree,
            ids,
            grant,
            |id, data| {
                let id = TreeId::from_bytes(id);
                trees.insert(id, decode_tree(id, &data)?);
                Ok(())
            },
            |_| None,
        )
        .await?;
        let mut next = Vec::new();
        for (dir, id, tree_path) in level {
            let tree = trees.get(&id).context("server skipped a tree")?;
            for (name, entry) in tree.entries() {
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
                // The ref path of an entry is its parent tree's path plus
                // its name, not its whole local path.
                let mut ref_path = tree_path.clone();
                if !ref_path.is_empty() {
                    ref_path.extend_from_slice(b"/");
                }
                ref_path.extend_from_slice(name.as_bytes());
                if sparse.contains(&path) {
                    found.insert(path.clone());
                }
                if let Entry::File(file) = entry
                    && let FileState::Gitlink { commit } = file.state()
                {
                    eprintln!("note: skipping submodule {path:?} at {commit}");
                    continue;
                }
                if let Err(why) = check_name(name.as_bytes()) {
                    bail!("refusing to write {path:?}: {why}");
                }
                match entry {
                    Entry::Dir(id) => next.push((Some(path), *id, ref_path)),
                    Entry::File(file) => {
                        files.insert(path, *file);
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
    roots: &mut Objects,
    grant: &mut Grant<'_>,
    dest: &mut Checkout,
    files: &Files,
) -> Result<()> {
    // A blob may back several files.
    let mut by_blob: HashMap<BlobId, Vec<(&RepoPath, &FileEntry)>> = HashMap::new();
    for (path, file) in files {
        if let Some(blob) = file.state().blob() {
            by_blob.entry(blob).or_default().push((path, file));
        }
    }
    let ids = files.iter().filter_map(|(path, file)| {
        // The path of a file that the blob backs is the address that the
        // ticket's walk reaches.
        file.state()
            .blob()
            .map(|blob| (*blob.as_bytes(), path.as_bytes().to_vec()))
    });
    let size_of = |id: [u8; 32]| {
        by_blob
            .get(&BlobId::from_bytes(id))
            .and_then(|files| files.first())
            .map(|(_, file)| file.size())
    };
    fetch(
        objects,
        roots,
        ObjectKind::Blob,
        ids,
        grant,
        |id, data| {
            let id = BlobId::from_bytes(id);
            check_blob(id, &data)?;
            for &(path, file) in by_blob.get(&id).into_iter().flatten() {
                // The tree claimed a size for this blob; the bytes that arrived
                // must have it, or the checkout would write a lie.
                if file.size() != data.len() as u64 {
                    bail!(
                        "{path:?}: {} bytes arrived, the tree says {}",
                        data.len(),
                        file.size()
                    );
                }
                match file.state() {
                    FileState::Regular { executable, .. } => dest.file(path, executable, &data)?,
                    FileState::Symlink { .. } => dest.symlink(path, &data)?,
                    // A gitlink has no blob, so it never gets here.
                    FileState::Gitlink { .. } => {}
                }
            }
            Ok(())
        },
        size_of,
    )
    .await
}

/// Fetches objects of one kind, each once and in the order given, and calls
/// `got` with each object's ID and bytes as it arrives, unpacked within the
/// object limit. Each ref carries the path that the ticket's walk reaches;
/// a path that the ticket does not reach is refused. A reply may stop
/// early; the rest are asked for again.
///
/// # Errors
///
/// Fails if a call fails, if the server sends an object that was not asked
/// for or none of those asked for, if an object's parts or encoding are bad,
/// or if `got` fails. On `ticket expired`, the ticket is minted again from
/// the server, once per call, and the call is retried.
async fn fetch(
    objects: &mut Objects,
    roots: &mut Objects,
    kind: ObjectKind,
    ids: impl IntoIterator<Item = ([u8; 32], Vec<u8>)>,
    grant: &mut Grant<'_>,
    mut got: impl FnMut([u8; 32], Vec<u8>) -> Result<()>,
    size_of: impl Fn([u8; 32]) -> Option<u64>,
) -> Result<()> {
    // The path of each id, in the order of the first ask.
    let mut want: LinkedHashMap<[u8; 32], Vec<u8>> = ids.into_iter().collect();
    let mut minted = false;
    while !want.is_empty() {
        let refs = want
            .iter()
            .take(MAX_OBJECT_REFS)
            .map(|(id, path)| ObjectRef {
                kind: kind.into(),
                id: id.to_vec(),
                path: path.clone(),
            })
            .collect::<Vec<ObjectRef>>();
        // Blobs come as the zstd files that the server and the daemon store,
        // and are unpacked here, once.
        let call = objects
            .get_objects(GetObjectsRequest {
                refs,
                accept: vec![Encoding::Zstd.into()],
                ticket: grant.ticket.clone(),
            })
            .await;
        let stream = match call {
            Ok(stream) => stream,
            Err(status) if expired(&status) && !minted => {
                // The checkout outlived its 15-minute ticket: mint a new
                // one for the same root tree, once per call, and continue.
                minted = true;
                grant.ticket = mint(roots, grant.repo, &grant.changeset).await?;
                continue;
            }
            Err(status) => return Err(status).context("cannot fetch objects"),
        };
        let mut stream = stream.into_inner();
        let before = want.len();
        while let Some(first) = stream.message().await.context("cannot fetch objects")? {
            let id: [u8; 32] = id_from(&first.id)?;
            if first.kind != i32::from(kind) || want.remove(&id).is_none() {
                bail!("server sent an object that was not asked for");
            }
            let name = match kind {
                ObjectKind::Tree => format!("tree {}", TreeId::from_bytes(id)),
                _ => format!("blob {}", BlobId::from_bytes(id)),
            };
            let bad = |why| anyhow!("server sent a bad object for {name}: {why}");
            let header = Header::of(&first, true).map_err(bad)?;
            // The tree said how big this blob is. A first part whose size
            // disagrees ends the fetch before any part of the body reads.
            if let Some(want) = size_of(id)
                && header.size as u64 != want
            {
                bail!(
                    "server sent {name}: {} bytes, the tree says {want}",
                    header.size
                );
            }
            let mut object = Assembler::new(header, first);
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

/// Is this a failure that a fresh ticket can fix?
/// The read permission of one checkout: the server's ticket over its root
/// tree, and what mints a fresh ticket when this one expires.
struct Grant<'a> {
    repo: &'a str,
    changeset: [u8; 32],
    ticket: Vec<u8>,
}

fn expired(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::PermissionDenied && status.message().contains("ticket expired")
}

/// Mints a ticket for the root tree of `changeset` on the server: the same
/// walk, refreshed.
async fn mint(roots: &mut Objects, repo: &str, changeset: &[u8; 32]) -> Result<Vec<u8>> {
    let reply = roots
        .get_root_tree(GetRootTreeRequest {
            repo: repo.to_owned(),
            changeset_id: changeset.to_vec(),
        })
        .await
        .context("cannot mint a fresh ticket")?
        .into_inner();
    if reply.ticket.is_empty() {
        bail!("cannot mint a fresh ticket: the server sent no ticket");
    }
    Ok(reply.ticket)
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
        let tree = Tree::new(
            [(
                PathElem::new("hello.txt").unwrap(),
                Entry::File(FileEntry::new(
                    FileState::Regular {
                        content: blob,
                        executable: false,
                    },
                    b"hello\n".len() as u64,
                )),
            )]
            .into(),
        )
        .expect("tree fits the format");
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
