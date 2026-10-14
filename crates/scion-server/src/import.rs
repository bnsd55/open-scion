//! One-way import from a local git repository.
//!
//! Each git commit becomes one changeset. Its parents are the changesets of
//! the git parents, in git order. Its `changes` are the git tree diff against
//! the first parent. The import records each root tree as it goes, and checks
//! a sample of them against git before the batch that holds them commits.
//!
//! Git headers that Scion does not model (`encoding`, `gpgsig`, `mergetag`)
//! are dropped, so a changeset ID is not a function of the git object name.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

use gix::ObjectId;
use gix::actor::SignatureRef;
use hashlink::LruCache;
use rusqlite::{Connection, params};
use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileState, GitOid, IdentityTime, PathElem, RepoPath,
    TreeId,
};

use crate::db::{self, RepoId};
use crate::error::{Error, Result};
use crate::store::{BlobBatch, BlobStatus, MAX_OBJECT_BYTES, Store};
use crate::tree;

/// `git_import.git_algo` for SHA-1 object names.
const SHA1: i64 = 1;
/// Commits per write transaction.
const BATCH: usize = 1000;
/// Cache for decoded git objects. A tree diff re-reads the trees that the
/// previous commit's diff just read.
const OBJECT_CACHE_BYTES: usize = 64 << 20;
/// Most entries in each blob cache of an import. An entry takes at most about
/// 100 bytes, so a full cache takes at most about 6.5 MB.
const BLOB_CACHE_ENTRIES: usize = 1 << 16;

/// What to import.
#[derive(Clone, Debug)]
pub struct ImportOptions {
    /// Git revision to import, for example `HEAD` or `main`.
    pub rev: String,
    /// Scion ref to point at the imported tip.
    pub target_ref: String,
    /// How many non-root, non-merge commits to check against git, besides
    /// every root and merge. Only commits that this run imports are checked.
    pub verify_sample: usize,
}

/// What an import did.
#[derive(Clone, Debug)]
pub struct ImportReport {
    /// Commits reachable from the revision.
    pub commits: usize,
    /// Commits newly stored (the rest were imported before).
    pub imported: usize,
    /// Commits whose Scion tree was checked against the git tree.
    pub verified: usize,
    /// The changeset for the imported revision.
    pub tip: ChangesetId,
}

/// Imports every commit reachable from `opts.rev` and points
/// `opts.target_ref` at the tip. Safe to repeat.
///
/// Commits are written in batches, parents first. A batch commits only after
/// its sampled commits match git, so an interrupted or failed import keeps
/// the batches before it and a rerun continues from there.
///
/// # Errors
///
/// [`Error::Git`] if git cannot be read, or a path or file is not valid in
/// Scion. [`Error::Corrupt`] if a verified commit's tree differs from git's or
/// names a blob that is not stored intact. [`Error::FailedPrecondition`] if
/// `opts.target_ref` points at a changeset that the tip does not descend
/// from; the commits stay imported and the ref is left alone.
pub fn import_git(
    store: &Store,
    repo: &str,
    git_dir: &Path,
    opts: &ImportOptions,
) -> Result<ImportReport> {
    import_with_cache(store, repo, git_dir, opts, BLOB_CACHE_ENTRIES)
}

/// [`import_git`] with at most `cache_entries` entries in each blob cache.
fn import_with_cache(
    store: &Store,
    repo: &str,
    git_dir: &Path,
    opts: &ImportOptions,
    cache_entries: usize,
) -> Result<ImportReport> {
    let started = Instant::now();
    let mut git = gix::open(git_dir)
        .map_err(|e| Error::Git(format!("cannot open {}: {e}", git_dir.display())))?;
    if git.object_hash() != gix::hash::Kind::Sha1 {
        return Err(Error::Git(format!(
            "{}: only SHA-1 repositories are supported",
            git_dir.display()
        )));
    }
    git.object_cache_size_if_unset(OBJECT_CACHE_BYTES);
    let tip = git
        .rev_parse_single(opts.rev.as_str())
        .and_then(|id| id.object())
        .and_then(gix::Object::peel_to_commit)
        .map_err(|e| Error::Git(format!("cannot resolve {:?} to a commit: {e}", opts.rev)))?
        .id;

    let repo_id = store.write(|c| db::create_repo(c, repo))?;
    let mut known = store.read(|c| imported_commits(c, repo_id))?;
    let plan = plan(&git, tip, &known, opts.verify_sample)?;
    tracing::info!(
        reachable = plan.reachable,
        new = plan.todo.len(),
        to_verify = plan.verify.len(),
        "git import planned"
    );

    let mut importer = Importer {
        git: &git,
        blobs: LruCache::new(cache_entries),
        put: Vec::new(),
    };
    let mut checker = Checker {
        git: &git,
        store,
        equal: HashSet::new(),
        blobs: LruCache::new(cache_entries),
        intact: LruCache::new(cache_entries),
    };
    let mut done = 0;
    for batch in plan.todo.chunks(BATCH) {
        // Blobs are made durable first, so no stored metadata points at a
        // blob that a crash could lose.
        let mut blobs = store.blobs().batch();
        let mut prepared = Vec::with_capacity(batch.len());
        for &commit in batch {
            let (cs, git_tree) = importer.changeset(commit, &known, &mut blobs)?;
            known.insert(commit, cs.id());
            prepared.push((commit, cs, git_tree));
        }
        blobs.sync()?;
        // A put writes or reads back its blob, so the checker reads back only
        // blobs from earlier runs, or blobs that its cache dropped.
        checker
            .intact
            .extend(importer.put.drain(..).map(|blob| (blob, ())));
        store.write(|c| {
            for (commit, cs, git_tree) in &prepared {
                let root = record(c, repo_id, *commit, cs)?;
                if plan.verify.contains(commit) {
                    checker.check(c, *commit, *git_tree, root)?;
                }
            }
            Ok(())
        })?;
        done += batch.len();
        tracing::info!(
            done,
            total = plan.todo.len(),
            commits_per_sec = per_sec(done, started),
            "importing git commits"
        );
    }

    // The tip was imported before, or planned and imported above.
    let tip = known
        .get(&tip)
        .copied()
        .ok_or_else(|| Error::Corrupt(format!("commit {tip} was not imported")))?;
    store.write(|c| point_ref(c, repo_id, &opts.target_ref, tip))?;
    // Every planned check ran, or the import failed above.
    let report = ImportReport {
        commits: plan.reachable,
        imported: plan.todo.len(),
        verified: plan.verify.len(),
        tip,
    };
    tracing::info!(
        commits = report.commits,
        imported = report.imported,
        verified = report.verified,
        secs = started.elapsed().as_secs(),
        %tip,
        "git import done"
    );
    Ok(report)
}

/// What to import, in order.
struct Plan {
    /// How many commits are reachable from the tip.
    reachable: usize,
    /// Commits not imported yet, parents before children.
    todo: Vec<ObjectId>,
    /// Commits in `todo` to check against git.
    verify: HashSet<ObjectId>,
}

/// Walks every commit reachable from `tip` and orders the new ones parents
/// first. Picks every new root and merge, plus `sample` evenly spaced other
/// new commits, for verification.
fn plan(
    git: &gix::Repository,
    tip: ObjectId,
    known: &HashMap<ObjectId, ChangesetId>,
    sample: usize,
) -> Result<Plan> {
    /// Depth-first walk with an explicit stack, so deep history cannot
    /// overflow the call stack.
    enum Step {
        /// Push the commit's parents, then its `Leave`.
        Enter(ObjectId),
        /// Every parent is ordered; order this commit. Roots and merges are
        /// always checked.
        Leave {
            commit: ObjectId,
            always_check: bool,
        },
    }

    let mut seen = HashSet::new();
    let mut todo = Vec::new();
    let mut verify = HashSet::new();
    let mut ordinary = Vec::new();
    let mut stack = vec![Step::Enter(tip)];
    while let Some(step) = stack.pop() {
        match step {
            Step::Enter(commit) => {
                if !seen.insert(commit) {
                    continue;
                }
                let parents: Vec<ObjectId> = find_commit(git, commit)?
                    .parent_ids()
                    .map(gix::Id::detach)
                    .collect();
                // A batch commits only after its parents, so an imported
                // commit's ancestors are imported too. They are still walked,
                // to count them.
                if !known.contains_key(&commit) {
                    stack.push(Step::Leave {
                        commit,
                        always_check: parents.len() != 1,
                    });
                }
                // Reversed, so the first parent's history is ordered first.
                let unseen = parents.into_iter().rev().filter(|p| !seen.contains(p));
                stack.extend(unseen.map(Step::Enter));
            }
            Step::Leave {
                commit,
                always_check,
            } => {
                if always_check {
                    verify.insert(commit);
                } else {
                    ordinary.push(commit);
                }
                todo.push(commit);
            }
        }
    }
    // Evenly spaced, ending at the newest.
    let picks = sample.min(ordinary.len());
    verify.extend((1..=picks).map(|i| ordinary[i * ordinary.len() / picks - 1]));
    Ok(Plan {
        reachable: seen.len(),
        todo,
        verify,
    })
}

/// Stores one changeset, its git mapping and its root tree. Returns the root
/// tree.
fn record(c: &Connection, repo: RepoId, commit: ObjectId, cs: &Changeset) -> Result<TreeId> {
    let (id, root) = tree::store_changeset(c, repo, cs).map_err(|e| match e {
        Error::Apply(e) => Error::Corrupt(format!(
            "commit {commit}: changes do not apply to the first parent: {e}"
        )),
        other => other,
    })?;
    c.prepare_cached(
        "INSERT OR IGNORE INTO git_import (repo_id, git_algo, git_oid, cs)
         VALUES (?1, ?2, ?3, ?4)",
    )?
    .execute(params![repo.0, SHA1, commit.as_slice(), id.as_bytes()])?;
    Ok(root)
}

/// Every git commit already imported into `repo`.
fn imported_commits(c: &Connection, repo: RepoId) -> Result<HashMap<ObjectId, ChangesetId>> {
    let mut stmt =
        c.prepare("SELECT git_oid, cs FROM git_import WHERE repo_id = ?1 AND git_algo = ?2")?;
    let rows = stmt.query_map(params![repo.0, SHA1], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    let mut out = HashMap::new();
    for row in rows {
        let (oid, cs) = row?;
        let corrupt = || Error::Corrupt("git_import row".to_owned());
        let oid = ObjectId::try_from(oid.as_slice()).map_err(|_| corrupt())?;
        let cs = ChangesetId::try_from(cs.as_slice()).map_err(|_| corrupt())?;
        out.insert(oid, cs);
    }
    Ok(out)
}

/// Points `name` at `tip`, creating it if missing. Leaves it alone if it is
/// already there, so a repeated import changes nothing.
///
/// Moves an existing ref only forward, to a descendant of where it points.
/// Anything else would drop the changesets on the ref that git does not
/// have, such as lands made after an earlier import.
fn point_ref(c: &Connection, repo: RepoId, name: &str, tip: ChangesetId) -> Result<()> {
    let current = db::get_ref(c, repo, name)?;
    if let Some(current) = current {
        // Already there, or the ref already contains the tip (changes were
        // landed on top of an earlier import): nothing to do.
        if current.target == tip || db::is_ancestor(c, repo, tip, current.target)? {
            return Ok(());
        }
        if !db::is_ancestor(c, repo, current.target, tip)? {
            return Err(Error::FailedPrecondition(format!(
                "ref {name:?} has diverged from git: it points at {}, which is not \
                 an ancestor of the imported tip {tip}; import into another ref",
                current.target
            )));
        }
    }
    // The caller's write transaction holds the database lock, so the version
    // read above is still current.
    if db::cas_ref(c, repo, name, current.map(|r| r.version), tip)? {
        Ok(())
    } else {
        Err(Error::Corrupt(format!(
            "ref {name:?} changed inside a write transaction"
        )))
    }
}

/// Turns git commits into changesets.
struct Importer<'a> {
    git: &'a gix::Repository,
    /// Blobs recently stored in this run, so most are read from git once. A
    /// miss costs a git read and a read-back of the stored blob, since
    /// `BlobBatch::put` keeps a stored blob only after checking it.
    blobs: LruCache<ObjectId, BlobId>,
    /// Blobs put since the checker last took them.
    put: Vec<BlobId>,
}

impl Importer<'_> {
    /// Builds the changeset for `commit` and writes its new blobs to `batch`.
    /// Returns it with the commit's git tree. Every parent must be in `known`.
    fn changeset(
        &mut self,
        commit: ObjectId,
        known: &HashMap<ObjectId, ChangesetId>,
        batch: &mut BlobBatch<'_>,
    ) -> Result<(Changeset, ObjectId)> {
        let git = self.git;
        let object = find_commit(git, commit)?;
        let decoded = object
            .decode()
            .map_err(|e| Error::Git(format!("cannot decode commit {commit}: {e}")))?;
        let parents = decoded
            .parents()
            .map(|parent| {
                known.get(&parent).copied().ok_or_else(|| {
                    Error::Corrupt(format!(
                        "commit {commit}: parent {parent} is not imported, but a descendant is"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let base = decoded
            .parents()
            .next()
            .map(|parent| {
                find_commit(git, parent)?
                    .tree_id()
                    .map(gix::Id::detach)
                    .map_err(|e| Error::Git(format!("commit {parent}: no tree: {e}")))
            })
            .transpose()?;
        let git_tree = decoded.tree();
        let changeset = Changeset {
            parents,
            author: identity(commit, "author", decoded.author())?,
            committer: Some(identity(commit, "committer", decoded.committer())?),
            message: decoded.message.to_vec(),
            changes: self.diff(commit, base, git_tree, batch)?,
        };
        Ok((changeset, git_tree))
    }

    /// The files that differ between git trees `from` (`None` is empty) and
    /// `to`. Subtrees with the same ID on both sides are skipped.
    fn diff(
        &mut self,
        commit: ObjectId,
        from: Option<ObjectId>,
        to: ObjectId,
        batch: &mut BlobBatch<'_>,
    ) -> Result<BTreeMap<RepoPath, Option<FileState>>> {
        let mut changes = BTreeMap::new();
        // (directory path, tree before, tree after). `None` is a directory
        // that does not exist on that side.
        let mut stack = vec![(Vec::new(), from, Some(to))];
        while let Some((dir, from, to)) = stack.pop() {
            if from == to {
                continue;
            }
            let from = read_tree(self.git, commit, from)?;
            let to = read_tree(self.git, commit, to)?;
            for (name, before, after) in pair_up(&from, &to) {
                if before == after {
                    continue;
                }
                let path = child_path(commit, &dir, name)?;
                let (dir_before, dir_after) = (subtree(before), subtree(after));
                if dir_before.is_some() || dir_after.is_some() {
                    stack.push((path.as_bytes().to_vec(), dir_before, dir_after));
                }
                // Puts and deletes are file-level. `tree::apply` runs every
                // delete first, so a file can become a directory and back.
                match (before, after) {
                    (_, Some(GitEntry::File(kind, id))) => {
                        let state =
                            file_state(*kind, *id, |id| self.blob(commit, &path, id, batch))?;
                        changes.insert(path, Some(state));
                    }
                    (Some(GitEntry::File(..)), _) => {
                        changes.insert(path, None);
                    }
                    _ => {}
                }
            }
        }
        Ok(changes)
    }

    /// Writes the git blob for `path` in `commit` to `batch` and returns its
    /// Scion ID.
    fn blob(
        &mut self,
        commit: ObjectId,
        path: &RepoPath,
        oid: ObjectId,
        batch: &mut BlobBatch<'_>,
    ) -> Result<BlobId> {
        cached(&mut self.blobs, oid, || {
            let blob = read_blob(self.git, oid)?;
            if blob.data.len() > MAX_OBJECT_BYTES {
                return Err(Error::Git(format!(
                    "commit {commit}: \"{}\" is larger than {MAX_OBJECT_BYTES} bytes, \
                     the most Scion stores",
                    path.as_bytes().escape_ascii()
                )));
            }
            let id = batch.put(&blob.data)?;
            self.put.push(id);
            Ok(id)
        })
    }
}

/// Checks Scion trees against git trees.
struct Checker<'a> {
    git: &'a gix::Repository,
    store: &'a Store,
    /// Tree pairs already found equal, so shared subtrees are checked once.
    equal: HashSet<(ObjectId, TreeId)>,
    /// Blob IDs hashed from git content, apart from the importer's cache.
    blobs: LruCache<ObjectId, BlobId>,
    /// Stored blobs recently known to be intact: put in this run, or read
    /// back by an earlier check. A miss costs a read-back of the stored blob.
    intact: LruCache<BlobId, ()>,
}

impl Checker<'_> {
    /// Fails unless the Scion tree `scion` has exactly the names, kinds,
    /// flags and content of the git tree `git_tree`.
    fn check(
        &mut self,
        c: &Connection,
        commit: ObjectId,
        git_tree: ObjectId,
        scion: TreeId,
    ) -> Result<()> {
        let mut stack = vec![(Vec::new(), git_tree, scion)];
        while let Some((dir, git_id, scion_id)) = stack.pop() {
            if !self.equal.insert((git_id, scion_id)) {
                continue;
            }
            let want = read_tree(self.git, commit, Some(git_id))?;
            let have: BTreeMap<Vec<u8>, Entry> = db::get_tree(c, scion_id)?
                .entries
                .into_iter()
                .map(|(name, entry)| (name.as_bytes().to_vec(), entry))
                .collect();
            for (name, want, have) in pair_up(&want, &have) {
                let path = join(&dir, name);
                let differs = |what: String| {
                    Error::Corrupt(format!(
                        "commit {commit}: imported tree differs from git at \"{}\": {what}",
                        path.escape_ascii()
                    ))
                };
                match (want, have) {
                    (Some(GitEntry::Dir(git_id)), Some(Entry::Dir(scion_id))) => {
                        stack.push((path, *git_id, *scion_id));
                    }
                    (Some(GitEntry::File(kind, oid)), Some(Entry::File(state))) => {
                        let expected = file_state(*kind, *oid, |oid| self.blob(oid))?;
                        if expected != *state {
                            return Err(differs(format!("git {expected:?}, Scion {state:?}")));
                        }
                        if let Some(blob) = state.blob()
                            && self.intact.get(&blob).is_none()
                        {
                            let problem = match self.store.blobs().verify(blob)? {
                                BlobStatus::Valid => {
                                    self.intact.insert(blob, ());
                                    continue;
                                }
                                BlobStatus::Missing => "is not stored",
                                BlobStatus::Corrupt => "is corrupt",
                            };
                            return Err(differs(format!("blob {blob} {problem}")));
                        }
                    }
                    (Some(_), Some(_)) => {
                        return Err(differs(
                            "a file on one side, a directory on the other".into(),
                        ));
                    }
                    (Some(_), None) => return Err(differs("missing in Scion".into())),
                    (None, _) => return Err(differs("not in git".into())),
                }
            }
        }
        Ok(())
    }

    /// The Scion ID of a git blob, hashed from its git content.
    fn blob(&mut self, oid: ObjectId) -> Result<BlobId> {
        cached(&mut self.blobs, oid, || {
            Ok(BlobId::of(&read_blob(self.git, oid)?.data))
        })
    }
}

/// Looks up `oid` in `cache`, filling it from `compute` on a miss.
fn cached(
    cache: &mut LruCache<ObjectId, BlobId>,
    oid: ObjectId,
    compute: impl FnOnce() -> Result<BlobId>,
) -> Result<BlobId> {
    if let Some(id) = cache.get(&oid) {
        return Ok(*id);
    }
    let id = compute()?;
    cache.insert(oid, id);
    Ok(id)
}

/// A git tree entry, classified the way Scion stores it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GitEntry {
    Dir(ObjectId),
    File(FileKind, ObjectId),
}

/// What a non-directory git entry becomes. Equal kinds with equal object IDs
/// are the same Scion state, so `100644` and `100664` do not differ.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FileKind {
    Regular { executable: bool },
    Symlink,
    Gitlink,
}

fn subtree(entry: Option<&GitEntry>) -> Option<ObjectId> {
    match entry {
        Some(GitEntry::Dir(id)) => Some(*id),
        _ => None,
    }
}

/// The Scion state of a git file entry. `blob` gives the Scion ID of a git
/// blob.
fn file_state(
    kind: FileKind,
    oid: ObjectId,
    mut blob: impl FnMut(ObjectId) -> Result<BlobId>,
) -> Result<FileState> {
    Ok(match kind {
        FileKind::Regular { executable } => FileState::Regular {
            content: blob(oid)?,
            executable,
        },
        FileKind::Symlink => FileState::Symlink { target: blob(oid)? },
        FileKind::Gitlink => FileState::Gitlink {
            commit: match oid {
                ObjectId::Sha1(bytes) => GitOid::Sha1(bytes),
                other => return Err(Error::Git(format!("unsupported object ID {other}"))),
            },
        },
    })
}

/// A git tree's entries by raw name, in Scion's byte order. `None` is the
/// empty tree.
fn read_tree(
    git: &gix::Repository,
    commit: ObjectId,
    id: Option<ObjectId>,
) -> Result<BTreeMap<Vec<u8>, GitEntry>> {
    let Some(id) = id else {
        return Ok(BTreeMap::new());
    };
    let bad = |what: String| Error::Git(format!("commit {commit}: tree {id}: {what}"));
    let tree = git
        .find_tree(id)
        .map_err(|e| bad(format!("cannot read: {e}")))?;
    let decoded = tree
        .decode()
        .map_err(|e| bad(format!("cannot decode: {e}")))?;
    let mut entries = BTreeMap::new();
    for e in &decoded.entries {
        let oid = e.oid.to_owned();
        let entry = match e.mode.value() {
            0o040_000 => GitEntry::Dir(oid),
            0o100_644 | 0o100_664 => GitEntry::File(FileKind::Regular { executable: false }, oid),
            0o100_755 => GitEntry::File(FileKind::Regular { executable: true }, oid),
            0o120_000 => GitEntry::File(FileKind::Symlink, oid),
            0o160_000 => GitEntry::File(FileKind::Gitlink, oid),
            mode => {
                return Err(bad(format!(
                    "\"{}\" has unsupported mode {mode:o}",
                    e.filename.escape_ascii()
                )));
            }
        };
        if entries.insert(e.filename.to_vec(), entry).is_some() {
            return Err(bad(format!(
                "duplicate entry \"{}\"",
                e.filename.escape_ascii()
            )));
        }
    }
    Ok(entries)
}

/// Pairs the entries of two directories by name, in byte order.
fn pair_up<'m, A, B>(
    a: &'m BTreeMap<Vec<u8>, A>,
    b: &'m BTreeMap<Vec<u8>, B>,
) -> impl Iterator<Item = (&'m [u8], Option<&'m A>, Option<&'m B>)> {
    let names: BTreeSet<&Vec<u8>> = a.keys().chain(b.keys()).collect();
    names
        .into_iter()
        .map(|name| (name.as_slice(), a.get(name), b.get(name)))
}

fn join(dir: &[u8], name: &[u8]) -> Vec<u8> {
    if dir.is_empty() {
        name.to_vec()
    } else {
        [dir, b"/", name].concat()
    }
}

/// `dir/name` as a Scion path.
fn child_path(commit: ObjectId, dir: &[u8], name: &[u8]) -> Result<RepoPath> {
    let path = join(dir, name);
    PathElem::new(name)
        .and_then(|_| RepoPath::new(path.as_slice()))
        .map_err(|e| {
            Error::Git(format!(
                "commit {commit}: path \"{}\" is not valid in Scion: {e}",
                path.escape_ascii()
            ))
        })
}

fn find_commit(git: &gix::Repository, commit: ObjectId) -> Result<gix::Commit<'_>> {
    git.find_commit(commit)
        .map_err(|e| Error::Git(format!("cannot read commit {commit}: {e}")))
}

fn read_blob(git: &gix::Repository, oid: ObjectId) -> Result<gix::Blob<'_>> {
    git.find_blob(oid)
        .map_err(|e| Error::Git(format!("cannot read blob {oid}: {e}")))
}

fn identity(
    commit: ObjectId,
    role: &str,
    signature: gix::Result<SignatureRef<'_>>,
) -> Result<IdentityTime> {
    let bad = |e: gix::Error| Error::Git(format!("commit {commit}: bad {role}: {e}"));
    let signature = signature.map_err(bad)?;
    let time = signature.time().map_err(bad)?;
    Ok(IdentityTime {
        name: signature.name.to_vec(),
        email: signature.email.to_vec(),
        unix_secs: time.seconds,
        // Git stores whole minutes, so this is exact.
        tz_offset_minutes: time.offset / 60,
    })
}

fn per_sec(count: usize, since: Instant) -> u128 {
    count as u128 * 1000 / since.elapsed().as_millis().max(1)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::process::Command;

    use super::*;

    /// Runs git in `dir` with a fixed identity and no user or system config.
    fn git(dir: &Path, args: &[&str]) {
        let out = Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Ada")
            .env("GIT_AUTHOR_EMAIL", "ada@example.com")
            .env("GIT_COMMITTER_NAME", "Ada")
            .env("GIT_COMMITTER_EMAIL", "ada@example.com")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    #[test]
    fn a_tiny_blob_cache_imports_the_same_changesets() {
        let repo = tempfile::tempdir().unwrap();
        let dir = repo.path();
        git(dir, &["init", "-q", "-b", "main"]);
        // Eight distinct blobs, then the same blobs at new paths. With a cache
        // of two entries, most of them are met again after they were evicted.
        for i in 0..8 {
            fs::write(dir.join(format!("f{i}")), format!("{i}\n")).unwrap();
        }
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "add"]);
        fs::create_dir(dir.join("d")).unwrap();
        for i in 0..8 {
            fs::rename(dir.join(format!("f{i}")), dir.join(format!("d/f{i}"))).unwrap();
        }
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "move"]);

        let opts = ImportOptions {
            rev: "main".into(),
            target_ref: "main".into(),
            verify_sample: usize::MAX,
        };
        let import = |cache_entries| {
            let data = tempfile::tempdir().unwrap();
            let store = Store::open(data.path()).unwrap();
            import_with_cache(&store, "main", dir, &opts, cache_entries).unwrap()
        };
        let tiny = import(2);
        let unbounded = import(usize::MAX);
        assert_eq!((tiny.commits, tiny.verified), (2, 2));
        // Each changeset names its parents by ID, so equal tips mean that
        // every changeset is the same.
        assert_eq!(tiny.tip, unbounded.tip);
    }
}
