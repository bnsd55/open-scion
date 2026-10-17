//! `scion-server fsck <data>`: checks a SQLite data directory and reports
//! what it finds. It changes nothing: it reads, and leaves every repair to
//! you. It belongs to the SQLite backend: its SQL lives in `db.rs`, and
//! another backend brings its own check.
//!
//! It reads in short batches, so it runs beside a live server. Each check
//! looks at the state at the moment it looks: a row's blob file is checked
//! when the row is read, never against a listing taken earlier. A clean
//! report means "no damage seen", not a snapshot.
//!
//! What it checks:
//!
//! - every file under `objects/blob` unpacks and hashes to its ID;
//! - every blob that `repo_blobs`, a changeset's puts, or a tree entry
//!   names exists as a file;
//! - every changeset, tree, and history node decodes and matches its ID;
//! - every ref points at a changeset of its repo, and every row of
//!   `repo_changesets`, `lands`, and `derived` names a stored changeset;
//! - every derived value decodes with its kind's decoder for its version.

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr as _;

use scion_core::{BlobId, Changeset, ChangesetId, Tree, TreeId};

use crate::db;
use crate::derived::Kind as _;
use crate::graph::CommitGraph;
use crate::history::{self, NodeId, PathHistory};
use crate::store::{BlobStatus, Store};
use crate::tree::RootTree;

/// Rows to read per batch.
pub const BATCH: usize = 4096;

/// A stage of the check, reported between two batches of the stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// The blob files.
    BlobFiles,
    /// The `changesets` rows.
    Changesets,
    /// The `trees` rows.
    Trees,
    /// The `history_nodes` rows.
    HistoryNodes,
    /// The `repo_blobs` rows.
    RepoBlobs,
    /// The `refs` rows.
    Refs,
    /// The `lands` rows.
    Lands,
    /// The `derived` rows.
    Derived,
}

/// One problem that a run found.
#[derive(Debug)]
pub struct Problem {
    /// The kind of the problem, as printed.
    kind: &'static str,
    /// The ID of the damaged object, as printed.
    id: String,
    /// The path of the damaged file, when it has one.
    path: Option<PathBuf>,
    /// What is wrong with it.
    detail: String,
}

impl Problem {
    /// A problem of `kind` with `id`, its `path`, and `detail`.
    fn new(kind: &'static str, id: String, path: Option<PathBuf>, detail: String) -> Self {
        Self {
            kind,
            id,
            path,
            detail,
        }
    }
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.kind, self.id)?;
        if let Some(path) = &self.path {
            write!(f, " {}", path.display())?;
        }
        write!(f, " {}", self.detail)
    }
}

/// What one run found: the damage, and the derived rows of a version this
/// build does not know. Old rows are not damage: the steps of the kind
/// remove them.
#[derive(Debug, Default)]
pub struct Report {
    /// One problem per piece of damage.
    pub problems: Vec<Problem>,
    /// The (kind, version) of rows this build cannot decode, once per pair.
    pub old: BTreeSet<(String, u32)>,
}

impl Report {
    /// Records one problem.
    fn problem(&mut self, problem: Problem) {
        self.problems.push(problem);
    }

    /// Whether the store looked clean: no damage seen, not a snapshot.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.problems.is_empty()
    }
}

/// Checks the data directory of `store`.
///
/// `batch` rows are read per short transaction, so a live server shares the
/// store while this runs. `between` runs between two batches, with the
/// stage that just finished; a test uses it to write while the check reads.
///
/// # Errors
///
/// Errors of the database or the file system: the check itself could not
/// run, which is not damage in the store.
pub fn run(
    store: &Store,
    batch: usize,
    between: &mut dyn FnMut(Stage),
) -> crate::error::Result<Report> {
    let mut report = Report::default();
    check_blob_files(store, &mut report)?;
    between(Stage::BlobFiles);
    check_changesets(store, batch, between, &mut report)?;
    check_trees(store, batch, between, &mut report)?;
    check_history_nodes(store, batch, between, &mut report)?;
    check_repo_blobs(store, batch, between, &mut report)?;
    check_refs(store, &mut report)?;
    between(Stage::Refs);
    check_lands(store, &mut report)?;
    between(Stage::Lands);
    check_derived(store, batch, between, &mut report)?;
    Ok(report)
}

/// Checks every file under `objects/blob`: its name parses to a blob ID,
/// and the file unpacks to content with that ID.
fn check_blob_files(store: &Store, report: &mut Report) -> crate::error::Result<()> {
    let root = store.blobs().root().to_path_buf();
    let dirs = match std::fs::read_dir(&root) {
        Ok(dirs) => dirs,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for dir in dirs {
        let dir = dir?;
        let fan = dir.file_name().to_string_lossy().into_owned();
        let fan_path = dir.path();
        if !fan_path.is_dir() {
            report.problem(Problem::new(
                "blob-corrupt",
                fan,
                Some(fan_path),
                "not a blob folder".into(),
            ));
            continue;
        }
        for file in std::fs::read_dir(&fan_path)? {
            let file = file?;
            let name = file.file_name().to_string_lossy().into_owned();
            let path = file.path();
            let full = format!("{fan}{name}");
            let Ok(id) = BlobId::from_str(&full) else {
                report.problem(Problem::new(
                    "blob-corrupt",
                    full,
                    Some(path),
                    "not a blob name".into(),
                ));
                continue;
            };
            if path.is_dir() {
                report.problem(Problem::new(
                    "blob-corrupt",
                    full,
                    Some(path),
                    "not a blob file".into(),
                ));
                continue;
            }
            match store.blobs().verify(id) {
                Ok(BlobStatus::Valid) => {}
                Ok(BlobStatus::Missing) => report.problem(Problem::new(
                    "blob-missing",
                    id.to_string(),
                    Some(path),
                    "gone when checked".into(),
                )),
                Ok(BlobStatus::Corrupt) => report.problem(Problem::new(
                    "blob-corrupt",
                    id.to_string(),
                    Some(path),
                    "content does not match its ID".into(),
                )),
                Err(e) => report.problem(Problem::new(
                    "blob-corrupt",
                    id.to_string(),
                    Some(path),
                    e.to_string(),
                )),
            }
        }
    }
    Ok(())
}

/// Checks every changeset: it decodes and matches its ID, and every blob of
/// its puts exists when the row is read.
fn check_changesets(
    store: &Store,
    batch: usize,
    between: &mut dyn FnMut(Stage),
    report: &mut Report,
) -> crate::error::Result<()> {
    let mut last: Option<Vec<u8>> = None;
    loop {
        let from = last.clone().unwrap_or_default();
        let rows = store.read(|c| {
            db::fsck_changesets(c, from.as_slice(), batch).map(|rows| {
                rows.into_iter()
                    .map(|(id, body)| {
                        let decoded = ChangesetId::try_from(id.as_slice())
                            .ok()
                            .zip(Changeset::decode_with_id(&body).ok())
                            .map(|(id, (cs, actual))| (id, actual, cs));
                        (id, decoded)
                    })
                    .collect::<Vec<_>>()
            })
        })?;
        let count = rows.len();
        let next = rows.last().map(|(id, _)| id.clone());
        for (id, decoded) in rows {
            let shown = id_string(&id);
            let Some((id, actual, cs)) = decoded else {
                report.problem(Problem::new(
                    "changeset-bad",
                    shown,
                    None,
                    "does not decode as it is stored".into(),
                ));
                continue;
            };
            if actual != id {
                report.problem(Problem::new(
                    "changeset-bad",
                    shown,
                    None,
                    "body does not match its ID".into(),
                ));
                continue;
            }
            for (_, blob) in db::puts(&cs) {
                named_blob_exists(store, blob, report, &format!("named by changeset {id}"));
            }
        }
        last = next;
        between(Stage::Changesets);
        if count < batch {
            break;
        }
    }
    Ok(())
}

/// Checks every tree: it decodes and matches its ID, and every blob of its
/// file entries exists when the row is read.
fn check_trees(
    store: &Store,
    batch: usize,
    between: &mut dyn FnMut(Stage),
    report: &mut Report,
) -> crate::error::Result<()> {
    let mut last: Option<Vec<u8>> = None;
    loop {
        let from = last.clone().unwrap_or_default();
        let rows = store.read(|c| db::fsck_trees(c, from.as_slice(), batch))?;
        let count = rows.len();
        let next = rows.last().map(|(id, _)| id.clone());
        for (id, body) in rows {
            let shown = id_string(&id);
            let Ok(id) = TreeId::try_from(id.as_slice()) else {
                report.problem(Problem::new(
                    "tree-bad",
                    shown,
                    None,
                    "does not decode as it is stored".into(),
                ));
                continue;
            };
            match Tree::decode_with_id(&body) {
                Err(e) => report.problem(Problem::new(
                    "tree-bad",
                    id.to_string(),
                    None,
                    format!("cannot decode: {e}"),
                )),
                Ok((tree, actual)) => {
                    if actual != id {
                        report.problem(Problem::new(
                            "tree-bad",
                            id.to_string(),
                            None,
                            "body does not match its ID".into(),
                        ));
                        continue;
                    }
                    for file in tree.entries.values() {
                        let scion_core::Entry::File(file) = file else {
                            continue;
                        };
                        let Some(blob) = file.state().blob() else {
                            continue;
                        };
                        named_blob_exists(store, blob, report, &format!("named by tree {id}"));
                        // The entry's length must be the blob's own: a tree
                        // whose size lies makes every stat of the file lie.
                        match store.blobs().size(blob) {
                            Ok(size) if size as u64 == file.size() => {}
                            Ok(size) => report.problem(Problem::new(
                                "tree-size",
                                id.to_string(),
                                None,
                                format!("entry says {} bytes, the blob holds {size}", file.size()),
                            )),
                            Err(e) => report.problem(Problem::new(
                                "tree-size",
                                id.to_string(),
                                None,
                                format!("cannot size the blob: {e}"),
                            )),
                        }
                    }
                }
            }
        }
        last = next;
        between(Stage::Trees);
        if count < batch {
            break;
        }
    }
    Ok(())
}

/// Checks every history node: its body decodes, and its ID is the hash of
/// its bytes.
fn check_history_nodes(
    store: &Store,
    batch: usize,
    between: &mut dyn FnMut(Stage),
    report: &mut Report,
) -> crate::error::Result<()> {
    let mut last: Option<Vec<u8>> = None;
    loop {
        let from = last.clone().unwrap_or_default();
        let rows = store.read(|c| db::fsck_history_nodes(c, from.as_slice(), batch))?;
        let count = rows.len();
        let next = rows.last().map(|(id, _)| id.clone());
        for (id, body) in rows {
            let shown = id_string(&id);
            if !history::node_decodes(&body) {
                report.problem(Problem::new(
                    "history-node-bad",
                    shown,
                    None,
                    "body does not decode".into(),
                ));
                continue;
            }
            if NodeId::of(&body).as_bytes() != id.as_slice() {
                report.problem(Problem::new(
                    "history-node-bad",
                    shown,
                    None,
                    "body does not match its ID".into(),
                ));
            }
        }
        last = next;
        between(Stage::HistoryNodes);
        if count < batch {
            break;
        }
    }
    Ok(())
}

/// Checks every row of `repo_blobs`: its file exists when the row is read.
fn check_repo_blobs(
    store: &Store,
    batch: usize,
    between: &mut dyn FnMut(Stage),
    report: &mut Report,
) -> crate::error::Result<()> {
    let mut last: Option<(i64, Vec<u8>)> = None;
    loop {
        let (from_repo, from_blob) = last.clone().unwrap_or((0, Vec::new()));
        let rows =
            store.read(|c| db::fsck_repo_blobs(c, from_repo, from_blob.as_slice(), batch))?;
        let count = rows.len();
        let next = rows.last().cloned();
        for (repo, blob) in rows {
            let Ok(id) = BlobId::try_from(blob.as_slice()) else {
                report.problem(Problem::new(
                    "blob-corrupt",
                    id_string(&blob),
                    None,
                    format!("repo_blobs names it for repo {repo}, and it is no blob ID"),
                ));
                continue;
            };
            named_blob_exists(
                store,
                id,
                report,
                &format!("named by repo_blobs (repo {repo})"),
            );
        }
        last = next;
        between(Stage::RepoBlobs);
        if count < batch {
            break;
        }
    }
    Ok(())
}

/// Whether the file of `blob` exists at the moment of the check.
fn named_blob_exists(store: &Store, blob: BlobId, report: &mut Report, why: &str) {
    match store.blobs().open_file(blob) {
        Ok(_) => {}
        Err(crate::error::Error::NotFound(_)) => report.problem(Problem::new(
            "blob-missing",
            blob.to_string(),
            Some(store.blobs().location(blob).1),
            why.into(),
        )),
        Err(e) => report.problem(Problem::new(
            "blob-corrupt",
            blob.to_string(),
            Some(store.blobs().location(blob).1),
            e.to_string(),
        )),
    }
}

/// Checks every ref: its target is a changeset of its repo.
fn check_refs(store: &Store, report: &mut Report) -> crate::error::Result<()> {
    let rows = store.read(db::fsck_dangling_refs)?;
    for (repo, name, target) in rows {
        report.problem(Problem::new(
            "ref-dangling",
            format!("{repo}/{name}"),
            None,
            format!("points at {}", id_string(&target)),
        ));
    }
    Ok(())
}

/// Checks every land: its changesets are stored.
fn check_lands(store: &Store, report: &mut Report) -> crate::error::Result<()> {
    let rows = store.read(db::fsck_lands_missing)?;
    for (land, cs, column) in rows {
        report.problem(Problem::new(
            "changeset-missing",
            id_string(&cs),
            None,
            format!("named by lands (land {land}, {column})"),
        ));
    }
    Ok(())
}

/// Checks every derived row: its changeset is stored, and its value decodes
/// with its kind's decoder for its version. A row of a version this build
/// does not know is old, not damage.
fn check_derived(
    store: &Store,
    batch: usize,
    between: &mut dyn FnMut(Stage),
    report: &mut Report,
) -> crate::error::Result<()> {
    let mut last: Option<(String, u32, Vec<u8>)> = None;
    loop {
        let (from_kind, from_version, from_cs) =
            last.clone().unwrap_or((String::new(), 0, Vec::new()));
        let rows = store.read(|c| {
            db::fsck_derived(
                c,
                from_kind.as_str(),
                from_version,
                from_cs.as_slice(),
                batch,
            )
        })?;
        let count = rows.len();
        let next = rows
            .last()
            .map(|(kind, version, cs, _, _)| (kind.clone(), *version, cs.clone()));
        for (kind, version, cs, value, stored) in rows {
            let shown = format!("{kind}/{version}/{}", id_string(&cs));
            if !stored {
                report.problem(Problem::new(
                    "changeset-missing",
                    id_string(&cs),
                    None,
                    format!("named by derived ({shown})"),
                ));
                continue;
            }
            let decodes = if kind == RootTree::NAME && version == RootTree::VERSION {
                RootTree::decode(&value).is_some()
            } else if kind == CommitGraph::NAME && version == CommitGraph::VERSION {
                CommitGraph::decode(&value).is_some()
            } else if kind == PathHistory::NAME && version == PathHistory::VERSION {
                PathHistory::decode(&value).is_some()
            } else {
                report.old.insert((kind, version));
                continue;
            };
            if !decodes {
                report.problem(Problem::new(
                    "derived-bad",
                    shown,
                    None,
                    "value does not decode for its kind and version".into(),
                ));
            }
        }
        last = next;
        between(Stage::Derived);
        if count < batch {
            break;
        }
    }
    Ok(())
}

/// The hex of raw ID bytes, for reports.
fn id_string(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SqliteTx;
    use crate::meta::MetaTx as _;
    use scion_core::{FileState, IdentityTime, RepoPath};
    use std::time::Duration;
    use tempfile::TempDir;

    const REPO: &str = "fsck";

    /// A changeset on `parent` that puts `files`, or deletes them on `None`.
    fn cs(parent: Option<ChangesetId>, files: &[(&str, Option<FileState>)]) -> Changeset {
        Changeset {
            parents: parent.into_iter().collect(),
            author: IdentityTime {
                name: b"Ada".to_vec(),
                email: b"ada@example.com".to_vec(),
                unix_secs: 1_700_000_000,
                tz_offset_minutes: 0,
            },
            committer: None,
            message: b"fsck\n".to_vec(),
            changes: files
                .iter()
                .map(|(path, state)| (RepoPath::new(*path).unwrap(), *state))
                .collect(),
        }
    }

    /// Stores `cs` in the repo and moves `main` to it, as a land does.
    fn land(store: &Store, cs: &Changeset) -> ChangesetId {
        use crate::meta::MetaTx as _;
        store
            .write(|c| {
                let mut tx = SqliteTx(c);
                let repo = tx.create_repo(REPO)?;
                let (id, _) = crate::tree::store_changeset(
                    &mut tx,
                    repo,
                    cs,
                    &mut crate::tree::lens_of(store),
                )?;
                let version = tx.get_ref(repo, "main")?.map(|r| r.version);
                assert!(tx.cas_ref(repo, "main", version, id)?);
                Ok(id)
            })
            .unwrap()
    }

    /// A file state whose content is `text`, stored as a blob.
    fn file(store: &Store, text: &[u8]) -> FileState {
        FileState::Regular {
            content: store.blobs().batch().put(text).unwrap(),
            executable: false,
        }
    }

    /// A store with one changeset on `main`.
    fn store() -> (TempDir, Store, ChangesetId) {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let id = land(
            &store,
            &cs(None, &[("a.txt", Some(file(&store, b"one\n")))]),
        );
        (dir, store, id)
    }

    /// The problems of one run, as `kind id` strings.
    fn kinds(report: &Report) -> Vec<String> {
        report
            .problems
            .iter()
            .map(|p| format!("{} {}", p.kind, p.id))
            .collect()
    }

    /// Whether one problem of `kind` mentions `needle`.
    fn has(report: &Report, kind: &str, needle: &str) -> bool {
        report.problems.iter().any(|p| {
            p.kind == kind
                && (p.id.contains(needle)
                    || p.detail.contains(needle)
                    || p.path
                        .as_ref()
                        .is_some_and(|path| path.to_string_lossy().contains(needle)))
        })
    }

    #[test]
    fn a_clean_store_reports_nothing() {
        let (_dir, store, _) = store();
        let report = run(&store, 1, &mut |_| {}).unwrap();
        assert!(report.is_clean(), "{:?}", kinds(&report));
        assert!(report.old.is_empty());
    }

    /// The blob of the first put of the changeset `id`.
    fn first_put(store: &Store, id: ChangesetId) -> BlobId {
        store
            .read(|c| {
                let mut tx = SqliteTx(c);
                let repo = tx.repo_id(REPO)?;
                Ok(db::puts(&tx.get_changeset(repo, id)?).next().unwrap().1)
            })
            .unwrap()
    }

    #[test]
    fn a_truncated_blob_file_is_corrupt() {
        let (_dir, store, id) = store();
        let blob = first_put(&store, id);
        let path = store.blobs().location(blob).1;
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
        let report = run(&store, 1, &mut |_| {}).unwrap();
        assert!(
            has(&report, "blob-corrupt", &blob.to_string()),
            "{:?}",
            kinds(&report)
        );
    }

    #[test]
    fn a_missing_blob_file_that_a_row_names_is_missing() {
        let (_dir, store, id) = store();
        let blob = first_put(&store, id);
        std::fs::remove_file(store.blobs().location(blob).1).unwrap();
        let report = run(&store, 1, &mut |_| {}).unwrap();
        assert!(
            has(&report, "blob-missing", &blob.to_string()),
            "{:?}",
            kinds(&report)
        );
        assert_eq!(report.problems.len(), 4, "{:?}", kinds(&report));
    }

    #[test]
    fn a_broken_changeset_body_is_bad() {
        let (_dir, store, id) = store();
        store
            .write(|c| {
                let mut body: Vec<u8> = c.query_row(
                    "SELECT body FROM changesets WHERE id = ?1",
                    [id.as_bytes()],
                    |row| row.get(0),
                )?;
                let n = body.len();
                body[n - 1] ^= 1;
                c.execute(
                    "UPDATE changesets SET body = ?1 WHERE id = ?2",
                    rusqlite::params![body, id.as_bytes()],
                )?;
                Ok(())
            })
            .unwrap();
        let report = run(&store, 1, &mut |_| {}).unwrap();
        assert!(
            has(&report, "changeset-bad", &id.to_string()),
            "{:?}",
            kinds(&report)
        );
    }

    #[test]
    fn a_broken_tree_body_is_bad() {
        let (_dir, store, id) = store();
        let tree = store
            .read(|c| {
                let repo = SqliteTx(c).repo_id(REPO)?;
                Ok(crate::tree::root_tree(&store, repo, id).unwrap())
            })
            .unwrap();
        store
            .write(|c| {
                let mut body: Vec<u8> = c.query_row(
                    "SELECT body FROM trees WHERE id = ?1",
                    [tree.as_bytes()],
                    |row| row.get(0),
                )?;
                let n = body.len();
                body[n - 1] ^= 1;
                c.execute(
                    "UPDATE trees SET body = ?1 WHERE id = ?2",
                    rusqlite::params![body, tree.as_bytes()],
                )?;
                Ok(())
            })
            .unwrap();
        let report = run(&store, 1, &mut |_| {}).unwrap();
        assert!(
            has(&report, "tree-bad", &tree.to_string()),
            "{:?}",
            kinds(&report)
        );
    }

    #[test]
    fn a_ref_to_a_changeset_no_repo_may_read_dangles() {
        let (dir, store, id) = store();
        // The damage: the schema forbids it with its keys on, so a raw
        // connection makes it, as an older build may have.
        let raw = rusqlite::Connection::open(dir.path().join("scion.db")).unwrap();
        raw.pragma_update(None, "foreign_keys", false).unwrap();
        raw.execute("DELETE FROM repo_changesets WHERE cs = ?1", [id.as_bytes()])
            .unwrap();
        drop(raw);
        let report = run(&store, 1, &mut |_| {}).unwrap();
        assert!(has(&report, "ref-dangling", "main"), "{:?}", kinds(&report));
    }

    #[test]
    fn a_broken_history_node_body_is_bad() {
        let (dir, store, _) = store();
        let raw = rusqlite::Connection::open(dir.path().join("scion.db")).unwrap();
        let rows: Vec<(Vec<u8>, Vec<u8>)> = raw
            .prepare("SELECT id, body FROM history_nodes")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let (id, _) = rows.first().expect("a land stores history nodes").clone();
        raw.execute(
            "UPDATE history_nodes SET body = ?1 WHERE id = ?2",
            rusqlite::params![&b"not a node"[..], &id[..]],
        )
        .unwrap();
        drop(raw);
        let report = run(&store, 1, &mut |_| {}).unwrap();
        assert!(
            has(&report, "history-node-bad", "does not decode"),
            "{:?}",
            kinds(&report)
        );
    }

    #[test]
    fn a_derived_row_for_a_missing_changeset_is_missing() {
        let (dir, store, _) = store();
        let missing = [7_u8; 32];
        // The damage: the schema forbids it with its keys on, so a raw
        // connection makes it, as an older build may have.
        let raw = rusqlite::Connection::open(dir.path().join("scion.db")).unwrap();
        raw.pragma_update(None, "foreign_keys", false).unwrap();
        raw.execute(
            "INSERT INTO derived (kind, version, cs, value) VALUES ('root_tree', 1, ?1, ?2)",
            rusqlite::params![&missing[..], &missing[..]],
        )
        .unwrap();
        drop(raw);
        let report = run(&store, 1, &mut |_| {}).unwrap();
        assert!(
            has(&report, "changeset-missing", "named by derived"),
            "{:?}",
            kinds(&report)
        );
    }

    #[test]
    fn a_derived_value_that_does_not_decode_is_bad() {
        let (_dir, store, _) = store();
        store
            .write(|c| {
                c.execute(
                    "UPDATE derived SET value = ?1 WHERE kind = 'commit_graph'",
                    rusqlite::params![&[0_u8; 32][..]],
                )?;
                Ok(())
            })
            .unwrap();
        let report = run(&store, 1, &mut |_| {}).unwrap();
        assert!(
            has(&report, "derived-bad", "commit_graph"),
            "{:?}",
            kinds(&report)
        );
    }

    #[test]
    fn a_derived_row_of_an_unknown_version_is_old_not_damage() {
        let (_dir, store, id) = store();
        store
            .write(|c| {
                c.execute(
                    "INSERT INTO derived (kind, version, cs, value) VALUES ('root_tree', 9, ?1, ?2)",
                    rusqlite::params![id.as_bytes(), id.as_bytes()],
                )?;
                Ok(())
            })
            .unwrap();
        let report = run(&store, 1, &mut |_| {}).unwrap();
        assert!(report.is_clean(), "{:?}", kinds(&report));
        assert_eq!(report.old, BTreeSet::from([("root_tree".to_owned(), 9)]));
    }

    #[test]
    fn fsck_beside_a_live_server_sees_no_damage() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let mut id = land(
            &store,
            &cs(None, &[("a.txt", Some(file(&store, b"one\n")))]),
        );
        for i in 1..4 {
            id = land(
                &store,
                &cs(
                    Some(id),
                    &[(&format!("f{i}.txt"), Some(file(&store, b"more\n")))],
                ),
            );
        }
        // One row per batch, and a land between two of them: the writes of
        // a live server must not look like damage.
        let mut landed = false;
        let report = run(&store, 1, &mut |stage| {
            if stage == Stage::RepoBlobs && !landed {
                landed = true;
                land(
                    &store,
                    &cs(Some(id), &[("late.txt", Some(file(&store, b"late\n")))]),
                );
            }
        })
        .unwrap();
        assert!(landed, "the land between the batches never ran");
        assert!(report.is_clean(), "{:?}", kinds(&report));
        std::thread::sleep(Duration::from_millis(1));
    }
}
