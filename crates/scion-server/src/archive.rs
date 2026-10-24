//! The versioned logical archive (`scion-archive-1`): a whole repository,
//! or the whole store, as a documented directory of files that `verify`
//! checks without the store's code and that `restore` replays into an
//! empty store. The format is `docs/archive-format.md`; this module is
//! its one writer and reader.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use gix::ObjectId;
use scion_core::{BlobId, Changeset, ChangesetId};

use crate::auth::Caller;
use crate::backend::Backend;
use crate::deadline::Deadline;
use crate::error::{Error, Result};
use crate::land::LandRequest;
use crate::meta::RepoId;
use crate::meta::{Meta, MetaTx, PendingLand};
use crate::tree;
use std::time::Duration;

/// The marker file of a data directory (`backend.rs` owns its format).
const MARKER: &str = "backend";

/// What one repo of an export holds, as one read snapshot read it.
struct RepoSection {
    name: String,
    changesets: Vec<(ChangesetId, i64, Vec<u8>)>,
    refs: Vec<(String, ChangesetId, i64)>,
    lands: Vec<PendingLand>,
    members: Vec<BlobId>,
    provenance: Vec<(String, ChangesetId)>,
}

/// A land of an archive, as its file holds it: the fields a restore needs
/// to record it again.
pub(crate) struct ArchiveLand {
    pub(crate) caller: String,
    pub(crate) request_key: String,
    pub(crate) target_ref: String,
    pub(crate) input_cs: ChangesetId,
}

// The test's writer between a section's statements. A test installs it
// on its own thread, and the export runs on the caller's thread, so the
// hook fires inside that test's export and in no other's. One-shot:
// the first section takes it, the rest pass. None passes nothing.
#[cfg(any(test, feature = "test-hooks"))]
thread_local! {
    static SECTION_HOOK: std::cell::RefCell<Option<Box<dyn Fn() + Send>>> =
        const { std::cell::RefCell::new(None) };
}

/// Installs the hook that runs between a section's statements, or takes
/// it back with `None`.
#[cfg(any(test, feature = "test-hooks"))]
pub fn set_section_hook(hook: Option<Box<dyn Fn() + Send>>) {
    SECTION_HOOK.with(|slot| *slot.borrow_mut() = hook);
}

/// Takes the installed hook, if any.
#[cfg(any(test, feature = "test-hooks"))]
fn take_section_hook() -> Option<Box<dyn Fn() + Send>> {
    SECTION_HOOK.with(|slot| slot.borrow_mut().take())
}

/// Exports `repo`, or the whole store with `all`, to the archive directory
/// `out`, which must not exist. The metadata sections are one read
/// snapshot (one transaction over the store's metadata); the blob files
/// are copied after it, which is
/// safe because a blob file is immutable and `gc` keeps a blob past its
/// grace. Returns the manifest as written.
///
/// # Errors
///
/// What the store reads return, what a blob file read returns, or an
/// `out` that already exists.
pub fn export(
    store: &Backend,
    data: &Path,
    repo: Option<&str>,
    all: bool,
    out: &Path,
) -> Result<serde_json::Value> {
    if repo.is_some() == all {
        return Err(Error::InvalidArgument(
            "give exactly one of --repo NAME or --all".to_owned(),
        ));
    }
    if out.exists() {
        return Err(Error::FailedPrecondition(format!(
            "{} already exists; the export refuses to write into it",
            out.display()
        )));
    }

    // The repo names first: a `--repo` that is not stored fails here,
    // before the archive holds anything.
    let names: Vec<String> = store.read(|tx| {
        Ok(match repo {
            Some(name) => {
                tx.repo_id(name)?;
                vec![name.to_owned()]
            }
            None => tx
                .repos()?
                .into_iter()
                .map(|(_, name)| name)
                .collect::<Vec<String>>(),
        })
    })?;

    let mut report_repos = Vec::new();
    let mut total_changesets = 0_u64;
    let mut total_refs = 0_u64;
    let mut total_lands = 0_u64;
    let mut total_members = 0_u64;
    // The archive's root: made recursively, as before — the operator
    // names a path whose parents may not exist, and the fold risk this
    // module guards is between REPO names, not in the operator's own
    // path. Each section directory below is a NON-recursive create, so
    // a fold on the disk is an error, not a silent reuse of another
    // repo's directory.
    make_dir(out)?;
    let repos_dir = out.join("repos");
    fs::DirBuilder::new()
        .create(&repos_dir)
        .map_err(|error| Error::Corrupt(format!("cannot make {}: {error}", repos_dir.display())))?;
    // The section directories this export made, by their (device,
    // inode): on a disk that folds names, two repos' directories can be
    // one directory, and the disk is the only judge of that.
    let mut made: HashMap<(u64, u64), String> = HashMap::new();
    for name in &names {
        let section = snapshot_section(store, name)?;

        let report = write_section(data, out, &section, &mut made)?;
        total_changesets += report.changesets;
        total_refs += report.refs;
        total_lands += report.lands;
        total_members += report.members;
        report_repos.push(report.json);
    }

    // The manifest goes last: an archive without one is not an archive,
    // and a reader refuses it. The snapshot instant is the clock reading
    // taken while the metadata was read; the store UUID names where the
    // metadata came from.
    let manifest = serde_json::json!({
        "format": "scion-archive",
        "version": 1,
        "kind": if all { "store" } else { "repo" },
        "store_uuid": crate::backend::store_uuid_of(data).map(|uuid| hex(&uuid)),
        "snapshot_at": snapshot_instant(),
        "snapshot_note": "the metadata sections are one read snapshot; the blob files are immutable, and gc keeps a blob past its grace",
        "counts": {
            "repos": names.len(),
            "changesets": total_changesets,
            "refs": total_refs,
            "pending_lands": total_lands,
            "blobs": total_members,
        },
        "repos": report_repos,
    });
    write_json(&out.join("manifest.json"), &manifest)?;
    Ok(manifest)
}

/// Checks an archive against the format, without the store's code: the
/// core codec, blake3, and zstd only. It re-hashes every blob file and
/// every changeset body against its ID, and checks the graph: every ref
/// target, changeset parent, and land input is a changeset the archive
/// holds, and every member's blob file is there. Returns one line per
/// damage.
///
/// # Errors
///
/// Only when the archive cannot be read at all: no manifest, a version
/// this reader refuses, or a section that cannot be parsed.
pub fn verify(archive: &Path) -> Result<Vec<String>> {
    let manifest = read_manifest(archive)?;
    let names = repo_names(&manifest)?;
    let mut damages = Vec::new();
    // The manifest is data from a file: a name twice means the reader
    // and the restore would both walk the repo twice, and the restore
    // would replay two stores into one. Name it before anything else.
    // And on a disk that folds names, two names can be one directory:
    // the disk's own (device, inode) is the judge, not our fold. The
    // section of a folded pair is one directory read twice, and the
    // restore would replay one repo's data under both names.
    let mut seen_names = HashSet::new();
    let mut seen_dirs: HashMap<(u64, u64), String> = HashMap::new();
    let mut sections = Vec::new();
    for name in &names {
        if !seen_names.insert(name) {
            damages.push(format!(
                "the manifest names repo {name} twice; one repo, one section"
            ));
            continue;
        }
        // The disk, not a fold table, judges whether two names are one
        // directory: a symlink stands in for another section, and a
        // folded pair is one directory read twice.
        let dir = archive.join("repos").join(name);
        match check_section_dir(&dir, name, &mut seen_dirs) {
            Ok(()) => sections.push(name.clone()),
            Err(damage) => damages.push(damage),
        }
    }
    for name in &sections {
        let dir = archive.join("repos").join(name);
        let members = read_members(&dir.join("members.txt"))?;
        let member_set: HashSet<BlobId> = members.iter().copied().collect();
        let mut ids: HashSet<ChangesetId> = HashSet::new();
        check_frames(
            &dir.join("changesets.bin"),
            name,
            &member_set,
            &mut ids,
            &mut damages,
        )?;
        for (ref_name, target, _version) in read_refs(&dir.join("refs.json"))? {
            if !ids.contains(&target) {
                damages.push(format!(
                    "repo {name}: the ref {ref_name} names {target}, which the archive does not hold"
                ));
            }
        }
        // The pending lands replay as the store's own `record_land`
        // calls, and the store keeps a request key unique per repo: two
        // lands on one key make the replay fail halfway, with half a
        // store behind it. Name the collision here, before any write.
        let mut seen_keys = HashSet::new();
        for land in read_lands(&dir.join("lands.json"))? {
            if !seen_keys.insert(land.request_key.clone()) {
                damages.push(format!(
                    "repo {name}: two pending lands share the request key {}; one key, one land",
                    land.request_key
                ));
            }
            if !ids.contains(&land.input_cs) {
                damages.push(format!(
                    "repo {name}: the land with request key {} names input {}, which the archive does not hold",
                    land.request_key,
                    land.input_cs
                ));
            }
        }
        for member in &members {
            let path = blob_path(archive, member);
            match fs::File::open(&path) {
                Ok(file) => match decode_blob(&path, file) {
                    Ok(raw) => {
                        if BlobId::of(&raw) != *member {
                            damages.push(format!(
                                "the blob file of {member} holds other bytes than its name says"
                            ));
                        }
                    }
                    // A bomb, or a frame the decoder cannot read: one
                    // damage, and verify goes on to name the rest.
                    Err(why) => damages.push(format!("{why}")),
                },
                Err(_) => damages.push(format!("the blob file of {member} is missing")),
            }
        }
    }
    Ok(damages)
}

/// Checks that the section directory of `name` is a plain directory,
/// and that the disk does not fold it onto a directory another name
/// already claimed. One damage per failure, naming both names on a
/// fold.
fn check_section_dir(
    dir: &Path,
    name: &str,
    seen_dirs: &mut HashMap<(u64, u64), String>,
) -> Result<(), String> {
    let section = match fs::symlink_metadata(dir) {
        Ok(section) => section,
        Err(error) => {
            return Err(format!(
                "the manifest names repo {name}, whose section cannot be read: {error}"
            ));
        }
    };
    if section.file_type().is_symlink() || !section.is_dir() {
        return Err(format!(
            "the section of repo {name} is not a plain directory"
        ));
    }
    let identity = (section.dev(), section.ino());
    if let Some(other) = seen_dirs.get(&identity) {
        return Err(format!(
            "the repos {other} and {name} fold to one directory on this disk"
        ));
    }
    seen_dirs.insert(identity, name.to_owned());
    Ok(())
}

/// Walks one repo's frames the way the verify does: one record at a
/// time, the ids the refs and the lands cross-reference, the body
/// re-hash, the parents, and the changes' blobs against members.txt.
/// The verify holds the ids it must cross-reference, never the whole
/// history.
fn check_frames(
    path: &Path,
    name: &str,
    member_set: &HashSet<BlobId>,
    ids: &mut HashSet<ChangesetId>,
    damages: &mut Vec<String>,
) -> Result<()> {
    let mut frames = Frames::open(path)?;
    while let Some((id, generation, body)) = frames.next()? {
        ids.insert(id);
        // The ID of a changeset is the hash of its body, so the
        // re-hash is the check the file names cannot lie about.
        let decoded = match Changeset::decode_with_id(&body) {
            Ok((cs, hashed)) if hashed == id => Some(cs),
            Ok(_) => {
                damages.push(format!(
                    "repo {name}: changeset {id} does not hash to its ID"
                ));
                None
            }
            Err(error) => {
                damages.push(format!(
                    "repo {name}: changeset {id} does not decode: {error}"
                ));
                None
            }
        };
        if generation < 1 {
            damages.push(format!(
                "repo {name}: changeset {id} has generation {generation}"
            ));
        }
        let Some(cs) = decoded else { continue };
        for parent in cs.parents() {
            if !ids.contains(parent) {
                damages.push(format!(
                    "repo {name}: changeset {id} names parent {parent}, which the archive does not hold"
                ));
            }
        }
        // The replay rebuilds every tree from the changes' blobs
        // alone, so a changeset whose change names a blob the
        // members.txt does not list passes here and dies mid-replay
        // — the reader and the writer would disagree about the same
        // archive. Name it here, where the reader reads.
        for state in cs.changes().values().flatten() {
            if let Some(blob) = state.blob().filter(|blob| !member_set.contains(blob)) {
                damages.push(format!(
                    "repo {name}: changeset {id} names blob {blob}, which its members.txt does not list"
                ));
            }
        }
    }
    Ok(())
}

/// Replays an archive into an empty store: the blob bytes go through the
/// What a restore put back: the repositories' names and how many refs
/// and pending lands the replay recorded again.
#[derive(Clone, Debug)]
pub struct RestoreReport {
    /// The restored repositories' names.
    pub repos: Vec<String>,
    /// The refs the restore put back.
    pub refs: u64,
    /// The pending lands the restore recorded again.
    pub lands: u64,
}

/// Replays an archive into an EMPTY data directory: the blob bytes go
/// through the store's own write path, so every object ID is the hash of
/// its bytes; the changesets replay in generation order, which is
/// topological, so the store derives the same generations, trees, member
/// rows, and root-tree rows it held; the refs go back with the versions
/// the export read; the pending lands are recorded again, so a retry
/// with the same request key resumes; the git provenance is put back.
/// The tree bodies are not in the archive: the replay's
/// `store_changeset` rebuilds them. The data directory the caller named
/// is filled by renaming a staging directory beside it into place only
/// when the whole replay held — a restore that fails, for any reason,
/// leaves the target as it found it: an operator cannot tell restored
/// state from unrestored, so a half store is the one thing this must
/// never leave.
///
/// The archive is input from outside the server: [`verify`] runs first,
/// and any damage refuses the replay, so the reader of the files and the
/// writer of the store can never disagree.
///
/// # Errors
///
/// A data directory that already holds a store or has no name, a failed
/// open, damage [`verify`] finds, or what the replay's writes return.
pub fn restore(
    archive: &Path,
    data: &Path,
    postgres_url: Option<&str>,
    postgres_ca: Option<&Path>,
) -> Result<RestoreReport> {
    restore_impl(
        archive,
        data,
        postgres_url,
        postgres_ca,
        DEFAULT_TX_BYTES,
        Backend::open,
    )
}

/// Where the replay's frames come from: the stream, one record at a
/// time, or - the mutation lever the ceiling job flips - the whole file
/// in memory before the first row, the whole-history read the stream
/// replaced. The lever exists so the RSS guard can prove it is not
/// vacuous: the job runs the ceiling test once per shape and refuses a
/// run where both pass.
enum ReplayFrames {
    Stream(Frames),
    Whole(std::vec::IntoIter<(ChangesetId, i64, Vec<u8>)>),
}

impl ReplayFrames {
    fn next(&mut self) -> Result<Option<(ChangesetId, i64, Vec<u8>)>> {
        match self {
            Self::Stream(frames) => frames.next(),
            Self::Whole(items) => Ok(items.next()),
        }
    }
}

/// Whether the replay's whole-file mutation lever is flipped. Test-hooks
/// only: the ceiling job sets it to prove the guard is not vacuous. A
/// default build has no lever: the environment is never read, and the
/// replay always streams.
#[cfg(any(test, feature = "test-hooks"))]
fn mutation_whole_file() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var_os("SCION_TEST_RESTORE_MUTATION").as_deref()
            == Some(std::ffi::OsStr::new("whole_file"))
    })
}

/// The default build: no lever exists to flip.
#[cfg(not(any(test, feature = "test-hooks")))]
fn mutation_whole_file() -> bool {
    false
}

/// The restore with the store's opener chosen: the one seam the tests
/// use, so a test can arm the store's fail seam on the very store a
/// restore opens and watch the choreography — staging, replay, rename,
/// the drop of what it made — under a failure that is the store's, not
/// the archive's. Everything else is the public restore's, behind the
/// `test-hooks` feature.
///
/// # Errors
///
/// As [`restore`].
#[cfg(any(test, feature = "test-hooks"))]
pub fn restore_with(
    archive: &Path,
    data: &Path,
    postgres_url: Option<&str>,
    postgres_ca: Option<&Path>,
    open: impl FnOnce(&Path, Option<&str>, Option<&Path>) -> Result<Backend>,
) -> Result<RestoreReport> {
    restore_impl(
        archive,
        data,
        postgres_url,
        postgres_ca,
        DEFAULT_TX_BYTES,
        open,
    )
}

/// [`restore_with`], with the replay's transaction byte budget named:
/// a test that must see many committed transactions sets a small one.
/// A single record larger than the budget still gets its own
/// transaction.
///
/// # Errors
///
/// As [`restore`].
#[cfg(any(test, feature = "test-hooks"))]
pub fn restore_with_budget(
    archive: &Path,
    data: &Path,
    postgres_url: Option<&str>,
    postgres_ca: Option<&Path>,
    tx_bytes: usize,
    open: impl FnOnce(&Path, Option<&str>, Option<&Path>) -> Result<Backend>,
) -> Result<RestoreReport> {
    restore_impl(archive, data, postgres_url, postgres_ca, tx_bytes, open)
}

/// The whole restore — preflight, verify, the staging choreography, the
/// rename — with the opener a parameter. Private: the public [`restore`]
/// and the test hook are its only callers.
fn restore_impl(
    archive: &Path,
    data: &Path,
    postgres_url: Option<&str>,
    postgres_ca: Option<&Path>,
    tx_bytes: usize,
    open: impl FnOnce(&Path, Option<&str>, Option<&Path>) -> Result<Backend>,
) -> Result<RestoreReport> {
    for existing in ["scion.db", MARKER, "objects"] {
        if data.join(existing).exists() {
            return Err(Error::FailedPrecondition(format!(
                "{} already holds a store; a restore goes into an empty one",
                data.display()
            )));
        }
    }
    // The staging directory sits beside the target, on the same
    // filesystem, so the final publication is one rename: either the
    // target is the restored store or it is what it was.
    let file_name = data.file_name().ok_or_else(|| {
        Error::FailedPrecondition(format!(
            "{} has no file name; a restore needs a named directory",
            data.display()
        ))
    })?;
    let staging = data.with_file_name(format!("{}.restoring", file_name.to_string_lossy()));
    if staging.exists() {
        // The scratch name is this restore's own: a leftover of a run
        // that died mid-replay, never a store an operator made. A kill
        // runs no failure path, so this cleanup at the START is the
        // only one a killed restore's leftovers ever meet.
        tracing::warn!(staging = %staging.display(), "removing a staging directory a killed restore left behind");
        fs::remove_dir_all(&staging)?;
    }
    fs::create_dir_all(&staging)?;
    // The metadata of a killed restore's committed batches sits in the
    // database the URL names: drop that schema before the replay, so
    // the replay starts empty whatever the last run left. A live store
    // is safe: its server holds the one-server lock the drop takes.
    if let Some(url) = postgres_url {
        crate::pg::reset_schema(url, postgres_ca).map_err(|error| match error {
            Error::Corrupt(why) if why.contains("names a store") => {
                // The refusal's truth: the schema may hold a complete
                // store a restore published to the database but never
                // renamed into place (killed after its last
                // transaction). The staging directory that names it
                // sits beside this restore's target.
                Error::Corrupt(format!(
                    "{why}; this schema holds a complete store that no data \
directory names: a restore may have stopped after its last transaction. \
Rename the {}.restoring directory of the target you meant into place and \
start a server on it, or restore into a new schema",
                    file_name.to_string_lossy()
                ))
            }
            other => other,
        })?;
    }
    // The CLI's audit log may already sit in the target: the restore's
    // own intent line went there before the replay, and the target must
    // hold nothing else. Carry the file into the staging store, so the
    // rename that publishes keeps it and the result line lands in the
    // restored store's own audit file.
    let audit_log = data.join("audit.log");
    if audit_log.exists() {
        fs::rename(&audit_log, staging.join("audit.log"))?;
    }
    let replayed = restore_into_with(
        archive,
        &staging,
        postgres_url,
        postgres_ca,
        replay_tx_bytes(tx_bytes),
        open,
    );
    match replayed {
        Ok(report) => {
            if let Err(error) = fs::rename(&staging, data) {
                if !carry_back_audit(&staging, data) {
                    return Err(Error::FailedPrecondition(format!(
                        "the restore replayed whole, but {} could not take {}'s place, and the \
                         run's audit file could not move back: the run's lines stay in {}: {error}",
                        data.display(),
                        staging.display(),
                        staging.display()
                    )));
                }
                let _ = fs::remove_dir_all(&staging);
                return Err(Error::FailedPrecondition(format!(
                    "the restore replayed whole, but {} could not take {}'s place: {error}",
                    data.display(),
                    staging.display()
                )));
            }
            Ok(report)
        }
        Err(error) => {
            // The run's audit lines (its intent, and any line after it)
            // went into the staging store's audit file: carry them back
            // to the target before the scratch goes, so a failed restore
            // leaves the evidence an auditor needs. The caller's audit
            // handle holds the file's fd, so the failed line it writes
            // next lands in the carried-back file.
            if !carry_back_audit(&staging, data) {
                return Err(Error::FailedPrecondition(format!(
                    "the replay failed, and the run's audit file could not move back to {}: \
                     the run's lines stay in {}: {error}",
                    data.display(),
                    staging.display()
                )));
            }
            let _ = fs::remove_dir_all(&staging);
            // On Postgres the replay committed whole batches before it
            // failed: drop the schema they sit in, best effort, so the
            // operator's retry against the same database starts empty.
            if let Some(url) = postgres_url
                && let Err(why) = crate::pg::reset_own_staging(url, postgres_ca)
            {
                tracing::warn!(error = %why, "cannot drop the restored database's schema");
            }
            Err(error)
        }
    }
}

/// Moves the staging store's audit file back to the target on a failed
/// restore: it is the record of the run that failed, and the scratch
/// must not take it. Returns whether the staging directory may go: it
/// stays when it still holds the audit file's only copy.
fn carry_back_audit(staging: &Path, data: &Path) -> bool {
    let staged = staging.join("audit.log");
    if !staged.exists() {
        return true;
    }
    fs::rename(&staged, data.join("audit.log")).is_ok()
}

/// The replay itself, into whatever directory the caller named: the
/// publication (the rename into the target) is [`restore`]'s, so a
/// failure here cannot leave anything behind.
fn restore_into_with(
    archive: &Path,
    data: &Path,
    postgres_url: Option<&str>,
    postgres_ca: Option<&Path>,
    tx_bytes: usize,
    open: impl FnOnce(&Path, Option<&str>, Option<&Path>) -> Result<Backend>,
) -> Result<RestoreReport> {
    let manifest = read_manifest(archive)?;
    let names = repo_names(&manifest)?;
    // One check path: the same verify a reader runs, before any write. A
    // damage here refuses the replay, and names itself.
    let damages = verify(archive)?;
    if !damages.is_empty() {
        return Err(Error::Corrupt(format!(
            "the archive is not sound; verify names {} damages, the first: {}",
            damages.len(),
            damages[0]
        )));
    }
    let store = open(data, postgres_url, postgres_ca)?;
    let small_sections = stage_sections(archive, &store, &names)?;
    let mut refs_put = 0u64;
    let mut lands_put = 0u64;
    let mut txs = 0u64;
    // One write transaction per byte budget of changeset bodies, per
    // repo: the frames stream off changesets.bin one record at a time,
    // and a transaction commits when its budget is spent. The archive
    // writes the frames in generation order (a parent always has a
    // smaller generation), so a child's parent is committed in this or
    // an earlier transaction; the restore is the sole writer of a fresh
    // store, so the intermediate states are never visible. The
    // publication - the rename of the staging directory into the target
    // - is the restore's all-or-nothing point, and it happens after the
    // last transaction.
    let last_repo = small_sections.last().map(|(name, ..)| name.as_str());
    for (name, members, refs, lands, provenance) in &small_sections {
        let (repo_id, repo_txs) = replay_changesets(&store, archive, name, members, tx_bytes)?;
        txs += repo_txs;
        store.write(|c| {
            for (ref_name, target, version) in refs {
                c.restore_ref(repo_id, ref_name, target.as_bytes(), *version)?;
                refs_put += 1;
            }
            for land in lands {
                lands_put += 1;
                c.record_land(
                    &Caller(land.caller.clone()),
                    &LandRequest {
                        repo: name.clone(),
                        changeset: land.input_cs,
                        target_ref: land.target_ref.clone(),
                        request_key: land.request_key.clone(),
                        deadline: Deadline::new(
                            std::time::Instant::now() + Duration::from_secs(600),
                        ),
                    },
                    None,
                )?;
            }
            c.put_git_imports(repo_id, provenance)?;
            // Publication is atomic with this transaction on Postgres:
            // the store that answers reads must never carry the staging
            // mark the next restore would drop it by - and only once,
            // in the LAST repo's transaction: a mark cleared by an
            // earlier repo's publication would leave a kill between
            // repos with partial data and no mark, which the next
            // restore would misread as a complete store.
            if Some(name.as_str()) == last_repo {
                c.clear_restore_mark()?;
            }
            Ok(())
        })?;
        txs += 1;
        // The kill seam of the between-repos test: the process dies
        // after this repo's publication, with no failure path, the way
        // a kill lands. The mark is (or is not) whatever the code left.
        #[cfg(any(test, feature = "test-hooks"))]
        if std::env::var("SCION_TEST_RESTORE_EXIT_AFTER").as_deref() == Ok(name.as_str()) {
            std::process::exit(9);
        }
    }
    if small_sections.is_empty() {
        // No repos, no loop, no publication transaction to carry the
        // clear: the mark leaves in a transaction of its own. Nothing
        // is partial with zero repos - an empty store is a complete
        // store, and clients will write repos into it.
        store.write(|c| c.clear_restore_mark())?;
        txs += 1;
    }
    note_restore_txs(txs);
    Ok(RestoreReport {
        repos: names,
        refs: refs_put,
        lands: lands_put,
    })
}

/// One repo's small sections as the replay holds them: the members'
/// blob IDs, the refs with the versions the export read, the pending
/// lands, and the git provenance.
type SmallSections = Vec<(
    String,
    Vec<BlobId>,
    Vec<(String, ChangesetId, i64)>,
    Vec<ArchiveLand>,
    Vec<(ObjectId, ChangesetId)>,
)>;

/// The replay's staging pass: every repo's small sections read, and
/// every member blob put through the store's own write path (a put of
/// the raw bytes writes the file with its durability dance, and the ID
/// is the hash of the raw bytes, so the IDs carry over). The blobs are
/// files under the staging directory the publication renames, so a
/// failure here is the staging drop's, and no row was written yet.
fn stage_sections(archive: &Path, store: &Backend, names: &[String]) -> Result<SmallSections> {
    let mut small_sections = Vec::new();
    for name in names {
        let dir = archive.join("repos").join(name);
        let members = read_members(&dir.join("members.txt"))?;
        let refs = read_refs(&dir.join("refs.json"))?;
        let lands = read_lands(&dir.join("lands.json"))?;
        let provenance = read_provenance(&dir.join("provenance.json"))?;
        for member in &members {
            let path = blob_path(archive, member);
            let file = fs::File::open(&path)?;
            let raw = decode_blob(&path, file)?;
            let stored = store.blobs().put(&raw)?;
            if stored != *member {
                return Err(Error::Corrupt(format!(
                    "the archive's blob {member} holds other bytes than its name says"
                )));
            }
        }
        small_sections.push((name.clone(), members, refs, lands, provenance));
    }
    Ok(small_sections)
}

/// One repo's changeset replay: the repo row and its lens in one
/// transaction, then one write transaction per byte budget of
/// changeset bodies. The frames stream off changesets.bin one record
/// at a time - or, when the ceiling job's mutation lever is flipped,
/// the whole file sits in memory first, the whole-history read the
/// stream replaced. The archive writes the frames in generation order
/// (a parent always has a smaller generation), so a child's parent is
/// committed in this or an earlier transaction; the restore is the
/// sole writer of a fresh store, so the intermediate states are never
/// visible. Returns the repo's id and the transactions it used.
fn replay_changesets(
    store: &Backend,
    archive: &Path,
    name: &str,
    members: &[BlobId],
    tx_bytes: usize,
) -> Result<(RepoId, u64)> {
    let mut txs = 0u64;
    let repo_id = store.write(|c| c.create_repo(name))?;
    txs += 1;
    // The mutation lever the ceiling test flips (SCION_TEST_RESTORE_MUTATION
    // = whole_file, test-hooks only): the replay reads the whole
    // changesets.bin into memory before it stores a row, which is
    // exactly the whole-history read the streaming replay replaced.
    // The ceiling job runs the test once without the lever (it must
    // pass) and once with it (it must fail), so a guard that a
    // whole-file read slips under fails the run instead of the
    // nightly's silence.
    let mut source = if mutation_whole_file() {
        ReplayFrames::Whole(
            read_changesets(
                &Path::new(archive)
                    .join("repos")
                    .join(name)
                    .join("changesets.bin"),
            )?
            .into_iter(),
        )
    } else {
        ReplayFrames::Stream(Frames::open(
            &Path::new(archive)
                .join("repos")
                .join(name)
                .join("changesets.bin"),
        )?)
    };
    let mut pending = source.next()?;
    let mut batch_no = 0u64;
    while pending.is_some() {
        let mut spent = 0usize;
        let mut batch: Vec<Changeset> = Vec::new();
        while let Some((_id, _generation, body)) = pending.take() {
            let (cs, _) = Changeset::decode_with_id(&body)
                .map_err(|error| Error::Corrupt(format!("a stored changeset body: {error}")))?;
            spent += body.len();
            batch.push(cs);
            pending = source.next()?;
            if spent >= tx_bytes {
                break;
            }
        }
        store.write(|c| {
            // The lens the tree building needs: the size of every
            // member blob, read from the files the put above just
            // made durable.
            let mut lens = HashMap::new();
            for member in members {
                lens.insert(*member, store.blobs().size(*member)? as u64);
            }
            for cs in &batch {
                tree::store_changeset(c, repo_id, cs, &mut tree::lens_from(&lens))?;
            }
            Ok(())
        })?;
        txs += 1;
        batch_no += 1;
        restore_batch_note(name, batch_no);
    }
    Ok((repo_id, txs))
}

/// The default replay transaction: one transaction per 64 MiB of
/// changeset bodies, the import batches' shape. A restore of a
/// gigabyte-scale archive commits tens of transactions, not one, and
/// holds one changeset in memory at a time.
const DEFAULT_TX_BYTES: usize = 64 * 1024 * 1024;

/// The replay's transaction byte budget: `SCION_TEST_RESTORE_TX_BYTES` when
/// a test's child process names one, the caller's budget otherwise.
/// Test-hooks only: a default build never reads the environment, so a
/// value set around a release binary cannot change a restore.
#[cfg(any(test, feature = "test-hooks"))]
fn replay_tx_bytes(asked: usize) -> usize {
    std::env::var("SCION_TEST_RESTORE_TX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(asked)
}

/// The default build: the caller's budget, and the environment unread.
#[cfg(not(any(test, feature = "test-hooks")))]
fn replay_tx_bytes(asked: usize) -> usize {
    asked
}

/// One line per committed batch when the operator asks for it: a
/// restore's progress, and the seam a test watches before it kills the
/// replay.
fn restore_batch_note(name: &str, batch: u64) {
    tracing::debug!(repo = name, batch, "restore: batch committed");
    if batch.is_multiple_of(1000) {
        tracing::info!(repo = name, batch, "restore: batch committed");
    }
    // The echo is the seam a test watches before it kills the replay:
    // test-hooks only, so a default build's stderr is the operator's,
    // whatever the environment says.
    #[cfg(any(test, feature = "test-hooks"))]
    if std::env::var_os("SCION_TEST_RESTORE_ECHO_BATCH").is_some() {
        eprintln!("restore: {name} batch {batch} committed");
    }
}

/// The transactions the restores of this process committed, for the
/// tests that must see more than one.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) static RESTORE_TXS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(any(test, feature = "test-hooks"))]
fn note_restore_txs(txs: u64) {
    RESTORE_TXS.fetch_add(txs, std::sync::atomic::Ordering::Relaxed);
    tracing::info!(txs, "restore: replay committed its transactions");
}

#[cfg(not(any(test, feature = "test-hooks")))]
fn note_restore_txs(txs: u64) {
    tracing::info!(txs, "restore: replay committed its transactions");
}

/// Undoes the identity a failed restore's open minted in the database:
/// a fresh store names no store, and the next open mints again. The
/// tables the migration made stay (they are empty, and the next open's
/// migration is idempotent); only the name goes.
/// The archive's blob file of `id`, in the store's own fan-out shape.
fn blob_path(archive: &Path, id: &BlobId) -> PathBuf {
    let hex = id.to_string();
    archive.join("blobs").join(&hex[..2]).join(&hex[2..])
}

/// The store's blob file of `id`.
fn stored_blob_path(data: &Path, id: &BlobId) -> PathBuf {
    let hex = id.to_string();
    data.join("objects")
        .join("blob")
        .join(&hex[..2])
        .join(&hex[2..])
}

/// Copies the blob files of `ids` from the store's object folder into the
/// archive, byte for byte: the files are immutable, and the archive holds
/// them in the same shape, so a reader needs no store code for them.
fn copy_blobs(data: &Path, archive: &Path, ids: &[BlobId]) -> Result<()> {
    for id in ids {
        let from = stored_blob_path(data, id);
        let to = blob_path(archive, id);
        make_dir(to.parent().expect("a blob file has a folder"))?;
        fs::copy(&from, &to)
            .map_err(|error| Error::Corrupt(format!("cannot copy {}: {error}", from.display())))?;
    }
    Ok(())
}

/// The metadata of `name`, as ONE read transaction holds it: the
/// snapshot every section of an export shares. The explicit read
/// transaction is what makes the claim true on SQLite: each statement
/// would otherwise see its own snapshot, and a land that commits
/// between them would leave a ref naming a changeset the archive does
/// not hold.
fn snapshot_section(store: &Backend, name: &str) -> Result<RepoSection> {
    store.read_snapshot(|tx| {
        let repo_id = tx.repo_id(name)?;
        let changesets = tx
            .repo_changesets_with_bodies(repo_id)?
            .into_iter()
            .map(|(id, generation, body)| {
                let id = ChangesetId::try_from(id.as_slice())
                    .map_err(|error| Error::Corrupt(format!("a stored changeset ID: {error}")))?;
                Ok((id, generation, body))
            })
            .collect::<Result<Vec<_>>>()?;
        // A test's writer, called between the section's statements: what
        // it commits must stay outside this section's snapshot.
        #[cfg(any(test, feature = "test-hooks"))]
        if let Some(hook) = take_section_hook() {
            hook();
        }
        let refs = tx
            .list_refs(repo_id)?
            .into_iter()
            .map(|(name, target, version)| {
                let target = ChangesetId::try_from(target.as_slice())
                    .map_err(|error| Error::Corrupt(format!("a stored ref target: {error}")))?;
                Ok((name, target, version))
            })
            .collect::<Result<Vec<_>>>()?;
        let lands = tx.pending_lands(repo_id)?;
        let members = tx
            .repo_blob_ids(repo_id)?
            .into_iter()
            .map(|bytes| {
                BlobId::try_from(bytes.as_slice())
                    .map_err(|error| Error::Corrupt(format!("a stored blob ID: {error}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let provenance = tx
            .imported_commits(repo_id)?
            .into_iter()
            .map(|(oid, cs)| (oid.to_string(), cs))
            .collect();
        Ok(RepoSection {
            name: name.to_owned(),
            changesets,
            refs,
            lands,
            members,
            provenance,
        })
    })
}

/// The counts one written section reports, and the manifest line for it.
struct RepoReport {
    changesets: u64,
    refs: u64,
    lands: u64,
    members: u64,
    json: serde_json::Value,
}

/// Writes the files of one repo section, and copies its blob files.
fn write_section(
    data: &Path,
    out: &Path,
    section: &RepoSection,
    made: &mut HashMap<(u64, u64), String>,
) -> Result<RepoReport> {
    let dir = out.join("repos").join(&section.name);
    // One non-recursive create per section: on a disk that folds names
    // (case, Unicode normalization) a second repo whose name folds into
    // an earlier one's lands in the SAME directory, and its files
    // overwrite the first's — the archive would be corrupt at write
    // time, silently. AlreadyExists is the fold, and the export names
    // both names and stops.
    if let Err(error) = fs::DirBuilder::new().create(&dir) {
        if error.kind() == io::ErrorKind::AlreadyExists {
            let partner = same_directory(made, &dir, &section.name)?;
            return Err(Error::Corrupt(format!(
                "the repos {partner} and {} fold to one directory on this disk; \
                 export to a case-sensitive disk",
                section.name
            )));
        }
        return Err(Error::Corrupt(format!(
            "cannot make {}: {error}",
            dir.display()
        )));
    }
    let identity = directory_identity(&dir)?;
    made.insert(identity, section.name.clone());
    write_changesets(&dir.join("changesets.bin"), &section.changesets)?;
    let refs_json: Vec<serde_json::Value> = section
        .refs
        .iter()
        .map(|(name, target, version)| {
            serde_json::json!({
                "name": name,
                "target": target.to_string(),
                "version": version,
            })
        })
        .collect();
    write_json(&dir.join("refs.json"), &serde_json::Value::Array(refs_json))?;
    let lands_json: Vec<serde_json::Value> = section
        .lands
        .iter()
        .map(|land| {
            serde_json::json!({
                "caller": land.caller,
                "request_key": land.request_key,
                "target_ref": land.target_ref,
                "input_cs": hex(&land.input_cs),
            })
        })
        .collect();
    write_json(
        &dir.join("lands.json"),
        &serde_json::Value::Array(lands_json),
    )?;
    write_members(&dir.join("members.txt"), &section.members)?;
    let provenance_json: Vec<serde_json::Value> = section
        .provenance
        .iter()
        .map(|(oid, cs)| serde_json::json!({ "oid": oid, "cs": cs.to_string() }))
        .collect();
    write_json(
        &dir.join("provenance.json"),
        &serde_json::Value::Array(provenance_json),
    )?;
    copy_blobs(data, out, &section.members)?;
    Ok(RepoReport {
        changesets: section.changesets.len() as u64,
        refs: section.refs.len() as u64,
        lands: section.lands.len() as u64,
        members: section.members.len() as u64,
        json: serde_json::json!({
            "name": section.name,
            "changesets": section.changesets.len(),
            "refs": section.refs.len(),
            "pending_lands": section.lands.len(),
            "members": section.members.len(),
            "provenance": section.provenance.len(),
        }),
    })
}

/// Writes the changeset stream: one record per changeset, `[u32 body]
/// [32 id][i64 generation][body]`, little-endian, in generation order.
fn write_changesets(path: &Path, changesets: &[(ChangesetId, i64, Vec<u8>)]) -> Result<()> {
    let mut file = fs::File::create(path)?;
    for (id, generation, body) in changesets {
        let len = u32::try_from(body.len())
            .map_err(|_| Error::InvalidArgument("a changeset body of over 4 GiB".to_owned()))?;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(id.as_bytes())?;
        file.write_all(&generation.to_le_bytes())?;
        file.write_all(body)?;
    }
    file.flush()?;
    Ok(())
}

/// Reads the changeset stream back, one frame at a time: the verify and
/// the restore hold one record, never the file.
pub(crate) struct Frames {
    inner: io::BufReader<fs::File>,
    path: std::path::PathBuf,
    /// The file's whole length, read once at open: a record whose
    /// claimed body length runs past the file's end is torn, and the
    /// claim never becomes an allocation.
    remaining: u64,
}

impl Frames {
    /// Opens the framed stream of `path`.
    ///
    /// # Errors
    ///
    /// Why the file cannot be opened.
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let file = fs::File::open(path)?;
        let remaining = file.metadata()?.len();
        Ok(Self {
            inner: io::BufReader::new(file),
            path: path.to_path_buf(),
            remaining,
        })
    }

    /// The next frame: `(id, generation, body)`, or `None` at the end.
    ///
    /// # Errors
    ///
    /// Why the bytes are not a whole frame: the file ends inside one.
    pub(crate) fn next(&mut self) -> Result<Option<(ChangesetId, i64, Vec<u8>)>> {
        let mut prefix = [0_u8; 44];
        let mut at = 0_usize;
        loop {
            let read = self.inner.read(&mut prefix[at..])?;
            if read == 0 {
                break;
            }
            at += read;
        }
        if at == 0 {
            return Ok(None);
        }
        self.remaining -= at as u64;
        if at < prefix.len() {
            return Err(torn(&self.path));
        }
        let len = u32::from_le_bytes(prefix[0..4].try_into().map_err(|_| torn(&self.path))?);
        let len = usize::try_from(len).map_err(|_| torn(&self.path))?;
        let generation =
            i64::from_le_bytes(prefix[36..44].try_into().map_err(|_| torn(&self.path))?);
        // The length is a claim from the bytes: a claim past the file's
        // own end is a torn record, and the file's tail is never read
        // into a 4 GiB buffer of zeros to find that out.
        if len as u64 > self.remaining {
            return Err(torn(&self.path));
        }
        let mut body = vec![0_u8; len];
        self.inner
            .read_exact(&mut body)
            .map_err(|_| torn(&self.path))?;
        self.remaining -= len as u64;
        let id = ChangesetId::try_from(&prefix[4..36]).map_err(|error| {
            Error::Corrupt(format!(
                "{} holds a bad changeset ID: {error}",
                self.path.display()
            ))
        })?;
        Ok(Some((id, generation, body)))
    }
}

fn torn(path: &Path) -> Error {
    Error::Corrupt(format!("{} ends inside a record", path.display()))
}

/// One record's bytes off the stream, for the readers that still walk a
/// whole file at once (the golden reader of the v1 format, and the
/// replay's whole-file mutation lever).
pub(crate) fn read_changesets(path: &Path) -> Result<Vec<(ChangesetId, i64, Vec<u8>)>> {
    let mut frames = Frames::open(path)?;
    let mut out = Vec::new();
    while let Some(frame) = frames.next()? {
        out.push(frame);
    }
    Ok(out)
}

/// Writes the member list: one 64-hex-digit blob ID per line.
fn write_members(path: &Path, members: &[BlobId]) -> Result<()> {
    let mut file = fs::File::create(path)?;
    for member in members {
        writeln!(file, "{member}")?;
    }
    file.flush()?;
    Ok(())
}

/// Reads the member list back.
pub(crate) fn read_members(path: &Path) -> Result<Vec<BlobId>> {
    let text = fs::read_to_string(path)?;
    text.lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let bytes = decode_hex(line).ok_or_else(|| {
                Error::Corrupt(format!(
                    "{} holds a line that is not a blob ID",
                    path.display()
                ))
            })?;
            BlobId::try_from(bytes.as_slice()).map_err(|error| {
                Error::Corrupt(format!("{} holds a bad blob ID: {error}", path.display()))
            })
        })
        .collect()
}

/// Writes the refs file: `[{name, target, version}]`.
pub(crate) fn read_refs(path: &Path) -> Result<Vec<(String, ChangesetId, i64)>> {
    let value = read_json(path)?;
    let rows = value.as_array().ok_or_else(|| corrupt_json(path))?;
    rows.iter()
        .map(|row| {
            let name = row["name"]
                .as_str()
                .ok_or_else(|| corrupt_json(path))?
                .to_owned();
            let target = decode_hex(row["target"].as_str().ok_or_else(|| corrupt_json(path))?)
                .ok_or_else(|| corrupt_json(path))?;
            let target = ChangesetId::try_from(target.as_slice()).map_err(|error| {
                Error::Corrupt(format!(
                    "{} holds a bad ref target: {error}",
                    path.display()
                ))
            })?;
            let version = row["version"].as_i64().ok_or_else(|| corrupt_json(path))?;
            Ok((name, target, version))
        })
        .collect()
}

/// Reads the lands file back.
pub(crate) fn read_lands(path: &Path) -> Result<Vec<ArchiveLand>> {
    let value = read_json(path)?;
    let rows = value.as_array().ok_or_else(|| corrupt_json(path))?;
    rows.iter()
        .map(|row| {
            Ok(ArchiveLand {
                caller: row["caller"]
                    .as_str()
                    .ok_or_else(|| corrupt_json(path))?
                    .to_owned(),
                request_key: row["request_key"]
                    .as_str()
                    .ok_or_else(|| corrupt_json(path))?
                    .to_owned(),
                target_ref: row["target_ref"]
                    .as_str()
                    .ok_or_else(|| corrupt_json(path))?
                    .to_owned(),
                input_cs: ChangesetId::try_from(
                    decode_hex(row["input_cs"].as_str().ok_or_else(|| corrupt_json(path))?)
                        .ok_or_else(|| corrupt_json(path))?
                        .as_slice(),
                )
                .map_err(|error| {
                    Error::Corrupt(format!(
                        "{} holds a bad land input: {error}",
                        path.display()
                    ))
                })?,
            })
        })
        .collect()
}

/// Reads the provenance file back.
pub(crate) fn read_provenance(path: &Path) -> Result<Vec<(ObjectId, ChangesetId)>> {
    let value = read_json(path)?;
    let rows = value.as_array().ok_or_else(|| corrupt_json(path))?;
    rows.iter()
        .map(|row| {
            let oid = row["oid"].as_str().ok_or_else(|| corrupt_json(path))?;
            let cs = row["cs"].as_str().ok_or_else(|| corrupt_json(path))?;
            Ok((
                ObjectId::from_hex(oid.as_bytes()).map_err(|_| corrupt_json(path))?,
                ChangesetId::try_from(decode_hex(cs).ok_or_else(|| corrupt_json(path))?.as_slice())
                    .map_err(|error| {
                        Error::Corrupt(format!(
                            "{} holds a bad changeset ID: {error}",
                            path.display()
                        ))
                    })?,
            ))
        })
        .collect()
}

fn write_json(path: &Path, value: &serde_json::Value) -> Result<()> {
    let mut file = fs::File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)
        .map_err(|error| Error::Corrupt(format!("cannot write {}: {error}", path.display())))?;
    file.flush()?;
    Ok(())
}

fn read_json(path: &Path) -> Result<serde_json::Value> {
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| Error::Corrupt(format!("{} does not parse: {error}", path.display())))
}

pub(crate) fn read_manifest(archive: &Path) -> Result<serde_json::Value> {
    let manifest = read_json(&archive.join("manifest.json"))?;
    if manifest["format"] != "scion-archive" {
        return Err(Error::FailedPrecondition(format!(
            "{} is not a scion archive",
            archive.display()
        )));
    }
    if manifest["version"] != 1 {
        return Err(Error::FailedPrecondition(format!(
            "archive version {} is not 1; this reader refuses it",
            manifest["version"]
        )));
    }
    Ok(manifest)
}

fn repo_names(manifest: &serde_json::Value) -> Result<Vec<String>> {
    let repos = manifest["repos"]
        .as_array()
        .ok_or_else(|| Error::Corrupt("the manifest holds no repos".to_owned()))?;
    repos
        .iter()
        .map(|repo| {
            let name = repo["name"]
                .as_str()
                .ok_or_else(|| Error::Corrupt("a manifest repo has no name".to_owned()))?;
            repo_name(name)?;
            Ok(name.to_owned())
        })
        .collect()
}

/// Checks a repo name the archive holds before it ever becomes a path: the
/// live rule for names a client sends (`names::check`, the rule
/// `create_repo`'s callers apply), and then that the name is ONE path
/// component, so `../escape` and `/abs` cannot name a file outside the
/// archive. The name comes from outside the server: treat it as untrusted.
fn repo_name(name: &str) -> Result<()> {
    crate::names::check("repo", name)?;
    if name.contains('/') || name == "." || name == ".." {
        return Err(Error::Corrupt(format!(
            "the manifest's repo name {name:?} is not one path component"
        )));
    }
    Ok(())
}

/// The most a blob file may expand to: the server's own cap for one blob,
/// plus the byte the check of it needs. A small file that decompresses
/// past this is a bomb, and the reader refuses it without holding it.
fn blob_decode_cap() -> u64 {
    crate::store::MAX_OBJECT_BYTES as u64 + 1
}

/// Decompresses a stored blob file, bounded: at most [`blob_decode_cap`]
/// bytes leave the decoder, so a crafted frame cannot grow in memory past
/// the cap. The bytes past the cap are the refusal.
fn decode_blob(path: &Path, bytes: fs::File) -> Result<Vec<u8>> {
    use std::io::Read as _;
    let decoder = zstd::stream::Decoder::new(bytes)?;
    let mut raw = decoder.take(blob_decode_cap());
    let mut out = Vec::new();
    raw.read_to_end(&mut out)?;
    if out.len() > crate::store::MAX_OBJECT_BYTES {
        return Err(Error::Corrupt(format!(
            "{} expands past the cap of {} bytes",
            path.display(),
            crate::store::MAX_OBJECT_BYTES
        )));
    }
    Ok(out)
}

/// A local-time RFC 3339 stamp of now, to the second.
fn snapshot_instant() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let (days, rest) = (secs / 86_400, secs % 86_400);
    let (hour, minute, second) = (rest / 3_600, (rest / 60) % 60, rest % 60);
    // Civil-from-days, Howard Hinnant's algorithm.
    let z = i64::from(u32::try_from(days).unwrap_or(u32::MAX)) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// The (device, inode) of a directory: the disk's own answer to whether
/// two names name one directory.
fn directory_identity(dir: &Path) -> Result<(u64, u64)> {
    let meta = fs::symlink_metadata(dir)
        .map_err(|error| Error::Corrupt(format!("cannot read {}: {error}", dir.display())))?;
    Ok((meta.dev(), meta.ino()))
}

/// The name already recorded for the directory `dir` now stands for, if
/// one is: the fold partner that the export refuses to overwrite.
fn same_directory(made: &HashMap<(u64, u64), String>, dir: &Path, name: &str) -> Result<String> {
    let identity = directory_identity(dir)?;
    Ok(made
        .get(&identity)
        .cloned()
        .unwrap_or_else(|| name.to_owned()))
}

fn make_dir(dir: &Path) -> Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .create(dir)
        .map_err(|error| Error::Corrupt(format!("cannot make {}: {error}", dir.display())))?;
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::new(), |out, b| out + &format!("{b:02x}"))
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).ok())
        .collect()
}

fn corrupt_json(path: &Path) -> Error {
    Error::Corrupt(format!(
        "{} does not hold the shape the format says",
        path.display()
    ))
}
