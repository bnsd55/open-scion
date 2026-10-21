//! The versioned logical archive (`scion-archive-1`): a whole repository,
//! or the whole store, as a documented directory of files that `verify`
//! checks without the store's code and that `restore` replays into an
//! empty store. The format is `docs/archive-format.md`; this module is
//! its one writer and reader.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Write as _};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use gix::ObjectId;
use scion_core::{BlobId, Changeset, ChangesetId};

use crate::auth::Caller;
use crate::backend::Backend;
use crate::deadline::Deadline;
use crate::error::{Error, Result};
use crate::land::LandRequest;
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
        let changesets = read_changesets(&dir.join("changesets.bin"))?;
        let members = read_members(&dir.join("members.txt"))?;
        let member_set: HashSet<BlobId> = members.iter().copied().collect();
        let ids: HashSet<ChangesetId> = changesets.iter().map(|(id, _, _)| *id).collect();
        let mut decoded = Vec::new();
        for (id, generation, body) in &changesets {
            // The ID of a changeset is the hash of its body, so the
            // re-hash is the check the file names cannot lie about.
            match Changeset::decode_with_id(body) {
                Ok((cs, hashed)) if hashed == *id => decoded.push((*id, cs)),
                Ok(_) => damages.push(format!(
                    "repo {name}: changeset {id} does not hash to its ID"
                )),
                Err(error) => damages.push(format!(
                    "repo {name}: changeset {id} does not decode: {error}"
                )),
            }
            if *generation < 1 {
                damages.push(format!(
                    "repo {name}: changeset {id} has generation {generation}"
                ));
            }
        }
        for (id, cs) in &decoded {
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

/// Replays an archive into an empty store: the blob bytes go through the
/// store's own write path, so every object ID is the hash of its bytes;
/// the changesets replay in generation order, which is topological, so
/// the store derives the same generations, trees, member rows, and
/// root-tree rows it held; the refs go back with the versions the export
/// read; the pending lands are recorded again, so a retry with the same
/// request key resumes; the git provenance is put back. The tree bodies
/// are not in the archive: the replay's `store_changeset` rebuilds them.
/// Returns the repo names it restored.
///
/// The archive is input from outside the server: [`verify`] runs first,
/// and any damage refuses the replay, so the reader of the files and the
/// writer of the store can never disagree.
///
/// # Errors
///
/// A data directory that already holds a store or has no name, a failed
/// open, damage [`verify`] finds, or what the replay's writes return.: the blob bytes go through the
/// store's own write path, so every object ID is the hash of its bytes;
/// the data directory the caller named is filled by renaming a staging
/// directory beside it into place only when the whole replay held — a
/// restore that fails, for any reason, leaves the target as it found it:
/// an operator cannot tell restored state from unrestored, so a half
/// store is the one thing this must never leave.
pub fn restore(
    archive: &Path,
    data: &Path,
    postgres_url: Option<&str>,
    postgres_ca: Option<&Path>,
) -> Result<Vec<String>> {
    restore_impl(archive, data, postgres_url, postgres_ca, |path, url, ca| {
        Backend::open(path, url, ca)
    })
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
) -> Result<Vec<String>> {
    restore_impl(archive, data, postgres_url, postgres_ca, open)
}

/// The whole restore — preflight, verify, the staging choreography, the
/// rename — with the opener a parameter. Private: the public [`restore`]
/// and the test hook are its only callers.
fn restore_impl(
    archive: &Path,
    data: &Path,
    postgres_url: Option<&str>,
    postgres_ca: Option<&Path>,
    open: impl FnOnce(&Path, Option<&str>, Option<&Path>) -> Result<Backend>,
) -> Result<Vec<String>> {
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
        // that died mid-replay, never a store an operator made.
        fs::remove_dir_all(&staging)?;
    }
    fs::create_dir_all(&staging)?;
    let replayed = restore_into_with(archive, &staging, postgres_url, postgres_ca, open);
    match replayed {
        Ok(names) => {
            if let Err(error) = fs::rename(&staging, data) {
                let _ = fs::remove_dir_all(&staging);
                return Err(Error::FailedPrecondition(format!(
                    "the restore replayed whole, but {} could not take {}'s place: {error}",
                    data.display(),
                    staging.display()
                )));
            }
            Ok(names)
        }
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            // On Postgres the store's identity was minted by this
            // restore's open (a database that already named a store
            // refuses a marker-less staging directory), so the failed
            // replay leaves only that mint behind. Drop it, best
            // effort, so the operator's retry against the same
            // database is refused by nothing.
            if let Err(why) = reset_postgres_identity(postgres_url, postgres_ca) {
                tracing::warn!(error = %why, "cannot reset the restored database's identity");
            }
            Err(error)
        }
    }
}

/// The replay itself, into whatever directory the caller named: the
/// publication (the rename into the target) is [`restore`]'s, so a
/// failure here cannot leave anything behind.
fn restore_into_with(
    archive: &Path,
    data: &Path,
    postgres_url: Option<&str>,
    postgres_ca: Option<&Path>,
    open: impl FnOnce(&Path, Option<&str>, Option<&Path>) -> Result<Backend>,
) -> Result<Vec<String>> {
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
    // The sections and the blobs first, all of them: the blobs are
    // files under `data` (the staging directory the publication
    // renames), so a failure here is the staging drop's, and no row
    // was written yet.
    let mut sections = Vec::new();
    for name in &names {
        let dir = archive.join("repos").join(name);
        let changesets = read_changesets(&dir.join("changesets.bin"))?;
        let members = read_members(&dir.join("members.txt"))?;
        let refs = read_refs(&dir.join("refs.json"))?;
        let lands = read_lands(&dir.join("lands.json"))?;
        let provenance = read_provenance(&dir.join("provenance.json"))?;

        // The blobs go through the store's own write path: a put of the
        // raw bytes writes the file with its durability dance, and the ID
        // is the hash of the raw bytes, so the IDs carry over.
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
        sections.push((name.clone(), changesets, members, refs, lands, provenance));
    }
    // One write transaction for the whole metadata replay, every repo:
    // the changesets replay in generation order (a parent always has a
    // smaller generation, so every parent is in before its child), and
    // a failure anywhere rolls back every row, on either backend — the
    // database a restore was replaying into comes out as it was found,
    // not half-named. The restore opens a fresh store, so the
    // transaction is the only writer and its length is the restore's.
    store.write(|c| {
        for (name, changesets, members, refs, lands, provenance) in &sections {
            let repo_id = c.create_repo(name)?;
            // The lens the tree building needs: the size of every member
            // blob, read from the files the put above just made durable.
            let mut lens = HashMap::new();
            for member in members {
                lens.insert(*member, store.blobs().size(*member)? as u64);
            }
            for (_id, _gen, body) in changesets {
                let (cs, _) = Changeset::decode_with_id(body)
                    .map_err(|error| Error::Corrupt(format!("a stored changeset body: {error}")))?;
                tree::store_changeset(c, repo_id, &cs, &mut tree::lens_from(&lens))?;
            }
            for (ref_name, target, version) in refs {
                c.restore_ref(repo_id, ref_name, target.as_bytes(), *version)?;
            }
            for land in lands {
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
            for (oid, cs) in provenance {
                c.put_git_import(repo_id, *oid, *cs)?;
            }
        }
        Ok(())
    })?;
    Ok(names)
}

/// Undoes the identity a failed restore's open minted in the database:
/// a fresh store names no store, and the next open mints again. The
/// tables the migration made stay (they are empty, and the next open's
/// migration is idempotent); only the name goes.
fn reset_postgres_identity(url: Option<&str>, ca: Option<&Path>) -> Result<()> {
    let Some(url) = url else { return Ok(()) };
    crate::pg::reset_identity(url, ca)
}

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
/// snapshot every section of an export shares.
fn snapshot_section(store: &Backend, name: &str) -> Result<RepoSection> {
    store.read(|tx| {
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

/// Reads the changeset stream back.
//
// SHORTCUT: the whole stream is read into memory, so a restore holds one
// repo's full history (bodies) in RAM. Ceiling: a repo whose history is
// gigabytes needs gigabytes. Upgrade path: stream the frames from a
// `BufReader`, one record at a time, into the replay transaction.
pub(crate) fn read_changesets(path: &Path) -> Result<Vec<(ChangesetId, i64, Vec<u8>)>> {
    let bytes = fs::read(path)?;
    let mut out = Vec::new();
    let mut at = 0_usize;
    while at < bytes.len() {
        let end = at + 4;
        let len = u32::from_le_bytes(
            bytes
                .get(at..end)
                .ok_or_else(|| torn(path))?
                .try_into()
                .map_err(|_| torn(path))?,
        );
        let len = usize::try_from(len).map_err(|_| torn(path))?;
        let end = end + 32;
        let id = bytes.get(at + 4..end).ok_or_else(|| torn(path))?.to_vec();
        let end = end + 8;
        let generation = i64::from_le_bytes(
            bytes
                .get(at + 36..end)
                .ok_or_else(|| torn(path))?
                .try_into()
                .map_err(|_| torn(path))?,
        );
        let end = end + len;
        let body = bytes.get(at + 44..end).ok_or_else(|| torn(path))?.to_vec();
        let id = ChangesetId::try_from(id.as_slice()).map_err(|error| {
            Error::Corrupt(format!(
                "{} holds a bad changeset ID: {error}",
                path.display()
            ))
        })?;
        out.push((id, generation, body));
        at = end;
    }
    Ok(out)
}

fn torn(path: &Path) -> Error {
    Error::Corrupt(format!("{} ends inside a record", path.display()))
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
