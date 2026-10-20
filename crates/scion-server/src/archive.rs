//! The versioned logical archive (`scion-archive-1`): a whole repository,
//! or the whole store, as a documented directory of files that `verify`
//! checks without the store's code and that `restore` replays into an
//! empty store. The format is `docs/archive-format.md`; this module is
//! its one writer and reader.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write as _;
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
struct ArchiveLand {
    caller: String,
    request_key: String,
    target_ref: String,
    input_cs: ChangesetId,
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
    for name in &names {
        let section = snapshot_section(store, name)?;

        let report = write_section(data, out, &section)?;
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
    for name in &names {
        let dir = archive.join("repos").join(name);
        let changesets = read_changesets(&dir.join("changesets.bin"))?;
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
        }
        for (ref_name, target, _version) in read_refs(&dir.join("refs.json"))? {
            if !ids.contains(&target) {
                damages.push(format!(
                    "repo {name}: the ref {ref_name} names {target}, which the archive does not hold"
                ));
            }
        }
        for land in read_lands(&dir.join("lands.json"))? {
            if !ids.contains(&land.input_cs) {
                damages.push(format!(
                    "repo {name}: the land with request key {} names input {}, which the archive does not hold",
                    land.request_key,
                    land.input_cs
                ));
            }
        }
        for member in read_members(&dir.join("members.txt"))? {
            let path = blob_path(archive, &member);
            match fs::read(&path) {
                Ok(bytes) => {
                    let raw = zstd::decode_all(bytes.as_slice()).map_err(|error| {
                        Error::Corrupt(format!("{} does not decompress: {error}", path.display()))
                    })?;
                    if BlobId::of(&raw) != member {
                        damages.push(format!(
                            "the blob file of {member} holds other bytes than its name says"
                        ));
                    }
                }
                Err(_) => damages.push(format!("the blob file of {member} is missing")),
            }
        }
    }
    Ok(damages)
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
/// # Errors
///
/// A data directory that already holds a store, a failed open, a blob
/// whose bytes do not hash to its name, or what the replay's writes
/// return.
pub fn restore(archive: &Path, data: &Path, postgres_url: Option<&str>) -> Result<Vec<String>> {
    for existing in ["scion.db", MARKER, "objects"] {
        if data.join(existing).exists() {
            return Err(Error::FailedPrecondition(format!(
                "{} already holds a store; a restore goes into an empty one",
                data.display()
            )));
        }
    }
    let manifest = read_manifest(archive)?;
    let names = repo_names(&manifest)?;
    let store = Backend::open(data, postgres_url, None)?;
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
            let bytes = fs::read(blob_path(archive, member))?;
            let raw = zstd::decode_all(bytes.as_slice())
                .map_err(|error| Error::Corrupt(format!("a blob does not decompress: {error}")))?;
            let stored = store.blobs().put(&raw)?;
            if stored != *member {
                return Err(Error::Corrupt(format!(
                    "the archive's blob {member} holds other bytes than its name says"
                )));
            }
        }
        let repo_id = store.write(|c| c.create_repo(name))?;
        // The lens the tree building needs: the size of every member blob,
        // read from the files the put above just made durable.
        let mut lens = HashMap::new();
        for member in &members {
            lens.insert(*member, store.blobs().size(*member)? as u64);
        }
        // One write transaction per repo: the changesets replay in
        // generation order, and a parent always has a smaller generation,
        // so every parent is in before its child.
        store.write(|c| {
            for (_id, _gen, body) in &changesets {
                let (cs, _) = Changeset::decode_with_id(body)
                    .map_err(|error| Error::Corrupt(format!("a stored changeset body: {error}")))?;
                tree::store_changeset(c, repo_id, &cs, &mut tree::lens_from(&lens))?;
            }
            Ok(())
        })?;
        store.write(|c| {
            for (ref_name, target, version) in &refs {
                c.restore_ref(repo_id, ref_name, target.as_bytes(), *version)?;
            }
            for land in &lands {
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
            for (oid, cs) in &provenance {
                c.put_git_import(repo_id, *oid, *cs)?;
            }
            Ok(())
        })?;
    }
    Ok(names)
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
fn write_section(data: &Path, out: &Path, section: &RepoSection) -> Result<RepoReport> {
    let dir = out.join("repos").join(&section.name);
    make_dir(&dir)?;
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
fn read_changesets(path: &Path) -> Result<Vec<(ChangesetId, i64, Vec<u8>)>> {
    let bytes = fs::read(path)?;
    let mut out = Vec::new();
    let mut at = 0_usize;
    while at < bytes.len() {
        let end = at + 4;
        let len = bytes
            .get(at..end)
            .ok_or_else(|| torn(path))?
            .iter()
            .rev()
            .fold(0_u64, |n, b| (n << 8) | u64::from(*b));
        let len = usize::try_from(len).map_err(|_| torn(path))?;
        let end = end + 32;
        let id = bytes.get(at + 4..end).ok_or_else(|| torn(path))?.to_vec();
        let end = end + 8;
        let generation = bytes
            .get(at + 36..end)
            .ok_or_else(|| torn(path))?
            .iter()
            .rev()
            .fold(0_i64, |n, b| (n << 8) | i64::from(*b));
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
fn read_members(path: &Path) -> Result<Vec<BlobId>> {
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
fn read_refs(path: &Path) -> Result<Vec<(String, ChangesetId, i64)>> {
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
fn read_lands(path: &Path) -> Result<Vec<ArchiveLand>> {
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
fn read_provenance(path: &Path) -> Result<Vec<(ObjectId, ChangesetId)>> {
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

fn read_manifest(archive: &Path) -> Result<serde_json::Value> {
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
            repo["name"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::Corrupt("a manifest repo has no name".to_owned()))
        })
        .collect()
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
