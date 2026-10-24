//! `mount` and `unmount`: the virtual checkout
//! ([#11](https://github.com/bnsd55/openscion/issues/11)).

use anyhow::{Context as _, Result, anyhow, bail};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use scion_core::RepoPath;
use scion_proto::v1::GetRootTreeRequest;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use crate::client::{Ctx, id_from};
use crate::outln;
use crate::vfs::{CHANGES, Change, Vfs};

use self::record::{MountRecord, write_sessions};

/// The export name of every mount.
const EXPORT: &str = "scion";

/// Serves a ref as a filesystem that reads files on demand.
///
/// For single-user machines only: any local user who can read the mount
/// table can reach the mounted content through the NFS port.
#[derive(clap::Args)]
pub struct MountArgs {
    /// Directory to mount the checkout on.
    dir: PathBuf,
    /// Ref name or changeset ID to serve.
    #[arg(default_value = "main")]
    rev: String,
    /// Serve NFS, but do not try to mount: print the command instead.
    #[arg(long)]
    no_mount: bool,
    /// Take writes on the mount, for the v0.1 unstable write path: a
    /// symlinked parent directory in the overlay can still redirect a
    /// write outside it, so the mount is read-only without this. A build
    /// without the `unstable-writable-mount` feature refuses the flag
    /// before it connects.
    #[arg(long)]
    unstable_writable: bool,
}

impl MountArgs {
    /// Whether the mount was asked to take writes, which only a build
    /// with the `unstable-writable-mount` feature serves.
    pub fn writable_requested(&self) -> bool {
        self.unstable_writable
    }
}

/// Arguments of `scn unmount`.
#[derive(clap::Args)]
pub struct UnmountArgs {
    /// A directory that `scn mount` mounted.
    dir: PathBuf,
}

/// The mount record and its session file: what one live mount is, and
/// how `mount`, `unmount` and `commit --from-mount` hand it over. The
/// module is crate-internal: the session file is the two commands'
/// contract, not the CLI's API, and the tests reach it through
/// `test_support::cmd::mount`.
pub(crate) mod record {
    use std::io::Write as _;
    use std::path::{Path, PathBuf};

    use anyhow::Result;

    /// One live mount, as the session file holds it.
    #[derive(Clone, Debug, PartialEq)]
    // The test seam re-exports this (test_support.cmd.mount), and
    // unreachable_pub does not see through a re-export.
    #[allow(unreachable_pub)]
    pub struct MountRecord {
        /// The mounted directory, absolute.
        pub dir: PathBuf,
        /// The overlay directory with the changes.
        pub overlay: PathBuf,
        /// The revision that the mount serves.
        pub rev: String,
        /// The process that serves the NFS server.
        pub pid: u32,
        /// Whether the mount takes writes: `--unstable-writable` sets it. A
        /// record that predates the flag was a read-only mount.
        pub writable: bool,
    }

    /// Writes `records` as the session file `file`. The write is atomic: a
    /// sibling temp file, made durable, renamed over the table. A crash mid
    /// write leaves the old table, not a torn one; a concurrent scn writes
    /// its own temp file, so a clobber is impossible.
    // The test seam re-exports this (test_support.cmd.mount), and
    // unreachable_pub does not see through a re-export.
    #[allow(unreachable_pub)]
    pub fn write_sessions(file: &Path, sessions: &[MountRecord]) -> Result<()> {
        let records: Vec<serde_json::Value> = sessions
            .iter()
            .map(|record| {
                serde_json::json!({
                    "dir": record.dir.display().to_string(),
                    "overlay": record.overlay.display().to_string(),
                    "rev": record.rev,
                    "pid": record.pid,
                    "writable": record.writable,
                })
            })
            .collect();
        let temp = file.with_extension(format!("json.{}.tmp", std::process::id()));
        let mut out = std::fs::File::create(&temp)?;
        out.write_all(&serde_json::to_vec(&records)?)?;
        out.sync_all()?;
        std::fs::rename(&temp, file)?;
        if let Ok(dir) = std::fs::File::open(file.parent().unwrap_or(Path::new("."))) {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}

/// Serves the ref over the network filesystem on a free loopback port,
/// mounts it, and
/// waits for a signal. The changes go to an overlay directory, which
/// `scn commit --from-mount DIR` commits.
pub async fn mount(ctx: &mut Ctx, args: MountArgs) -> Result<()> {
    let dir = std::path::absolute(&args.dir)?;
    // Refuse before anything is served: a missing mount point fails at
    // mount time anyway, and a second mount of a live dir would start a
    // second NFS server that no one can use.
    check_mount_point(&dir)?;
    if let Some(known) = live_record_for(&dir, &load_sessions()?, &serving) {
        bail!(
            "{}: scn already serves this directory (pid {}); stop it first: scn unmount {}",
            dir.display(),
            known.pid,
            dir.display()
        );
    }
    // A build without the writable-mount feature refused the flag in
    // `main`, before anything connected; the write path it guards is
    // compiled out of that build.
    let base = ctx.resolve(&args.rev).await?;
    // The server mints the mount's ticket: the read permission of the
    // whole walk below the root tree of the mounted changeset. It goes to
    // the server, never to a tier, which mints no tickets.
    let reply = ctx
        .roots
        .get_root_tree(GetRootTreeRequest {
            repo: ctx.repo.clone(),
            changeset_id: base.as_bytes().to_vec(),
        })
        .await
        .with_context(|| format!("cannot read the tree of {}", args.rev))?
        .into_inner();
    let root = id_from(&reply.tree_id)?;

    // The overlay holds the changes; it dies with the mount.
    let overlay = overlay_dir()?;
    let objects = ctx.objects.clone();
    let roots = ctx.roots.clone();
    let repo = ctx.repo.clone();
    let vfs = Vfs::new(
        objects,
        roots,
        repo,
        base,
        root,
        &reply.ticket,
        overlay.join(CHANGES),
    );
    #[cfg(feature = "unstable-writable-mount")]
    let vfs = vfs.writable(args.unstable_writable);
    let secret = vfs.secret_name();

    // The identity of the serving process: an exclusive lock on the
    // overlay's serve file, held for as long as this process lives. A
    // record is only alive while its holder's lock is held, so a reused
    // pid can neither block a mount nor take an unrelated signal. The
    // file opens with close-on-exec, as Rust opens files, so a child of
    // the server never inherits the lock.
    let _serve_lock = hold_serve_lock(&overlay)?;
    // The record exists before anything serves: a kill or a save error
    // in the serving window leaves a table that names the server, so
    // the next `scn mount` refuses and `scn unmount` finds it.
    save_session(&MountRecord {
        dir: dir.clone(),
        overlay: overlay.clone(),
        rev: args.rev.clone(),
        pid: std::process::id(),
        #[cfg(feature = "unstable-writable-mount")]
        writable: args.unstable_writable,
        #[cfg(not(feature = "unstable-writable-mount"))]
        writable: false,
    })?;
    // The bind is the one step that can still fail before serving: on
    // its failure, remove the record, so the next mount takes over.
    let mut listener = match NFSTcpListener::bind("127.0.0.1:0", vfs).await {
        Ok(listener) => listener,
        Err(e) => {
            let file = session_file()?;
            let _ = drop_session(&file, &dir);
            return Err(anyhow::Error::new(e).context("cannot listen for NFS"));
        }
    };
    // The printed mount target is /scion/{secret}; without this, the
    // default export is /, the mountd keeps the whole /scion/{secret}
    // path, and the VFS reads "scion" as the secret and refuses the mount.
    listener.with_export_name(EXPORT);
    let port = listener.get_listen_port();
    let server = tokio::spawn(async move {
        if let Err(e) = listener.handle_forever().await {
            eprintln!("scn mount: the NFS server stopped: {e}");
        }
    });

    outln!(
        "single-user machines only: any local user who can read the mount table can reach this mount"
    );
    outln!("{}", mount_command(port, &secret, &dir));
    if !args.no_mount {
        try_mount(port, &secret, &dir)?;
    }

    wait_stopped().await;
    drop(server);
    // Unmount before the overlay goes, so no file disappears mid-read.
    unmount_dir(&dir);
    for warning in cleanup_after_stop(&dir, &overlay) {
        eprintln!("scn mount: {warning}");
    }
    outln!("stopped serving {}", dir.display());
    Ok(())
}

/// Stops the mount on `dir`, and tells the serving process to clean up.
// The signature matches the other commands, which are all async.
#[allow(clippy::unused_async)]
pub async fn unmount(_ctx: &mut Ctx, args: UnmountArgs) -> Result<()> {
    let dir = std::path::absolute(&args.dir)?;
    let file = session_file()?;
    let record = drop_session(&file, &dir)?
        .ok_or_else(|| anyhow!("{}: no mount of scn is recorded", dir.display()))?;
    // The serving process unmounts and removes the overlay on this signal.
    if !stop_serving(&record, &dir) {
        // The process is alive and the signal did not go out: the mount is
        // still up. Put the record back, so a retry can find it.
        save_session(&record)?;
        anyhow::bail!(
            "{}: cannot stop the serving process (pid {}): it may still serve the mount",
            dir.display(),
            record.pid
        );
    }
    // The unmount itself may need root; say so, and let the user run it.
    if !std::process::Command::new("umount")
        .arg(&dir)
        .status()
        .is_ok_and(|s| s.success())
    {
        outln!("unmount failed; to unmount, run:");
        outln!("sudo umount {}", dir.display());
    }
    outln!("unmounted {}", dir.display());
    Ok(())
}

/// Refuses a mount point that is not a directory: the plain mount would
/// fail there anyway, after `scn` had started to serve.
fn check_mount_point(dir: &Path) -> Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    bail!(
        "{}: the mount point does not exist or is not a directory; create it first: mkdir -p {}",
        dir.display(),
        dir.display()
    );
}

/// The live record of `dir`, if the mount table holds one for a server
/// that still runs: a record is alive while its holder's serve lock is
/// held. A record whose lock is free is stale: the caller takes the
/// directory over, and saving the session replaces the record.
fn live_record_for<'a>(
    dir: &Path,
    sessions: &'a [MountRecord],
    alive: &impl Fn(&MountRecord) -> bool,
) -> Option<&'a MountRecord> {
    sessions
        .iter()
        .find(|record| record.dir == dir && alive(record))
}

/// Answers whether a live server holds `record`'s serve lock. The lock,
/// not the pid, is the identity: a pid can be reused after a crash, but
/// a lock dies with the process that holds it. A missing lock file or
/// overlay says the mount ended.
fn serving(record: &MountRecord) -> bool {
    let Ok(file) = std::fs::File::open(record.overlay.join("serve.lock")) else {
        return false;
    };
    // The probe takes the lock to test it and releases it by dropping
    // the file: held means a live server, free means the record is
    // stale. An error says nothing about a live holder: answer "not
    // serving", so a probe never signals on a guess.
    matches!(try_lock(&file), Ok(false))
}

/// Locks `file` without waiting. `Ok(true)` when the lock was free and
/// is now held by `file`, `Ok(false)` when another holder keeps it, and
/// an error when the lock cannot be tested. Locks on two opens of a
/// file conflict, even in one process.
fn try_lock(file: &std::fs::File) -> Result<bool, rustix::io::Errno> {
    match rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(true),
        Err(rustix::io::Errno::WOULDBLOCK) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Opens and exclusively locks `<overlay>/serve.lock` for the calling
/// process, which holds it for as long as it serves. The file opens with
/// close-on-exec, as Rust opens files, so a child of the server never
/// inherits the lock.
fn hold_serve_lock(overlay: &Path) -> Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(overlay.join("serve.lock"))
        .context("cannot open the serve lock of the overlay")?;
    // Only a free lock is good news: a held lock means another server
    // serves, and any other error means the filesystem misbehaves. Both
    // refuse the start, so no server can serve without its lock, and no
    // second mount can read a broken lock as a free one.
    match try_lock(&file) {
        Ok(true) => Ok(file),
        Ok(false) => bail!(
            "{}: another scn process serves this overlay",
            overlay.display()
        ),
        Err(e) => bail!("cannot test the serve lock of {}: {e}", overlay.display()),
    }
}

/// The `sudo` mount command, for the user to run when the plain mount is
/// mounted content ([#11], section 8 of the docs).
fn mount_command(port: u16, secret: &str, dir: &Path) -> String {
    let target = format!("127.0.0.1:/{EXPORT}/{secret}");
    if cfg!(target_os = "macos") {
        format!(
            "sudo mount_nfs -o resvport,port={port},mountport={port},vers=3,tcp,soft,nolocks {target} {}",
            dir.display()
        )
    } else {
        format!(
            "sudo mount -t nfs -o vers=3,tcp,port={port},mountport={port},nolock,soft {target} {}",
            dir.display()
        )
    }
}

/// Tries to mount without sudo; on failure, prints the sudo command and
/// keeps serving ([#11]).
fn try_mount(port: u16, secret: &str, dir: &Path) -> Result<()> {
    let mut command = if cfg!(target_os = "macos") {
        let mut command = std::process::Command::new("mount_nfs");
        command.arg(format!(
            "-oport={port},mountport={port},vers=3,tcp,soft,nolocks"
        ));
        command
    } else {
        let mut command = std::process::Command::new("mount");
        command.args(["-t", "nfs"]);
        command.arg(format!(
            "-overs=3,tcp,port={port},mountport={port},nolock,soft"
        ));
        command
    };
    command
        .arg(format!("127.0.0.1:/{EXPORT}/{secret}"))
        .arg(dir);
    let mounted = command.status().is_ok_and(|status| status.success());
    if !mounted {
        outln!("mount failed; to mount, run:");
        outln!("{}", mount_command(port, secret, dir));
    }
    Ok(())
}

/// Unmounts `dir`, best effort.
fn unmount_dir(dir: &Path) {
    let _ = std::process::Command::new("umount").arg(dir).status();
    // macOS may need this form.
    if cfg!(target_os = "macos") {
        let _ = std::process::Command::new("diskutil")
            .args(["unmount", "force"])
            .arg(dir)
            .status();
    }
}

/// A private directory for the changes, under the system's temporary
/// directory, with a random name.
fn overlay_dir() -> Result<PathBuf> {
    use std::io::Read as _;
    let mut bytes = [0u8; 8];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .context("cannot read /dev/urandom")?;
    let name: String = bytes.iter().fold(String::new(), |mut all, b| {
        use std::fmt::Write as _;
        let _ = write!(all, "{b:02x}");
        all
    });
    let dir = std::env::temp_dir().join(format!("scn-mount-{name}"));
    // Made with mode 0700, which a umask can only narrow, so the directory
    // is never open to others, not even before the chmod. A name that
    // exists is refused, as with `create_dir`.
    let private = {
        use std::os::unix::fs::DirBuilderExt as _;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder
    };
    private
        .create(&dir)
        .context("cannot make the overlay directory")?;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    private.create(dir.join(CHANGES))?;
    Ok(dir)
}

/// The path of the session file, under the home directory.
pub(crate) fn session_file() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("cannot read $HOME"))?;
    session_file_in(&home)
}

/// The path of the session file below `home`. Its directory, and any missing
/// directory above it, is made with mode 0700 whatever the umask.
fn session_file_in(home: &Path) -> Result<PathBuf> {
    let dir = home.join(".scion");
    scion_proto::runtime::auth::create_private_dir_all(&dir)?;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    Ok(dir.join("mounts.json"))
}

/// Reads the session file.
pub(crate) fn load_sessions() -> Result<Vec<MountRecord>> {
    read_sessions(&session_file()?)
}

/// Reads the sessions of `file`. A file that is not there holds no
/// sessions; a file that is there but corrupt is an error, so a broken
/// mount table cannot silently read as "no mounts".
fn read_sessions(file: &Path) -> Result<Vec<MountRecord>> {
    let text = match std::fs::read_to_string(file) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(anyhow!("cannot read {}: {e}", file.display())),
    };
    let json: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("{} is not valid JSON", file.display()))?;
    parse_sessions(&json).with_context(|| format!("{} does not hold mount records", file.display()))
}

/// The records of a session file's JSON.
fn parse_sessions(json: &serde_json::Value) -> Result<Vec<MountRecord>> {
    let records = json
        .as_array()
        .ok_or_else(|| anyhow!("the file holds no array of records"))?;
    records
        .iter()
        .enumerate()
        .map(|(n, record)| {
            Ok(MountRecord {
                dir: PathBuf::from(
                    record
                        .get("dir")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("record {n} has no dir"))?,
                ),
                overlay: PathBuf::from(
                    record
                        .get("overlay")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("record {n} has no overlay"))?,
                ),
                rev: record
                    .get("rev")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow!("record {n} has no rev"))?
                    .to_owned(),
                pid: u32::try_from(
                    record
                        .get("pid")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| anyhow!("record {n} has no pid"))?,
                )?,
                writable: record
                    .get("writable")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
            })
        })
        .collect()
}

/// Adds `record` to the session file, replacing the record of its dir.
pub(crate) fn save_session(record: &MountRecord) -> Result<()> {
    let file = session_file()?;
    add_session(&file, record)
}

/// Adds `record` to the session file `file`, replacing the record of its
/// dir.
fn add_session(file: &Path, record: &MountRecord) -> Result<()> {
    let mut sessions = load_sessions_at(file)?;
    sessions.retain(|known| known.dir != record.dir);
    sessions.push(record.clone());
    write_sessions(file, &sessions)
}

/// Drops the session of `dir` from `file`, and returns its record. The
/// table is read strictly: a corrupt file is an error, not "no mount".
fn drop_session(file: &Path, dir: &Path) -> Result<Option<MountRecord>> {
    let sessions = load_sessions_at(file)?;
    let Some(record) = sessions.iter().find(|r| r.dir == dir).cloned() else {
        return Ok(None);
    };
    let rest: Vec<MountRecord> = sessions.into_iter().filter(|r| r.dir != dir).collect();
    write_sessions(file, &rest)?;
    Ok(Some(record))
}

/// Reads the sessions of `file` (the test-visible form of
/// [`load_sessions`]).
fn load_sessions_at(file: &Path) -> Result<Vec<MountRecord>> {
    read_sessions(file)
}

/// The cleanup of a mount that has stopped serving: drop its session, and
/// remove its overlay. Returns what went wrong, for the caller to report;
/// a missing overlay is fine (it is already gone).
fn cleanup_after_stop(dir: &Path, overlay: &Path) -> Vec<String> {
    match session_file() {
        Ok(file) => cleanup_at(&file, dir, overlay),
        Err(e) => vec![format!("cannot update the mount table: {e}")],
    }
}

/// [`cleanup_after_stop`] for the session file `file`.
fn cleanup_at(file: &Path, dir: &Path, overlay: &Path) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Err(e) = drop_session(file, dir) {
        warnings.push(format!("cannot update the mount table: {e}"));
    }
    if let Err(e) = std::fs::remove_dir_all(overlay)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warnings.push(format!(
            "cannot remove the overlay {}: {e}",
            overlay.display()
        ));
    }
    warnings
}

/// Sends `pid` the `signal`, and answers whether it was delivered.
fn kill_pid(pid: u32, signal: &str) -> bool {
    std::process::Command::new("kill")
        .args([signal, &pid.to_string()])
        .status()
        .is_ok_and(|s| s.success())
}

/// Signals the serving process of `record` to stop. Answers `false` when
/// the process is alive and the signal did not go out: the mount is still
/// served, and the caller must keep the record.
fn stop_serving(record: &MountRecord, dir: &Path) -> bool {
    stop_serving_with(record, dir, kill_pid)
}

/// [`stop_serving`] with the kill injected, so a test can drive both
/// branches without a real process.
fn stop_serving_with(record: &MountRecord, dir: &Path, kill: impl Fn(u32, &str) -> bool) -> bool {
    // A signal goes out only while the recorded server's lock is held:
    // a stale record's pid may be anyone by now.
    if !serving(record) {
        return true;
    }
    let signaled = kill(record.pid, "-TERM");
    let alive = kill(record.pid, "-0");
    if signaled || !alive {
        return true;
    }
    let _ = dir;
    false
}

/// Waits for a stop signal.
async fn wait_stopped() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut stop = signal(SignalKind::interrupt()).expect("cannot listen for SIGINT");
    let mut term = signal(SignalKind::terminate()).expect("cannot listen for SIGTERM");
    tokio::select! {
        _ = stop.recv() => {}
        _ = term.recv() => {}
    }
}

/// The recorded mount of `dir`.
pub(crate) fn session_for_dir(dir: &Path) -> Result<MountRecord> {
    let dir = std::path::absolute(dir).context("cannot resolve the mount directory")?;
    load_sessions()?
        .into_iter()
        .find(|record| record.dir == dir)
        .ok_or_else(|| anyhow!("{}: no mount of scn is recorded", dir.display()))
}

/// The changes of a mount, from its overlay: each file or symlink under it,
/// and each recorded deletion of the ref.
pub(crate) fn overlay_changes(record: &MountRecord) -> Result<Vec<(RepoPath, Change)>> {
    let mut changes = Vec::new();
    let tree = record.overlay.join(CHANGES);
    walk_changes(&tree, b"", &mut changes)?;
    // The recorded deletions, but not of a path that the overlay holds.
    for path in crate::vfs::read_deletes(&tree)? {
        if !changes.iter().any(|(known, _)| known == &path) {
            changes.push((path, Change::Delete));
        }
    }

    Ok(changes)
}

/// Walks one directory of the overlay, collecting its changes.
fn walk_changes(dir: &Path, prefix: &[u8], out: &mut Vec<(RepoPath, Change)>) -> Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::PermissionsExt as _;

    let read = |path: &Path| {
        std::fs::read_dir(path).with_context(|| format!("cannot read {}", path.display()))
    };
    for entry in read(dir)? {
        let entry = entry.with_context(|| format!("cannot read {}", dir.display()))?;
        let name = entry.file_name();
        let rel = if prefix.is_empty() {
            name.as_bytes().to_vec()
        } else {
            let mut rel = prefix.to_vec();
            rel.push(b'/');
            rel.extend_from_slice(name.as_bytes());
            rel
        };
        let path = RepoPath::new(rel.clone())
            .with_context(|| format!("bad name in the overlay: {rel:?}"))?;
        // The note of deletions is bookkeeping, not a change.
        if rel == crate::vfs::DELETES.as_bytes() {
            continue;
        }
        let kind = entry
            .file_type()
            .with_context(|| format!("cannot read {}", entry.path().display()))?;
        if kind.is_dir() {
            walk_changes(&entry.path(), &rel, out)?;
        } else if kind.is_symlink() {
            let target = std::fs::read_link(entry.path())?;
            let target = target.as_os_str().as_bytes().to_vec();
            out.push((path, Change::PutLink(target)));
        } else {
            let exec = std::fs::metadata(entry.path())
                .with_context(|| format!("cannot read {}", entry.path().display()))?
                .permissions()
                .mode()
                & 0o111
                != 0;
            out.push((path, Change::Put { exec }));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[test]
    fn the_overlay_and_the_session_directory_are_private_whatever_the_umask() {
        // The run's umask, 002 or 022, would leave 0775 or 0755 on a plain
        // mkdir.
        let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o7777;
        let overlay = overlay_dir().unwrap();
        let modes = (mode(&overlay), mode(&overlay.join(CHANGES)));
        fs::remove_dir_all(&overlay).unwrap();
        assert_eq!(modes, (0o700, 0o700));

        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        let file = session_file_in(&home).unwrap();
        assert_eq!(file, home.join(".scion").join("mounts.json"));
        assert_eq!(mode(&home), 0o700);
        assert_eq!(mode(&home.join(".scion")), 0o700);
    }

    #[test]
    fn a_session_survives_a_round_trip() {
        let home = tempfile::tempdir().unwrap();
        let file = home.path().join("mounts.json");
        let record = MountRecord {
            dir: PathBuf::from("/tmp/view"),
            overlay: PathBuf::from("/tmp/overlay"),
            rev: "main".to_owned(),
            pid: 123,
            writable: false,
        };
        let sessions = vec![record.clone()];
        write_sessions(&file, &sessions).unwrap();
        let found = read_sessions(&file).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0], record);
        // A second mount of the same dir replaces the record.
        let other = MountRecord {
            dir: record.dir.clone(),
            overlay: PathBuf::from("/tmp/overlay-2"),
            rev: "dev".to_owned(),
            pid: 5,
            writable: false,
        };
        let mut sessions = read_sessions(&file).unwrap();
        sessions.retain(|known| known.dir != other.dir);
        sessions.push(other.clone());
        write_sessions(&file, &sessions).unwrap();
        let found = read_sessions(&file).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].overlay, other.overlay);
        // Removing the session returns its record.
        let gone = sessions.pop().unwrap();
        assert_eq!(gone.overlay, other.overlay);
        write_sessions(&file, &[]).unwrap();
        assert_eq!(read_sessions(&file).unwrap(), Vec::new());
    }

    /// A record that predates the flag has no `writable` key: it parses
    /// as a read-only mount.
    #[test]
    fn a_record_without_the_writable_key_is_read_only() {
        let home = tempfile::tempdir().unwrap();
        let file = home.path().join("mounts.json");
        let old = serde_json::json!([{
            "dir": "/tmp/view",
            "overlay": "/tmp/overlay",
            "rev": "main",
            "pid": 123,
        }]);
        std::fs::write(&file, serde_json::to_vec(&old).unwrap()).unwrap();
        let found = read_sessions(&file).unwrap();
        assert_eq!(found.len(), 1);
        assert!(!found[0].writable, "the old record is a read-only mount");
    }

    #[test]
    fn the_changes_of_a_mount_come_from_the_overlay() {
        let home = tempfile::tempdir().unwrap();
        let overlay = home.path().join("overlay");
        fs::create_dir_all(overlay.join(CHANGES).join("src")).unwrap();
        fs::write(overlay.join(CHANGES).join("README"), b"changed\n").unwrap();
        fs::write(
            overlay.join(CHANGES).join("src").join("new.rs"),
            b"fn f() {}\n",
        )
        .unwrap();
        fs::set_permissions(
            overlay.join(CHANGES).join("src").join("new.rs"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("README", overlay.join(CHANGES).join("link")).unwrap();
        // One deletion, of a path that the overlay does not hold.
        fs::write(
            overlay.join(CHANGES).join(crate::vfs::DELETES),
            r#"["gone.txt", "src/old.rs"]"#,
        )
        .unwrap();
        let record = MountRecord {
            dir: PathBuf::from("/tmp/view"),
            overlay: overlay.clone(),
            rev: "main".to_owned(),
            pid: 1,
            writable: false,
        };
        let mut changes = overlay_changes(&record).unwrap();
        changes.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
        let names: Vec<String> = changes
            .iter()
            .map(|(path, _)| String::from_utf8_lossy(path.as_bytes()).into_owned())
            .collect();
        assert_eq!(
            names,
            ["README", "gone.txt", "link", "src/new.rs", "src/old.rs"]
        );
        let puts: Vec<bool> = changes
            .iter()
            .map(|(_, change)| matches!(change, Change::Put { .. }))
            .collect();
        assert_eq!(puts, [true, false, false, true, false]);
        let link = changes
            .iter()
            .find(|(path, _)| path.as_bytes() == b"link")
            .unwrap();
        assert!(matches!(link.1, Change::PutLink(ref target) if target == b"README"));
        let exec = match &changes[3].1 {
            Change::Put { exec } => *exec,
            _ => false,
        };
        assert!(exec);
    }
}

#[cfg(test)]
mod silent_failures {
    use super::*;
    use std::fs;

    /// The gate every test of this module that forks a process or holds
    /// or reads a serve lock runs under, with the crate's own fork gate:
    /// a fork of one test can hold another test's lock exactly like its
    /// own, so the gate lives where every forking test of the binary can
    /// reach it.
    use crate::fork_gate::serial;

    /// A spawned stand-in server in its own process group. Its drop
    /// signals the whole group and reaps the child, also on a panic, so
    /// no test leaves a process behind whose pid a later test could read
    /// or whose lock a fork could carry.
    struct Parked(std::process::Child);

    impl Parked {
        fn spawn(seconds: &str) -> Self {
            use std::os::unix::process::CommandExt as _;
            Self(
                std::process::Command::new("sleep")
                    .arg(seconds)
                    .process_group(0)
                    .spawn()
                    .unwrap(),
            )
        }
    }

    impl std::ops::Deref for Parked {
        type Target = std::process::Child;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl std::ops::DerefMut for Parked {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl Drop for Parked {
        fn drop(&mut self) {
            // The group's id is the child's pid, so the kill takes the
            // child and anything it left; then the reap.
            let _ = std::process::Command::new("kill")
                .args(["-9", &format!("-{}", self.0.id())])
                .status();
            let _ = self.0.wait();
        }
    }

    fn temp_file(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("scn-mount-test-{}-{}", std::process::id(), name));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("mounts.json")
    }

    fn record(dir: &str, pid: u32) -> MountRecord {
        MountRecord {
            dir: PathBuf::from(dir),
            overlay: PathBuf::from(format!("{dir}/overlay")),
            rev: "main".to_owned(),
            pid,
            writable: false,
        }
    }

    /// A kill that cannot signal a live process keeps the record, so a
    /// retry of the unmount finds the mount again.
    #[test]
    fn a_failed_kill_keeps_the_session() {
        let file = temp_file("failed-kill");
        std::fs::remove_file(&file).ok();
        let home = tempfile::tempdir().unwrap();
        let overlay = home.path().join("overlay");
        fs::create_dir_all(&overlay).unwrap();
        let r = MountRecord {
            dir: PathBuf::from("/mnt/keep"),
            overlay: overlay.clone(),
            rev: "main".to_owned(),
            pid: 4242,
            writable: false,
        };
        add_session(&file, &r).unwrap();
        // The recorded server is live: this process holds its serve lock.
        let _lock = hold_serve_lock(&overlay).unwrap();
        let stop = |pid: u32, signal: &str| {
            // Alive, but the signal does not go out.
            assert_eq!(pid, 4242);
            signal == "-0"
        };
        assert!(!stop_serving_with(&r, &r.dir.clone(), stop));
        let sessions = load_sessions_at(&file).unwrap();
        assert_eq!(sessions.len(), 1, "the record stays for a retry");
        std::fs::remove_dir_all(file.parent().unwrap()).unwrap();
    }

    /// A process that is already gone counts as stopped: the record is
    /// dropped and the unmount goes on.
    #[test]
    fn a_dead_process_is_stopped() {
        let file = temp_file("dead");
        std::fs::remove_file(&file).ok();
        let home = tempfile::tempdir().unwrap();
        let overlay = home.path().join("overlay");
        fs::create_dir_all(&overlay).unwrap();
        let r = MountRecord {
            dir: PathBuf::from("/mnt/dead"),
            overlay: overlay.clone(),
            rev: "main".to_owned(),
            pid: 4243,
            writable: false,
        };
        add_session(&file, &r).unwrap();
        // The serve lock is held, like a live server holds it, and the
        // process behind the pid answers nothing: no such process.
        let _lock = hold_serve_lock(&overlay).unwrap();
        let stop = |pid: u32, _signal: &str| {
            assert_eq!(pid, 4243);
            false // kill -0 and kill -TERM both fail: no such process
        };
        assert!(stop_serving_with(&r, &r.dir.clone(), stop));
        assert_eq!(drop_session(&file, &r.dir).unwrap(), Some(r.clone()));
        std::fs::remove_dir_all(file.parent().unwrap()).unwrap();
    }

    /// A corrupt mount table is an error, not "no mounts": nothing reads
    /// it as empty, so a mount cannot quietly double-mount a directory.
    #[test]
    fn a_corrupt_table_is_an_error() {
        let file = temp_file("corrupt");
        std::fs::write(&file, b"{not json").unwrap();
        assert!(read_sessions(&file).is_err());
        let r = record("/mnt/corrupt", 4244);
        assert!(
            add_session(&file, &r).is_err(),
            "a mount start must fail loudly, not read the table as empty"
        );
        assert!(drop_session(&file, &r.dir).is_err());
        std::fs::remove_dir_all(file.parent().unwrap()).unwrap();
    }

    /// A missing file holds no sessions, and the stop cleanup of a mount
    /// whose overlay is already gone reports nothing.
    #[test]
    fn a_missing_file_is_empty_and_the_cleanup_is_quiet() {
        let file = temp_file("missing");
        std::fs::remove_file(&file).ok();
        assert_eq!(read_sessions(&file).unwrap(), Vec::new());
        let overlay = file.parent().unwrap().join("overlay-gone");
        let warnings = cleanup_at(&file, &PathBuf::from("/mnt/none"), &overlay);
        assert!(warnings.is_empty(), "{warnings:?}");
        std::fs::remove_dir_all(file.parent().unwrap()).unwrap();
    }

    /// The stop cleanup reports a mount table it cannot update.
    #[test]
    fn the_cleanup_reports_a_broken_table() {
        let file = temp_file("cleanup-broken");
        std::fs::write(&file, b"not json").unwrap();
        let overlay = file.parent().unwrap().join("overlay");
        std::fs::create_dir_all(&overlay).unwrap();
        let warnings = cleanup_at(&file, &PathBuf::from("/mnt/x"), &overlay);
        assert!(
            warnings.iter().any(|w| w.contains("mount table")),
            "{warnings:?}"
        );
        std::fs::remove_dir_all(file.parent().unwrap()).unwrap();
    }

    /// A mount point that is not a directory is refused before anything
    /// is served; an existing directory passes.
    #[test]
    fn a_missing_mount_point_is_refused_before_serving() {
        let base = std::env::temp_dir().join(format!("scn-mount-point-{}", std::process::id()));
        let gone = base.join("gone");
        let err = check_mount_point(&gone).unwrap_err().to_string();
        assert!(err.contains("mkdir -p"), "{err}");
        // A file at the path is refused too: it cannot take a mount.
        let file = base.join("file");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(&file, b"").unwrap();
        assert!(check_mount_point(&file).is_err());
        let dir = base.join("dir");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(check_mount_point(&dir).is_ok());
        std::fs::remove_dir_all(&base).unwrap();
    }

    /// A live record for the same directory is refused with the owning
    /// pid; a stale record (the pid is gone) and no record pass.
    #[test]
    fn a_live_mount_of_the_same_dir_is_refused_with_its_pid() {
        let dir = PathBuf::from("/mnt/view");
        let sessions = vec![record("/mnt/view", 4245)];
        let live = |known: &MountRecord| known.pid == 4245;
        let err = match live_record_for(&dir, &sessions, &live) {
            Some(known) => format!("pid {}", known.pid),
            None => String::new(),
        };
        assert_eq!(err, "pid 4245");
        // The pid is gone: the record is stale, the mount may start.
        let dead = |_: &MountRecord| false;
        assert!(live_record_for(&dir, &sessions, &dead).is_none());
        assert!(live_record_for(&dir, &[], &live).is_none());
        // A record of another directory does not block this one.
        let other = vec![record("/mnt/other", 4245)];
        assert!(live_record_for(&dir, &other, &live).is_none());
    }

    /// The identity of a server is its serve lock, not its pid: a record
    /// whose lock is free is stale, so an unmount must not signal the
    /// process its pid now points at - after a crash that can be anyone.
    #[test]
    #[cfg(unix)]
    fn an_unmount_does_not_signal_an_unrelated_process() {
        let _serial = serial();
        let home = tempfile::tempdir().unwrap();
        let overlay = home.path().join("overlay");
        fs::create_dir_all(&overlay).unwrap();
        // The lock file exists, and no one holds it: no server lives.
        std::fs::File::create(overlay.join("serve.lock")).unwrap();
        let mut sleeper = Parked::spawn("30");
        let known = MountRecord {
            dir: PathBuf::from("/mnt/view"),
            overlay: overlay.clone(),
            rev: "main".to_owned(),
            pid: sleeper.id(),
            writable: false,
        };
        // The stale record is taken over, and the sleeper lives on. The
        // lock state is polled with a ceiling: macOS may tear a released
        // lock down one tick late, so one read right after a release can
        // see it held.
        assert!(stop_serving(&known, &known.dir));
        assert!(wait_for(2, || !serving(&known)));
        assert!(
            sleeper.try_wait().unwrap().is_none(),
            "an unrelated process was signalled"
        );
    }

    /// A record whose serve lock is held is live: the unmount signals the
    /// recorded pid, and the process goes away.
    #[test]
    fn an_unmount_signals_the_live_server() {
        let _serial = serial();
        let home = tempfile::tempdir().unwrap();
        let overlay = home.path().join("overlay");
        fs::create_dir_all(&overlay).unwrap();
        let mut server = Parked::spawn("30");
        let known = MountRecord {
            dir: PathBuf::from("/mnt/view"),
            overlay: overlay.clone(),
            rev: "main".to_owned(),
            pid: server.id(),
            writable: false,
        };
        // This test process holds the serve lock, like the serving
        // process would; the pid it recorded points at the child.
        let _lock = hold_serve_lock(&overlay).unwrap();
        assert!(serving(&known));
        assert!(stop_serving(&known, &known.dir));
        assert!(wait_for(2, || server.try_wait().unwrap().is_some()));
    }

    /// A dead server's record never refuses a mount: the lock dies with
    /// the holder, and the takeover polls the real condition, because
    /// macOS may tear a reaped child's lock down one tick late.
    #[test]
    fn a_dead_server_record_is_taken_over() {
        let _serial = serial();
        let home = tempfile::tempdir().unwrap();
        let overlay = home.path().join("overlay");
        fs::create_dir_all(&overlay).unwrap();
        let known = MountRecord {
            dir: PathBuf::from("/mnt/view"),
            overlay: overlay.clone(),
            rev: "main".to_owned(),
            pid: std::process::id(),
            writable: false,
        };
        let lock = hold_serve_lock(&overlay).unwrap();
        assert!(
            live_record_for(
                Path::new("/mnt/view"),
                std::slice::from_ref(&known),
                &serving
            )
            .is_some()
        );
        drop(lock);
        assert!(wait_for(2, || live_record_for(
            Path::new("/mnt/view"),
            std::slice::from_ref(&known),
            &serving
        )
        .is_none()));
    }

    /// A save that cannot write its temp file errs, and the old table is
    /// still there whole: the rename never happened.
    #[test]
    fn a_failed_save_keeps_the_old_table() {
        let home = tempfile::tempdir().unwrap();
        let file = home.path().join("mounts.json");
        let first = record("/mnt/view", 11);
        write_sessions(&file, &[first]).unwrap();
        // The temp path is taken, so the save cannot complete.
        let blocked = file.with_extension(format!("json.{}.tmp", std::process::id()));
        fs::create_dir(&blocked).unwrap();
        assert!(add_session(&file, &record("/mnt/other", 12)).is_err());
        assert_eq!(read_sessions(&file).unwrap().len(), 1);
        // With the path free, the save goes through.
        fs::remove_dir(&blocked).unwrap();
        add_session(&file, &record("/mnt/other", 12)).unwrap();
        let found = read_sessions(&file).unwrap();
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].dir, PathBuf::from("/mnt/view"));
    }

    /// Waits until `cond` holds, with a ceiling: macOS may keep a reaped
    /// child's flock for one tick, so a lock test polls the real
    /// condition instead of assuming the first read.
    fn wait_for(ceiling_secs: u64, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(ceiling_secs);
        loop {
            if cond() {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
