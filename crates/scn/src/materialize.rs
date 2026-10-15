//! Writes a checkout to disk, and the name rules that make that safe.
//!
//! A checkout is written into a new temp directory with a random name and
//! mode 0700, made through a directory handle. It becomes the destination
//! only once every file is written:
//!
//! - If the destination is missing, the temp directory is made next to it,
//!   through a handle on its parent. It gets the mode a plain `mkdir` would
//!   give, and is renamed onto the destination name. The rename is the check
//!   that counts: it fails if the destination has appeared meanwhile as a
//!   non-empty directory, a file or a symlink.
//! - If the destination is an empty directory, it is opened with
//!   `O_NOFOLLOW`, so a symlink is refused. It must be owned by the user who
//!   runs the checkout, and must not be writable by all users unless it has
//!   the sticky bit. The temp directory is made inside it. Each entry
//!   of the temp directory is then moved up by a rename that never replaces
//!   what is there, and the destination must then hold only the checkout.
//!   The destination itself stays, with its mode, owner and extended
//!   attributes.
//!
//! On any failure, every path the checkout made is removed, deepest first,
//! through the same bounded directory handles as the writes, and then the
//! temp directory. So the destination stays as it was.
//!
//! Inside the temp directory every write goes through directory handles: each
//! parent is opened one part at a time with `O_NOFOLLOW`, and files are made
//! with `O_EXCL`, so a symlink or a file in the way fails the write instead of
//! redirecting it. The handles on the parents of the last path stay open, so
//! paths written in path order open each directory once. The temp directory
//! starts empty and the checkout records every path it makes, so a path that
//! already exists means the filesystem folded two names into one, by case or
//! by Unicode form.

use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsFd as _, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rustix::fs::{AtFlags, Dir, Mode, OFlags, RenameFlags, Stat};
use rustix::io::Errno;
use scion_core::RepoPath;

/// Directory names that tools keep metadata in. A checkout never writes them.
const RESERVED: [&[u8]; 2] = [b".git", b".scion"];

/// Name prefix of the temp directory a checkout is written into.
const TEMP_PREFIX: &str = ".scn-checkout-";

/// The most directory handles a checkout keeps open. A deeper path keeps the
/// innermost ones. macOS allows 256 open files by default.
const MAX_OPEN_DIRS: usize = 64;

/// Checks that a file or directory name is safe to write and to print.
///
/// # Errors
///
/// Says why if the name has a control character (C0, DEL or C1: printed, it
/// can rewrite a terminal), a bidirectional text control (printed, it can make
/// a name read as another), or is `.git` or `.scion` in any ASCII case. Bytes
/// that are not UTF-8 are checked one by one: 0x80 to 0x9F are C1 controls on
/// an 8-bit terminal.
// SHORTCUT: no Windows rules: device names (`CON`, `NUL`) and names that end
// in a dot or a space pass. Case and Unicode folding is caught at write time,
// on the filesystem in use. Upgrade: per-platform rules before checkout runs
// on Windows.
pub fn check_name(name: &[u8]) -> Result<(), &'static str> {
    for chunk in name.utf8_chunks() {
        let text = chunk.valid();
        if text.chars().any(char::is_control)
            || chunk.invalid().iter().any(|b| (0x80..=0x9f).contains(b))
        {
            return Err("it contains a control character");
        }
        if text
            .chars()
            .any(|c| matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'))
        {
            return Err("it contains a bidirectional text control");
        }
    }
    if RESERVED
        .iter()
        .any(|reserved| name.eq_ignore_ascii_case(reserved))
    {
        return Err("it uses a reserved metadata name");
    }
    Ok(())
}

/// Checks early that `dest` is missing, or an empty directory that the user
/// who runs this owns, to fail before any work. [`Checkout::start`] checks
/// again, and [`Checkout::finish`] checks what can change meanwhile.
///
/// # Errors
///
/// Fails if `dest` is a symlink, a file, or a directory that is not empty,
/// that another user owns, or that all users can write without the sticky
/// bit.
pub fn check_dest(dest: &Path) -> Result<()> {
    open_dest(dest).map(drop)
}

/// Opens `dest` if it is an empty directory that the user who runs this
/// owns, or returns `None` if it is missing.
///
/// # Errors
///
/// As [`check_dest`].
fn open_dest(dest: &Path) -> Result<Option<OwnedFd>> {
    let shown = dest.display();
    // Rebuilt from its parts: with a final `/` or `/.`, the open would follow
    // a symlink.
    let path: PathBuf = dest.components().collect();
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let dir = match rustix::fs::open(&path, flags, Mode::empty()) {
        Ok(dir) => dir,
        Err(Errno::NOENT) => return Ok(None),
        Err(Errno::LOOP | Errno::NOTDIR) => bail!("{shown} is a symlink or not a directory"),
        Err(e) => return Err(io::Error::from(e)).with_context(|| format!("cannot open {shown}")),
    };
    let stat = rustix::fs::fstat(&dir)
        .map_err(io::Error::from)
        .with_context(|| format!("cannot read {shown}"))?;
    if !is_mine(&stat) {
        bail!("{shown} is owned by another user");
    }
    // Any user could rename a file of their own over one that has moved up,
    // and the final check compares only names. Group members are trusted,
    // as they are in a new destination's parent.
    let mode = Mode::from_raw_mode(stat.st_mode);
    if mode.contains(Mode::WOTH) && !mode.contains(Mode::SVTX) {
        bail!("{shown} is writable by all users: remove that, or set the sticky bit");
    }
    let names = entries(&dir).with_context(|| format!("cannot read {shown}"))?;
    if names.is_empty() {
        return Ok(Some(dir));
    }
    if names
        .iter()
        .all(|name| name.starts_with(TEMP_PREFIX.as_bytes()))
    {
        let left: Vec<_> = names
            .iter()
            .map(|name| format!("\"{}\"", name.escape_ascii()))
            .collect();
        bail!(
            "{shown} is not empty: a stopped checkout left {} in it. Remove that, then try again",
            left.join(", ")
        );
    }
    bail!("{shown} is not empty")
}

/// A checkout being written into a temp directory. Dropped without
/// [`Checkout::finish`], it removes every path it made, wherever it is now,
/// and then the temp directory.
pub struct Checkout {
    /// The directory that holds the temp directory: the destination if it
    /// exists, or else its parent.
    base: OwnedFd,
    /// The path of `base`, for messages.
    base_path: PathBuf,
    /// The temp directory's name in `base`, and a handle on it.
    temp: Vec<u8>,
    root: OwnedFd,
    /// How the temp directory becomes the destination.
    dest: Dest,
    /// Every path this checkout has made, relative to the temp directory,
    /// and whether it is a directory. A path sorts after each of its
    /// parents.
    made: BTreeMap<Vec<u8>, bool>,
    /// Handles on the directories that hold the last path written, outermost
    /// first, with their paths relative to the temp directory. Each is the
    /// parent of the next. A handle follows its directory when it moves up.
    open: VecDeque<(Vec<u8>, OwnedFd)>,
    /// Names moved up into an existing destination so far, sorted, as
    /// [`entries`] gives them.
    moved: Vec<Vec<u8>>,
    finished: bool,
    /// How many directories this checkout has opened.
    #[cfg(test)]
    opens: usize,
}

/// How a finished temp directory becomes the destination.
enum Dest {
    /// The destination is missing. The temp directory gets `mode`, the mode
    /// a plain `mkdir` gives, and is renamed to `name` in `base`.
    New { name: Vec<u8>, mode: Mode },
    /// The destination is `base`, an empty directory. Each entry of the temp
    /// directory moves up into it.
    Existing,
}

impl Checkout {
    /// Makes an empty, private temp directory for a checkout into `dest`:
    /// inside `dest` if it is an empty directory, or else next to it.
    ///
    /// # Errors
    ///
    /// Fails if `dest` is a symlink, a file, or a directory that is not empty
    /// or that another user owns; if a missing `dest` has no final name (for
    /// example `..`); or if a directory cannot be opened or written.
    pub fn start(dest: &Path) -> Result<Self> {
        match open_dest(dest)? {
            Some(dir) => Self::make(dir, dest.to_path_buf(), None),
            None => Self::start_next_to(dest),
        }
    }

    /// Starts a checkout next to `dest`, which is missing.
    fn start_next_to(dest: &Path) -> Result<Self> {
        let shown = dest.display();
        let name = dest
            .file_name()
            .with_context(|| format!("{shown} does not name a directory to create"))?;
        let parent_path = match dest.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let parent = rustix::fs::open(&parent_path, flags, Mode::empty())
            .with_context(|| format!("cannot open {}", parent_path.display()))?;
        Self::make(parent, parent_path, Some(name.as_bytes().to_vec()))
    }

    /// Makes the temp directory in `base` and opens it. With a `name`, the
    /// destination is that new directory in `base`; without one, it is
    /// `base`.
    fn make(base: OwnedFd, base_path: PathBuf, name: Option<Vec<u8>>) -> Result<Self> {
        let temp = make_temp_dir(&base)
            .with_context(|| format!("cannot make a directory in {}", base_path.display()))?;
        let set_up = || -> io::Result<(OwnedFd, Dest)> {
            let root = open_dir(&base, &temp)?;
            let dest = match name {
                Some(name) => Dest::New {
                    name,
                    mode: mkdir_mode(&root)?,
                },
                // Opened by name: a user who can write `base` could have put
                // their own directory in its place.
                None if is_mine(&rustix::fs::fstat(&root)?) => Dest::Existing,
                None => return Err(io::Error::other("another user replaced it")),
            };
            Ok((root, dest))
        };
        let (root, dest) = match set_up() {
            Ok(set) => set,
            Err(e) => {
                // The probe removes itself, so the directory is empty and a
                // plain rmdir removes it. A directory that replaced it is
                // not ours, and stays.
                let _ = rustix::fs::unlinkat(&base, temp.as_slice(), AtFlags::REMOVEDIR);
                return Err(e).context("cannot set up the new directory");
            }
        };
        Ok(Self {
            base,
            base_path,
            temp,
            root,
            dest,
            made: BTreeMap::new(),
            open: VecDeque::new(),
            moved: Vec::new(),
            finished: false,
            #[cfg(test)]
            opens: 0,
        })
    }

    /// Writes a regular file. Executable files get mode 0777 and others 0666,
    /// less the umask, as git does.
    ///
    /// # Errors
    ///
    /// Fails if a part breaks [`check_name`], if a parent is a symlink or not
    /// a directory, if the path collides with another one on this filesystem,
    /// or if the write fails.
    pub fn file(&mut self, path: &RepoPath, executable: bool, data: &[u8]) -> Result<()> {
        let mut write = || -> io::Result<()> {
            let (dir, name) = self.parent(path)?;
            let mode = Mode::from_raw_mode(if executable { 0o777 } else { 0o666 });
            let flags =
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let file = rustix::fs::openat(dir, name, flags, mode)
                .map_err(|e| collision_if_exists(e, path.as_bytes()))?;
            self.made.insert(path.as_bytes().to_vec(), false);
            fs::File::from(file).write_all(data)
        };
        write().with_context(|| format!("cannot write {path:?}"))
    }

    /// Makes a symlink with `target` as its raw target bytes. The target is
    /// never resolved.
    ///
    /// # Errors
    ///
    /// As [`Checkout::file`], or if the target is not a valid symlink target
    /// (empty, or has a NUL).
    pub fn symlink(&mut self, path: &RepoPath, target: &[u8]) -> Result<()> {
        let mut link = || -> io::Result<()> {
            let (dir, name) = self.parent(path)?;
            rustix::fs::symlinkat(target, dir, name)
                .map_err(|e| collision_if_exists(e, path.as_bytes()))?;
            self.made.insert(path.as_bytes().to_vec(), false);
            Ok(())
        };
        link().with_context(|| format!("cannot write {path:?}"))
    }

    /// Moves the checkout into place. A missing destination: the temp
    /// directory gets the mode of a plain `mkdir` and is renamed onto it. An
    /// existing one: each entry of the temp directory moves up into it.
    ///
    /// # Errors
    ///
    /// Fails, and removes what it wrote, if the destination is a non-empty
    /// directory, a file or a symlink, or if something appeared in an
    /// existing destination during the checkout.
    pub fn finish(mut self) -> Result<()> {
        match &self.dest {
            Dest::New { name, mode } => self.rename_onto(name, *mode)?,
            Dest::Existing => self.move_up()?,
        }
        self.finished = true;
        Ok(())
    }

    /// Gives the temp directory `mode`, and renames it to `name` in `base`.
    fn rename_onto(&self, name: &[u8], mode: Mode) -> Result<()> {
        let dest = self.base_path.join(OsStr::from_bytes(name));
        let shown = dest.display();
        rustix::fs::fchmod(&self.root, mode)
            .map_err(io::Error::from)
            .with_context(|| format!("cannot set the mode of {shown}"))?;
        match rustix::fs::renameat(&self.base, &self.temp, &self.base, name) {
            Ok(()) => Ok(()),
            Err(Errno::NOTEMPTY | Errno::EXIST) => bail!("{shown} is not empty"),
            Err(Errno::NOTDIR) => bail!("{shown} is a symlink or not a directory"),
            Err(e) => Err(io::Error::from(e))
                .with_context(|| format!("cannot move the checkout to {shown}")),
        }
    }

    /// Moves each entry of the temp directory up into the destination, never
    /// over something already there. Then checks that the destination holds
    /// only the checkout, and removes the temp directory.
    // SHORTCUT: needs a rename that never replaces (`RENAME_NOREPLACE` on
    // Linux, `RENAME_EXCL` on macOS). On Linux, NFS has none: `renameat2`
    // fails with EINVAL. Untested on macOS NFS. Upgrade (issue #74): check,
    // then rename, on such a filesystem.
    fn move_up(&mut self) -> Result<()> {
        let shown = self.base_path.display();
        for name in entries(&self.root).context("cannot read the checkout")? {
            let flags = RenameFlags::NOREPLACE;
            match rustix::fs::renameat_with(&self.root, &name, &self.base, &name, flags) {
                Ok(()) => self.moved.push(name),
                Err(Errno::EXIST) => bail!("{shown} is no longer empty"),
                Err(Errno::INVAL | Errno::NOTSUP | Errno::NOSYS) => bail!(
                    "cannot move files into {shown} without replacing what is there on this \
                     filesystem: check out into a new directory instead"
                ),
                Err(e) => {
                    return Err(io::Error::from(e)).with_context(|| {
                        format!("cannot move \"{}\" into {shown}", name.escape_ascii())
                    });
                }
            }
        }
        // Sorted, as `moved` is.
        let mut now = entries(&self.base).with_context(|| format!("cannot read {shown}"))?;
        now.retain(|name| *name != self.temp);
        if now != self.moved {
            bail!("{shown} is no longer empty");
        }
        rustix::fs::unlinkat(&self.base, self.temp.as_slice(), AtFlags::REMOVEDIR)
            .map_err(io::Error::from)
            .with_context(|| format!("cannot remove the temp directory in {shown}"))
    }

    /// [`Checkout::holder`] that makes missing parents. Fails before making
    /// anything if a part breaks [`check_name`].
    fn parent<'p>(&mut self, path: &'p RepoPath) -> io::Result<(BorrowedFd<'_>, &'p [u8])> {
        let bytes = path.as_bytes();
        let mut parts = bytes.split(|&b| b == b'/');
        if let Some(why) = parts.find_map(|part| check_name(part).err()) {
            return Err(io::Error::other(why));
        }
        self.holder(bytes, true)
    }

    /// Opens the directory that holds `path`, a path relative to the temp
    /// directory, and returns it with the last part of `path`. With `make`,
    /// makes the parents that this checkout has not made yet.
    ///
    /// The handles on the directories of the last path stay open, so a path
    /// in the same directory, or below it, opens only what is new. A
    /// top-level entry that has moved up is opened in the destination.
    fn holder<'p>(&mut self, path: &'p [u8], make: bool) -> io::Result<(BorrowedFd<'_>, &'p [u8])> {
        let (dir, name) = match path.iter().rposition(|&b| b == b'/') {
            Some(slash) => (&path[..slash], &path[slash + 1..]),
            None => (&b""[..], path),
        };
        let first = path.split(|&b| b == b'/').next().unwrap_or_default();
        let moved = self
            .moved
            .binary_search_by(|name| name.as_slice().cmp(first))
            .is_ok();
        // Where the first part of `path` is: what the handles start from.
        let top = if moved { &self.base } else { &self.root };
        while self
            .open
            .back()
            .is_some_and(|(open, _)| !is_within(dir, open))
        {
            self.open.pop_back();
        }
        // The parts of `dir` below the last directory still open.
        let mut start = self.open.back().map_or(0, |(open, _)| open.len() + 1);
        while start < dir.len() {
            let end = dir[start..]
                .iter()
                .position(|&b| b == b'/')
                .map_or(dir.len(), |i| start + i);
            let (part, prefix) = (&dir[start..end], &dir[..end]);
            let at = self.open.back().map_or(top, |(_, dir)| dir);
            if make && !self.made.contains_key(prefix) {
                rustix::fs::mkdirat(at, part, Mode::from_raw_mode(0o777))
                    .map_err(|e| collision_if_exists(e, prefix))?;
                self.made.insert(prefix.to_vec(), true);
            }
            let opened = open_dir(at, part)?;
            #[cfg(test)]
            {
                self.opens += 1;
            }
            if self.open.len() == MAX_OPEN_DIRS {
                self.open.pop_front();
            }
            self.open.push_back((prefix.to_vec(), opened));
            start = end + 1;
        }
        let dir = self.open.back().map_or(top, |(_, dir)| dir);
        Ok((dir.as_fd(), name))
    }

    /// Removes `path`, which this checkout made, through the handles that
    /// stay open. `unlinkat` never follows a symlink. A directory must be
    /// empty.
    fn remove(&mut self, path: &[u8], is_dir: bool) -> io::Result<()> {
        let (dir, name) = self.holder(path, false)?;
        let flags = if is_dir {
            AtFlags::REMOVEDIR
        } else {
            AtFlags::empty()
        };
        Ok(rustix::fs::unlinkat(dir, name, flags)?)
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // In reverse path order, a directory comes after everything in it.
        let (mut first, mut failed) = (None, 0);
        for (path, is_dir) in std::mem::take(&mut self.made).into_iter().rev() {
            if let Err(e) = self.remove(&path, is_dir) {
                first.get_or_insert_with(|| format!("\"{}\": {e}", path.escape_ascii()));
                failed += 1;
            }
        }
        if let Some(first) = first {
            eprintln!(
                "warning: cannot remove {failed} paths of the checkout in {}, first {first}",
                self.base_path.display()
            );
        }
        if let Err(e) = rustix::fs::unlinkat(&self.base, self.temp.as_slice(), AtFlags::REMOVEDIR) {
            let temp = self.base_path.join(OsStr::from_bytes(&self.temp));
            eprintln!("warning: cannot remove {}: {e}", temp.display());
        }
    }
}

/// Makes a new, empty directory in `parent` with a random name and mode 0700,
/// so other users cannot guess its name, enter it, or add to it.
fn make_temp_dir(parent: &OwnedFd) -> io::Result<Vec<u8>> {
    let mut random = [0_u8; 8];
    fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
    let name = format!("{TEMP_PREFIX}{:016x}", u64::from_ne_bytes(random)).into_bytes();
    rustix::fs::mkdirat(parent, name.as_slice(), Mode::from_raw_mode(0o700))?;
    Ok(name)
}

/// The mode a plain `mkdir` gives in `dir`: 0777 less the umask, and the
/// setgid bit where `dir` passes it on. Reading the umask changes it for the
/// whole process, so this makes a probe directory in `dir`, which must be
/// empty and private, and reads its mode.
fn mkdir_mode(dir: &OwnedFd) -> io::Result<Mode> {
    rustix::fs::mkdirat(dir, "probe", Mode::from_raw_mode(0o777))?;
    let stat = rustix::fs::statat(dir, "probe", AtFlags::SYMLINK_NOFOLLOW);
    rustix::fs::unlinkat(dir, "probe", AtFlags::REMOVEDIR)?;
    Ok(Mode::from_raw_mode(stat?.st_mode & 0o7777))
}

/// Whether the user who runs this process owns what `stat` describes.
fn is_mine(stat: &Stat) -> bool {
    stat.st_uid == rustix::process::geteuid().as_raw()
}

/// Opens directory `name` in `dir`. Fails if `name` is a symlink or anything
/// else that is not a directory.
fn open_dir(dir: &OwnedFd, name: &[u8]) -> io::Result<OwnedFd> {
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    rustix::fs::openat(dir, name, flags, Mode::empty()).map_err(|e| match e {
        Errno::LOOP | Errno::NOTDIR => io::Error::other(format!(
            "\"{}\" is a symlink or not a directory",
            name.escape_ascii()
        )),
        e => e.into(),
    })
}

/// The names in `dir`, sorted, without `.` and `..`.
fn entries(dir: &OwnedFd) -> io::Result<Vec<Vec<u8>>> {
    let mut names = Vec::new();
    for entry in Dir::read_from(dir)? {
        let name = entry?.file_name().to_bytes().to_vec();
        if name != b"." && name != b".." {
            names.push(name);
        }
    }
    names.sort_unstable();
    Ok(names)
}

/// Whether `path` is `dir` or below it.
pub fn is_within(path: &[u8], dir: &[u8]) -> bool {
    path.strip_prefix(dir)
        .is_some_and(|rest| rest.first().is_none_or(|&b| b == b'/'))
}

/// Maps "already exists" to a folding collision. The temp directory starts
/// empty and every path this checkout makes is recorded, so a path that
/// already exists without being recorded is another name folded into it.
fn collision_if_exists(e: Errno, path: &[u8]) -> io::Error {
    if e == Errno::EXIST {
        io::Error::other(format!(
            "\"{}\" collides with another path on this filesystem (case or Unicode folding)",
            path.escape_ascii()
        ))
    } else {
        e.into()
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    use super::*;

    fn path(p: &str) -> RepoPath {
        RepoPath::new(p).unwrap()
    }

    /// Names in `dir` that a checkout's temp directory would have.
    fn temp_dirs(dir: &Path) -> Vec<String> {
        names(dir)
            .into_iter()
            .filter(|name| name.starts_with(TEMP_PREFIX))
            .collect()
    }

    /// The names in `dir`, sorted.
    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort_unstable();
        names
    }

    /// The permission bits of `path`, setgid included.
    fn mode(path: &Path) -> u32 {
        fs::metadata(path).unwrap().permissions().mode() & 0o7777
    }

    #[test]
    fn never_writes_through_a_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        let (new, existing) = (tmp.path().join("new"), tmp.path().join("existing"));
        fs::create_dir(&existing).unwrap();

        // From the temp directory, next to a new destination or inside an
        // existing one, the target is `outside`, which exists and is writable.
        for (dest, target) in [(&new, &b"../outside"[..]), (&existing, b"../../outside")] {
            let mut checkout = Checkout::start(dest).unwrap();
            checkout.symlink(&path("link"), target).unwrap();
            let err = checkout
                .file(&path("link/x"), false, b"escaped")
                .unwrap_err();
            assert!(format!("{err:#}").contains("symlink"), "{err:#}");
            let err = checkout.symlink(&path("link/deeper/y"), b"z").unwrap_err();
            assert!(format!("{err:#}").contains("symlink"), "{err:#}");
        }

        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
        assert!(!new.exists());
        assert_eq!(names(&existing), Vec::<String>::new());
        assert_eq!(temp_dirs(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn the_rename_is_the_destination_check() {
        let tmp = tempfile::tempdir().unwrap();
        let write = |dest: &Path| {
            let mut checkout = Checkout::start(dest)?;
            checkout.file(&path("a/b"), false, b"b\n")?;
            checkout.finish()
        };

        // Missing, or an empty directory: the checkout takes its place.
        let missing = tmp.path().join("missing");
        let empty = tmp.path().join("empty");
        fs::create_dir(&empty).unwrap();
        for dest in [&missing, &empty] {
            check_dest(dest).unwrap();
            write(dest).unwrap();
            assert_eq!(fs::read(dest.join("a/b")).unwrap(), b"b\n");
        }

        // A non-empty directory, or a symlink to an empty one: refused, as it was.
        let full = tmp.path().join("full");
        fs::create_dir(&full).unwrap();
        fs::write(full.join("keep"), "keep").unwrap();
        let target = tmp.path().join("target");
        fs::create_dir(&target).unwrap();
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        for (dest, why) in [(&full, "not empty"), (&link, "symlink")] {
            assert!(check_dest(dest).unwrap_err().to_string().contains(why));
            let err = write(dest).unwrap_err();
            assert!(format!("{err:#}").contains(why), "{err:#}");
        }
        // A final `/` or `/.` does not make the symlink followed.
        for suffix in ["/", "/."] {
            let dest = PathBuf::from(format!("{}{suffix}", link.display()));
            let err = write(&dest).unwrap_err();
            assert!(format!("{err:#}").contains("symlink"), "{err:#}");
        }

        // Missing at the start, then made before the rename: the rename
        // refuses it.
        for (name, why) in [("late", "not empty"), ("late-link", "symlink")] {
            let dest = tmp.path().join(name);
            let mut checkout = Checkout::start(&dest).unwrap();
            checkout.file(&path("a/b"), false, b"b\n").unwrap();
            if why == "symlink" {
                std::os::unix::fs::symlink(&target, &dest).unwrap();
            } else {
                fs::create_dir(&dest).unwrap();
                fs::write(dest.join("keep"), "keep").unwrap();
            }
            let err = checkout.finish().unwrap_err();
            assert!(format!("{err:#}").contains(why), "{err:#}");
        }
        assert_eq!(names(&tmp.path().join("late")), ["keep"]);

        assert_eq!(fs::read_dir(&full).unwrap().count(), 1);
        assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
        assert!(fs::symlink_metadata(&link).unwrap().is_symlink());
        assert_eq!(temp_dirs(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn an_existing_empty_directory_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let parent = tmp.path().join("parent");
        let dest = parent.join("dest");
        fs::create_dir_all(&dest).unwrap();
        fs::set_permissions(&dest, fs::Permissions::from_mode(0o750)).unwrap();
        let inode = fs::metadata(&dest).unwrap().ino();

        // Nothing can be made next to `dest`.
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o555)).unwrap();
        let written = (|| {
            check_dest(&dest)?;
            let mut checkout = Checkout::start(&dest)?;
            checkout.file(&path("a/b/c"), false, b"c\n")?;
            checkout.symlink(&path("a/link"), b"b/c")?;
            checkout.file(&path("top"), true, b"top\n")?;
            checkout.finish()
        })();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o755)).unwrap();
        written.unwrap();

        assert_eq!(names(&dest), ["a", "top"]);
        assert_eq!(fs::read(dest.join("a/b/c")).unwrap(), b"c\n");
        assert_eq!(
            fs::read_link(dest.join("a/link")).unwrap(),
            Path::new("b/c")
        );
        // The same directory, with its mode.
        assert_eq!(fs::metadata(&dest).unwrap().ino(), inode);
        assert_eq!(mode(&dest), 0o750);
        assert_eq!(names(&parent), ["dest"]);
    }

    #[test]
    fn a_failed_checkout_leaves_an_existing_directory_as_it_was() {
        let tmp = tempfile::tempdir().unwrap();
        let start = |name: &str| {
            let dest = tmp.path().join(name);
            fs::create_dir(&dest).unwrap();
            let mut checkout = Checkout::start(&dest).unwrap();
            for file in ["a/x", "b", "c/y"] {
                checkout.file(&path(file), false, b"ours\n").unwrap();
            }
            (dest, checkout)
        };

        let (dest, checkout) = start("dropped");
        drop(checkout);
        assert_eq!(names(&dest), Vec::<String>::new());

        // A file appears during the checkout, at a name that the checkout
        // uses, or at another one. It stays, and what moved up is removed.
        for (name, theirs) in [("same-name", "b"), ("other-name", "d")] {
            let (dest, checkout) = start(name);
            fs::write(dest.join(theirs), "theirs\n").unwrap();
            let err = checkout.finish().unwrap_err();
            assert!(format!("{err:#}").contains("no longer empty"), "{err:#}");
            assert_eq!(names(&dest), [theirs]);
            assert_eq!(fs::read(dest.join(theirs)).unwrap(), b"theirs\n");
        }
    }

    #[test]
    fn a_directory_of_another_user_is_refused() {
        // `/` belongs to root, and is not empty.
        if rustix::process::geteuid().is_root() {
            return;
        }
        let root = Path::new("/");
        for err in [
            check_dest(root).unwrap_err(),
            Checkout::start(root).err().unwrap(),
        ] {
            assert!(err.to_string().contains("owned by another user"), "{err:#}");
        }
    }

    #[test]
    fn a_directory_that_all_users_can_write_needs_the_sticky_bit() {
        let tmp = tempfile::tempdir().unwrap();
        for (mode, ok) in [(0o777, false), (0o1777, true), (0o775, true), (0o700, true)] {
            let dest = tmp.path().join(format!("{mode:o}"));
            fs::create_dir(&dest).unwrap();
            fs::set_permissions(&dest, fs::Permissions::from_mode(mode)).unwrap();
            let results = [check_dest(&dest), Checkout::start(&dest).map(drop)];
            for result in results {
                match result {
                    Ok(()) => assert!(ok, "{mode:o} was accepted"),
                    Err(e) => {
                        assert!(!ok, "{mode:o}: {e:#}");
                        assert!(e.to_string().contains("writable by all users"), "{e:#}");
                    }
                }
            }
            assert_eq!(names(&dest), Vec::<String>::new());
        }
    }

    #[test]
    fn a_temp_directory_left_behind_is_named() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let left = format!("{TEMP_PREFIX}0123456789abcdef");
        fs::create_dir_all(dest.join(&left)).unwrap();
        let err = check_dest(&dest).unwrap_err().to_string();
        assert!(err.contains("stopped checkout"), "{err}");
        assert!(err.contains(&left), "{err}");
        // Next to anything else, it is a plain "not empty".
        fs::write(dest.join("other"), "").unwrap();
        let err = check_dest(&dest).unwrap_err().to_string();
        assert!(!err.contains("stopped checkout"), "{err}");
    }

    #[test]
    fn sibling_directories_that_share_a_prefix_stay_apart() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let mut checkout = Checkout::start(&dest).unwrap();
        // In path order: `-` sorts before `/`, and `/` before `b`. So `a` is
        // open when `ab/z` comes, and `ab` starts with `a`.
        for file in ["a-b/y", "a/x", "ab/z"] {
            checkout.file(&path(file), false, file.as_bytes()).unwrap();
        }
        checkout.finish().unwrap();
        assert_eq!(names(&dest), ["a", "a-b", "ab"]);
        for (dir, file) in [("a", "x"), ("a-b", "y"), ("ab", "z")] {
            assert_eq!(names(&dest.join(dir)), [file], "{dir}");
        }
    }

    #[test]
    fn the_cleanup_never_follows_a_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("keep"), "keep").unwrap();
        let (new, existing) = (tmp.path().join("new"), tmp.path().join("existing"));
        fs::create_dir(&existing).unwrap();

        for dest in [&new, &existing] {
            let mut checkout = Checkout::start(dest).unwrap();
            let target = outside.as_os_str().as_bytes();
            checkout.symlink(&path("a/link"), target).unwrap();
            checkout.symlink(&path("b"), target).unwrap();
            drop(checkout);
        }

        assert_eq!(names(&outside), ["keep"]);
        assert!(!new.exists());
        assert_eq!(names(&existing), Vec::<String>::new());
        assert_eq!(temp_dirs(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn the_temp_directory_is_private_with_a_random_name() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let first = Checkout::start(&dest).unwrap();
        let [name] = temp_dirs(tmp.path()).try_into().unwrap();
        assert_eq!(mode(&tmp.path().join(&name)), 0o700);
        drop(first);
        // A second checkout from the same process gets another name.
        let _second = Checkout::start(&dest).unwrap();
        assert_ne!(temp_dirs(tmp.path()), [name]);

        // Inside an existing destination too.
        let existing = tmp.path().join("existing");
        fs::create_dir(&existing).unwrap();
        let _inside = Checkout::start(&existing).unwrap();
        let [name] = temp_dirs(&existing).try_into().unwrap();
        assert_eq!(mode(&existing.join(name)), 0o700);
    }

    #[test]
    fn the_result_has_the_mode_of_a_plain_mkdir() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let mut checkout = Checkout::start(&dest).unwrap();
        checkout.file(&path("a/b"), false, b"b\n").unwrap();
        checkout.finish().unwrap();
        let plain = tmp.path().join("plain");
        fs::create_dir(&plain).unwrap();
        // With a umask of 077 both are 0700, and a missing chmod passes.
        assert_eq!(mode(&dest), mode(&plain));
    }

    #[test]
    fn paths_in_path_order_open_each_directory_once() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let mut checkout = Checkout::start(&dest).unwrap();
        let mut paths: Vec<String> = ["a", "a/b", "c"]
            .iter()
            .flat_map(|dir| (0..100).map(move |i| format!("{dir}/f{i:03}")))
            .collect();
        paths.sort_unstable();
        for file in &paths {
            checkout.file(&path(file), false, file.as_bytes()).unwrap();
        }
        // Without the open handles: one open per part of each parent, 400.
        assert_eq!(checkout.opens, 3);
        checkout.finish().unwrap();
        for file in &paths {
            assert_eq!(fs::read(dest.join(file)).unwrap(), file.as_bytes());
        }
    }

    #[test]
    fn paths_deeper_than_the_open_handles_are_written() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let mut checkout = Checkout::start(&dest).unwrap();
        let depth = MAX_OPEN_DIRS + 6;
        let deep = vec!["d"; depth].join("/");
        let files = [
            format!("{deep}/f"),
            format!("{deep}/g"),
            "d/f".into(),
            "e".into(),
        ];
        for file in &files {
            checkout.file(&path(file), false, file.as_bytes()).unwrap();
            assert!(checkout.open.len() <= MAX_OPEN_DIRS);
        }
        // `d` opens again for `d/f`: its handle was closed to keep the bound.
        assert_eq!(checkout.opens, depth + 1);
        checkout.finish().unwrap();
        for file in &files {
            assert_eq!(fs::read(dest.join(file)).unwrap(), file.as_bytes());
        }
    }

    #[test]
    fn refuses_control_characters_and_metadata_names() {
        for ok in [
            &b"a"[..],
            b"git",
            b".gitignore",
            b".scionrc",
            "caf\u{e9}".as_bytes(),
            "\u{100}".as_bytes(), // UTF-8 C4 80: a 0x80 byte that is not C1
            b"\xff",
            b"\xc3",
        ] {
            assert_eq!(check_name(ok), Ok(()), "{}", ok.escape_ascii());
        }
        for bad in [
            &b"a\x1bb"[..],
            b"\x7f",
            b"tab\t",
            "\u{85}".as_bytes(),
            "a\u{9b}b".as_bytes(),
            b"raw\x9b",
            b"\x80",
            "\u{202e}cod.exe".as_bytes(),
            "\u{2066}".as_bytes(),
            "a\u{2069}".as_bytes(),
            b".git",
            b".GIT",
            b".Scion",
        ] {
            assert!(check_name(bad).is_err(), "{}", bad.escape_ascii());
        }

        // The writer applies the rules too, to every part.
        let parent = tempfile::tempdir().unwrap();
        let mut checkout = Checkout::start(&parent.path().join("dest")).unwrap();
        assert!(checkout.file(&path("a/.git/hooks/x"), true, b"x").is_err());
        assert!(checkout.symlink(&path("a/b\x1b"), b"x").is_err());
        let staging = parent.path().join(OsStr::from_bytes(&checkout.temp));
        assert_eq!(fs::read_dir(staging).unwrap().count(), 0);
    }
}
