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
//!   attributes. Where the filesystem has no such rename (NFS), each entry
//!   moves up in another way that never replaces: see
//!   [`Checkout::move_up_without_rename`].
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
//! starts empty, only this checkout writes it, and the checkout records every
//! path it makes. So a path that already exists is the call's own, sent again
//! by NFS after a lost reply, or the filesystem folded two names into one, by
//! case or by Unicode form: see [`Checkout::is_lost_reply`].

use std::collections::{BTreeMap, VecDeque};
use std::ffi::{CStr, OsStr};
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::ops::Bound;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use anyhow::{Context, Result, bail};
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, RenameFlags, Stat};
use rustix::io::Errno;
use scion_core::RepoPath;

/// Directory names that tools keep metadata in. A checkout never writes them.
const RESERVED: [&[u8]; 2] = [b".git", b".scion"];

/// Name prefix of the temp directory a checkout is written into.
const TEMP_PREFIX: &str = ".scn-checkout-";

/// The most directory handles a checkout keeps open. A deeper path keeps the
/// innermost ones. macOS allows 256 open files by default.
const MAX_OPEN_DIRS: usize = 64;

/// How a file is made: never over a name that exists, symlink or not.
const NEW_FILE: OFlags = OFlags::WRONLY
    .union(OFlags::CREATE)
    .union(OFlags::EXCL)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

/// The mode of the directory that claims a name where no rename that never
/// replaces exists. See [`Checkout::move_up_without_rename`].
const CLAIM_MODE: Mode = Mode::RWXU;

/// The temp directory that a dropped [`Checkout`] is removing now. A process
/// that exits during the removal names it as left. `scn` removes at most one
/// checkout at a time.
static REMOVING: Mutex<Option<PathBuf>> = Mutex::new(None);

/// The temp directory that a dropped [`Checkout`] is removing now, if any.
pub fn removing() -> Option<PathBuf> {
    REMOVING
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
}

fn set_removing(temp: Option<PathBuf>) {
    *REMOVING.lock().unwrap_or_else(PoisonError::into_inner) = temp;
}

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
    /// Makes `linkat` fail with EPERM, as on a filesystem without hard
    /// links.
    #[cfg(test)]
    no_links: bool,
    /// Makes the checkout take entries of this user ID for its own.
    #[cfg(test)]
    uid: Option<u32>,
}

/// How a test makes a checkout act as on NFS. In each case, the rename that
/// never replaces fails with EINVAL.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum Nfs {
    /// Every other call works.
    Plain,
    /// The step of a move after the one that made the name in the
    /// destination fails.
    StepFails,
    /// Each step works, but its reply is lost. NFS sends the call again, and
    /// that call fails. Every call that makes or removes a name is a step,
    /// from the start of the checkout to its cleanup.
    ReplyLost,
    /// As `ReplyLost`, and the exclusive create of a file in the temp
    /// directory lands without its mode. On NFS version 3 that create sends
    /// no mode: the client sets it after the reply.
    ModeLost,
}

#[cfg(test)]
thread_local! {
    /// Makes the checkouts of this thread act as on NFS. Not a field of
    /// [`Checkout`]: the temp directory is made before the checkout exists.
    static NFS: std::cell::Cell<Option<Nfs>> = const { std::cell::Cell::new(None) };
}

/// What a call in the temp directory makes.
#[derive(Clone, Copy)]
enum New<'t> {
    Dir,
    File,
    /// A symlink with this raw target.
    Symlink(&'t [u8]),
}

impl New<'_> {
    /// Whether the checkout records it as a directory.
    fn is_dir(self) -> bool {
        matches!(self, Self::Dir)
    }
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
            #[cfg(test)]
            no_links: false,
            #[cfg(test)]
            uid: None,
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
            let name = self.parent(path)?;
            let dir = self.held(path.as_bytes());
            let mode = Mode::from_raw_mode(if executable { 0o777 } else { 0o666 });
            let created = self.make_new(dir, path.as_bytes(), New::File, || {
                let file = rustix::fs::openat(dir, name, NEW_FILE, mode);
                #[cfg(test)]
                if let (Some(Nfs::ModeLost), Ok(file)) = (NFS.get(), &file) {
                    rustix::fs::fchmod(file, Mode::empty())?;
                }
                file
            })?;
            // Recorded first, so that the cleanup removes it if a later
            // call fails.
            self.made.insert(path.as_bytes().to_vec(), false);
            let file = if let Some(file) = created {
                file
            } else {
                // Made before its reply was lost, and empty. On NFS version
                // 3 it can also have no mode yet: an exclusive create sends
                // none, and the client sets it only after the reply. Setting
                // it here needs the umask, which cannot be read without
                // changing it, so the file is made once more. A second
                // EEXIST fails.
                let dir = self.held(path.as_bytes());
                step_done(false, Errno::NOENT, || {
                    rustix::fs::unlinkat(dir, name, AtFlags::empty())
                })?;
                rustix::fs::openat(dir, name, NEW_FILE, mode)?
            };
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
            let name = self.parent(path)?;
            let dir = self.held(path.as_bytes());
            self.make_new(dir, path.as_bytes(), New::Symlink(target), || {
                rustix::fs::symlinkat(target, dir, name)
            })?;
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
        // On NFS version 3, a rename sent again after a lost reply fails with
        // ENOENT. It worked if `name` now has the temp directory's inode.
        let moved =
            || rustix::fs::fstat(&self.root).is_ok_and(|temp| has_inode(&self.base, name, &temp));
        match step(false, Some(Errno::NOENT), || {
            rustix::fs::renameat(&self.base, &self.temp, &self.base, name)
        }) {
            Ok(()) => Ok(()),
            Err(Errno::NOENT) if moved() => Ok(()),
            Err(Errno::NOTEMPTY | Errno::EXIST) => bail!("{shown} is not empty"),
            Err(Errno::NOTDIR) => bail!("{shown} is a symlink or not a directory"),
            Err(e) => Err(io::Error::from(e))
                .with_context(|| format!("cannot move the checkout to {shown}")),
        }
    }

    /// Moves each entry of the temp directory up into the destination, never
    /// over something already there. Then checks that the destination holds
    /// only the checkout, and removes the temp directory.
    fn move_up(&mut self) -> Result<()> {
        let shown = self.base_path.display();
        // Set once the filesystem shows it has no rename that never replaces:
        // NFS on Linux, for example.
        let mut fallback = false;
        for name in entries(&self.root).context("cannot read the checkout")? {
            let moved = if fallback {
                self.move_up_without_rename(&name)
            } else {
                match self.rename_up(&name) {
                    Err(Errno::INVAL | Errno::NOTSUP | Errno::NOSYS) => {
                        fallback = true;
                        self.move_up_without_rename(&name)
                    }
                    moved => moved,
                }
            };
            match moved {
                Ok(()) => self.moved.push(name),
                // NOTDIR: a file or symlink took the place of a claimed name.
                Err(Errno::EXIST | Errno::NOTEMPTY | Errno::NOTDIR) => {
                    bail!("{shown} is no longer empty")
                }
                Err(e) => {
                    let why = if fallback {
                        ": this filesystem has no rename that never replaces, and the \
                         fallback failed too (a checkout into a new directory works)"
                    } else {
                        ""
                    };
                    return Err(io::Error::from(e)).with_context(|| {
                        format!("cannot move \"{}\" into {shown}{why}", name.escape_ascii())
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
        // Gone already means its reply was lost. Failing here would make the
        // drop remove the finished checkout.
        step_done(false, Errno::NOENT, || {
            rustix::fs::unlinkat(&self.base, self.temp.as_slice(), AtFlags::REMOVEDIR)
        })
        .map_err(io::Error::from)
        .with_context(|| format!("cannot remove the temp directory in {shown}"))
    }

    /// Moves `name` up from the temp directory by a rename that never
    /// replaces: `RENAME_NOREPLACE` on Linux, `RENAME_EXCL` on macOS.
    fn rename_up(&self, name: &[u8]) -> rustix::io::Result<()> {
        #[cfg(test)]
        if NFS.get().is_some() {
            return Err(Errno::INVAL);
        }
        rustix::fs::renameat_with(&self.root, name, &self.base, name, RenameFlags::NOREPLACE)
    }

    /// Moves `name` up from the temp directory where no rename that never
    /// replaces exists. A file or symlink gets a hard link at `name`, which
    /// never replaces and never follows a symlink, and then loses its temp
    /// name. Where the filesystem has no hard links, it is copied instead:
    /// see [`Checkout::copy_up`]. A directory first claims `name` with a
    /// new, empty, private directory, and then a plain rename replaces that
    /// one. Only a user that [`open_dest`] already trusts can swap the
    /// claimed directory before the rename, and a rename replaces only an
    /// empty directory.
    ///
    /// On NFS version 3, a lost reply makes the client send a call again, and
    /// a call that worked then fails: `linkat` with EEXIST, `renameat` or
    /// `unlinkat` with ENOENT. So after an error, the entry has moved if
    /// `name` in the destination has its inode. A `mkdirat` or `symlinkat`
    /// sent again fails with EEXIST, and makes no inode to compare: see
    /// [`Checkout::is_claim`]. If a later step fails, the name made in the
    /// destination is removed, so the entry is only in the temp directory,
    /// where the cleanup finds it.
    fn move_up_without_rename(&self, name: &[u8]) -> rustix::io::Result<()> {
        // Its type from the filesystem, not from `made`: a filesystem can
        // give back a name in another Unicode form. Only this checkout can
        // write the temp directory.
        let entry = rustix::fs::statat(&self.root, name, AtFlags::SYMLINK_NOFOLLOW)?;
        let moved = |result: rustix::io::Result<()>| {
            result.or_else(|e| {
                if has_inode(&self.base, name, &entry) {
                    Ok(())
                } else {
                    Err(e)
                }
            })
        };
        if FileType::from_raw_mode(entry.st_mode) == FileType::Directory {
            match step(false, Some(Errno::EXIST), || {
                rustix::fs::mkdirat(&self.base, name, CLAIM_MODE)
            }) {
                Err(Errno::EXIST) if self.is_claim(name) => {}
                claimed => claimed?,
            }
            let renamed = step(true, Some(Errno::NOENT), || {
                rustix::fs::renameat(&self.root, name, &self.base, name)
            });
            return moved(renamed).inspect_err(|_| self.remove_from_base(name, true));
        }
        match step(false, Some(Errno::EXIST), || self.link_up(name)) {
            // No hard links, or none to a symlink: some FUSE filesystems,
            // and some filesystems on macOS.
            Err(Errno::PERM | Errno::NOTSUP | Errno::XDEV | Errno::NOSYS) => {
                self.copy_up(name, &entry)?;
            }
            linked => moved(linked)?,
        }
        step_done(true, Errno::NOENT, || {
            rustix::fs::unlinkat(&self.root, name, AtFlags::empty())
        })
        .inspect_err(|_| self.remove_from_base(name, false))
    }

    /// Gives `name` in the temp directory a hard link at `name` in the
    /// destination. Without `AT_SYMLINK_FOLLOW`, a symlink is linked itself.
    fn link_up(&self, name: &[u8]) -> rustix::io::Result<()> {
        #[cfg(test)]
        if self.no_links {
            return Err(Errno::PERM);
        }
        rustix::fs::linkat(&self.root, name, &self.base, name, AtFlags::empty())
    }

    /// Copies the file or symlink `name` from the temp directory to `name`
    /// in the destination, where the filesystem has no hard links. The call
    /// that makes the name never replaces: a file is made with `O_EXCL`,
    /// and `symlinkat` fails if the name exists. If a later step fails, the
    /// name made is removed.
    fn copy_up(&self, name: &[u8], entry: &Stat) -> rustix::io::Result<()> {
        if FileType::from_raw_mode(entry.st_mode) == FileType::Symlink {
            let target = rustix::fs::readlinkat(&self.root, name, Vec::new())?;
            return match step(false, Some(Errno::EXIST), || {
                rustix::fs::symlinkat(target.as_c_str(), &self.base, name)
            }) {
                Err(Errno::EXIST) if self.is_my_symlink(name, &target) => Ok(()),
                made => made,
            };
        }
        let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let from = rustix::fs::openat(&self.root, name, flags, Mode::empty())?;
        // The temp file's mode, which already has the umask. NFS version 3
        // can send an exclusive create again: the server keeps a verifier
        // for it.
        let mode = Mode::from_raw_mode(entry.st_mode & 0o777);
        let to = rustix::fs::openat(&self.base, name, NEW_FILE, mode)?;
        // The call owns both handles, so they close before any unlink: NFS
        // keeps an unlinked file that is still open under another name. A
        // plain `fsync` (not `F_FULLFSYNC`, as `sync_all` is on macOS)
        // reports a write error that a network filesystem can defer, before
        // the temp file goes.
        step(true, None, || {
            let mut to = fs::File::from(to);
            io::copy(&mut fs::File::from(from), &mut to)
                .map_err(|e| Errno::from_io_error(&e).unwrap_or(Errno::IO))?;
            rustix::fs::fsync(&to)
        })
        .inspect_err(|_| self.remove_from_base(name, false))
    }

    /// Whether `name` in the destination, after EEXIST from the `mkdirat`
    /// of [`Checkout::move_up_without_rename`], looks like its claim: an
    /// empty directory with [`CLAIM_MODE`] that this user owns, checked
    /// through a handle. Only this user or root can make a directory that
    /// this user owns, and only a user who can write the destination can
    /// move one in. [`open_dest`] trusts them all.
    fn is_claim(&self, name: &[u8]) -> bool {
        let Ok(dir) = open_dir(&self.base, name) else {
            return false;
        };
        // Without the setgid bit, which the destination can pass on.
        rustix::fs::fstat(&dir).is_ok_and(|stat| {
            self.owns(&stat) && Mode::from_raw_mode(stat.st_mode & 0o777) == CLAIM_MODE
        }) && entries(&dir).is_ok_and(|names| names.is_empty())
    }

    /// [`is_mine`], as the checkout sees it. Tests can make it run as
    /// another user.
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn owns(&self, stat: &Stat) -> bool {
        #[cfg(test)]
        if let Some(uid) = self.uid {
            return stat.st_uid == uid;
        }
        is_mine(stat)
    }

    /// Whether `name` in the destination is a symlink to `target` that this
    /// user owns. As [`Checkout::is_claim`], for `symlinkat`.
    fn is_my_symlink(&self, name: &[u8], target: &CStr) -> bool {
        rustix::fs::statat(&self.base, name, AtFlags::SYMLINK_NOFOLLOW).is_ok_and(|stat| {
            FileType::from_raw_mode(stat.st_mode) == FileType::Symlink && self.owns(&stat)
        }) && rustix::fs::readlinkat(&self.base, name, Vec::new())
            .is_ok_and(|now| now.as_c_str() == target)
    }

    /// Whether the entry at `path` in the temp directory, in `dir`, was made
    /// by the call that has just failed there with EEXIST: NFS version 3
    /// sent that call again after a lost reply. Only this checkout writes
    /// the temp directory, and it records every path it makes. So the entry
    /// is the call's own, or a recorded path in `dir` that the filesystem
    /// folded into `path`, by case or by Unicode form. The call's own entry
    /// is what `new` makes, with nothing in it yet, and is this user's. A
    /// folded one has the inode of a recorded path in `dir`. A directory
    /// that the checkout made already holds an entry, so a folded directory
    /// fails even where each name shows its own inode.
    fn is_lost_reply(&self, dir: BorrowedFd<'_>, path: &[u8], new: New<'_>) -> bool {
        let name = path.rsplit(|&b| b == b'/').next().unwrap_or_default();
        let Ok(stat) = rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) else {
            return false;
        };
        let kind = FileType::from_raw_mode(stat.st_mode);
        let made = match new {
            // `open_dir` refuses anything but a directory.
            New::Dir => open_dir(dir, name)
                .and_then(|opened| entries(&opened))
                .is_ok_and(|names| names.is_empty()),
            New::File => kind == FileType::RegularFile && stat.st_size == 0,
            New::Symlink(target) => {
                kind == FileType::Symlink
                    && rustix::fs::readlinkat(dir, name, Vec::new())
                        .is_ok_and(|now| now.as_bytes() == target)
            }
        };
        if !made || !self.owns(&stat) {
            return false;
        }
        // The recorded paths in `dir` of the same kind, by their names
        // there. Paths below them come too, and are skipped. A stat for each
        // costs time only after EEXIST. One that fails counts as folded: a
        // soft NFS mount can fail it with EIO.
        let (parent, is_dir) = (&path[..path.len() - name.len()], new.is_dir());
        let folded = self
            .made
            .range::<[u8], _>((Bound::Included(parent), Bound::Unbounded))
            .map_while(|(made, &made_dir)| Some((made.strip_prefix(parent)?, made_dir)))
            .filter(|&(other, made_dir)| made_dir == is_dir && !other.contains(&b'/'))
            .any(|(other, _)| {
                rustix::fs::statat(dir, other, AtFlags::SYMLINK_NOFOLLOW).map_or(true, |now| {
                    (now.st_dev, now.st_ino) == (stat.st_dev, stat.st_ino)
                })
            });
        !folded
    }

    /// Runs `call`, which makes `path` in `dir` as `new`, in the temp
    /// directory. Gives `None` if the call failed with EEXIST but had made
    /// the entry before NFS lost its reply: see [`Checkout::is_lost_reply`].
    /// Any other entry there is a path folded into `path`.
    fn make_new<T>(
        &self,
        dir: BorrowedFd<'_>,
        path: &[u8],
        new: New<'_>,
        call: impl FnOnce() -> rustix::io::Result<T>,
    ) -> io::Result<Option<T>> {
        match step(false, Some(Errno::EXIST), call) {
            Ok(made) => Ok(Some(made)),
            Err(Errno::EXIST) if self.is_lost_reply(dir, path, new) => Ok(None),
            Err(Errno::EXIST) => Err(io::Error::other(format!(
                "\"{}\" collides with another path on this filesystem (case or Unicode folding)",
                path.escape_ascii()
            ))),
            Err(e) => Err(e.into()),
        }
    }

    /// Removes `name` from `base`, and warns if it cannot. A directory must
    /// be empty.
    fn remove_from_base(&self, name: &[u8], is_dir: bool) {
        let flags = if is_dir {
            AtFlags::REMOVEDIR
        } else {
            AtFlags::empty()
        };
        if let Err(e) = step_done(false, Errno::NOENT, || {
            rustix::fs::unlinkat(&self.base, name, flags)
        }) {
            let path = self.base_path.join(OsStr::from_bytes(name));
            // After SIGHUP the terminal can be gone. A failed warning must
            // not stop the removal.
            let _ = writeln!(
                io::stderr(),
                "warning: cannot remove {}: {e}",
                path.display()
            );
        }
    }

    /// [`Checkout::holder`] that makes missing parents. Fails before making
    /// anything if a part breaks [`check_name`].
    fn parent<'p>(&mut self, path: &'p RepoPath) -> io::Result<&'p [u8]> {
        let bytes = path.as_bytes();
        let mut parts = bytes.split(|&b| b == b'/');
        if let Some(why) = parts.find_map(|part| check_name(part).err()) {
            return Err(io::Error::other(why));
        }
        self.holder(bytes, true)
    }

    /// Opens the directory that holds `path`, a path relative to the temp
    /// directory, and returns the last part of `path`. [`Checkout::held`]
    /// then gives the directory. With `make`, makes the parents that this
    /// checkout has not made yet.
    ///
    /// The handles on the directories of the last path stay open, so a path
    /// in the same directory, or below it, opens only what is new. A
    /// top-level entry that has moved up is opened in the destination.
    fn holder<'p>(&mut self, path: &'p [u8], make: bool) -> io::Result<&'p [u8]> {
        let (dir, name) = match path.iter().rposition(|&b| b == b'/') {
            Some(slash) => (&path[..slash], &path[slash + 1..]),
            None => (&b""[..], path),
        };
        // Where the first part of `path` is: what the handles start from.
        let top = if self.has_moved(path) {
            &self.base
        } else {
            &self.root
        };
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
                self.make_new(at.as_fd(), prefix, New::Dir, || {
                    rustix::fs::mkdirat(at, part, Mode::from_raw_mode(0o777))
                })?;
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
        Ok(name)
    }

    /// The directory that holds `path`, which [`Checkout::holder`] has just
    /// opened.
    fn held(&self, path: &[u8]) -> BorrowedFd<'_> {
        match self.open.back() {
            Some((_, dir)) => dir.as_fd(),
            // `path` is top-level.
            None if self.has_moved(path) => self.base.as_fd(),
            None => self.root.as_fd(),
        }
    }

    /// The path of the temp directory.
    fn temp_path(&self) -> PathBuf {
        self.base_path.join(OsStr::from_bytes(&self.temp))
    }

    /// Whether the first part of `path` has moved up into the destination.
    fn has_moved(&self, path: &[u8]) -> bool {
        let first = path.split(|&b| b == b'/').next().unwrap_or_default();
        self.moved
            .binary_search_by(|name| name.as_slice().cmp(first))
            .is_ok()
    }

    /// Removes `path`, which this checkout made, through the handles that
    /// stay open. `unlinkat` never follows a symlink. A directory must be
    /// empty.
    fn remove(&mut self, path: &[u8], is_dir: bool) -> io::Result<()> {
        let name = self.holder(path, false)?;
        let flags = if is_dir {
            AtFlags::REMOVEDIR
        } else {
            AtFlags::empty()
        };
        Ok(step_done(false, Errno::NOENT, || {
            rustix::fs::unlinkat(self.held(path), name, flags)
        })?)
    }
}

/// Runs `call`, one call that changes the filesystem. `again` is the error
/// that the call gives when NFS version 3 sends it again after it worked,
/// or `None` if it gives none. Tests make a step of
/// [`Checkout::move_up_without_rename`] `after_claim` fail, or lose each
/// reply.
#[cfg_attr(not(test), allow(unused_variables))]
fn step<T>(
    after_claim: bool,
    again: Option<Errno>,
    call: impl FnOnce() -> rustix::io::Result<T>,
) -> rustix::io::Result<T> {
    #[cfg(test)]
    match (NFS.get(), again) {
        (Some(Nfs::StepFails), _) if after_claim => return Err(Errno::IO),
        (Some(Nfs::ReplyLost | Nfs::ModeLost), Some(again)) => return call().and(Err(again)),
        _ => {}
    }
    call()
}

/// [`step`] for a call that gives `again` when it is done already: only
/// this call makes or removes its name, or the name is gone already.
fn step_done(
    after_claim: bool,
    again: Errno,
    call: impl FnOnce() -> rustix::io::Result<()>,
) -> rustix::io::Result<()> {
    match step(after_claim, Some(again), call) {
        Err(e) if e == again => Ok(()),
        done => done,
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        set_removing(Some(self.temp_path()));
        // In reverse path order, a directory comes after everything in it.
        let (mut first, mut failed) = (None, 0);
        for (path, is_dir) in std::mem::take(&mut self.made).into_iter().rev() {
            if let Err(e) = self.remove(&path, is_dir) {
                first.get_or_insert_with(|| format!("\"{}\": {e}", path.escape_ascii()));
                failed += 1;
            }
        }
        // After SIGHUP the terminal can be gone. A failed warning must not
        // stop the removal.
        if let Some(first) = first {
            let _ = writeln!(
                io::stderr(),
                "warning: cannot remove {failed} paths of the checkout in {}, first {first}",
                self.base_path.display()
            );
        }
        self.remove_from_base(&self.temp, true);
        set_removing(None);
    }
}

/// Makes a new, empty directory in `parent` with a random name and mode 0700,
/// so other users cannot guess its name, enter it, or add to it.
fn make_temp_dir(parent: &OwnedFd) -> io::Result<Vec<u8>> {
    let mut random = [0_u8; 8];
    fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
    let name = format!("{TEMP_PREFIX}{:016x}", u64::from_ne_bytes(random)).into_bytes();
    // On NFS version 3, a `mkdirat` sent again after a lost reply fails with
    // EEXIST. No other call makes this random name.
    step_done(false, Errno::EXIST, || {
        rustix::fs::mkdirat(parent, name.as_slice(), Mode::from_raw_mode(0o700))
    })?;
    Ok(name)
}

/// The mode a plain `mkdir` gives in `dir`: 0777 less the umask, and the
/// setgid bit where `dir` passes it on. Reading the umask changes it for the
/// whole process, so this makes a probe directory in `dir`, which must be
/// empty and private, and reads its mode.
fn mkdir_mode(dir: &OwnedFd) -> io::Result<Mode> {
    // Only this call makes and removes `probe` in `dir`. So EEXIST and ENOENT
    // come from the call that NFS version 3 sent again after a lost reply.
    step_done(false, Errno::EXIST, || {
        rustix::fs::mkdirat(dir, "probe", Mode::from_raw_mode(0o777))
    })?;
    let stat = rustix::fs::statat(dir, "probe", AtFlags::SYMLINK_NOFOLLOW);
    step_done(false, Errno::NOENT, || {
        rustix::fs::unlinkat(dir, "probe", AtFlags::REMOVEDIR)
    })?;
    Ok(Mode::from_raw_mode(stat?.st_mode & 0o7777))
}

/// Whether `name` in `dir` is the inode that `of` describes. After an error
/// in the destination, it tells whether the call that NFS sent again had
/// worked.
fn has_inode(dir: impl AsFd, name: &[u8], of: &Stat) -> bool {
    rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)
        .is_ok_and(|now| (now.st_dev, now.st_ino) == (of.st_dev, of.st_ino))
}

/// Whether the user who runs this process owns what `stat` describes.
fn is_mine(stat: &Stat) -> bool {
    stat.st_uid == rustix::process::geteuid().as_raw()
}

/// Opens directory `name` in `dir`. Fails if `name` is a symlink or anything
/// else that is not a directory.
fn open_dir(dir: impl AsFd, name: &[u8]) -> io::Result<OwnedFd> {
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

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

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

    /// Starts a checkout into `dest`. With `nfs`, the filesystem acts as NFS
    /// does, from the start: it has no rename that never replaces. With
    /// `no_links`, it has no hard links either.
    fn start(dest: &Path, nfs: Option<Nfs>, no_links: bool) -> Result<Checkout> {
        NFS.set(nfs);
        let mut checkout = Checkout::start(dest)?;
        checkout.no_links = no_links;
        Ok(checkout)
    }

    #[test]
    fn an_existing_empty_directory_is_kept() {
        existing_empty_directory_is_kept(None, false);
    }

    #[test]
    fn an_existing_empty_directory_is_kept_without_a_no_replace_rename() {
        existing_empty_directory_is_kept(Some(Nfs::Plain), false);
    }

    #[test]
    fn an_existing_empty_directory_is_kept_when_replies_are_lost() {
        existing_empty_directory_is_kept(Some(Nfs::ReplyLost), false);
    }

    #[test]
    fn an_existing_empty_directory_is_kept_without_hard_links() {
        existing_empty_directory_is_kept(Some(Nfs::Plain), true);
    }

    #[test]
    fn an_existing_empty_directory_is_kept_without_hard_links_when_replies_are_lost() {
        existing_empty_directory_is_kept(Some(Nfs::ReplyLost), true);
    }

    fn existing_empty_directory_is_kept(nfs: Option<Nfs>, no_links: bool) {
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
            let mut checkout = start(&dest, nfs, no_links)?;
            checkout.file(&path("a/b/c"), false, b"c\n")?;
            checkout.symlink(&path("a/link"), b"b/c")?;
            // Moves up while its target is still in the temp directory.
            checkout.symlink(&path("link"), b"top")?;
            checkout.file(&path("top"), true, b"top\n")?;
            checkout.finish()
        })();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o755)).unwrap();
        written.unwrap();

        assert_eq!(names(&dest), ["a", "link", "top"]);
        assert_eq!(fs::read(dest.join("a/b/c")).unwrap(), b"c\n");
        assert_eq!(
            fs::read_link(dest.join("a/link")).unwrap(),
            Path::new("b/c")
        );
        assert_eq!(fs::read_link(dest.join("link")).unwrap(), Path::new("top"));
        assert_eq!(fs::read(dest.join("top")).unwrap(), b"top\n");
        assert_ne!(mode(&dest.join("top")) & 0o100, 0, "top is executable");
        // The same directory, with its mode.
        assert_eq!(fs::metadata(&dest).unwrap().ino(), inode);
        assert_eq!(mode(&dest), 0o750);
        assert_eq!(names(&parent), ["dest"]);
    }

    #[test]
    fn a_failed_checkout_leaves_an_existing_directory_as_it_was() {
        let tmp = tempfile::tempdir().unwrap();
        let begin = |name: &str, nfs: Option<Nfs>, no_links: bool| {
            let dest = tmp.path().join(name);
            fs::create_dir(&dest).unwrap();
            let mut checkout = start(&dest, nfs, no_links).unwrap();
            for file in ["a/x", "b", "c/y"] {
                checkout.file(&path(file), false, b"ours\n").unwrap();
            }
            checkout.symlink(&path("s"), b"ours").unwrap();
            (dest, checkout)
        };

        let (dest, checkout) = begin("dropped", None, false);
        drop(checkout);
        assert_eq!(names(&dest), Vec::<String>::new());

        // An entry appears during the checkout, at a name that the checkout
        // uses for a file, a directory or a symlink, or at another one. It
        // stays as it was, and what moved up is removed. A plain rename
        // would replace the empty directory, which is not mode 0700. With
        // lost replies, an entry that appeared is not taken for one that
        // moved or was made.
        let cases = [
            ("b", "file"),
            ("d", "file"),
            ("a", "file"),
            ("c", "empty directory"),
            ("b", "symlink"),
            ("a", "symlink"),
            ("s", "symlink"),
        ];
        let filesystems = [
            (None, false),
            (Some(Nfs::Plain), false),
            (Some(Nfs::ReplyLost), false),
            (Some(Nfs::Plain), true),
            (Some(Nfs::ReplyLost), true),
        ];
        for (nfs, no_links) in filesystems {
            for (theirs, kind) in cases {
                let name = format!("{nfs:?}-{no_links}-{theirs}-{kind}");
                let (dest, checkout) = begin(&name, nfs, no_links);
                let at = dest.join(theirs);
                match kind {
                    "file" => fs::write(&at, "theirs\n").unwrap(),
                    "symlink" => std::os::unix::fs::symlink("theirs", &at).unwrap(),
                    _ => {
                        fs::create_dir(&at).unwrap();
                        fs::set_permissions(&at, fs::Permissions::from_mode(0o755)).unwrap();
                    }
                }
                let inode = fs::symlink_metadata(&at).unwrap().ino();
                let err = checkout.finish().unwrap_err();
                let case = format!("{theirs} as a {kind}, {nfs:?}, no links: {no_links}");
                assert!(
                    format!("{err:#}").contains("no longer empty"),
                    "{case}: {err:#}"
                );
                assert_eq!(names(&dest), [theirs], "{case}");
                assert_eq!(fs::symlink_metadata(&at).unwrap().ino(), inode, "{case}");
                match kind {
                    "file" => assert_eq!(fs::read(&at).unwrap(), b"theirs\n", "{case}"),
                    "symlink" => {
                        assert_eq!(fs::read_link(&at).unwrap(), Path::new("theirs"), "{case}");
                    }
                    _ => assert_eq!(names(&at), Vec::<String>::new(), "{case}"),
                }
            }
        }
    }

    #[test]
    fn a_failed_move_without_rename_leaves_an_existing_directory_empty() {
        let tmp = tempfile::tempdir().unwrap();
        // The step after the claim fails, for a directory, a file and a
        // symlink. Without hard links, that step is the copy of the file.
        for no_links in [false, true] {
            for first in ["a/x", "b", "s"] {
                let dest = tmp
                    .path()
                    .join(format!("{no_links}-{}", first.replace('/', "-")));
                fs::create_dir(&dest).unwrap();
                let mut checkout = start(&dest, Some(Nfs::StepFails), no_links).unwrap();
                write(&mut checkout, first, b"ours\n").unwrap();
                let err = checkout.finish().unwrap_err();
                assert!(
                    format!("{err:#}").contains("fallback failed too"),
                    "{err:#}"
                );
                let case = format!("{first}, no links: {no_links}");
                assert_eq!(names(&dest), Vec::<String>::new(), "{case}");
            }
        }
    }

    #[test]
    fn a_lost_reply_never_takes_the_entry_of_another_user() {
        let tmp = tempfile::tempdir().unwrap();
        // The claim of `a`, and the symlink `s` without hard links, are
        // made, but their replies are lost. The checkout runs as another
        // user, so what it finds is not its own.
        for (first, no_links) in [("a/x", false), ("s", true)] {
            let dest = tmp.path().join(first.replace('/', "-"));
            fs::create_dir(&dest).unwrap();
            let mut checkout = start(&dest, Some(Nfs::ReplyLost), no_links).unwrap();
            write(&mut checkout, first, b"ours\n").unwrap();
            checkout.uid = Some(rustix::process::geteuid().as_raw() + 1);
            let err = checkout.finish().unwrap_err();
            assert!(
                format!("{err:#}").contains("no longer empty"),
                "{first}: {err:#}"
            );
        }
        // While the checkout is written, too: the directory `d`, the file
        // `f` and the symlink `s`.
        for written in ["d/x", "f", "s"] {
            let dest = tmp.path().join("new");
            let mut checkout = start(&dest, Some(Nfs::ReplyLost), false).unwrap();
            checkout.uid = Some(rustix::process::geteuid().as_raw() + 1);
            let err = write(&mut checkout, written, b"ours").unwrap_err();
            assert!(
                format!("{err:#}").contains("collides"),
                "{written}: {err:#}"
            );
            unrecorded(&checkout, written);
        }
    }

    /// Writes `written` as a symlink to `data` if its last part starts with
    /// `s`, or else as a file.
    fn write(checkout: &mut Checkout, written: &str, data: &[u8]) -> Result<()> {
        let name = written.rsplit('/').next().unwrap_or_default();
        if name.starts_with('s') {
            checkout.symlink(&path(written), data)
        } else {
            checkout.file(&path(written), false, data)
        }
    }

    /// Removes the top-level entry of `written`, which `checkout` has not
    /// recorded, so that its cleanup can remove everything.
    fn unrecorded(checkout: &Checkout, written: &str) {
        let at = checkout
            .temp_path()
            .join(written.split('/').next().unwrap_or_default());
        fs::remove_file(&at)
            .or_else(|_| fs::remove_dir_all(&at))
            .unwrap();
    }

    #[test]
    fn a_new_destination_is_made_when_replies_are_lost() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        // Each call that makes or removes a name finds it done: the temp
        // directory, the probe of the mode, and each directory, file and
        // symlink.
        let mut checkout = start(&dest, Some(Nfs::ReplyLost), false).unwrap();
        checkout.file(&path("a/b/c"), false, b"c\n").unwrap();
        checkout.file(&path("a/empty"), false, b"").unwrap();
        checkout.symlink(&path("a/link"), b"b/c").unwrap();
        checkout.file(&path("x"), true, b"x\n").unwrap();
        checkout.finish().unwrap();

        assert_eq!(names(&dest), ["a", "x"]);
        assert_eq!(names(&dest.join("a")), ["b", "empty", "link"]);
        assert_eq!(fs::read(dest.join("a/b/c")).unwrap(), b"c\n");
        assert_eq!(fs::read(dest.join("a/empty")).unwrap(), b"");
        assert_eq!(
            fs::read_link(dest.join("a/link")).unwrap(),
            Path::new("b/c")
        );
        assert_eq!(fs::read(dest.join("x")).unwrap(), b"x\n");
        assert_ne!(mode(&dest.join("x")) & 0o100, 0, "x is executable");
        let plain = tmp.path().join("plain");
        fs::create_dir(&plain).unwrap();
        assert_eq!(mode(&dest), mode(&plain));
        assert_eq!(names(tmp.path()), ["dest", "plain"]);
    }

    #[test]
    fn a_path_folded_into_a_recorded_one_collides() {
        let tmp = tempfile::tempdir().unwrap();
        // A hard link stands in for a filesystem that folds `y` into the
        // recorded `x`: both names are one inode. The files are empty, and
        // the symlinks have one target, so only the inode tells the fold
        // from a call sent again after a lost reply.
        for nfs in [None, Some(Nfs::ReplyLost)] {
            for (x, y, data) in [("d/x", "d/y", &b""[..]), ("d/sx", "d/sy", b"t")] {
                let mut checkout = start(&tmp.path().join("dest"), nfs, false).unwrap();
                write(&mut checkout, x, data).unwrap();
                let staged = checkout.temp_path();
                rustix::fs::linkat(
                    rustix::fs::CWD,
                    staged.join(x).as_path(),
                    rustix::fs::CWD,
                    staged.join(y).as_path(),
                    AtFlags::empty(),
                )
                .unwrap();
                let err = write(&mut checkout, y, data).unwrap_err();
                let case = format!("{y}, {nfs:?}");
                assert!(format!("{err:#}").contains("collides"), "{case}: {err:#}");
                fs::remove_file(staged.join(y)).unwrap();
            }
        }
    }

    #[test]
    fn an_entry_unlike_what_the_call_makes_collides() {
        let tmp = tempfile::tempdir().unwrap();
        // An entry that the checkout has not recorded, at the name that a
        // call makes, with an inode that no recorded path has. It is the
        // call's own only if it is what the call makes.
        let cases = [
            ("a file with data", "f"),
            ("an empty file", "d/x"),
            ("an empty directory", "f"),
            ("a directory with an entry", "d/x"),
            ("a symlink to an empty directory", "d/x"),
            ("a symlink to another target", "s"),
        ];
        let empty = tmp.path().join("empty");
        fs::create_dir(&empty).unwrap();
        for (there, written) in cases {
            let mut checkout = start(&tmp.path().join("dest"), None, false).unwrap();
            let at = checkout
                .temp_path()
                .join(written.split('/').next().unwrap_or_default());
            match there {
                "a file with data" => fs::write(&at, "theirs"),
                "an empty file" => fs::write(&at, ""),
                "an empty directory" => fs::create_dir(&at),
                "a directory with an entry" => {
                    fs::create_dir(&at).and_then(|()| fs::write(at.join("theirs"), ""))
                }
                "a symlink to an empty directory" => std::os::unix::fs::symlink(&empty, &at),
                "a symlink to another target" => std::os::unix::fs::symlink("theirs", &at),
                _ => unreachable!("{there}"),
            }
            .unwrap();
            let err = write(&mut checkout, written, b"ours").unwrap_err();
            assert!(format!("{err:#}").contains("collides"), "{there}: {err:#}");
            unrecorded(&checkout, written);
        }
    }

    #[test]
    fn a_recorded_path_that_cannot_be_read_counts_as_folded() {
        let tmp = tempfile::tempdir().unwrap();
        // The stat of the recorded `d/x` fails, here with ENOENT, and on a
        // soft NFS mount with EIO. `d/y` looks like the call's own, but a
        // fold of the two cannot be ruled out.
        let mut checkout = start(&tmp.path().join("dest"), None, false).unwrap();
        write(&mut checkout, "d/x", b"").unwrap();
        let d = checkout.temp_path().join("d");
        fs::remove_file(d.join("x")).unwrap();
        fs::write(d.join("y"), "").unwrap();
        let err = write(&mut checkout, "d/y", b"").unwrap_err();
        assert!(format!("{err:#}").contains("collides"), "{err:#}");
        fs::remove_file(d.join("y")).unwrap();
    }

    #[test]
    fn a_file_found_after_a_lost_reply_gets_its_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        // Each exclusive create lands with mode 0, and its reply is lost.
        let mut checkout = start(&dest, Some(Nfs::ModeLost), false).unwrap();
        checkout.file(&path("f"), false, b"f\n").unwrap();
        checkout.file(&path("x"), true, b"x\n").unwrap();
        checkout.finish().unwrap();
        // The modes that a plain create gives, less the umask.
        let plain = |name: &str, bits| {
            let at = tmp.path().join(name);
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true).mode(bits);
            options.open(&at).unwrap();
            mode(&at)
        };
        assert_eq!(mode(&dest.join("f")), plain("plain-f", 0o666));
        assert_eq!(mode(&dest.join("x")), plain("plain-x", 0o777));
        assert_eq!(fs::read(dest.join("f")).unwrap(), b"f\n");
        assert_eq!(fs::read(dest.join("x")).unwrap(), b"x\n");
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
        assert_eq!(fs::read_dir(checkout.temp_path()).unwrap().count(), 0);
    }
}
