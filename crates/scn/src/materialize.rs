//! Writes a checkout to disk, and the name rules that make that safe.
//!
//! A checkout is written into a new temp directory next to the destination,
//! made through a handle on the destination's parent, with a random name and
//! mode 0700. It is renamed onto the destination name only once every file is
//! written, with the mode a plain `mkdir` would give. The rename is the check
//! that counts: it replaces a missing or empty directory, and fails if the
//! destination is a non-empty directory, a file or a symlink. On any failure
//! the temp directory is removed, so the destination stays as it was.
//!
//! Inside the temp directory every write goes through directory handles: each
//! parent is opened one part at a time with `O_NOFOLLOW`, and files are made
//! with `O_EXCL`, so a symlink or a file in the way fails the write instead of
//! redirecting it. The temp directory starts empty and the checkout records
//! every path it makes, so a path that already exists means the filesystem
//! folded two names into one, by case or by Unicode form.

use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, ErrorKind, Read as _, Write as _};
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rustix::fs::{AtFlags, Mode, OFlags};
use rustix::io::Errno;
use scion_core::RepoPath;

/// Directory names that tools keep metadata in. A checkout never writes them.
const RESERVED: [&[u8]; 2] = [b".git", b".scion"];

/// Name prefix of the temp directory a checkout is written into.
const TEMP_PREFIX: &str = ".scn-checkout-";

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

/// Checks early that `dest` is missing or an empty directory, to fail before
/// any work. [`Checkout::finish`] checks again, atomically.
///
/// # Errors
///
/// Fails if `dest` is a symlink, a file, or a directory that is not empty.
pub fn check_dest(dest: &Path) -> Result<()> {
    let shown = dest.display();
    match fs::symlink_metadata(dest) {
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("cannot read {shown}")),
        Ok(meta) if meta.is_symlink() => bail!("{shown} is a symlink"),
        Ok(meta) if !meta.is_dir() => bail!("{shown} exists and is not a directory"),
        Ok(_) => {
            let mut entries = fs::read_dir(dest).with_context(|| format!("cannot read {shown}"))?;
            if entries.next().is_some() {
                bail!("{shown} is not empty");
            }
            Ok(())
        }
    }
}

/// A checkout being written into a temp directory next to its destination.
/// Dropped without [`Checkout::finish`], it removes the temp directory.
pub struct Checkout {
    /// The destination's parent directory.
    parent: OwnedFd,
    /// The parent's path, used only to remove the temp directory.
    parent_path: PathBuf,
    /// The temp directory's name in `parent`, and a handle on it.
    temp: Vec<u8>,
    root: OwnedFd,
    /// The mode the destination gets: what a plain `mkdir` gives.
    mode: Mode,
    /// The destination's name in `parent`.
    name: Vec<u8>,
    /// Every path this checkout has made, relative to the temp directory.
    made: HashSet<Vec<u8>>,
    finished: bool,
}

impl Checkout {
    /// Makes an empty, private temp directory next to `dest`, through a
    /// handle on the parent of `dest`.
    ///
    /// # Errors
    ///
    /// Fails if `dest` has no final name (for example `..`), or if its parent
    /// cannot be opened or written.
    pub fn start(dest: &Path) -> Result<Self> {
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
        let temp = make_temp_dir(&parent)
            .with_context(|| format!("cannot make a directory in {}", parent_path.display()))?;
        let set_up = || -> io::Result<(OwnedFd, Mode)> {
            let root = open_dir(&parent, &temp)?;
            let mode = mkdir_mode(&root)?;
            Ok((root, mode))
        };
        let (root, mode) = match set_up() {
            Ok(set) => set,
            Err(e) => {
                // The probe removes itself, so the directory is empty and a
                // plain rmdir removes it.
                let _ = rustix::fs::unlinkat(&parent, temp.as_slice(), AtFlags::REMOVEDIR);
                return Err(e).context("cannot set up the new directory");
            }
        };
        Ok(Self {
            parent,
            parent_path,
            temp,
            root,
            mode,
            name: name.as_bytes().to_vec(),
            made: HashSet::new(),
            finished: false,
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
            let file = rustix::fs::openat(&dir, name, flags, mode)
                .map_err(|e| collision_if_exists(e, path.as_bytes()))?;
            self.made.insert(path.as_bytes().to_vec());
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
            rustix::fs::symlinkat(target, &dir, name)
                .map_err(|e| collision_if_exists(e, path.as_bytes()))?;
            self.made.insert(path.as_bytes().to_vec());
            Ok(())
        };
        link().with_context(|| format!("cannot write {path:?}"))
    }

    /// Gives the temp directory the mode of a plain `mkdir`, and renames it
    /// onto the destination.
    ///
    /// # Errors
    ///
    /// Fails, and removes the temp directory, if the destination is a
    /// non-empty directory, a file or a symlink.
    pub fn finish(mut self) -> Result<()> {
        let dest = self.parent_path.join(OsStr::from_bytes(&self.name));
        rustix::fs::fchmod(&self.root, self.mode)
            .map_err(io::Error::from)
            .with_context(|| format!("cannot set the mode of {}", dest.display()))?;
        match rustix::fs::renameat(&self.parent, &self.temp, &self.parent, &self.name) {
            Ok(()) => {
                self.finished = true;
                Ok(())
            }
            Err(Errno::NOTEMPTY | Errno::EXIST) => bail!("{} is not empty", dest.display()),
            Err(Errno::NOTDIR) => {
                bail!("{} is a symlink or not a directory", dest.display())
            }
            Err(e) => Err(io::Error::from(e))
                .with_context(|| format!("cannot move the checkout to {}", dest.display())),
        }
    }

    /// Opens the directory that holds `path` in the temp directory, making
    /// missing parents, and returns it with the last part of `path`. Fails
    /// before making anything if a part breaks [`check_name`].
    // SHORTCUT: one `openat` per parent for every file, and files arrive in
    // blob order, not path order. Upgrade (issue #41): fetch blobs in path
    // order and keep the last directory open.
    fn parent<'p>(&mut self, path: &'p RepoPath) -> io::Result<(OwnedFd, &'p [u8])> {
        let bytes = path.as_bytes();
        let parts = || bytes.split(|&b| b == b'/');
        if let Some(why) = parts().find_map(|part| check_name(part).err()) {
            return Err(io::Error::other(why));
        }
        let mut parents = parts();
        // A path has at least one part.
        let name = parents.next_back().unwrap_or_default();
        let mut dir = self.root.try_clone()?;
        let mut end = 0;
        for part in parents {
            end += part.len();
            let prefix = &bytes[..end];
            if !self.made.contains(prefix) {
                rustix::fs::mkdirat(&dir, part, Mode::from_raw_mode(0o777))
                    .map_err(|e| collision_if_exists(e, prefix))?;
                self.made.insert(prefix.to_vec());
            }
            dir = open_dir(&dir, part)?;
            end += 1;
        }
        Ok((dir, name))
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // SHORTCUT: removes the temp directory by path, not through the
        // parent handle, so if the parent path was swapped, it stays behind.
        // Upgrade: remove it through the parent handle (#44).
        let temp = self.parent_path.join(OsStr::from_bytes(&self.temp));
        // Does not follow symlinks inside the directory.
        if let Err(e) = fs::remove_dir_all(&temp) {
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
    use super::*;

    fn path(p: &str) -> RepoPath {
        RepoPath::new(p).unwrap()
    }

    /// Names in `dir` that a checkout's temp directory would have.
    fn temp_dirs(dir: &Path) -> Vec<String> {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(TEMP_PREFIX))
            .collect()
    }

    #[test]
    fn never_writes_through_a_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        let dest = tmp.path().join("dest");
        let mut checkout = Checkout::start(&dest).unwrap();

        // From the temp directory next to `dest`, `../outside` exists and is
        // writable.
        checkout.symlink(&path("link"), b"../outside").unwrap();
        let err = checkout
            .file(&path("link/x"), false, b"escaped")
            .unwrap_err();
        assert!(format!("{err:#}").contains("symlink"), "{err:#}");
        let err = checkout.symlink(&path("link/deeper/y"), b"z").unwrap_err();
        assert!(format!("{err:#}").contains("symlink"), "{err:#}");

        drop(checkout);
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
        assert!(!dest.exists());
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
        assert_eq!(fs::read_dir(&full).unwrap().count(), 1);
        assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
        assert!(fs::symlink_metadata(&link).unwrap().is_symlink());
        assert_eq!(temp_dirs(tmp.path()), Vec::<String>::new());
    }

    /// The permission bits of `path`, setgid included.
    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt as _;
        fs::metadata(path).unwrap().permissions().mode() & 0o7777
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
