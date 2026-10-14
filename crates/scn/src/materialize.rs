//! Writes checked-out files under one directory, and the name rules that make
//! that safe.
//!
//! Every write goes through a handle on the checkout root. Each parent
//! directory is created and opened one part at a time with `O_NOFOLLOW`, so a
//! symlink in the way, whether this checkout made it or something else did,
//! fails the write instead of redirecting it. Files are created with
//! `O_EXCL`, so nothing that already exists is overwritten.

use std::fs;
use std::io::{self, ErrorKind, Write as _};
use std::os::fd::OwnedFd;
use std::path::Path;

use anyhow::{Context, Result, bail};
use rustix::fs::{Mode, OFlags};
use rustix::io::Errno;
use scion_core::RepoPath;

/// Directory names that tools keep metadata in. A checkout never writes them.
const RESERVED: [&[u8]; 2] = [b".git", b".scion"];

/// Checks that a file or directory name is safe to write and to print.
///
/// # Errors
///
/// Says why if the name has an ASCII control byte (it could rewrite a
/// terminal when printed), or is `.git` or `.scion` in any ASCII case.
// SHORTCUT: ASCII rules only. Windows device names (`CON`, `NUL`), names that
// end in a dot or a space, and names that differ only in Unicode case or
// normalization still pass. Upgrade: per-platform rules before checkout runs
// on Windows, and Unicode case folding for macOS.
pub fn check_name(name: &[u8]) -> Result<(), &'static str> {
    if name.iter().any(u8::is_ascii_control) {
        Err("it contains a control byte")
    } else if RESERVED
        .iter()
        .any(|reserved| name.eq_ignore_ascii_case(reserved))
    {
        Err("it uses a reserved metadata name")
    } else {
        Ok(())
    }
}

/// The checkout root, open as a directory handle.
pub struct Root {
    dir: OwnedFd,
}

impl Root {
    /// Creates `path` and opens it. An empty directory that already exists is
    /// used as it is.
    ///
    /// # Errors
    ///
    /// Fails if `path` exists and is not an empty directory, or if it cannot
    /// be created or opened.
    pub fn create(path: &Path) -> Result<Self> {
        let shown = path.display();
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                let mut entries =
                    fs::read_dir(path).with_context(|| format!("cannot read {shown}"))?;
                if entries.next().is_some() {
                    bail!("{shown} is not empty");
                }
            }
            Err(e) => return Err(e).with_context(|| format!("cannot create {shown}")),
        }
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let dir = rustix::fs::open(path, flags, Mode::empty())
            .with_context(|| format!("cannot open {shown}"))?;
        Ok(Self { dir })
    }

    /// Writes a regular file. Executable files get mode 0777 and others 0666,
    /// less the umask, as git does.
    ///
    /// # Errors
    ///
    /// Fails if a parent is a symlink or not a directory, if `path` exists,
    /// or if the write fails.
    pub fn file(&self, path: &RepoPath, executable: bool, data: &[u8]) -> Result<()> {
        let write = || -> io::Result<()> {
            let (dir, name) = self.parent(path)?;
            let mode = Mode::from_raw_mode(if executable { 0o777 } else { 0o666 });
            let flags =
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let file = rustix::fs::openat(&dir, name, flags, mode)?;
            fs::File::from(file).write_all(data)
        };
        write().with_context(|| format!("cannot write {path:?}"))
    }

    /// Creates a symlink with `target` as its raw target bytes. The target is
    /// never resolved.
    ///
    /// # Errors
    ///
    /// Fails if a parent is a symlink or not a directory, if `path` exists,
    /// or if the target is not a valid symlink target (empty, or has a NUL).
    pub fn symlink(&self, path: &RepoPath, target: &[u8]) -> Result<()> {
        let link = || -> io::Result<()> {
            let (dir, name) = self.parent(path)?;
            Ok(rustix::fs::symlinkat(target, &dir, name)?)
        };
        link().with_context(|| format!("cannot write {path:?}"))
    }

    /// Opens the directory that holds `path`, creating missing parents, and
    /// returns it with the last part of `path`. Fails before creating
    /// anything if a part breaks [`check_name`].
    // SHORTCUT: one `mkdirat` and one `openat` per parent for every file, and
    // files arrive in blob order, not path order. Upgrade: create each
    // directory once from the walk, fetch blobs in path order, and keep the
    // last directory open.
    fn parent<'p>(&self, path: &'p RepoPath) -> io::Result<(OwnedFd, &'p [u8])> {
        let parts = || path.as_bytes().split(|&b| b == b'/');
        if let Some(why) = parts().find_map(|part| check_name(part).err()) {
            return Err(io::Error::other(why));
        }
        let mut parents = parts();
        // A path has at least one part.
        let name = parents.next_back().unwrap_or_default();
        let mut dir = self.dir.try_clone()?;
        for part in parents {
            dir = open_dir(&dir, part)?;
        }
        Ok((dir, name))
    }
}

/// Opens directory `name` in `dir`, creating it if missing. Fails if `name` is
/// a symlink or anything else that is not a directory.
fn open_dir(dir: &OwnedFd, name: &[u8]) -> io::Result<OwnedFd> {
    match rustix::fs::mkdirat(dir, name, Mode::from_raw_mode(0o777)) {
        Ok(()) | Err(Errno::EXIST) => {}
        Err(e) => return Err(e.into()),
    }
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    rustix::fs::openat(dir, name, flags, Mode::empty()).map_err(|e| match e {
        Errno::LOOP | Errno::NOTDIR => io::Error::other(format!(
            "\"{}\" is a symlink or not a directory",
            name.escape_ascii()
        )),
        e => e.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(p: &str) -> RepoPath {
        RepoPath::new(p).unwrap()
    }

    #[test]
    fn never_writes_through_a_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::create_dir(tmp.path().join("a")).unwrap();
        let dest = tmp.path().join("a/dest");
        let root = Root::create(&dest).unwrap();

        // `dest/link` resolves to `outside`, which exists and is writable.
        root.symlink(&path("link"), b"../../outside").unwrap();
        let err = root.file(&path("link/x"), false, b"escaped").unwrap_err();
        assert!(format!("{err:#}").contains("symlink"), "{err:#}");
        let err = root.symlink(&path("link/deeper/y"), b"z").unwrap_err();
        assert!(format!("{err:#}").contains("symlink"), "{err:#}");

        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
        assert_eq!(
            fs::read_link(dest.join("link")).unwrap(),
            Path::new("../../outside")
        );
    }

    #[test]
    fn writes_only_into_a_new_or_empty_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        Root::create(&dest).unwrap();
        Root::create(&dest).unwrap();
        fs::write(dest.join("x"), "x").unwrap();
        let err = Root::create(&dest).err().unwrap();
        assert!(err.to_string().contains("not empty"), "{err:#}");
    }

    #[test]
    fn refuses_control_bytes_and_metadata_names() {
        for ok in [
            &b"a"[..],
            b"git",
            b".gitignore",
            b".scionrc",
            b"caf\xc3\xa9",
        ] {
            assert_eq!(check_name(ok), Ok(()), "{}", ok.escape_ascii());
        }
        for bad in [
            &b"a\x1bb"[..],
            b"\x7f",
            b"tab\t",
            b".git",
            b".GIT",
            b".Scion",
        ] {
            assert!(check_name(bad).is_err(), "{}", bad.escape_ascii());
        }

        // The writer applies the rules too, to every part.
        let tmp = tempfile::tempdir().unwrap();
        let root = Root::create(tmp.path()).unwrap();
        assert!(root.file(&path("a/.git/hooks/x"), true, b"x").is_err());
        assert!(root.symlink(&path("a/b\x1b"), b"x").is_err());
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 0);
    }
}
