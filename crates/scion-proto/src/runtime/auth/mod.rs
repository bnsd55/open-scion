//! Bearer tokens: the check on every RPC ([`Auth`]), shared by the server and
//! the cache tiers, and the client side that sends the token ([`Bearer`]).
//! Also the check that no other user can swap a local path on its way
//! ([`check_ancestors`]), shared by `scn` and the daemon, and the way every
//! process makes its own directories private ([`create_private_dir_all`]).
//! Files that hold secrets, or keys that must stay whole, are read through
//! one checked descriptor ([`read_secret_file`], [`read_shared_file`]),
//! whose ACL checks wrap the one `ffi` module.

use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

#[cfg(target_os = "macos")]
mod ffi;

use rustix::fs::OFlags;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Who made a request: the principal of its token. Added to request
/// extensions by [`Auth`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Caller(pub String);

/// The principal of the one token that [`Auth::new`] accepts.
pub const DEFAULT_PRINCIPAL: &str = "default";

/// Accepts requests that carry `authorization: Bearer <token>` with a known
/// token, and tags each with the token's principal.
#[derive(Clone)]
pub struct Auth {
    /// The BLAKE3 digest of each token, and its principal. A presented token
    /// is hashed, so every comparison has the same length.
    tokens: Arc<[([u8; 32], Caller)]>,
}

impl Auth {
    /// Accepts this one token, as principal [`DEFAULT_PRINCIPAL`].
    #[must_use]
    pub fn new(token: &str) -> Self {
        Self::digests([(token_digest(token), Caller(DEFAULT_PRINCIPAL.into()))])
    }

    /// Accepts each token whose digest ([`token_digest`]) is given, as its
    /// principal. The digests must differ, so a token has one principal.
    #[must_use]
    pub fn digests(tokens: impl IntoIterator<Item = ([u8; 32], Caller)>) -> Self {
        Self {
            tokens: tokens.into_iter().collect(),
        }
    }

    /// The caller of `request`, from its `authorization` header. Every
    /// call but `GetObjects` needs one: the permission of an object read
    /// is its ticket, not a token.
    ///
    /// # Errors
    ///
    /// [`Status::unauthenticated`] without a `Bearer` token of this set.
    pub fn caller_of<T>(&self, request: &Request<T>) -> Result<Caller, Status> {
        request
            .metadata()
            .get("authorization")
            .and_then(|value| value.as_bytes().strip_prefix(b"Bearer "))
            .and_then(|token| self.principal(token))
            .cloned()
            .ok_or_else(|| Status::unauthenticated("missing or wrong token"))
    }

    /// The principal of `token`, if any. Compares its digest with every
    /// known digest, without an early exit, so the time does not depend on
    /// which digest matches or how much of one does.
    fn principal(&self, token: &[u8]) -> Option<&Caller> {
        let presented = digest(token);
        let mut found = None;
        for (known, caller) in self.tokens.iter() {
            if constant_time_eq(&presented, known) {
                found = Some(caller);
            }
        }
        found
    }
}

impl Interceptor for Auth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let caller = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.as_bytes().strip_prefix(b"Bearer "))
            .and_then(|token| self.principal(token))
            .cloned()
            .ok_or_else(|| {
                // One line for the refusal. The span's fields name the RPC
                // and the peer. Never the token, not even its digest.
                tracing::warn!(
                    reason = "missing or wrong token",
                    "the call is unauthenticated"
                );
                Status::unauthenticated("missing or wrong token")
            })?;
        request.extensions_mut().insert(caller);
        Ok(request)
    }
}

/// The digest of a token that [`Auth::digests`] takes: BLAKE3 of its bytes.
#[must_use]
pub fn token_digest(token: &str) -> [u8; 32] {
    digest(token.as_bytes())
}

/// Adds `authorization: Bearer <token>` to every request.
#[derive(Clone)]
pub struct Bearer(MetadataValue<Ascii>);

impl Bearer {
    /// Sends this token.
    ///
    /// # Errors
    ///
    /// If the token cannot go in a header: it is not visible ASCII.
    pub fn new(token: &str) -> Result<Self, InvalidMetadataValue> {
        format!("Bearer {token}").parse().map(Self)
    }
}

impl Interceptor for Bearer {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}

/// Resolves the directory that holds `path`, then checks each directory from
/// there up to `/`. Each must be owned by root or by user `me`, and must not
/// be writable by group or others unless it has the sticky bit (as `/tmp`
/// has). Else another user could put their own socket or directory at
/// `path` after a check of `path` alone.
///
/// Returns `path` with its directory resolved. Use it, not `path`, so no
/// symlink on the way is followed again after the check. The last part of
/// `path` is not resolved or checked: that is the caller's check.
///
/// A symlink on the way, in a directory that another user can write, is
/// still theirs to repoint before the check. They cannot point it at a
/// directory of their own, as that fails the check. But they can choose
/// which directory of root or this user is used: for the daemon, which
/// directory's `tmp` is cleared and whose `objects` are evicted. And what the
/// caller does through `path` before the check goes where the link pointed
/// then: `DiskStore::open` makes its directory first.
///
/// By choice, as OpenSSH's `StrictModes`, a group-writable directory is
/// refused: the sticky bit excuses a group only when others can write too
/// (the `/tmp` shape, `01777`). Only the owner and the mode bits are read
/// on the walk itself, and an ACL that grants what the mode's bits do
/// not is refused too: on Linux by the POSIX-ACL xattrs, on macOS through
/// the descriptor's extended ACL (the one `ffi` module the workspace's
/// lint lets hold it).
///
/// # Errors
///
/// [`io::ErrorKind::PermissionDenied`] naming the first directory that breaks
/// a rule, and why. I/O errors if the directory cannot be resolved.
pub fn check_ancestors(path: &Path, me: u32) -> io::Result<PathBuf> {
    // A path that ends in `.` or `..` has no last part: resolve all of it.
    let path = match path.file_name() {
        Some(_) => path.to_owned(),
        None => fs::canonicalize(path)?,
    };
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return Ok(path); // `/`: nothing is above it.
    };
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    // The resolved directory has no symlink. Each directory on it is checked,
    // so no other user can change it after the check.
    let parent = fs::canonicalize(parent)?;
    for dir in parent.ancestors() {
        let meta = fs::symlink_metadata(dir)?;
        let (why, fix) = if meta.uid() != 0 && meta.uid() != me {
            let why = format!("is owned by another user (uid {})", meta.uid());
            (why, String::new())
        } else if meta.mode() & 0o020 != 0 && meta.mode() & 0o002 == 0 {
            // A group that can write the directory can plant and swap its
            // own entries, and the sticky bit does not stop that: it
            // bounds who may remove a name, not who may add one. It
            // excuses a group only when others can write too, the `/tmp`
            // shape, where the others-write rule below is the real wall.
            let why = "can be written by the group, and the sticky bit does not excuse a group"
                .to_owned();
            (why, format!(" (fix: chmod g-w {})", dir.display()))
        } else if meta.mode() & 0o002 != 0 && meta.mode() & 0o1000 == 0 {
            let why = "can be written by others, and has no sticky bit".to_owned();
            (why, format!(" (fix: chmod go-w {})", dir.display()))
        } else {
            // (Linux, macOS) An ACL that grants what the mode's bits do
            // not is one more way another account reaches this directory.
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            if ancestor_has_hidden_acl(dir, meta.mode())? {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "{} carries an ACL beyond its mode bits, so another user could swap {} (fix: setfacl -b {})",
                        dir.display(),
                        path.display(),
                        dir.display()
                    ),
                ));
            }
            continue;
        };
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} {why}, so another user could swap {}{fix}",
                dir.display(),
                path.display()
            ),
        ));
    }
    Ok(parent.join(name))
}

/// Reads `file` for a secret that only the account that runs this process
/// (or root) may hold: an access file of bearer tokens, a TLS private key,
/// a ticket seed. The directories above `file` are checked
/// ([`check_ancestors`]), the file is opened once through the resolved path
/// with `O_NOFOLLOW`, and kind, owner (this user or root), and mode run on
/// the open descriptor that the bytes are read from: a regular file that no
/// group or others account can read. A swap between the checks and the read
/// then dies at the open or on the descriptor's checks, and the bytes come
/// from the checked descriptor either way. Only the owner, the group and
/// the mode bits are read on the descriptor, and an ACL that grants
/// what the mode's bits do not is refused too: on Linux by the POSIX-ACL
/// xattrs, on macOS through the descriptor's extended ACL.
///
/// # Errors
///
/// [`io::ErrorKind::PermissionDenied`] naming the rule that broke and the
/// `chmod` that fixes it; I/O errors if the file cannot be opened or read.
pub fn read_secret_file(file: &Path) -> io::Result<Vec<u8>> {
    read_checked_file(file, true)
}

/// Reads `file` that is public but must stay whole, such as the public key
/// that a ticket rotation keeps alive: the same one-descriptor check as
/// [`read_secret_file`], except the mode rule is write, not read - no group
/// or others account may be able to write it, because a changed key breaks
/// every tier that verifies against the old one. Read permission for the
/// others is fine.
///
/// # Errors
///
/// As [`read_secret_file`].
pub fn read_shared_file(file: &Path) -> io::Result<Vec<u8>> {
    read_checked_file(file, false)
}

/// Wraps the error of [`read_secret_file`] or [`read_shared_file`] for a
/// caller that reports with anyhow: a refusal already names the file and
/// the fix, so it stands alone; every other error gets the file named.
///
/// # Errors
///
/// Always: `why`, as the caller can report it.
#[must_use]
pub fn checked_read_error(file: &Path, why: io::Error) -> anyhow::Error {
    if why.kind() == io::ErrorKind::PermissionDenied {
        anyhow::anyhow!("{why}")
    } else {
        anyhow::Error::new(why).context(format!("cannot read {}", file.display()))
    }
}

/// [`read_secret_file`] and [`read_shared_file`]: `secret` picks the mode
/// rule (no group or others read, or no write).
fn read_checked_file(file: &Path, secret: bool) -> io::Result<Vec<u8>> {
    let mut opened = open_checked_file(file, secret)?;
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut opened, &mut bytes)?;
    Ok(bytes)
}

/// Opens `file` for a secret or a whole-keeping read, with every check of
/// [`read_secret_file`] run on the open descriptor, and returns the
/// checked file for the caller to read itself: a caller with a large file
/// streams it through a [`std::io::BufReader`] instead of holding the
/// whole file in memory. `secret` picks the mode rule (no group or others
/// read, or no write).
///
/// # Errors
///
/// As [`read_secret_file`].
pub fn open_checked_file(file: &Path, secret: bool) -> io::Result<std::fs::File> {
    let me = rustix::process::geteuid().as_raw();
    let resolved = check_ancestors(file, me)?;
    let opened = rustix::fs::open(
        &resolved,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|why| {
        if why == rustix::io::Errno::LOOP {
            // ELOOP says a symlink refused the open: at the file itself, or
            // a loop on the way. The first is worth naming precisely.
            let at_end = match fs::symlink_metadata(resolved) {
                Ok(meta) => meta.file_type().is_symlink(),
                Err(_) => false,
            };
            if at_end {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("{} is a symlink (a symlink is refused too)", file.display()),
                )
            } else {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("a symlink loop blocks {}", file.display()),
                )
            }
        } else {
            why.into()
        }
    })?;
    let opened = fs::File::from(opened);
    let meta = opened.metadata()?;
    ensure_file_checks(meta.mode(), meta.uid(), me, secret, file)?;
    // (Linux, macOS) An ACL that grants what the mode's bits do not is
    // one more account that can reach this file: the check runs on the
    // checked descriptor, before the caller reads anything from it.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    if fd_has_hidden_acl(&opened, meta.mode(), false) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} carries an ACL beyond its mode bits, so another user could reach it (fix: setfacl -b {})",
                file.display(),
                file.display()
            ),
        ));
    }
    Ok(opened)
}

/// The extended attributes that carry POSIX ACLs, and the tags and id of
/// the minimal form's three entries.
#[cfg(target_os = "linux")]
const POSIX_ACL_ACCESS: &str = "system.posix_acl_access";
#[cfg(target_os = "linux")]
const POSIX_ACL_DEFAULT: &str = "system.posix_acl_default";

/// Whether a POSIX access-ACL attribute keeps the file's own mode: only
/// the exact minimal form adds nothing the mode does not already grant.
/// The minimal form is 28 bytes: version 2, then one `USER_OBJ`, one
/// `GROUP_OBJ` and one `OTHER` entry, each tag, perm and an undefined id,
/// and the permissions equal to the mode's own bits. A named user or
/// group, a mask, any other version or length, or a perm above the mode's
/// bits grants access beyond what the owner asked for.
#[cfg(any(target_os = "linux", test))]
#[must_use]
fn minimal_acl_matches_mode(acl: &[u8], mode: u32) -> bool {
    // The minimal form's entries, in order: tag, the mode's bits for that
    // class, and the undefined id every minimal entry carries.
    let want = [
        (0x0001u16, ((mode >> 6) & 7) as u16), // ACL_USER_OBJ
        (0x0004u16, ((mode >> 3) & 7) as u16), // ACL_GROUP_OBJ
        (0x0020u16, (mode & 7) as u16),        // ACL_OTHER
    ];
    if acl.len() != 28 {
        return false;
    }
    if u32::from_le_bytes([acl[0], acl[1], acl[2], acl[3]]) != 2 {
        return false;
    }
    for (i, (tag, perm)) in want.iter().enumerate() {
        let entry = &acl[4 + i * 8..4 + (i + 1) * 8];
        if u16::from_le_bytes([entry[0], entry[1]]) != *tag {
            return false;
        }
        if u16::from_le_bytes([entry[2], entry[3]]) != *perm {
            return false;
        }
        if u32::from_le_bytes([entry[4], entry[5], entry[6], entry[7]]) != u32::MAX {
            return false;
        }
    }
    true
}

/// Whether the descriptor's ACLs grant what `mode`'s bits do not. On
/// Linux: a POSIX access ACL beyond the exact minimal form, or, on a
/// directory, any default ACL at all (files created under it would
/// inherit one). On macOS: any extended ACL at all - a descriptor either
/// has ACL entries or it has none. A missing attribute is fine; any read
/// error other than `ENODATA` is a hidden ACL too, because this check
/// cannot vouch for what it cannot read. The FFI this needs on macOS
/// lives in `ffi`, the one module the workspace's unsafe-code lint lets
/// hold it.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn fd_has_hidden_acl(fd: impl rustix::fd::AsFd, mode: u32, dir: bool) -> bool {
    #[cfg(target_os = "linux")]
    {
        // A minimal ACL xattr is 28 bytes; 64 covers any form this check
        // could accept, and a longer one is refused by the parse. The
        // read answers the byte count; the bytes live in `buf`.
        let mut buf = [0u8; 64];
        let fd = rustix::fd::AsFd::as_fd(&fd);
        match rustix::fs::fgetxattr(fd, POSIX_ACL_ACCESS, &mut buf) {
            Err(rustix::io::Errno::NODATA) => {}
            Ok(len) => {
                if !minimal_acl_matches_mode(&buf[..len], mode) {
                    return true;
                }
            }
            Err(_) => return true,
        }
        if dir {
            let default = rustix::fs::fgetxattr(fd, POSIX_ACL_DEFAULT, &mut buf);
            return !matches!(default, Err(rustix::io::Errno::NODATA));
        }
        false
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd as _;
        let _ = (mode, dir);
        ffi::fd_has_acl(fd.as_fd().as_raw_fd())
    }
}

/// Whether an ancestor directory's ACLs grant what its mode's bits do
/// not. Opened `O_NOFOLLOW` and descriptor-checked, so the fd is the
/// directory the walk checked; `O_PATH` on Linux needs no permission of
/// its own.
#[cfg(target_os = "linux")]
fn ancestor_has_hidden_acl(dir: &Path, mode: u32) -> io::Result<bool> {
    let opened = rustix::fs::open(
        dir,
        OFlags::PATH | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    Ok(fd_has_hidden_acl(&opened, mode, true))
}

/// The macOS ancestor check: `O_DIRECTORY` does not exist there, so the
/// directory opens read-only, still `O_NOFOLLOW`.
#[cfg(target_os = "macos")]
fn ancestor_has_hidden_acl(dir: &Path, _mode: u32) -> io::Result<bool> {
    let opened = rustix::fs::open(
        dir,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    Ok(fd_has_hidden_acl(&opened, 0, true))
}

/// The descriptor checks of [`read_checked_file`]: a regular file, owned by
/// the account that runs this process or by root, with a mode that keeps
/// the file whole (a secret is also read by its owner alone). Pure over the
/// fstat fields, so the tests can prove each refusal without a second
/// account.
/// The type and permission bits of an `st_mode`, as POSIX fixes them for
/// every platform: the kind in the `S_IFMT` mask, `S_IFREG` for a regular
/// file. Plain constants, so the check is over the `u32` mode the tests
/// hold, with no conversion that is a no-op on Linux and would truncate
/// on macOS.
const S_IFMT: u32 = 0o170_000;
const S_IFREG: u32 = 0o100_000;

fn ensure_file_checks(mode: u32, uid: u32, me: u32, secret: bool, file: &Path) -> io::Result<()> {
    if mode & S_IFMT != S_IFREG {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} is not a file (a symlink is refused too)",
                file.display()
            ),
        ));
    }
    if uid != 0 && uid != me {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} is owned by another user (uid {}); this process runs as uid {}",
                file.display(),
                uid,
                me
            ),
        ));
    }
    let bits = mode & 0o777;
    if secret {
        if bits.trailing_zeros() < 6 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "{} can be read by group or others (fix: chmod 600 {})",
                    file.display(),
                    file.display()
                ),
            ));
        }
    } else if bits & 0o022 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} can be written by group or others (fix: chmod go-w {})",
                file.display(),
                file.display()
            ),
        ));
    }
    Ok(())
}

/// Makes `dir` and each missing directory above it with mode 0700, where
/// [`fs::create_dir_all`] asks for 0777. A umask only takes bits away from
/// the mode asked for, so whatever the umask is, group and others get no
/// access: Ubuntu's default umask 002 does not make these directories
/// writable by the group, as it does with 0777. A directory that exists
/// already keeps its mode, for the caller's own check, such as
/// [`check_ancestors`], to judge.
///
/// # Errors
///
/// As [`fs::DirBuilder::create`]: a part of `dir` is a file, or cannot be
/// made.
pub fn create_private_dir_all(dir: &Path) -> io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

fn digest(token: &[u8]) -> [u8; 32] {
    *blake3::hash(token).as_bytes()
}

/// Compares two digests without an early exit, so timing reveals nothing.
fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0, |diff, (x, y)| diff | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    use super::*;

    /// A temp directory, and its owner.
    fn tempdir() -> (tempfile::TempDir, u32) {
        let tmp = tempfile::tempdir().unwrap();
        let me = fs::metadata(tmp.path()).unwrap().uid();
        (tmp, me)
    }

    fn mkdir(path: &Path, mode: u32) {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    fn mode_of(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().mode() & 0o7777
    }

    #[test]
    fn private_directories_are_made_0700_and_existing_ones_keep_their_mode() {
        let (tmp, me) = tempdir();
        // The temp directory's own mode is the umask's: set it, so only the
        // directories made below it are judged.
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        // Every part that is made is private, not only the last one. The
        // run's umask, 002 or 022, would leave 0775 or 0755 on a plain mkdir.
        create_private_dir_all(&tmp.path().join("a/b/c")).unwrap();
        for made in ["a", "a/b", "a/b/c"] {
            assert_eq!(mode_of(&tmp.path().join(made)), 0o700, "{made}");
        }
        check_ancestors(&tmp.path().join("a/b/c/daemon.sock"), me).unwrap();
        // Again: a directory that exists is no error.
        create_private_dir_all(&tmp.path().join("a/b/c")).unwrap();
        // One that exists keeps its mode, open or not: the checks judge it.
        mkdir(&tmp.path().join("open"), 0o777);
        create_private_dir_all(&tmp.path().join("open/new")).unwrap();
        assert_eq!(mode_of(&tmp.path().join("open")), 0o777);
        assert_eq!(mode_of(&tmp.path().join("open/new")), 0o700);
        // A file in the way fails.
        fs::write(tmp.path().join("file"), b"").unwrap();
        create_private_dir_all(&tmp.path().join("file/dir")).unwrap_err();
    }

    #[test]
    fn each_token_gets_its_own_principal() {
        let caller = |name: &str| Caller(name.into());
        let call = |auth: &mut Auth, header: Option<&str>| {
            let mut request = Request::new(());
            if let Some(header) = header {
                let value = header.parse().unwrap();
                request.metadata_mut().insert("authorization", value);
            }
            let request = auth.call(request)?;
            Ok::<_, Status>(request.extensions().get::<Caller>().cloned())
        };
        let mut auth = Auth::digests([
            (token_digest("a-token"), caller("alice")),
            (token_digest("b-token"), caller("bob")),
        ]);
        assert_eq!(
            call(&mut auth, Some("Bearer a-token")).unwrap(),
            Some(caller("alice"))
        );
        assert_eq!(
            call(&mut auth, Some("Bearer b-token")).unwrap(),
            Some(caller("bob"))
        );
        for bad in [
            None,
            Some("Bearer c-token"),
            Some("a-token"),
            Some("Bearer a-token "),
        ] {
            let status = call(&mut auth, bad).unwrap_err();
            assert_eq!(status.code(), tonic::Code::Unauthenticated, "{bad:?}");
        }
        let mut single = Auth::new("t");
        let default = Some(caller(DEFAULT_PRINCIPAL));
        assert_eq!(call(&mut single, Some("Bearer t")).unwrap(), default);
    }

    #[test]
    fn a_private_tree_is_accepted_for_its_owner_only() {
        let (tmp, me) = tempdir();
        let real = fs::canonicalize(tmp.path()).unwrap();
        let path = tmp.path().join("daemon.sock");
        assert_eq!(
            check_ancestors(&path, me).unwrap(),
            real.join("daemon.sock")
        );
        // A path with no last part is resolved whole.
        mkdir(&tmp.path().join("sub"), 0o700);
        assert_eq!(
            check_ancestors(&tmp.path().join("sub/.."), me).unwrap(),
            real
        );
        // The same tree, as another user sees it. A directory that root owns
        // is accepted for every user.
        if me != 0 {
            let err = check_ancestors(&path, me + 1).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
            assert!(err.to_string().contains("owned by another user"), "{err}");
        }
    }

    #[test]
    fn a_directory_that_others_can_write_is_refused_unless_sticky() {
        let (tmp, me) = tempdir();
        let open = tmp.path().join("open");
        mkdir(&open, 0o777);
        mkdir(&open.join("private"), 0o700);
        // Two levels down, so the walk must go past the first parent.
        let path = open.join("private/daemon.sock");
        let err = check_ancestors(&path, me).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        assert!(err.to_string().contains("/open can be written"), "{err}");
        let fix = format!("chmod go-w {}", fs::canonicalize(&open).unwrap().display());
        assert!(err.to_string().contains(&fix), "{err}");
        // Others cannot rename or remove what they do not own in it.
        fs::set_permissions(&open, fs::Permissions::from_mode(0o1777)).unwrap();
        check_ancestors(&path, me).unwrap();
        // Writable by the group only is refused too.
        fs::set_permissions(&open, fs::Permissions::from_mode(0o770)).unwrap();
        check_ancestors(&path, me).unwrap_err();
    }

    /// A group-writable ancestor is refused whatever the sticky bit does:
    /// the sticky bit bounds who may remove a name, not who may add one.
    /// It excuses a group only when others can write too, the `/tmp` shape.
    #[test]
    fn a_group_writable_ancestor_is_refused_even_with_the_sticky_bit() {
        let (tmp, me) = tempdir();
        let open = tmp.path().join("open");
        mkdir(&open, 0o700);
        mkdir(&open.join("private"), 0o700);
        let path = open.join("private/daemon.sock");
        // 01770 and 02770: sticky, group-writable, others clear.
        for mode in [0o1770, 0o2770] {
            fs::set_permissions(&open, fs::Permissions::from_mode(mode)).unwrap();
            let err = check_ancestors(&path, me).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
            assert!(
                err.to_string()
                    .contains("the sticky bit does not excuse a group"),
                "{err}"
            );
        }
        // The `/tmp` shape keeps passing: 01777 is sticky, g+w and o+w.
        fs::set_permissions(&open, fs::Permissions::from_mode(0o1777)).unwrap();
        check_ancestors(&path, me).unwrap();
    }

    /// Builds a POSIX ACL xattr: version 2, then the entries as
    /// (tag, perm, id), the tags the POSIX draft fixes: `ACL_USER_OBJ` 1,
    /// `ACL_GROUP_OBJ` 4, `ACL_OTHER` 32, `ACL_USER` 2, `ACL_MASK` 16.
    fn acl_bytes(entries: &[(u16, u16, u32)]) -> Vec<u8> {
        let mut bytes = 2u32.to_le_bytes().to_vec();
        for (tag, perm, id) in entries {
            bytes.extend_from_slice(&tag.to_le_bytes());
            bytes.extend_from_slice(&perm.to_le_bytes());
            bytes.extend_from_slice(&id.to_le_bytes());
        }
        bytes
    }

    /// The exact minimal ACL of the mode parses: one `USER_OBJ`, one
    /// `GROUP_OBJ`, one `OTHER`, undefined ids, perms equal to the mode's
    /// bits.
    #[test]
    fn a_minimal_acl_matching_the_mode_passes() {
        let mode = 0o640u32;
        let acl = acl_bytes(&[
            (0x0001, ((mode >> 6) & 7) as u16, u32::MAX),
            (0x0004, ((mode >> 3) & 7) as u16, u32::MAX),
            (0x0020, (mode & 7) as u16, u32::MAX),
        ]);
        assert!(minimal_acl_matches_mode(&acl, mode));
    }

    /// Every shape beyond the exact minimal form is refused: each clause
    /// below fails if its check is deleted.
    #[test]
    fn every_acl_shape_beyond_the_minimal_is_refused() {
        let mode = 0o640u32;
        let user_obj = (0x0001u16, ((mode >> 6) & 7) as u16, u32::MAX);
        let group_obj = (0x0004u16, ((mode >> 3) & 7) as u16, u32::MAX);
        let other = (0x0020u16, (mode & 7) as u16, u32::MAX);
        // A named user (and so a mask, and so a fourth entry).
        let acl = acl_bytes(&[
            user_obj,
            group_obj,
            (0x0002, 4, 1000),
            (0x0010, 4, u32::MAX),
            other,
        ]);
        assert!(!minimal_acl_matches_mode(&acl, mode));
        // The wrong version.
        let mut acl = acl_bytes(&[user_obj, group_obj, other]);
        acl[0] = 3;
        assert!(!minimal_acl_matches_mode(&acl, mode));
        // The wrong length.
        let mut acl = acl_bytes(&[user_obj, group_obj, other]);
        acl.pop();
        assert!(!minimal_acl_matches_mode(&acl, mode));
        // A perm above what the mode grants, per class.
        let acl = acl_bytes(&[user_obj, (0x0004, 7, u32::MAX), other]);
        assert!(!minimal_acl_matches_mode(&acl, mode));
        let acl = acl_bytes(&[user_obj, group_obj, (0x0020, 7, u32::MAX)]);
        assert!(!minimal_acl_matches_mode(&acl, mode));
        let acl = acl_bytes(&[(0x0001, 7, u32::MAX), group_obj, other]);
        assert!(!minimal_acl_matches_mode(&acl, mode));
        // An id that is not the undefined one.
        let acl = acl_bytes(&[(0x0001, ((mode >> 6) & 7) as u16, 0), group_obj, other]);
        assert!(!minimal_acl_matches_mode(&acl, mode));
        // Entries out of order or missing.
        let acl = acl_bytes(&[group_obj, user_obj, other]);
        assert!(!minimal_acl_matches_mode(&acl, mode));
    }

    /// A real ACL on the filesystem is refused, and its removal restores
    /// the read. Skipped where `setfacl` is absent.
    #[cfg(target_os = "linux")]
    #[test]
    fn an_acl_beyond_the_mode_is_refused_for_real() {
        let have_setfacl = std::process::Command::new("setfacl")
            .arg("--version")
            .output()
            .is_ok_and(|out| out.status.success());
        if !have_setfacl {
            eprintln!("setfacl is missing; the real-ACL case is skipped");
            return;
        }
        let (tmp, _me) = tempdir();
        let file = tmp.path().join("shared");
        fs::write(&file, b"bytes").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
        let run = |args: &[&str]| {
            std::process::Command::new("setfacl")
                .args(args)
                .output()
                .unwrap()
        };
        // A named user forces a mask: an ACL beyond the mode's own bits.
        assert!(
            run(["-m", "u:1:r", &file.to_string_lossy()].as_ref())
                .status
                .success()
        );
        let err = read_shared_file(&file).unwrap_err();
        assert!(err.to_string().contains("beyond its mode bits"), "{err}");
        // Removing it restores the read.
        assert!(
            run(["-b", &file.to_string_lossy()].as_ref())
                .status
                .success()
        );
        read_shared_file(&file).unwrap();
        // A default ACL on a directory is refused for every file under it.
        let dir = tmp.path().join("with-default");
        mkdir(&dir, 0o700);
        assert!(
            run(["-d", "-m", "u:1:r", &dir.to_string_lossy()].as_ref())
                .status
                .success()
        );
        let err = check_ancestors(&dir.join("child"), _me).unwrap_err();
        assert!(
            err.to_string().contains("ACL beyond its mode bits"),
            "{err}"
        );
    }

    /// A real extended ACL on macOS is refused, and its removal restores
    /// the read: `chmod +a` grants an entry to `everyone`, `chmod -N`
    /// clears every ACL entry the file carries.
    #[cfg(target_os = "macos")]
    #[test]
    fn an_extended_acl_is_refused_for_real() {
        let (tmp, _me) = tempdir();
        let file = tmp.path().join("shared");
        fs::write(&file, b"bytes").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
        let run = |args: &[&str]| {
            std::process::Command::new("chmod")
                .args(args)
                .output()
                .unwrap()
        };
        assert!(
            run(["+a", "everyone allow read", &file.to_string_lossy()].as_ref())
                .status
                .success()
        );
        let err = read_shared_file(&file).unwrap_err();
        assert!(
            err.to_string().contains("ACL beyond its mode bits"),
            "{err}"
        );
        assert!(
            run(["-N", &file.to_string_lossy()].as_ref())
                .status
                .success()
        );
        read_shared_file(&file).unwrap();
    }

    /// The workspace denies the unsafe-code lint everywhere but this
    /// crate, and this crate keeps it in the one private `ffi` module:
    /// walk the crate's sources and refuse the lint's opt-out, or a bare
    /// token, anywhere else. The test fails the moment the boundary
    /// breaks.
    #[test]
    fn unsafe_code_lives_only_in_the_ffi_module() {
        fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, files);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    files.push(path);
                }
            }
        }
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        walk(&src, &mut files);
        // Built in pieces: the needle must not appear in this test's own
        // source, or the test would refuse itself.
        let opt_out = ["allow(", "unsafe", "_code", ")"].concat();
        let ffi = src.join("runtime/auth/ffi.rs");
        let mut saw_ffi = false;
        for file in files {
            let text = fs::read_to_string(&file).unwrap();
            let code = text.replace("unsafe_code", "");
            if file == ffi {
                saw_ffi = true;
                assert!(
                    text.contains(&opt_out),
                    "the ffi module must opt out of the lint explicitly"
                );
            } else {
                let code = code.replace("`unsafe`", "");
                // The needles are built in pieces, or this test's own
                // source would hold the tokens it refuses.
                let (sp, brace) = ("unsafe\u{20}", "unsafe\u{7b}");
                assert!(
                    !code.contains(sp) && !code.contains(brace),
                    "{} holds a token outside the ffi module",
                    file.display()
                );
                assert!(
                    !text.contains(&opt_out),
                    "{} opts out of the unsafe-code lint",
                    file.display()
                );
            }
        }
        assert!(saw_ffi, "the ffi module went missing");
    }

    #[test]
    fn symlinks_are_resolved_before_the_check() {
        let (tmp, me) = tempdir();
        let real = fs::canonicalize(tmp.path()).unwrap();
        let open = tmp.path().join("open");
        mkdir(&open, 0o777);
        mkdir(&tmp.path().join("private"), 0o700);
        // A link in a directory that others can write. The path that comes
        // back does not go through it, so a swap of the link changes nothing.
        symlink(tmp.path().join("private"), open.join("link")).unwrap();
        let resolved = check_ancestors(&open.join("link/daemon.sock"), me).unwrap();
        assert_eq!(resolved, real.join("private/daemon.sock"));
        // A link to a directory that others can write: that directory is
        // checked.
        symlink(&open, tmp.path().join("to-open")).unwrap();
        let err = check_ancestors(&tmp.path().join("to-open/daemon.sock"), me).unwrap_err();
        assert!(err.to_string().contains("/open can be written"), "{err}");
    }

    /// The raw fstat mode of a regular file with permission bits `bits`.
    fn file_mode(bits: u32) -> u32 {
        0o100_000 | bits
    }

    #[test]
    fn a_file_owned_by_this_account_or_root_passes_the_descriptor_checks() {
        ensure_file_checks(file_mode(0o600), 501, 501, true, Path::new("key")).unwrap();
        ensure_file_checks(file_mode(0o644), 501, 501, false, Path::new("key")).unwrap();
    }

    /// A file owned by root passes: a service manager that holds the keys
    /// at root-owned files is normal.
    #[test]
    fn a_root_owned_file_passes_the_descriptor_checks() {
        ensure_file_checks(file_mode(0o600), 0, 501, true, Path::new("key")).unwrap();
        ensure_file_checks(file_mode(0o644), 0, 501, false, Path::new("key")).unwrap();
    }

    /// The owner check is what keeps a secret another account holds -
    /// and can replace at will - out, whatever its mode.
    #[test]
    fn a_file_owned_by_another_account_is_refused() {
        let err = ensure_file_checks(file_mode(0o644), 502, 501, false, Path::new("key"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("owned by another user"), "{err}");
    }

    #[test]
    fn a_file_that_is_not_a_file_is_refused() {
        let err = ensure_file_checks(0o040_700, 501, 501, true, Path::new("key"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("is not a file"), "{err}");
    }

    #[test]
    fn a_group_readable_secret_is_refused_and_a_group_writable_shared_file_is_refused() {
        let err = ensure_file_checks(file_mode(0o640), 501, 501, true, Path::new("key"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("read by group or others"), "{err}");
        let err = ensure_file_checks(file_mode(0o664), 501, 501, false, Path::new("key"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("written by group or others"), "{err}");
        // A 0644 shared file passes: the others may read what is public.
        ensure_file_checks(file_mode(0o644), 501, 501, false, Path::new("key")).unwrap();
    }

    /// The whole path of [`read_secret_file`]: a private file reads back
    /// through the checked descriptor, and a link at the end dies at the
    /// open even when its target has a perfect mode and owner.
    #[test]
    fn a_secret_reads_through_the_descriptor_and_a_link_dies_at_the_open() {
        let (tmp, me) = tempdir();
        let secret = tmp.path().join("s");
        fs::write(&secret, b"bytes").unwrap();
        fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_secret_file(&secret).unwrap(), b"bytes");
        let link = tmp.path().join("link");
        symlink(&secret, &link).unwrap();
        let err = read_secret_file(&link).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied, "{err}");
        assert!(err.to_string().contains("symlink"), "{err}");
        let _ = me;
    }
}
