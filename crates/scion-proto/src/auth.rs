//! Bearer tokens: the check on every RPC ([`Auth`]), shared by the server and
//! the cache tiers, and the client side that sends the token ([`Bearer`]).
//! Also the check that no other user can swap a local path on its way
//! ([`check_ancestors`]), shared by `scn` and the daemon, and the way every
//! process makes its own directories private ([`create_private_dir_all`]).

use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

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
/// refused, and only the owner and the mode bits are read, not ACLs.
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
        } else if meta.mode() & 0o022 != 0 && meta.mode() & 0o1000 == 0 {
            let why = "can be written by group or others, and has no sticky bit".to_owned();
            (why, format!(" (fix: chmod go-w {})", dir.display()))
        } else {
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
}
