//! Bearer tokens: the check on every RPC ([`Auth`]), shared by the server and
//! the cache tiers, and the client side that sends the token ([`Bearer`]).
//! Also the check that no other user can swap a local path on its way
//! ([`check_ancestors`]), shared by `scn` and the daemon.

use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Who made a request. Added to request extensions by [`Auth`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Caller(pub String);

/// Accepts requests that carry `authorization: Bearer <token>`.
// SHORTCUT: one static token, one principal ("default"). Upgrade: a token to
// principal map, then per-path write rules.
#[derive(Clone)]
pub struct Auth {
    /// Hash of the expected token, so every comparison has the same length.
    token: Arc<[u8; 32]>,
}

impl Auth {
    /// Accepts this token.
    #[must_use]
    pub fn new(token: &str) -> Self {
        Self {
            token: Arc::new(digest(token.as_bytes())),
        }
    }
}

impl Interceptor for Auth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let presented = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.as_bytes().strip_prefix(b"Bearer "));
        if !presented.is_some_and(|token| constant_time_eq(&digest(token), &self.token)) {
            return Err(Status::unauthenticated("missing or wrong token"));
        }
        request.extensions_mut().insert(Caller("default".into()));
        Ok(request)
    }
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
/// Returns `path` with its directory resolved. Use it, not `path`: a symlink
/// on the way is not followed again, so a symlink in a directory that others
/// can change is harmless. The last part of `path` is not resolved or
/// checked: that is the caller's check.
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
        let why = if meta.uid() != 0 && meta.uid() != me {
            format!("is owned by another user (uid {})", meta.uid())
        } else if meta.mode() & 0o022 != 0 && meta.mode() & 0o1000 == 0 {
            "can be written by group or others, and has no sticky bit".to_owned()
        } else {
            continue;
        };
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} {why}, so another user could swap {}",
                dir.display(),
                path.display()
            ),
        ));
    }
    Ok(parent.join(name))
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
