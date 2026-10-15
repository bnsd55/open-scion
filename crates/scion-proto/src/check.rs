//! Checks on request fields. The server and every tier make them the same
//! way, so a tier refuses what the server refuses.

use tonic::Status;

use crate::MAX_OBJECT_REFS;

/// Longest accepted name (repo, ref, request key), in bytes.
pub const MAX_NAME_BYTES: usize = 256;

/// Checks a client-supplied name: 1 to [`MAX_NAME_BYTES`] bytes and no
/// control characters, so names are safe to log and to print.
///
/// # Errors
///
/// Why the name breaks a rule, naming `what`.
pub fn name(what: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_NAME_BYTES {
        return Err(format!("{what} must be 1 to {MAX_NAME_BYTES} bytes"));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{what} must not contain control characters"));
    }
    Ok(())
}

/// Parses 32 raw ID bytes from a request.
///
/// # Errors
///
/// `INVALID_ARGUMENT` naming `what` if there are not exactly 32 bytes.
pub fn id<T: for<'a> TryFrom<&'a [u8]>>(bytes: &[u8], what: &str) -> Result<T, Status> {
    T::try_from(bytes).map_err(|_| Status::invalid_argument(format!("{what} must be 32 bytes")))
}

/// Checks how many objects a `GetObjects` call names.
///
/// # Errors
///
/// `INVALID_ARGUMENT` unless `count` is 1 to [`MAX_OBJECT_REFS`].
pub fn object_count(count: usize) -> Result<(), Status> {
    if count == 0 || count > MAX_OBJECT_REFS {
        return Err(Status::invalid_argument(format!(
            "a call names 1 to {MAX_OBJECT_REFS} objects"
        )));
    }
    Ok(())
}
