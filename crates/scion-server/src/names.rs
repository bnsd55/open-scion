//! Limits on names that clients send: repos, refs and request keys.

use crate::error::{Error, Result};

/// Checks a client-supplied name: 1 to
/// [`MAX_NAME_BYTES`](scion_proto::runtime::check::MAX_NAME_BYTES) bytes and
/// no control characters, so names are safe to log and to print. The tiers
/// use the same rule ([`scion_proto::runtime::check::name`]).
///
/// # Errors
///
/// [`Error::InvalidArgument`] naming `what` if the name breaks a rule.
pub(crate) fn check(what: &str, value: &str) -> Result<()> {
    scion_proto::runtime::check::name(what, value).map_err(Error::InvalidArgument)
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_proto::runtime::check::MAX_NAME_BYTES;

    #[test]
    fn accepts_ordinary_names_and_rejects_the_rest() {
        for ok in [
            "main",
            "release/2026.10",
            "bench:abc",
            &"x".repeat(MAX_NAME_BYTES),
        ] {
            assert!(check("ref", ok).is_ok(), "{ok:?}");
        }
        for bad in [
            "",
            "a\nb",
            "\u{1b}[31m",
            "tab\there",
            &"x".repeat(MAX_NAME_BYTES + 1),
        ] {
            assert!(check("ref", bad).is_err(), "{bad:?}");
        }
    }
}
