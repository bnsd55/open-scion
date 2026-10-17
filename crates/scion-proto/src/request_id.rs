//! The request ID: one per client operation, carried through every hop.
//!
//! `scn` makes one per command and sends it on every call. A tier forwards
//! the one its caller sent. Every served call records it as the
//! `request_id` field of its `call` span, so one checkout can be followed
//! across scn, daemon, cache and server logs.
//!
//! The header is [`REQUEST_ID`]. A value is 1 to 64 bytes of visible ASCII
//! without a space. A header that is longer, or not visible ASCII, is
//! dropped, not cleaned up: nothing of it is logged.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The metadata key of the request ID.
pub const REQUEST_ID: &str = "x-request-id";

/// The longest request ID that is kept.
const MAX_BYTES: usize = 64;

/// Counts the IDs this process made, so two made in the same nanosecond
/// still differ.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// The request ID of an inbound header, cleaned: `None` if the call sent
/// none, or one that is not 1 to 64 bytes of visible ASCII without a
/// space. What is refused never reaches a log.
#[must_use]
pub fn sanitize(bytes: Option<&[u8]>) -> Option<String> {
    let bytes = bytes?;
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return None;
    }
    if !bytes.iter().all(|b| (0x21..=0x7e).contains(b)) {
        return None;
    }
    // The check above leaves only ASCII, so this cannot fail.
    Some(String::from_utf8_lossy(bytes).into_owned())
}

/// A request ID: 16 lowercase hex digits from the time and a counter.
#[must_use]
pub fn generate() -> String {
    // The nanos fit u64 until the year 2554.
    #[allow(clippy::cast_possible_truncation)]
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "{:016x}",
        nanos ^ counter.wrapping_mul(0x9e37_79b9_7f4a_7c15)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_header_that_fits_is_kept() {
        assert_eq!(sanitize(Some(b"abc-123")), Some("abc-123".to_owned()));
        // 64 bytes, the limit exactly.
        let full = vec![b'x'; 64];
        assert_eq!(
            sanitize(Some(&full)),
            Some(String::from_utf8(full).unwrap())
        );
    }

    #[test]
    fn junk_is_dropped_whole() {
        // Too long: only its refusal is interesting, so none of it is kept.
        assert_eq!(sanitize(Some(&[b'y'; 65][..])), None);
        // Not visible ASCII: a space, a control byte, UTF-8.
        assert_eq!(sanitize(Some(b"two words")), None);
        assert_eq!(sanitize(Some(b"a\tb")), None);
        assert_eq!(sanitize(Some(&[0x63, 0x61, 0x66, 0xe9][..])), None);
        // Empty and missing.
        assert_eq!(sanitize(Some(b"")), None);
        assert_eq!(sanitize(None), None);
    }

    #[test]
    fn a_generated_id_is_hex_and_short() {
        let a = generate();
        let b = generate();
        assert_eq!(a.len(), 16);
        assert!(
            a.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
        assert_ne!(a, b, "two IDs made one after the other differ");
    }
}
