//! A request ID header comes from the client and lands in logs. sanitize
//! must hand out only 1 to 64 bytes of visible ASCII, and cleaning a
//! cleaned ID gives the same ID.

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_proto::request_id;

// sanitize's own cap, restated here: a logged ID is at most 64 bytes.
const MAX_BYTES: usize = 64;

fuzz_target!(|data: &[u8]| {
    match request_id::sanitize(Some(data)) {
        Some(id) => {
            // Safe to log: visible ASCII only, within the cap, and the
            // bytes the client sent, changed by nothing.
            assert!(!id.is_empty() && id.len() <= MAX_BYTES);
            assert!(id.bytes().all(|b| (0x21..=0x7e).contains(&b)));
            assert_eq!(id.as_bytes(), data);
            // Cleaning is a fixed point: sanitize(sanitize(x)) is
            // sanitize(x).
            assert_eq!(request_id::sanitize(Some(id.as_bytes())), Some(id));
        }
        None => {
            let bad = data.is_empty()
                || data.len() > MAX_BYTES
                || !data.iter().all(|b| (0x21..=0x7e).contains(b));
            assert!(bad, "sanitize refused {:?} for no reason", data);
        }
    }
    // A missing header stays missing.
    assert_eq!(request_id::sanitize(None), None);
});
