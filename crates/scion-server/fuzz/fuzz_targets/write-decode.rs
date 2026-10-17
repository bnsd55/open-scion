//! write::decode is the server's door for changesets a client sent. Any
//! input must end in an error, or in a changeset whose encoding is exactly
//! the input (the decoder is strict, so it accepts only canonical bytes),
//! whose ID is its own hash, and whose change count is at most
//! MAX_CHANGES. Never a panic.

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_server::write;

fuzz_target!(|data: &[u8]| {
    if let Ok((cs, id)) = write::decode(data) {
        let (bytes, again) = cs.encode_with_id();
        assert_eq!(bytes, data);
        assert_eq!(again, id);
        assert!(cs.changes.len() <= write::MAX_CHANGES);
    }
});
