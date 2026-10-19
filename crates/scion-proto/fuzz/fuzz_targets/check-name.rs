//! Client names (repos, refs) pass check::name before they reach the
//! database or a log: accepted names are 1 to 256 bytes and hold no
//! control characters. Any input ends in an error or a checked name,
//! never a panic.

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_proto::runtime::check;

fuzz_target!(|data: &[u8]| {
    // The check runs on what gRPC metadata already made UTF-8, so the
    // target parses the same way and lets the rest go.
    let Ok(name) = std::str::from_utf8(data) else {
        return;
    };
    match check::name("ref", name) {
        Ok(()) => {
            assert!(!name.is_empty() && name.len() <= check::MAX_NAME_BYTES);
            assert!(!name.chars().any(char::is_control));
        }
        Err(_) => {
            let bad = name.is_empty()
                || name.len() > check::MAX_NAME_BYTES
                || name.chars().any(char::is_control);
            assert!(bad, "check::name refused {name:?} for no reason");
        }
    }
});
