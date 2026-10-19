//! The access file parser takes bytes that an operator wrote: any input
//! ends in a parsed file or an error message, never a panic. The format
//! has no encoder, so there is no round-trip to check.

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_server::Access;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = Access::parse(text);
    }
});
