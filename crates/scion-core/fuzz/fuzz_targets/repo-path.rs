//! A repo path is raw bytes a client sent: `RepoPath::new` takes any bytes
//! and ends in a path or an error, never a panic. A path it accepts keeps
//! its bytes exactly, and one of its parts round-trips as a `PathElem`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_core::{PathElem, RepoPath};

fuzz_target!(|data: &[u8]| {
    if let Ok(path) = RepoPath::new(data) {
        assert_eq!(path.as_bytes(), data);
        for part in data.split(|&b| b == b'/') {
            let elem = PathElem::new(part).expect("a part of a valid path");
            assert_eq!(elem.as_bytes(), part);
        }
    }
});
