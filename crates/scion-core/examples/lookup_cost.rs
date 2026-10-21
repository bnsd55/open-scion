//! The measurement behind the tree-lookup change: find one entry in the
//! encoded `TreeV1` bytes of a 100,000-entry folder, two ways.
//!
//! `old` is what `tree::lookup` did before the change: decode the whole
//! node into the entry map, clone the map, remove the name. `new` is
//! the scan: one walk of the encoded bytes, no map, no clone.
//!
//! Run each side in its own process, so an outer `time -l` can read the
//! peak allocation of the one path alone:
//!
//! ```text
//! /usr/bin/time -l cargo run --release -p open-scion-core --example lookup_cost -- old
//! /usr/bin/time -l cargo run --release -p open-scion-core --example lookup_cost -- new
//! ```

use std::collections::BTreeMap;
use std::time::Instant;

use scion_core::{BlobId, Entry, FileEntry, FileState, PathElem, Tree};

fn main() {
    let what = std::env::args().nth(1).unwrap_or_else(|| "new".into());
    let entries: BTreeMap<PathElem, Entry> = (0..100_000)
        .map(|i| {
            (
                PathElem::new(format!("f{i:05}")).unwrap(),
                Entry::File(FileEntry::new(
                    FileState::Regular {
                        content: BlobId::of(b"x"),
                        executable: false,
                    },
                    1,
                )),
            )
        })
        .collect();
    let tree = Tree::new(entries).unwrap();
    println!("entries: {}", tree.entries().len());
    let body = tree.encode();
    println!(
        "a 100,000-entry folder: the encoded body is {} bytes",
        body.len()
    );

    // The name that is there, one that sorts between the two last
    // entries (the scan stops near the end), and one past every name.
    let hit = b"f50000";
    let near_end_miss = b"f90000a";
    let miss = b"g999999";

    // Warm every cache both paths touch.
    if what == "old" {
        assert!(
            Tree::decode(&body)
                .unwrap()
                .entries()
                .clone()
                .remove(&PathElem::new("f50000").unwrap())
                .is_some()
        );
    } else {
        assert!(Tree::find_entry(&body, hit).unwrap().is_some());
    }

    let rounds = 20;
    let start = Instant::now();
    for _ in 0..rounds {
        if what == "old" {
            assert!(
                Tree::decode(&body)
                    .unwrap()
                    .entries()
                    .clone()
                    .remove(&PathElem::new("f50000").unwrap())
                    .is_some()
            );
        } else {
            assert!(Tree::find_entry(&body, hit).unwrap().is_some());
        }
    }
    let hit_us = start.elapsed().as_micros() / rounds;

    let start = Instant::now();
    for _ in 0..rounds {
        if what == "old" {
            assert!(
                Tree::decode(&body)
                    .unwrap()
                    .entries()
                    .clone()
                    .remove(&PathElem::new("g999999".to_string()).unwrap())
                    .is_none()
            );
        } else {
            assert!(Tree::find_entry(&body, miss).unwrap().is_none());
        }
    }
    let miss_us = start.elapsed().as_micros() / rounds;

    let start = Instant::now();
    for _ in 0..rounds {
        if what == "old" {
            assert!(
                Tree::decode(&body)
                    .unwrap()
                    .entries()
                    .clone()
                    .remove(&PathElem::new("f90000a".to_string()).unwrap())
                    .is_none()
            );
        } else {
            assert!(Tree::find_entry(&body, near_end_miss).unwrap().is_none());
        }
    }
    let near_end_us = start.elapsed().as_micros() / rounds;

    println!(
        "{what}: a hit costs {hit_us} us, a near-end miss {near_end_us} us, a miss past the last name {miss_us} us, per lookup"
    );
}
