#![forbid(unsafe_code)]
//! The archive section readers, one at a time, fed raw bytes: any bytes
//! end in a verdict (the rows) or a refusal, never a panic. Each fuzzed
//! input names its reader with the first byte and carries that file's
//! bytes; the reader sees exactly those bytes, as a crafted archive's
//! section file would hold.

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_server::test_support::archive as read;

/// Which section reader the rest of the input goes to.
#[derive(Clone, Copy)]
enum Section {
    Changesets,
    Refs,
    Lands,
    Members,
    Provenance,
    Manifest,
}

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().expect("a temp archive");
    let archive = dir.path().join("scion-archive-1");
    std::fs::create_dir_all(&archive).expect("the archive directory");
    let (section, body) = match data.first() {
        Some(0) => (Section::Changesets, &data[1..]),
        Some(1) => (Section::Refs, &data[1..]),
        Some(2) => (Section::Lands, &data[1..]),
        Some(3) => (Section::Members, &data[1..]),
        Some(4) => (Section::Provenance, &data[1..]),
        Some(5) => (Section::Manifest, &data[1..]),
        _ => (Section::Changesets, data),
    };
    let verdict = match section {
        Section::Changesets => {
            let path = archive.join("changesets.bin");
            std::fs::write(&path, body).expect("the section file");
            read::read_changesets(&path)
                .map(|rows| assert!(rows.len() <= body.len(), "more rows than frames"))
        }
        Section::Refs => {
            let path = archive.join("refs.json");
            std::fs::write(&path, body).expect("the section file");
            read::read_refs(&path).map(|rows| {
                for (name, _id, _gen) in &rows {
                    assert!(!name.is_empty(), "an accepted ref name is not empty");
                }
            })
        }
        Section::Lands => {
            let path = archive.join("lands.json");
            std::fs::write(&path, body).expect("the section file");
            read::read_lands(&path).map(drop)
        }
        Section::Members => {
            let path = archive.join("members.txt");
            std::fs::write(&path, body).expect("the section file");
            read::read_members(&path).map(drop)
        }
        Section::Provenance => {
            let path = archive.join("provenance.json");
            std::fs::write(&path, body).expect("the section file");
            read::read_provenance(&path).map(drop)
        }
        Section::Manifest => {
            std::fs::write(archive.join("manifest.json"), body).expect("the manifest");
            read::read_manifest(&archive).map(drop)
        }
    };
    // The verdict itself is the reader's business (verify judges the
    // whole archive, and archive-verify fuzzes that door). Here the
    // contract is only that the reader comes back at all.
    let _ = verdict;
});
