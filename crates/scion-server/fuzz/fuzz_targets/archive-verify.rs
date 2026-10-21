#![forbid(unsafe_code)]
//! The archive readers take bytes that an operator or a backup job wrote
//! outside the server: any bytes end in a verdict (sound, damages, or a
//! refusal) or in a restore that refuses, never a panic. The fuzzed bytes
//! land, one sink at a time, in the files of a valid-shaped archive: the
//! changeset stream, the member list, and each JSON file.

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_server::archive;

/// Which file of the archive the fuzzed bytes go into.
#[derive(Clone, Copy)]
enum Sink {
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
    let repo = archive.join("repos").join("r");
    std::fs::create_dir_all(&repo).expect("the repo directory");
    let (sink, rest) = match data.first() {
        Some(0) => (Sink::Changesets, &data[1..]),
        Some(1) => (Sink::Refs, &data[1..]),
        Some(2) => (Sink::Lands, &data[1..]),
        Some(3) => (Sink::Members, &data[1..]),
        Some(4) => (Sink::Provenance, &data[1..]),
        Some(5) => (Sink::Manifest, &data[1..]),
        _ => (Sink::Changesets, data),
    };
    let empty_array = "[]";
    let files: [(&str, &[u8]); 5] = [
        ("changesets.bin", &[]),
        ("refs.json", empty_array.as_bytes()),
        ("lands.json", empty_array.as_bytes()),
        ("members.txt", b""),
        ("provenance.json", empty_array.as_bytes()),
    ];
    for (name, body) in files {
        let body = if sink_file(sink) == name { rest } else { body };
        std::fs::write(repo.join(name), body).expect("a file of the archive");
    }
    let manifest = match sink {
        Sink::Manifest => String::from_utf8_lossy(rest).into_owned(),
        _ => r#"{"format":"scion-archive","version":1,"kind":"repo",
                 "repos":[{"name":"r","changesets":0}]}"#.to_owned(),
    };
    std::fs::write(archive.join("manifest.json"), manifest).expect("the manifest");

    // verify answers damages or refuses; it must never panic. A damaged
    // archive must refuse a restore: run it when the data dir stays empty.
    if let Ok(damages) = archive::verify(&archive) {
        let target = tempfile::tempdir().expect("a temp data dir");
        match archive::restore(&archive, target.path(), None, None) {
            Ok(_) => assert!(
                damages.is_empty(),
                "a restore ran on an archive verify names: {damages:?}"
            ),
            Err(_) => {}
        }
    }
});

fn sink_file(sink: Sink) -> &'static str {
    match sink {
        Sink::Changesets => "changesets.bin",
        Sink::Refs => "refs.json",
        Sink::Lands => "lands.json",
        Sink::Members => "members.txt",
        Sink::Provenance => "provenance.json",
        Sink::Manifest => "manifest.json",
    }
}
