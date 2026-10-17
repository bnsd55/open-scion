//! Every input the ticket parser accepts must leave it calm, and a ticket
//! that the parser accepts round-trips: re-signing the fields it read
//! gives back the same bytes (Ed25519 is deterministic).

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_proto::ticket::{self};

/// One key for the whole run: its seed is fixed, so the round-trip is
/// deterministic. A real server rotates keys; one key exercises the same
/// code paths.
fn key() -> ed25519_dalek::SigningKey {
    ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
}

fuzz_target!(|data: &[u8]| {
    // The clock the target lives by: fixed, so a ticket the target signs
    // is valid whenever the target parses it.
    const NOW: u64 = 1_700_000_000;

    // The raw input goes to both parsers: too short, another version,
    // trailing bytes, a clock window that is off, a bad signature, all
    // must end in an error, never a panic.
    let _ = ticket::parse_unverified(data, NOW);
    let _ = ticket::parse(data, &[key().verifying_key()], NOW);

    // A ticket the target signs from bytes of the input parses, and
    // re-signing what the parser read gives back the same bytes.
    let tree = root_tree(data);
    for repo in names(data, 8) {
        for principal in names(data, 4) {
            if let Ok(signed) = ticket::sign(&key(), &repo, &principal, tree, NOW) {
                let parsed = ticket::parse(&signed, &[key().verifying_key()], NOW)
                    .expect("a ticket the target signed parses");
                // The fields say what the signed bytes said, byte for byte:
                // Ed25519 signs deterministically, so re-signing what the
                // parser read must give back the input.
                assert_eq!(
                    ticket::sign(&key(), &parsed.repo, &parsed.principal, parsed.root_tree, NOW)
                        .expect("the parsed names re-sign"),
                    signed
                );
                let _ = parsed.expires;
            }
        }
    }
});

/// The first 32 bytes of the input, as the ticket's root tree.
fn root_tree(data: &[u8]) -> [u8; 32] {
    let mut tree = [0u8; 32];
    let n = data.len().min(32);
    tree[..n].copy_from_slice(&data[..n]);
    tree
}

/// Names for the ticket's repo and principal: up to `most` pieces of the
/// input as UTF-8, and the fixed fallback that is always valid.
fn names(data: &[u8], most: usize) -> Vec<String> {
    let mut names = vec!["r".to_owned()];
    if let Ok(text) = std::str::from_utf8(data) {
        for piece in text.split_whitespace().take(most) {
            if scion_proto::check::name("name", piece).is_ok() {
                names.push(piece.to_owned());
            }
        }
    }
    names
}
