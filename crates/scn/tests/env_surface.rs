//! The env vars the shipped binaries read, as the machine form of
//! stability.md's 1.x promise for production configuration: the list is
//! additive-only in 1.x. The golden names the vars 1.x keeps; the check
//! reads the source of `scn` and `scion-server`, so neither removing a
//! var from the code nor editing a doc can make this pass. Adding a var
//! passes once it is added to the golden consciously — a var is a
//! production surface, so it enters the golden on purpose.
//!
//! The scan sees every way a binary reads a var: clap's `env = "X"`
//! attribute, a `std::env::var("X")` or `var_os("X")` call, and a var
//! name handed to a helper as a string. A var that moves from one
//! shape to another (as #402 moved the secrets out of clap) stays
//! seen; only deleting the read turns the check red.

const GOLDEN: &[&str] = &[
    // `scn` reads the SCION_* ones (clap `env = ...`); `scion-server`
    // its own; the server's log filter is `RUST_LOG`, which
    // `tracing_subscriber` reads by convention, not by literal.
    "SCION_SERVER",
    "SCION_TOKEN",
    "SCION_OBJECTS",
    "SCION_TLS_CA",
    "SCION_AUTHOR",
    "SCION_REPO",
    // `scion-server` reads these: `SCION_DATA` by clap, the token and
    // URL by hand (`std::env::var`), the URL file by clap (a `_FILE`
    // var names a file that holds the secret, so the secret itself
    // never sits in argv or env).
    "SCION_DATA",
    "SCION_POSTGRES_URL",
    "SCION_POSTGRES_URL_FILE",
    // `scion-cache` and `scion-daemon` read these: the daemon's data
    // dir by clap, the two tokens through `secret_of`, whose env name
    // is a string argument (`std::env::var(name)` inside).
    "SCION_DAEMON_DATA",
    "SCION_UPSTREAM_TOKEN",
];

#[test]
fn the_env_vars_the_golden_names_still_exist() {
    let missing: Vec<&str> = GOLDEN
        .iter()
        .copied()
        .filter(|var| {
            !sources()
                .iter()
                .any(|text| read_var_names(text).iter().any(|read| read == *var))
        })
        .collect();
    assert!(
        missing.is_empty(),
        "the 1.x production configuration shrank; a script or service that \
         set these vars would break. Restore them, or drop them in a major \
         release with an upgrade note: {missing:?}"
    );
}

/// Every `SCION_*` literal the binaries read, by any read shape, must
/// sit in the golden: a new var cannot enter quietly. The `SCION_TEST_*`
/// vars are test hooks (never a promise) and stay out.
#[test]
fn every_env_var_the_binaries_read_is_in_the_golden() {
    let undeclared: BTreeSet<String> = sources()
        .iter()
        .flat_map(|text| read_var_names(text))
        .filter(|name| name.starts_with("SCION_") && !name.starts_with("SCION_TEST_"))
        .filter(|name| !GOLDEN.contains(&name.as_str()))
        .collect();
    assert!(
        undeclared.is_empty(),
        "the binaries read env vars the 1.x golden does not name; add them to \
         the golden consciously: {undeclared:?}"
    );
}

/// The env var names a source file reads, by every shape the code
/// uses: clap's `env = "X"` attribute; the hand reads
/// `std::env::var("X")` / `std::env::var_os("X")`; and a name handed
/// to a helper as a string argument — a `secret_of(..., "SCION_X")`
/// call reads the var by the name it was given, so the call's string
/// arguments count as reads too (the scan follows the call to its
/// closing paren, the calls are multi-line).
fn read_var_names(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    for needle in ["env = \"", "env::var(\"", "env::var_os(\""] {
        let mut from = 0;
        while let Some(at) = text[from..].find(needle) {
            let rest = &text[from + at + needle.len()..];
            match rest.find('"') {
                Some(end) => names.push(rest[..end].to_owned()),
                None => break,
            }
            from += at + needle.len();
        }
    }
    let mut from = 0;
    while let Some(at) = text[from..].find("secret_of(") {
        let call = &text[from + at + "secret_of(".len()..];
        let mut depth = 1usize;
        let mut open: Option<usize> = None;
        for (i, c) in call.char_indices() {
            match c {
                '"' => match open {
                    None => open = Some(i + 1),
                    Some(start) => {
                        names.push(call[start..i].to_owned());
                        open = None;
                    }
                },
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
        }
        from += at + "secret_of(".len();
    }
    // The `secret_of` shape grabs every quoted argument of the call,
    // the flags included (`--token`); keep the env-name shape only:
    // all caps, digits and underscores.
    names.retain(|name| {
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit())
    });
    names
}

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Every `.rs` source file of the four shipped binaries: `scn`,
/// `scion-server`, and the tier crate that builds `scion-cache` and
/// `scion-daemon`.
fn sources() -> Vec<String> {
    let mut texts = Vec::new();
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for crate_dir in ["scn", "scion-server", "scion-tier"] {
        walk(&manifest.join("..").join(crate_dir).join("src"), &mut texts);
    }
    texts
}

fn walk(dir: &Path, texts: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk(&path, texts);
        } else if path.extension().is_some_and(|e| e == "rs") {
            texts.push(std::fs::read_to_string(&path).unwrap_or_default());
        }
    }
}
