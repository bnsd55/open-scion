//! The env vars the shipped binaries read, as the machine form of
//! stability.md's 1.x promise for production configuration: the list is
//! additive-only in 1.x. The golden names the vars 1.x keeps; the check
//! reads the source of `scn` and `scion-server`, so neither removing a
//! var from the code nor editing a doc can make this pass. Adding a var
//! passes once it is added to the golden consciously — a var is a
//! production surface, so it enters the golden on purpose.

const GOLDEN: &[&str] = &[
    // `scn` reads the SCION_* ones (clap `env = ...`), `scion-server`
    // its own two; the server's log filter is `RUST_LOG`, which
    // `tracing_subscriber` reads by convention, not by literal.
    "SCION_SERVER",
    "SCION_TOKEN",
    "SCION_OBJECTS",
    "SCION_TLS_CA",
    "SCION_AUTHOR",
    "SCION_REPO",
    // `scion-server` reads these (clap `env = ...`):
    "SCION_DATA",
    "SCION_POSTGRES_URL",
];

#[test]
fn the_env_vars_the_golden_names_still_exist() {
    let source = sources();
    let missing: Vec<&str> = GOLDEN
        .iter()
        .copied()
        .filter(|var| {
            !source
                .iter()
                .any(|text| text.contains(&format!("env = \"{var}\"")))
        })
        .collect();
    assert!(
        missing.is_empty(),
        "the 1.x production configuration shrank; a script or service that \
         set these vars would break. Restore them, or drop them in a major \
         release with an upgrade note: {missing:?}"
    );
}

/// Every `SCION_*` literal the binaries read must sit in the golden: a
/// new var cannot enter quietly. The `SCION_TEST_*` vars are test
/// hooks (never a promise) and stay out.
#[test]
fn every_env_var_the_binaries_read_is_in_the_golden() {
    let undeclared: BTreeSet<String> = sources()
        .iter()
        .flat_map(|text| {
            text.match_indices("env = \"")
                .map(move |(i, _)| {
                    let rest = &text[i + 7..];
                    rest.find('"').map(|end| rest[..end].to_owned())
                })
                .collect::<Vec<_>>()
        })
        .flatten()
        .filter(|name| name.starts_with("SCION_") && !name.starts_with("SCION_TEST_"))
        .filter(|name| !GOLDEN.contains(&name.as_str()))
        .collect();
    assert!(
        undeclared.is_empty(),
        "the binaries read env vars the 1.x golden does not name; add them to \
         the golden consciously: {undeclared:?}"
    );
}

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Every `.rs` source file of `scn` and `scion-server`: the shipped
/// binaries whose env reads are the promise.
fn sources() -> Vec<String> {
    let mut texts = Vec::new();
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for crate_dir in ["scn", "scion-server"] {
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
