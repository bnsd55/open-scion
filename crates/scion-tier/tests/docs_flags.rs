//! Every `--long` flag of `scion-cache` and `scion-daemon` is named in
//! docs/operations.md. A flag that a tier binary grows and the guide
//! does not name fails here, so the guide cannot drift from `--help`.

use std::path::Path;
use std::process::Command;

/// The `--long` flags a help text names, in prose or in its option list.
fn help_flags(text: &str) -> Vec<String> {
    let mut flags: Vec<String> = text
        .split(|c: char| c.is_whitespace() || c == '`')
        .filter_map(|word| word.strip_prefix("--"))
        .filter(|rest| {
            rest.chars().next().is_some_and(|c| c.is_ascii_lowercase())
                && rest
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c == '-' || c.is_ascii_digit())
        })
        .map(|rest| format!("--{rest}"))
        .filter(|flag| flag != "--help" && flag != "--version")
        .collect();
    flags.sort();
    flags.dedup();
    flags
}

fn doc_text() -> String {
    let doc = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/operations.md");
    std::fs::read_to_string(&doc).expect("docs/operations.md opens")
}

fn tier_flags(binary: &str) -> Vec<String> {
    let exe = if binary == "scion-cache" {
        env!("CARGO_BIN_EXE_scion-cache")
    } else {
        env!("CARGO_BIN_EXE_scion-daemon")
    };
    let out = Command::new(exe)
        .arg("--help")
        .output()
        .unwrap_or_else(|_| panic!("{binary} runs"));
    assert!(out.status.success(), "{binary} --help failed");
    let text = String::from_utf8(out.stdout).expect("help is utf-8");
    let flags = help_flags(&text);
    // The extractor works: a tier names its upstream, so an empty list
    // means the parse broke, not that no flags exist.
    assert!(
        flags.contains(&"--upstream".to_string()),
        "the flag parse of {binary} found nothing: {flags:?}"
    );
    flags
}

#[test]
fn every_tier_flag_is_in_the_operations_guide() {
    let doc = doc_text();
    for binary in ["scion-cache", "scion-daemon"] {
        for flag in tier_flags(binary) {
            assert!(
                doc.contains(&flag),
                "{binary} names {flag}, but docs/operations.md never does"
            );
        }
    }
}

/// The extractor itself: it finds flags in an option list and in prose,
/// keeps only lowercase `--long` names, and ignores shorts and values.
#[test]
fn help_flags_parses() {
    let text = "Usage: x [OPTIONS]\n\
                \x20 --max-bytes <MAX_BYTES>  Most bytes [default: 10 GiB]\n\
                \x20 -h, --help               Print help\n\
                \x20 --socket <SOCKET>        Unix socket, mode 0600\n";
    assert_eq!(help_flags(text), vec!["--max-bytes", "--socket"]);
}
