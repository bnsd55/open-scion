//! Every `--long` flag of `scion-server` and its subcommands is named in
//! docs/operations.md. A flag that the binary grows and the guide does
//! not name fails here, so the guide cannot drift from `--help`.

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

/// The doc names `flag` as a flag: `--repo` does not match `--repo-dir`,
/// and `--max-bytes` does not match `--max-bytes-foo`. The next char
/// after the flag must not go on with a flag name.
fn doc_names_flag(doc: &str, flag: &str) -> bool {
    let goes_on = |c: char| c.is_ascii_lowercase() || c == '-' || c.is_ascii_digit();
    let mut from = 0;
    while let Some(at) = doc[from..].find(flag) {
        let at = from + at;
        let whole = doc[at + flag.len()..]
            .chars()
            .next()
            .is_none_or(|c| !goes_on(c));
        if whole {
            return true;
        }
        from = at + 1;
    }
    false
}

fn assert_flags_documented(binary: &str, args: &[&str], doc: &str, flags: &[String]) {
    for flag in flags {
        assert!(
            doc_names_flag(doc, flag),
            "{binary} {} names {flag}, but docs/operations.md never does",
            args.join(" ")
        );
    }
}

fn scion_server(args: &[&str]) -> Vec<String> {
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(args)
        .arg("--help")
        .output()
        .expect("scion-server runs");
    assert!(
        out.status.success(),
        "scion-server {} --help failed",
        args.join(" ")
    );
    let text = String::from_utf8(out.stdout).expect("help is utf-8");
    let flags = help_flags(&text);
    // The extractor works: a help text of a serving binary names its
    // flags, so an empty list means the parse broke, not that no flags
    // exist.
    if args == ["serve"] {
        assert!(
            flags.contains(&"--max-connections".to_string()),
            "the flag parse found nothing: {flags:?}"
        );
    }
    flags
}

#[test]
fn every_server_flag_is_in_the_operations_guide() {
    let doc = doc_text();
    for args in [
        Vec::<&str>::new(),
        vec!["serve"],
        vec!["hash-token"],
        vec!["ticket-key"],
        vec!["import"],
        vec!["fsck"],
        vec!["gc"],
        vec!["derive"],
    ] {
        let flags = scion_server(&args);
        assert_flags_documented("scion-server", &args, &doc, &flags);
    }
}

/// The extractor itself: it finds flags in an option list and in prose,
/// keeps only lowercase `--long` names, and ignores shorts and values.
#[test]
fn help_flags_parses() {
    let text = "Usage: x [OPTIONS]\n\
                \x20 --data <DATA>  The directory [env: SCION_DATA=]\n\
                \x20 -h, --help     Print help\n\
                Give `--token` or `--access`, not both. A land over 86400 s\n";
    assert_eq!(help_flags(text), vec!["--access", "--data", "--token"]);
}

/// `doc_names_flag` matches whole flag names only, at any position:
/// `--repo` matches when the doc also names `--repo-dir` later or
/// earlier, `--max-bytes` does not match a doc that only names
/// `--max-bytes-foo`, and a flag at the very end matches.
#[test]
fn doc_names_flag_matches_whole_names() {
    let doc = "run `--repo-dir` here, `--max-bytes-foo` there, but --repo at the end";
    assert!(doc_names_flag(doc, "--repo"));
    assert!(doc_names_flag(doc, "--repo-dir"));
    assert!(!doc_names_flag(doc, "--max-bytes"));
    assert!(!doc_names_flag(doc, "--repor"));
    assert!(!doc_names_flag(doc, "--missing"));
}
