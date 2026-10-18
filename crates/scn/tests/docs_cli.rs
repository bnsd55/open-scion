//! Every subcommand of `scn`, and every `--long` flag of each, is named
//! in docs/cli.md. A command or a flag that the binary grows and the
//! reference does not name fails here, so the reference cannot drift
//! from `--help`.

use std::path::Path;
use std::process::Command;

/// The `--long` flags a help text names, in prose or in its option list.
/// `--help` and `--version` are not flags of a command; they go.
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

/// The doc names `flag` as a flag: `--repo` does not match `--repo-dir`,
/// and `--max-bytes` does not match `--max-bytes-foo`. The next char
/// after the flag must not go on with a flag name, at any occurrence.
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

fn doc_text() -> String {
    let doc = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/cli.md");
    std::fs::read_to_string(&doc).expect("docs/cli.md opens")
}

fn scn(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(args)
        .output()
        .expect("scn runs");
    assert!(out.status.success(), "scn {} failed", args.join(" "));
    String::from_utf8(out.stdout).expect("help is utf-8")
}

/// The subcommands of `scn`, from the `Commands:` section of its
/// top-level help, so a new subcommand is checked without editing this
/// test. `help` prints help, not flags; drop it.
fn scn_subcommands() -> Vec<String> {
    let text = scn(&["--help"]);
    let commands = text
        .split("Commands:")
        .nth(1)
        .expect("scn --help lists Commands:")
        .split("Options:")
        .next()
        .expect("the Commands: section ends at Options:");
    let mut names: Vec<String> = commands
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| name.chars().all(|c| c.is_ascii_lowercase() || c == '-') && *name != "help")
        .map(str::to_string)
        .collect();
    names.sort();
    names.dedup();
    // The parse works: a checkout is a command, so an empty or a wrong
    // list means the parse broke, not that no commands exist.
    assert!(
        names.contains(&"checkout".to_string()),
        "the subcommand parse found nothing: {names:?}"
    );
    names
}

#[test]
fn every_scn_command_and_flag_is_in_the_reference() {
    let doc = doc_text();
    let subcommands = scn_subcommands();
    let mut commands = vec![Vec::<&str>::new()];
    commands.extend(subcommands.iter().map(|c| vec![c.as_str()]));
    for args in &commands {
        let mut call = args.clone();
        call.push("--help");
        let text = scn(&call);
        let flags = help_flags(&text);
        if args.as_slice() == ["log"] {
            // The extractor works: a reading command names --server, so
            // an empty list means the parse broke.
            assert!(
                flags.contains(&"--server".to_string()),
                "the flag parse of scn log found nothing: {flags:?}"
            );
        }
        for flag in flags {
            assert!(
                doc_names_flag(&doc, &flag),
                "scn {} names {flag}, but docs/cli.md never does",
                args.join(" ")
            );
        }
        if let [command] = args.as_slice() {
            assert!(
                doc_names_flag(&doc, &format!("scn {command}")),
                "docs/cli.md never describes scn {command}"
            );
        }
    }
}

/// The extractor finds flags in an option list and in prose, keeps only
/// lowercase `--long` names, and drops `--help` and `--version`.
#[test]
fn help_flags_parses() {
    let text = "Usage: scn [OPTIONS] cat <SPEC>\n\
                \x20 --server <SERVER>  Address [env: SCION_SERVER=]\n\
                \x20 -h, --help         Print help\n\
                \x20 -V, --version      Print version\n\
                Or `--token TOKEN` on the command line; pass it in SCION_TOKEN.\n";
    assert_eq!(help_flags(text), vec!["--server", "--token"]);
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
