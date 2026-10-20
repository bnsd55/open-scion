//! Every command docs/operations.md writes in a shell block names only
//! flags the binary's `--help` knows, so a command the guide grew or
//! kept past a rename fails here: the guide's commands cannot drift
//! from `--help` in the other direction `docs_flags` does not cover.
//!
//! Each runbook section also names the test that runs its steps on a
//! `Proof:` line; every named test must exist, so renaming or deleting
//! a proof breaks the guide and fails here.

use std::path::Path;
use std::process::Command;

/// The scion-server subcommands the guide's commands may use.
const SUBCOMMANDS: [&str; 7] = [
    "serve",
    "hash-token",
    "ticket-key",
    "import",
    "fsck",
    "gc",
    "derive",
];

/// The logical command lines of the guide's shell blocks: a block's
/// lines joined on the shell's backslash continuations, one string per
/// command.
fn sh_commands(doc: &str) -> Vec<String> {
    let mut commands = Vec::new();
    let mut lines = doc.lines().peekable();
    while let Some(line) = lines.next() {
        if line.trim() != "```sh" {
            continue;
        }
        let mut current = String::new();
        for line in lines.by_ref() {
            if line.trim() == "```" {
                break;
            }
            let line = line.trim();
            let continues = line.ends_with('\\');
            let line = line.trim_end_matches('\\');
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(line.trim());
            if !continues {
                commands.push(std::mem::take(&mut current));
            }
        }
    }
    commands.retain(|c| !c.is_empty());
    commands
}

/// The `--long` flags one command line names, whole names only, with
/// the word position each sits at.
fn used_flags_at(command: &str) -> Vec<(usize, String)> {
    let mut flags = Vec::new();
    for (at, word) in command.split_whitespace().enumerate() {
        let Some(mut rest) = word.strip_prefix("--") else {
            continue;
        };
        if let Some(eq) = rest.find('=') {
            rest = &rest[..eq];
        }
        if rest.is_empty()
            || !rest.chars().next().is_some_and(|c| c.is_ascii_lowercase())
            || !rest
                .chars()
                .all(|c| c.is_ascii_lowercase() || c == '-' || c.is_ascii_digit())
        {
            continue;
        }
        flags.push((at, format!("--{rest}")));
    }
    flags
}

/// The `--long` flags a help text names, in prose or in its option
/// list.
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
    help_flags(&String::from_utf8(out.stdout).expect("help is utf-8"))
}

/// The flags a command line may use, split at the subcommand: the
/// words before scion-server's subcommand check against the base
/// binary's help, the words after it against the subcommand's help.
/// Returns (position, accepted flags) pairs so a flag checks against
/// the help of the part of the command it sits in.
fn accepted_flags(command: &str) -> Vec<(usize, Vec<String>)> {
    let words: Vec<&str> = command.split_whitespace().collect();
    let sub = words.iter().position(|word| SUBCOMMANDS.contains(word));
    match sub {
        None => vec![(0, scion_server(&[]))],
        Some(at) => {
            let name = words[at];
            vec![(0, scion_server(&[])), (at + 1, scion_server(&[name]))]
        }
    }
}

fn doc() -> String {
    let doc = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/operations.md");
    std::fs::read_to_string(&doc).expect("docs/operations.md opens")
}

#[test]
fn every_command_in_the_operations_guide_uses_real_flags() {
    let mut checked = 0;
    for command in sh_commands(&doc()) {
        let words: Vec<&str> = command.split_whitespace().collect();
        let binary = words.iter().find_map(|word| {
            word.trim_start_matches("$(")
                .trim_start_matches("./")
                .trim_end_matches(')')
                .strip_prefix("scion-")
                .map(|rest| format!("scion-{rest}"))
        });
        let Some(binary) = binary else { continue };
        if binary != "scion-server" {
            continue; // scion-cache and scion-daemon: docs_ops in scion-tier
        }
        let accepted = accepted_flags(&command);
        assert!(
            accepted
                .iter()
                .any(|(_, flags)| flags.contains(&"--data".to_string())),
            "the flag parse of scion-server found nothing: {accepted:?}"
        );
        for (at, flag) in used_flags_at(&command) {
            checked += 1;
            assert!(
                accepted
                    .iter()
                    .rev()
                    .find(|(from, _)| at >= *from)
                    .is_some_and(|(_, flags)| flags.contains(&flag)),
                "the guide's command uses {flag}, but scion-server --help does not name it: {command}"
            );
        }
    }
    assert!(
        checked >= 8,
        "the guide's blocks shrank: only {checked} flags checked"
    );
}

/// Every runbook section's `Proof:` line names a test that exists: the
/// file is in the repo and holds the function.
#[test]
fn every_proof_line_names_a_test_that_exists() {
    let doc = doc();
    let mut proofs = 0;
    for line in doc.lines() {
        let Some(proof) = line.trim().strip_prefix("Proof: ") else {
            continue;
        };
        proofs += 1;
        let (file, fun) = proof
            .rsplit_once("::")
            .unwrap_or_else(|| panic!("Proof: {proof} is not path::function"));
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(file);
        let text = std::fs::read_to_string(&source)
            .unwrap_or_else(|_| panic!("the proof file {file} is not in the repo"));
        assert!(
            text.contains(&format!("fn {fun}")),
            "the proof names {file}::{fun}, but that function is not in the file"
        );
    }
    assert!(
        proofs >= 4,
        "the guide's Proof lines shrank: only {proofs} found"
    );
}

/// The command-line extractor: continuations join, other tools pass,
/// placeholders and values never read as flags.
#[test]
fn sh_commands_and_flags_parse() {
    let commands = sh_commands(
        "before\n\
         \x60\x60\x60sh\n\
         ./scion-server --data /srv serve \\\n\
         \x20 --token <TOKEN>\n\
         cp -R /srv /backups\n\
         TOK=$(./scion-server hash-token --token=<S>)\n\
         \x60\x60\x60\n",
    );
    assert_eq!(commands.len(), 3, "{commands:?}");
    assert_eq!(
        commands[0], "./scion-server --data /srv serve --token <TOKEN>",
        "{commands:?}"
    );
    assert_eq!(
        used_flags_at(&commands[0]),
        vec![(1, "--data".to_string()), (4, "--token".to_string())]
    );
    assert!(
        used_flags_at(&commands[1]).is_empty(),
        "cp -R names no flags"
    );
    assert_eq!(
        used_flags_at(&commands[2]),
        vec![(2, "--token".to_string())]
    );
}
