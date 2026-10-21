//! Every scion-cache and scion-daemon command docs/operations.md
//! writes in a shell block names only flags the binary's `--help`
//! knows, so a command the guide keeps past a flag's rename fails
//! here: the guide's commands cannot drift from `--help` in the other
//! direction `docs_flags` does not cover. `scion-server`'s commands and
//! the guide's `Proof:` lines are `docs_ops` in `scion-server`.

use std::path::Path;
use std::process::Command;

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

/// The `--long` flags one command line names, whole names only.
fn used_flags(command: &str) -> Vec<String> {
    let mut flags = Vec::new();
    for word in command.split_whitespace() {
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
        flags.push(format!("--{rest}"));
    }
    flags
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

#[test]
fn every_tier_command_in_the_operations_guide_uses_real_flags() {
    let doc = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/operations.md");
    let doc = std::fs::read_to_string(&doc).expect("docs/operations.md opens");
    let mut checked = 0;
    for command in sh_commands(&doc) {
        let words: Vec<&str> = command.split_whitespace().collect();
        let binary = words.iter().find_map(|word| {
            word.trim_start_matches("./")
                .strip_prefix("scion-")
                .map(|rest| format!("scion-{rest}"))
        });
        let Some(binary) = binary else { continue };
        if binary != "scion-cache" && binary != "scion-daemon" {
            continue;
        }
        let accepted = tier_flags(&binary);
        assert!(
            accepted.contains(&"--upstream".to_string()),
            "the flag parse of {binary} found nothing: {accepted:?}"
        );
        for flag in used_flags(&command) {
            checked += 1;
            assert!(
                accepted.contains(&flag),
                "the guide's command uses {flag}, but {binary} --help does not name it: {command}"
            );
        }
    }
    assert!(
        checked >= 2,
        "the guide's tier commands shrank: only {checked} flags checked"
    );
}

/// The extractor joins continuations and keeps other tools' lines.
#[test]
fn sh_commands_parse() {
    let commands = sh_commands(
        "before\n\
         \x60\x60\x60sh\n\
         ./scion-cache --token <T> \\\n\
         \x20 --upstream http://S:7878 \\\n\
         \x20 --ticket-public-key <K>\n\
         cp -R /srv /backups\n\
         \x60\x60\x60\n",
    );
    assert_eq!(commands.len(), 2, "{commands:?}");
    assert_eq!(
        commands[0],
        "./scion-cache --token <T> --upstream http://S:7878 --ticket-public-key <K>"
    );
    assert_eq!(
        used_flags(&commands[0]),
        vec!["--token", "--upstream", "--ticket-public-key"]
    );
}
