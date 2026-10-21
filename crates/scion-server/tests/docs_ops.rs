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
        let named = proof
            .split(" (")
            .next()
            .unwrap_or(proof)
            .split_whitespace()
            .next()
            .unwrap_or(proof);
        let Some((file, fun)) = named.rsplit_once("::") else {
            panic!("Proof: {proof} is not path::function");
        };
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(file);
        let text = std::fs::read_to_string(&source)
            .unwrap_or_else(|_| panic!("the proof file {file} is not in the repo"));
        assert!(
            text.contains(&format!("fn {fun}(")) || text.contains(&format!("fn {fun}<")),
            "the proof names {file}::{fun}, but that function is not in the file"
        );
    }
    assert!(
        proofs >= 4,
        "the guide's Proof lines shrank: only {proofs} found"
    );
}

/// Every `Proof:` line in the operations guide either names a test
/// that drives its section's command block through the shared
/// `runbook_block` helper, or says why it cannot: a `(drill)` proof
/// names the recorded drill, a `(behaviour, no command)` proof names
/// the behaviour its test holds when the runbook's steps are operator
/// actions rather than shell commands, and a proof still converting
/// sits in `NOT_YET_DRIVING`. Anything else fails here, and so does a
/// stale row: the list must match the guide exactly, so no proof is
/// exempt silently and no finished conversion keeps its exemption.
#[test]
fn every_proof_runs_the_guides_command_or_says_why() {
    let doc = doc();
    let mut heading = String::new();
    let mut proofs = 0;
    let mut drivers = 0;
    let mut pending: Vec<(String, String)> = Vec::new();
    for line in doc.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("### ") || trimmed.starts_with("## ") {
            heading = trimmed.trim_start_matches('#').trim().to_owned();
            continue;
        }
        let Some(proof) = trimmed.strip_prefix("Proof: ") else {
            continue;
        };
        proofs += 1;
        if proof.contains("(drill)") || proof.contains("(behaviour, no command") {
            continue;
        }
        let named = proof
            .split(" (")
            .next()
            .unwrap_or(proof)
            .split_whitespace()
            .next()
            .unwrap_or(proof);
        let Some((file, fun)) = named.rsplit_once("::") else {
            panic!("Proof: {proof} is not path::function");
        };
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(file);
        let text = std::fs::read_to_string(&source)
            .unwrap_or_else(|_| panic!("the proof file {file} is not in the repo"));
        let runs_block = [
            "runbook_block",
            "run_guide_block",
            "run_guide_block_async",
            "start_on",
            "spawn_on",
            "guide_argv",
        ]
        .iter()
        .any(|helper| text.contains(&format!("{helper}(\"{heading}\"")));
        if runs_block {
            drivers += 1;
            continue;
        }
        assert!(
            NOT_YET_DRIVING.contains(&(heading.as_str(), fun)),
            "the proof of \"{heading}\" names {file}::{fun}, which does not run the section's own command block through runbook_block; convert the test, mark the Proof line with (behaviour, no command) and a reason, or add a NOT_YET_DRIVING row and list it in the PR body"
        );
        pending.push((heading.clone(), fun.to_owned()));
    }
    // The list is exactly the pending set: no stale rows, no missing
    // ones.
    let mut listed: Vec<(String, String)> = NOT_YET_DRIVING
        .iter()
        .map(|(h, f)| ((*h).to_owned(), (*f).to_owned()))
        .collect();
    listed.sort();
    pending.sort();
    pending.dedup();
    assert_eq!(
        listed, pending,
        "NOT_YET_DRIVING must name exactly the proofs that do not yet run their section's block"
    );
    assert!(
        proofs >= 17,
        "the guide's Proof lines shrank: only {proofs} found"
    );
    assert!(
        drivers >= 3,
        "the proofs that drive the guide's own blocks shrank: only {drivers}"
    );
}

/// The proofs whose tests still run the guide's commands their own
/// way, not through the shared `runbook_block` helper. Every row is
/// work to delete: convert the test to the helper, or move the proof
/// to a Proof-line marker - the list must match the guide exactly, so
/// a stale row fails the check. This list, mirrored in the PR body,
/// is today's honest state: no proof is exempt silently.
const NOT_YET_DRIVING: &[(&str, &str)] = &[
    (
        "The durability point",
        "every_reply_survives_a_kill_of_the_server",
    ),
    ("The durability point", "every_reply_survives_a_power_cut"),
    (
        "The durability point",
        "an_acknowledged_blob_survives_a_cut_at_the_last_barrier_before_its_reply",
    ),
    (
        "The durability point",
        "a_retry_after_a_failed_dir_fsync_makes_the_kept_file_durable",
    ),
    (
        "The durability point",
        "a_backup_taken_while_serving_restores_whole",
    ),
    (
        "The durability point",
        "a_restore_of_a_stated_store_takes_a_stated_time",
    ),
    (
        "A failed migration",
        "a_newer_schema_refuses_the_open_and_a_restore_serves",
    ),
    (
        "A failed migration",
        "a_kill_during_a_schema_upgrade_leaves_the_old_schema_or_the_new_one",
    ),
    (
        "5. Routines",
        "a_real_run_writes_the_report_as_textfile_metrics",
    ),
    (
        "5. Routines",
        "a_failing_disk_runbook_fscks_restores_and_serves",
    ),
    (
        "Stalled replies high",
        "a_stalled_log_reply_is_counted_and_closes_the_connection",
    ),
];

/// The alerts' responses point at the operations guide, and every
/// pointer resolves: a `docs/operations.md#anchor` names a heading of
/// the guide (GitHub's slug of it), a `section N` names the guide's
/// `## N.` heading. The reference lives in deploy/prometheus/*.yml,
/// whose files the alert rules and their test expectations both are.
#[test]
fn every_alert_response_anchor_resolves() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/prometheus");
    let mut checked = 0;
    for entry in std::fs::read_dir(&dir).expect("deploy/prometheus opens") {
        let path = entry.expect("entry reads").path();
        if path.extension().is_none_or(|e| e != "yml") {
            continue;
        }
        // The YAML folds its prose: a reference may wrap mid-phrase, so
        // the file reads as one line before the pointers are matched.
        let text = std::fs::read_to_string(&path).expect("the alert file opens");
        let text = text.replace('\n', " ");
        for (at, _) in text.match_indices("docs/operations.md") {
            {
                let rest = &text[at + "docs/operations.md".len()..];
                let rest = rest.trim_start_matches(['(', ' ']);
                if let Some(anchor) = rest.strip_prefix('#') {
                    let slug: String = anchor
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
                        .collect();
                    assert!(
                        headings(&doc()).iter().any(|h| h == &slug),
                        "the alert file {} points at docs/operations.md#{}, but no heading of the guide has that slug",
                        path.display(),
                        slug
                    );
                } else if let Some(rest) = rest.strip_prefix("sections ") {
                    for part in rest.split(" and ") {
                        let section: String = part
                            .trim_start()
                            .chars()
                            .take_while(char::is_ascii_digit)
                            .collect();
                        assert!(
                            !section.is_empty() && doc().contains(&format!("## {section}.")),
                            "the alert file {} points at docs/operations.md section {}, but the guide has no such section",
                            path.display(),
                            section
                        );
                        checked += 1;
                    }
                    continue;
                } else if let Some(rest) = rest.strip_prefix("section") {
                    let section: String = rest
                        .trim_start()
                        .chars()
                        .take_while(char::is_ascii_digit)
                        .collect();
                    assert!(
                        !section.is_empty() && doc().contains(&format!("## {section}.")),
                        "the alert file {} points at docs/operations.md section {}, but the guide has no such section",
                        path.display(),
                        section
                    );
                }
                checked += 1;
            }
        }
    }
    assert!(
        checked >= 20,
        "the alerts' guide references shrank: only {checked} found"
    );
}

/// Every heading of the guide, as GitHub slugs it: lowercased, spaces
/// and dots to dashes, other punctuation dropped.
fn headings(doc: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in doc.lines() {
        let Some(heading) = line.strip_prefix("#") else {
            continue;
        };
        let heading = heading.trim_start_matches('#').trim();
        let mut slug = String::new();
        for c in heading.chars() {
            if c.is_ascii_alphanumeric() {
                slug.extend(c.to_lowercase());
            } else if c == ' ' || c == '.' {
                slug.push('-');
            }
        }
        while slug.ends_with('-') {
            slug.pop();
        }
        out.push(slug);
    }
    out
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
