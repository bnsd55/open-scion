//! The guide's own commands, for the tests that prove a runbook:
//! [`runbook_block`] reads the first fenced `sh` block under a heading
//! of `docs/operations.md` verbatim, and [`run_guide_block`] runs it
//! with the test's values in place of the documented ones. A Proof whose
//! test drives the block this way runs the guide's own words, so a
//! guide that drifts from the test fails one of them.

use std::path::Path;
use std::process::Command;

/// The first fenced `sh` block under `heading` of docs/operations.md,
/// with the documented literals swapped for the test's values:
/// `subs` pairs a literal the guide prints (an address, a data
/// directory, a token file) with the value the test runs instead.
///
/// # Panics
///
/// If the heading or a block under it is missing: a Proof that names a
/// section without a command block cannot run the guide.
#[must_use]
pub fn runbook_block(heading: &str, subs: &[(&str, &str)]) -> String {
    runbook_block_with(heading, "", subs)
}

/// The first fenced `sh` block under `heading` that contains `needle`,
/// with `subs` substituted - for a section whose runbook has several
/// blocks (a backup's snapshot, its object copy), where a Proof's test
/// needs the step it performs. An empty `needle` is the first block.
///
/// # Panics
///
/// If the heading, a matching block, or a documented literal is
/// missing.
#[must_use]
pub fn runbook_block_with(heading: &str, needle: &str, subs: &[(&str, &str)]) -> String {
    let doc = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/operations.md");
    let text = std::fs::read_to_string(&doc).expect("docs/operations.md opens");
    let mut lines = text
        .lines()
        .skip_while(|line| line.trim() != format!("### {heading}").trim());
    lines.next();
    let mut block: Vec<&str> = Vec::new();
    let mut chosen = false;
    while let Some(line) = lines.next() {
        match line.trim() {
            "```sh" if block.is_empty() => {
                for line in lines.by_ref() {
                    if line.trim() == "```" {
                        break;
                    }
                    block.push(line);
                }
                if needle.is_empty() || block.join("\n").contains(needle) {
                    chosen = true;
                    break;
                }
                block.clear();
            }
            s if s.starts_with("### ") || s.starts_with("## ") => break,
            _ => {}
        }
    }
    assert!(
        chosen,
        "no sh block under {heading}{}",
        if needle.is_empty() {
            String::new()
        } else {
            format!(" mentions {needle}")
        }
    );
    let mut command = block.join("\n");
    // The longest documented literal goes first: /srv/scion is a prefix
    // of /srv/scion-token, and a prefix-first replace would destroy the
    // longer literal before its turn.
    let mut subs: Vec<&(&str, &str)> = subs.iter().collect();
    subs.sort_by_key(|(documented, _)| std::cmp::Reverse(documented.len()));
    for (documented, value) in subs {
        assert!(
            command.contains(documented),
            "the block under {heading} does not use the documented {documented}"
        );
        command = command.replace(documented, value);
    }
    command
}

/// Runs the block under `heading` with `subs` substituted, and returns
/// its stdout. The block runs through `sh`, exactly as an operator's
/// shell would.
///
/// # Panics
///
/// If the block exits non-zero: the guide's own command failed, which
/// is the proof failing, not the test.
#[must_use]
pub fn run_guide_block(heading: &str, subs: &[(&str, &str)]) -> String {
    run_guide_block_with(heading, "", subs)
}

/// Runs the block under `heading` that contains `needle`, with `subs`
/// substituted, and returns its stdout - the step's own command, as an
/// operator's shell would run it.
///
/// # Panics
///
/// If the block exits non-zero: the guide's own command failed, which
/// is the proof failing, not the test.
#[must_use]
pub fn run_guide_block_with(heading: &str, needle: &str, subs: &[(&str, &str)]) -> String {
    let command = runbook_block_with(heading, needle, subs);
    let out = Command::new("sh")
        .arg("-c")
        .arg(&command)
        .output()
        .expect("sh runs the guide's block");
    assert!(
        out.status.success(),
        "the guide's block failed: {command}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("the block's stdout is utf-8")
}
