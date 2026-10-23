//! A changelog fragment the release script refuses fails its own PR.
//! The release refresh (scripts/merge-changelog.sh) applies rules to
//! every changes/<slug>.<section>.md; a fragment that breaks one - a
//! front-matter preamble, a section it would silently drop, an empty
//! file, a row marker on the first line - reaches main otherwise, and
//! only the release notices. The test runs the script's own --check
//! mode over the tree's fragments, so the rules live in one place and
//! cannot drift from what the release runs.

use std::path::Path;
use std::process::Command;

/// The repository root: the crate's manifest is crates/<crate>/
/// Cargo.toml, so the root is two directories up.
fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the manifest sits two directories under the repo root")
}

#[test]
fn every_changelog_fragment_passes_the_release_scripts_check() {
    let root = repo_root();
    let script = root.join("scripts/merge-changelog.sh");
    assert!(
        script.is_file(),
        "scripts/merge-changelog.sh is missing from the repo root"
    );
    let out = Command::new(&script)
        .arg("--check")
        .current_dir(root)
        .output()
        .expect("the check script runs");
    assert!(
        out.status.success(),
        "a changelog fragment fails the release script's rules:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
