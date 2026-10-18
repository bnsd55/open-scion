//! The demo's output folders stay out of git: `docker/demo.sh` makes its
//! certificates in `docker/certs` (a CA private key among them) and its
//! work tree in `docker/work`, and a `git add -A` after a demo run must
//! never stage those. The check runs `git check-ignore` on the two
//! folders the script's `mkdir -p` names.

use std::path::Path;

const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

#[test]
fn the_demo_output_folders_are_ignored() {
    let root = Path::new(ROOT);
    if !root.join(".git").exists() {
        // A build from a packaged source has no repository to ask.
        return;
    }
    // The folders of `rm -rf certs work; mkdir -p certs work` in
    // docker/demo.sh. If the script's outputs move, move these too.
    // A trailing-slash ignore pattern answers about the files inside the
    // folder, so the probes are files, not the folders themselves.
    for probe in ["docker/certs/ca.key", "docker/work/seed/README.md"] {
        let ignored = std::process::Command::new("git")
            .args(["check-ignore", "-q", probe])
            .current_dir(root)
            .status()
            .expect("git runs");
        assert!(
            ignored.success(),
            "{probe} is not git-ignored: the demo writes its private keys there"
        );
    }
}
