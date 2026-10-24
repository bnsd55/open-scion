//! The capacity harness's own safety rules (issue #387, the #555
//! review): the refusals and the arithmetic the table publishes stand
//! on, tested without a run - a run needs a runner, these need bash.

use std::path::Path;
use std::process::Command;

fn script(name: &str) -> &'static str {
    Box::leak(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../../scripts/{name}"))
            .to_str()
            .unwrap()
            .to_string()
            .into_boxed_str(),
    )
}

/// R1: the harness drops and recreates the database it runs on, so it
/// must refuse a URL whose database is not one of its own
/// `scion_capacity_*` name before any psql runs, and so before any
/// DROP.
/// The proof: a stub psql on PATH that logs every call; the refused
/// run's log stays empty.
#[test]
fn a_url_naming_another_database_is_refused_before_any_drop() {
    let work = tempfile::tempdir().unwrap();
    let stub = work.path().join("bin");
    std::fs::create_dir_all(&stub).unwrap();
    let log = work.path().join("psql.log");
    let stub_psq = stub.join("psql");
    std::fs::write(
        &stub_psq,
        format!("#!/bin/sh\necho \"$@\" >> {}\nexit 0\n", log.display()),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&stub_psq, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    // A database name that could be real: not the harness's own.
    let out = Command::new("bash")
        .arg(script("capacity.sh"))
        .env(
            "SCION_PG_URL",
            "postgres://u:p@localhost:5432/scion_refused",
        )
        .env(
            "PATH",
            format!("{}:{}", stub.display(), std::env::var("PATH").unwrap()),
        )
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "the harness ran on a database it does not own"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("scion_capacity_"),
        "the refusal does not name the harness's own naming: {stderr}"
    );
    assert!(
        !log.exists(),
        "psql ran on a refused URL: {}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
}

/// R2: the growth per 1,000 lands divides by the lands the counter
/// measured. The plan's lands (a bench's budget) never divide: a cell
/// that lost lands reports its own bytes over its own lands.
#[test]
fn the_growth_divides_by_the_lands_that_happened() {
    let out = Command::new("bash")
        .arg("-c")
        .arg(format!(
            "source {}\nkib_per_1k 100 200 50\nkib_per_1k 100 200 0\nkib_per_1k 0 0 2000\n",
            script("common.sh")
        ))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(String::from)
        .collect();
    // 100 KiB over the 50 lands that happened: 2,000 KiB per 1k lands,
    // not the 50 the plan's 2,000 lands would have reported.
    assert_eq!(lines[0], "2000", "the measured lands must divide");
    // A counter that grew by no lands: no per-land growth exists, and
    // the row says so instead of inventing one.
    assert_eq!(lines[1], "-", "no lands landed: the growth is not a number");
    // Zero growth over real lands is a real 0, not a dash: the
    // denominator is what the dash guards.
    assert_eq!(lines[2], "0", "no growth over measured lands is a real 0");
}
