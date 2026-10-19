//! The `serve` flags are refused before the data directory opens: a bad
//! value answers with an error that names its flag, and never takes the
//! data directory's one-server lock or creates a file in it.

use std::process::Command;

/// Runs `scion-server serve ARGS` with `data` as the data directory.
fn serve(data: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data)
        .args(["serve", "--token", "t"])
        .args(args)
        .env_remove("SCION_TOKEN")
        .env_remove("SCION_DATA")
        .env_remove("SCION_TEST_BACKEND")
        .env_remove("SCION_TEST_POSTGRES_URL")
        .output()
        .expect("scion-server runs")
}

/// The refused run names its flag, and the empty data directory stayed
/// empty: the store never opened, so no lock was taken and no file was
/// created.
fn refused_and_left_empty(data: &std::path::Path, out: &std::process::Output, flag: &str) {
    assert!(!out.status.success(), "{flag} must be refused: {out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains(flag), "{flag} is named in the error: {err}");
    let made: Vec<_> = std::fs::read_dir(data)
        .expect("the data directory still exists")
        .collect();
    assert!(made.is_empty(), "a refused serve creates no file: {made:?}");
}

#[test]
fn a_land_stale_secs_of_zero_or_less_is_refused() {
    for secs in ["0", "-1"] {
        let dir = tempfile::tempdir().unwrap();
        let out = serve(dir.path(), &["--land-stale-secs", secs]);
        refused_and_left_empty(dir.path(), &out, "--land-stale-secs");
    }
}

#[test]
fn a_zero_per_principal_cap_is_refused() {
    for (flag, value) in [
        ("--max-principal-requests", "0"),
        ("--max-principal-upload-bytes", "0"),
        ("--max-principal-pending-lands", "0"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let out = serve(dir.path(), &[flag, "0"]);
        refused_and_left_empty(dir.path(), &out, flag);
        assert_eq!(value, "0", "the refused value");
    }
}

#[test]
fn a_non_loopback_listen_is_refused_before_the_data_directory_opens() {
    let dir = tempfile::tempdir().unwrap();
    let out = serve(dir.path(), &["--listen", "10.0.0.1:7878"]);
    assert!(!out.status.success(), "the listen rule refuses: {out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("--insecure-listen"),
        "the error names the way out: {err}"
    );
    let made: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert!(made.is_empty(), "no file was created: {made:?}");
}

#[test]
fn a_non_loopback_metrics_listen_is_refused_before_the_data_directory_opens() {
    let dir = tempfile::tempdir().unwrap();
    let out = serve(dir.path(), &["--metrics-listen", "10.0.0.1:7878"]);
    assert!(!out.status.success(), "the metrics rule refuses: {out:?}");
    let made: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert!(made.is_empty(), "no file was created: {made:?}");
}

#[test]
fn a_serve_that_gives_token_and_access_is_refused_before_the_store_opens() {
    let dir = tempfile::tempdir().unwrap();
    let access = tempfile::tempdir().unwrap();
    let file = access.path().join("access");
    std::fs::write(&file, "token\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(dir.path())
        .args(["serve", "--token", "t"])
        .arg("--access")
        .arg(&file)
        .env_remove("SCION_TOKEN")
        .env_remove("SCION_DATA")
        .env_remove("SCION_TEST_BACKEND")
        .env_remove("SCION_TEST_POSTGRES_URL")
        .output()
        .unwrap();
    refused_and_left_empty(dir.path(), &out, "--access");
}
