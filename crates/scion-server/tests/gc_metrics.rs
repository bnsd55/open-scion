//! The metrics file of `scion-server gc --metrics-file`: one finished run
//! writes its gauges in the Prometheus text format of the node exporter's
//! textfile collector, the values match the report the command prints, a
//! dry run labels itself `dry_run="true"`, and a failed run never touches
//! the file. A dry run after a real run rewrites the whole file: what it
//! leaves is the dry run's own series, never a mix of two runs.

use std::fs;
use std::process::{Command, Output};

/// Runs `scion-server gc` on a fresh store in `dir`, with `--metrics-file`
/// set to `metrics`, and gives its output.
fn run_gc(dir: &std::path::Path, metrics: &std::path::Path, dry: bool) -> Output {
    let data = dir.join("data");
    fs::create_dir_all(&data).unwrap();
    let mut args = vec![
        "--data".to_owned(),
        data.display().to_string(),
        "gc".to_owned(),
        "--metrics-file".to_owned(),
        metrics.display().to_string(),
    ];
    if dry {
        args.push("--dry-run".to_owned());
    }
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(&args)
        .env("NO_COLOR", "1")
        .output()
        .unwrap()
}

/// One metric line's value: the line that starts with `name`, after its
/// optional `{labels}`.
fn value_of<'a>(text: &'a str, name: &str) -> &'a str {
    text.lines()
        .find(|line| line.starts_with(name))
        .unwrap_or_else(|| panic!("no {name} line in:\n{text}"))
        .split_once(' ')
        .expect("a value after the name")
        .1
        .trim()
}

/// The row count the command's report printed.
fn rows_of(stdout: &str) -> u64 {
    // "gc: N repos, M rows pruned, K files removed"
    let part = stdout.split(", ").nth(1).expect("the rows part");
    part.split(' ').next().unwrap().parse().unwrap()
}

/// The file count the command's report printed.
fn files_of(stdout: &str) -> u64 {
    let part = stdout.split(", ").nth(2).expect("the files part");
    part.split(' ').next().unwrap().parse().unwrap()
}

#[test]
fn a_real_run_writes_the_report_as_textfile_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let metrics = dir.path().join("gc.prom");

    let out = run_gc(dir.path(), &metrics, false);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let text = fs::read_to_string(&metrics).unwrap();

    // One line per metric, named and typed as the textfile collector
    // expects, and no temp file left beside the target.
    for name in [
        "scion_gc_last_success_timestamp_seconds",
        "scion_gc_duration_seconds",
        "scion_gc_rows_pruned",
        "scion_gc_files_removed",
    ] {
        assert!(text.contains(&format!("# TYPE {name} gauge")), "{text}");
    }
    assert!(!dir.path().join("gc.tmp").exists(), "the temp file is gone");

    // The values are the report's: the file says what stdout printed.
    assert_eq!(
        value_of(&text, "scion_gc_rows_pruned")
            .parse::<u64>()
            .unwrap(),
        rows_of(&stdout),
        "{text}"
    );
    assert_eq!(
        value_of(&text, "scion_gc_files_removed")
            .parse::<u64>()
            .unwrap(),
        files_of(&stdout),
        "{text}"
    );
    // A real run says so.
    let dry_line = text
        .lines()
        .find(|line| line.starts_with("scion_gc_rows_pruned"))
        .unwrap();
    assert!(dry_line.contains("dry_run=\"false\""), "{dry_line}");

    // The timestamp is a now, not a zero: the alert on the folder reads
    // the age of the last success from it.
    let stamp = value_of(&text, "scion_gc_last_success_timestamp_seconds")
        .parse::<u64>()
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(stamp > 1_700_000_000, "{stamp}");
    assert!(now.abs_diff(stamp) < 300, "stamp {stamp} against now {now}");
    // The duration is a number, and not negative (the text format has no
    // sign here), so parse it as float.
    value_of(&text, "scion_gc_duration_seconds")
        .parse::<f64>()
        .expect("a float duration");
}

#[test]
fn a_dry_run_labels_itself_and_a_failed_run_touches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let metrics = dir.path().join("gc.prom");

    // A real run first, then a dry run on the same --metrics-file: what
    // the file shows after both is the dry run's own series. Every one of
    // the four gauges carries the dry_run label, and none of the real
    // run's series survive: each run rewrites the whole file, so an
    // operator reading it never mixes two runs.
    let out = run_gc(dir.path(), &metrics, false);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = fs::read_to_string(&metrics).unwrap();
    let dry_line = text
        .lines()
        .find(|line| line.starts_with("scion_gc_rows_pruned"))
        .unwrap();
    assert!(dry_line.contains("dry_run=\"false\""), "{dry_line}");

    let out = run_gc(dir.path(), &metrics, true);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = fs::read_to_string(&metrics).unwrap();
    let samples = text
        .lines()
        .filter(|line| line.starts_with("scion_gc_"))
        .collect::<Vec<_>>();
    assert_eq!(
        samples.len(),
        4,
        "one file, one run, four series: {samples:?}"
    );
    for line in samples {
        assert!(line.contains("dry_run=\"true\""), "{line}");
    }

    // A failed run leaves the old file alone: a run whose data directory
    // cannot open answers 2 and writes nothing — the target of --data is
    // a file, so the open fails. The file from the dry run stays, with
    // its stale timestamp.
    let before = fs::read(&metrics).unwrap();
    fs::write(dir.path().join("blocker"), b"not a data directory").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args([
            "--data".as_ref(),
            dir.path().join("blocker").as_os_str(),
            "gc".as_ref(),
            "--metrics-file".as_ref(),
            metrics.as_os_str(),
        ])
        .env("NO_COLOR", "1")
        .output()
        .unwrap();
    assert!(!out.status.success(), "a gc on an absent store must fail");
    assert_eq!(fs::read(&metrics).unwrap(), before, "the file is untouched");
    let strays = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension() == Some(std::ffi::OsStr::new("tmp")))
        .collect::<Vec<_>>();
    assert!(
        strays.is_empty(),
        "no temp file left beside the target: {strays:?}"
    );
}
