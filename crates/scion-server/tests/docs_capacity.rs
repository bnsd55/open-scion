//! The capacity table of docs/operations.md (issue #387) cannot drift
//! from the envelope's shape: every cell of the grid (3 writers x 3
//! readers x 2 backends) is a row, numbers parse or the percentile
//! group is all `-` together (no half-filled cell publishes), the
//! caption states the edges, and
//! the footer names a commit that exists in this checkout's git - so a
//! table in the doc points at the run that produced it.

use std::path::Path;
use std::process::Command;

const DOC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/operations.md");

/// The grid's axes, in the order the table lists them.
const BACKENDS: [&str; 2] = ["sqlite", "postgres"];
const WRITERS: [u32; 3] = [2, 50, 200];
const READERS: [u32; 3] = [0, 5, 20];

/// The table's data rows: the markdown lines after the `|---|` rule of
/// the first table in the Capacity section.
fn table_rows(section: &str) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut grid = false;
    let mut rule_seen = false;
    let mut header_width = 0;
    for line in section.lines() {
        if !line.starts_with('|') {
            if grid && rule_seen {
                break;
            }
            continue;
        }
        let cells: Vec<String> = line
            .trim_matches('|')
            .split('|')
            .map(|c| c.trim().to_string())
            .collect();
        if !grid {
            // The grid table starts at the `| backend |` header; the
            // section's metadata table (Commit, Runner) comes first.
            grid = cells.first().is_some_and(|c| c == "backend");
            header_width = cells.len();
            continue;
        }
        if !rule_seen {
            rule_seen = cells.first().is_some_and(|c| c.starts_with('-'));
            assert!(
                cells.len() == header_width,
                "the separator row's width differs from the header's: {line}"
            );
            continue;
        }
        assert!(
            cells.len() == header_width,
            "a capacity row's width differs from the header's: {line}"
        );
        rows.push(cells);
    }
    rows
}

fn capacity_section(doc: &str) -> &str {
    let start = doc
        .find("## 7. Capacity")
        .unwrap_or_else(|| panic!("docs/operations.md has no Capacity section"));
    let rest = &doc[start..];
    match rest.find("\n## ") {
        Some(at) => &rest[..at],
        None => rest,
    }
}

/// The counters of the reader log, tested by the script against a
/// fixture in the log's real shapes: a line that starts with its attempt
/// number must still count its event (a grep anchored to the line's
/// start counted zero of everything, a review of #461).
#[test]
fn the_reader_counters_count_the_log_s_own_shapes() {
    let script = Path::new(DOC)
        .parent()
        .and_then(Path::parent)
        .expect("docs/ has a parent")
        .join("scripts/test-capacity-counters.sh");
    let out = Command::new("bash")
        .arg(&script)
        .output()
        .expect("the counter script runs");
    assert!(
        out.status.success(),
        "the counter script fails: {}\n{}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

fn repo_dir() -> &'static Path {
    Path::new(DOC)
        .parent()
        .and_then(Path::parent)
        .expect("docs/ has a parent")
}

#[test]
fn every_cell_of_the_grid_has_a_row_in_order() {
    let doc = std::fs::read_to_string(Path::new(DOC)).expect("docs/operations.md exists");
    let rows = table_rows(capacity_section(&doc));
    let mut want: Vec<(String, u32, u32)> = Vec::new();
    for backend in BACKENDS {
        for w in WRITERS {
            for r in READERS {
                want.push((backend.to_string(), w, r));
            }
        }
    }
    assert_eq!(
        rows.len(),
        want.len(),
        "the capacity table has {} rows, the grid needs {}",
        rows.len(),
        want.len()
    );
    for (row, (backend, w, r)) in rows.iter().zip(&want) {
        assert_eq!(&row[0], backend, "a row is on the wrong backend: {row:?}");
        assert_eq!(row[1], w.to_string(), "wrong writer count: {row:?}");
        assert_eq!(row[2], r.to_string(), "wrong reader count: {row:?}");
    }
}

#[test]
fn numeric_columns_parse_and_dashes_say_what_failed() {
    let doc = std::fs::read_to_string(Path::new(DOC)).expect("docs/operations.md exists");
    // Column indexes in the row: 3 land p99, 4 checkout p50, 5 checkout
    // p99, 6 timeouts, 7 errors, 9 landed, 10 blob growth, 11 database
    // growth.
    for row in table_rows(capacity_section(&doc)) {
        check_row(&row);
    }
}

/// The one row's shape, shared by the doc's table and the fixture rows
/// the rule is tested against.
fn check_row(row: &[String]) {
    let all_dash = row[3..].iter().all(|c| c.as_str() == "-");
    if all_dash {
        return; // the skeleton: no run has filled this cell yet
    }
    let timeouts: u64 = row[6]
        .parse()
        .unwrap_or_else(|_| panic!("timeouts does not parse: {row:?}"));
    let errors: u64 = row[7]
        .parse()
        .unwrap_or_else(|_| panic!("errors does not parse: {row:?}"));
    // The percentile group (land p99, checkout p50, checkout p99)
    // stands together or parses together: a bench that died names
    // none of them, a live one names all three, and a cell with no
    // readers (r=0) names the land p99 and dashes both checkout
    // columns. A dash in one and a number in another is otherwise a
    // broken row, not a measurement.
    let group = &row[3..6];
    let dashes = group.iter().filter(|c| c.as_str() == "-").count();
    match dashes {
        0 => {
            for cell in group {
                cell.parse::<f64>()
                    .unwrap_or_else(|_| panic!("a number does not parse: {cell:?} in {row:?}"));
            }
        }
        // A no-reader cell: the writers ran, the readers never did.
        2 if row[3] != "-" && row[4] == "-" && row[5] == "-" => {
            row[3]
                .parse::<f64>()
                .unwrap_or_else(|_| panic!("the land p99 does not parse: {row:?}"));
        }
        3 if errors + timeouts > 0 => {}
        _ => panic!("a half-reported percentile group must not publish: {row:?}"),
    }
    // The counters and the resources are always numbers: the run
    // knows them whatever the bench did.
    for cell in &row[6..10] {
        cell.parse::<f64>()
            .unwrap_or_else(|_| panic!("a number does not parse: {cell:?} in {row:?}"));
    }
    // The growth columns parse as numbers, or say why they do not:
    // a "-" in the database column is SQLite (it has no separate
    // database), and a "-" in a growth column is a cell whose counter
    // grew by no lands at all.
    for (cell, what) in row[10..].iter().zip(["blob growth", "database growth"]) {
        if cell != "-" {
            cell.parse::<f64>()
                .unwrap_or_else(|_| panic!("{what} does not parse: {cell:?} in {row:?}"));
        }
    }
}

/// The row shapes the rule must accept and refuse, as fixture rows in
/// the table's own shape: a filled no-reader row, a dead bench's row,
/// and the half-filled row that must never publish.
#[test]
fn a_no_reader_row_names_the_land_and_dashes_the_reads() {
    let shapes: [(&[&str], bool); 6] = [
        // (cells after the cell name, does the rule accept it?)
        (
            &["63.1", "70", "115", "0", "0", "30376", "2000", "1106", "-"],
            true,
        ), // a live sqlite cell
        (
            &["63.1", "-", "-", "0", "0", "30376", "2000", "1106", "-"],
            true,
        ), // r=0: no readers ran
        (
            &["-", "-", "-", "0", "2", "30376", "1800", "990", "-"],
            true,
        ), // the bench died; the lands it got through still divide
        (
            &["63.1", "-", "115", "0", "0", "30376", "2000", "1106", "-"],
            false,
        ), // half-filled: never
        (
            &[
                "63.1", "70", "115", "0", "0", "30376", "2000", "1106", "1402",
            ],
            true,
        ), // a live postgres cell: both growth columns
        (
            &[
                "63.1", "70", "115", "0", "0", "30376", "2000", "nonsense", "-",
            ],
            false,
        ), // a growth column that is neither number nor "-": never
    ];
    for (cells, accepts) in shapes {
        let mut row: Vec<String> = vec!["sqlite".into(), "2".into(), "0".into()];
        row.extend(cells.iter().map(ToString::to_string));
        let checked = std::panic::catch_unwind(|| check_row(&row));
        assert_eq!(checked.is_ok(), accepts, "the rule disagrees about {row:?}");
    }
}

#[test]
fn the_caption_pins_the_filesystem_row() {
    let doc = std::fs::read_to_string(Path::new(DOC)).expect("docs/operations.md exists");
    let section = capacity_section(&doc);
    assert!(
        section.contains("| Filesystem | the filesystem the run measured"),
        "the caption omits the filesystem row the machine.txt artifact fills"
    );
}

#[test]
fn the_caption_names_both_edges() {
    let doc = std::fs::read_to_string(Path::new(DOC)).expect("docs/operations.md exists");
    let section = capacity_section(&doc);
    for edge in ["checkout p99 over", "land p99 over"] {
        assert!(section.contains(edge), "the caption omits the {edge} edge");
    }
}

#[test]
fn the_footer_names_a_commit_that_exists() {
    let doc = std::fs::read_to_string(Path::new(DOC)).expect("docs/operations.md exists");
    let section = capacity_section(&doc);
    let hex = section
        .lines()
        .find_map(|line| line.strip_prefix("| Commit | "))
        .and_then(|rest| {
            rest.split_whitespace()
                .find(|w| w.len() == 40 && w.chars().all(|c| c.is_ascii_hexdigit()))
        });
    // A skeleton table (every cell still `-`) names no commit yet; a
    // filled table must.
    let filled = table_rows(section)
        .iter()
        .any(|row| !row[3..].iter().all(|c| c.as_str() == "-"));
    if !filled {
        return;
    }
    let Some(hex) = hex else {
        panic!("the capacity table has numbers but the footer names no commit");
    };
    // A shallow checkout (a PR's) may not carry the object; the
    // capacity run's clone is full, and there the commit must resolve.
    let shallow = match Command::new("git")
        .args(["rev-parse", "--is-shallow-repository"])
        .current_dir(repo_dir())
        .output()
    {
        Ok(out) => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        Err(_) => "unknown".to_string(),
    };
    if shallow != "false" {
        return;
    }
    let ok = Command::new("git")
        .args(["cat-file", "-e", &format!("{hex}^{{commit}}")])
        .current_dir(repo_dir())
        .status()
        .expect("git runs")
        .success();
    assert!(ok, "the commit the capacity table names does not exist");
}
