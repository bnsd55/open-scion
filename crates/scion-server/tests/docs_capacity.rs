//! The capacity table of docs/operations.md (issue #387) cannot drift
//! from the envelope's shape: every cell of the grid (3 writers x 3
//! readers x 2 backends) is a row, numbers parse or a row is all `-`
//! (no half-filled cell publishes), the caption states the edges, and
//! the footer names a commit that exists in this checkout's git - so a
//! table in the doc points at the run that produced it.

use std::path::Path;
use std::process::Command;

const DOC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/operations.md");

/// The grid's axes, in the order the table lists them.
const BACKENDS: [&str; 2] = ["sqlite", "postgres"];
const WRITERS: [u32; 3] = [2, 50, 200];
const READERS: [u32; 3] = [0, 5, 20];

/// The numeric columns, in order, after backend/writers/readers.
const NUMERIC_COLUMNS: usize = 7;

/// The table's data rows: the markdown lines after the `|---|` rule of
/// the first table in the Capacity section.
fn table_rows(section: &str) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut grid = false;
    let mut rule_seen = false;
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
            continue;
        }
        if !rule_seen {
            rule_seen = cells.first().is_some_and(|c| c.starts_with('-'));
            continue;
        }
        assert!(
            cells.len() >= NUMERIC_COLUMNS + 3,
            "a capacity row has too few columns: {line}"
        );
        rows.push(cells[..NUMERIC_COLUMNS + 3].to_vec());
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
    // Column indexes in the row: 3 lands/min, 4 land p99, 7 errors.
    for row in table_rows(capacity_section(&doc)) {
        let all_dash = row[3..].iter().all(|c| c.as_str() == "-");
        if all_dash {
            continue; // the skeleton: no run has filled this cell yet
        }
        let errors: u64 = row[7]
            .parse()
            .unwrap_or_else(|_| panic!("errors does not parse: {row:?}"));
        for (at, cell) in row[3..].iter().enumerate() {
            if cell.as_str() != "-" {
                cell.parse::<f64>().unwrap_or_else(|_| {
                    panic!("a number does not parse: {cell:?} in {row:?}")
                });
                continue;
            }
            // A dash is allowed only where a failed bench left the land
            // percentile unknown, and only in a cell that had errors:
            // the cell is the data, and the errors column says so.
            assert!(
                at == 1 && errors > 0,
                "a dash without a failing cell must not publish: {row:?}"
            );
        }
    }
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
