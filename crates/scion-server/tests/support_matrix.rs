//! The platform matrix of docs/support.md and the CI jobs cannot drift:
//! a cell marked `supported` must name a job that exists in the
//! workflow file it names, and that job must run on the cell's OS and
//! architecture (`runs-on`), with the cell's Postgres major in its
//! services. A `best effort` or `unsupported` cell must name no job.
//! The reverse holds too: every CI job that runs a platform suite
//! (`test*`, `nfs-mount`, `power-loss`) must be claimed by at least one
//! row, so a renamed job fails here instead of silently leaving the
//! matrix.
//!
//! Cross-compiling is not support evidence (issue #388): the only
//! thing that flips a cell to supported is a native job, and this test
//! is where that claim is held.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
const MATRIX: &str = "docs/support.md";
const WORKFLOWS: &str = ".github/workflows";

/// The workflows whose jobs the matrix may claim.
const CHECKED_WORKFLOWS: &[&str] = &["ci.yml"];

/// Jobs that run a platform suite: the matrix must claim each of them.
const PLATFORM_JOBS: &[&str] = &["nfs-mount", "power-loss"];

#[test]
fn every_supported_cell_runs_natively_and_every_platform_job_is_claimed() {
    let rows = matrix_rows();
    assert!(
        rows.len() >= 19,
        "the matrix lost rows: {} in docs/support.md",
        rows.len()
    );

    let mut jobs: BTreeMap<String, Job> = BTreeMap::new();
    for workflow in CHECKED_WORKFLOWS {
        let path = Path::new(ROOT).join(WORKFLOWS).join(workflow);
        let text =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} opens: {e}", path.display()));
        for (name, job) in parse_jobs(&text) {
            jobs.insert(format!("{workflow}:{name}"), job);
        }
    }
    assert!(
        jobs.len() >= 12,
        "the workflows lost jobs: {} found",
        jobs.len()
    );

    let mut claimed: Vec<String> = Vec::new();
    // A platform job can also be claimed as evidence in a row's Why
    // (`power-loss` is the daemon's power-cut evidence, not a cell of
    // its own): a backticked mention in the Why counts.
    for key in jobs.keys() {
        let name = key.split(':').nth(1).unwrap_or("");
        if rows
            .iter()
            .any(|row| row.why.contains(&format!("`{name}`")))
        {
            claimed.push(name.to_owned());
        }
    }
    for row in &rows {
        let cell = format!(
            "{} on {} {} ({})",
            row.component, row.os, row.arch, row.filesystem
        );
        match row.status.as_str() {
            "supported" => {
                let (file, name) = row
                    .job
                    .clone()
                    .unwrap_or_else(|| panic!("`{cell}` is supported but names no job"));
                let key = format!("{file}:{name}");
                let job = jobs
                    .get(&key)
                    .unwrap_or_else(|| panic!("`{cell}` names {key}, which no workflow defines"));
                assert!(
                    job.runs_on_matches(&row.os, &row.arch),
                    "`{cell}` names {key}, which runs on `{}`, not on {} {}",
                    job.runs_on,
                    row.os,
                    row.arch
                );
                if let Some(major) = &row.postgres {
                    assert!(
                        job.runs_postgres(major),
                        "`{cell}` names {key}, whose services do not run postgres {major}"
                    );
                }
                claimed.push(name);
            }
            "best effort" | "unsupported" => {
                assert!(
                    row.job.is_none(),
                    "`{cell}` is {status} but claims a job; only a native job \
                     makes a cell supported",
                    status = row.status
                );
            }
            other => panic!("`{cell}` has an unknown status {other:?}"),
        }
    }

    for name in PLATFORM_JOBS {
        assert!(
            jobs.keys().any(|k| k.split(':').nth(1) == Some(*name)),
            "the {name} job is gone from the workflows while the matrix \
             still names it"
        );
        assert!(
            claimed.iter().any(|c| c == name),
            "the {name} job runs a platform suite no matrix row claims"
        );
    }
    for key in jobs.keys() {
        let name = key.split(':').nth(1).unwrap_or("");
        let platform = name.starts_with("test") || PLATFORM_JOBS.contains(&name);
        assert!(
            !platform || claimed.iter().any(|c| c == name),
            "the {key} job runs a platform suite no matrix row claims"
        );
    }
}

/// A job of a workflow: its `runs-on` label, its service images, and
/// the version list of its strategy matrix (a matrix job's service
/// image is the expression `${{ matrix.version }}`, which the check
/// resolves through the list).
struct Job {
    runs_on: String,
    services: Vec<String>,
    matrix_versions: Vec<String>,
}

impl Job {
    /// Whether the label is a hosted runner of this OS and arch: Linux
    /// x64 `ubuntu-*` (no `-arm`), Linux arm64 `ubuntu-*-arm`, macOS
    /// arm64 `macos-*` (every hosted macOS runner today is arm64;
    /// there is no hosted macOS x64 left to name).
    fn runs_on_matches(&self, os: &str, arch: &str) -> bool {
        match (os, arch) {
            ("Linux", "x64") => {
                self.runs_on.starts_with("ubuntu-") && !self.runs_on.ends_with("-arm")
            }
            ("Linux", "arm64") => {
                self.runs_on.starts_with("ubuntu-") && self.runs_on.ends_with("-arm")
            }
            ("macOS", "arm64") => self.runs_on.starts_with("macos-"),
            _ => false,
        }
    }

    /// Whether a service runs `postgres:<major>`: a literal image by
    /// its own version, a matrix image through the job's version list.
    fn runs_postgres(&self, major: &str) -> bool {
        self.services
            .iter()
            .any(|image| match image.strip_prefix("postgres:") {
                Some(version) if version.contains("matrix.version") => self
                    .matrix_versions
                    .iter()
                    .any(|v| v.split('.').next() == Some(major)),
                Some(version) => version.split('.').next() == Some(major),
                None => false,
            })
    }
}

/// The jobs of one workflow file, as `(name, Job)`. The scan knows the
/// four line shapes this repo's workflows use: the `jobs:` block, a job
/// key at two-space indent, `runs-on:` and `services:`/`image:` under
/// it, and a strategy matrix `os: [a, b]` — a job whose `runs-on` is
/// `${{ matrix.os }}` becomes one job per listed OS.
fn parse_jobs(text: &str) -> Vec<(String, Job)> {
    let mut jobs: Vec<(String, Job)> = Vec::new();
    let mut in_jobs = false;
    let mut key: Option<String> = None;
    let mut runs_on: Option<String> = None;
    let mut matrix_oses: Vec<String> = Vec::new();
    let mut matrix_versions: Vec<String> = Vec::new();
    let mut services: Vec<String> = Vec::new();
    let mut in_services = false;
    for line in text.lines() {
        if line.starts_with("jobs:") {
            in_jobs = true;
            continue;
        }
        if !in_jobs {
            continue;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !line.starts_with([' ', '\t']) {
            in_jobs = false;
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        let trimmed = line.trim();
        if indent == 2 && trimmed.ends_with(':') {
            if let Some(name) = key.take() {
                flush(
                    &mut jobs,
                    name,
                    runs_on.take(),
                    &mut matrix_oses,
                    &mut matrix_versions,
                    &mut services,
                );
            }
            key = Some(trimmed.trim_end_matches(':').to_owned());
            in_services = false;
            continue;
        }
        if key.is_none() {
            continue;
        }
        if let Some((k, v)) = trimmed.split_once(':') {
            match k.trim() {
                "runs-on" => runs_on = Some(v.trim().trim_matches('\'').to_owned()),
                "services" => in_services = true,
                "image" if in_services => services.push(v.trim().trim_matches('\'').to_owned()),
                "os" if v.trim().starts_with('[') => {
                    matrix_oses = list(v);
                }
                "version" if v.trim().starts_with('[') => {
                    matrix_versions = list(v);
                }
                _ => {}
            }
            if indent <= 4 && k.trim() != "image" && k.trim() != "services" && in_services {
                in_services = false;
            }
        }
    }
    if let Some(name) = key.take() {
        flush(
            &mut jobs,
            name,
            runs_on.take(),
            &mut matrix_oses,
            &mut matrix_versions,
            &mut services,
        );
    }
    jobs
}

/// `[a, b, c]` as its parts.
fn list(value: &str) -> Vec<String> {
    value
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|item| item.trim().trim_matches('"').trim_matches('\'').to_owned())
        .filter(|item| !item.is_empty())
        .collect()
}

/// Push one job, or one per strategy-matrix OS when `runs-on` is the
/// matrix expression.
fn flush(
    jobs: &mut Vec<(String, Job)>,
    name: String,
    runs_on: Option<String>,
    matrix_oses: &mut Vec<String>,
    matrix_versions: &mut Vec<String>,
    services: &mut Vec<String>,
) {
    let Some(runs_on) = runs_on else { return };
    if runs_on.contains("matrix.os") {
        for os in matrix_oses.drain(..) {
            jobs.push((
                format!("{name} ({os})"),
                Job {
                    runs_on: os.clone(),
                    services: services.clone(),
                    matrix_versions: matrix_versions.clone(),
                },
            ));
        }
    } else {
        jobs.push((
            name,
            Job {
                runs_on,
                services: std::mem::take(services),
                matrix_versions: std::mem::take(matrix_versions),
            },
        ));
    }
    services.clear();
}

struct Row {
    component: String,
    os: String,
    arch: String,
    filesystem: String,
    postgres: Option<String>,
    status: String,
    job: Option<(String, String)>,
    why: String,
}

/// The matrix rows of docs/support.md: the table under "## Platform
/// matrix", eight cells per row, the job cell as `` `name` (file) ``
/// or `—`.
fn matrix_rows() -> Vec<Row> {
    let text = fs::read_to_string(Path::new(ROOT).join(MATRIX))
        .unwrap_or_else(|e| panic!("{MATRIX} opens: {e}"));
    let table = text
        .split_once("## Platform matrix")
        .expect("the matrix section exists")
        .1
        .to_owned();
    let mut rows = Vec::new();
    for line in table.lines().skip_while(|l| !l.starts_with("| scion")) {
        let cells: Vec<&str> = line.split('|').collect();
        // `| a | b | … |` splits into an empty head and tail.
        if cells.len() != 10 {
            continue;
        }
        let cell = |i: usize| cells[i].trim();
        if cell(1) == "Component" || cell(1).starts_with("---") {
            continue;
        }
        let postgres = cell(5);
        let job = cell(7);
        let job = if job == "—" {
            None
        } else {
            let (name, file) = job
                .split_once(" (")
                .unwrap_or_else(|| panic!("the job cell `{job}` is `name` (file)"));
            Some((
                file.trim_end_matches(')').to_owned(),
                name.trim().trim_matches('`').to_owned(),
            ))
        };
        rows.push(Row {
            component: cell(1).to_owned(),
            os: cell(2).to_owned(),
            arch: cell(3).to_owned(),
            filesystem: cell(4).to_owned(),
            postgres: if postgres == "—" {
                None
            } else {
                Some(postgres.to_owned())
            },
            status: cell(6).to_owned(),
            job,
            why: cell(8).to_owned(),
        });
    }
    rows
}
