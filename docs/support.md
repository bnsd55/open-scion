# Support

What a reported problem gets, at what speed, and for how long. This is
the support policy the v0.4 gate asks for ([ROADMAP.md](ROADMAP.md)).
It describes what the project does today and names the pieces that
start with v0.4.

## Platform matrix

What runs where, at what confidence. A cell is a component on an OS,
an architecture, a data filesystem and a Postgres major. The statuses
are four: **supported** — a native CI job builds and tests the cell on
its own platform, every push; **experimental** — tested in CI, but not
yet recommended for production and not relied on for data-safety
evidence; **best effort** — a scheduled job (the nightly, or a release
candidate's dispatch) still runs the cell, but not on every push, and
nothing depends on it; **unsupported** — nothing runs it and nothing
is claimed. The rule of the v0.8 gate ([ROADMAP.md](ROADMAP.md)): a
cell the matrix marks **supported** runs natively in CI — a job of
[.github/workflows/ci.yml](../.github/workflows/ci.yml) builds and
tests it on its own platform, every push. Cross-compiling is not
evidence: a binary built on another platform proves nothing about the
cell it targets, and a release ships artifacts only for the cells
marked supported here.

| Component | OS | Arch | Data filesystem | Postgres | Status | CI job | Why |
|---|---|---|---|---|---|---|---|
| scion-server | Linux | x64 | ext4 | — | supported | `test` (ci.yml) | The default runner disk; the whole workspace suite runs on it |
| scion-server | Linux | x64 | ext4 | 16 | experimental | `test-postgres` (ci.yml) | Tested in CI; not yet recommended for production or relied on for data-safety evidence. Runs nightly and on release candidates |
| scion-server | Linux | x64 | ext4 | 17 | experimental | `test-postgres` (ci.yml) | Tested in CI; not yet recommended for production or relied on for data-safety evidence. The one major that also runs per push, on data-path changes |
| scion-server | Linux | x64 | ext4 | 18 | experimental | `test-postgres` (ci.yml) | Tested in CI; not yet recommended for production or relied on for data-safety evidence. Runs nightly and on release candidates |
| scion-server | Linux | x64 | ext4 | 15 | unsupported | — | Upstream supports 15 until November 2027, but the project does not claim it yet; one matrix line adds it when an operator asks |
| scion-server | Linux | x64 | tmpfs | — | supported | `test` (ci.yml) | The `disk_full` step fills a real tmpfs (`SCION_DISK_REQUIRE=1`) |
| scion-server | Linux | arm64 | ext4 | — | best effort | `test-arm64` (ci.yml) | The arm64 suite runs nightly and on release candidates, not per push; the cell is best effort until v0.8, and the Linux arm64 binaries return with the v0.8 support wave |
| scion-server | Linux | arm64 | ext4 | 17 | best effort | — | No job runs Postgres on arm64; the Postgres-backed arm64 run comes with the Postgres-major wave |
| scion-server | Linux | x64 | xfs | — | best effort | `test-xfs` (ci.yml) | Every store the server suite opens lands on a real xfs loop device (TMPDIR); the job runs nightly and on release candidates, not per push |
| scion-server | macOS | arm64 | APFS | — | supported | `test-macos` (ci.yml) | Native `macos-15` runner, full workspace suite; the runner's disk is APFS |
| scion-server | Windows | x64 | any | — | unsupported | — | No job builds or tests it; the lock files and paths assume POSIX. Unsupported until a native job exists |
| scion-cache | Linux | x64 | memory | — | supported | `test` (ci.yml) | The suite covers the tier crate; objects live in memory |
| scion-cache | Linux | arm64 | memory | — | best effort | `test-arm64` (ci.yml) | The suite covers the tier crate on arm64; the job runs nightly and on release candidates |
| scion-cache | macOS | arm64 | memory | — | supported | `test-macos` (ci.yml) | The suite covers the tier crate |
| scion-daemon | Linux | x64 | local disk | — | supported | `test` (ci.yml) | The suite covers the tier crate; the daemon's power-cut evidence is the `power-loss` job (LazyFS fault injection, a harness — not a filesystem promise) |
| scion-daemon | Linux | arm64 | local disk | — | best effort | `test-arm64` (ci.yml) | The suite covers the tier crate on arm64; the job runs nightly and on release candidates; the power-cut evidence stays on x64 (`power-loss`) |
| scion-daemon | macOS | arm64 | local disk (APFS) | — | supported | `test-macos` (ci.yml) | The suite covers the tier crate |
| scn | Linux | x64 | any | — | supported | `test` (ci.yml) | The CLI suite runs in the workspace job |
| scn | Linux | arm64 | any | — | best effort | `test-arm64` (ci.yml) | The CLI suite runs in the arm64 job, which runs nightly and on release candidates |
| scn | macOS | arm64 | any | — | supported | `test-macos` (ci.yml) | The CLI suite runs in the macOS job; the runner's disk is APFS |
| scn mount | Linux | x64 | NFS | — | supported | `nfs-mount` (ci.yml) | The kernel NFS client mounts scn's server on the runner |
| scn mount | macOS | arm64 | NFS | — | best effort | — | macOS can mount NFS, but no job proves it |
| scn mount | Windows | any | any | — | unsupported | — | The mount serves NFSv3 and rides POSIX mounts |

Runner labels move (macOS versions retire, Ubuntu releases come): the
labels this table names are checked against the workflows by a test,
`support_matrix` in `crates/scion-server/tests/`, the same way
[stability.md](stability.md)'s promises are pinned. A cell flips to
supported only when its native job exists; the test fails while a
supported row's job is missing, mismatched, or gone. A best-effort or
experimental row may name the scheduled job that still runs it — the
test checks that the named job exists — and flips to supported only
when that job runs the cell per push.

Postgres majors are promised per cell, not as a family. Every Postgres
cell is **experimental** in v0.1.0: tested in CI, but not yet
recommended for production or relied on for data-safety evidence —
SQLite is the backend the alpha builds its evidence on. The
`test-postgres` job runs Postgres 17 per push (on data-path changes),
and all three supported-upstream majors (16, 17, 18) nightly and on
release candidates; adding a major is one service line plus its suite
minutes — the cell row is added when the job runs it.

## Severity levels

| Level | It means | Example from this project |
|---|---|---|
| Critical | Data loss or a security boundary crossed, on the paths the docs promise | A read-rule bypass: a token whose access file denies a path can still read blobs under it, or a mount reads another principal's blobs |
| High | Data the user acknowledged is gone or wrong, or a crash a client can trigger | A lost acknowledged land: `Land` returned success, the caller switched work to the new head, and the next restart shows the old ref |
| Medium | A feature answers wrong or slowly under its documented limits, with a workaround | A blame call stops at its rename bound and reports fewer lines than the doc's example shows |
| Low | Cosmetic or documentation-only, no wrong answer | A status code's message text names the wrong limit |

## What each level gets

Today, with one supported line (the latest commit on `main`, per
[SECURITY.md](../SECURITY.md)), every fix lands on `main` and rides the
next release.

From v0.4 the split is: the current minor (release N) gets fixes for
all four levels — bugs, security problems, and data loss. The previous
minor (N-1) gets only critical and high security and data-loss fixes,
until it leaves the N-1 window described in
[compatibility.md](compatibility.md): clients, servers, caches and
daemons of version N and N-1 must work together, so a security or data
problem in N-1 is fixed in N-1 while that window is open. Medium and
low problems are fixed on the current minor only, and ride the normal
release train.

## How support ends

From v0.4, a release announces the end of its support in its release
notes: the notes say which release stops taking fixes, and the N-1
window closes when the next minor lands, per
[compatibility.md](compatibility.md). The version-matrix job
([.github/workflows/version-matrix.yml](../.github/workflows/
version-matrix.yml)) keeps the N-1 pairs tested while the window
is open.

## What 0.x means

There is no long-term-support minor in 0.x: every minor can change
what [stability.md](stability.md) does not pin, and only the latest
minor takes the full fix list. The first release that could carry an
LTS promise is v1.0, per the roadmap.
