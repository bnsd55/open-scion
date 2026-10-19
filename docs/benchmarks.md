# Benchmarks

Two scripts make the numbers of this project, so anyone can run them again:

| Script | What it shows |
|---|---|
| [`scripts/demo.sh`](../scripts/demo.sh) | The whole system on one machine: import, many agents that land at once, and sparse checkouts through the cache tiers ([#55](https://github.com/bnsd55/openscion/issues/55)). |
| [`scripts/bench-checkout.sh`](../scripts/bench-checkout.sh) | 50 sparse checkouts at once through one `scion-daemon`, against git doing the same checkout ([#6](https://github.com/bnsd55/openscion/issues/6)). |

Each script builds the release binaries, fetches the dataset, and runs every
process on loopback, inside one private temp directory. When it exits, for
any reason except `kill -9`, it stops every process and removes that
directory. It uses only the command-line tools (`scion-server`, `scion-cache`,
`scion-daemon`, `scn` and `git`), so it keeps working when the internals
change.

## Dataset

| | |
|---|---|
| Repository | [tokio-rs/tokio](https://github.com/tokio-rs/tokio), MIT license |
| Commit | `be689a35f5ade5a39e507f79d3ec85cdab27806f` (tag `tokio-1.53.0`) |
| History | 4,582 commits; 865 files at that commit |
| Sparse checkout | `tokio/src/`: 377 files, 3.3 MiB |

The scripts fetch the commit by its ID, so the history is always the same.
Git copies it in a few seconds. `REPO_URL`, `REPO_COMMIT` and `SPARSE_DIR`
change the dataset.

## Requirements

- Linux or macOS, and network access to GitHub.
- Rust 1.88 or newer and `protoc`, as for the build.
- Git 2.28 or newer, with `git daemon`.
- `/usr/bin/time` (on Debian and Ubuntu: `apt install time`).
- `curl` and `pkill` (on Debian and Ubuntu: `apt install curl procps`):
  the scripts check for both and stop without them.
- About 2 GiB of free disk for `bench-checkout.sh` with 50 jobs: each of
  the 50 plain git clones holds the whole history.

## Run them

```sh
scripts/demo.sh
AGENTS=200 LANDS=10000 scripts/demo.sh

scripts/bench-checkout.sh
JOBS=10 SPARSE_DIR=tokio-util scripts/bench-checkout.sh
```

| Setting | Default | Used by | Meaning |
|---|---|---|---|
| `AGENTS` | 100 | demo | Agents that land at the same time (`scn bench --agents`) |
| `LANDS` | 2000 | demo | Lands by all agents together (`scn bench --lands`) |
| `JOBS` | 10 (demo), 50 (bench) | both | Checkouts at the same time |
| `REPS` | 5 | bench | Repetitions of each run; the table shows the median with min and max |
| `SPARSE_DIR` | `tokio/src` | both | The directory that each checkout writes |
| `REPO_URL`, `REPO_COMMIT` | the dataset above | both | The repository and the commit to import |
| `SERVER_PORT`, `CACHE_PORT` | 17878, 17879 | both | Loopback ports of the server and the cache |
| `GIT_PORT` | 19418 | bench | Loopback port of `git daemon` |
| `SERVER_METRICS_PORT`, `CACHE_METRICS_PORT` | 17880, 17881 | bench | Loopback ports of the server's and the cache's metrics exports |

Progress goes to stderr, and the results table to stdout. The output names
no machine details, so a table can be shared as it is.

## The demo

1. Fetch the dataset and import it into a new server data directory. The
   import time is measured.
2. Start `scion-server`, then a `scion-cache` in front of it, then a
   `scion-daemon` in front of the cache.
3. `scn bench`: `AGENTS` agents land `LANDS` changes in all, at the same
   time, directly on the server. Then it walks `main` and checks that `main`
   gained each landed change exactly once. The demo fails if it did not.
4. One sparse checkout through the empty daemon (cold), then `JOBS`
   checkouts at once through the daemon, which now holds the objects (warm).
5. Print the table, stop the processes and remove everything.

## The checkout benchmark

`REPS` repetitions, 5 by default. In each repetition, five runs follow one
another, alternating between the tools, so machine drift hits both alike:
the cold scion run, the plain and partial git clones, the warm scion run,
the mirror git clone. In each run, `JOBS` jobs start at the same time, and
each writes the same files: the files below `SPARSE_DIR` at the commit, and
nothing else. The script checks this: the first job of each run must write
the same files, with the same content, as the first job of the first run.

| Run | Each job runs | State before the run |
|---|---|---|
| scion, cold daemon | `scn checkout main --sparse tokio/src --into DIR`, through the daemon | Empty daemon, empty cache |
| scion, warm daemon | The same | The daemon holds the objects of the cold run. The cache is empty. |
| git clone | `git clone --no-checkout URL DIR`, then the sparse checkout below | Nothing on the machine |
| git clone --filter=blob:none | `git clone --no-checkout --filter=blob:none URL DIR`, then the sparse checkout below. Git fetches the blobs of `tokio/src` when it writes them. | Nothing on the machine |
| git clone --reference mirror | `git clone --no-checkout --reference MIRROR URL DIR`, then the sparse checkout below | A full mirror on the machine, made before the run |

The sparse checkout of every git job is
`git sparse-checkout set --no-cone /tokio/src/`, then `git checkout main`.
So git writes nothing before the sparse rules are set. Cone mode would also
write the files of every directory above `tokio/src`, and `scn checkout`
does not, so the jobs use no cone.

The article names these git mitigations: sparse checkouts and promisor
remotes (partial clone), and git mirrors. It also names shallow clones,
which this benchmark does not run.

- Git serves the repository with `git daemon` on loopback (`git://`), so
  git and scion both fetch from a server over TCP.
- Git runs without the user's and the system's git config, so local
  settings do not change the numbers.
- The daemon keeps its disk store from the cold run to the warm run.

### What each number means

| Column | Meaning |
|---|---|
| wall s | Wall time of the run: from the start of the first job to the end of the last. `/usr/bin/time -p` around the whole batch measures it. |
| CPU s | User plus system CPU time of every process of the run: the jobs, and the server side. For scion, the server side is `scion-server`, `scion-cache` and `scion-daemon`. For git, it is `git daemon` and the `upload-pack` and `pack-objects` processes that it starts. The server side starts fresh for each run, so this includes its start and stop. |
| server CPU s | The server alone: `scion-server`, or `git daemon` and its processes. This is the server load. |
| server MiB | The bytes that the server sent. For git: the pack files that the clones received. For scion: the server's out-bytes counters (`scion_rpc_bytes_total{direction="out"}`), scraped from its metrics export before and after the run. The counters include every reply byte: the objects, protocol framing, and the `ResolveRef` and `GetRootTree` answers, so a scion number counts a little more than git's pack bytes do. |

The second table divides each git run by a scion run in the same state. A
plain clone and a partial clone keep nothing on the machine, so they compare
with the cold daemon. The mirror is git's warm state, so it compares with
the warm daemon. A `-` means that the scion number is 0 at the precision
measured.

### What it does not measure

- A real network. Everything runs on loopback, so the bytes count, but
  network latency and bandwidth do not slow a transfer.
- A cache shared by many machines. There is one daemon and one cache.
- Cold disk reads. The script does not drop the operating system's file
  cache, so the server reads its files from memory after the import. On
  macOS, dropping it needs `sudo`, so a cold run means a cold daemon and
  cache, not a cold OS page cache.
- The cost of the access checks and tickets against git, which has none:
  both sides pay what their own checks cost, inside the numbers.
- Virtual checkout, where files load when first read
  ([#11](https://github.com/bnsd55/openscion/issues/11)).

## Where the checkout numbers come from

The workflow [`bench.yml`](../.github/workflows/bench.yml) runs the
checkout benchmark when a person asks for it (`workflow_dispatch` only:
never on pushes, pull requests or a schedule). It runs on
`ubuntu-24.04`, a shared GitHub runner. A shared runner is noisy: other
work lands on the same machine between and during runs, so the wall-time
and CPU columns move from run to run, and the min and max in each cell
are how that noise shows. The published table comes from one workflow
run, whole.

| | |
|---|---|
| Dataset | [tokio-rs/tokio](https://github.com/tokio-rs/tokio) at `be689a35f5ade5a39e507f79d3ec85cdab27806f` (tag `tokio-1.53.0`) |
| Runner | `ubuntu-24.04`, a shared GitHub runner |
| Commands | `scripts/bench-checkout.sh`, `JOBS=50 REPS=5` |

Before the table, each run prints the facts that drift: its scion commit,
its git version, its core count and its kernel. The job summary carries
those four lines above the table, so the numbers and the facts of the run
travel together.

## Results

The numbers here come from one `bench.yml` run, as ratios between the
tools. A table of one run names no machine details, so it can be shared
as it is.

- Run: [bench.yml run 39306408455](https://github.com/bnsd55/openscion/actions/runs/39306408455),
  on `main` at `7b7789fd76ad616efafcc52637079213f47a8aa2`.
- The facts of the run: scion commit `7b7789f`, git `2.55.0`, 4 cores,
  kernel `6.17.0-1022-azure` (runner image 20261012.334).
- 50 checkouts at once of `tokio/src/` (377 files), 5 repetitions per
  state; the table shows the median, with min and max.

| Run (median, min–max) | wall s | CPU s | server CPU s | server MiB |
|---|---|---|---|---|
| scion, cold daemon | 1.50 (1.45–1.52) | 5.34 (5.18–5.45) | 0.09 (0.08–0.10) | 0.91 (0.91–0.91) |
| git clone | 33.15 (32.76–33.57) | 130.02 (130.00–130.17) | 18.21 (18.18–18.34) | 799.89 (799.89–799.89) |
| git clone --filter=blob:none | 13.96 (13.89–14.01) | 52.11 (51.99–52.35) | 29.18 (29.09–29.36) | 289.28 (289.28–289.28) |
| scion, warm daemon | 1.35 (1.32–1.41) | 4.86 (4.75–4.92) | 0.03 (0.03–0.04) | 0.01 (0.01–0.01) |
| git clone --reference mirror | 0.80 (0.79–0.84) | 3.01 (2.99–3.04) | 0.16 (0.15–0.16) | 0.00 (0.00–0.00) |

| git ÷ scion (medians) | wall | CPU | server CPU | server MiB |
|---|---|---|---|---|
| git clone ÷ scion, cold daemon | 22.1x | 24.3x | 202.3x | 879.0x |
| git clone --filter=blob:none ÷ scion, cold daemon | 9.3x | 9.8x | 324.2x | 317.9x |
| git clone --reference mirror ÷ scion, warm daemon | 0.6x | 0.6x | 5.3x | 0.0x |

Where scion wins: a cold checkout of `tokio/src` is 9.3–22.1x faster
than git's clones of the same files, because the daemon and the server
send only what `tokio/src` needs — 0.91 MiB against git's 289–800 MiB
of pack files — and the server side stays nearly idle (0.09 CPU s
against git's 18–29 s).

Where scion loses: the warm daemon, 1.35 s, against git's mirror clone,
0.80 s — git is 1.7x faster here, and lighter on the CPU too (3.01
against 4.86 CPU s). The data says where the gap lives: the server side
is idle in both (0.03 against 0.16 CPU s, 0.01 MiB moved — the daemon
serves the objects the cold run left), so the difference is what each
of the 50 fresh client processes pays: a scn checkout starts a process,
connects, resolves the ref and walks the tree call by call, while git's
jobs read local objects the mirror already holds and move nothing over
the wire. A long-lived client, or a mount that keeps one process, does
not pay that 50 times — but this benchmark's 50 fresh checkouts do.
