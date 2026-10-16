# Benchmarks

Two scripts make the numbers of this project, so anyone can run them again:

| Script | What it shows |
|---|---|
| [`scripts/demo.sh`](../scripts/demo.sh) | The whole system on one machine: import, many agents that land at once, and sparse checkouts through the cache tiers ([#55](https://github.com/bnsd55/open-scion/issues/55)). |
| [`scripts/bench-checkout.sh`](../scripts/bench-checkout.sh) | 50 sparse checkouts at once through one `scion-daemon`, against git doing the same checkout ([#6](https://github.com/bnsd55/open-scion/issues/6)). |

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
| `SPARSE_DIR` | `tokio/src` | both | The directory that each checkout writes |
| `REPO_URL`, `REPO_COMMIT` | the dataset above | both | The repository and the commit to import |
| `SERVER_PORT`, `CACHE_PORT` | 17878, 17879 | both | Loopback ports of the server and the cache |
| `GIT_PORT` | 19418 | bench | Loopback port of `git daemon` |

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

Five runs. In each run, `JOBS` jobs start at the same time, and each writes
the same files: the files below `SPARSE_DIR` at the commit, and nothing else.
The script checks this: the first job of each run must write the same files,
with the same content, as the first job of the first run.

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
| server MiB | The object bytes that the server sent. For git: the pack files that the clones received. For scion: what the daemon's data directory gained in the run, which is what the server sent: the daemon stores each object in the form in which it arrives, a blob as the server's zstd file. Not counted: protocol framing, the `ResolveRef` and `GetRootTree` answers, and git's ref negotiation. |

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
  cache, so the server reads its files from memory after the import.
- Virtual checkout, where files load when first read
  ([#11](https://github.com/bnsd55/open-scion/issues/11)).

## Results

Numbers to be filled after [#53](https://github.com/bnsd55/open-scion/issues/53)
(wire format) lands. #53 makes the server send blobs in their stored zstd
form, which changes the bytes and the CPU time of every scion run. Results
come from one machine, as ratios between the tools.
