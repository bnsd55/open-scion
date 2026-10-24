<p align="center">
  <img src="https://github.com/bnsd55/openscion/raw/main/docs/assets/banner.jpg" alt="openscion logo: a young scion grafted onto a rootstock whose roots are circuit traces" width="720">
</p>

# openscion

[![CI](https://github.com/bnsd55/openscion/actions/workflows/ci.yml/badge.svg)](https://github.com/bnsd55/openscion/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.88+](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)

A version control system for large monorepos where most of the writers are
AI agents. The server keeps the history and makes the commits; clients ask
only for what they need. Written in Rust.

> **Alpha (0.x).** It runs on one machine and is not ready for production
> code. What holds between versions is in [docs/stability.md](docs/stability.md);
> the order of the work, and what stays out until it passes, is in
> [docs/ROADMAP.md](docs/ROADMAP.md); planned work is tracked in
> [issues](https://github.com/bnsd55/openscion/issues)
> and [milestones](https://github.com/bnsd55/openscion/milestones).

## What this is, and what it is not

openscion is an independent, open-source implementation of the design that
Anthropic described in
[Redesigning source control](https://www.anthropic.com/engineering/redesigning-source-control)
(October 2026). Anthropic built a system called Scion for its own monorepo.
This project rebuilds the ideas from that public article.

- It is **not** affiliated with or endorsed by Anthropic, and it contains no
  Anthropic code. Where the article does not say how something works, this
  project makes its own choice, and the design notes below say so.
- It is **not** related to the [SCION Internet architecture](https://github.com/scionproto/scion),
  to [GoogleCloudPlatform/scion](https://github.com/GoogleCloudPlatform/scion),
  or to the `scion` crate on crates.io.

**Platforms.** Linux and macOS. Windows is not supported. What exactly is
tested and what is not: [Supported platforms](#supported-platforms) below.
What each release promises per surface: [docs/stability.md](docs/stability.md).
To check a release before you run it — the signed tag, the checksums,
the build attestations, the SBOM: [docs/verify.md](docs/verify.md).

**Interface.** openscion is a command-line tool and a service, and its
interface is text: commands, plain files, and log lines. That is the
accessibility surface this project supports. Human output may change
between releases; `--json` output is the stable form for scripts and
assistive tools ([docs/stability.md](docs/stability.md)).

What each severity level gets and for how long:
[docs/support.md](docs/support.md).

**Scope of v0.1.** v0.1 is one server process on one machine. The article
describes a horizontally scalable system: a small fleet of servers that need
little coordination, with metadata in Spanner and file contents in blob
storage, and a cache tier per region and a daemon per node in front of them.
Here, all data is in one SQLite file and one blob folder on one machine, all
writes go through one database connection, and lands to a ref are serialized
by a lock in the server's memory. So a second server process cannot share the
data. A backend that scales out is not built
([#47](https://github.com/bnsd55/openscion/issues/47)); the cache tiers are
built and run in front of the server (below).

## Why

Git assumes that every clone is a full, offline copy of the repository, and
that every merge starts on a developer's machine. That works for people. It
works badly when thousands of agents and CI jobs write to one large repository
all day. Anthropic reported more than 30,000 merges and about 25 million CI
checkouts per day on a repository with about 100 GB of history.

With git, each of those jobs copies data it does not need, and each merge
needs a fresh clone, a local rebase and a push that fails when someone else
pushed first.

## How it works

```
 git:          agent ── clone everything ──> work ── rebase, push ──> server
 openscion:   agent ── "src/a.rs changed" ──> server: commit, rebase, move main
```

**The server does the work.** History, commits and rebases live on the
server. An agent reads only the files it needs over the API, sends the files
it changed, and asks the server to land them. It never clones the repository.

**A commit lists only what changed.** A commit is called a *changeset*. It
stores the files that changed compared to its first parent, plus content
hashes of those files. Files and directory trees are stored once, by content
hash (BLAKE3), and never change. So creating a changeset costs as much as the
change, not as much as the repository.

**Landing is a server-side rebase.** To land a changeset on `main`, the
server checks whether any of its paths also changed on `main` after the
changeset's base. If not, it puts the same changes on top of the newest
`main` and moves `main` with one compare-and-swap. Two agents that change
different files never conflict and never need to pull. An agent sees a
conflict only when someone else changed the same file, or a directory above
or below it.

```
agent A changes a.rs ──land──> main 101   ok
agent B changes b.rs ──land──> main 102   ok: the server rebased B onto 101
agent C changes a.rs ──land──> conflict   a.rs changed after C's base
```

**Derived data is computed from the parents.** Anything that is not part of a
changeset, such as the full directory tree of a commit, is *derived*: computed
from the parent's value plus the changes, then cached. A client never sees
how it is stored, so the server can change it freely.

### For an agent

```sh
./target/release/scn cat main:src/lib.rs                                      # read one file, no clone
cs=$(./target/release/scn commit -m "fix parser" --put src/lib.rs=./lib.rs)   # create a changeset
./target/release/scn land "$cs"                                               # rebase onto main and move it
```

`scn land` exits non-zero on a conflict and prints the paths, so an agent can
re-read them, redo the change, and try again. A retry of the same land is safe:
the server returns the first result.

Every call carries a request ID (16 hex characters, one per command), which
the server and each tier log, so one command can be followed through every
hop's log
([docs/operations.md](https://github.com/bnsd55/openscion/blob/main/docs/operations.md), section 6). A partial reader - a
principal whose read rules name folder prefixes - reads the paths its rules
cover and cannot write; the server derives a view of the repo for it.

## Try it

`scripts/demo.sh` does all of the steps below on a fixed public repository
(tokio at commit `be689a35f5ade5a39e507f79d3ec85cdab27806f`), prints the
numbers, and removes what it made. The method and the checkout
benchmark against git are in [docs/benchmarks.md](https://github.com/bnsd55/openscion/blob/main/docs/benchmarks.md).

The steps below want a git repository with a `README.md` and code under
a `tokio/src/` folder (tokio is a workspace; its library lives in
`tokio/src`). Get one:

```sh
git clone https://github.com/tokio-rs/tokio /tmp/tokio
git -C /tmp/tokio checkout be689a35f5ade5a39e507f79d3ec85cdab27806f
```

(another repository works if you change the paths below to its own;
with tokio, every line runs as written).

```sh
cargo build --release
export SCION_TOKEN=dev SCION_SERVER=http://127.0.0.1:7878
install -d -m 0700 /tmp/scion
./target/release/scion-server --data /tmp/scion import /tmp/tokio
./target/release/scion-server --data /tmp/scion ticket-key --out /tmp/scion/ticket.key > /tmp/scion/ticket.pub
# the public key is now in ticket.pub; pass it to the tiers below
./target/release/scion-server --data /tmp/scion serve --ticket-key /tmp/scion/ticket.key &
./target/release/scion-server --data /tmp/scion fsck              # check the data directory; safe while the server runs
./target/release/scion-server --data /tmp/scion gc --dry-run      # what a collection would remove; safe while the server runs

./target/release/scn ls main:tokio/src            # mode, kind, size, name per entry
./target/release/scn log -n 5
./target/release/scn log --path tokio/src -n 5    # changesets that changed tokio/src/
./target/release/scn blame main:README.md
./target/release/scn cat main:README.md
./target/release/scn checkout main --into /tmp/work # --sparse tokio/src for part of it
mkdir -p /tmp/view
# The mount serves NFS from scn itself and holds this terminal: run it
# in another terminal, and leave it there until `scn unmount`. On macOS
# it runs `sudo mount_nfs`, so it asks for your password once.
./target/release/scn mount /tmp/view main           # files load when first read
# The mount is read-only, so this commit is refused until you mount
# with `--unstable-writable` (see below); the refusal names the flag.
./target/release/scn commit -m "from the mount" --from-mount /tmp/view
./target/release/scn unmount /tmp/view
echo hello > hello.txt
cs=$(./target/release/scn commit -m "add hello" --put hello.txt=hello.txt)
./target/release/scn diff main "$cs"                # A hello.txt, as git diff --name-status
./target/release/scn merge-base main "$cs"          # as git merge-base --all
./target/release/scn is-ancestor main "$cs"         # exits 0 for yes, 1 for no
./target/release/scn land "$cs"
./target/release/scn bench --agents 200 --lands 10000
```

Every `scn` command takes `--json` for machine-readable output. A `scn`
error exits 2. A checkout that a signal stops removes what it wrote, and
then dies by the signal (a shell reports 130 for Ctrl-C).

`scn mount` serves a ref over NFSv3 from `scn` itself, so a file loads when
it is first read, not when the checkout starts. It holds the terminal it
runs in until `scn unmount` stops it, and on macOS it runs `sudo
mount_nfs`, so it asks for your password once. In v0.1 the mount is
read-only: every write is answered `EROFS`, and `scn commit --from-mount`
refuses, because a symlinked parent directory in the overlay can still
redirect a write outside it. `scn mount --unstable-writable` takes writes
into a private overlay directory, and `scn commit --from-mount DIR`
commits exactly what
the mount holds. It is for single-user machines only: any local user who
can read the mount table can reach the mounted content through the NFS
port. `scn unmount DIR` stops it and removes the overlay.

### Health and metrics

The server, a cache and a daemon serve the standard gRPC health service
(`grpc.health.v1.Check` and `Watch`) on their main port, without a token:
a probe carries none, so a load balancer or `grpcurl` can check them.

The server and a cache can also export Prometheus metrics: start them with
`--metrics-listen 127.0.0.1:9090` (off by default) and scrape
`GET /metrics`. The listener is plain HTTP with no token, so it is refused
on a non-loopback address unless `--insecure-listen` is given. The
registry counts requests by RPC and code, a latency histogram, reply
bytes, lands by outcome, and a tier's cache hits and misses; the details
are in [docs/architecture.md](https://github.com/bnsd55/openscion/blob/main/docs/architecture.md), section 11.

### Garbage collection

```sh
./target/release/scion-server --data /tmp/scion gc --dry-run   # report only
./target/release/scion-server --data /tmp/scion gc             # collect, safe beside a live server
```

`gc` removes the blob files and rows that nothing names any more: a row
whose blob no stored changeset of its repo puts, and a file that no repo's
row names, both older than the grace period (`--grace-secs`, a week).
Rows go before files, so a crash leaves orphan files only, which the next
run removes. It is safe beside a live server and safe to repeat.

### Access file

`--token T` gives one principal, `default`, which may read and write every
repo. Prefer `--token-file FILE` (a file the owner alone can read) or
`SCION_TOKEN` in the environment: a token on the command line is readable by
every local account (`ps`), so the argv form prints a deprecation warning
and goes away in a later release. For several principals, write an access
file and start the server with
`--access FILE` (give one of the forms, not two). The file is UTF-8 text, one
rule per line, fields separated by one tab. Empty lines and lines that start
with `#` are skipped:

```text
# token <principal> <the BLAKE3 digest of the token>
token	alice	<64 hex digits>
token	pat	<64 hex digits>
token	ci	<64 hex digits>
# read <principal> <repo> [prefix]: reads at or below the prefix, by
# path parts. No prefix means the whole repo.
read	ci	main
# write <principal> <repo> [prefix]: changes at or below the prefix, by
# path parts. No prefix means the whole repo. A write rule needs a
# whole-repo read rule: a partial reader cannot write in v0.1 (#183).
read	alice	main
write	alice	main	src
write	ci	main
```

The file holds token digests, so it is a secret: the server reads it only
when its owner (or root) alone can read it, and refuses one that group or
others can read at start. `chmod 600 FILE` fixes it, and is worth doing
under umask 002, where a fresh file starts 0664.

A principal whose read rules are all prefixes is a *partial reader*: it
reads only what they cover, through the view of the repo that the server
derives for it. `ResolveRef` and `GetRootTree` answer the real changeset
ID and the view root; `ReadFile` and `ListDir` read through the view, and
a path outside the prefixes is answered exactly like a missing one. The
history RPCs (`GetChangeset`, `Log`, `Blame`, `MergeBase`, `IsAncestor`,
`Diff`), every write, and `GetObjects` without a view-root ticket answer
`UNIMPLEMENTED` with the same text for every such principal. A file that
gives a partial reader a write rule is refused at load. The file holds at
most 32 distinct read rule sets per repo, and a set at most 256 prefixes.

The file holds digests, never tokens. Print the digest of a token with
`scion-server hash-token`:

```sh
token=$(read -s -p token: t; echo "$t")
digest=$(printf %s "$token" | ./target/release/scion-server hash-token)
```

A principal with no rule for a repo cannot see the repo: every call answers
`NOT_FOUND`, as for a repo that does not exist. A change to a path outside
the principal's write prefixes answers `PERMISSION_DENIED` and names the
path. A land is checked for the principal that lands it, even when another
principal made the changeset.

### Quotas

With `--access`, three caps bound what one principal holds at once, and a
call over a cap fails before any work with `RESOURCE_EXHAUSTED`, naming the
flag of the cap it hit:

| Flag | Default with `--access` | Bounds |
|---|---|---|
| `--max-principal-requests` | 64 | Calls in flight, until each reply starts |
| `--max-principal-upload-bytes` | 536870912 (512 MiB) | Upload message bytes in flight |
| `--max-principal-pending-lands` | 64 | Lands in the `PENDING` state |

The pending-lands count comes from the store, so it is right after a
restart. A land that stays `PENDING` for longer than `--land-stale-secs`
(a day) is rejected by a sweep that runs at start and then once per hour:
no caller can still be finishing it, because a call runs at most 600 s. A
retry of a rejected land needs a new request key.

With `--token T`, every client is the same principal, so a per-principal
quota would be a global one: the three caps are off unless a flag sets one.

Checkouts can go through the cache tiers: a `scion-daemon` per machine, on a
Unix socket, in front of a regional `scion-cache`, in front of the server.
Each tier keeps what it fetched, so a second checkout on the machine sends
nothing past the daemon. The tiers serve only checkouts; the other `scn`
commands talk to the server. The daemon's data directory must be private to
your user (mode 0700, not a shared path such as `/tmp`), because its socket
lives there. Each directory above it must be owned by root or by you, and
must not be writable by group or others unless it has the sticky bit (as
`/tmp` has). `scn` checks the same for the socket. The error names the
directory; `chmod go-w DIR` fixes it. With umask 002 (a private group per
user), a plain mkdir makes a group-writable directory, and it is refused by
choice (the rule stays fail-closed). Make the directory with
`install -d -m 0700 DIR`, or run `chmod g-w DIR`. Stop the daemon with Ctrl-C or SIGTERM: it then writes its index, and
its next start reads that one file instead of every stored object.

A regional cache can also write snapshots, so that a new node's daemon does
not start empty: give `scion-cache --snapshot DIR --snapshot-repo R` and it
writes `DIR/R.scnsnap` every `--snapshot-every` seconds, with the most
recently used objects of that repo, up to `--snapshot-bytes`. Copy the file
to the node (any copy tool or shared mount) and start the daemon with
`--seed FILE --seed-full-repo` (the snapshot is a full-repo credential, so
the daemon refuses the seed without the say-so): it imports the objects,
each checked against its ID, before
it serves, and only if its store is empty. A warm daemon ignores the file.
A snapshot holds every object of the repo, so give a node only repos that
all its users may read whole: a folder rule hides parts of a repo from a
partial reader, but not from the snapshot file.

A read needs a ticket: `scn checkout` asks the server for one (`--server`,
where it also resolves the ref), and fetches the objects through a tier
(`--objects`). A tier that carries the objects verifies each ticket against
the server's public key; the daemon forwards it. One daemon serves one OS
user: the user it serves can read its store files anyway.

```sh
export SCION_UPSTREAM_TOKEN=dev                  # what each tier sends upstream
PUB=$(cat /tmp/scion/ticket.pub)                 # from the server's ticket-key
./target/release/scion-cache --ticket-public-key "$PUB" &   # memory, 127.0.0.1:7879
./target/release/scion-daemon --data "$HOME/.scion-daemon" &   # disk, Unix socket
SCION_SERVER=http://127.0.0.1:7878 \
SCION_OBJECTS="unix://$HOME/.scion-daemon/daemon.sock" \
  ./target/release/scn checkout main --into /tmp/work2
```

Without `--ticket-key` on the server, no object is readable: a read needs a
ticket, and the server mints none. An object ID alone is not a permission.

`scn bench` runs many agents that land changes at the same time, then checks
that `main` gained each landed change exactly once. In a run of 200 agents
that landed 10,000 changes, none was lost or duplicated. Every land is on
disk before it replies, and concurrent requests share each full disk flush
([#3](https://github.com/bnsd55/openscion/issues/3)). The numbers depend on
the machine; how to rerun them, and what each one means, is in
[docs/benchmarks.md](https://github.com/bnsd55/openscion/blob/main/docs/benchmarks.md).

### TLS

Over loopback the token travels unencrypted, which is fine on one machine. For
a non-loopback address, serve TLS: give the server, or a `scion-cache`, a
certificate and key in PEM files. Then it accepts the address without
`--insecure-listen`. A certificate from a private CA is what the client trusts:
`scn` takes it with `--tls-ca`, a tier for its upstream with its own
`--tls-ca`. For links between tiers, the server and the cache can require a
client certificate with `--tls-client-ca` (mTLS); the cache and the daemon
present one with `--tls-client-cert` and `--tls-client-key`. To make the
certificates of a test setup, a tool such as `minica` or `openssl` makes a CA
and leaves signed for each host's name or address.

The server reads the private key only when its owner (or root) alone can
read it: `chmod 600 server.key`. The certificate and every CA file must
not be group- or others-writable — a swapped certificate or CA changes
whom a client trusts or which client certificates the server accepts —
so `chmod go-w` on them, and check after a copy tool or a umask 002
session, where a fresh file starts 0664.

```sh
./target/release/scion-server --data /tmp/scion serve \
  --listen 0.0.0.0:7878 --tls-cert server.pem --tls-key server.key &
./target/release/scn --server https://scion.example.com:7878 \
  --tls-ca ca.pem log -n 5
```

A bearer token over plain TCP to a non-loopback address stays refused.

## Layout

| Path | Contents |
|---|---|
| [`crates/scion-core`](https://github.com/bnsd55/openscion/blob/main/crates/scion-core) | Object model and canonical byte format |
| [`crates/scion-proto`](https://github.com/bnsd55/openscion/blob/main/crates/scion-proto) | gRPC API ([`proto/scion.proto`](https://github.com/bnsd55/openscion/blob/main/proto/scion.proto)) |
| [`crates/scion-server`](https://github.com/bnsd55/openscion/blob/main/crates/scion-server) | Storage, commits, lands, reads, git import |
| [`crates/scion-tier`](https://github.com/bnsd55/openscion/blob/main/crates/scion-tier) | Cache tiers: `scion-cache` and `scion-daemon` |
| [`crates/scn`](https://github.com/bnsd55/openscion/blob/main/crates/scn) | Command-line client |
| [`scripts`](https://github.com/bnsd55/openscion/tree/main/scripts) | The demo and the checkout benchmark against git |
| [`docs/architecture.md`](https://github.com/bnsd55/openscion/blob/main/docs/architecture.md) | How the parts fit together: storage, lands, reads, and the cache tiers |
| [`docs/format-v1.md`](https://github.com/bnsd55/openscion/blob/main/docs/format-v1.md) | The v1 byte format. Object IDs are hashes of these bytes. |
| [`docs/protocol-v1.md`](https://github.com/bnsd55/openscion/blob/main/docs/protocol-v1.md) | The v1 API contract: calls, limits, status codes, retries, durability and what a client checks |
| [`docs/cli.md`](https://github.com/bnsd55/openscion/blob/main/docs/cli.md) | Every `scn` command: what it does, its flags, the global flags and env vars, and the exit codes |
| [`docs/cli-vs-git.md`](https://github.com/bnsd55/openscion/blob/main/docs/cli-vs-git.md) | Every `scn` command next to the closest `git` and `gh` operation, and the differences that matter for a reader arriving from a GitHub workflow |
| [`docs/operations.md`](https://github.com/bnsd55/openscion/blob/main/docs/operations.md) | How to run a server: deploy, keys, the access file, Postgres, backups, and what each failure means |
| [`docs/threat-model.md`](https://github.com/bnsd55/openscion/blob/main/docs/threat-model.md) | The supported profile, the assets, the actors, the trust boundaries, and each security invariant with the test that proves it |
| [`docs/faults.md`](https://github.com/bnsd55/openscion/blob/main/docs/faults.md) | One row per fault: what the client sees, what the operator sees, what survives a restart, and the test that proves it |
| [`docs/compatibility.md`](https://github.com/bnsd55/openscion/blob/main/docs/compatibility.md) | Which component pairs work at versions N and N-1, what "works" means per pair, and what a break needs |
| [`docs/benchmarks.md`](https://github.com/bnsd55/openscion/blob/main/docs/benchmarks.md) | How the demo and the benchmarks run, what each number means, and how to rerun them |
| [`docs/RELEASING.md`](https://github.com/bnsd55/openscion/blob/main/docs/RELEASING.md) | For maintainers: how a release is cut and published |
| [`docs/stability.md`](https://github.com/bnsd55/openscion/blob/main/docs/stability.md) | What 0.x promises per surface: the crates' APIs, the CLI and its `--json`, protocol v1, object format v1, the schemas, snapshots, tickets, and the test hooks |
| [`docs/design/scalable-backend.md`](https://github.com/bnsd55/openscion/blob/main/docs/design/scalable-backend.md) | A design proposal, not built: stateless servers on a shared metadata store and object store |

## Design notes

These are this project's choices where the article does not give details.

| Topic | Choice |
|---|---|
| Hash and format | BLAKE3 with a different prefix per object kind, over a written, frozen byte format |
| Storage | SQLite for metadata and trees, one compressed file per blob. Anthropic uses Spanner and cloud storage; this is the single-machine stand-in. |
| Blobs per repo | One file per content, shared by all repos. A repo may name only its own blobs: those uploaded to it, and those that its changesets name. No answer of `PutBlob` or `CreateChangeset` shows whether another repo has a blob. `GetObjects` serves only what a read ticket's walk reaches, and the ticket is minted on a root the repo's own changesets reach. |
| Conflicts | Path-level only: the same path, or a directory above or below it. No text merge. |
| Landing | One land at a time per ref, inside one process. A changeset can be created in parallel without any lock. |
| Retries | Each land has a caller-chosen request key, so a retry returns the first result |
| Git | One-way import. Commit IDs are not kept identical to git commit IDs. |
| Cache tiers | As in the article, a daemon per machine on a socket and a regional cache in memory, and objects sent in the disk format that the daemon keeps. The Unix socket, TCP between tiers, the disk store, zstd as that format, the byte limits, and one shared fetch per missed object are this project's choices. |
| History and blame | Path history is derived data, as in the article: the form, one history tree per changeset, is this project's choice. Blame is computed on each call from first-parent changes, with a bound per call; the article keeps it as derived data too, which is planned ([#61](https://github.com/bnsd55/openscion/issues/61)). Blame follows renames, as `git blame` does; path history does not, as `git log` without `--follow`. |
| Diff | Compares file states, not lines, in pages of 1,000 paths. Finds renames as `git diff` does, over the whole diff below a path, with a bound per call. |

Deliberate simplifications are marked `SHORTCUT:` in the code, each with its
limit and its upgrade path.

## Supported platforms

What CI runs, on the runners that CI books, is what "supported" means
here. Every job is in [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

| Where | Runner, architecture | What runs there |
|---|---|---|
| Ubuntu 24.04, x86-64 | GitHub's `ubuntu-24.04` image, Linux 6.x, ext4 | All 15 CI jobs: the lint and doc jobs, `test`, the tmpfs disk-full test, block-level power cuts (dm-log-writes), the Postgres suite, the power-loss suite on LazyFS 0.3.1, the mount (NFS) job through the kernel's NFS client, the demo in Docker, the fuzz smoke on `x86_64-unknown-linux-gnu`, the release smoke of the README quick start, and `publish check` |
| macOS 15, Apple Silicon (arm64) | GitHub's `macos-15` image, APFS | `test (macOS)`: the whole test suite; `release smoke (macos-15)`: the README quick start on release binaries |

The macOS row carries its repetition evidence: after PR
[#309](https://github.com/bnsd55/openscion/pull/309)'s fork gate, the mount
and commit tests that fork passed 100 runs in a row on macos-15 ([the
mount-reps
run](https://github.com/bnsd55/openscion/actions/runs/39310255775)).

PostgreSQL 17.11 (`postgres:17.11`) is the version the Postgres job tests;
it is the only version CI exercises, and other server versions are untested.
The release builds target x86-64 Linux; macOS runs the same suite from
source. The toolchain floor is Rust 1.88 (`rust-version`, checked by the
`check (MSRV)` job).

Not supported, or tested only in parts:

- **Windows**: not supported, no build.
- **FreeBSD**: does not build (`rustix::fs::renameat_with` does not exist
  there; [architecture.md](docs/architecture.md), section 11).
- **A data directory on a network filesystem**: untested. The servers keep
  every byte in one directory (SQLite file or Postgres, plus a blob folder)
  and rely on its fsync and rename behavior; the power-loss and disk-full
  tests run on local and emulated block devices only.
- **`scn checkout` on NFS, FUSE or SMB mounts**: the code has paths for
  them (rename that never replaces, hard links, lost replies), and the
  tests force those paths, but they are untested on a real NFS mount or
  FUSE filesystem ([architecture.md](docs/architecture.md), section 11).
- **`scn mount`**: single-user machines only, by design: the kernel's NFS
  client reaches it as any local user ([#11](https://github.com/bnsd55/openscion/issues/11)).

The CI jobs: `fmt + clippy`, `rustdoc`, `cargo-deny`, `test` (with the
tmpfs disk-full and dm-log-writes steps), `test (postgres)` (Postgres
17.11), `test (macOS)`, `power-loss` (LazyFS), `mount (NFS)`, `check
(MSRV)` (Rust 1.88), `conformance (grpcurl)`, `docker demo`, `fuzz smoke`,
`release smoke` (ubuntu-24.04 and macos-15) and `publish check`.

## Build and test

Requires Rust 1.88 or newer and `protoc` (the Protocol Buffers compiler; for
example `brew install protobuf` or `apt install protobuf-compiler`).

v0.1.0 is a source release: the binaries of a version build from its
signed tag (`git checkout <tag>; cargo build --release --locked`). A
release that ships binaries says so in its release notes, and
`docs/verify.md` verifies them. An operator upgrading keeps the previous
release's binaries beside the new ones: a rollback needs them
(`docs/operations.md`, "Upgrades").

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

CI also runs `cargo deny check` (license and advisory checks on every
crate), a rustdoc build whose warnings fail the build, and the test that
follows every relative link in the repo's markdown.

Fuzz the decoders (needs nightly and `cargo-fuzz`). The corpus directory is
not checked in, and cargo-fuzz refuses to start without one; the seeds
directory is:

```sh
cd crates/scion-core
mkdir -p "$PWD/fuzz/corpus/decode"
cargo +nightly fuzz run decode "$PWD/fuzz/corpus/decode" "$PWD/fuzz/seeds/decode"
```

## Contributing

The project is at an early stage and the design still moves. Issues and
discussion are welcome. Before a large pull request, please open an issue to
agree on the approach. Report a security problem privately, not as a public
issue: [SECURITY.md](SECURITY.md) says how.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
