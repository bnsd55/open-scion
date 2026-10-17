<p align="center">
  <img src="docs/assets/banner.jpg" alt="open-scion logo: a young scion grafted onto a rootstock whose roots are circuit traces" width="720">
</p>

# open-scion

[![CI](https://github.com/bnsd55/open-scion/actions/workflows/ci.yml/badge.svg)](https://github.com/bnsd55/open-scion/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.88+](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
![Status: proof of concept](https://img.shields.io/badge/status-proof%20of%20concept-yellow.svg)

A version control system for large monorepos where most of the writers are
AI agents. The server keeps the history and makes the commits; clients ask
only for what they need. Written in Rust.

> **Early proof of concept.** It runs on one machine and is not ready for real
> code. Planned work is tracked in [issues](https://github.com/bnsd55/open-scion/issues)
> and [milestones](https://github.com/bnsd55/open-scion/milestones).

## What this is, and what it is not

open-scion is an independent, open-source implementation of the design that
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

**Platforms.** Linux and macOS. Windows is not supported.

**Scope of v0.1.** v0.1 is one server process on one machine. The article
describes a horizontally scalable system: a small fleet of servers that need
little coordination, with metadata in Spanner and file contents in blob
storage, and a cache tier per region and a daemon per node in front of them.
Here, all data is in one SQLite file and one blob folder on one machine, all
writes go through one database connection, and lands to a ref are serialized
by a lock in the server's memory. So a second server process cannot share the
data. A backend that scales out is not built
([#47](https://github.com/bnsd55/open-scion/issues/47)). The cache tiers are
in review ([#64](https://github.com/bnsd55/open-scion/pull/64)).

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
 open-scion:   agent ── "src/a.rs changed" ──> server: commit, rebase, move main
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
scn cat main:src/lib.rs                                      # read one file, no clone
cs=$(scn commit -m "fix parser" --put src/lib.rs=./lib.rs)   # create a changeset
scn land "$cs"                                               # rebase onto main and move it
```

`scn land` exits non-zero on a conflict and prints the paths, so an agent can
re-read them, redo the change, and try again. A retry of the same land is safe:
the server returns the first result.

## Try it

`scripts/demo.sh` does all of the steps below on a fixed public repository,
prints the numbers, and removes what it made. The method and the checkout
benchmark against git are in [docs/benchmarks.md](docs/benchmarks.md).

```sh
cargo build --release
export SCION_TOKEN=dev SCION_SERVER=http://127.0.0.1:7878
./target/release/scion-server --data /tmp/scion import /path/to/a/git/repo
./target/release/scion-server --data /tmp/scion ticket-key --out /tmp/scion/ticket.key
# prints the public key; pass it to the tiers below
./target/release/scion-server --data /tmp/scion serve --ticket-key /tmp/scion/ticket.key &

./target/release/scn log -n 5
./target/release/scn log --path src -n 5          # changesets that changed src/
./target/release/scn blame main:README.md         # last changeset of each line
./target/release/scn cat main:README.md
./target/release/scn checkout main --into /tmp/work   # --sparse DIR for part of it
echo hello > hello.txt
cs=$(./target/release/scn commit -m "add hello" --put hello.txt=hello.txt)
./target/release/scn diff main "$cs"              # A hello.txt, as git diff --name-status
./target/release/scn merge-base main "$cs"        # as git merge-base --all
./target/release/scn is-ancestor main "$cs"       # exits 0 for yes, 1 for no
./target/release/scn land "$cs"
./target/release/scn bench --agents 200 --lands 10000
```

Every `scn` command takes `--json` for machine-readable output. A `scn`
error exits 2. A checkout that a signal stops removes what it wrote, and
then dies by the signal (a shell reports 130 for Ctrl-C).

### Access file

`--token T` gives one principal, `default`, which may read and write every
repo. For several principals, write an access file and start the server with
`--access FILE` (give one of the two, not both). The file is UTF-8 text, one
rule per line, fields separated by one tab. Empty lines and lines that start
with `#` are skipped:

```text
# token <principal> <the BLAKE3 digest of the token>
token	alice	d4289b3345cfbdfb36fb37ee89e85bd74447f837b04c18aee6233c731b1419a7
token	ci	<64 hex digits>
# read <principal> <repo>: every RPC on that repo
read	ci	main
# write <principal> <repo> [prefix]: changes at or below the prefix, by
# path parts. No prefix means the whole repo. Write implies read.
write	alice	main	src
write	ci	main
```

The file holds digests, never tokens. Print the digest of a token with
`scion-server hash-token`:

```sh
token=$(read -s -p token: t; echo "$t")
digest=$(./target/release/scion-server hash-token --token "$token")
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
user), new directories are group-writable, so they are refused until you run
that. Stop the daemon with Ctrl-C or SIGTERM: it then writes its index, and
its next start reads that one file instead of every stored object.

A regional cache can also write snapshots, so that a new node's daemon does
not start empty: give `scion-cache --snapshot DIR --snapshot-repo R` and it
writes `DIR/R.scnsnap` every `--snapshot-every` seconds, with the most
recently used objects of that repo, up to `--snapshot-bytes`. Copy the file
to the node (any copy tool or shared mount) and start the daemon with
`--seed FILE`: it imports the objects, each checked against its ID, before
it serves, and only if its store is empty. A warm daemon ignores the file.

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
that `main` gained each landed change exactly once. With one server process on
one machine, 200 agents landed 10,000 changes at about 200 lands per second,
with none lost or duplicated. Every land is on disk before it replies, and
concurrent requests share each full disk flush
([#3](https://github.com/bnsd55/open-scion/issues/3)). Importing a git
repository with 23,871 commits took 50 seconds.

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
| [`crates/scion-core`](crates/scion-core) | Object model and canonical byte format |
| [`crates/scion-proto`](crates/scion-proto) | gRPC API ([`proto/scion.proto`](proto/scion.proto)) |
| [`crates/scion-server`](crates/scion-server) | Storage, commits, lands, reads, git import |
| [`crates/scion-tier`](crates/scion-tier) | Cache tiers: `scion-cache` and `scion-daemon` |
| [`crates/scn`](crates/scn) | Command-line client |
| [`scripts`](scripts) | The demo and the checkout benchmark against git |
| [`docs/architecture.md`](docs/architecture.md) | How the parts fit together: storage, lands, reads, and the cache tiers |
| [`docs/format-v1.md`](docs/format-v1.md) | The v1 byte format. Object IDs are hashes of these bytes. |
| [`docs/protocol-v1.md`](docs/protocol-v1.md) | The v1 API contract: calls, limits, status codes, retries, durability and what a client checks |
| [`docs/benchmarks.md`](docs/benchmarks.md) | How the demo and the benchmarks run, what each number means, and how to rerun them |
| [`docs/design/scalable-backend.md`](docs/design/scalable-backend.md) | A design proposal, not built: stateless servers on a shared metadata store and object store |

## Design notes

These are this project's choices where the article does not give details.

| Topic | Choice |
|---|---|
| Hash and format | BLAKE3 with a different prefix per object kind, over a written, frozen byte format |
| Storage | SQLite for metadata and trees, one compressed file per blob. Anthropic uses Spanner and cloud storage; this is the single-machine stand-in. |
| Blobs per repo | One file per content, shared by all repos. A repo may name only its own blobs: those uploaded to it, and those that its changesets name. No answer of `PutBlob` or `CreateChangeset` shows whether another repo has a blob. `GetObjects` does not check this yet ([#13](https://github.com/bnsd55/open-scion/issues/13)). |
| Conflicts | Path-level only: the same path, or a directory above or below it. No text merge. |
| Landing | One land at a time per ref, inside one process. A changeset can be created in parallel without any lock. |
| Retries | Each land has a caller-chosen request key, so a retry returns the first result |
| Git | One-way import. Commit IDs are not kept identical to git commit IDs. |
| Cache tiers | As in the article, a daemon per machine on a socket and a regional cache in memory, and objects sent in the disk format that the daemon keeps. The Unix socket, TCP between tiers, the disk store, zstd as that format, the byte limits, and one shared fetch per missed object are this project's choices. |
| History and blame | Path history is derived data, as in the article: the form, one history tree per changeset, is this project's choice. Blame is computed on each call from first-parent changes, with a bound per call; the article keeps it as derived data too, which is planned ([#61](https://github.com/bnsd55/open-scion/issues/61)). Blame follows renames, as `git blame` does; path history does not, as `git log` without `--follow`. |
| Diff | Compares file states, not lines, in pages of 1,000 paths. Finds renames as `git diff` does, over the whole diff below a path, with a bound per call. |

Deliberate simplifications are marked `SHORTCUT:` in the code, each with its
limit and its upgrade path.

## Build and test

Requires Rust 1.88 or newer and `protoc` (the Protocol Buffers compiler; for
example `brew install protobuf` or `apt install protobuf-compiler`).

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Fuzz the decoders (needs nightly and `cargo-fuzz`):

```sh
cd crates/scion-core
cargo +nightly fuzz run decode "$PWD/fuzz/corpus/decode" "$PWD/fuzz/seeds/decode"
```

## Contributing

The project is at an early stage and the design still moves. Issues and
discussion are welcome. Before a large pull request, please open an issue to
agree on the approach.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
