# Stability

What each surface of openscion promises in 0.x, what may change without
notice, and how to upgrade. The crates carry one version, cut as
[RELEASING.md](RELEASING.md) says. In 0.x, SemVer means: a patch release
(`0.1.z`) fixes bugs and never changes a contract; a minor release
(`0.x.0`) may change anything that this file does not pin, including a
crate's API, the CLI's JSON fields, and limits, and may move the schema
forward with a migration.

| Surface | What 0.x promises | A patch may change | A minor may change | Upgrade direction |
|---|---|---|---|---|
| Wire protocol v1 (`scion.v1.Scion`, `scion.v1.Objects`; [protocol-v1.md](protocol-v1.md)) | The calls, their status codes, limits and durability rules, so an independent client or tier keeps working. | Nothing on the wire. A server fix may change error detail strings, never codes. | Adds calls and optional request or reply fields (a new client with an old server must stay within what the old server answers). A breaking change is a new protocol version served beside v1, not a change to v1, kept through the minor and dropped no earlier than the next minor ([compatibility.md](compatibility.md)). | A client pins the URL path `scion.v1.*`; a server serves every version it still supports. |
| Object format v1 (changesets, trees, IDs; [format-v1.md](format-v1.md)) | The bytes and the ID hashes never change: an ID written today still means the same bytes in every later 0.x. `crates/scion-core/tests/golden.rs` pins the bytes. | Nothing. | Nothing that touches the bytes; that is a new format version, read beside v1, never a rewrite of v1. | Old repositories stay readable; no migration of objects is ever needed for a v1 format. |
| SQLite schema (`crates/scion-server/migrations`, `PRAGMA user_version`) | A released migration never changes; open migrates forward, one migration per released version, each in one transaction. | Nothing: a patch ships no migration. | Adds migrations at the end, so a database from an older 0.x opens on the new build. A database newer than the build is corrupt data and is refused. | Upgrade by running the newer binary: it migrates on open. Downgrading below the database's schema version is refused. |
| Postgres schema (`migrations/postgres`; a database may hold several stores, one per schema, each with its own one-server lock) | The same migration rules as SQLite: released migrations never change; a run takes the schema advisory lock, so one server migrates at a time. | Nothing: a patch ships no migration. | Adds migrations at the end, the same forward-only rule. | Run the newer binary against the database; it migrates on open, and refuses a database from a newer build. |
| Snapshot files (`scion-daemon` `--seed`, `--snapshot`) | A reader trusts none of it: it checks every object against its ID before it stores it, so a reader accepts any file whose objects check out. The magic today is `scnsnap1\n`. | Nothing that a written snapshot depends on: a new build reads the snapshots an older release wrote. | May write a new magic and read the old ones beside it, until at least the next minor ([compatibility.md](compatibility.md)). | Snapshots are caches, not data: discard and re-snapshot at any time. |
| Ticket bytes (`scion-proto`, prefix `scion\0ticket\0v1\0`) | A ticket issued by one 0.x server is accepted by that server while its key lives; the bytes' prefix is versioned, and the signature covers it. | Nothing. | May change the bytes behind the prefix, or issue a v2 beside v1, read beside v1 until at least the next minor ([compatibility.md](compatibility.md)); keys do not survive a prefix change. | Clients hold tickets opaquely and re-request them; nothing to migrate. |
| `scn` CLI, human output | Nothing about the wording or layout; [cli.md](cli.md) documents the current shape. | May change wording. | May change anything. | Scripts that read human output are not stable; use `--json`. |
| `scn` CLI, `--json` output | A patch keeps the field names and types of a command's JSON. | Nothing. | May add fields, and may change or remove fields (0.x: the JSON is best-effort machine-readable, not a contract). | Scripts that read the JSON should ignore unknown fields. |
| Crate APIs (`scion-core`, `scion-proto`, `scion-tier`, `scion-server` as a library, `scion` as a library) | The crates are not published on crates.io and are not planned for it: releases are source-only, and the release binaries are the supported surface (D12). The Rust APIs are internal, and nothing outside this repo may depend on them. The toolchain floor is the workspace's MSRV (1.88 now), checked by CI. | Bug fixes only, no API change. | May break any API: the crates are this project's own implementation, and the `scion-server` library especially is the server binary's inside, not a public SDK. The facade listed in each crate's `lib.rs` is what a later publish would freeze, and `test_support` is never a promise. The `scion` library is its CLI's pieces: it exists for the scn binary and the package's tests, and is not an API to build on. A narrow client crate may publish when a real outside consumer exists; until then there is nothing to pin on crates.io. | Build from a tagged source tree; there is nothing to pin on crates.io. |
| The `test-hooks` feature of `scion-server` | Nothing, ever: it exists for this repo's own tests and gated proofs, default off. | May change or vanish. | May change or vanish. | Nothing may depend on it; the repo's own test hooks are the only users. |
| The `unstable-writable-mount` feature of `scn` | The write path of the mount (`--unstable-writable`, `commit --from-mount`, the VFS write calls) exists only in a build with this feature, off by default; a default or release build refuses the flag and the argument by name and serves read-only. | May change or vanish. | May change or vanish; the read-only mount is the stable surface ([ROADMAP.md](ROADMAP.md) D24). | Nothing may depend on it; a build that wants the write path names the feature. |

What this file means by the version numbers: all five crates share one
version (`[workspace.package]`), a release bumps it in one place
([RELEASING.md](RELEASING.md)), and the CHANGELOG records what a release
changes. In 0.x, the patch-vs-minor rule above is the whole promise,
plus the cross-version reach it carries: components of versions N and
N-1 work together per [compatibility.md](compatibility.md), so a minor
that changes a wire, ticket or snapshot contract serves or reads the
old version beside the new one and drops it no earlier than the next
minor. 1.0 will say which surfaces are frozen.

The toolchain floor (the MSRV, `rust-version` in
`[workspace.package]`) rises only in a minor release: a patch release
never raises it. A release that raises the floor says so in its release
notes. The floor never rises above the stable toolchain of six months
ago, so a user on a recent-at-release Rust can always build a 0.x
source tree. The rule lives here; CI's `check (MSRV)` job proves the
floor builds, not that the six-month rule holds.

## The 1.x promises

What each surface promises in 1.x. The 0.x table above keeps saying
what 0.x does; this section is what 1.0 freezes. The rule for all of
them: **additive only** — a 1.x minor may add calls, fields, flags,
env vars, exit codes and migrations, and never remove or retype one.
A breaking change is a new version served or read beside the old one
([compatibility.md](compatibility.md)), dropped no earlier than the
next minor.

| Surface | What 1.x promises | The machine check |
|---|---|---|
| Object format v1 | The bytes and the ID hashes never change — not in 1.x, not later. A new format is new IDs, read beside v1, forever. | Exists: `crates/scion-core/tests/golden.rs` pins the bytes and IDs. |
| Protocol v1 | Served throughout 1.x: the calls, their status codes, limits and durability rules never break; optional fields are added only; a break is v2 beside v1. | `crates/scion-proto/tests/protocol_surface.rs`: the calls and field numbers of the checked-in proto are additive-only against a golden (a removed call, a renumbered field, or a retyped field fails; an added one passes). |
| `scn` CLI, `--json` output | Field names and types are additive-only in 1.x: a minor may add fields, never change or remove one. | `crates/scn/tests/json_golden.rs`: every command the test runs answers with the fields and types its golden row names, read from the binary itself. |
| `scn` CLI, exit codes | 0 on success, 1 only for `is-ancestor`'s "no", 2 on any error. | `crates/scn/tests/exit_codes.rs`: one line per row of the table. |
| `scion-server` exit codes | `fsck` exits 0 when it saw no damage, 1 when it did, 2 when it could not run. | `crates/scion-server/tests/fsck_exit.rs`: clean, damaged, and unopenable stores, one line per row. |
| Production configuration | The flags, env vars and file formats the shipped binaries read change additively in 1.x: what a release ships stays, and 1.x only adds. The flags: docs_flags and docs_cli pin the names the docs carry. The env vars: `crates/scn/tests/env_surface.rs` reads the sources of both binaries, so a removed var fails and a new one enters the golden on purpose. The file formats carry no version field yet (the access file and the marker have none), so the 1.x promise is: keys add only, and a line a build does not know is refused at load, never silently read as a rule — the access file's parser refuses unknown keys, and `an_unknown_line_is_refused` in `crates/scion-server/tests/access.rs` pins it. A version field arrives with the first change that cannot stay additive. | Exists for flags; the env golden is new; the unknown-key refusal is pinned by `an_unknown_line_is_refused`. |
| SQLite and Postgres schemas | The 0.x rule is the 1.x rule: a released migration never changes, open migrates forward one migration per released version, a database newer than the build is refused, and the N/N-1 window holds (a store from release N-1 opens on N; the pre-upgrade backup restores onto N-1). | Exists: the migration tests; the N-1 restore drill (#379 item 3) once it lands. |
| Ticket bytes | The 0.x rule holds, and the window is pinned: v2 may be issued beside v1, both read until at least the next minor (compatibility.md). | Exists: the ticket prefix tests pin the version bytes. |
| Snapshot files | The 0.x rule holds, and the window is pinned: a new magic is written beside the old one until at least the next minor. | Exists: the snapshot magic tests. |

What carries no machine check, and why: "served throughout 1.x" is a
statement about every release, not a run — its machine form is the
additive goldens above, which fail CI on the change that would break a
client, before it ships. The version matrix is
[compatibility.md](compatibility.md)'s surface, and the tagged-store
and N-1 drills are #382's and #379's: stability.md links to them
instead of duplicating their checks.

## Where each promise is written down

- Wire protocol: [protocol-v1.md](protocol-v1.md), the contract of
  `scion.v1.Scion` and `scion.v1.Objects`. The version sits in the gRPC
  service name, so a second version is a second service.
- Object bytes and IDs: [format-v1.md](format-v1.md). The v1 sits in every
  ID's hash domain (`scion\0blob\0v1\0` and its siblings), so a new format
  is new IDs, never a silent rewrite. Pinned by golden tests.
- Schemas and migrations: `crates/scion-server/migrations` (SQLite) and
  `migrations/postgres` (Postgres), numbered `0001`, `0002`, ...; the
  schema version lives in the database
  ([operations.md](operations.md)).
- Snapshot bytes: the magic `scnsnap1\n` in
  [`crates/scion-tier/src/snapshot.rs`](../crates/scion-tier/src/snapshot.rs).
- Ticket bytes: the prefix `scion\0ticket\0v1\0` in
  [`crates/scion-proto/src/ticket.rs`](../crates/scion-proto/src/ticket.rs).
- The CLI's commands, flags and `--json` fields: [cli.md](cli.md).
- The 1.x goldens: the proto surface
  (`crates/scion-proto/tests/protocol_surface_v1.golden`), the JSON
  fields (`crates/scn/tests/json_fields_v1.golden`), and the env vars
  named in `crates/scn/tests/env_surface.rs`.
