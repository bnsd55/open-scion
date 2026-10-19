# Stability

What each surface of open-scion promises in 0.x, what may change without
notice, and how to upgrade. The crates carry one version, cut as
[RELEASING.md](RELEASING.md) says. In 0.x, SemVer means: a patch release
(`0.1.z`) fixes bugs and never changes a contract; a minor release
(`0.x.0`) may change anything that this file does not pin, including a
crate's API, the CLI's JSON fields, and limits, and may move the schema
forward with a migration.

| Surface | What 0.x promises | A patch may change | A minor may change | Upgrade direction |
|---|---|---|---|---|
| Wire protocol v1 (`scion.v1.Scion`, `scion.v1.Objects`; [protocol-v1.md](protocol-v1.md)) | The calls, their status codes, limits and durability rules, so an independent client or tier keeps working. | Nothing on the wire. A server fix may change error detail strings, never codes. | Adds calls and optional request or reply fields (a new client with an old server must stay within what the old server answers). A breaking change is a new protocol version served beside v1, not a change to v1. | A client pins the URL path `scion.v1.*`; a server serves every version it still supports. |
| Object format v1 (changesets, trees, IDs; [format-v1.md](format-v1.md)) | The bytes and the ID hashes never change: an ID written today still means the same bytes in every later 0.x. `crates/scion-core/tests/golden.rs` pins the bytes. | Nothing. | Nothing that touches the bytes; that is a new format version, read beside v1, never a rewrite of v1. | Old repositories stay readable; no migration of objects is ever needed for a v1 format. |
| SQLite schema (`crates/scion-server/migrations`, `PRAGMA user_version`) | A released migration never changes; open migrates forward, one migration per released version, each in one transaction. | Nothing: a patch ships no migration. | Adds migrations at the end, so a database from an older 0.x opens on the new build. A database newer than the build is corrupt data and is refused. | Upgrade by running the newer binary: it migrates on open. Downgrading below the database's schema version is refused. |
| Postgres schema (`migrations/postgres`, one schema per database) | The same migration rules as SQLite: released migrations never change; a run takes the schema advisory lock, so one server migrates at a time. | Nothing: a patch ships no migration. | Adds migrations at the end, the same forward-only rule. | Run the newer binary against the database; it migrates on open, and refuses a database from a newer build. |
| Snapshot files (`scion-daemon` `--seed`, `--snapshot`) | A reader trusts none of it: it checks every object against its ID before it stores it, so a reader accepts any file whose objects check out. The magic today is `scnsnap1\n`. | Nothing that a written snapshot depends on: a new build reads the snapshots an older release wrote. | May write a new magic and read the old ones beside it. | Snapshots are caches, not data: discard and re-snapshot at any time. |
| Ticket bytes (`scion-proto`, prefix `scion\0ticket\0v1\0`) | A ticket issued by one 0.x server is accepted by that server while its key lives; the bytes' prefix is versioned, and the signature covers it. | Nothing. | May change the bytes behind the prefix, or issue a v2 beside v1; keys do not survive a prefix change. | Clients hold tickets opaquely and re-request them; nothing to migrate. |
| `scn` CLI, human output | Nothing about the wording or layout; [cli.md](cli.md) documents the current shape. | May change wording. | May change anything. | Scripts that read human output are not stable; use `--json`. |
| `scn` CLI, `--json` output | A patch keeps the field names and types of a command's JSON. | Nothing. | May add fields, and may change or remove fields (0.x: the JSON is best-effort machine-readable, not a contract). | Scripts that read the JSON should ignore unknown fields. |
| Crate APIs (`scion-core`, `scion-proto`, `scion-tier`, `scion-server` as a library) | 0.x: the crates publish on crates.io and compile with the workspace's MSRV (1.88 now). | Bug fixes only, no API change. | May break any API: the crates are this project's own implementation, and the `scion-server` library especially is the server binary's inside, not a public SDK. | Pin the versions you depend on, and read the CHANGELOG per minor release. |
| The `test-hooks` feature of `scion-server` | Nothing, ever: it exists for this repo's own tests and gated proofs, default off. | May change or vanish. | May change or vanish. | Nothing may depend on it; the repo's own test hooks are the only users. |

What this file means by the version numbers: all five crates share one
version (`[workspace.package]`), a release bumps it in one place
([RELEASING.md](RELEASING.md)), and the CHANGELOG records what a release
changes. In 0.x, the patch-vs-minor rule above is the whole promise; 1.0
will say which surfaces are frozen.

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
