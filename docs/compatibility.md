# Which pairs work at versions N and N-1

The v0.2.0 gate asks for "clients, servers, caches and daemons of
version N and N-1 work together, per a written compatibility policy"
([ROADMAP.md](ROADMAP.md)). This file is that policy. The contracts
themselves live in [stability.md](stability.md); this file says who may
talk to whom, what that talk must deliver, and what a break needs. All
five crates carry one version ([RELEASING.md](RELEASING.md)), so "at
version N" names a release, not a per-crate number.

## The moving pieces

- `scion-server`: the store (metadata + blobs) and the v1 gRPC services.
- `scion-cache` and `scion-daemon`: a tier in front of a server (or of
  another tier); the daemon adds a per-node unix socket and snapshots.
- `scn`: the client — commands, checkouts, and the mount.
- The store on disk: a data directory (a SQLite or Postgres schema plus
  blob files) that a server opens.

## The pairs, and what "works" means

| Pair | Versions | "Works" means |
|---|---|---|
| `scn` ↔ `scion-server` | N vs. N-1, both directions, across one minor step | Every RPC of `scion.v1.*` the client sends is answered within [protocol-v1.md](protocol-v1.md)'s status codes and limits; the ticket it holds verifies; the commands it runs do what [cli.md](cli.md) says |
| `scion-cache` / `scion-daemon` ↔ `scion-server` | N vs. N-1, both directions, one minor step | Reads forwarded within the same v1 contract; the ticket verifies; snapshots and tickets follow their versioned formats (stability.md) |
| `scn` ↔ `scion-daemon` (the `unix://` path) | N vs. N-1, both directions, one minor step | Reads over the socket behave as [cli.md](cli.md) documents; the CLI's `--json` rules are stability.md's |
| `scion-server` N ↔ a store of N-1 | One direction only | The open migrates forward, one transaction per released migration; a store newer than the build is refused (stability.md's schema rows) |
| `scn` ↔ repositories of any 0.x | Forever, both directions | Object bytes and IDs never change (object format v1): a checkout of anything any 0.x server wrote reads on any later build |
| tier ↔ tier (a chain of caches) | N vs. N-1, both directions, one minor step | Forwarding stays within the v1 object contract; a chain works when every hop does |

A pair this table does not list has no cross-version promise: two
servers of different versions behind one load balancer share a store
only if their schema versions agree, and a server that finds a newer
schema is refused at open, not half-broken.

## What a break needs

[stability.md](stability.md) sets the rule this file enforces: a patch
(`0.1.z`) fixes bugs and never changes a contract, so a patch release
keeps every pair above working in both directions; a minor (`0.x.0`)
may change anything stability.md does not pin. A change that would
break a listed pair therefore:

- rides a minor bump, never a patch;
- on the wire, a new protocol version served beside v1, not a change to
  v1 (the version is the gRPC service name, so both live on one
  server);
- in the objects, a new format version beside v1, never a rewrite of
  the bytes an ID names;
- in the store, a forward-only migration at the end of the list, so a
  store of N-1 opens on N;
- and the release notes of that minor name every pair it narrows.

Within a minor line, a newer client keeps working against an N-1
server: it sends nothing the old server cannot answer, and degrades or
errors clearly where the old server cannot follow.

## What is proven today, and what lands later

Today the promises rest on the contracts and their tests: the golden
tests pin the object bytes and IDs; the conformance job exercises
protocol v1 with a non-Rust client; the schema drift test proves both
backends migrate; the demo and the release smoke run a server and
`scn` from one tree.

What is not yet proven is cross-version: every CI job builds one tree.
The version matrix (#239, v0.3.0, part 1) starts once v0.1.0 is
tagged: it builds the tagged N-1 binaries and runs the pairs of the
table — a client of N against a server of N-1 and the reverse, a tier
of N behind a server of N-1, a checkout of a store N-1 wrote, and the
nightly soak across the pair. Until then the pairs are a policy, not a
tested fact; the matrix turns each row into a check.
