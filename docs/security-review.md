# The security review packet

This page is what a hired reviewer starts from: what to look at, what
has already been attacked from the inside, how to build and attack the
system locally in ten minutes, and where the project itself thinks it
is weakest. It links the standing documents instead of copying them;
where it states a fact, the test or the pull request that proves it is
named. Report findings as [SECURITY.md](../SECURITY.md) describes, not
as a public issue.

## 1. The scope

In scope, in the supported profile
([threat-model.md](threat-model.md), section 1):

- `scion-server` — the active server: the RPC surface, the metadata
  stores (SQLite and Postgres), the blob store, the archive verify and
  restore, the audit log.
- `scion-cache` and `scion-daemon` — the tiers in front of it, and
  what they trust.
- `scn` — the client: how it holds tokens, how it checks what a server
  says, and its NFS mount.
- The supported deployment from [operations.md](operations.md): one
  active server per data directory on one host, loopback or
  TLS-protected listeners, secrets as files or environment values.

Out of scope, and why — each is a boundary the model does not claim:

- A data directory on a shared or network filesystem: the open checks
  read the owner and the mode bits, not ACLs, and a remote owner is
  whatever the mount reports ([threat-model.md](threat-model.md),
  section 1).
- `--insecure-listen`: it sends tokens and data in the clear on
  purpose, and says so at start.
- An untrusted local account on the server host: the file-mode rules
  assume the accounts are the server's own; the page-cache side
  channels exist regardless (section 6 of the threat model).
- `scn mount` on a multi-user host: the kernel's NFS client reaches it
  as any local user ([#11](https://github.com/bnsd55/openscion/issues/11)).
- Windows, or a filesystem that reports one owner for every entry.
- The upstream space: tonic/prost's gRPC decode and `nfsserve`'s NFS
  decode are their projects' code; where our decoder takes over, our
  fuzz targets cover it ([fuzzing.md](fuzzing.md)).

## 2. The trust boundaries and the assets

Read [threat-model.md](threat-model.md) sections 2 and 4: the assets
table (history, refs, file contents per read rule, tokens, ticket keys,
TLS keys, snapshots) and the six boundaries — the network, the RPC auth
step, the read ticket and its walk, between tiers, the client machine,
the data directory. Every claim in that document names the test that
proves it; the checks sit at those six lines, and a reviewer's probes
should cross each one at least once.

## 3. What the internal attack passes already covered

The internal passes attacked each surface before it was called done.
The table names the surface, the attacks the tests now encode, what
they found, and the pull request that closed it — so a reviewer starts
past them. The standing list of classes, each with its defense and its
test, is [threat-model.md](threat-model.md), section 5.

| Surface | What the pass tried | What it found | Closed by |
| --- | --- | --- | --- |
| Auth and tokens | Every RPC without a token; a repo outside a principal's rules; one token standing for two principals | All answered as the model demands; the classes became standing tests | [#414](https://github.com/bnsd55/openscion/pull/414) and the invariant tests of [threat-model.md](threat-model.md), section 5 |
| Deny-only and partial reads | A principal whose rules deny prefixes; a partial reader's ticket, writes, history and directory views | A deny-only entry read through as allow; closed; the partial view is derived and ticket-minted on the view root | [#403](https://github.com/bnsd55/openscion/pull/403), and the `access.rs` tests |
| Tickets and object reads | A ticket serving beyond its walk; a forged or replayed ticket; object frames from a hostile peer | The ticket check and the walk hold on server and tiers; the frame decoder is fuzzed | the `tickets.rs` suite, and [fuzzing.md](fuzzing.md)'s `object` target |
| Secrets on disk | Symlinks, hard links, group-writable and ACL-amplified files behind `--tls-key`, `--ticket-key`, the access file, seeds | Files were read by path; now every checked file is read through one descriptor whose stat is the file that was read | [#354](https://github.com/bnsd55/openscion/pull/354), [#356](https://github.com/bnsd55/openscion/pull/356), [#360](https://github.com/bnsd55/openscion/pull/360), [#369](https://github.com/bnsd55/openscion/pull/369) |
| Secrets in argv | Every secret flag's exposure through `ps`; where the value can go instead | The argv form warns and goes away; every secret flag has a checked file form and an environment form | [#402](https://github.com/bnsd55/openscion/pull/402); the audit of the flags is in [operations.md](operations.md) |
| Writes | `PutBlob` and `CreateChangeset` with hostile bytes; caps on whole objects | The decoders fuzz; the changeset stream has bounded decode; the byte cap is `MAX_OBJECT_BYTES` | the `write.rs` tests and [fuzzing.md](fuzzing.md)'s `write-decode` target |
| Lands | Two lands sharing a request key; a replay of a landed land; a land whose call dies mid-call | The ref lock serializes moves; the audit chain (land intent, land result, ref line) now holds even when the call is cut | [#443](https://github.com/bnsd55/openscion/pull/443), and the `land_model.rs` reference model |
| Import | A raw git date git cannot write back; a commit or tree object past the byte cap decoded whole | Both refused now: the date round-trip gate and the header-read caps (in review) | [#473](https://github.com/bnsd55/openscion/pull/473) |
| Archives | A manifest naming one repo twice; folded names; colliding land keys; a replay dying halfway; a corrupted blob | `verify` refuses each before any write; the restore publishes by rename; the Postgres replay's empty-outcome check is fixed | [#438](https://github.com/bnsd55/openscion/pull/438), [#478](https://github.com/bnsd55/openscion/pull/478), the `archive_attacks.rs` suite |
| Floods and quotas | A refused-auth flood from one address rotating ports; upload and connection budgets | The meter folds one machine's refusals by routed block, so one host cannot bury a stranger | [#414](https://github.com/bnsd55/openscion/pull/414), the `quotas.rs` tests |
| The audit log itself | Who can read it; what a lost line means; a torn line at open | Mode 0600, fail-closed readiness, torn-line glue at open; the evidence-first rule (an intent must be durable before the change) | [#414](https://github.com/bnsd55/openscion/pull/414), [#443](https://github.com/bnsd55/openscion/pull/443) |

The lettered pass labels from the internal tracker map to these
surfaces; this table is organized by surface because that is how a
reviewer probes.

## 4. Ten minutes, locally

Build and start one server with one repo, on loopback, with a token:

```sh
git clone https://github.com/bnsd55/openscion && cd openscion
cargo build --release
export SCION_TOKEN=dev SCION_SERVER=http://127.0.0.1:7878
git clone --depth 1 https://github.com/tokio-rs/tokio /tmp/tokio
./target/release/scion-server --data /tmp/scion import /tmp/tokio
./target/release/scion-server --data /tmp/scion ticket-key --out /tmp/scion/ticket.key > /tmp/scion/ticket.pub
./target/release/scion-server --data /tmp/scion serve --ticket-key /tmp/scion/ticket.key &
```

Attack it: `scn` is the client (`./target/release/scn ls main:src` and
the rest of the README's [Try it](../README.md#try-it) steps run as
written); the gRPC surface is behind `SCION_TOKEN=dev` on port 7878;
the data directory `/tmp/scion` is SQLite plus blobs. The file-mode
rules refuse a directory other accounts can write — make one and watch
the open refuse it. The archive round trip is the fastest path to the
archive attacks: `scion-server --data /tmp/scion export --all --out
/tmp/arch` (the format is [archive-format.md](archive-format.md)),
`scion-server verify /tmp/arch`, then restore into a fresh directory
and compare with fsck.

The attack suites run without a server; each names its attacks in its
header:

```sh
cargo test -p open-scion-server --test access --test tickets \
  --test archive_attacks --test import_attacks --test quotas --test audit
```

Postgres lanes need a scratch instance:
`SCION_TEST_BACKEND=postgres SCION_TEST_POSTGRES_URL=postgres://...`.

## 5. The fuzz targets and their corpora

[fuzzing.md](fuzzing.md) maps every decoder that crosses a trust
boundary to its target, and says why the decoders without a target
have none. CI runs a 60-second smoke of every target on each push; the
nightly run gives each a longer budget and keeps its corpus
(`.github/workflows/fuzz.yml`). The targets live in
`crates/<crate>/fuzz/`, one file per target; a crash reproduces with
the input the run printed — [fuzzing.md](fuzzing.md), "Reproducing a
crash locally". Each fuzz workspace carries its own Cargo.lock, which
cargo-deny checks in CI (#487's lane).

## 6. Where we want the most eyes

The honest list, strongest first:

- **The partial-reader ref channel.** A landing on hidden paths moves
  `main`, and a partial reader's `ResolveRef` answers the new ID over
  an unchanged view: pinned as documented behavior, not closed
  ([threat-model.md](threat-model.md), section 6). A reviewer's
  judgment on whether the channel is acceptable at alpha is wanted.
- **The Postgres metadata layer.** The newest storage surface: the
  logical archive, the replay transaction, the schema migrations, the
  identity mint. The archive attacks cover the replay's outcomes; the
  SQL layer itself has had less adversarial attention.
- **The constant-time token compare.** The code is constant time; no
  test observes it, and the digest form of the access file is the main
  defence. An independent timing analysis would close the claim.
- **The audit drain path.** Single-writer, readiness latches
  fail-closed, and the result lines come from the store job that made
  the change; a reviewer who breaks the drain (a slow disk, a full
  queue) would find what we missed.
- **TLS revocation and hostname checks beyond the CA.** No test, no
  issue yet; a reviewer's recommendation on what alpha owes here is
  wanted.
- **The page cache and the mount's OS-level channels.** Out of reach
  of any test in this repository; named in the threat model so a
  reviewer does not mistake them for oversights.

Nothing in this packet is an exploit recipe: every named weakness is
either closed with a test or pinned as documented behavior in the
threat model.
