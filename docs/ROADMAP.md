# Roadmap

The order of the work, as the owner decided it (D7). Each milestone has
the work that ends it; a milestone is done when its gate passes on every
supported platform, and nothing shortcuts one. Planned work the roadmap
does not name is tracked in
[issues](https://github.com/bnsd55/openscion/issues).

## v0.1.0 - the first release

The source release that every later one is cut from. Its steps live in
[RELEASING.md](RELEASING.md): one SSH-signed annotated tag, one
immutable GitHub release, nothing published anywhere else.

## v0.1.1 - distribution and portability

Make the project safe to install anywhere it claims to work.

- A clean install and the [quick start](../README.md#try-it) pass on
  every supported platform (Linux and macOS), under umask 002 and umask
  022: done, [#317](https://github.com/bnsd55/openscion/issues/317)
  landed the release smoke, and its CI job runs on every push, in a
  matrix over ubuntu-24.04 and macos-15, since [#326](https://github.com/bnsd55/openscion/pull/326).
- Known flaky tests, named until they are fixed, the gate closes when
  the nightly confirms their fixes: the land-cut audit test's call
  counting (fixed: the test runs rounds of 8 until one cuts a call, 10
  at most, and every round's calls are held to the promise,
  [#577](https://github.com/bnsd55/openscion/pull/577)); the
  graph-query cap test's two 10 s liveness bounds, too tight for the
  arm64 job's debug build (the bounds move to 120 s, #556); the
  land-model cost the arm64 run exposes; and the admin-audit seed's
  server.lock, which can lag its drop by a tick when the spawned CLI
  opens at once (the seed now waits for the real lock-free condition,
  #559). Nothing watches the full suite on a schedule — only the
  nightly soak and the nightly version matrix run on a clock; the
  suite runs per push and per pull request. The earlier suspected
  flake, scn's `failed printing to stdout: Broken pipe` in the release
  smoke, is confirmed fixed: [#341](https://github.com/bnsd55/openscion/pull/341)
  landed the fix, and none of the 57 CI runs on `main` from its merge
  to 2026-10-21 shows a `Broken pipe` in the release smoke.
- [Ubuntu's umask 002 and user private groups trip the group-writable
  checks](https://github.com/bnsd55/openscion/issues/302): closed by
  choice - the rule stays fail-closed, the refusal names the chmod
  that fixes the directory, and the runbook shows how to make the
  directory so the rule never fires.

## v0.2.0 - compatibility and recovery

Make the pieces trustworthy together, and the data safe to lose.

- Clients, servers, caches and daemons of version N and N-1 work
  together, per a written compatibility policy: done,
  [#329](https://github.com/bnsd55/openscion/issues/329) landed
  [compatibility.md](compatibility.md), and
  [#336](https://github.com/bnsd55/openscion/issues/336) and
  [#347](https://github.com/bnsd55/openscion/issues/347) landed the
  version-matrix job that runs the policy's pairs.
- A store written by an old build opens and upgrades: done,
  [#318](https://github.com/bnsd55/openscion/issues/318) closed through
  [#323](https://github.com/bnsd55/openscion/pull/323), which builds a
  version-1 store of each backend in place and serves from it after the
  open migrates it.
- A backup of the data directory, and of a Postgres-backed store,
  restores and serves: done,
  [#319](https://github.com/bnsd55/openscion/issues/319) closed through
  [#322](https://github.com/bnsd55/openscion/pull/322) — the runbook of
  [operations](operations.md) for both backends, and a test that
  restores a snapshot taken while the server serves.
- `ticket-key` rotation runs while reads serve: done,
  [#242](https://github.com/bnsd55/openscion/issues/242) landed the
  end-to-end test of the steps in [operations](operations.md#2-ticket-keys).
- The version matrix runs the client, server, cache, daemon and store
  pairs the policy claims, and the operating metrics it reports are
  named: done,
  [#239](https://github.com/bnsd55/openscion/issues/239) closed with
  the matrix green on `main`, the soak running nightly
  ([#320](https://github.com/bnsd55/openscion/issues/320); its last
  scheduled run failed on the RSS warm-up before
  [#365](https://github.com/bnsd55/openscion/pull/365) landed the fix,
  and the first scheduled run after it confirms it), and the last
  missing operating metric (gc's) a textfile
  ([#327](https://github.com/bnsd55/openscion/issues/327)).
- The fault and recovery matrix: disk full, EIO, a Postgres restart,
  mismatched restores: [#238](https://github.com/bnsd55/openscion/issues/238)
  closed with a row per case in [faults](faults.md), each naming its
  test. The Postgres rows reopen: until #412 their evidence ran on
  SQLite, and the Postgres fault cells are best effort until their
  scheduled runs carry them.

## v0.3.0 - a single-node operational beta

Prove the one-machine server under real load.

- A 72-hour soak and fault job runs against one server, with resource
  ceilings named in advance, and its raw numbers published in
  [benchmarks](benchmarks.md): open. The 12-hour fault soak (Run B)
  ran and passed; its numbers are in
  [benchmarks](benchmarks.md#fault-soak-run-b). The 72-hour soak (Run
  A) runs on an owner machine since 2026-10-21, and its report comes
  on [#399](https://github.com/bnsd55/openscion/issues/399); the run
  counts only for the commit it was launched from — it is not evidence
  for a later tag. A
  120-minute
  nightly soak ([#320](https://github.com/bnsd55/openscion/issues/320))
  runs scheduled on `main`; its RSS warm-up failure was fixed in
  [#365](https://github.com/bnsd55/openscion/pull/365), and the
  scheduled run of 2026-10-21 passed.
- [The checkout benchmark stands against `git clone`](https://github.com/bnsd55/openscion/issues/6):
  done, the comparison and its numbers are in
  [benchmarks](benchmarks.md).
- The capacity harness measures what it claims — the reader axis, the
  failure counters, the throughput and the store-growth columns all
  read true — before the 72-hour soak's numbers stand on it: moved
  into this gate, ahead of the soak, [#461](https://github.com/bnsd55/openscion/issues/461).

## v0.4.0 - the production boundary

Say exactly what v1.0 will support.

- One active `scion-server` is the production topology. SQLite holds
  metadata: it is the only production backend of this gate. A named
  Postgres major holds metadata too, but Postgres is experimental —
  tested in CI, not relied on for data-safety evidence — and its
  promotion to production moves after v0.5's durability evidence. One
  blob store holds content. Cache tiers may sit in front.
- The server host, its administrators and the data directories are
  trusted. Network peers, tokens and principals are not.
- A shared data directory, a server data directory on a network
  filesystem, `--insecure-listen`, an untrusted local account and
  `scn mount` on a multi-user host are outside the production profile.
  The mount v1 ships is read-only: the writable mount is a build
  feature, off in release builds, and out until v1.0.0 passes (D24).
- `docs/threat-model.md` names the assets, attackers, trust boundaries
  and security invariants. Each invariant names its test.
- `docs/support.md` defines severity, backports and end of support. The
  current minor gets bug, security and data-loss fixes. The previous
  minor gets critical and high security and data-loss fixes.
- Credential handling passes end to end. No production secret needs to
  appear in argv.
- The project holds the OpenSSF Best Practices passing badge. A pinned
  OpenSSF Scorecard policy runs on `main`.
- `docs/fuzzing.md` maps every untrusted decoder and object walk to a
  nightly target with a kept corpus and a named time budget.

## v0.5.0 - durability and recovery

Prove that accepted data survives the failures the production profile
names.

- The fault and recovery matrix passes for every production metadata
  backend.
- Crash, power-loss, disk-full, EIO, Postgres-restart, cancellation,
  retry and garbage-collection tests compare the result with an
  independent operation ledger, then run `fsck`.
- A reference model checks concurrent lands, retries and restarts.
- A live backup and restore is consistent across metadata and blobs.
- The current release restores a backup made by N-1. An upgrade drill
  restores the pre-upgrade backup and serves it with the N-1 binary.
- `docs/operations.md` states the durability point, the tested backup
  RPO and the measured restore time.
- A confirmed severity-1 data-loss or corruption defect blocks the
  release.

## v0.6.0 - the 1.x contracts

Freeze the surfaces users need, not the implementation.

- `docs/stability.md` names the 1.x promises for object format v1,
  protocol v1, CLI `--json`, exit codes, production configuration and
  schemas.
- Object format v1 never changes. Protocol v1 remains served throughout
  1.x. CLI JSON and production configuration change additively.
- Tickets and snapshots follow the N and N-1 mixed-version window.
- The version matrix runs the supported client, server, cache, daemon
  and store pairs.
- A store written by the newest 8 tagged releases opens and upgrades
  (an older tag is named in a warning, not built). Rollback restores the
  pre-upgrade backup.
- A full repository exports to a versioned logical archive. CI verifies
  and restores the archive.
- Postgres 17 is promoted to production (D23) when all of it holds:
  the contract test ([#560](https://github.com/bnsd55/openscion/issues/560))
  passes on Postgres 17, the real Postgres fault rows pass, backup and
  restore pass on Postgres 17, and the release-candidate trial runs on
  Postgres 17 as well as on SQLite. The durability evidence of v0.5 is
  the floor this stands on. The per-requirement gap list with the
  evidence behind each verdict is `docs/pg17-qualification.md`.
- No crate publishes to crates.io at v1.0 (D12). A narrow client crate
  may publish when a real outside consumer exists. The server, tier and
  CLI internals stay private, and a machine check watches the facade
  that a later publish would freeze.

## v0.7.0 - operations

Make one server routine to run.

- `docs/operations.md` has runbooks for install, upgrade, rollback,
  backup, restore, key and certificate rotation, disk full, EIO, a
  Postgres restart, a failed migration, an `fsck` failure and garbage
  collection: done - the disk-full and failing-disk runbooks
  ([#448](https://github.com/bnsd55/openscion/pull/448)), the
  Postgres-restart runbook
  ([#453](https://github.com/bnsd55/openscion/pull/453)), the
  failed-migration and fsck-failure runbooks
  ([#454](https://github.com/bnsd55/openscion/pull/454)), and the
  first-boot (the install runbook) and certificate-rotation runbooks
  ([#457](https://github.com/bnsd55/openscion/pull/457)).
- Each runbook has a CI test or a recorded release drill: done, with
  each runbook's `Proof:` line held by the docs_ops registry, except
  one arm: rollback's run-the-old-binary arm, whose drill is #431 (the
  pre-upgrade backup restores and serves under the N-1 binary). The
  restore arm is proven
  ([#454](https://github.com/bnsd55/openscion/pull/454)'s
  `a_newer_schema_refuses_the_open_and_a_restore_serves`).
- Readiness, liveness and graceful drain have end-to-end tests.
  Readiness goes false before the server drains in-flight calls.
- Prometheus alert rules and the log fields they use are versioned and
  checked.
- An audit log records authentication, writes, ref changes and
  administrative changes without tokens or file contents.
- The operating metrics publish the supported capacity envelope, tail
  latency, memory, disk growth and recovery time on a named workload
  and machine.
- The documentation says that one active server has no automatic
  failover.

## v0.8.0 - releases and project continuity

Make releases repeatable without one person's machine or account.

- A component, OS, architecture, filesystem and Postgres support matrix
  names one native CI job for every supported cell.
- CI builds every release artifact from the protected tag.
- The release smoke passes natively on every supported cell under umask
  002 and 022.
- The unsigned release payload rebuilds byte for byte.
- Each artifact has a checksum, an SPDX or CycloneDX SBOM and
  verifiable provenance. From this release, the binaries and SBOMs get
  GitHub artifact attestations (D18). The recorded release workflow is
  assessed against SLSA Build L3, as [docs/slsa.md](slsa.md)
  records.
- Production code has at least 80 percent statement coverage, measured
  with a pinned tool, with the exclusions checked in (D15).
- Required tests have no hidden retry. The scheduled full suite passes
  30 consecutive runs, with no open flaky-test issue.
- The project holds the OpenSSF Best Practices silver badge.
- `GOVERNANCE.md` names the roles and decision rules. Protected
  branches need an independent human review and all required checks.
- Two maintainers can accept a change, handle a private report and cut
  a release without one person's account, machine or private key.

## v0.9.0 - release candidate

Run the finished product before naming it stable.

- Features and formats freeze at the first release candidate.
- An outside security review covers the server, tiers, CLI and
  supported deployment. No critical or high finding stays open. A
  medium finding needs a mitigation, an owner and an expiry.
- The public review summary names the scope, reviewer, date, finding
  counts and disposition.
- One release candidate runs the production trial for 30 days on every
  metadata backend advertised as production.
- `docs/production-trial.md` records the commit, binaries, backend,
  filesystem, workload, counts, upgrades, restores, key and certificate
  rotations and incidents.
- A second operator follows the install, upgrade, rollback and restore
  runbooks without help from their author.
- The full release procedure runs from a clean tag. No release-blocking
  flake, security finding, data-loss defect or corruption defect stays
  open.

## v1.0.0 - single-server production

Cut the tested release. Add no new surface.

- Findings from v0.9.0 are closed. A medium-or-lower security finding
  may carry a written risk acceptance with an owner and expiry.
- v1.0.0 differs from the final release candidate only in version,
  changelog and release metadata. The release smoke runs again.
- `README.md`, `SECURITY.md`, `docs/stability.md`,
  `docs/compatibility.md`, `docs/operations.md` and
  `docs/RELEASING.md` state the supported deployment, support window
  and 1.x promises. The supported deployment names SQLite and Postgres
  17 (D23): one server each, per the production topology.
- The release notes link the gate evidence.

## Out until v1.0.0 passes

Work the gates above do not earn yet. Issues stay open; none starts
before v1.0.0 passes.

- [A horizontally scalable backend](https://github.com/bnsd55/openscion/issues/47):
  more than one server process sharing data, per region and per node.
  v1.0 promises one active server and no automatic failover (D11).
- [History and writes for a principal with folder read
  rules](https://github.com/bnsd55/openscion/issues/183).
- Blame as derived data.
- The writable `scn` mount: v1's mount is read-only, and the overlay
  write and `commit --from-mount` are a build feature, off in release
  builds, that returns only after a handle-walking rewrite passes the
  hostile tests (D24).
- A virtual checkout safe from other local users: the server trusts
  every local account that can read its data directory, so this is a
  different storage and virtual-checkout design, not a hardening of
  this one.
- Protection from an untrusted local account on the server host.
