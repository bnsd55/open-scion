# Roadmap

The order of the work, as the owner decided it (D7). Each milestone has
the work that ends it; a milestone is done when its gate passes on every
supported platform, and nothing shortcuts one. Planned work the roadmap
does not name is tracked in
[issues](https://github.com/bnsd55/openscion/issues).

## v0.1.1 - distribution and portability

Make the project safe to install anywhere it claims to work.

- A clean install and the [quick start](../README.md#try-it) pass on every
  supported platform (Linux and macOS), under umask 002 and umask 022,
  per [the release smoke](https://github.com/bnsd55/openscion/issues/317).
- No known flaky test.
- [Ubuntu's umask 002 and user private groups trip the group-writable
  checks](https://github.com/bnsd55/openscion/issues/302).
- [Publish the crates to crates.io](https://github.com/bnsd55/openscion/issues/21).

## v0.2.0 - compatibility and recovery

Make the pieces trustworthy together, and the data safe to lose.

- Clients, servers, caches and daemons of version N and N-1 work
  together, per a written compatibility policy.
- [A store written by an old build opens and
  upgrades](https://github.com/bnsd55/openscion/issues/318).
- [A backup of the data directory, and of a Postgres-backed store,
  restores and serves](https://github.com/bnsd55/openscion/issues/319).
- `ticket-key` rotation runs while reads serve: done, [#242](https://github.com/bnsd55/openscion/issues/242)
  landed the end-to-end test of the steps in [operations](operations.md#2-ticket-keys).
- [The version matrix and the missing operating
  metrics](https://github.com/bnsd55/openscion/issues/239), for the
  pairs the policy claims.
- [The fault and recovery matrix: disk full, EIO, a Postgres restart,
  mismatched restores](https://github.com/bnsd55/openscion/issues/238).

## v0.3.0 - a single-node operational beta

Prove the one-machine server under real load.

- A 72-hour soak and fault job runs against one server, with resource
  ceilings named in advance, and its raw numbers published in
  [benchmarks](benchmarks.md); the soak workload and the metrics it
  reports are [the other half of #239](https://github.com/bnsd55/openscion/issues/239).
- [The checkout benchmark stands against `git clone`](https://github.com/bnsd55/openscion/issues/6)
  and the numbers say where the line is.

## Out until those pass

Work the gates above do not earn yet. Issues stay open; none starts
before v0.3.0 passes.

- [A horizontally scalable backend](https://github.com/bnsd55/openscion/issues/47):
  more than one server process sharing data, per region and per node.
- New RPCs, including [history and writes for a principal with folder
  read rules](https://github.com/bnsd55/openscion/issues/183).
- Blame as derived data.
- More writable-mount features.
