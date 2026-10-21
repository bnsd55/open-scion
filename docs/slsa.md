# SLSA Build L3 — an honest assessment

This document says where this project stands against the SLSA Build
track (v1.2), and what each requirement actually rests on. The release
pipeline it refers to is [.github/workflows/release.yml](../.github/
workflows/release.yml); the roadmap names SLSA L3 as a v0.8.0 gate
([ROADMAP.md](ROADMAP.md)).

## What the pipeline does

A `v*` tag (annotated, SSH-signed) starts the release workflow. It
verifies the tag through GitHub's API — the tag's SSH signature was
checked with the key on the owner's account at push time, so the
workflow holds no key — then builds every artifact from the tagged
commit twice per cell, on different runners, and fails on one differing
byte. Each artifact, its source tarball, its checksums file and the
SBOM of each shipped binary's crate get a GitHub artifact attestation:
a DSSE envelope signed by the GitHub-hosted builder, bound to the
subject's digest. The release step verifies every attestation against
this repository, then publishes one immutable release. Nothing goes to
crates.io (D12).

## The requirements, one by one

The table is the SLSA v1.2 build requirements. For each: what it means
here, and what it rests on.

| Requirement | Status | What it rests on |
|---|---|---|
| Build service | MET | GitHub Actions is the build service; the provenance is `actions/attest-build-provenance` v4.2.2 (pinned by commit SHA), signed as DSSE by the hosted builder |
| Provenance exists | MET | One attestation per artifact: binaries, source tarball, checksums.txt, and an `actions/attest-sbom` v4.1.0 attestation binding each SBOM to the binaries it describes |
| Provenance content | MET | Each attestation records the repository, the run, the workflow path and the subject digest; the built tree is pinned to the tag's commit by `ref:` on checkout |
| Source identity | MET | The tag must be verified (`verified=true`, `reason=valid`), must carry the owner's tagger email, and its commit must be an ancestor of `main` |
| Isolated build | MET | Each build job checks out only the tag's commit; no secret is visible to a build step (the token is contents:read except in the publish step); the rebuild pair runs on separate runners |
| Non-falsifiable provenance | MET | The attestations are signed by the GitHub-hosted builder's identity, not by anything the workflow computes; `gh attestation verify --repo bnsd55/openscion` checks the chain |

## The gaps, in words

- The second pair of eyes: the v0.8.0 gate and SLSA's hardened-builder
  story both want the pipeline reviewed by someone other than the
  author. The second-maintainer drill is scheduled for v0.8.0; until it
  has run, read "MET" as "met, single-maintainer".
- The repository rules that make source identity enforceable — a
  ruleset on `refs/tags/v*` (deletion denied, creation restricted to
  the owner) and repository-wide immutable releases — are settings
  only the owner can turn on. They are on the checklist in
  [RELEASING.md](RELEASING.md). Until they exist, source identity
  rests on the workflow's own checks, which a maintainer could bypass.
- `release-environment.txt` travels with the artifacts and is hashed,
  but it is not a SLSA "resolved dependencies" record. Reproducibility
  (#390) is the compensating control: two runners agreeing byte for
  byte proves the environment is described well enough to rebuild.

## How to check any of this

`gh attestation verify <file> --repo bnsd55/openscion` against any
release asset prints the DSSE envelope and the subject digest. The
workflow's dry-run does the same on a branch without publishing: its
`verify` job's log is the evidence that the pipeline is real before
the first tag.
