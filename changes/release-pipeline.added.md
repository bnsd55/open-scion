The release pipeline ([.github/workflows/release.yml](.github/workflows/
release.yml)): on a pushed `v*` tag it verifies the tag's SSH signature
through GitHub's API (the workflow holds no key), builds every artifact
— the four binaries, the source tarball, an SBOM per shipped binary's
crate — twice per cell from `.github/release-cells.json`, fails on one
differing byte between the two builds, attests each artifact and SBOM
with GitHub's build-provenance and SBOM attestations, verifies every
attestation, and publishes one immutable release whose notes come from
the tagged `CHANGELOG.md`. A `workflow_dispatch` dry-run runs the whole
pipeline but publishes nothing, so it is provable before the first tag.
[docs/slsa.md](docs/slsa.md) is the honest Build L3 assessment;
[docs/RELEASING.md](docs/RELEASING.md) lists the repository settings
only the owner can turn on.
