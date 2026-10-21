# Verifying a release

You downloaded an openscion release (say v0.1.0) and want to know it is
the software this repository says it is, before you run it. This page
walks the four checks: the signed tag, the checksums, the build
attestations, and the SBOM. It names the exact commands the release
pipeline itself runs, so everything here works on any published
release.

What a release ships: one SSH-signed annotated git tag, and one
immutable GitHub release. The release carries, per platform cell
(`linux-x86_64` and `macos-aarch64`):

- four binaries: `scion-server`, `scion-cache`, `scion-daemon`, `scn`
- the source tarball `openscion-<version>-source.tar.gz`
- `checksums.txt`, a SHA-256 line per file above
- `release-environment.txt`, the record of how the build ran
  (runner image, `rustc`, `protoc`, `SOURCE_DATE_EPOCH`)
- one CycloneDX SBOM per shipped crate: `open-scion-server.cdx.json`,
  `open-scion-tier.cdx.json`, `open-scion.cdx.json`

Every one of these was built twice on different runners from the tag's
commit, and the build failed unless both copies agreed byte for byte.
The pipeline that did this is
[.github/workflows/release.yml](../.github/workflows/release.yml);
[slsa.md](slsa.md) says what each guarantee rests on.

## 1. The tag is the owner's

The tag names the commit that was built, and the owner signs it
personally with their SSH key. GitHub checked that signature against
the key on the owner's account when the tag was pushed; you can check
it again yourself.

Fetch the tag and pin the owner's key once. The key comes from the
owner's GitHub account over HTTPS, and the signer line binds it to the
owner's email — the same email the pipeline demands on the tag:

```sh
git clone https://github.com/bnsd55/openscion && cd openscion
git fetch origin tag v0.1.0

curl -s https://api.github.com/users/bnsd55.keys \
  | awk '{print "bnsd55@gmail.com", $1, $2}' \
  > ~/.ssh/openscion_allowed_signers
```

Then verify the signature with git, and that the tag points at the
commit `main` carries:

```sh
git -c gpg.format=ssh \
    -c gpg.ssh.allowedSignersFile="$HOME/.ssh/openscion_allowed_signers" \
  tag -v v0.1.0

test "$(git ls-remote origin 'refs/tags/v0.1.0^{}' | cut -f1)" \
  = "$(git rev-parse 'v0.1.0^{}')"
```

A good tag prints `Good "git" signature for bnsd55@gmail.com`. A tag
that fails the signature check, or a tag whose commit is not on
`main`, is not a release of this repository: stop here.

## 2. The checksums match the files

```sh
sha256sum -c checksums.txt          # Linux
shasum -a 256 -c checksums.txt      # macOS
```

Every payload file must answer `OK`. `release-environment.txt` has no
line in `checksums.txt` on purpose: it describes the build instead of
being part of it, and the attestations below cover what ships.

## 3. Each artifact carries a build attestation

Each binary, the source tarball and `checksums.txt` carry a GitHub
artifact attestation: a signed statement from GitHub's hosted builder
that this repository's workflow produced exactly this file (this
digest) at this run. The pipeline itself verifies every artifact the
same way before it publishes, so the commands below are the ones the
release run already proved.

```sh
gh attestation verify scion-server --repo bnsd55/openscion
gh attestation verify scion-cache   --repo bnsd55/openscion
gh attestation verify scion-daemon  --repo bnsd55/openscion
gh attestation verify scn           --repo bnsd55/openscion
gh attestation verify openscion-<version>-source.tar.gz --repo bnsd55/openscion
gh attestation verify checksums.txt --repo bnsd55/openscion
```

Each must end in `Verified, ... attestations were verified`. This needs
the `gh` CLI, installed and signed in (`gh auth login`). `--owner
bnsd55` is the equivalent form; the examples use `--repo` because that
is the exact flag the release pipeline runs.

The `*.cdx.json` SBOM files are not attested as subjects — they are
the payloads of the SBOM attestations. You verify an SBOM through the
artifacts it describes, which is the next step.

## 4. The SBOM names what is inside the binaries

Each `.cdx.json` file lists the dependencies of one shipped crate. An
attestation of its own binds the SBOM to the binaries it describes,
produced by the same run that built them — so a tampered SBOM fails
the check on the binary it belongs to. The practical upshot: the
checks in step 3 are what bind each SBOM to the binaries; reading the
SBOM tells you what those binaries carry. `cargo cyclonedx` made them;
any CycloneDX reader can list the contents.

## If a check fails

Do not run the binary. The release is immutable, so nobody can replace
its files in place — a mismatch means either a broken download (fetch
the file again and compare) or something worth reporting. Report
privately, not as a public issue: [SECURITY.md](../SECURITY.md) says
how.

## Building from source instead

Everything above checks what the pipeline built. If you build from the
tag yourself (`git checkout v0.1.0`, then the README's build steps),
reproducibility is the check that applies: the pipeline's two builds
of one tag agreed byte for byte, and a build with the same tools and
flags is expected to match the release's binaries exactly.
