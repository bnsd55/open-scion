A release page for users: [docs/verify.md](docs/verify.md) walks the
  four checks a download needs before it runs — the SSH-signed tag
  (`git tag -v` with the owner's allowed-signers line), the checksums,
  the GitHub build attestation (`gh attestation verify --repo
  bnsd55/openscion`, the exact flag the pipeline runs), and the SBOM.
  The README's platform note links it.
