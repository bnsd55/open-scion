This fixes the Scorecard job's four failing checks. Three fixes and
two exceptions:

- The Dockerfile pins both base images by their multi-arch index
  digest, with the tag in a comment: rust:1.88-bookworm and
  debian:bookworm-slim, the same way the workflows pin actions.
- The three fuzz sub-workspace lockfiles (scion-proto, scion-server,
  scion-tier) pick up rustls-webpki 0.103.16, the fixed version for
  RUSTSEC-2026-0382; the root lockfile had it since #409, but
  Scorecard walks every lockfile and `cargo deny check` only reads the
  root workspace, so the fuzz locks kept the job red.
- The policy's header no longer claims every floor is met today:
  Maintained (the repo is younger than 90 days) and Branch-Protection
  (the check needs a token with admin read) were never met, and stand
  as dated exceptions now, with reasons and expiries.
