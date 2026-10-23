The supply-chain job now runs cargo-deny in every fuzz
  sub-workspace, each of which holds its own Cargo.lock the root check
  never read — the gap that let RUSTSEC-2026-0382 sit in three fuzz
  lockfiles while the root stayed clean. Each fuzz workspace carries
  its own `deny.toml`: the same advisories floor, the license list the
  harness alone needs, and looser bans where a fuzz workspace differs
  from the product by design. A new script,
  `scripts/check-deny-coverage.sh`, fails the job when a Cargo.lock
  sits where no deny step reads it, so a new sub-workspace cannot slip
  past either.
