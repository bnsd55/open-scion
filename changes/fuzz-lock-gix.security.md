The server's fuzz lockfile no longer pins the vulnerable gix
(RUSTSEC-2026-0382): `cargo update -p gix` moved the gix family in
`crates/scion-server/fuzz/Cargo.lock` to 0.90.0 (#587 moved only the
root lock). A fuzz lockfile that no longer matches its manifest now
fails the deps job (`cargo metadata --locked` per fuzz sub-workspace),
where a stale pin previously re-resolved in place and failed nothing,
and Dependabot watches each fuzz sub-workspace, so the next advisory
in one cannot sit unseen (#572 rule: every fault row names its guard).
