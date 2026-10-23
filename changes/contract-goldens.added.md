The three contract formats now carry the goldens that enforce their
additive rules: the store's byte pins and unknown-tag refusal
(crates/scion-core/tests/golden.rs), the protocol's wire golden and
unknown-field-ignore test (crates/scion-proto/tests/wire_golden.rs
with protocol_wire_v1.golden), and the archive's byte golden and
unknown-version refusal (crates/scion-server/tests/archive_attacks.rs
against archive_v1.golden). The pairing is enforced on the pull
request's own diff: scripts/check-golden-pairing.sh (the CI lint job).
A pinned-byte change (the wire golden, the store pins, the archive
golden) fails without its format document's version line in the same
pull request. The proto file and the surface golden are additive: they
need no new version, only the same pull request changing
docs/protocol-v1.md (any line - the contract must describe the new
surface), with its own message.
