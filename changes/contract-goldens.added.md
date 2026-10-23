The three contract formats now carry the goldens that enforce their
additive rules: the store's byte pins and unknown-tag refusal
(crates/scion-core/tests/golden.rs), the protocol's wire golden and
unknown-field-ignore test (crates/scion-proto/tests/wire_golden.rs
with protocol_wire_v1.golden), and the archive's byte golden and
unknown-version refusal (crates/scion-server/tests/archive_golden.rs,
archive_attacks.rs). Each golden is paired with its format document's
version line: a golden change fails CI unless the same PR moves the
doc's version.
