# Fuzzing

Every place the code decodes input that crossed a trust boundary has a fuzz
target: bytes from the network, from a file an operator placed, or from a
client round trip. A target's property is the same everywhere — any input
ends in a value or an error, never a panic, and what a decoder accepts
round-trips against its encoder where one exists.

The targets live beside their crates, in `crates/<crate>/fuzz/`, one file per
target under `fuzz_targets/`. Each crate's fuzz build has its own lockfile and
is outside the main workspace. CI runs a 60-second smoke of every target on
each push (`.github/workflows/ci.yml`); the nightly run gives each target a
longer budget and keeps its corpus between runs (`.github/workflows/fuzz.yml`).

## The decoder map

### Bytes from the network

| decoder | target |
| --- | --- |
| changeset and tree codecs (`scion-core/src/codec.rs`, `Changeset::decode`, `Tree::decode`) | `scion-core/fuzz: decode` |
| repo paths from requests (`scion-core/src/path.rs`, `RepoPath::new`, `PathElem::new`; the server wraps them in `service.rs:parse_path`) | `scion-core/fuzz: repo-path` |
| object frames (header, parts, zstd; `scion-proto/src/runtime/object.rs`, `object::decode`, `frame_size`, `Assembler`) | `scion-proto/fuzz: object` |
| tickets (`scion-proto/src/ticket.rs`, `parse`, `parse_unverified`) | `scion-proto/fuzz: ticket` |
| repo, ref and request-key names (`scion-proto/src/runtime/check.rs`, `check::name`) | `scion-proto/fuzz: check-name` |
| request IDs from headers (`scion-proto/src/runtime/request_id.rs`, `sanitize`) | `scion-proto/fuzz: request-id` |
| changesets a client sent (`scion-server/src/write.rs`, `write::decode`) | `scion-server/fuzz: write-decode` |
| blame cursors a client carried back (`scion-server/src/blame.rs`, `decode_cursor`, `decode_window`) | `scion-server/fuzz: blame-cursor` |

### Bytes from a file

| decoder | target |
| --- | --- |
| the access file (`scion-server/src/access.rs`, `Access::parse`) | `scion-server/fuzz: access` |
| the archive files (`scion-server/src/archive.rs`, `verify`/`restore`: the manifest and its names, `changesets.bin`, `refs.json`, `lands.json`, `members.txt`, `provenance.json`, the blob files) | `scion-server/fuzz: archive-verify` |
| each archive section reader fed raw bytes (`scion-server/src/archive.rs`, `read_changesets`, `read_refs`, `read_lands`, `read_members`, `read_provenance`) | `scion-server/fuzz: archive-read` |
| the tier snapshot seed file (`scion-tier/src/snapshot.rs`, `seed`; an operator drops the file beside a cold store) | `scion-tier/fuzz: snapshot-seed` |

### No target

| decoder | why not |
| --- | --- |
| store-value decoders (`history.rs` `decode_node`, `decode_index`, `decode_row`; `graph.rs` `CommitGraph::decode`; the `derived.rs` value decodes; `tree.rs` `decode`; `fsck.rs`; `codec.rs` `Tree::find_entry`, which scans stored bodies after their ID check and stops at the name it finds — a reader, not a validator) | the bytes are always server-written: the same process encodes before it stores and decodes after it reads. Not a trust boundary. The blame cursor is the exception, because its bytes travel through the client, so it has one. |
| a land's conflict-path list (`scion-server/src/land.rs`, `decode_paths`: the `conflict_paths` blob of a lands-table row) | server-written, like the store-value decoders above. |
| tier store records (`scion-tier/src/store.rs`, `parse_record`, `Key::from_bytes`) | fixed-width records from the server's own store file. |
| ticket key config (`scion-server/src/tickets.rs`, `from_hex`) | operator config, fixed 32-byte hex. |
| key and token files read as hex (`scion-server/src/main.rs`, `read_hex`) | operator config, hex text; the server refuses a malformed file instead of parsing on. |
| land request names (`scion-server/src/land.rs`, `check_name`) | a length check only, no parse state; the property the `check-name` target asserts covers the shape. |
| scn command-line parsing (`parse_repo_path`, `parse_dir`, `parse_put`) | local operator arguments, not bytes from the network or a file. |
| NFS (`scn mount`) | the wire decode is `nfsserve` upstream's space; the object bytes scn's vfs reads go through `object::decode`, which the `object` target covers. |
| gRPC and protobuf | tonic and prost upstream's space; the `object` target notes where our decoder takes over from prost's. |
| scn's client-side decodes of server-sent objects (`scn/src/cmd/checkout.rs` `decode_tree`, `scn/src/cmd/read.rs` `decode_changeset`) | they wrap the fuzzed codec with an equality check: the object must decode and hash to the ID it was sent for. The codec target covers the decode. |
| scn's own state files (`scn/src/cmd/mount.rs` `read_sessions`, `scn/src/vfs.rs` the note JSON) | written by scn itself, like the store-value decoders; not bytes another party crafts. |

## Reproducing a crash locally

A nightly CI run that fails uploads the crashing input as an artifact
(`crash-<target>`); download it, or find one in
`crates/<crate>/fuzz/artifacts/<target>/` from a local run. Then, from
`crates/<crate>`:

```
cargo fuzz run --target x86_64-unknown-linux-gnu <target> path/to/crashing-input
```

cargo-fuzz builds the target on nightly and runs it once on that input, under
the address sanitizer. A crash reproduces with the same panic the CI log
showed; a fix lands with the input's bytes folded into the target's
`fuzz/seeds/<target>/` directory, so the corpus keeps the case forever.
