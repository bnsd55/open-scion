# The Scion Archive Format (`scion-archive-1`)

A scion archive is a directory. It holds one repository or a whole store
as plain files: JSON metadata, a length-prefixed changeset stream, and
the store's blob files byte for byte. A reader in any language can
verify and restore an archive with three abilities only: read files,
parse JSON, and decompress zstd frames. Verification needs the blake3
hash, which the IDs themselves are.

This document is the contract. `verify` checks an archive against THIS
document, not against the store's code.

## The version rule

An archive's `manifest.json` names `"format": "scion-archive"` and a
`"version"`. Version 1 is this document. A reader refuses an archive
whose format or version it does not know: `scion-archive-1` readers
accept version 1 only. A future version that breaks the rules below
MUST carry a new version number, and SHOULD carry a new top-level
directory name (`scion-archive-2`), so the two formats never mix.

## The layout

```
<archive>/
  manifest.json
  blobs/<2 hex>/<62 hex>
  repos/<name>/
    changesets.bin
    refs.json
    lands.json
    members.txt
    provenance.json
```

A repo-kind archive holds one directory under `repos/`. A store-kind
archive holds every repo of the store.

## manifest.json

Pretty-printed JSON, UTF-8, with these keys:

| key             | type   | meaning                                                     |
|-----------------|--------|-------------------------------------------------------------|
| `format`        | string | always `"scion-archive"`                                    |
| `version`       | number | `1`                                                         |
| `kind`          | string | `"repo"` or `"store"`                                       |
| `store_uuid`    | string | 32 lowercase hex digits, the source store's UUID; `null` if the source had none |
| `snapshot_at`   | string | RFC 3339 UTC stamp, to the second, of the metadata read     |
| `snapshot_note` | string | what one snapshot means, for a human reader                 |
| `counts`        | object | `repos`, `changesets`, `refs`, `pending_lands`, `blobs` — all numbers, totals over the archive |
| `repos`         | array  | one object per repo: `name` plus the same five counts for it |

The manifest is written LAST. An archive without a manifest is not an
archive, and a reader refuses it. Every repo named in `manifest.json`
MUST have a directory under `repos/`.

The metadata sections are ONE read snapshot of the store: a reader may
assume that every ref, parent, and land input named in the archive is
backed by a changeset the same archive holds. The blob files are
copied after the snapshot; this is safe because a blob file is
immutable and the store's `gc` keeps a blob past its grace period.

## The blob files

`blobs/<hex[0..2]>/<hex[2..]>`, one file per blob, named by the blob's
ID: the 64 lowercase hex digits of the blake3 hash of the blob's RAW
(uncompressed) bytes. The file holds the blob's bytes in the zstd
framing the store writes, which a reader passes to zstd streaming
decompression. The first two hex digits of the name are the directory,
so an archive holds at most 256 files per directory.

Verification of a blob file: decompress it, hash the raw bytes with
blake3, and the hex of the hash MUST equal the file's name.

## changesets.bin

One record per changeset, back to back, in generation order (a parent
always carries a smaller generation than its children, so the order is
topological). A record, little-endian throughout:

| offset | size | field                                                       |
|--------|------|-------------------------------------------------------------|
| 0      | 4    | `body_len` — u32, the length of the encoded changeset body  |
| 4      | 32   | `id` — the changeset ID: the blake3 of the body, 32 raw bytes |
| 36     | 8    | `generation` — i64, the store's derived generation, at least 1 |
| 44     | `body_len` | the encoded changeset body (scion-core codec)          |

Verification of a record: `id` MUST be the blake3 of the `body_len`
bytes that follow it, the body MUST decode as a scion-core changeset,
and `generation` MUST be at least 1.

## refs.json

Pretty-printed JSON array, one row per ref, in name order:

```json
[ { "name": "main", "target": "<64 hex>", "version": 42 } ]
```

`target` is the 64 lowercase hex digits of a changeset ID that the
archive's `changesets.bin` MUST hold. `version` is the store's
compare-and-swap version of the ref at the snapshot.

## lands.json

Pretty-printed JSON array, one row per land that was still `pending`
at the snapshot, in land-ID order:

```json
[ { "caller": "ci-bot",
    "request_key": "land-2024-17",
    "target_ref": "main",
    "input_cs": "<64 hex>" } ]
```

`input_cs` is the 64 lowercase hex digits of a changeset ID that the
archive's `changesets.bin` MUST hold. A restore records these lands
again, so a client retry with the same `request_key` gets the same
land and never lands twice.

## members.txt

One blob ID per line: 64 lowercase hex digits, the members (blobs) of
the repo. Empty lines are ignored. Every named blob MUST have a file
under `blobs/` with that name.

## provenance.json

Pretty-printed JSON array, one row per git commit the store imported
into the repo:

```json
[ { "oid": "<40 hex>", "cs": "<64 hex>" } ]
```

`oid` is the SHA-1 of the git commit; `cs` is the 64 lowercase hex
digits of the changeset it became, which the archive's
`changesets.bin` MUST hold.

## What an archive deliberately does not hold

- **Access rules, tokens, ticket keys**: restore configures them from
  the operator's files, not from the archive.
- **The tree bodies**: a changeset names its tree, and a restore
  rebuilds every tree from the changeset bodies and the blob files,
  through the store's own write path.
- **Derived state** (path history, view caches): rebuilt lazily.
- **A point-in-time lock on the store**: an export runs beside a live
  server and is NOT an atomic snapshot of the blob files; the manifest
  says so in `snapshot_note`.

## Verify, as this document defines it

`verify` reads only the archive:

1. `manifest.json` exists, names the format and version the reader
   knows, and every repo it names has a directory — a plain one, not a
   symlink, and no two repos' directories one and the same: on a disk
   that folds names (case, or Unicode normalization), two names can be
   one directory, and both the export and the verify refuse the fold
   instead of replaying one repo's data under two names.
2. Every changeset record re-hashes to its ID and decodes, and every
   generation is at least 1.
3. Every changeset parent, ref target, land input, and provenance
   changeset is a changeset the archive holds.
4. Every member blob file exists, decompresses, and re-hashes to its
   name.

One line per damage, and an empty list when the archive is sound.
