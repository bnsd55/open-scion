# Design: big folders cost too much per land

**Status: proposal. Nothing in this file is built.** It answers the
land-cost review finding (coder7's `land_cost.rs` measurements): each land
rewrites one tree node per folder on each changed path, a node holds the
whole folder, and the cost is linear in folder size. [`format-v1.md`]
(../format-v1.md) is the contract this design keeps. Numbers marked
**estimate** are back-of-envelope figures, not measurements; the measured
counterparts are coder7's `land_cost.rs` numbers, quoted when they arrive.

## 1. Which bytes are format, which are ours

The `trees` table (metadata schema 0001) holds one row per tree node:
`(id, body)`. Two things in that row are frozen by the format:

- **The body's bytes.** `TreeV1` is the public tree format that clients
  fetch (`docs/format-v1.md`, TreeV1 section). A node body is
  `u8(1) || u32be(n_entries) || EntryV1*`, names strictly increasing, and
  the decoder accepts exactly one encoding. The server must be able to
  serve these exact bytes.
- **The ID.** `TreeId = BLAKE3("scion\0tree\0v1\0" || TreeV1)`. The row's
  `id` must hash the body it is stored with; `get_tree_body` sends the
  bytes as they are and clients re-hash them.

Everything else about the row is ours to change:

- **The granularity.** Nothing in the format says "one row per directory".
  A folder's public `TreeV1` bytes can be *stored* as several rows —
  chunks of its sorted entry list — as long as the server can reassemble
  them into the exact canonical bytes and hash them to the same ID. The
  `tag 4 dir` payload is a child's `TreeId`, so the parent-child edges
  stay the same; only how a child's bytes are laid down changes.
- **The row schema.** `trees` is `STRICT, WITHOUT ROWID` with two columns;
  adding chunk rows, a base pointer, or a compressed-body column is a
  server migration (SCHEMA_VERSION 10), not a format change.

The precedent is in-house: path-history nodes (`history_nodes`, migration
0005) already chunk a directory level that would otherwise be one row.
`history.rs`'s `Edit` splits a level into an index of leaf chunks (64 KiB
bound, 7 bits), and a land stamps only the chunks on changed paths. The
root tree is the one place still paying full-folder rewrites.

## 2. The cost, measured from the code

A `TreeV1` entry costs, per entry: name bytes (~10–20), the tag byte, a
32-byte `BlobId`/`TreeId`, and a `u64be` length — about **64 bytes per
entry (estimate)**. A land that changes one file in a folder rewrites that
folder's whole node:

| folder entries | node body per land | per 10,000 lands |
|---|---|---|
| 100 | 6.4 KB | 64 MB |
| 1,000 | 64 KB | 640 MB |
| 10,000 | 640 KB | 6.4 GB |
| 100,000 | 6.4 MB | 64 GB |

The same row is also read whole: `tree::lookup` calls `get_tree` at each
level and clones the full entry map (`tree.rs:119`), so a file read in a
100,000-entry directory costs ~6.4 MB (estimate) to find one entry.

The land's other tree is not the problem: each changeset writes one
history node per folder on its changed paths, but those nodes already
chunk at 64 KiB, so a 100,000-name directory's history read is its chunk,
"about 8 KiB" (`read.rs`), plus the index. `Log` with a path reads one
history node per path part per step (bounded by `MAX_LOG_NODE_BYTES`), and
the conflict check reads the same nodes. The folder cost is the root
tree's.

## 3. Three options

### O1 — chunked folder nodes (content-defined chunks)

Store a large folder as an index row plus chunk rows, split on the sorted
entry stream by content-defined cut points (as `history.rs` does). A land
rewrites only the chunks its entries fall in, plus the spine of chunk
digests up to the root.

- Bytes per land: about `64 B × chunk_size × touched_chunks` — with
  128-entry chunks, ~10 KB (estimate), flat in folder size.
- `Log` with a path: reads history nodes (unchanged) plus one chunk per
  part to resolve the path — smaller than today's whole node.
- `ListDir`: reads all k chunks of the folder (same bytes, k row reads).
  The `Tree` reply is assembled from the chunks; for the common
  small-folder case the cut never fires and the behavior is identical.
- Conflict check: history nodes, already chunked, unchanged.
- Migration: schema 10 adds the chunk rows; every tree is re-derived from
  its changesets — the same delete-and-rebuild migration 0007 ran when
  the tree format last changed. Trees are derived data
  (`tree::store_changeset` rebuilds from parents + changes), so the
  rebuild code exists.

### O2 — delta nodes with a full node every k

Keep whole-folder nodes, but a land writes a delta row (touched entries +
base id) and a full folder node is re-materialized every k-th land.

- Bytes per land: `64 B × n / k` amortized — with k = 16 on a 10,000-entry
  folder, ~40 KB (estimate), linear with a small constant.
- Reads: `ListDir` at a fresh head applies up to k deltas; a cold read of
  an old head still lands on the full node. The linear baseline survives;
  only its frequency drops.
- Migration: additive (new optional columns), cheap.

### O3 — compress node bodies

zstd the `body` column (the ID still hashes the raw bytes; store a flag or
prefix per row). Tree bodies are sorted names and random 32-byte IDs:
random hashes do not compress, so the win is names and the `len` fields —
**estimate** 15–25% on mixed trees, near 0 where names are hex-like.
640 KB → ~430 KB per land: the linear slope survives, just smaller. CPU
cost is trivial; the change is one row-encoding seam.

## 4. Recommendation and trigger

**Recommendation: O1 when the trigger fires, with O3 stacked on top.**
O1 is the only option that breaks the linear term; it copies the chunking
the codebase already has for path history; and it also shrinks the read
side (`lookup`, `ListDir` on big directories). O2 spends complexity to
keep the same linear shape with a smaller constant, and delta chains tax
the random reads a tree format exists to serve. O3 is cheap and stacks,
but is not a fix.

**Trigger — pay for the change when a folder is this big and this hot
(estimate):**

- A folder of **≥ 1,000 entries** taking **≥ 100 lands a day**: that is
  ≥ 64 MB/day of write amplification from one folder — 23 GB/year.
- Or the `trees` table growing **≥ 1 GB/week** with lands whose changed
  paths share folders.
- Below 100 entries the per-land cost is 6.4 KB — nothing to fix; do
  nothing there. The migration's own cost is one rewrite of every tree,
  the same bytes as one land per folder today.

## 5. What this does not change

`TreeV1` bytes, IDs, the wire protocol, and every client-visible read stay
as they are. A store migrated to chunks serves the same `TreeV1` bodies;
`format-v1.md` is untouched, and a client cannot tell.
