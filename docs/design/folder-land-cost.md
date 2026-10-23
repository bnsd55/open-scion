# Design: big folders cost too much per land

**Status: the read-path fix in section 3 landed in #506; the rest is a
proposal.** It answers the
land-cost review finding (a measurement of the bytes one land adds, at
10, 1,000 and 10,000 entries, per folder and per path history): each
land rewrites one tree node and one history node per folder on each
changed path, each holds the whole folder (history only until its 64 KiB
bound), and the cost is linear in folder size. On the tokio store, the
measurement read `history_nodes` 194 MB against `trees` 137 MB.
[`format-v1.md`](../format-v1.md) is the contract this design keeps.
Numbers marked **estimate** are back-of-envelope figures, not
measurements; the tokio numbers are the measurements.

## 1 Which bytes are format, which are ours

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

## 2 The cost, measured from the code

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

The same row is also read whole. Before #506, `tree::lookup` called
`get_tree` at each level and cloned the full entry map, so one file read
in a 100,000-entry directory cost ~6.4 MB (estimate) to find one entry.
Section 3 takes that read path; it landed in #506.

The land's other tree costs the same shape, and more. Each changeset also
writes one history node per folder on its changed paths. Those nodes
chunk at a 64 KiB bound (7 bits), but the chunking only caps *large*
folders: a folder of under about 1,000 entries (history entries are
~64–90 bytes: name, flags, a 32-byte changeset id, a generation, and
often a 32-byte child id) sits in **one** chunk, and every land rewrites
that chunk whole. On the tokio store, the measurement read
`history_nodes` at 194 MB against `trees` at 137 MB — for typical folders, history is the
larger half of the per-land cost, not a footnote.

What the two sides cost per land on a folder of n entries (estimate):

| tree | bytes per land | capped at |
|---|---|---|
| root tree (`trees`) | ~64 B × n | never — the node is the folder |
| path history (`history_nodes`) | ~64–90 B × n | ~64 KiB per level |

`Log` with a path and the conflict check read history nodes: one node
(chunk or index) per path part per step, bounded by `MAX_LOG_NODE_BYTES`.
A smaller history bound than 64 KiB would cut the land cost for
medium-size folders (a 500-entry folder becomes two chunks instead of
one), but it charges reads for it: the index rows grow (each level holds
more of them, all rebuildable but all written), and every step of `Log`
with a path reads the index plus one more chunk, so a scan's row count
rises with the chunk count. The bound trades land bytes against read
row counts, not against nothing.

## 3 The read path (landed in #506)

`tree::lookup` used to read each level's whole node and clone its entire
entry map to remove one name: a file read in a 100,000-entry directory
copied ~6.4 MB (estimate) on every call — CPU and memory on the read
path, on every read, not only storage. This was separate from the land
cost and did not need the chunked layout to fix.

**The fix that landed: a scan of the encoded body that stops at the
name, decodes one entry, and never builds the entry map
(`Tree::find_entry`, #506).** `TreeV1` entries are sorted by name
bytes, and the body's layout is self-describing (length-prefixed
names, fixed-size ids), so the scan reads entries in order and stops
at the first name equal to or past the target. A binary search is not
possible on this body: the entries have variable length and there is
no offset table. The scan lives in scion-core's `codec.rs`
(`Tree::find_entry`), called from `tree::lookup`; no format, schema,
or migration changes.

Note what neither fix changes: `TreeV1` is also what clients fetch, so a
100,000-entry folder is a ~6.4 MB object on the wire whatever the server
stores. Storage chunking reassembles the same bytes; shrinking the
wire object means giving clients partial tree reads — format v2
territory, out of scope here.

## 4 Three options

### O1 — chunked folder nodes (content-defined chunks)

Store a large folder as an index row plus chunk rows, split on the sorted
entry stream by content-defined cut points (as `history.rs` does). A land
rewrites only the chunks its entries fall in, plus the spine of chunk
digests up to the root.

- Bytes per land, root tree: about `64 B × chunk_size × touched_chunks`
  — with 128-entry chunks, ~10 KB (estimate), flat in folder size.
- Bytes per land, path history: the same scheme applies; today a typical
  folder's level is one chunk rewritten whole, so chunking history with a
  *smaller* bound (or chunking the root tree the same way) is what caps
  the larger half the measurement read. One mechanism covers both tables.
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

## 5 Recommendation and trigger

**Recommendation: O1 when the trigger fires, with O3 stacked on top.**
The §3 lookup fix has landed (#506); it already pays on every read. O1 is the only option that breaks the linear term
(on both tables — the same chunking applies to root-tree storage and to
a smaller history bound); it copies the chunking the codebase already
has for path history; and it also shrinks the read side (`ListDir` on
big directories pays row counts, not whole-node copies). O2 spends complexity to
keep the same linear shape with a smaller constant, and delta chains tax
the random reads a tree format exists to serve. O3 is cheap and stacks,
but is not a fix.

**Trigger — pay for the change when a folder is this big and this hot
(estimate, both tables together):**

- A folder of **≥ 1,000 entries** taking **≥ 100 lands a day**: that is
  ≥ 64 MB/day of write amplification from one folder — 23 GB/year —
  split between its root-tree node and its history chunk.
- Or `trees` + `history_nodes` growing **≥ 1 GB/week** with lands whose
  changed paths share folders (on tokio the pair measured 331 MB).
- Below 100 entries the per-land cost is ~6.4 KB per tree — nothing to
  fix; do nothing there. The migration's own cost is one rewrite of
  every tree and history node, the same bytes as one land per folder
  today.

## 6 What this does not change

`TreeV1` bytes, IDs, and every server-side check stay as they are. A
store migrated to chunks serves the same `TreeV1` bodies;
`format-v1.md` is untouched. The wire is the one thing no storage change
touches: clients fetch whole `TreeV1` objects (§3), so a 100,000-entry
folder is a ~6.4 MB reply whatever the server stores. Smaller wire
objects for big folders mean partial tree reads for clients — format v2
territory.
