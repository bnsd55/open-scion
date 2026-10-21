---
ms: "the Postgres disk-full mapping names every code a full disk sends"
---

A Postgres metadata write that runs out of space can reach the store
under more than one SQLSTATE. The store's mapping took one of them,
53100; the others a full disk produces - 53400, the temp-file cap a
`temp_file_limit` sets - fell through to the internal answer, and a
client that a full metadata disk hit saw the engine's error where the
fixed shortage reply was promised.

The mapping now names the codes of a full disk and why: 53100 (the
data or WAL directory is full) and 53400 (the space this query may
use) answer the same fixed `ResourceExhausted` shortage as the blob
path's does; 53200 (out of memory), 53000 (the class's catch-all) and
58030 (an io_error - a failing device, not a space shortage) stay
internal on purpose, each pinned by a unit test so a later widening
cannot answer a memory error with a disk reply.
