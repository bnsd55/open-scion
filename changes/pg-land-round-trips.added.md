---
ms: "a guard counts the statements of one Postgres land and fails above today's count"
---

One land on Postgres runs its work as serial client-to-database
statements inside the per-ref critical section, and every land queued
behind the one that holds the ref waits for all of them. The analysis
lives in the land performance issue.

The store layer counts the statements a process runs against Postgres,
behind the test hooks, and a Postgres-only guard measures one land
through the server's Land RPC - the land already on the head, and one
that must rebase - and fails the run when either is above its count.

A transaction now reads each changeset's generation once: a
generation is fixed at insert and never updated, so the derived walks
and the land that ask for the same few ids again get them from the
transaction's own cache, and a merge reads every parent's generation
in one statement instead of one per parent. The guard's limits fell
from 51 to 48 and from 56 to 55 with this change.

The multi-row writes went the same way: the trees a change builds land
as one statement per table instead of one per row, and so do the blobs
a changeset puts and the rows of its parents - the work that scales
with the change's width. A third guard case pins that: a land of 50
files ran 106 statements before the batch and 56 after.

A repeated parent now stores the same way on both backends (the
batched read had diverged from the per-row loop on Postgres), and a
test holds the two backends to the same answer.

The land's outcome check rides the plan's read transaction: one read
answers both whether a twin call settled the land and what the land
must do now, where the loop read them in two steps before. The counts
do not move (the loop's read ran without transaction boilerplate
already); what moves is that the plan can no longer run for a land
that a twin call has settled. On SQLite that read is one snapshot; on
Postgres it runs in autocommit statements, so the two reads can see
different states - a settle that lands between them is stopped at
`set_land_state`'s pending guard ("no longer pending"), which is the
backstop there.
