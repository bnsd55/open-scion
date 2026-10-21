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
from 51 to 48 and from 56 to 55 with this change, and each further
step of the plan lowers them again.
