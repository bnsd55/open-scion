---
ms: "a guard counts the statements of one Postgres land and fails above today's count"
---

One land on Postgres runs its work as serial client-to-database
statements inside the per-ref critical section, and every land queued
behind the one that holds the ref waits for all of them. The analysis
lives in the land performance issue.

The store layer now counts the statements a process runs against
Postgres, behind the test hooks, and a Postgres-only guard measures one
land through the server's Land RPC and fails the run when it is above
today's count of 51. The planned work lowers the number to a budget of
15; each step moves the guard's limit down before it can claim to.
