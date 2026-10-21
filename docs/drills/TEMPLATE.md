# Drill: <date> - <name>

A recorded drill is the proof of a runbook step that no CI test can run:
a live CA swap, an N-1 rollback on real hardware, a restore from a real
backup. Run the steps on the release candidate, then record them here and
commit the file. One file per drill; name it `YYYY-MM-DD-<name>.md`.

- Version drilled: <the release candidate's version and commit>
- Runbook: <the docs/operations.md section the steps come from>
- Who ran it: <name>

## Steps

1. <the exact command, copied from the terminal>

   <what it printed that matters>

2. <the next step>

## Outcome

<what the runbook promises and what the drill saw: the check that passed,
or the problem found and its issue link.>
