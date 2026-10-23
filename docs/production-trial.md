# Production trial: <version>, <backend>

The record of one release candidate serving real work for 30 days on
one production backend (#395). The workload floors are fixed in this
file BEFORE day 1, and the trial passes or fails against them at the
end; a floor changed mid-trial voids the trial. One file per trial;
name it `docs/release-notes/production-trial-<version>-<backend>.md`.
A second operator fills every field as it happens: each entry carries
the time it was made and the evidence link (a run log, a metrics
export, an `fsck` report), so the file is the trial, not a memory of
it.

## The candidate

| | |
|---|---|
| Version | <the release candidate's version> |
| Commit | <the `git rev-parse HEAD` of the release candidate> |
| Binaries | <the release's four binaries, by name> |
| Checksums | <paste the release's checksums.txt, or its link> |
| How it was built | <the tag it came from, or the workflow run link> |

## The machine

| | |
|---|---|
| Host | <the machine, named: who owns it, where it runs> |
| OS | <the operating system and version> |
| CPUs, RAM | <what the trial ran on> |

## The store

| | |
|---|---|
| Backend | <sqlite or postgres> |
| Backend version | <the version, as the server reports it> |
| Data directory | <the path, and the filesystem it sits on> |
| Filesystem | <the filesystem name; network filesystems are out of profile> |

## The workload, and its floors (fixed before day 1)

<Describe the real work the candidate serves: what the agents do, how
many, on which repositories, what the readers read. Then fix the
floors. A floor is a number the trial must not cross for the trial to
pass; write it as a hard bound, not a wish.>

| Floor | Value (fixed before day 1) | Measured against |
|---|---|---|
| Availability | <e.g. the server answers every full day of the trial> | the daily table's errors column and the incident log |
| Land latency p99 | <blank for the owner to fix> | the daily table |
| Read latency p99 | <blank for the owner to fix> | the daily table |
| Server RSS | <blank for the owner to fix> | the daily table |
| Disk growth | <blank for the owner to fix> | the daily table |
| Data loss | <zero: every acknowledged land is in the next day's read> | the daily verify and the restore drill |

## The daily table

One row per day, filled by the second operator from the same source
each day (the server's metrics export, at a fixed hour).

| Day | Date | Lands | Reads | Land p99 ms | Read p99 ms | Server RSS MiB | Store size | Errors |
|---|---|---|---|---|---|---|---|---|
| 1 | | | | | | | | |
| 2 | | | | | | | | |
| ... | | | | | | | | |
| 30 | | | | | | | | |

## Upgrades during the trial

<One subsection per upgrade the trial ran. If none, write "none", and
say why the release candidate was trusted to run untouched.>

### <date> — <from version> to <to version>

- Time: <start and end, from the logs>
- What happened: <the steps, in order, as run>
- Evidence: <the run log or the commands' output>

## Restores during the trial

<One subsection per restore drill run mid-trial: the backup taken, the
restore, `fsck`, the server back up. If none, write "none": the trial
then cannot claim a proven restore, and the record says so.>

### <date> — restore of <what>

- Time: <start and end>
- What happened: <the steps, in order, as run>
- Evidence: <the fsck output, the workflow run, or the log>

## Key and certificate rotations during the trial

<One subsection per rotation: the ticket key, the server certificate.
If none, write "none".>

### <date> — <what rotated>

- Time: <start and end>
- What happened: <the steps from operations.md, as run>
- Evidence: <the log>

## Incidents

<One subsection per incident: anything that stopped work, lost data,
or crossed a floor. An incident that crosses a floor is a failed
trial, recorded here and named in the outcome. If none, write
"none".>

### <date> — <what happened>

- Time: <when it started, when it ended>
- What happened: <the facts, in order>
- Floor crossed: <which floor, or none>
- Evidence: <the link>

## The outcome

<The pass or fail rule, applied to the record above: every day's row
is present, no floor was crossed, no acknowledged land was lost, and
every incident is recorded with its evidence. Write one of:

- PASS: the floors held for all 30 days; the evidence is the table and
  the incident log above.
- FAIL: <which floor, which day, and the issue link that carries the
  work.>

The rule is not a judgement call: a crossed floor is a fail, a
lost acknowledged land is a fail, a missing day's row is an
unfinished trial.>
