# Operator drill: a stranger follows the runbooks

This file is the script, not a record. A second operator who has never
seen the project takes it, works alone, and may read only
[docs/operations.md](../operations.md) — no source, no issue, no
teammate. Every step below is taken from that document and names the
section it came from, so a reviewer can check the two against each
other. The drill's output is two things: the evidence each task names,
and the guesses log at the bottom — every place the runbook made the
operator guess, written down while it happens. A guess is a gap in the
runbook, and the gaps become its next revision.

Run each drill on a fresh machine (or VM) as a non-root user. One dated
file per run, from [TEMPLATE.md](TEMPLATE.md), records what happened;
this file never changes with a run.

## T1 — Install

Source: the whole of operations.md (its "The pieces" list names the
binaries; no section says how to obtain one).

1. Get the three binaries — `scion-server`, `scion-cache`, `scion-daemon`
   — onto the machine.

   Done when: all three run `--help` and exit 0.

2. Note what you had to assume to get them (release page? build from
   source? which toolchain?). The runbook does not say; write it in the
   guesses log.

## T2 — First boot: one server

Source: §1 "One server", §2 "Ticket keys", §6 "Health".

1. `mkdir /srv/scion && chmod 0700 /srv/scion`

2. `SCION_TOKEN=<TOKEN> ./scion-server --data /srv/scion serve`

   Done when: the log says the server listens on `127.0.0.1:7878`.

3. Stop it (Ctrl-C). Mint a ticket key — without one no object is
   readable:

   ```sh
   ./scion-server ticket-key --out /srv/scion-keys/ticket.key
   chmod 600 /srv/scion-keys/ticket.key
   ```

   Done when: the command prints the public key as 64 hex characters,
   and a second run of the same command is an error.

4. Start for real:

   ```sh
   ./scion-server --data /srv/scion serve --token-file <TOKEN_FILE> \
     --ticket-key /srv/scion-keys/ticket.key
   ```

5. Probe readiness and liveness: the health service answers
   `grpc.health.v1` on the serving port; the service name for readiness
   is `scion.v1.Scion`, the empty name is liveness.

   Done when: readiness answers `SERVING` for `scion.v1.Scion`, and the
   empty name answers `SERVING` too.

6. Try to start a second server on the same data directory.

   Done when: the second refuses to start (the store lock), and the
   first is unaffected.

## T3 — Backup a running server

Source: §3 "Backup and restore".

1. Take the metadata snapshot first, then the blobs — never the other
   order:

   ```sh
   sqlite3 /srv/scion/scion.db "VACUUM INTO '/backups/scion.db'"
   cp -R /srv/scion/objects /backups/scion-$(date +%F)/objects
   ```

   Done when: both commands exit 0. Do not run `gc` while the backup
   runs.

2. Check what you took: `fsck` on the copy is read-only and may run
   beside a live server.

   Done when: `fsck` on the backup directory exits 0 and prints no
   problem lines.

3. Write down the recovery point the runbook promises for this backup
   (§3 "The durability point"), in your own words, before you look at
   step 1 of T4.

## T4 — Restore

Source: §3 "Backup and restore".

1. Put the metadata, and `objects/`, into a fresh directory.

2. `fsck` on it.

   Done when: `fsck` exits 0.

3. Start the server on the restored directory with the same token and
   ticket key as T2.

   Done when: the server listens, and a client can read what was
   written before the backup's snapshot. (§3 names what may be lost:
   the writes acknowledged after the snapshot began.)

## T5 — Disk-full recovery

Source: §5 "Routines" (gc), §6 "What to watch" (`scion_data_free_bytes`).

1. The alert the runbook gives: `scion_data_free_bytes` — free bytes on
   the filesystem that holds the data directory, refreshed every 30 s.
   Turn `--metrics-listen 127.0.0.1:9100` on and read the metric.

   Done when: you can see the current free bytes.

2. Fill the disk (a test file to near zero free), and watch the metric
   and the server's behaviour.

   Done when: you can say what the server does — and what you did — to
   get space back. The runbook's tool for this is the collector:

   ```sh
   scion-server --data /srv/scion gc --dry-run
   ```

   then, when the report makes sense, without `--dry-run`, and with a
   metrics file if you scrape one. Write down what the runbook does
   NOT tell you about a full disk — §6 only says to alert before it
   reaches zero.

## T6 — Rotate the ticket key

Source: §2 "Ticket keys" (the four steps are quoted there in order).

1. Make the new key: `scion-server ticket-key --out NEW.key` — it
   prints the new public key, 64 hex characters.

2. Give the tiers BOTH public keys, new first, before the server moves:
   `--ticket-public-key <NEW_HEX> --ticket-public-key <OLD_HEX>`.
   Restart the caches.

3. Restart the server with both keys: `--ticket-key NEW.key
   --ticket-key-old OLD-PUBLIC`, where `OLD-PUBLIC` is a file holding
   the OLD key's public hex, not its seed.

4. After 15 minutes (a ticket's whole life), restart the server without
   `--ticket-key-old`, and the caches with only the new public key.

Done when: a checkout reads through the whole rotation without a
refused ticket in the log, and the last state carries no old key
anywhere.

## T7 — Upgrade

Source: §4 "Upgrades", §3 for the backup copy.

1. Stop the server (the drain: readiness goes `NOT_SERVING`, listeners
   close, in-flight calls finish, exit 0).

2. Copy the data directory as a backup (§3: with the server stopped the
   copy is exact).

3. Start the new binary on the same directory. Watch the log.

   Done when: the migration progress is in the log, the server
   listens, and `fsck` after it exits 0.

4. Prove the guard the runbook promises: never let a newer database
   open with an older binary. The runbook quotes the refusal —
   `database schema version N is newer than this build` — see it once
   by starting the OLD binary on the upgraded directory.

   Done when: the old binary refuses with that shape of error, and you
   did NOT edit `user_version` to fix it.

## Guesses log

Fill one line the moment a step makes you decide something the runbook
did not say. A filled row is a finding, not a failure.

| Task | Step | What the runbook left out | What I guessed |
| --- | --- | --- | --- |
| T1 | | | |
| T2 | | | |
| T3 | | | |
| T4 | | | |
| T5 | | | |
| T6 | | | |
| T7 | | | |
