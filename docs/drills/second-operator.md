# Drill: <date> - a second operator follows the runbooks

A recorded drill is the proof that a second operator can follow the
runbooks unaided (#396). One file per run, in this shape: run the steps
on the machine in front of you, fill the notes while they happen, and
commit the file. Every command below is copied from
[docs/operations.md](../operations.md) and names the section it came
from; the drill adds nothing the runbook does not say. A step that runs
only on Linux is marked **(Linux)**.

- Version drilled: <the version and commit>
- Runbook: docs/operations.md (sections named per task)
- Who ran it: <name>

## Steps

### T1 - One server up (operations.md §1 "One server")

1. The data directory holds every blob and every row, so keep it mode
   0700 and owned by the user the server runs as - the server refuses
   one that group or others can write (§1). The runbook names the
   directory, not the command: create it your way, and note what you
   did.

   Notes: ___

2. Start the server, the runbook's own command:

   ```sh
   SCION_TOKEN=<TOKEN> ./scion-server --data /srv/scion serve
   ```

   Done when: the log says the server listens on `127.0.0.1:7878` (or
   the `--listen` you gave).

   Notes: ___

3. Stop it, and watch the drain (§1): readiness answers `NOT_SERVING`,
   the listeners close, the in-flight calls finish, exit 0.

   Done when: the process exits 0 and the log shows the drain order.

   Notes: ___

### T2 - A ticket key, so objects read (§2 "Ticket keys")

1. Mint the key and make the file private:

   ```sh
   ./scion-server ticket-key --out /srv/scion-keys/ticket.key   # prints the public key
   chmod 600 /srv/scion-keys/ticket.key
   ```

   Done when: the public key prints as 64 hex characters, and a second
   run of the same command is an error.

   Notes: ___

2. Start the server again with the key, the runbook's line:

   ```sh
   ./scion-server --data /srv/scion serve --token <TOKEN> \
     --ticket-key /srv/scion-keys/ticket.key
   ```

   Done when: a client can put and read an object (§1's `scion` client,
   or the walkthrough in the README).

   Notes: ___

### T3 - Rotate the ticket key (§2 "Ticket keys")

1. Make the new key. It prints the new public key, 64 hex characters.

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

   Notes: ___

### T4 - Backup a running server (§3 "Backup and restore")

1. The metadata snapshot first, then the blobs - never the other order:

   ```sh
   sqlite3 /srv/scion/scion.db "VACUUM INTO '/backups/scion.db'"
   cp -R /srv/scion/objects /backups/scion-$(date +%F)/objects
   ```

   Done when: both commands exit 0. Do not run `gc` while the backup
   runs (§3 says why).

   Notes: ___

2. Copy the backend marker, when the directory has one (a Postgres
   deployment writes it; a SQLite directory keeps its UUID in
   `scion.db` itself):

   ```sh
   cp /srv/scion/backend /backups/scion-$(date +%F)/backend
   ```

   Notes: ___

3. `fsck` on the copy is read-only and may run beside a live server.

   Done when: `fsck` on the backup directory exits 0 and prints no
   problem lines.

   Notes: ___

4. Write down the recovery point the runbook promises for this backup
   (§3 "The durability point"), in your own words, before you look at
   T5.

   Notes: ___

### T5 - Restore (§3)

1. Put the metadata, the marker, and `objects/` into a fresh directory.

2. `fsck` on it.

   Done when: `fsck` exits 0. The runbook says to run it after every
   restore, before the server serves: it catches the gap a torn or
   gc-raced backup left.

   Notes: ___

3. Start the server on the restored directory with the same token and
   ticket key as T2.

   Done when: the server listens, and a client reads what was written
   before the backup's snapshot. (§3 names what may be lost: the writes
   acknowledged after the snapshot began.)

   Notes: ___

### T6 - Upgrade (§4 "Upgrades", §3 for the backup copy)

1. Stop the server (the drain: readiness goes `NOT_SERVING`, listeners
   close, in-flight calls finish, exit 0).

2. Copy the data directory as a backup (§3: with the server stopped the
   copy is exact):

   ```sh
   cp -R /srv/scion /backups/scion-$(date +%F)
   ```

3. Start the new binary on the same directory. Watch the log.

   Done when: the migration progress is in the log, the server
   listens, and `fsck` after it exits 0.

   Notes: ___

4. Prove the guard the runbook promises: never let a newer database
   open with an older binary. The runbook quotes the refusal -
   `database schema version N is newer than this build` - see it once
   by starting the OLD binary on the upgraded directory.

   Done when: the old binary refuses with that shape of error, and you
   did NOT edit `user_version` to fix it (the runbook forbids it).

   Notes: ___

### T7 - Disk full (§5 "Routines", §6 "What to watch")

1. The alert the runbook gives: `scion_data_free_bytes` - free bytes on
   the filesystem that holds the data directory, refreshed every 30 s.
   Turn `--metrics-listen 127.0.0.1:9100` on and read the metric.

   Done when: you can see the current free bytes.

   Notes: ___

2. Fill the disk (a test file to near zero free), and watch the metric
   and the server's behaviour. The runbook's tool is the collector, on
   the schedule's own line (§5 "Routines"), whose paths are yours to
   substitute:

   ```sh
   scion-server --data /tmp/scion gc --metrics-file /var/lib/node_exporter/textfile/scion_gc.prom
   ```

   The disk-full runbook's first run takes the `--dry-run` flag ("first
   a `--dry-run` to see what it would collect, then a real run"), then
   the real run, and `fsck` when it ends (§5 "Disk full": a full disk
   can leave a write that failed mid-way; there is no repair - restore
   from a backup, §3, when fsck reports one).

   Done when: you can say what the server does - and what you did - to
   get space back, and fsck is clean.

   Notes: ___

### T8 - A failing disk is a restore (§5 "A failing disk", §3)

1. Damage on disk shows as `fsck` exit 1 with the problem named. The
   runbook's answer is the restore of section 3: the server stops, the
   copy goes into a fresh directory, fsck is clean on the restored copy
   before the server starts.

   Done when: the restored copy serves, and fsck on it was clean
   before the start.

   Notes: ___

### T9 - A Postgres restart is not a takeover (§5 "A Postgres restart") **(Linux: the runbook's database runs in a container)**

1. Watch `scion_pg_pool_wait_seconds` and the busy errors: they tell a
   database that is down from one that is slow.

2. Restart the database container; the server needs nothing.

3. After the restart, one write is the check: it runs, and the reads
   behind it see the data.

   Done when: the write runs after the database is back. If a write
   keeps failing, the runbook points at the fence (§1 "One server
   process serves one database"): a fenced server exits 70, and a
   supervisor restart of it takes the lock back or refuses.

   Notes: ___

### T10 - Read what the alerts read (§6 "What to watch")

1. The alert runbooks' pattern, on the stalled replies alert:

   ```sh
   curl -s 127.0.0.1:9100/metrics | grep '^scion_stalled_reply_bytes'
   grep 'closing a connection' server.log
   ```

   Done when: you can name the client the close notice points at, and
   say which of the runbook's two causes it was (one client, once; or
   the same peer again and again).

   Notes: ___

2. Walk the other three alert runbooks of §6 (latency p99, view memo,
   tier miss rate) the same way, from their own blocks.

   Notes: ___

## Guesses log

Fill one line the moment a step makes you decide something the runbook
did not say. A filled row is a finding, not a failure: the gaps become
the runbook's next revision.

| Task | Step | What the runbook left out | What I guessed |
| --- | --- | --- | --- |
| T1 | | | |
| T2 | | | |
| T3 | | | |
| T4 | | | |
| T5 | | | |
| T6 | | | |
| T7 | | | |
| T8 | | | |
| T9 | | | |
| T10 | | | |

## Outcome

<what the runbooks promised and what the drill saw: the checks that
passed, or the problem found and its issue link.>
