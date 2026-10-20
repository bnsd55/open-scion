# Operating Scion

How to run the server and its tiers, keep the data safe, and upgrade: the
steps an operator needs, written from the code on `main`. Every command in
this guide was run once in a fresh directory. The flags of a release are its
truth: `--help` of each binary lists everything.

A section whose steps a test runs ends with a `Proof:` line that names the
test (file and function). The docs_ops tests fail when a proof's test is
gone, when a command of a shell block names a flag `--help` does not, and
when the drills of docs/drills/ lose their template.

The pieces:

- `scion-server` — the one process that stores and lands. One data directory
  holds everything: `scion.db` (metadata and trees, SQLite), `objects/` (one
  zstd file per blob), `tmp/` (staging), and three lock files. A second
  server on the same directory refuses to start.
- `scion-cache` — a regional tier in front of the server. Keeps objects in
  memory and answers object reads only: a client resolves refs and gets its
  read tickets from the server, which the tier refuses to serve itself.
- `scion-daemon` — one per machine user, in front of a cache. Keeps objects
  on disk, serves on a private Unix socket. One daemon serves one OS user.

## 1. Deployment shapes

### One server

```sh
SCION_TOKEN=<TOKEN> ./scion-server --data /srv/scion serve
```

That is the whole deployment: clients talk to `127.0.0.1:7878` (or `--listen`
HOST:PORT). The data directory holds every blob and every row, so keep it
mode 0700 and owned by the user the server runs as: the server refuses one
that group or others can write, or that sits under a directory another user
can change. Without `--ticket-key` no object is readable, so a real server
adds a ticket key (section 2):

```sh
./scion-server ticket-key --out /srv/scion-keys/ticket.key   # prints the public key
chmod 600 /srv/scion-keys/ticket.key
./scion-server --data /srv/scion serve --token <TOKEN> \
  --ticket-key /srv/scion-keys/ticket.key
```

Size the process with the three connection caps: `--max-connections` (128)
bounds the clients served at once, `--max-streams` (256) bounds the calls
one client may hold open at once, and `--max-stalled-bytes` (64 MiB)
bounds what the stalled replies of one connection may hold before the
connection is closed. A cache and a daemon take the same three flags.

Choose the tokens with care: `--token T` (or `SCION_TOKEN=T`) makes one
principal, `default`, with every right on every repo. A token on the command
line is readable by every local account, so the argv form warns and goes
away in a later release; pass the token through `SCION_TOKEN` or
`--token-file FILE` (a file the owner alone may read). For several people or
services, use an access file instead (see "Tokens: one, or an access file",
below).

Stopping the server: Ctrl-C or SIGTERM starts a graceful drain, in this
order — the health service answers `NOT_SERVING` for the service names
(readiness goes false), the listeners close to new connections, the
in-flight calls finish, and the process exits 0. The whole server (the
empty health name) stays `SERVING` while it answers, so a liveness probe
does not kill a draining server. The in-flight calls have
`--drain-timeout-secs` (default 600) to finish; past it they are cut, the
log names the count, and the process exits 1. A second signal while the
drain runs ends it at once, with the same exit. Wait out the drain; send
the second signal only when a cut is cheaper than the wait.

A call that was answered kept its promise: a put or a land that the client
saw a reply for is durable, over the stop and after the restart. A call
that was cut got no reply and carries no promise. A cut land stays
`pending`: a retry with the same request key resumes it where it stopped;
pending lands sweep themselves after `--land-stale-secs` (see the quotas
above). The metrics gauge `scion_calls_in_flight` counts the calls the
drain still waits for.

#### One server, and no automatic failover

There is no automatic failover. One active server holds the store: a
second server on the same data directory (or the same Postgres database)
refuses to start, and a running server whose lock the database gave to
another process is fenced — it answers `FailedPrecondition` to every call
and exits. That is the guard against two servers taking one store, not a
failover path.

When the one server dies, every client's calls fail until it is back.
Nothing takes over by itself: no standby, no leader election, no routing.
The operator restarts the server on the same data directory (`systemctl
restart scion`, or the container's restart policy); a restart retakes the
store and serves the writes that were acknowledged before the death, and
the calls that were in flight are the client's to retry. While the server
is down, the tiers and daemons in front of it serve what they have cached
and refuse the rest. If the directory itself is lost, the recovery is a
restore from backup (section 3).

Proof: crates/scion-server/tests/backend_refusals.rs::a_lost_server_lock_fences_the_backend
Proof: crates/scion-server/tests/backend_refusals.rs::an_unreachable_database_is_not_a_takeover
Proof: crates/scion-server/tests/crash.rs::every_reply_survives_a_kill_of_the_server

### Server and a regional cache

A cache serves object reads to a region and refuses every other call
(`UNIMPLEMENTED`): the client resolves refs and reads paths on the server,
and asks the cache only for objects. Each hop has its own token pair:
(`--token`), and what it sends upstream (`--upstream-token`). A `scn`
client's `--token` is what it sends, the other side of the pair of the tier
in front of it.

```sh
SCION_TOKEN=<CACHE_TOKEN> SCION_UPSTREAM_TOKEN=<SERVER_TOKEN> \
  ./scion-cache --upstream http://SERVER:7878 \
  --ticket-public-key <PUBLIC_KEY_HEX>
```

The cache keeps its objects in memory and drops the least recently used
first: `--max-bytes` (1 GiB) is the most object bytes it holds. A snapshot
holds at most `--snapshot-bytes` object bytes (256 MiB).

The cache needs `--ticket-public-key` (the hex that `ticket-key` printed):
without it, it serves no objects. A `scn` user then points reads at the
cache with `scn checkout --server http://SERVER:7878 --objects http://CACHE:7879`.

### Server, cache, and a daemon per user

```sh
SCION_TOKEN=<USER_TOKEN> SCION_UPSTREAM_TOKEN=<CACHE_TOKEN> \
  ./scion-daemon --data ~/.scion-daemon --upstream http://CACHE:7879
```

The daemon's data directory must be private to its user (mode 0700, not a
shared path such as `/tmp`); it serves on a 0600 Unix socket in it
(`--socket`, default `daemon.sock` in the data directory). Its disk store
keeps at most `--max-bytes` object bytes (10 GiB) and drops the least
recently used first. Every
directory above it must be owned by root or by the user, and not writable by
group or others unless it has the sticky bit. It refuses anything else, and
`scn` checks the socket the same way; the error names the directory and
`chmod go-w DIR` fixes it.

One token rule the flag list hides: `scn` sends its one `--token` on every
call, to the server (refs and reads) and to the daemon (objects). So the
token a user holds must be accepted by the server **and** by that user's
daemon. With the chain above, the daemon of a user runs
`--token <USER_TOKEN>`, where `<USER_TOKEN>` is that user's token at the
server too. A new node can start warm: copy a snapshot file from the cache
(`--snapshot DIR --snapshot-repo R`, refreshed every `--snapshot-every`
seconds) and start the daemon with `--seed FILE --seed-full-repo`. A snapshot
holds every object of its repo by ID, so it is a full-repo credential: anyone
who has the file can read what a folder read rule would hide from a partial
reader, and a seeded daemon serves those paths, because it checks objects, not
rules. The daemon refuses a seed without `--seed-full-repo`, which says that
every user of this node may read the repo whole; snapshot only such repos.

### The metadata in Postgres

By default the data directory keeps its metadata in its own SQLite file. With
`--postgres-url`, the server keeps it in a Postgres database instead, so it
can be administered and replicated with the Postgres tooling you already
run, and outlives one machine. The blob files stay in the data directory in
every backend: they are the big majority of the bytes, and the database
never stores them.

```sh
scion-server --data /srv/scion \
  --postgres-url postgres://user@dbhost/scion?sslmode=require \
  --postgres-ca /srv/scion/db-ca.pem serve --access /srv/scion/access.txt
```

- `--postgres-url` names the database. Off this machine the URL must ask
  for TLS (`sslmode=require`): the open refuses a remote URL that does not.
- `--postgres-ca` (a PEM file) is required with a remote host too: the
  database must present a certificate it signed, and there is no mode that
  accepts any certificate. A host on this machine (`localhost`, a loopback
  address) may go without either. The CA file must not be group- or
  others-writable: a swapped CA changes whom the database connection
  trusts, and the open refuses one (`chmod go-w /srv/scion/db-ca.pem`).
- The open checks the database runs `synchronous_commit=on`: a returned
  commit must be durable. A database that answers otherwise is refused.
- The open migrates the database's schema to this build's version, one
  transaction per migration, under a database lock with the same 5 s clock:
  an open that waits longer answers busy, and the caller retries. A
  database of a newer build is refused: run that build's server.
- One server process serves one database: a second `serve` on the same
  `--postgres-url` fails at its open with "another server holds this
  database". An import does not take that lock and runs beside a server.
- A write that waits for another writer's lock gives up after 5 s and
  answers unavailable, like the SQLite store past its busy timeout; the
  client retries.
- `fsck` and `gc` serve every backend: on a Postgres-backed server they
  run against the database through the same metadata interface, and the
  blob files stay in the data directory either way. The directory's own
  marker still refuses an open whose `--postgres-url` names another
  backend.
- The choice is recorded in the data directory's `backend` file: a
  `--postgres-url` open refuses a directory that holds SQLite metadata, and
  a plain open refuses a directory it names, so a second, empty store
  cannot grow beside the database. The first open also mints a store UUID
  and writes it in the database and the marker's second line: an open of
  the database against a directory that names another store is refused
  with both UUIDs, so a restore that mixes a database with the wrong
  directory fails at the start instead of serving metadata whose blobs
  are not on this disk.
- One server process serves one database, and it stays that way: if the
  server's connection drops and the lock is lost to another process, the
  server is fenced. Every read and write fails with the loss named, the
  health service answers `NOT_SERVING` on the same port, and after a few
  seconds the process exits with code 70, so a supervisor restart finds
  the lock taken and refuses, or takes it back on an empty database. A
  session checks itself once a minute, so a loss is seen even while no
  client calls: the check runs on the server's own session, through the
  same queue as every statement, never on a second connection.
- The SQLite store has none of this: one file is opened by one process at
  the kernel level, so there is no lock to lose and no fence. The notes
  above that name the one-server lock and the fence are Postgres only.

Backup and restore of a Postgres-backed server: `pg_dump` the database
first, then copy the data directory. A changeset in the dump names only
blob files that existed when the dump was taken, and a blob file is written
before its database row commits, so the later copy holds every file the
dump names. Restore into a fresh database, copy the data directory back,
start the server with the same `--postgres-url`.

### Tokens: one, or an access file

`--token` and `--access` are exclusive. An access file names tokens by their
BLAKE3 digest (never the token itself) and maps each to a principal with
per-repo read and write rules:

```sh
TOK=$(printf %s <SECRET> | ./scion-server hash-token)
printf 'token\tops\t%s\nread\tops\tr\nwrite\tops\tr\n' "$TOK" > /srv/scion/access.txt
chmod 600 /srv/scion/access.txt
./scion-server --data /srv/scion serve --access /srv/scion/access.txt \
  --ticket-key /srv/scion-keys/ticket.key
```

With `--access`, the per-principal caps are on by default (64 requests, 512
MiB upload bytes, 64 pending lands per principal); raise or lower them with
`--max-principal-requests`, `--max-principal-upload-bytes`,
`--max-principal-pending-lands`. With `--token`, every caller is the same
principal, so the caps are off unless a flag sets one.

Rewriting the access file takes effect at the next start (or restart): the
server reads it once. A ticket keeps working for its whole life after a
reload that narrows its principal, up to 15 minutes from signing, on this
server and on any cache that holds the repo's objects. To revoke reads:
rewrite the file and restart, and either wait out the tickets (15 minutes)
or rotate the ticket key (`--ticket-key`) to cut them off at once. Objects
already on a user's daemon are out of reach of both; give a daemon only
repos all its users may read whole.

### Folder read rules

A `read` or `write` rule may name a path prefix as its fourth field, by
path parts: `read alice main src` lets alice read `src` and below
(`src/a` yes, `srcx` no), and nothing else of that repo. No prefix means
the whole repo. A write rule needs a whole-repo read rule — no blind
writes — and a file that gives a partial reader (one whose read rules are
all prefixes) a write rule is refused at load. The server derives each
principal's canonical prefixes into a rule-set digest, which keys the
derived view data; a repo holds at most 32 distinct rule sets, a set at
most 256 prefixes, and an access file over either cap is refused at load.

A partial reader reads the repo through its *view*: the real trees with
every entry outside its prefixes removed. `ResolveRef` answers the ref's
real changeset; `GetRootTree` answers the view root, with a read ticket
bound to it; `ListDir`, `ReadFile` and `GetObjects` serve from that view,
and a path outside the prefixes answers `NOT_FOUND`, the same as a
missing one, with no hint that it is hidden. The history and query calls
(`GetChangeset`, `Log`, `Blame`, `MergeBase`, `IsAncestor`, `Diff`) answer
`UNIMPLEMENTED` for a partial reader, one uniform answer on every call.
The first read of a rule set walks the real tree to derive the view, so
its latency varies with the repo; later reads of the same rule set and
changeset find the derived view.

Narrowing a principal's rules takes the same path as any access-file
change: rewrite the file and restart. A ticket signed under the wider
rules keeps working for its whole life, up to 15 minutes — wait it out,
or rotate the ticket key (section 2) to cut the old tickets off at once.
Objects a user already pulled to their daemon are outside the server's
reach either way.

Snapshots interact with the rules: a snapshot holds every object of its
repo by ID, so a daemon seeded from one serves paths that folder read
rules hide. Snapshot only repos that all of a node's users may read whole,
and say so with `--seed-full-repo` (see "Server, cache, and a daemon per
user"). Two side channels stay even without a seed: a partial reader sees
the real changeset IDs of its view, so a new ID under a view root whose
objects it cannot fetch shows that a hidden-only change landed; and the
cache's walk metrics count objects a partial reader asked for that the
walk refused. Metrics are for the operator, not for the readers.

### Network and TLS

The server and the cache refuse a non-loopback address unless they serve TLS
(`--tls-cert` and `--tls-key`) or get `--insecure-listen`. A client trusts a
private CA: `scn --tls-ca ca.pem`, a tier for its upstream with its own
`--tls-ca`. Between tiers, either end can demand a client certificate
(`--tls-client-ca` on the server or cache; `--tls-client-cert` and
`--tls-client-key` on the cache or daemon). A bearer token over plain TCP to
a non-loopback address stays refused.

The server reads the private key only when its owner (or root) alone can
read it, and refuses one that group or others can read at start
(`chmod 600 KEY`). The certificate and every CA file (`--tls-cert`, the
CAs of `--tls-ca`, `--tls-client-ca`, `--postgres-ca`) must not be
group- or others-writable, for a swapped file changes whom a client
trusts or whose certificates the server accepts: `chmod go-w` them, and
check after a copy tool or a umask 002 session, where a fresh file
starts 0664.

### In containers

`docker/compose.yml` brings up a server and a cache in one image, both with
TLS and a ticket key, and `docker/demo.sh` seeds a repo and checks it out
through the cache. It is the quickest end-to-end look at a two-tier
deployment, and a template for the real files (the secrets are made at
compose time, the server's data lives in a volume).

### Environment variables

Several flags can be set with an environment variable instead: the
variable holds the same value the flag would. These are the ones the
binaries read (each names the flag it stands for, and which binary reads
it):

| Variable | Flag | Read by | Notes |
| --- | --- | --- | --- |
| `SCION_DATA` | `--data` | `scion-server` | Data directory, default `scion-data`. |
| `SCION_POSTGRES_URL` | `--postgres-url` | `scion-server` | The URL holds the database credentials; the environment keeps them out of the process list (`ps`). `--postgres-url-file` reads it from a file the owner alone may read, and the argv form is deprecated. |
| `SCION_TOKEN` | `--token` | `scion-server` (`serve`, `hash-token`), `scion-cache`, `scion-daemon`, `scn` | A secret; the environment keeps it out of the process list. The `--token-file` flag reads the token from a file the owner alone may read, and `hash-token` reads it from stdin. The argv form prints a deprecation warning and goes away in a later release. |
| `SCION_UPSTREAM_TOKEN` | `--upstream-token` | `scion-cache`, `scion-daemon` | A secret, as `SCION_TOKEN`; `--upstream-token-file` reads it from a file, and the argv form is deprecated. |
| `SCION_DAEMON_DATA` | `--data` | `scion-daemon` | Data directory, default `scion-daemon-data`. |
| `SCION_AUTHOR` | `--author` | `scn commit` | Default `scn <scn@localhost>`. |
| `SCION_SERVER` | `--server` | `scn` | Default `http://127.0.0.1:7878`. |
| `SCION_OBJECTS` | `--objects` | `scn` | Address of a cache or daemon for object reads. |
| `SCION_TLS_CA` | `--tls-ca` | `scn` | PEM file of a private CA. |
| `SCION_REPO` | `--repo` | `scn` | Default `main`. |

Two more names exist for the test suite only, never for an operator:
`SCION_TEST_BACKEND` and `SCION_TEST_POSTGRES_URL`.

## 2. Ticket keys

An object read carries a ticket that the server signs with an Ed25519 key.
The seed lives in a 0600 file that `scion-server ticket-key --out FILE`
creates; the file is never replaced by a second run (a second run is an
error, so a key cannot rotate silently under running tiers), and the server
refuses to start with a seed file that group or others can read, that is a
symlink, or that sits under directories another user can change.

Rotation, step by step:

1. Make the new key: `scion-server ticket-key --out NEW.key` — it prints the
   new public key as 64 hex characters.
2. Give the tiers both public keys, new first, **before** the server moves
   to the new key — a cache that verifies only the old key refuses every
   ticket the new server mints:
   `--ticket-public-key <NEW_HEX> --ticket-public-key <OLD_HEX>` (repeat the
   flag; two are the most). Restart the caches; a cache that verifies
   against both serves tickets of either key.
3. Restart the server with both keys: `--ticket-key NEW.key
   --ticket-key-old OLD-PUBLIC`. The old argument is a file holding the
   **public** key of the previous key (the hex that the old run printed),
   not its seed. A public key is not a secret: anyone may read the file,
   but group and others must not be able to WRITE it (a changed key breaks
   every tier that verifies against the old one), it must not be a
   symlink, and the directories above it must be private to its owner or
   root. `chmod go-w OLD-PUBLIC` fixes a copy tool's 0666. Tickets of
   either key verify while both are given.
4. When no client can hold a ticket of the old key any more (tickets live 15
   minutes), restart the server without `--ticket-key-old` and the caches
   with only the new public key.

A daemon checks no ticket and forwards it, so it needs no key at any step.

Proof: crates/scn/tests/rotation.rs::ticket_key_rotation_while_a_checkout_reads

## 3. Backup and restore

### The durability point

What an acknowledged write has survived, before you need a backup:

- A **blob put** is answered after the blob's temp file was fsynced, the
  file was renamed into `objects/`, the directories of every new file were
  fsynced, and the drive's write cache was flushed. The server does not
  answer from its page cache: a power cut after the reply can lose nothing
  the caller was told was stored, on either backend — the blob path is the
  data directory's in both.
- A **land** is answered after its blobs were synced as above and its
  metadata commit was made durable. On SQLite, the commit fsyncs the WAL
  (`synchronous = FULL`); on Postgres, every connection runs
  `synchronous_commit = on`, and the server refuses to open a database
  that turned it off. A land's reply therefore names a commit the
  database will not lose, and the database's own durability rules carry
  it from there.
- What the above does **not** survive: the loss of the drive itself, or
  the loss of the machine. That is what the backup below is for.

Proof: crates/scion-server/tests/power.rs::every_reply_survives_a_power_cut
Proof: crates/scion-server/tests/dm_log_writes.rs::an_acknowledged_blob_survives_a_cut_at_the_last_barrier_before_its_reply
Proof: crates/scion-server/tests/crash.rs::every_reply_survives_a_kill_of_the_server
Proof: crates/scion-server/tests/storage.rs::a_retry_after_a_failed_dir_fsync_makes_the_kept_file_durable

The Postgres metadata half of the first two rules is Postgres's own
contract (`synchronous_commit = on`), which the server checks at every
open (`crates/scion-server/src/pg.rs`, the `SHOW synchronous_commit`
check): a test of the database's fsync would test Postgres, not Scion.

A backup (data taken outside the server) has a wider point: the
**recovery point** of a backup is the moment of its metadata snapshot.
Every write acknowledged after the snapshot began may be lost — a backup
run every N minutes has a recovery point of about N minutes of
acknowledged writes, plus the write in flight while the snapshot ran.
Writes acknowledged **before** the snapshot read back whole after a
restore, whatever was happening in the meantime.

Proof: crates/scion-server/tests/backup.rs::a_backup_taken_while_serving_restores_whole
Proof (open, #411): the live-backup test of that PR pins what a backup taken while
the server keeps writing may and may not hold

A data directory is `scion.db` plus `objects/`; the lock files and `tmp/`
are process state, not data. A copy of the directory is a backup:

```sh
cp -R /srv/scion /backups/scion-$(date +%F)
```

With the server stopped, the copy is exact. While the server runs, take
the backup in an order that keeps it whole: the metadata first, the blob
files second. A blob file is written once and is durable before the row
that names it commits, so a metadata snapshot never names a blob that a
later copy of `objects/` misses; a backup that takes the blobs first can
miss a blob that a row of its own metadata names.

On SQLite, the snapshot is `VACUUM INTO`, which reads a consistent
committed state and does not stop the server:

```sh
sqlite3 /srv/scion/scion.db "VACUUM INTO '/backups/scion.db'"
```

On Postgres, the metadata lives in the database, so the snapshot is a
dump:

```sh
pg_dump --no-owner --file /backups/metadata.sql <the metadata database>
```

Then copy the blob files into the backup:

```sh
cp -R /srv/scion/objects /backups/scion-$(date +%F)/objects
```

The `backend` marker is part of the metadata: it holds the store's UUID,
which the metadata holds too, and together they bind the database to its
data directory. An open refuses a database whose UUID differs from the
marker's, so the marker and the metadata always travel together. Copy
the marker when the directory has one (a Postgres deployment writes it;
a SQLite directory keeps its UUID in `scion.db` itself):

```sh
cp /srv/scion/backend /backups/scion-$(date +%F)/backend
```

A copy taken this way never tears a file that a commit is writing, and
`fsck` tells you what state a backup holds. `fsck` changes nothing and
can run while the server runs: it checks every blob file against its ID,
every blob a row names, every changeset, tree and history node against
its ID, every ref and derived row, and every derived value with its
kind's decoder. It prints one line per problem (kind, ID, path) and
exits 0 when it saw no damage, 1 when it did, 2 when it could not run.
A clean report means "no damage seen", not a snapshot: it reads in short
batches.

Two things bound a backup taken while the server runs:

- A write acknowledged before the metadata snapshot reads back after a
  restore. A write acknowledged during the backup may be in it or out
  of it.
- Do not run `gc` during a backup. A blob the snapshot names can lose
  its last row after the snapshot (a repo removed, a ref that moved),
  and the collector ages an orphan by the blob file's mtime, which for
  an old blob is older than the grace: the sweep can delete the file
  before the copy reaches it, and the restored store names a blob that
  is gone.

To restore: put the metadata (the snapshot, or the dump loaded into a
new database), the marker, and `objects/` into a fresh directory; run
`fsck` on it; start the server. `fsck` catches the gap a torn or
gc-raced backup left, so run it after every restore, before the server
serves.

The restore takes a measured time, not a promise: a store of 20 repos,
1000 changesets and about 32 MiB of raw blob content (1000 files of
about 32 KiB) restored in about <N> seconds on the `restore time` CI
job's `ubuntu-24.04` runner, `fsck` included. The number comes only
from that job (the runner label is the machine class, never a local
machine) and it is re-measured on every run of the guide's CI; the
`restore_bench.rs` test prints the seconds and fails past a 600 s
bound.

Proof: crates/scion-server/tests/restore_bench.rs::a_restore_of_a_stated_store_takes_a_stated_time

Proof: crates/scion-server/tests/backup.rs::a_backup_taken_while_serving_restores_whole

Proof: crates/scion-server/tests/backup.rs::a_backup_taken_while_serving_restores_whole

## 4. Upgrades

The schema version lives in the database. On open, a server migrates an
older schema forward, one migration per released version, each in one
transaction, with its progress in the log (a migration can take a while:
schema 4 walks every repo's changesets). Two rules:

- A database with a **newer** schema than the binary refuses to open:
  `corrupt data: database schema version 7 is newer than this build (6)`.
  Run the old binary until you can upgrade, or restore a backup. Never edit
  `user_version` by hand.
- If other processes (an `import`, a `fsck`) share the directory, a
  migration waits for them to stop: the error says `stop the other processes
  that use ...`. One process at a time migrates.

An import reads git revision `--rev` (default `HEAD`) into scion repo
`--repo` (default `main`), and points scion ref `--ref` (default `main`)
at the imported tip. Besides every root and merge, it re-checks `--verify`
(default 100) ordinary commits against git.

So an upgrade is: stop the server (and any tiers that share the directory —
none do by default), copy the directory as a backup (section 3), start the
new binary, watch the log for the migration, run `fsck`. Schema 7 (tree
entry sizes, `#172`) drops every stored tree and its derived rows, so the
first queries after it re-derive trees from the stored changesets; the
blobs and changesets stay. A tier's store may keep tree bytes of the old
format under their old IDs: nothing asks for those IDs again, and the
format's hash check guards what it does serve. New binaries can
also add derived data kinds; a server fills what a query needs, and
`scion-server derive --kind root_tree|commit_graph|path_history --ref R`
backfills a whole history in steps when you want it done before the first
query (it opens the directory as the server, so with the server stopped, and
it is safe to repeat).

Proof: crates/scion-server/tests/crash.rs::a_kill_during_a_schema_upgrade_leaves_the_old_schema_or_the_new_one

## 5. Routines

The routines are these:

- `fsck` on a schedule (it is read-only, so it can run beside a live
  server), and always on a fresh backup copy. Exit 1 with the problem lines
  is the signal to restore from a backup; there is no automatic repair.
- `tmp/` needs no attention: every open removes the staging folders of dead
  processes, and a lone open also removes files a build older than the
  schema left there.
- Pending lands sweep themselves: a land pending longer than
  `--land-stale-secs` (a day, by default) is rejected by a sweep that runs
  at start and then hourly. `finished` rows of the lands table are dropped
  after 7 days (7 days is `LAND_RETENTION_SECS` in `land.rs`): every new
  land's transaction deletes finished rows older than that, at most 100 per
  land recorded, and the newest land always stays.
- `scion-server gc` collects what nothing names any more: a row whose blob
  no stored changeset of its repo puts, and a blob file that no repo's row
  names, both older than `--grace-secs` (a week, by default: an upload that
  a client is about to name is younger). It is safe beside a live server
  and safe to repeat; rows go before files, so a crash leaves orphan files
  only, which the next run removes. `--dry-run` reports a run's result
  without removing anything. Run it on a schedule, and `fsck` after it the
  first few times, until you trust the two together. Its progress is in
  the gc log: one line when it starts, one per pruned batch and per swept
  file batch, one when it ends, each with the counts and the time it
  took, and a line at most every 10 s of a long run with the repos done
  and the total.

  Proof: crates/scion-server/tests/gc_metrics.rs::a_real_run_writes_the_report_as_textfile_metrics

**The collector's metrics file.** The collector runs as its own process,
so the server's `/metrics` cannot show what it did. Instead, give the run
a metrics file, and scrape it from the machine:

```sh
scion-server --data /tmp/scion gc --metrics-file /var/lib/node_exporter/textfile/scion_gc.prom
```

A cron line runs the collector on its schedule and writes the file, which
the node exporter's `textfile` collector serves with the rest of the
machine's metrics. The file holds the last finished run's end time
(`scion_gc_last_success_timestamp_seconds`), its duration
(`scion_gc_duration_seconds`), and the rows and files it collected
(`scion_gc_rows_pruned`, `scion_gc_files_removed`), each with a `dry_run`
label on every series. The file is written atomically, and only when the
run succeeds: a failed run leaves the previous file, so the timestamp
goes stale. One file holds one run: each run rewrites the whole file, so
after a dry run the series are the dry run's, until the next real run
replaces them. The alert to have on the folder is the age of the last
success, counted on the real runs only —
`time() - scion_gc_last_success_timestamp_seconds{dry_run="false"} > <N>`
— so a dry run cannot reset it: a run that did not finish, or a cron that
stopped firing, shows up as a timestamp that stops moving, not as a gap
in the scrape.
- Derived data can be rebuilt with `scion-server derive --kind ... --ref
  ...` (server stopped); it is safe to repeat and removes rows of older
  versions itself.
- A cache is stateless except its snapshots: its whole state is memory plus
  the snapshot files, so restart it whenever you like.

## 6. What to watch

**Metrics.** The server and the cache export Prometheus metrics at
`GET /metrics` on the address of `--metrics-listen` (off by default;
`scion-daemon` exports none). The endpoint is plain HTTP with no token, so
it refuses a non-loopback address unless `--insecure-listen` is given; the
usual shape is `--metrics-listen 127.0.0.1:9100` on the machine that runs
the process, scraped from there. The names:

- `scion_requests_total` — requests by RPC and code (the outcome codes are
  where refusals and failures show).
- `scion_request_seconds` — one latency histogram per RPC.
- `scion_rpc_bytes_total` — request and reply bytes by RPC.
- `scion_lands_total` — lands by outcome; the error labels (`aborted`, `deadline_exceeded`, `resource_exhausted`, `unavailable`, `not_found`, `invalid_argument`, `permission_denied`, `failed_precondition`, `cancelled`, `internal`) count lands whose call ended in an error, by the class of the answer the caller saw.
- `scion_land_replans_total` — lands that planned again because their ref
  moved under them; a rising rate means lands contending with imports or
  other writers on one ref.
- `scion_view_trees_walked_total` — real trees a view derivation opened for
  a partial reader; a rate that stays high across changesets of one rule
  set means the per-tree memo is not serving and every derivation re-walks.
- `scion_cache_hits_total` / `scion_cache_misses_total` — what a tier
  served from its store against what it fetched upstream.
- `scion_blob_sync_seconds` — one fsync attempt on the blob write path, failed ones included (temp file, stored file, blob directory, or the drive-cache flush)
  (batches that sync at once share one flush, so one caller's duration
  includes the wait for the flush in progress); a sum that outruns the
  counts says a drive that cannot keep up with the imports.
- `scion_data_free_bytes` — free bytes on the filesystem that holds the
  data directory, refreshed every 30 s; alert before it reaches zero.
- `scion_derive_walks_in_flight` / `scion_derive_walks_waiting` — derive
  walks that hold a permit, and walks that wait for one; waits that pile
  up while the in-flight count sits at the cap mean a cold history read
  that queues behind the walkers.
- `scion_pg_collector_key_wait_seconds` — a Postgres write's wait for the
  collector's advisory key; waits that climb toward the key's own 60 s
  bound mean the writer queue does not drain.
- `scion_pg_pool_wait_seconds` — (Postgres only) how long calls waited for
  a read connection. A rising tail means the 8 readers are held by long
  walks; the calls that waited past the pool's bound failed
  `unavailable`.
- `scion_pg_pool_wait_seconds` — (Postgres only) how long calls waited for
  a read connection. A read transaction is bounded at 300 s per statement
  and 30 s idle between statements, and a call waits at most 30 s for a
  reader before it fails `unavailable`. A rising tail means the 8 readers
  are held by long walks.
- `scion_pg_readers_busy` — (Postgres only) how many of the pool's read
  connections are in use now. Pinned at 8 means every read walks a big
  history; pair it with `scion_pg_tx_age_seconds`.
- `scion_pg_tx_age_seconds` — (Postgres only) the age of the oldest open
  read transaction, sampled every 5 s. A transaction that grows past a few
  seconds holds one of the pool's connections and a repeatable-read
  snapshot, which keeps the database from cleaning old rows; find the
  client behind it (`pg_stat_activity`), and if walks like that are
  common, raise the pool size.

**Health.** The server, cache and daemon answer standard gRPC health checks
(`grpc.health.v1.Check` and `Watch`) on their serving port, without a
token: the server's health covers the services `scion.v1.Scion` and
`scion.v1.Objects`, a tier's covers `scion.v1.Objects`. The server
serves no gRPC reflection, so a probe tool must know the health protocol
itself (`grpc_health_probe` does).

Probe the service's name for readiness and the empty name for liveness.
Readiness is `NOT_SERVING` from the start of a drain to the exit, so a load
balancer stops sending work before the listeners close. Liveness stays
`SERVING` while the process answers — including through a drain — so the
supervisor restarts a wedged process, not a draining one. Kubernetes
probes read the same service natively: a readiness probe of
`grpc.health.v1` on the serving port with `service: scion.v1.Scion`, a
liveness probe with no service name. A daemon serves a Unix socket, where
probes do not apply; its stop is the same drain, and the daemon's client
reconnects.

**The log lines.** Every process logs to stderr with `tracing`; the lines
that mean something. Every line of a call carries the `call` span's
fields, `rpc`, `peer` and `request_id`: one `scn` command sends one
`x-request-id` through every hop, so a grep for `request_id=<id>` in the
logs of scn, the daemon, the cache and the server shows the whole path
of one operation.

- `the call is unauthenticated` — a call without a valid token. A few are
  normal (a scanner, a stale client); a stream of them is a client with a
  wrong token.
- `a TLS handshake failed` — a client (or an upstream tier) reached the
  port but could not complete TLS: a wrong certificate, a client that does
  not trust the CA, or a plain-TCP client against a TLS port.
- `a GetObjects ticket was refused` (with `reason`) — the ticket of an
  object read failed its check: expired, signed by another key, or not in
  the shape of the protocol. During a key rotation, check the tiers carry
  both public keys before you drop the old one.
- `may not read the repo` / `may not write the path` — a principal hit its
  access rules. Expected if your rules are strict; look at the `principal`
  field to see who.
- `a connection waits: the most connections are open` — the server is at
  `--max-connections` and new clients queue. Raise the cap or add caches.
- `the database stayed busy` / `the database stayed busy; the batch
  retries` — SQLite contention, usually an import beside a live server.
  Rare bursts are fine; constant ones mean the disk is too slow.
- `stored blob is corrupt` / `rewriting a corrupt blob file` — the store
  found damage on disk. Run `fsck`, then restore the named blobs from a
  backup.
- `upstream could not give an object` (on a tier, with `repo`, `key`,
  `status`) — the tier's upstream refused or lacked an object. A `NOT_FOUND`
  here is a ticket walk that never reached the object upstream; anything
  else is an upstream problem.
- `the stale-land sweep failed` / `the stale-land sweep task failed` — the
  hourly sweep could not run. Lands then pile up past
  `--land-stale-secs`; look at the error above it.

A ticket refusal, an access refusal, or a corrupt blob each log one line
per event, never the bytes or the token that caused them.
