# Operating Scion

How to run the server and its tiers, keep the data safe, and upgrade: the
steps an operator needs, written from the code on `main`. Every command in
this guide was run once in a fresh directory. The flags of a release are its
truth: `--help` of each binary lists everything.

The pieces:

- `scion-server` — the one process that stores and lands. One data directory
  holds everything: `scion.db` (metadata and trees, SQLite), `objects/` (one
  zstd file per blob), `tmp/` (staging), and three lock files. A second
  server on the same directory refuses to start.
- `scion-cache` — a regional tier in front of the server. Keeps objects in
  memory, answers object reads, forwards everything else.
- `scion-daemon` — one per machine user, in front of a cache. Keeps objects
  on disk, serves on a private Unix socket. One daemon serves one OS user.

## 1. Deployment shapes

### One server

```sh
./scion-server --data /srv/scion serve --token <TOKEN>
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

Choose the tokens with care: `--token T` makes one principal, `default`, with
every right on every repo. For several people or services, use an access
file instead (see "Tokens: one, or an access file", below).

### Server and a regional cache

A cache serves object reads to a region and forwards everything else. Each
hop has its own token pair: what the tier requires from its clients
(`--token`), and what it sends upstream (`--upstream-token`). A `scn`
client's `--token` is what it sends, the other side of the pair of the tier
in front of it.

```sh
./scion-cache --token <CACHE_TOKEN> --upstream-token <SERVER_TOKEN> \
  --upstream http://SERVER:7878 \
  --ticket-public-key <PUBLIC_KEY_HEX>
```

The cache needs `--ticket-public-key` (the hex that `ticket-key` printed):
without it, it serves no objects. A `scn` user then points reads at the
cache with `scn checkout --server http://SERVER:7878 --objects http://CACHE:7879`.

### Server, cache, and a daemon per user

```sh
./scion-daemon --data ~/.scion-daemon \
  --token <USER_TOKEN> --upstream-token <CACHE_TOKEN> \
  --upstream http://CACHE:7879
```

The daemon's data directory must be private to its user (mode 0700, not a
shared path such as `/tmp`); it serves on a 0600 Unix socket in it. Every
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
seconds) and start the daemon with `--seed FILE`. A snapshot holds every
object of its repo by ID, so snapshot only repos that ALL the node's users
may read WHOLE: a seeded daemon serves paths that folder read rules hide,
because it checks objects, not rules.

### Tokens: one, or an access file

`--token` and `--access` are exclusive. An access file names tokens by their
BLAKE3 digest (never the token itself) and maps each to a principal with
per-repo read and write rules:

```sh
TOK=$(./scion-server hash-token --token <SECRET>)
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

### Network and TLS

The server and the cache refuse a non-loopback address unless they serve TLS
(`--tls-cert` and `--tls-key`) or get `--insecure-listen`. A client trusts a
private CA: `scn --tls-ca ca.pem`, a tier for its upstream with its own
`--tls-ca`. Between tiers, either end can demand a client certificate
(`--tls-client-ca` on the server or cache; `--tls-client-cert` and
`--tls-client-key` on the cache or daemon). A bearer token over plain TCP to
a non-loopback address stays refused.

### In containers

`docker/compose.yml` brings up a server and a cache in one image, both with
TLS and a ticket key, and `docker/demo.sh` seeds a repo and checks it out
through the cache. It is the quickest end-to-end look at a two-tier
deployment, and a template for the real files (the secrets are made at
compose time, the server's data lives in a volume).

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
2. Restart the server with both keys: `--ticket-key NEW.key
   --ticket-key-old OLD-PUBLIC`. The old argument is a file holding the
   **public** key of the previous key (the hex that the old run printed),
   not its seed. A public key is not a secret: anyone may read the file,
   but group and others must not be able to WRITE it (a changed key breaks
   every tier that verifies against the old one), it must not be a
   symlink, and the directories above it must be private to its owner or
   root. `chmod go-w OLD-PUBLIC` fixes a copy tool's 0666. Tickets of
   either key verify while both are given.
3. Give the tiers both public keys, new first:
   `--ticket-public-key <NEW_HEX> --ticket-public-key <OLD_HEX>` (repeat the
   flag; two are the most). Restart the caches; a cache that verifies
   against both serves tickets of either key.
4. When no client can hold a ticket of the old key any more (tickets live 15
   minutes), restart the server without `--ticket-key-old` and the caches
   with only the new public key.

A daemon checks no ticket and forwards it, so it needs no key at any step.

## 3. Backup and restore

A data directory is `scion.db` plus `objects/`; the lock files and `tmp/`
are process state, not data. A copy of the directory is a backup:

```sh
cp -R /srv/scion /backups/scion-$(date +%F)
```

With the server stopped, the copy is exact. While the server runs, a copy
can tear a file that a commit is writing; `fsck` on the copy tells you what
state you got:

```sh
./scion-server --data /backups/scion-2026-10-17 fsck
```

`fsck` changes nothing and can run while the server runs: it checks every
blob file against its ID, every blob a row names, every changeset, tree and
history node against its ID, every ref and derived row, and every derived
value with its kind's decoder. It prints one line per problem (kind, ID,
path) and exits 0 when it saw no damage, 1 when it did, 2 when it could not
run. A clean report means "no damage seen", not a snapshot: it reads in
short batches. For a quiet backup, stop the server, copy, fsck the copy,
start the server again. To restore, stop the server, replace the data
directory with the backup, run `fsck` on it, start the server.

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
  first few times, until you trust the two together.
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
- `scion_lands_total` — lands by outcome.
- `scion_land_replans_total` — lands that planned again because their ref
  moved under them; a rising rate means lands contending with imports or
  other writers on one ref.
- `scion_cache_hits_total` / `scion_cache_misses_total` — what a tier
  served from its store against what it fetched upstream.

**Health.** The server, cache and daemon answer standard gRPC health checks
(`grpc.health.v1.Check` and `Watch`) on their serving port, without a
 token, for the services `scion.v1.Scion` and `scion.v1.Objects`. The server
serves no gRPC reflection, so a probe tool must know the health protocol
itself (`grpc_health_probe` does).

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
