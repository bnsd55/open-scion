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

### First boot

A first start on a fresh machine is a token file and three commands,
and each command promises something. The token goes in a file, not in
argv: any user of the machine reads argv with `ps`.

```sh
umask 077
printf %s "$SCION_TOKEN" > /srv/scion-token
./scion-server --data /srv/scion serve --token-file /srv/scion-token
```

The `umask 077` and the printf make the token file private (0600). A
data directory the server does not find is refused, not created: every
subcommand except restore answers "<path> does not exist; create it or
pass --data" (D25), so make the directory before the first command.
The serve command keeps
its files private, and serves. The log's `serving` line names the address.

```sh
grpc_health_probe -addr 127.0.0.1:7878 -service scion.v1.Scion
```

The readiness probe answers `SERVING` once the server listens; the empty
service name is the liveness probe (`grpc_health_probe -addr
127.0.0.1:7878` with no `-service`).

```sh
git -C /srv/git/my-repo log --oneline -1
./scion-server --data /srv/scion import /srv/git/my-repo
```

The first import makes the repo (named `main` unless `--repo` says
otherwise), and a client reads it back:

```sh
SCION_SERVER=http://127.0.0.1:7878 scn --token-file /srv/scion-token log main
```

The client's global flags (`--server`, `--token-file`) go before the
subcommand; the environment forms are `SCION_SERVER` and `SCION_TOKEN`.
The import can run while the server serves.

Proof: crates/scion-server/tests/first_boot.rs::a_first_boot_serves_probes_and_reads_back

### One server

```sh
install -d -m 0700 /srv/scion
SCION_TOKEN=<TOKEN> ./scion-server --data /srv/scion serve
```

That is the whole deployment: clients talk to `127.0.0.1:7878` (or `--listen`
HOST:PORT). `install -d -m 0700` makes the directory the mode it says, whatever
the umask is - under umask 002 (Ubuntu's user private groups) a plain `mkdir`
makes a group-writable directory, and the server refuses it; the error names
the chmod that fixes it. The data directory holds every blob and every row, so keep it
mode 0700 and owned by the user the server runs as: the server refuses one
that group or others can write, or that sits under a directory another user
can change. Without `--ticket-key` no object is readable, so a real server
adds a ticket key (section 2):

```sh
./scion-server --data /srv/scion ticket-key --out /srv/scion-keys/ticket.key \
  | tee /srv/scion-keys/ticket.pub   # prints the public key
chmod 600 /srv/scion-keys/ticket.key
./scion-server --data /srv/scion serve --token <TOKEN> \
  --ticket-key /srv/scion-keys/ticket.key
```

The public key lands in `ticket.pub`, kept for the next rotation's
`--ticket-key-old` (section 2).

`ticket-key` records its run in the data directory's audit log, so it
takes the same `--data` as the serve line.

Size the process with the three connection caps: `--max-connections` (128)
bounds the clients served at once, `--max-streams` (256) bounds the calls
one client may hold open at once, and `--max-stalled-bytes` (64 MiB)
bounds what the stalled replies of one connection may hold before the
connection is closed. A cache and a daemon take the same three flags.

What one server process can hold for clients that never read, at the
defaults, in the worst case:

    128 connections x 64 MiB stalled bytes   8192 MiB
    the read budget (whole-object replies)     256 MiB
    the reply budget (unary replies)           128 MiB
    ------------------------------------------ --------
    about                                      8.4 GiB

The first line is `--max-connections` x `--max-stalled-bytes`
(`DEFAULT_CONNECTIONS`, `DEFAULT_STALLED_BYTES` in
crates/scion-proto/src/runtime/listen.rs): a connection whose stalled
replies hold more than `--max-stalled-bytes` is closed, and what they
hold goes free, so a slow client costs at most its own connection,
not a share of the server. The second is the read budget that a
whole-object reply's built body takes before the body is built
(`READ_BUDGET_BYTES` in crates/scion-server/src/service.rs); the
third is the unary-reply budget (`REPLY_MEMORY_BUDGET` in
crates/scion-server/src/reply.rs). Size the server's memory from that
sum: fewer connections, a smaller stall cap, or both, take the first
line down linearly.


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
second server on the same data directory (or the same Postgres schema)
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

Proof: crates/scion-server/tests/backend_refusals.rs::a_lost_server_lock_fences_the_backend (behaviour, no command: the section's serve command is not in the test's path - the test works at the database's own session lock, and the fence it proves is what the runbook's one-server rule protects)
Proof: crates/scion-server/tests/backend_refusals.rs::an_unreachable_database_is_not_a_takeover (behaviour, no command: the section's steps are actions on the database and its container, not scion commands; the test holds the behaviour the runbook promises - a stopped database is not a takeover)
Proof: crates/scion-server/tests/crash.rs::every_reply_survives_a_kill_of_the_server
Proof: crates/scion-server/tests/first_boot.rs::a_first_boot_serves_probes_and_reads_back

#### Stopping it for real: a unit file

An operator does not run the server in a shell; it runs under a
supervisor. The minimal systemd unit — the token through the file
(§1), the restart on a crash, and the drain on a stop — is:

```ini
# /etc/systemd/system/scion.service
[Service]
ExecStart=/usr/local/bin/scion-server --data /srv/scion serve \
  --token-file /etc/scion/token \
  --ticket-key /srv/scion-keys/ticket.key
Restart=on-failure
User=scion

[Install]
WantedBy=multi-user.target
```

`systemctl stop scion` sends SIGTERM, and the drain of the last
section is what happens next. Never kill by pattern: on a machine that
runs more than one service, `pkill -f scion-server` kills every
project's server, not yours. Stop by unit, or by the exact PID of the
process you started.

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

The URL carries the database's credentials, so give it as a file the
owner alone may read, not on the command line: a value on the command
line is readable by every local account (`ps`), an environment value
by the same user and root, and a file with the checks of the access
file by whoever can read it.

```sh
printf 'postgres://user@dbhost/scion?sslmode=require\n' > /srv/scion/db-url
chmod 600 /srv/scion/db-url
scion-server --data /srv/scion \
  --postgres-url-file /srv/scion/db-url \
  --postgres-ca /srv/scion/db-ca.pem serve --access /srv/scion/access.txt
```

For containers and units that hold no secret files, the environment
form is second best:

```sh
SCION_POSTGRES_URL='postgres://user@dbhost/scion?sslmode=require' \
  scion-server --data /srv/scion serve --access /srv/scion/access.txt
```

- `--postgres-url-file` (or `SCION_POSTGRES_URL`) names the database.
  `--postgres-url` takes the same value on the command line; it is
  deprecated: it warns now and goes away in a later release, because
  the process list shows its value to every local account. Off this
  machine the URL must ask for TLS (`sslmode=require`): the open
  refuses a remote URL that does not.
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
- One server process serves one store - a data directory, or a Postgres
  schema: a second `serve` on the same `--postgres-url` (and schema)
  fails at its open with "another server holds this store". An import
  does not take that lock and runs beside a server.
- The server opens one write connection and a pool of 8 read
  connections, plus the short-lived connections of a migration or an
  import. Size the database's `max_connections` for all of it: the
  server's own 9, the operator's tooling beside it (`pg_dump`, `psql`,
  a metrics scraper), and a margin. A database at its connection cap
  refuses the server's next connection, and every call that needs one
  answers `unavailable` until one is free.
- The one-server rule and the write queue are Postgres advisory locks,
  visible as `pg_locks` rows of kind `advisory`: the one-server lock is
  a key of the schema `current_schema()` names, the migration lock is a
  fixed key of the database, and every
  write transaction also takes the schema's collector key
  (`pg_advisory_xact_lock(hashtext(current_schema() || ':gc'))`),
  which orders it against the collector's sweeps and gives up on that
  key alone after 60 s. An `advisory` row in `pg_locks` is one of
  these, held by this server by design, not a stuck client to kill.
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
  are not on this disk. A directory whose marker names a store also
  refuses a database that holds no store at all - the usual cause is a
  `--postgres-url` whose search_path names a schema no store ever
  served - so the mistake fails at the start, not as "repo not found"
  on every later call.
- One server process serves one database, and it stays that way: if the
  server's connection drops and the lock is lost to another process, the
  server is fenced. Every read and write fails with the loss named, the
  health service answers `NOT_SERVING` on the same port, and after a few
  seconds the process exits with code 70, so a supervisor restart finds
  the lock taken and refuses, or takes it back on an empty database. A
  session checks itself once a minute, so a loss is seen even while no
  client calls: the check runs on the server's own session, through the
  same queue as every statement, never on a second connection.
- A start after an unclean kill can refuse its open with "another
  server holds this database": the dead server's session is still in
  the database, and the one-server lock dies only when Postgres ends
  that session. On the same host that is at once - the kernel closes a
  dead process's sockets - so the service manager's restart
  (`Restart=on-failure`) is the whole recovery: the first start exits,
  the next takes the lock. A server whose host vanished without a
  packet is another matter: Postgres learns of that death only through
  TCP keepalives, whose clock is the database's `tcp_keepalives_idle`
  (unset, it falls back to the system default, often 2 hours), so end
  the dead session by hand (`pg_terminate_backend`) or lower that
  clock where a lost host must free the lock sooner. The server's own
  connections carry the tighter bounds (`keepalives_idle` 30 s, a 30 s
  TCP user timeout), so the reverse - this server noticing a dead
  database - is bounded by the server, not the database.
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

### The audit log

The server records an audit log: an append-only JSON-lines file of
refused authentication (counted per peer), writes, ref changes, lands
and administrative changes, with no tokens and no contents. It lives at
`audit.log` in the data directory; `--audit-file` puts it elsewhere.
The CLI's state-changing subcommands record their own admin lines in
the same file: one intent before a change, and one result after it.
Every state-changing command is covered: gc, import, restore, derive,
ticket-key generation, and the schema migration an open performs. The
restore's lines land in the restored store's own
audit file, so its history opens with the restore that made it. The
collector is the runbook's cure for a full disk, so a gc whose intent
cannot go in because the disk is full runs anyway, and records its
intent, marked `deferred: disk_full`, after it freed the space. The
migration's line has no intent before it: it ran inside the store's
open, before the audit could exist, so its record, marked with the
versions it moved, goes in as soon as the audit exists.
There is no way to turn it off: a server that cannot record refuses the
changes it cannot account for, and flips readiness until a restart. The
file is mode 0600, never a symlink, in a directory only the server's
user may write.

The lines of concurrent changes can appear in a different order than the
changes themselves: each job records its result after its change. Each
ref line names its old and new head, so a ref's history is rebuilt by
linking old to new, not by line order.

Every intent line gets one result line: the store job that claimed the
change writes it, and a call that dies before its job starts has its
drop answer `cancelled`; a land the sweep rejects at the next start is
answered by the sweep (`why: stale`) — that line carries a fresh
request id of its own, because the sweep does not know the intent's:
the auditor joins it to the intent on the request key and the land id.
One change is left where the result can be missing: a job that dies
after it claimed the call but before it recorded anything - the server
log then carries what the audit line cannot.

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

### Rotating the server's certificate

The client trusts a CA, not the server's certificate: a certificate the
same CA signed, written to the server's `--tls-cert` and `--tls-key`
files, needs no client change. The rotation is:

1. Make the new certificate, signed by the CA the clients already hold,
   and write it over the server's cert and key files (`chmod 600` the
   key; the certificate may be 0644).
2. Restart the server: a stop (the drain protects the calls in flight)
   and a start with the same `--tls-cert` and `--tls-key` paths, which
   now hold the new pair.
3. The client's promise: its `--tls-ca` file did not change, and it
   connects.

A NEW CA is the other case: every client and tier that holds the old CA
file must get the new one BEFORE the server moves, or the clients it
serves cannot verify it. The swap on live clients is the recorded drill
(docs/drills/, template there), not a CI test: no test can hold the
clients' files of a running fleet.

Proof: crates/scn/tests/tls.rs::a_rotated_server_certificate_needs_no_client_change (behaviour, no command: the section's steps are openssl and flag changes, not scion commands; the test holds the behaviour - an old certificate keeps working until the rotation lands, and a rotated one needs no client change)

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

Every flag that takes a secret's value also has a file form
(`--x-file`) and an environment form. The file form is the deployment
form: the owner alone may read it, and a symlink is refused. The
environment value is visible to the same user and to root. The argv
form is deprecated: it warns now and goes away in a later release, and
its value is readable by every local account through `ps`.

| Variable | Flag | Read by | Notes |
| --- | --- | --- | --- |
| `SCION_DATA` | `--data` | `scion-server` | Data directory, default `scion-data`. |
| `SCION_POSTGRES_URL` | `--postgres-url` | `scion-server` | The URL holds the database credentials; the environment keeps them out of the process list (`ps`). `--postgres-url-file` reads it from a file the owner alone may read, and the argv form is deprecated: it warns now and goes away in a later release. |
| `SCION_POSTGRES_URL_FILE` | `--postgres-url-file` | `scion-server` | A file that holds the database URL. Only its owner may read it, and a symlink is refused. |
| `SCION_TOKEN` | `--token` | `scion-server` (`serve`, `hash-token`), `scion-cache`, `scion-daemon`, `scn` | A secret; the environment keeps it out of the process list. The `--token-file` flag reads the token from a file the owner alone may read, and `hash-token` reads it from stdin. The argv form prints a deprecation warning and goes away in a later release. |
| `SCION_UPSTREAM_TOKEN` | `--upstream-token` | `scion-cache`, `scion-daemon` | A secret, as `SCION_TOKEN`; `--upstream-token-file` reads it from a file, and the argv form is deprecated: it warns now and goes away in a later release. |
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

1. Make the new key, and capture its public key into its own file, kept
   for the next rotation:
   `scion-server --data /srv/scion ticket-key --out NEW.key | tee NEW.pub`
   — it prints the new public key as 64 hex characters.
2. Give the tiers both public keys, new first, **before** the server moves
   to the new key — a cache that verifies only the old key refuses every
   ticket the new server mints:
   `--ticket-public-key <NEW_HEX> --ticket-public-key <OLD_HEX>` (repeat the
   flag; two are the most). Restart the caches; a cache that verifies
   against both serves tickets of either key.
3. Restart the server with both keys: `--ticket-key NEW.key
   --ticket-key-old OLD-PUBLIC`. The old argument is the previous key's
   `.pub` file - the public key that the run which made the **previous**
   key captured (step 1 the last time), not its seed, and not this
   rotation's new key. The file's text is trimmed when read, so a
   trailing newline is fine; what it holds is the bare 64-hex public
   key and nothing else. A public key is
   not a secret: anyone may read the file, but group and others must not
   be able to WRITE it (a changed key breaks every tier that verifies
   against the old one), it must not be a symlink, and the directories
   above it must be private to its owner or root. `chmod go-w
   OLD-PUBLIC` fixes a copy tool's 0666. Tickets of either key verify
   while both are given.
4. When no client can hold a ticket of the old key any more (tickets live 15
   minutes), restart the server without `--ticket-key-old` and the caches
   with only the new public key.

A daemon checks no ticket and forwards it, so it needs no key at any step.

Proof: crates/scn/tests/rotation.rs::ticket_key_rotation_while_a_checkout_reads (behaviour, no command: the section's steps are server restarts and cache-key copies, not one command a test can run; the test holds the behaviour - tickets of either key verify while both are given, and a checkout that reads through the rotation never fails)

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

Proof: crates/scion-server/tests/power.rs::every_reply_survives_a_power_cut (behaviour, no command: the proof is the power cut the LazyFS device delivers at the disk boundary, which no shell command reproduces)
Proof: crates/scion-server/tests/dm_log_writes.rs::an_acknowledged_blob_survives_a_cut_at_the_last_barrier_before_its_reply (behaviour, no command: the proof is a cut at the device's last barrier before the reply, delivered by the dm-log device, which no shell command reproduces)
Proof: crates/scion-server/tests/crash.rs::every_reply_survives_a_kill_of_the_server (behaviour, no command: the proof is the kill's own timing - a reply answered and the process killed mid-flight - which no guide command reproduces)
Proof: crates/scion-server/tests/storage.rs::a_retry_after_a_failed_dir_fsync_makes_the_kept_file_durable (behaviour, no command: the proof injects the fsync failure at the directory boundary, which a shell cannot order; the retry it forces is the runbook's promise)

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

The restore takes a measured time, not a promise: the `restore-bench`
workflow (a run on `main` or a dispatch, on the fixed `ubuntu-24.04`
runner, never a local machine) restores a stated store — 40 repos,
12000 changesets, 750 MiB of raw blob content in files of about 64 KiB,
every blob distinct and incompressible — with `fsck`, an open and a
read, prints the seconds, and fails past a 600 s bound. The measured
time includes the fsync of every restored file and directory: a
restore is done when a crash cannot lose it. A real store's restore
time scales with its blob bytes and its changeset count. On the fixed
runner that store restored in 10.5 s (run 39539498175: 72 MiB/s on
disk, 1147 changesets/s; the 750 MiB on disk matched the raw bytes,
and the store built in 28.0 s). One sample at one size; a second size
is the next step when the number starts steering a decision. The
current numbers live in the workflow's run log on `main`, and a run
that drifts far from them is a regression to look at, not a promise
to restate here.

Proof: crates/scion-server/tests/restore_bench.rs::a_restore_of_a_stated_store_takes_a_stated_time

Proof: crates/scion-server/tests/backup.rs::a_backup_taken_while_serving_restores_whole

## 4. Upgrades

The schema version lives in the database. On open, a server migrates an
older schema forward, one migration per released version, each in one
transaction, with its progress in the log (a migration can take a while:
schema 4 walks every repo's changesets; on Postgres, schema 5 renumbers
the changeset parents from one to zero, the numbering SQLite always
wrote). Two rules:

- A database with a **newer** schema than the binary refuses to open:
  `corrupt data: database schema version 7 is newer than this build (6)`.
  Run the old binary until you can upgrade, or restore a backup. Never edit
  `user_version` by hand.
- If other processes (an `import`, a `fsck`) share the directory, a
  migration waits for them to stop: the error says `stop the other processes
  that use ...`. One process at a time migrates.

### A failed migration

A migration that cannot finish leaves the database at one whole step, so
the two failures an operator sees are the two rules above, and the runbook
is the rule that names the case:

1. The open refuses with `schema version N is newer than this build`:
   the data ran on a newer build than this binary. The fix is the old
   binary, or the restore of section 3. Never edit `user_version` by
   hand: the version is the migration's bookmark, and a hand-moved one
   leaves the database half-migrated in a way no migration can finish.
2. The start waits with `stop the other processes that use ...`: another
   process holds the directory. Stop it; the migration runs on the next
   start.

After the fix, the server starts and serves; `fsck` says the store is
whole.

Proof: crates/scion-server/tests/crash.rs::a_newer_schema_refuses_the_open_and_a_restore_serves (behaviour, no command: the section states the two failure rules in prose; the proof is the refusal and the restore those rules name)


An import reads git revision `--rev` (default `HEAD`) into scion repo
`--repo` (default `main`), and points scion ref `--ref` (default `main`)
at the imported tip. Besides every root and merge, it re-checks `--verify`
(default 100) ordinary commits against git. A commit whose author or
committer line carries git's unknown-offset zone `-0000` is refused: the
offset is unknown, not zero, and importing it would fold the sign into
the stored history. Every real zone imports with its offset; fix or
re-date such a commit in the git history and import again.

So an upgrade is: stop the server (and any tiers that share the directory —
none do by default), copy the directory as a backup (section 3), start the
new binary, watch the log for the migration, run `fsck`. A store that
has data logs its migration steps when a build migrates it.

Where the binaries come from. v0.1.0 is a source release: build the
version you need from its signed tag (`docs/RELEASING.md`):

```sh
git checkout <tag>
cargo build --release --locked
```

A release that ships binaries says so in its release notes, and
`docs/verify.md` verifies them. Whichever way the binaries came, keep
the previous build's binaries beside the new ones **before** an
upgrade: the rollback below needs them, because rebuilding the old tag
mid-incident needs the toolchain and the time you do not have.

Schema 7 (tree
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

Proof: crates/scion-server/tests/crash.rs::a_kill_during_a_schema_upgrade_leaves_the_old_schema_or_the_new_one (behaviour, no command: the section's upgrade steps are prose; the proof is the kill's own timing inside the migration)

### Rollback

The rollback depends on what changed: if the schema did not change, run
the old binary on the data as it is; if it did, restore the pre-upgrade
backup (section 3) and run the old binary on the copy. After a
migration, rollback means restoring the backup taken before the
upgrade: the old binary refuses the new schema - that refusal is the
stop, and the backup is the recovery. Never edit `user_version` by
hand - the version is the migration's bookmark, and a hand-moved one
leaves the database half-migrated in a way no migration can finish.
The run-the-old-binary arm is the version matrix's drill, #431: the
pre-upgrade backup restores and serves under the N-1 binary. The
restore arm is the failed-migration section's Proof: the test forges a
newer database, sees the refusal, restores the pre-upgrade backup, and
serves.

## 5. Routines

The routines are these:

- `fsck` on a schedule (it is read-only, so it can run beside a live
  server), and always on a fresh backup copy. Exit 1 with the problem lines
  is the signal to restore from a backup; there is no automatic repair.

  An fsck failure (exit 1) is the restore runbook of section 3, and the
  test that proves it is the failing-disk runbook's: damage on disk,
  the `fsck` command to its exit 1 with the problem named, the restore
  into a fresh directory, and fsck clean on the restored copy before
  the server starts.

  Proof: crates/scion-server/tests/storage.rs::a_failing_disk_runbook_fscks_restores_and_serves (behaviour, no command: the section's only command block is the collector's; the fsck-restore proof holds the stop-and-restore behaviour the guide states in prose)
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

### Disk full

The disk the data directory sits on has two alerts: `DiskNearlyFull` at
10 GiB free and `DiskFull` at 1 GiB free. At either one:

1. Run the gc routine (above): first a `--dry-run` to see what it would
   collect, then a real run. The collector is safe beside a live server,
   and on a store whose repos grew by accident (a forgotten import, a
   test repo nobody removed), this alone frees the space.
2. Run `fsck` when the run ends. A full disk can leave a write that
   failed mid-way; fsck tells you whether anything was damaged (there
   is no repair: restore from a backup, section 3, when it reports one).
3. If gc collected too little, the data is bigger than the disk: grow
   the volume, or move the data directory to a bigger one (a stop, a
   copy per section 3, a start at the new path). Growing a mounted
   volume needs no restart: the next write sees the new size.

Nothing in the store needs repair because the disk filled: a write that
failed out-of-space left no torn file, and when space is back, writes
resume with no restart. A blob whose upload was refused must be uploaded
again by the client; the server never kept half of it.

Proof: crates/scion-server/tests/disk_full.rs::a_disk_full_runbook_gcs_then_writes_resume (behaviour, no command: the section's steps point at the gc routine and fsck, which the storage tests run their own way; conversion to the helper is pending)

### A failing disk

A disk that fails reads does not fail writes: a read whose file the disk
cannot serve answers an internal error (the client retries into the same
device, so do not loop), and the rest of the store keeps serving. On the
first internal error from a read:

1. Run `fsck` (it is read-only, so it can run beside the live server).
   Exit 1 with the problem lines names the damage; the store has no
   repair.
2. If fsck reports damage, restore from a backup (section 3): a fresh
   directory gets the metadata snapshot and the objects copy, fsck runs
   on it, and the server starts at the new path. A read that failed and
   later succeeds means the disk recovered, not that the damage was
   never there: run fsck anyway.

Proof: crates/scion-server/tests/storage.rs::a_failing_disk_runbook_fscks_restores_and_serves (behaviour, no command: the section's steps are stop, fsck, restore, and start, which the storage test holds as behaviour; conversion to the helper is pending)

### A Postgres restart

A Postgres-backed server keeps running when the database restarts under
it: the calls that touch the database answer while it is down (a write
answers the busy error a client retries against), the store does not
fence itself (an unreachable database is no proof that another process
took the data), and no server restart is part of the fix. When the
database is back, the next write reconnects, takes the advisory lock
again, and runs; nothing needs cleaning up by hand. The runbook is:

1. Watch `scion_pg_pool_wait_seconds` and the busy errors: they tell a
   database that is down from one that is slow.
2. Fix the database (restart the container or the service); the server
   needs nothing.
3. After the restart, one write is the check: it runs, and the reads
   behind it see the data. If a write keeps failing after the database
   is back, the fence is the suspect: the fence is section 1's "One
   server process serves one database" - a fenced server exits 70, and
   a supervisor restart of it takes the lock back or refuses.

The one thing not to do: do not restart the server while the database
is down. A start against an unreachable database fails, and a restart
throws away the sessions that would reconnect on their own.

Proof: crates/scion-server/tests/backend_refusals.rs::an_unreachable_database_is_not_a_takeover (behaviour, no command: the section's steps are actions on the database container, not scion commands; the test holds the behaviour the runbook promises)

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
itself (`grpc_health_probe` does). Get it from that project's GitHub
releases (a single static binary per platform — no other install), or
use Kubernetes probes, which speak the same protocol natively.

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
logs of scn and the server shows the whole path of one operation. A
cache or a daemon logs a call only at DEBUG — its per-call counts are
the metrics' `scion_cache_hits_total` and `scion_cache_misses_total` —
so at the default level those hops answer through the metrics, not the
log.

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
- `a unary reply refused: the reply budget is spent` — a client holds
  more than the reply budget (128 MiB of unary replies it never read,
  across every connection); further unary reads are refused
  `resource_exhausted` until it reads what it opened or its connections
  close. The warn carries only the size, but the line sits in the
  call's span, whose fields name the client: every log line of a call
  carries `rpc`, `peer` and `request_id`.
- `a committed write's reply is over the reply budget: sent uncounted` —
  the budget was spent, but the call changed state and its answer went
  out anyway: the write landed and the client got the truth. Nothing was
  lost; fix the client that holds the budget.

A ticket refusal, an access refusal, or a corrupt blob each log one line
per event, never the bytes or the token that caused them.

### Latency p99 high

`LatencyP99High` fires when one RPC's 99th-percentile duration has sat
over 4 s for 15 minutes. One slow RPC is that call's path; every RPC
slow at once is the machine or the store under everything.

Find the RPC and the bucket its p99 sits in — the first `le` whose
cumulative count reaches 99% of the RPC's calls:

```sh
curl -s 127.0.0.1:9100/metrics | awk '
  /^scion_request_seconds_bucket/ {
    match($0, /rpc="[^"]*"/); rpc = substr($0, RSTART + 5, RLENGTH - 6)
    match($0, /le="[^"]*"/);  le  = substr($0, RSTART + 4, RLENGTH - 5)
    n[rpc]++; b[rpc, n[rpc]] = le; c[rpc, n[rpc]] = $NF
  }
  END {
    for (r in n) {
      total = c[r, n[r]] + 0
      for (i = 1; i <= n[r]; i++)
        if (c[r, i] + 0 >= 0.99 * total) { print r, "p99 in", b[r, i]; break }
    }
  }'
```

Tell the causes apart by which RPC is slow:

- A write RPC (`Scion/Land`, `Scion/CreateChangeset`): the store's
  fsyncs. `BlobSyncSlow` fires beside it when the mean fsync passes 2 s;
  if it does, the drive is the cause, and the fix is faster storage or
  fewer imports beside serving (section 5's import routine).
- A read RPC (`Objects/GetObjects`, `Objects/ReadFile`):
  `scion_cache_misses_total` rising on the tier in front points at cache
  misses (the tier miss rate runbook below); `scion_pg_pool_wait_seconds`
  with a rising tail (Postgres) points at readers held by long walks
  (the pool metrics above); a partial reader's slow first read of a rule
  set is the view memo warming (the runbook below).
- Every RPC: the host is short of CPU, memory or disk headroom; the
  fixes are off the server (move imports, scale out, add a cache).

Escalate when a read RPC's p99 stays over 4 s with the caches warm and
the drive quiet: that is a store shape a restart does not fix, and it
needs the store's owner.

Proof: crates/scion-server/tests/docs_alerts.rs::latency_p99_names_the_slow_rpc_from_the_served_histogram

### View memo bypassed

`ViewMemoBypassed` fires when the rate of `scion_view_trees_walked_total`
stays over 5 per second for 30 minutes: partial readers' view
derivations keep opening real trees instead of finding the per-tree
memo rows the derivation cached before (the `view_trees` table).

Read the counter twice, a minute apart, and subtract:

```sh
curl -s 127.0.0.1:9100/metrics | grep '^scion_view_trees_walked_total'
```

Tell the causes apart:

- The rate is high but falling, and the repo's tip moves fast: every new
  changeset re-derives the folders its changes touched. That is the
  design working — a new changeset walks only what moved — and no fix
  applies. The alert's 30-minute `for` exists so this warmup does not
  fire it.
- The rate is high and steady: the same (tree, context) pairs derive
  again and again. The memo rows were swept: rows of a rule-set digest
  that no grant hashes to any more die in bounded steps, so a recent
  change to the access file's read rules (or a projection upgrade) makes
  every read walk until the rows rebuild. Restore the rules, or let the
  first reads pay and the rate falls.
- The rate is high, steady, and `LatencyP99High` fires on a read RPC
  with it: the derivations hold the store's readers. On Postgres,
  `scion_pg_readers_busy` pinned at 8 with a growing
  `scion_pg_tx_age_seconds` says the walks hold the pool; stagger the
  readers, so the walks do not all run at once.

Escalate when steady high walks and read latency land together and the
access file has not changed for days: the working set outgrew the
memo's shape, and it needs the store's owner.

Proof: crates/scion-server/tests/docs_alerts.rs::view_memo_serves_the_second_read_of_one_changeset

### Stalled replies high

`StalledRepliesHigh` fires when `scion_stalled_reply_bytes` has sat over
48 MiB for 15 minutes: replies whose clients stopped reading mid-reply
hold their buffers in the server. The cap is `--max-stalled-bytes`
(default 64 MiB) per connection; a connection over it is closed with
everything it holds, and the server logs
`closing a connection: its stalled replies hold too much` once, with the
bytes it held.

Read the gauge and find the client that stopped:

```sh
curl -s 127.0.0.1:9100/metrics | grep '^scion_stalled_reply_bytes'
grep 'closing a connection' server.log
```

Tell the causes apart:

- One client, once: a crashed reader or a paused consumer. The cap
  already handled it — the buffers went with the connection. No fix.
- The same peer, again and again: its reader is slower than the
  server's producer. Fix the client (read the reply as it streams; the
  close notice names the peer address, and the calls of one client
  carry its `request_id` in both logs).
- Many clients, bytes near the cap on several connections: the replies
  are large and the clients are many — raise
  `--max-stalled-bytes` only if the box has the memory for it, or add a
  cache tier so the large replies do not all reach the server.

- Many clients leave whole unary replies unread: they hold the server's
  reply budget (128 MiB), and further unary reads are refused
  `resource_exhausted` until they read or disconnect. This alert does
  not fire for them — the gauge counts stalled streams, not parked
  whole replies — the refusal's warn line
  (`a unary reply refused: the reply budget is spent`) and the RPC's
  `resource_exhausted` count name it.

Escalate when the close notice repeats for one peer and the client is
not yours to fix: the client's owner stops the waste, not the server.

Proof: crates/scion-server/tests/log_stream.rs::a_stalled_log_reply_is_counted_and_closes_the_connection (behaviour, no command: the section's commands read a live server's metrics; the test holds the stall, the count, and the close on a paused clock, where a shell client would deadlock)

### Tier miss rate

`TierMissRate` fires when a tier's misses have held over half its reads
for 30 minutes: the tier fetches from upstream what it should have
served from its own store.

Read the two counters and divide — misses over all reads:

```sh
curl -s 127.0.0.1:9100/metrics \
  | awk '/^scion_cache_hits_total/ {h = $NF}
         /^scion_cache_misses_total/ {m = $NF}
         END {if (h + m > 0) print m / (h + m)}'
```

Tell the causes apart:

- The ratio falls over the first minutes after a tier restart: the
  memory store warms from the reads it serves. No fix. A cache cannot
  refill itself from its own snapshot file at start; what the snapshot
  is for is the daemon's warm start: the cache writes the file with
  `--snapshot`, and a daemon starts from it with `--seed` (section 1).
- The ratio is steady high and `CacheDown` is not firing: the working
  set does not fit the tier's memory. Raise `--max-bytes` (and
  `--snapshot-bytes`, so the snapshot can hold it), or put a tier near
  each group of users instead of one tier for all.
- The ratio is 1: nothing ever hits. The tier's store never sees a
  second read of the same object — the clients are cold readers of
  disjoint data, and a cache is the wrong tool for them; point the
  clients at the server or accept the misses.

Escalate when the ratio stays high with the caps raised and the box's
memory free: the tier's placement or shape needs its owner.

Proof: crates/scion-server/tests/docs_alerts.rs::tier_miss_ratio_falls_when_the_store_serves_the_second_read

## 7. Capacity

What one server takes, measured on one named machine, so the numbers have
a bottom. The table below is the published envelope
([#387](https://github.com/bnsd55/openscion/issues/387)): every cell is a
run of [`scripts/capacity.sh`](../scripts/capacity.sh) on a GitHub-hosted
runner of the class named in the caption, with the dataset of
[benchmarks.md](benchmarks.md) (tokio at a pinned commit), SQLite or
Postgres behind the server, `scn bench` landing through it and readers
checking out through a cache and a daemon beside it.

| | |
|---|---|
| Commit | ab6bfdc92314fc41d2a8c5dca0f0a13bd39b5612 the main commit the run measured (the run's own HEAD, fcb947e6, adds the grid's database name only), at [run 40119721010](https://github.com/bnsd55/openscion/actions/runs/40119721010) |
| Runner | the hosted runner class, by its public label and spec |
| Filesystem | the filesystem the run measured, from the run's `machine.txt` artifact (`df -T` on the work directory); the `ubuntu-24.04` class's root disk is ext4 ([support.md](support.md)) |
| Repetitions | 2 per cell; the table reports the cell's whole run, and the run's raw samples are the workflow run's `capacity.csv` artifact |

| backend | writers | readers | land p99 ms | checkout p50 ms | checkout p99 ms | timeouts | errors | server RSS MiB | landed | blob store KiB per 1k lands | database KiB per 1k lands |
|---|---|---|---|---|---|---|---|---|---|---|---|
| sqlite | 2 | 0 | 15 | - | - | 0 | 0 | 24 | 2000 | 112318 | - |
| sqlite | 2 | 5 | 31 | 172 | 238 | 0 | 0 | 31 | 2000 | 112324 | - |
| sqlite | 2 | 20 | 103 | 683 | 827 | 0 | 0 | 32 | 2000 | 112366 | - |
| sqlite | 50 | 0 | 228 | - | - | 0 | 0 | 43 | 2000 | 58798 | - |
| sqlite | 50 | 5 | 490 | 173 | 243 | 0 | 0 | 50 | 2000 | 58848 | - |
| sqlite | 50 | 20 | 2057 | 685 | 839 | 0 | 0 | 50 | 2000 | 58882 | - |
| sqlite | 200 | 0 | 958 | - | - | 0 | 0 | 57 | 2000 | 78358 | - |
| sqlite | 200 | 5 | 2170 | 175 | 248 | 0 | 0 | 74 | 2000 | 78384 | - |
| sqlite | 200 | 20 | 9924 | 684 | 830 | 0 | 0 | 77 | 2000 | 78364 | - |
| postgres | 2 | 0 | 64 | - | - | 0 | 0 | 20 | 2000 | 5668 | 87210 |
| postgres | 2 | 5 | 124 | 253 | 298 | 0 | 0 | 24 | 2000 | 5668 | 87530 |
| postgres | 2 | 20 | 243 | 879 | 1071 | 0 | 0 | 27 | 2000 | 5668 | 86618 |
| postgres | 50 | 0 | 2309 | - | - | 0 | 0 | 33 | 2000 | 5668 | 34674 |
| postgres | 50 | 5 | 4454 | 254 | 301 | 0 | 0 | 38 | 2000 | 5668 | 34558 |
| postgres | 50 | 20 | 7042 | 877 | 1079 | 0 | 0 | 38 | 2000 | 5664 | 34482 |
| postgres | 200 | 0 | 9326 | - | - | 0 | 0 | 45 | 2000 | 5668 | 62428 |
| postgres | 200 | 5 | 18244 | 254 | 296 | 0 | 0 | 63 | 2000 | 5666 | 62366 |
| postgres | 200 | 20 | 31584 | 869 | 1054 | 0 | 0 | 66 | 2000 | 5668 | 62406 |

Every cell runs on a store the cell's own import made: on SQLite a data directory
the cell created, on Postgres a database the cell dropped and
recreated, so a cell's numbers are the store's at its own size, and
the growth column is the cell's own lands' bytes. The timeouts column
counts reader attempts the 60-second checkout timeout killed; it is
its own column because a hang is a finding, not noise. The errors
column
is part of the data, not a failure of the run: the envelope's edge IS
where errors start, so a cell whose clients failed is recorded with its
error count and stands in the table. The edge, the point where a cell
stops being an answer: a checkout p99 over 5 seconds, a land p99 over
5 seconds, or a client error outside the accepted classes of the soak -
in the table, that is a cell with errors above 0. A reader who wants a
different edge applies it to the run's `capacity.csv` artifact, which
the footer's workflow run keeps. A cell whose bench died names no land
percentile (`-` there).

The growth splits into two columns, both divided by the lands the
server's counter measured - a cell that lost lands reports its own
bytes over its own lands, never the plan's. The blob store column
measures the cell's data directory; the database column measures the
database the cell reset (`pg_database_size`), so a Postgres row cannot
look complete while the rows it grew sit unmeasured in the service
container. SQLite has no separate database, so its database column is
"-". RSS is the server's
at the end of the cell: a capacity cell is minutes long, not the hours a
warm-up would need, so it is a level, not a leak bound - the leak bound
is the soak's.

Recovery is a measured time too, not a promise: the `recovery-bench`
workflow (a run on `main` or a dispatch, on the fixed `ubuntu-24.04`
runner, never a local machine) builds a stated store - 20 repos, 1000
changesets, 31 MiB of raw blob content in files of about 32 KiB, and 5
lands recorded pending - starts a server on it, flies lands through the
wire for two seconds, kills the server with SIGKILL mid-land, and then
measures from the restart's command until readiness answers `SERVING`
and the first read resolves a ref. It prints the seconds and fails past
a 60 s bound. The kill is the runbook's crash: no flush, no drain -
whatever the write-ahead log holds is what recovery starts from. On
the fixed runner that recovery took 0.1 s (run 39623531102: readiness
answered `SERVING` and the first read resolved within a tenth of a
second of the restart's command, with 5 pending lands left by the
kill; the store built in 0.7 s). One sample at one size; a second size
is the next step when the number starts steering a decision. The
current numbers live in the workflow's run log on `main`, and a run
that drifts far from them is a regression to look at, not a promise to
restate here.

Proof: crates/scion-server/tests/recovery_bench.rs::a_restart_after_a_kill_recovers_in_a_stated_time (measured, no command: the section's number is what the recovery-bench workflow measures - a kill, a restart, and the seconds to serving - which no shell command of the guide reproduces)
