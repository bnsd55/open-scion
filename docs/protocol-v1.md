# Scion protocol v1

This file is the contract of the two gRPC services in
[`proto/scion.proto`](../proto/scion.proto): `scion.v1.Scion` and
`scion.v1.Objects`. It is written so that someone can build an independent
client, cache tier or server from it. It describes the code on `main`. The
bytes of changesets and trees, and how IDs are computed, are in
[`format-v1.md`](format-v1.md). [`architecture.md`](architecture.md) explains
how the server implements this contract.

The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be interpreted
as described in BCP 14 ([RFC 2119](https://www.rfc-editor.org/rfc/rfc2119),
[RFC 8174](https://www.rfc-editor.org/rfc/rfc8174)) when, and only when, they
appear in capitals. Other sentences describe what the server on `main` does.

Where a rule comes from Anthropic's article
[Redesigning source control](https://www.anthropic.com/engineering/redesigning-source-control),
this file says so. Every other rule is this project's choice.

## 1. Services

| Service | Calls | Who answers |
|---|---|---|
| `Scion` | Writes (`PutBlob`, `CreateChangeset`, `Land`, `GetLand`), path reads (`ResolveRef`, `GetChangeset`, `ReadFile`, `ListDir`, `Log`, `Blame`) and queries (`MergeBase`, `IsAncestor`, `Diff`) | The server only |
| `Objects` | `ResolveRef`, `GetRootTree`, `GetObjects` | The server, or any cache tier (section 8) |

Both services are on one address. Every read except `ResolveRef` and
`GetLand` names an immutable object by ID, never a ref, so its answer never
changes. A client resolves a ref first. `Scion.ResolveRef` is deprecated: a
client SHOULD use `Objects.ResolveRef`, which a tier forwards. A v1 server
MUST keep both.

## 2. Transport and authentication

**Transport.** gRPC over HTTP/2. The server listens on `127.0.0.1:7878` by
default. It refuses a non-loopback address unless it is started with
`--insecure-listen`, or it serves TLS: `--tls-cert` and `--tls-key` (PEM
files), with the HTTP/2 protocol negotiated by ALPN. With `--tls-client-ca`,
every client MUST present a certificate that CA signed (mTLS). A client that
calls an `https://` address trusts the CA of its certificate: `scn` reads it
from `--tls-ca`, a tier from its own `--tls-ca`. A bearer token over plain
tcp to a non-loopback address is refused, so a client SHOULD reach a server
only over loopback, a trusted network, or TLS. Certificate revocation is not
in v1 ([#14](https://github.com/bnsd55/open-scion/issues/14)).

**Connections and streams.** The server serves at most 128 connections at
once (`--max-connections`). It reads nothing from a connection beyond that
until another one closes, so a call on it waits. Each connection may have at
most 256 open streams (`--max-streams`), which the server sends as
`SETTINGS_MAX_CONCURRENT_STREAMS`. As HTTP/2 requires, a client MUST NOT
open more; it waits for a stream to end. A client SHOULD close a connection
that it no longer uses, because an idle one keeps its place. The server
closes a connection:

- 20 s after it gets its place, if no HTTP/2 client preface came. A client
  MUST send the preface as soon as it connects, as HTTP/2 requires.
- after the preface, when it gets no frame on the connection for 60 s: it
  sends an HTTP/2 `PING`, and closes the connection if no answer comes
  within 20 s. A client MUST answer a `PING`, as HTTP/2 requires.
- after 30 min: it sends `GOAWAY`, so the client opens a new connection for
  new calls, and closes the connection 10 min later, with any call still in
  it. A client MUST handle `GOAWAY` as HTTP/2 says. A call cut this way
  fails, and the client MAY retry it (`GetObjects` and `ReadFile` only
  re-read; a `Land` retry uses the same request key).
- when the replies of the connection that stopped reading hold more than
  64 MiB together (`--max-stalled-bytes`). A reply that stalls adds what it
  leaves in HTTP/2, under 4 MiB, to a count of its connection; a connection
  over the limit is closed, which frees what every stalled reply of it
  holds. Other connections are not touched. A unary reply that its client
  never reads is not counted: hyper gives the server no hook that sees it
  stall.

So a connection beyond the cap waits at most 40 min. A client that stops reading a reply
keeps the memory that no budget counts for that reply until the stream ends
or the connection closes, so a client SHOULD reset a stream that it no longer
reads (gRPC libraries do when the call is dropped). A cache tier keeps the
same limits (section 8).

**No message compression.** The server enables no gRPC message compression.
A request with a `grpc-encoding` other than `identity` fails with
`UNIMPLEMENTED`, so a client MUST NOT compress requests.

**Authentication.** Every call on both services MUST carry the metadata
`authorization: Bearer <token>`: the exact prefix `Bearer `, then the token.
The token is printable ASCII, as a gRPC metadata value is; a tier refuses an
upstream token that is not. A missing or wrong token fails the call with
`UNAUTHENTICATED` before the request is read. A token names a principal. A
server started with `--token T` has one principal, `default`, which may read
and write every repo. A server
started with `--access FILE` reads the principal of each token and its rules
from the file; the file's format is a deployment matter and lives in the
README. The principal of a call's token is its caller. Land request keys and
land IDs are scoped by (repo, principal).

**Access rules.** A call on a repo that the caller may not read fails with
`NOT_FOUND`, the same as for a repo that does not exist, on every RPC. So a
caller cannot tell whether such a repo exists. A change fails with
`PERMISSION_DENIED` when the caller may read the repo but not make the
change (sections 6.1 to 6.3).

**Timeouts.** The server ends a call with `CANCELLED` when its reply has not
started after 600 s, or earlier if the client's `grpc-timeout` is shorter.
For `PutBlob` this covers the whole upload. For a streamed reply (`ReadFile`,
`GetObjects`) it covers only the start. A `ReadFile` reply has its own limits
(section 6.7). A call that ends this way may still finish its work on the
server (section 6.3).

**Queues.** The server queues work instead of failing it. At most 256 jobs run
at once across both services, at most 16 `PutBlob` uploads are read at once,
at most 4 `Blame` calls run at once, at most 4 `MergeBase` and `IsAncestor`
calls together, and at most 4 `Diff` calls run at once. A `ReadFile` call
waits for its share of a memory budget (section 6.7). Time in a queue counts
toward the timeout.
One wait has its own limit: a `GetObjects` call that gets no memory for its
first object within 30 s, or before the caller's `grpc-timeout`, fails with
`RESOURCE_EXHAUSTED` (section 7.3).

## 3. Common rules

**Names.** `repo`, `ResolveRefRequest.name`, `LandRequest.target_ref` and
`LandRequest.request_key` are strings of 1 to 256 bytes with no control
character (Unicode category Cc: U+0000 to U+001F and U+007F to U+009F).
Another value is `INVALID_ARGUMENT`. A repo that does not exist, or that the
caller may not read, is `NOT_FOUND` (section 2). No v1 call creates a repo
or a ref.

**IDs.** Every ID field is 32 raw bytes: a `ChangesetId`, a `TreeId` or a
`BlobId` ([`format-v1.md`](format-v1.md)). An ID of another length in a
request is `INVALID_ARGUMENT`. In a reply, an empty ID field means "none".

**Paths.** A path is raw bytes and follows the path rules of
[`format-v1.md`](format-v1.md). A path that breaks them is
`INVALID_ARGUMENT`. Where a path is optional (`ListDirRequest.path`,
`LogRequest.path`, `DiffRequest.path`, `DiffRequest.after`), empty means
"none".

**Visibility.** A repo sees only its own changesets. A changeset that the
repo does not have is `NOT_FOUND`, the same as one that does not exist. A
repo has the blobs uploaded to it and the blobs that its changesets name. A
changeset names only blobs of its repo (section 6.2), so `ReadFile`,
`ListDir`, `Log`, `Blame` and `Diff` reach only the repo's own content.
`GetObjects` is not scoped to a repo in v1 (section 5).

**Message size limits.**

| Message | Largest |
|---|---|
| Any request message, both services | 64 MiB |
| Any `Scion` reply message | 64 MiB |
| One `Objects` reply message | 1 MiB + 1 KiB: one part of an object, of up to 1 MiB of encoded data, plus its header |

A message over its limit fails the call with `OUT_OF_RANGE`, on whichever
side finds it. A client MUST accept received messages up to these sizes.
Many gRPC libraries accept only 4 MiB by default. An object (a blob's
content or a tree's `TreeV1` bytes) is at most 100 MiB. Streamed file content
moves in chunks of 1 MiB, and `GetObjects` sends each object in parts of
1 MiB (section 7.3).

**Status codes.** The status message is for people and is not part of the
contract. A client MUST NOT depend on its text.

| Code | When | Retry |
|---|---|---|
| `INVALID_ARGUMENT` | The request is malformed, or the state does not accept it. Each call lists its cases. | After the cause is fixed |
| `NOT_FOUND` | A repo, ref, changeset, path, object or land does not exist, or the caller may not see it. | After the cause is fixed |
| `UNAUTHENTICATED` | The token is missing or wrong. | After the cause is fixed |
| `PERMISSION_DENIED` | The caller may read the repo, but not make this change: a changed path outside its write prefixes (`CreateChangeset`, `Land`), or no write right at all (`PutBlob`). `CreateChangeset` and `Land` name the first denied path, in the order of the changeset's changes. | After the cause is fixed |
| `ABORTED` | `Land` only: other writers kept moving the ref. The land stays `PENDING`. | Yes, with the same request key |
| `OUT_OF_RANGE` | A message is larger than its limit. | After the cause is fixed |
| `CANCELLED` | The server's 600 s limit or the client's `grpc-timeout` ran out. A client library can report its own deadline as `DEADLINE_EXCEEDED`. | Yes. A `Land` retry uses the same request key. |
| `DEADLINE_EXCEEDED` | `ReadFile`: the client took no message for 120 s. `GetObjects`: the client took no part for 120 s in the middle of an object (section 7.3). `Land`: the land ran past 600 s from the start of the call. The land stays `PENDING`. `MergeBase`, `IsAncestor`: the walk ran past 600 s from the start of the call. From a tier: the caller's deadline ran out while the tier waited for its first object (section 8). | Yes. A `Land` retry uses the same request key. |
| `UNAVAILABLE` | The server cannot be reached or is shutting down, or its database stayed busy for 5 s (another process, such as an import, held its write lock). A tier also replies `UNAVAILABLE` when its upstream does not answer in time, or when it finds no room to fetch an object from upstream within 30 s (section 8). | Yes |
| `RESOURCE_EXHAUSTED` | `GetObjects` only: no memory for the first object within 30 s, or before the caller's deadline (section 7.3). | Yes, after a wait |
| `DATA_LOSS` | From a tier only: its upstream sent bytes that are not the object asked for (section 8). | MAY |
| `INTERNAL` | A database, I/O or stored-data error, or a server bug. The message is `internal error`; the detail stays in the server log. | MAY |
| `UNIMPLEMENTED` | The server does not have the call, or the request is compressed. | After the cause is fixed |

A land that conflicts or is rejected is not an error. It is a state in a
successful `Land` reply (section 6.3).

## 4. Durability

A successful reply never shows state that a power loss can undo:

- `PutBlob` replies after the blob is on disk.
- `CreateChangeset` replies after the changeset, its trees and every blob it
  names are on disk.
- `Land` replies after every transaction of the land is on disk, whatever
  the state of the land.
- A read that saw a commit replies only after that commit is on disk. This is
  true for an error reply too, for example `NOT_FOUND` after a read.
- A read that computes and stores derived data, such as a root tree that has
  no stored value yet, replies after that data is on disk.

A call that fails or is cut off MAY have done part of its work. In particular
a `Land` that returned `ABORTED`, `CANCELLED` or `DEADLINE_EXCEEDED`, or whose
reply was lost, can be pending or finished on the server. The client finds
out with a retry under the same request key (section 6.3).

## 5. Object IDs and verification

The article says all source control data is content-addressed, so any cache
can answer a request for an object. A client checks what it can by hash.

**A client MUST check:**

- every object from `GetObjects`, after it unpacks the object's encoded
  bytes within the limits of section 7.3. A tree's bytes MUST decode as
  strict `TreeV1` and hash to the requested tree ID. A blob's bytes MUST
  hash to the requested blob ID.
- that the `kind` and `id` in the first part of each object match a ref that
  the client asked for. An object that the client did not ask for is an
  error.

**A client SHOULD check:**

- that `GetChangesetResponse.changeset` hashes to the requested changeset ID,
  and that each `LogEntry.changeset` hashes to its `changeset_id`.
- that the IDs from `PutBlob` and `CreateChangeset` equal the IDs it computes
  itself from the bytes it sent.

**A client trusts, because it cannot check them by hash:**

- `ResolveRef`: which changeset a ref points at.
- `GetRootTree`: which tree belongs to a changeset. This mapping is private
  derived data ([`format-v1.md`](format-v1.md)).
- the computed answers of the `Scion` service: the content from `ReadFile`
  and the tree from `ListDir` (unless the client knows their IDs from
  elsewhere), which changesets `Log` selects, `Blame`, `MergeBase`,
  `IsAncestor`, which paths `Diff` lists, and `Land` outcomes.

In v0.1 a client also trusts every tier on its path for the `ResolveRef` and
`GetRootTree` answers that the tier passes on (section 8).

**An object ID is not a permission.** A changeset cannot name a blob of
another repo (section 6.2), and no answer of `PutBlob` or `CreateChangeset`
shows whether another repo has a blob. But in v1 `GetObjects` sends any
stored tree or blob to any caller that may read the repo it names, whatever
repo stored the object. Read tickets are not in v1
([#13](https://github.com/bnsd55/open-scion/issues/13)).

## 6. The `Scion` service

### 6.1 PutBlob

`rpc PutBlob(stream PutBlobRequest) returns (PutBlobResponse)`. Stores one
file's content.

- The first message MUST name the repo. Every later message MUST leave `repo`
  empty.
- Each message's `data` is the next bytes of the file, in order. A message
  can hold any size within the message limit. A client SHOULD send chunks of
  1 MiB. An empty file is one message with empty `data`.
- The server checks the repo when the first message arrives, before it reads
  the rest. The caller needs a write rule for the repo: an upload names no
  path, so any write rule covers it.
- The reply's `blob_id` is the `BlobId` of all the bytes together
  ([`format-v1.md`](format-v1.md)).
- The blob then belongs to the repo: a changeset of the repo may name it
  (section 6.2).
- Safe to repeat. The same bytes always give the same ID.
- The reply holds only the ID, which the client can compute itself. The
  server keeps a stored copy only if the repo has the blob already. Else it
  writes the content as for new content. So neither the reply nor its time
  shows whether another repo has the same content.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | No message; a bad repo name; a later message names a repo; more than 100 MiB in total. |
| `NOT_FOUND` | The repo does not exist, or the caller may not read it. |
| `PERMISSION_DENIED` | The caller may read the repo, but has no write rule for it. |

### 6.2 CreateChangeset

`rpc CreateChangeset(CreateChangesetRequest) returns (CreateChangesetResponse)`.
Stores a changeset. Moves no ref.

The request's `changeset` is `ChangesetV1` bytes. The server accepts it only
if all of these hold:

- it decodes strictly, with at most 100,000 changes;
- it has exactly one parent, and the repo has that parent;
- the caller may write every path it changes: each changed path is at or
  below one of the caller's write prefixes for the repo, by path parts
  (`src` covers `src/a`, not `srcx`). The server checks this before it reads
  any blob, so a denied create reads no blob;
- it has no committer (only git import sets one);
- its message is at most 1 MiB, and the author's name and email are at most
  1 KiB each;
- every regular-file or symlink put names a blob of the repo, which the
  server has stored intact. A blob of the repo is one uploaded to it with
  `PutBlob`, or one that a changeset of the repo names, so a rename needs no
  new upload. A gitlink names no blob, whatever its bytes;
- its changes apply to the parent's tree. For example, a delete of a missing
  path or of a directory does not apply.

The reply's `changeset_id` is the `ChangesetId` of the bytes. Safe to repeat:
the same bytes give the same ID and no second copy.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | Any rule above fails, including a parent that the repo does not have. The message names the first path in path order whose blob is not the repo's; if every put names the repo's blobs, the first path whose blob is missing or damaged. A blob of another repo gets the same answer as content that no repo stored, and the server checks every blob for the repo before it reads any, so the answer does not show whether another repo has it. |
| `NOT_FOUND` | The repo does not exist, or the caller may not read it. |
| `PERMISSION_DENIED` | A changed path is outside the caller's write prefixes. The message names the first denied path, in the order of the changeset's changes. |

### 6.3 Land

`rpc Land(LandRequest) returns (LandResponse)`. Rebases a changeset onto a
ref and moves the ref.

**Request.** `repo`; `changeset_id`, a changeset that the repo has;
`target_ref`; and `request_key`, chosen by the client.

**What the server does.** First it checks the write rules of the caller of
the land against every path that the changeset changes, by the rules of
`CreateChangeset`. A land of a changeset that another principal made needs
the write rights too: whoever lands it moves the ref. A denied land fails
with `PERMISSION_DENIED` and records nothing.

The changeset's first parent is its *base*. The
server checks, in order:

1. The ref exists. If not, the land is `REJECTED` at once.
2. The changeset has exactly one parent. If not (an imported root or merge),
   `REJECTED`.
3. The base is on the ref's first-parent history. If not, `REJECTED`. A base
   more than 100,000 changesets behind the head on that history MAY be
   `REJECTED` as too old. Then the client makes a new changeset on a newer
   head and lands that. A base within 100,000 changesets of the head is never
   too old. Further back, the answer depends on what the server holds in
   memory at the time, so the same base can pass later under a new key.
4. Each path P that the changeset changes is checked. P conflicts when a
   changeset on the ref after the base changed P itself, a path below P, or a
   file at a directory above P. If any path conflicts, the land is
   `CONFLICT`, and `conflict_paths` lists the changeset's paths that conflict.
   Conflicts are path-level only. The server does not merge text.
5. The server makes the rebased changeset: the same changeset with its parent
   replaced by the ref's head. Author, message and changes stay the same. If
   the base is the head, the result is the input changeset itself. If the
   changes do not apply to the head's tree, the land is `CONFLICT` with no
   `conflict_paths`, and `detail` says why.
6. The server stores the rebased changeset and moves the ref by
   compare-and-swap, in one transaction. The land is `LANDED`.

Lands to one ref run one at a time. If the ref moves between step 4 and the
swap (in v1 only git import does this), the server checks again from the new
head, up to 3 times in all. Then the call fails with `ABORTED`, and the land
stays `PENDING`.

A `Land` past the 600 s limit, or cancelled by its client (for example by a
shorter `grpc-timeout`), replies `CANCELLED` or `DEADLINE_EXCEEDED`. The
server stops the land at its next check, and the land stays `PENDING`.

**Reply.**

| Field | Meaning |
|---|---|
| `land_id` | A decimal integer, as a string. The server never gives the same ID to two lands. |
| `state` | `LANDED`, `CONFLICT` or `REJECTED`. A `Land` reply never has `PENDING`; only `GetLand` returns it. |
| `result_changeset_id` | When `LANDED`: the rebased changeset, which was the tip of the ref when the land finished. Empty otherwise. |
| `detail` | Why the land is `CONFLICT` or `REJECTED`, for people. Empty when `LANDED`. |
| `conflict_paths` | When `CONFLICT` from step 4: the paths that conflict. Empty otherwise. |

| State | Meaning | Final |
|---|---|---|
| `PENDING` | Recorded, not finished. | No |
| `LANDED` | The ref moved to `result_changeset_id`. | Yes |
| `CONFLICT` | A change on the ref after the base touched the same paths, or the changes do not apply to the head's tree. The client re-reads those paths on the ref, makes a new changeset on the new head, and lands that. | Yes |
| `REJECTED` | The land cannot succeed as asked. | Yes |

**Request keys and retries.**

- A request key names one land within (repo, principal). The first call with
  a key records a land. A later call with the same key, the same
  `changeset_id` and the same `target_ref` returns that land. If the land
  finished, the reply is the stored outcome, field for field. If it is
  pending, this call finishes it. Calls with the same key at the same time
  wait for each other and return the same land.
- A key MUST NOT be reused for a different changeset or ref. That is
  `INVALID_ARGUMENT`.
- A finished outcome is final for its key. A retry returns it even if the ref
  has changed since. To try again, a client uses a new key.
- A client that retries after `ABORTED`, `CANCELLED`, `DEADLINE_EXCEEDED`,
  `UNAVAILABLE` or a lost reply MUST use the same key, so that it never
  lands the same change twice. `scn land` uses `<changeset ID>:<ref>` as its
  default key.
- The server keeps a finished land for at least 7 days after it finished. A
  pending land is kept until it finishes. Within that time a retry with the
  same key is safe.
- After that, the server MAY delete the land. A retry with the same key is
  then a new land with a new `land_id`, checked like any other, and `GetLand`
  with the old ID returns `NOT_FOUND`. A changeset that landed already is then
  normally `CONFLICT`, because its own landed copy changed its paths after its
  base. A changeset with no changes lands again.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | A bad name or ID; a request key already used for a different changeset or ref. |
| `NOT_FOUND` | The repo does not exist, or the caller may not read it, or the repo does not have the changeset. |
| `PERMISSION_DENIED` | A changed path is outside the caller's write prefixes. The message names the first denied path. |
| `ABORTED` | The ref kept moving. The land stays `PENDING`. Retry with the same key. |
| `CANCELLED`, `DEADLINE_EXCEEDED` | The call ran past 600 s, or the client cancelled it. The land stays `PENDING`. Retry with the same key. |

### 6.4 GetLand

`rpc GetLand(GetLandRequest) returns (LandResponse)`. Returns the stored state
of a land, in the same form as `Land`, and can return `PENDING`. It only
reads: it does not finish a pending land. A `Land` retry with the same key
does.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | A bad repo name, or `land_id` is not a decimal integer. |
| `NOT_FOUND` | The repo does not exist; the repo has no such land; it was deleted after its retention period; another principal started it. |

### 6.5 ResolveRef (deprecated)

The same call as `Objects.ResolveRef` (section 7.1), with one shared
implementation.

### 6.6 GetChangeset

`rpc GetChangeset(GetChangesetRequest) returns (GetChangesetResponse)`.
Returns the `ChangesetV1` bytes of a changeset that the repo has.
`NOT_FOUND` if the repo or the changeset does not exist for that repo.

### 6.7 ReadFile

`rpc ReadFile(ReadFileRequest) returns (stream ReadFileResponse)`. Streams the
content of a regular file, or the target of a symlink, at `path` in
`changeset_id`.

- Messages come in file order. Each holds at most 1 MiB. Together they are
  the whole content.
- An empty file is a reply with no message.
- An error in the request comes before the first message. `INTERNAL` and
  `DEADLINE_EXCEEDED` can end the reply after zero or more messages. A client
  MUST take the content as whole only when the reply ends with `OK`.
- The server checks the content against the blob ID as it sends it. A
  damaged stored blob ends the reply with `INTERNAL` before its last message,
  so a client never gets all of a damaged file.
- The reply ends with `DEADLINE_EXCEEDED` when the client takes no message
  for 120 s (the stall limit). A client that keeps reading has no time
  limit of its own, but a reply that runs for more than 10 min can be cut
  when its connection is closed for its age (section 2).
- Before the reply starts, the call waits for its share of a memory budget of
  256 MiB that all `ReadFile` replies share: the file's size for a file of at
  most 1 MiB, 5 MiB for a larger one. A stalled reply frees its share. Up to
  three messages stay in the server and in HTTP/2 until the stream ends or
  the connection closes (section 2).

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | A bad repo name or ID; a bad or empty path; the path is a directory or a submodule. |
| `NOT_FOUND` | The repo or changeset does not exist; nothing is at the path. |
| `DEADLINE_EXCEEDED` | After zero or more messages: the client took no message for 120 s. |
| `INTERNAL` | After zero or more messages: the stored blob is damaged. |

### 6.8 ListDir

`rpc ListDir(ListDirRequest) returns (ListDirResponse)`. Returns one directory
level as `TreeV1` bytes. An empty path is the repo root.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | A bad repo name or ID; a bad path; the path is a file. |
| `NOT_FOUND` | The repo or changeset does not exist; nothing is at the path. |

### 6.9 Log

`rpc Log(LogRequest) returns (LogResponse)`. Walks first parents from
`changeset_id`, newest first, starting with `changeset_id` itself.

- With a `path`, the reply holds only the changesets whose changes include
  the path or a path below it. The test uses each changeset's own list of
  changes against its first parent, so a changeset on a merged branch never
  appears, and a merge appears when its first-parent diff touches the path.
- `limit` is the most entries the client wants. 0 means 1. A reply holds at
  most `limit` entries and at most 1,000.
- A call checks at most 1,000 changesets. A reply stops before an entry that
  would take its changeset bytes over 32 MiB, but always holds the first
  entry it found.
- `next_changeset_id` is the first changeset that this reply does not cover,
  or empty when the history ends. To continue, a client calls `Log` again
  with `changeset_id` set to it and the same `path`. A reply can have no
  entries and still set `next_changeset_id`, so a client MUST continue until
  it has enough entries or `next_changeset_id` is empty.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | A bad repo name or ID; a bad path. |
| `NOT_FOUND` | The repo or changeset does not exist. |

### 6.10 Blame

`rpc Blame(BlameRequest) returns (BlameResponse)`. For each line of a file,
the changeset that last changed it on first-parent history, across renames.

**Lines.** A line ends after a `\n` byte, or at the end of the file. An empty
file has no lines and gets no ranges.

**Reply.**

- `ranges` are in file order and cover every line exactly once.
  `start_line` counts from 1 in the requested file. `changeset_id` is the
  changeset that last changed those lines, and `origin_line` is the first of
  them, counting from 1, in the file at that changeset.
- `changesets` holds each changeset that a range names, once each: its
  author, and `path`, the path of the file at that changeset, where
  `origin_line` counts. The path differs from the requested path when the
  walk followed a rename.
- `next_changeset_id` is set when the call stopped at a bound before every
  line had a changeset. `next_path` is then the path of the file there. The
  ranges with an empty `changeset_id` came from that changeset or an older
  one, and their `origin_line` is their line in the file at
  `next_changeset_id` and `next_path`. To continue, a client blames
  `next_path` at `next_changeset_id` and maps those lines through
  `origin_line`.
- An empty `path` or `next_path` comes from a server that does not follow
  renames: it means the requested path.

**Bounds per call.** At most 1,000 changesets checked, 100 versions diffed,
and about 1M lines diffed (both sides of each diff; a line counts once per
64 bytes). A rename search counts as a line each chunk of up to 64 bytes
that it reads and each row of a chunk table that it indexes or compares,
and as 64 lines each file size that it reads. The first step of a call
always runs, so every call makes progress. The rules of a rename search are
those of `Diff` (section 6.13) for one added file, with git's limit of
1,000,000 pairs only: it looks at every deleted file, over several calls if
it must.

**A rename search over calls.** A rename search uses the budget that the
call has left. Where the budget runs out inside it, the call stops:
`next_changeset_id` is the changeset that adds the file, `next_path` is its
path, and `next_rename_searched` and `next_rename_best` say where the search
stopped. `next_rename_searched` counts the deleted files looked at, in path
order (only paths that held a file count). `next_rename_best` is the most
similar of those so far, or empty. The client MUST send both back unchanged
as `rename_searched` and `rename_best` with that `next_changeset_id` and
`next_path`, and MUST send 0 and empty with any other place. The next call
reads the best file again and goes on from where the search stopped, so it
finds the file that one search with no budget finds. A call whose first step
is a rename search looks at one more deleted file even past the budget, so
the search always moves. When a search ends but the diff after it does not
fit, the reply names the end of the search: `next_rename_searched` is the
number of deleted files, and `next_rename_best` the file found, if any. The
next call then uses that file and does not search again. A reply sets both
fields only with `next_changeset_id`.

**Rules.** The server walks first parents to each changeset that changed the
file, and diffs that version with the older version. The older version is
the file at the same path in the first parent. Where the changeset adds the
file (no file was at the path), it is the file that the changeset renamed
to the path: a deleted file with the same content, else a similar one, as
`git blame` finds it by default. The lines that the changeset added go to
it. The other lines are traced further back, at the older path. The diff is
Myers with an indent heuristic. Where several minimal diffs exist, which one
the server picks is not part of v1, and neither is which deleted file it
picks as the source. A binary file is diffed line by line, as `git blame`
does. An older version that is absent, not a regular file, larger than
16 MiB or longer than 1,048,576 lines counts as absent: the changeset that
replaced it gets its lines.

A client that ignores `next_path` continues at the requested path after a
rename. Its next call then fails with `NOT_FOUND`, or blames another file.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | A bad repo name or ID; a bad or empty path; the file is not a regular file, is larger than 16 MiB, or has more than 1,048,576 lines; `rename_searched` and `rename_best` are not a place where a rename search of that path at that changeset can stop. |
| `NOT_FOUND` | The repo or changeset does not exist; nothing is at the path. |

### 6.11 MergeBase

`rpc MergeBase(MergeBaseRequest) returns (MergeBaseResponse)`. The best
common ancestors of `a_changeset_id` and `b_changeset_id`. The article lists
finding merge bases among the server's APIs, so that tools need no local
repository.

- Ancestry follows every parent, not only first parents. A changeset is its
  own ancestor.
- A common ancestor is an ancestor of both changesets. A best common
  ancestor is a common ancestor that is not an ancestor of another common
  ancestor. This is the set that `git merge-base --all` prints.
- `changeset_ids` holds each best common ancestor once: highest generation
  first, then by ID bytes, lowest first. The generation of a changeset is 1
  for a root, else 1 + the highest generation of its parents.
- Usually there is one. After criss-cross merges there can be several, and
  the client chooses which to use. The order is fixed, so the first is a
  stable choice.
- If one changeset is an ancestor of the other, the answer is that one.
  `changeset_ids` is empty when the two histories share no changeset.

**Cost.** A call has no bound on its steps. A step reads one changeset's
parents, or one entry of the server's commit-graph index. When neither
first-parent history has a merge newer than where the two meet, the answer
takes a few dozen steps, at any depth. Otherwise the server walks from both
changesets, highest generation first, and stops once no other best common
ancestor can follow. It reads each merge on the way, and the changesets
where two parts of the walk are at the same generations. It jumps over the
other runs of first parents, a few dozen steps for each run. A call stops at
the 600 s limit (section 2), or when its client cancels it. There is no
resume.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | A bad repo name or ID. |
| `NOT_FOUND` | The repo or a changeset does not exist. |
| `CANCELLED`, `DEADLINE_EXCEEDED` | The call ran past 600 s, or the client cancelled it. |

### 6.12 IsAncestor

`rpc IsAncestor(IsAncestorRequest) returns (IsAncestorResponse)`.
`is_ancestor` is true when `ancestor_id` is `descendant_id`, or is reached
from it through parents. Ancestry follows every parent, as in `MergeBase`.

**Cost.** A call has no bound on its steps, as in `MergeBase`, and never
goes below the generation of `ancestor_id`. So when the two differ and
`ancestor_id`'s generation is not lower, the answer is false. Without a
merge between the two, the answer takes a few dozen steps, at any depth.
Each merge newer than `ancestor_id` that the search reaches costs three
steps. A call stops at the 600 s limit, or when its client cancels it.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | A bad repo name or ID. |
| `NOT_FOUND` | The repo or a changeset does not exist. |
| `CANCELLED`, `DEADLINE_EXCEEDED` | The call ran past 600 s, or the client cancelled it. |

### 6.13 Diff

`rpc Diff(DiffRequest) returns (DiffResponse)`. The paths whose file differs
between the snapshots of `old_changeset_id` and `new_changeset_id`. The
article says the server computes diffs, for example between a commit and its
merge base for a CI job.

- A file is anything at a path that is not a directory: a regular file, a
  symlink or a gitlink. A path differs when its `FileState` differs: kind,
  ID or executable flag. Content is compared by ID; there is no line diff.
  Renames are found, as below.
- Each `DiffEntry` has the `path`, the file there in the old changeset
  (`old`) and in the new one (`new`). An unset side means no file is there:
  nothing, or a directory. The two sides always differ.
- A `FileState` has a `kind` (`REGULAR`, `SYMLINK` or `GITLINK`) and an
  `id`: the 32-byte blob ID of a regular file's content or of a symlink's
  target, or the gitlink's git commit ID (20 bytes for SHA-1, 32 for
  SHA-256). `executable` is set only for a regular file.
- Entries are in tree order: paths compare part by part, and parts by raw
  bytes. So `a/b` comes before `a.txt`. This is not git's order, nor the byte
  order of whole paths. A path that is a file on one side and a directory on
  the other gives one entry for the file, then one for each file below the
  directory.
- With `path`, only that file, or the files below that directory. A path
  that exists on neither side gives an empty reply.
- With `after`, only the entries after that path in tree order. It need not
  be in the diff, nor below `path`.

**Bound and resume.** A reply holds at most 1,000 entries. When more follow,
`next_after` is the last path in `entries`, and the reply holds exactly
1,000 entries. To continue, a client sends the same request with `after` set
to `next_after`, until `next_after` is empty. Snapshots never change, so the
pages together are the whole diff, each entry once.

**Renames.** The server finds renames as `git diff` does by default, among
the added and deleted files of the whole diff below `path`. A rename keeps
its two entries: the deleted file at the old path and the added file at the
new path.

- The added entry (`old` unset) has `rename_from`, the old path. The deleted
  entry (`new` unset) has `rename_to`, the new path. Both have
  `similarity`: how similar the two files are, in percent, as git prints it.
  100 is the same content.
- A client that ignores these fields sees a delete and an add, as from a
  server that does not find renames. Then all three fields are empty.
- The two entries are in tree order like all others, so they can come in
  different replies. To print a rename once, as `git diff --name-status`
  does, a client prints it at the added entry and skips the deleted entry.
- Each call finds the renames of the whole diff below `path`, not of its
  page. So an entry has the same rename in every reply, whatever `after` is.

The rules are git's (`diffcore-rename.c`). Path order here is the byte order
of the whole path, git's order, not tree order. A deleted file is the source
of at most one rename.

1. Same content: a deleted file of the same kind and ID. A regular file can
   change its executable bit. Added files take their turn in path order.
   Each takes, among the first 100 deleted files left with that content, the
   first with the same name (the last part of the path), else the first.
2. Same name: a deleted and an added regular file whose name no other file
   left on its side has, if they are at least 75% similar.
3. Similar: each added regular file keeps the four deleted regular files
   most similar to it, of those at least 50% similar. Then those pairs, most
   similar first, become renames while both files are free. On a tie, a pair
   with the same name comes first, then by added path, then by deleted path.
   git breaks some ties among more than four files another way. Which file
   of such a tie the server pairs is not part of v1.

Similarity is git's: each file is cut into chunks that end after a newline or
after 64 bytes. In text (no NUL byte in the first 8,000 bytes), a CR before a
newline does not count. Each chunk is hashed as git hashes it. Similarity is
the bytes of the chunks that both files have, over the size of the larger
file. A symlink or a gitlink is a rename only with the same content.

**Rename bounds.** The server finds renames only in a diff of at most 10,000
entries below `path`; a larger diff has none. As in git, step 3 runs only
when the added files left times the deleted files left are at most
1,000,000. Steps 2 and 3 share a budget per call: about 4M, where each chunk
of a file read and each row of a chunk table indexed or compared count one,
and each file size read counts 64 (64 MiB of text in lines of 64 bytes
costs 1M). Past it, the server reads no
more files, so it misses the renames that step 2 or step 3 would find after
that point. The budget is the same on every call, so every reply gives the
same renames. The search never ends a page early.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | A bad repo name, ID, `path` or `after`. |
| `NOT_FOUND` | The repo or a changeset does not exist. |

## 7. The `Objects` service

The article says the client never tells the server what it already has: it
asks only for the trees and blobs it wants. A client resolves a ref, gets the
root tree, then asks for trees and blobs by ID, and checks each against its
ID (section 5).

Every call here needs a read rule for the repo. A caller without one gets
`NOT_FOUND`, as on every RPC (section 2).

### 7.1 ResolveRef

`rpc ResolveRef(ResolveRefRequest) returns (ResolveRefResponse)`. The
changeset that a ref points at now. The only mutable answer in the protocol.
`NOT_FOUND` if the repo or the ref does not exist, or the caller may not
read the repo.

### 7.2 GetRootTree

`rpc GetRootTree(GetRootTreeRequest) returns (GetRootTreeResponse)`. The ID of
the root tree of a changeset that the repo has. `NOT_FOUND` if the repo does
not exist, the caller may not read it, or the changeset does not exist for
that repo. The answer never changes.

### 7.3 GetObjects

`rpc GetObjects(GetObjectsRequest) returns (stream ObjectPart)`. Streams the
trees and blobs named by `refs`, each in an encoding that the request
accepts, in parts.

**Request.** `refs` names 1 to 1,000 objects, each a `kind` (`TREE` or
`BLOB`) and a 32-byte `id`. A ref can repeat. `accept` lists the encodings
that the client accepts besides `RAW`, which every client accepts. In v1 the
only other encoding is `ZSTD`. An empty list means `RAW` only. A server
ignores a value that it does not know. The server checks the whole request
before it sends any object.

**Encodings.** An object's bytes are a tree's `TreeV1` bytes or a blob's
content, and its ID is of those bytes. They travel in one of two encodings:

| Encoding | Encoded bytes |
|---|---|
| `RAW` | The bytes as they are. |
| `ZSTD` | One Zstandard frame ([RFC 8878](https://www.rfc-editor.org/rfc/rfc8878)) of the bytes, and no other bytes before or after it. It starts with the Zstandard magic number `0xFD2FB528`: not a skippable frame, not a legacy format. It has no `Dictionary_ID`, gives the size of the bytes as its `Frame_Content_Size`, and has a `Window_Size` of at most 8 MiB (for a single-segment frame, its size). It is at most `ZSTD_compressBound(size)` bytes, as the zstd library computes it: `size + size / 256`, plus `(128 KiB - size) / 2048` if `size` is below 128 KiB. libzstd makes such frames at every level up to 19. |

The article says caches send objects in the disk format that the daemon will
consume. Here, the server stores each blob as one zstd file, and sends a blob
`ZSTD` to a client that accepts it: the stored file as it is, which the
server does not unpack. It unpacks a blob for a client that accepts only
`RAW`. It sends a tree `RAW`. A server or tier MUST NOT send `ZSTD` to a
client that did not accept it. It MAY send any object `RAW`.

**Parts.** An object of E encoded bytes comes in one or more `ObjectPart`
messages. Each part holds 1 byte to 1 MiB of `data`, and the parts add up to
E. An object with E = 0 is one part with no data. The server and
`scion-tier` send parts of 1 MiB, the last one shorter. The first part also
sets `kind` (1), `id` (2), `encoding` (4), `encoded_size` (5, which is E) and
`size` (6, the size of the object's bytes, at most 100 MiB). For `RAW`,
`encoded_size` equals `size`. A later part sets none of fields 1, 2, 4, 5
and 6. So a receiver knows from the first part how much memory the object
takes, before the rest arrives.

**A receiver MUST**, before it uses, stores or forwards an object:

- refuse a first part whose `kind` and `id` are not an object that it asked
  for and has not got yet, whose encoding it did not accept, whose `size` is
  over 100 MiB, or whose `encoded_size` does not fit `size` as the table
  above says;
- refuse a part of more than 1 MiB, an empty part (but the one part of an
  empty object), parts that add up to more than E, a later part that sets
  any of fields 1, 2, 4, 5 and 6, and a reply that ends in the middle of an
  object;
- for `ZSTD`, refuse a frame that breaks the rules of the table above, or
  whose size is not `size`, and unpack it into at most `size` bytes with a
  window of at most 8 MiB, so that a frame never makes it hold more than the
  first part said;
- check the bytes against the ID (section 5).

A receiver ignores fields that it does not know, also in a later part. A
server sends objects in request order; a receiver MAY refuse one out of
order. `scion-tier` does; `scn` takes each object it asked for in any
order.

**Reply.**

- Objects come in request order, part by part. So a reply is always a
  prefix of `refs`.
- The reply sends the next object only while it has sent less than 64 MiB
  of encoded data. So a reply holds at least one object, and at most 64 MiB
  plus one object of up to 100 MiB.
- All replies share a budget of 400 MiB. Before the server reads an object,
  the object takes its share of the budget: all the memory that the reply
  holds for it (its bytes, and the decoder while it unpacks a blob for a
  `RAW` client). It keeps the share until the reply has handed the object's
  last part on. Each part is a buffer of its own, so a part that waits for
  the client never keeps the rest of the object. Not counted: up to three
  parts that wait for the client in the server and in HTTP/2, at most 1 MiB
  each, until the stream ends or the connection closes (section 2). The
  server finds the first object before it answers the call. If it has no memory for that
  object within 30 s, or before the caller's deadline, the call fails with
  `RESOURCE_EXHAUSTED` and sends no
  object. The client SHOULD wait before it asks again.
- The reply stops when the server waits 30 s for memory for a later object,
  or when the client takes no part for 120 s (the stall limit).
- A reply that stops between objects ends with status `OK`, after at least
  one object. A server or tier MUST NOT end a reply with `OK` and no object,
  or with `OK` in the middle of an object. A reply that stops in the middle
  of an object, because its client took no part for 120 s, ends with
  `DEADLINE_EXCEEDED`, as `ReadFile` does. The client MUST ask again for the
  objects it did not get.

**Errors.** An error status of the call is about the whole call, and no
object was sent: for example a repo that does not exist, or no memory for
the first object. An error in the stream is about the object in progress,
or the next object if none is in progress. It ends the reply after the
objects before it. A server or tier MUST NOT put an error about the whole
call in the stream. A missing object ends the reply with `NOT_FOUND`. A
damaged stored tree, or a damaged blob that the server unpacks, ends it
with `INTERNAL`. The server does not check a blob that it sends `ZSTD`: if
the stored file is damaged, the receiver finds it (section 5). The client
MAY keep the objects it got before the error.

| Error | When |
|---|---|
| `INVALID_ARGUMENT` | A bad repo name; no ref or more than 1,000; a kind that is not `TREE` or `BLOB`; an ID that is not 32 bytes. |
| `NOT_FOUND` | The repo does not exist (the call's status); a named object does not exist (in the stream, after the objects before it). |
| `RESOURCE_EXHAUSTED` | No memory for the first object within 30 s, or before the caller's deadline. No object was sent. |
| `DEADLINE_EXCEEDED` | From a tier: the caller's deadline ran out while it waited for the first object (section 8). In the stream: the client took no part for 120 s in the middle of an object. |
| `UNAVAILABLE` | From a tier: its upstream did not answer in time, or it found no fetch slot or no memory for an object from upstream within 30 s (section 8). The call's status for the first object, else in the stream. Ask again. |

## 8. Cache tiers

The article says each node runs a `scion-daemon` that talks only to a
regional `scion-cache`, which asks the servers on a miss, and that all three
answer identical calls. This project builds both: `scion-cache` keeps
objects in memory and listens on TCP; `scion-daemon` keeps them on disk and
listens on a Unix socket. Each sends its misses to one upstream, so the
order is daemon, cache, server. These rules hold for any tier:

- A tier MUST serve the `Objects` service with the same calls, fields, limits
  and reply rules as the server. A client MUST NOT need to know whether it
  talks to a tier or to the server.
- A tier MUST require a bearer token on every call, as the server does.
- A tier MUST forward every `ResolveRef` call to its upstream, and MUST NOT
  keep the answer.
- A tier MAY keep a `GetRootTree` answer forever, keyed by (repo,
  changeset), but only as its upstream gave it. It MUST NOT compute one.
- A tier MAY keep objects forever. A tier never trusts upstream bytes: it
  MUST check each object from its upstream as a client does (sections 5
  and 7.3), before it stores, forwards or sends it. An object that fails the
  check MUST NOT be stored or sent. It ends the reply with `DATA_LOSS`,
  after the objects before it.
- A tier MAY keep an object in any encoding, and MAY send it in any
  encoding that its client accepts. `scion-tier` asks its upstream for
  `ZSTD`, keeps a tree `RAW` and a blob as one zstd frame (the frame from
  upstream, or one that it packs from a `RAW` blob), and sends that frame as
  it is to a client that accepts `ZSTD`.
- An error status of an upstream call is about every object in that call.
  If it comes for the first object of a reply, the tier MUST fail its own
  call with that status. If it comes later, the tier ends its reply with
  `OK` there, and the client's next call gets the status. So a missing repo
  costs one upstream call per tier, not one per object.
- An error in an upstream stream is about the next object. A tier MAY treat
  one with a code other than `NOT_FOUND` or `DATA_LOSS` as a failure of the
  whole call, because a broken connection also ends a stream with an error.
  `scion-tier` does.
- A tier SHOULD wait longer for its upstream than that upstream may wait for
  its own, so it does not give up on a slow but healthy path. The limits of
  `scion-tier` nest. The server and each tier wait at most 30 s for memory
  before an object. A tier waits at most 30 s for one of its 64 fetch slots
  before it calls upstream, and at most 30 s for memory for an object from
  upstream, while that upstream reply waits. `scion-cache` waits at most
  60 s for each step of the server (an answer, or the next part), so a busy
  server answers first. `scion-daemon` waits 180 s for each step of the
  cache, which covers one step of the server, the cache's own three waits
  of 30 s, and 30 s to spare. On a timeout or a wait that ends, the tier
  fails with `UNAVAILABLE`. A caller's deadline passes down. A tier reads
  the caller's `grpc-timeout`, ends every wait at it less a margin (a tenth
  of it, at most 1 s), and sends what is left of it upstream as
  `grpc-timeout`. So the status of the deepest process reaches the caller
  in time, for any deadline. A caller whose deadline passes stops waiting,
  and the fetch that another caller with a later deadline waits for goes
  on; that caller asks again with its own if the fetch failed on a shorter
  one. With no `grpc-timeout`, the fixed limits above apply and the tier
  sends none upstream. `scn` sends a deadline on every call: 600 s, the
  server's own limit, so a waiting `scn` never hangs silently.
- The `Scion` service goes only to the server.

**The daemon's socket.** `scion-daemon` listens on a Unix socket with mode
0600, in a directory that only its user can reach. A client sends its token
to the socket, so it SHOULD check first that the socket file is a socket
(not a symlink) owned by its own user or by root. It SHOULD also check each
directory above the socket, up to `/`: owned by root or by its own user,
and not writable by group or others unless it has the sticky bit. Else
another user could swap the socket after the check. It SHOULD resolve the
socket's directory before this check and connect to the resolved path, so
that it follows no symlink after the check. `scn` does all of this, for
every `unix://` address. The daemon checks the directories above its socket
and its data directory in the same way.

Tiers are trusted in v0.1. A client cannot check a `ResolveRef` or
`GetRootTree` answer by hash, so it trusts every tier on its path for them.
Access tickets that a tier checks are not in v1
([#13](https://github.com/bnsd55/open-scion/issues/13)).

## 9. Compatibility

This file describes version 1 of the protocol: the `scion.v1` package of
[`proto/scion.proto`](../proto/scion.proto) together with object format v1
([`format-v1.md`](format-v1.md)). A caller states the version it wants by
which package it calls; there is no handshake and no version header. A
server answers every call of a package that it serves, and a call of a
package or a service that it does not serve fails with `UNIMPLEMENTED`, as
for a call that it does not have. A later version is a new package,
`scion.v2`. A server MAY serve both, so a client can move to the new
version call by call, and an older client keeps working.

A server, tier or client MUST ignore fields that it does not know, as
proto3 does.

Because unknown fields are ignored, an older server drops a new request field
without an error. For example, a server from before `LogRequest.path` ignores
the path and returns the history unfiltered. So a new request field is safe
only if an answer that ignores it is still correct for the client.

| Change | Needs a new version |
|---|---|
| A new call. An older server replies `UNIMPLEMENTED`. | No |
| A new reply field that an older client can ignore | No |
| A new request field whose empty value means today's behavior, and that an older server can safely ignore | No |
| A new `Encoding` value. A server sends it only to a client that lists it in `accept`, and ignores a value that it does not know. | No |
| A new field in `ObjectPart`, first or later part, that an older receiver can ignore | No |
| A higher limit on requests that the server accepts | No |
| Deprecating a call that keeps working | No |
| A field's number, type or meaning, or the meaning of its empty value | Yes |
| Removing or renaming a call, service, field or enum value | Yes |
| A new enum value in a reply, such as a new `LandState` | Yes |
| Object bytes or ID rules ([`format-v1.md`](format-v1.md)) | Yes, a new format version |
| A lower request limit, or a larger reply message than a client must accept today | Yes |
| What a client must check or may trust (section 5), or what a status code means | Yes |
| Request-key rules, or a land retention shorter than 7 days | Yes |

A server MAY serve both `scion.v1` and `scion.v2` at once. To retire a
version, a server stops serving its package; its clients then get
`UNIMPLEMENTED`, which tells them to move.

## 10. Not in v1

- Certificate revocation and a public CA store; trust comes only from a CA
  file ([#14](https://github.com/bnsd55/open-scion/issues/14)).
- More than one principal, per-path write rules and read tickets
  ([#13](https://github.com/bnsd55/open-scion/issues/13)).
- Per-principal quotas ([#15](https://github.com/bnsd55/open-scion/issues/15)).
- Calls that create a repo or a ref, delete a ref, move a ref other than by
  `Land`, or list repos or refs. `scion-server import` creates repos and refs.
- Client changesets with no parent, several parents or a committer. Only git
  import makes them.
- Text merge. Conflicts are path-level.
- `Diff` renames in a diff of more than 10,000 entries or past the search
  budget ([#107](https://github.com/bnsd55/open-scion/issues/107)).
- `Log` and `Blame` without per-call bounds
  ([#61](https://github.com/bnsd55/open-scion/issues/61)), and `Log` that
  follows renames.
