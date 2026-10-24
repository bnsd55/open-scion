# Threat model

This document says who the system trusts, what it protects, and what it
does not. It replaces the one-line promise of architecture.md section 8
("one trusted machine") with the precise model the code holds today:
every claim names the test that proves it, or says there is no test yet.
Read section 8 of architecture.md for how each part works; this
document says who may do what to whom, and where the checks sit.

Report security problems as [SECURITY.md](../SECURITY.md) describes.

## 1. The supported profile

One active server per data directory, on one host. The model holds only
inside this profile.

Trusted, by definition:

- The server host and everyone with an account on it. Whoever can read
  the data directory holds every blob and every metadata row; a copied
  data directory is a full copy of every repo.
- The operator and the server's administrators. They hold the tokens,
  the ticket key, and the TLS keys.
- The data directories the server uses. The file-mode rules assume the
  accounts that own them are the server's own.

Not trusted:

- The network between any two components.
- Whoever holds a token: a token grants only what its principal's rules
  grant, and a leaked token is revoked by removing its rule or rotating
  the key.
- Any principal, server or client, beyond the rules it was given.

Out of profile. Any of these voids the model:

- A data directory on a shared or network filesystem. The open checks
  read the owner and the mode bits, not ACLs, and a remote owner is
  whatever the mount reports.
- `--insecure-listen`: it sends tokens and data unencrypted to a
  non-loopback address on purpose.
- An untrusted local account on the server host. It can read the data
  directory if the operator's modes allow, and the page cache side
  channels of section 8 exist regardless.
- `scn mount` on a multi-user host. The NFS server cannot tell local
  users apart, and the mount path and its secret sit in the mount table
  every local user can read
  ([#11](https://github.com/bnsd55/openscion/issues/11)).
- Windows, or a filesystem that reports one owner for every entry.

## 2. Assets

| Asset | Where it lives | What protects it |
|---|---|---|
| History | The server's data directory: blobs, the index, Postgres | The directory's file modes; local trust (out of profile otherwise) |
| Refs | The server's metadata | The RPC auth step; read rules answer `NOT_FOUND` |
| File contents, per read rule | Blobs, reached only through a read ticket's walk | The ticket check and the walk on every serving tier |
| Tokens | The operator's hands, the access file as digests, `SCION_TOKEN` | Constant-time digest compare; never written to a file or a log by `scn` |
| Ticket keys | `--ticket-key` (seed) and `--ticket-key-old` (public half) | The descriptor checks: a regular file, owner this account or root, a seed mode 0600, a public key no other account can write |
| TLS keys | `--tls-key`, `--tls-client-key` | The checked descriptor read: no symlink, no other owner, no group or others read |
| TLS certificates and CAs | `--tls-cert`, `--tls-ca`, `--tls-client-ca` | The checked descriptor read: whole-keeping, no group or others write |
| Snapshots | The cache's `--snapshot` dir, a daemon's `--seed` file | A full-repo credential: read checks plus `--seed-full-repo` say-so |

## 3. Actors

- **The operator** runs the server and the tiers. Trusted inside the
  profile. Chooses the data directories, the modes, and the addresses.
- **A server admin** (an account on the server host). Trusted, as the
  profile says.
- **A whole-repo reader**: a principal whose read rules name whole
  repos. Reads everything of those repos, through tickets.
- **A partial reader**: a principal whose read rules are path prefixes.
  Reads `ReadFile` and `ListDir` through the view the server derives,
  and nothing else: its history and write calls answer `UNIMPLEMENTED`.
- **A writer**: a principal with write rules for a repo. A write rule
  needs a read rule without a prefix (a partial reader cannot write, in
  v0.1); the lander of another principal's changeset needs write.
- **A tier** (cache or daemon): holds one token, so one principal
  upstream; cannot mint tickets; a cache verifies and walks, a daemon
  forwards.
- **A network peer without a token**: gets `UNAUTHENTICATED` for every
  call but `GetObjects`, and `GetObjects` gets nothing without a ticket.
- **The holder of a leaked token**: exactly its principal, no more.
- **A local user of a client machine, other than the `scn` user**: can
  read `ps` and the mount table, so they can see tokens in `argv` and
  reach a mount through its NFS port; the client docs say to pass the
  token in the environment, and `scn mount` is single-user only.

## 4. Trust boundaries

1. **The network.** A non-loopback listener needs TLS or
   `--insecure-listen`; the server and the tiers refuse the rest at
   start, before the data directory opens
   (`scion-server/tests/serve_flags.rs::a_non_loopback_listen_is_refused_before_the_data_directory_opens`).
   There are no public TLS roots: a client trusts a CA only when the
   operator names one.
2. **The RPC auth step.** Every call but `GetObjects` presents a token;
   the server maps the token's BLAKE3 digest to a principal and its
   rules before any data moves
   (`scion-server/tests/every_rpc_unauthenticated.rs::every_rpc_answers_unauthenticated_without_a_valid_token`).
3. **The read ticket and the walk.** `GetObjects` has no token: its only
   permission is a ticket only the server can mint, which the server and
   each cache verify and walk before any byte moves, and a daemon
   forwards to an upstream that does both
   (`scion-server/tests/tickets.rs::a_ticket_serves_nothing_beyond_its_walk`,
   `scion-server/tests/tickets.rs::whatever_is_served_is_reachable_from_the_ticket`).
4. **Between tiers.** A daemon trusts its upstream and its socket user;
   a cache trusts the public keys it was given. Neither decides who may
   read: the ticket does, wherever it is checked.
5. **The client machine.** `scn` keeps the token out of files and logs,
   but the command line is public through `ps`; the environment is not.
6. **The data directory.** Local files answer who may read what only
   through the server; every open refuses a directory that group or
   others can write, and one under a directory another user can change.

## 5. Security invariants

Each invariant names the test that proves it, or `no test yet`. The
invariants are the promises the code makes; the tests are how a change
that breaks one is caught.

- **No call without a token.** Every RPC but `GetObjects` answers
  `UNAUTHENTICATED` without a valid token, on every transport.
  `scion-server/tests/every_rpc_unauthenticated.rs::every_rpc_answers_unauthenticated_without_a_valid_token`
- **A token is one principal.** The access file holds BLAKE3 digests and
  each digest maps to one principal with its rules.
  `scion-proto/src/runtime/auth/mod.rs::each_token_gets_its_own_principal`. The
  compare is constant
  time (`constant_time_eq`); no test yet - a timing test is
  not reliable, so the code is the proof.
- **A repo outside a principal's rules does not exist.** Every RPC
  answers `NOT_FOUND` for it, as for a repo that is not there.
  `scion-server/tests/access.rs::a_principal_without_a_read_rule_sees_no_repo_on_any_rpc`
- **A partial reader reads only its view.** The paths outside its
  prefixes answer exactly as missing paths, its history calls answer
  `UNIMPLEMENTED`, and it writes nothing: the view is derived from its
  prefixes, and `GetRootTree` mints the ticket on the view root.
  `scion-server/tests/access.rs::a_partial_reader_reads_only_its_view`,
  `scion-server/tests/access.rs::a_partial_readers_ticket_reaches_only_its_view`,
  `scion-server/tests/access.rs::a_partial_reader_writes_nothing`,
  `scion-server/tests/access.rs::writes_outside_a_prefix_are_denied_with_the_path`
- **Only the server mints tickets.** A ticket carries the principal, the
  repo, the root tree and an expiry, signed with the server's Ed25519
  key; the signature is verified before any field is used.
  `scion-server/tests/tickets.rs::only_a_ticket_of_this_server_serves`,
  `scion-server/tests/tickets.rs::tickets_out_of_time_or_shape_are_refused`
- **A ticket serves only its walk.** The walk from the ticket's root
  tree reaches trees and blobs on the named paths and nothing else; a
  gitlink ends it; a symlink is never followed; a view ticket cannot
  reach a hidden path or a warm blob outside the view.
  `scion-server/tests/tickets.rs::a_ticket_serves_nothing_beyond_its_walk`,
  `scion-server/tests/tickets.rs::a_gitlink_ends_the_walk`,
  `scion-server/tests/tickets.rs::whatever_is_served_is_reachable_from_the_ticket`,
  `scion-tier/tests/tier.rs::a_view_ticket_cannot_reach_a_warm_blob_outside_its_view`
- **A ticket is for one repo and one window.** A ticket of another repo
  serves nothing here; the expiry is 15 minutes with 60 seconds of
  skew, checked in both directions.
  `scion-server/tests/tickets.rs::a_ticket_of_another_repo_serves_nothing_of_this_one`,
  `scion-server/tests/tickets.rs::tickets_out_of_time_or_shape_are_refused`
- **A tier serves objects only.** A tier refuses every call but
  `GetObjects`, and refuses bytes that do not match the ID they were
  stored under.
  `scion-tier/tests/tier.rs::a_tier_serves_no_root_trees_or_refs`,
  `scion-tier/tests/tier.rs::a_tier_refuses_an_object_whose_bytes_do_not_match_its_id`
- **A daemon serves one OS user.** Its socket is 0600 in a directory
  only its user can reach, and it refuses any other; the daemon checks
  no ticket, so the rule is what keeps one user's objects from
  another's. The single-user rule itself rests on the operating system,
  not on the code: no test yet.
- **The keys on disk are what the checks say.** A secret is a regular
  file, owned by the account that runs the process or root, mode 0600;
  a whole-keeping file refuses group and others write; a symlink dies
  at the open; the ancestors are checked, with the sticky-bit exception;
  and the bytes are read from the checked descriptor.
  `scion-proto/src/runtime/auth/mod.rs::a_file_owned_by_another_account_is_refused`,
  `scion-proto/src/runtime/auth/mod.rs::a_group_readable_secret_is_refused_and_a_group_writable_shared_file_is_refused`,
  `scion-proto/src/runtime/auth/mod.rs::a_secret_reads_through_the_descriptor_and_a_link_dies_at_the_open`,
  `scion-proto/src/runtime/auth/mod.rs::a_directory_that_others_can_write_is_refused_unless_sticky`,
  `scion-server/src/main.rs::a_symlinked_key_is_refused_with_a_perfect_target`,
  `scion-proto/src/runtime/tls.rs::a_symlinked_certificate_is_refused_with_a_perfect_target`
- **A data directory opens only as one server, for one build.** The
  locks fence a second server and a build mismatch;
  `scion-server/tests/backend_refusals.rs::a_lost_server_lock_fences_the_backend`.
  The
  directory and every file mode hold whatever the umask of the run:
  `scion-server/tests/private_dirs.rs::every_directory_of_a_new_data_directory_is_private_whatever_the_umask`.
- **The ticket key is made private and once.** `scion-server
  ticket-key` creates, never replaces, and a second run to the same
  path fails.
  `scion-server/tests/ticket_key.rs::a_ticket_key_file_is_private_and_made_once`
- **A changed client CA changes nothing behind the check.** The
  certificates and CAs the server, the tiers and `scn` load are read
  through the checked descriptor, so a swap dies at the open or on the
  descriptor's checks.
  `scion-proto/src/runtime/tls.rs::a_client_ca_the_group_or_others_can_write_is_refused`,
  `scion-proto/src/runtime/tls.rs::a_symlinked_certificate_is_refused_with_a_perfect_target`
- **A seed cannot seed what was not checked.** A snapshot is read
  through the checked descriptor and streamed; a group-writable or
  symlinked snapshot is refused, and `--seed-full-repo` must say so.
  `scion-tier/src/snapshot.rs::a_group_writable_snapshot_is_refused`,
  `scion-tier/src/snapshot.rs::a_symlinked_snapshot_is_refused`
- **Two repos whose names differ only by case do not share one
  snapshot file.** On a case-insensitive disk, `main` and `MAIN` would
  fold one onto the other: the second repo's snapshot write replaced
  the first's, and a seed of the first read the second's file;
  `file_name` escapes
  every capital (and every byte outside the plain name set) as `%XX`,
  so the file name keeps lowercase only, and the property test holds
  it for any repo name
  (`scion-tier/src/snapshot.rs::any_repo_name_gives_one_plain_file_name`,
  with its regression file in
  `scion-tier/proptest-regressions/snapshot.txt`). #452
- **Checkout writes nothing it should not.** Names that are unsafe fail
  before anything is written, a failed checkout removes what it wrote,
  a signal cleans up, and an existing destination is left untouched on
  failure.
  `scn/tests/checkout.rs::unsafe_names_fail_before_anything_is_written`,
  `scn/tests/checkout.rs::a_misbehaving_server_is_caught_and_nothing_is_written`,
  `scn/tests/checkout.rs::a_signal_removes_what_the_checkout_wrote_and_then_kills_it`
- **A mount answers only its own secret.** A wrong mount path or a
  handle this mount did not make is refused.
  `scn/src/vfs.rs::a_wrong_secret_is_refused`. That the NFS port is
  reachable by
  other local users is out of profile, not hidden: the mount says so at
  start and in `--help`.
- **The directory checks read no ACL that the mode does not grant, and
  refuse every ACL beyond the mode.** The sticky-bit exception excuses
  no group write. `scion-proto/src/runtime/auth/mod.rs::a_minimal_acl_matching_the_mode_passes`,
  `scion-proto/src/runtime/auth/mod.rs::an_acl_beyond_the_mode_is_refused_for_real`,
  `scion-proto/src/runtime/auth/mod.rs::a_group_writable_ancestor_is_refused_even_with_the_sticky_bit`
- **Internal errors say `internal error`.** The detail stays in the
  log, named by the request id.
  `scion-server/tests/request_id.rs::a_calls_request_id_names_its_span_in_the_servers_log`
- **An archive is untrusted input.** `verify` and `restore` treat every
  byte of an archive as coming from outside the server: a repo name the
  manifest holds is checked against the live name rule and that it is
  one path component, before it ever joins a path
  (`scion-server/tests/archive.rs::a_manifest_name_that_escapes_the_archive_is_refused`),
  and a blob file decompresses through a bounded stream, at most the
  server's max blob size plus one, so a crafted frame cannot grow in
  memory past the cap
  (`scion-server/tests/archive.rs::a_blob_file_that_expands_past_the_cap_is_refused`).
- **A restore verifies first, and goes only into an empty store.** It
  runs the same `verify` a reader runs and refuses on any damage, and it
  refuses a data directory that already holds a store
  (`scion-server/tests/archive.rs::a_verify_names_a_blob_whose_bytes_someone_corrupted`,
  `scion-server/tests/archive.rs::a_restore_refuses_a_data_directory_that_already_holds_a_store`).
- **A secret path cannot hang or flood the checked read.** The open is
  `O_NONBLOCK`, so a planted FIFO dies at the open instead of blocking
  the server at startup; the kind check takes a regular file only; and
  the bytes are read bounded at a fixed cap, so a file that lies about
  its size, or grows between the fstat and the read, is refused
  deterministically. (#433)
  `open_checked_file`,
  `read_bounded` over
  `MAX_CHECKED_FILE`; the tests
  `scion-proto/src/runtime/auth/mod.rs::a_file_that_is_not_a_file_is_refused`,
  `scion-proto/src/runtime/auth/mod.rs::a_reader_that_yields_one_byte_past_the_cap_is_refused`
- **An archive cannot fold two names onto one file, duplicate a repo,
  or land half.** `verify` refuses a manifest that names one repo
  twice, and refuses two names that fold to one directory on a
  case-insensitive disk — the disk's own (device, inode) is the judge,
  not a fold table (`check_section_dir`); colliding land
  keys are named or tolerated, never half-restored; and a replay that
  dies halfway leaves the target empty, because `restore` fills a
  staging directory and publishes by one rename
  (`restore_impl`). (#438) The tests:
  `scion-server/tests/archive.rs::an_archive_naming_two_repos_that_fold_to_one_is_refused`,
  `scion-server/tests/archive_attacks.rs::a_manifest_naming_one_repo_twice_is_refused_before_any_write`,
  `scion-server/tests/archive_attacks.rs::two_pending_lands_sharing_a_request_key_are_named_or_tolerated`,
  `scion-server/tests/archive_attacks.rs::a_mid_replay_failure_leaves_the_target_empty`,
  `scion-server/tests/archive_attacks.rs::a_pg_replay_that_dies_halfway_leaves_the_database_empty`
- **A refused-auth flood is counted by the machine, not the socket.**
  The audit meter folds one peer's refusals into counted lines keyed
  by the address — an IPv6 by its /64, the smallest block one site
  routes — so one machine opening a thousand connections from a
  thousand ports cannot fill the peer cap for free and bury a real
  stranger in the shared bucket. (#414)
  `meter_key`, and the test
  `scion-server/src/audit.rs::one_address_rotating_ports_does_not_bury_a_stranger`
- **A client that never reads its unary replies holds the budget, not
  the server's memory.** The reply-memory layer counts every parked
  unary reply's bytes against one budget (128 MiB across every
  connection) and refuses further unary reads with `resource_exhausted`
  while it is spent; a committed write's reply goes out regardless, and
  every way a reply ends — the reply finishes, the client resets the
  stream, or the connection closes — frees its share. (#586)
  `crates/scion-server/tests/reply_budget_h2.rs`
- **A restore verifies first, and goes only into an empty store.** It
  runs the same `verify` a reader runs and refuses on any damage, and it
  refuses a data directory that already holds a store
  (`scion-server/tests/archive.rs::a_verify_names_a_blob_whose_bytes_someone_corrupted`,
  `scion-server/tests/archive.rs::a_restore_refuses_a_data_directory_that_already_holds_a_store`).
## 6. What has no test yet

Each of these is a claim this document makes without a test behind it.
They become issues as the project moves past alpha.

- The token compare is constant time: the code is (`auth.rs`,
  `constant_time_eq`), but no test observes it, and a timing test would
  be flaky by nature. The digest form of the access file is the main
  defence, and that is the load-time format. The check's whole work
  still scales with the number of tokens the access file holds, so its
  duration lets a caller estimate how many principals are configured.
  That is config size, not identity, and the compare itself stays
  constant time; a fixed-worst-case loop would buy nothing else.
- A narrowed access file does not cut outstanding tickets: they hold
  for at most 15 minutes from signing, by design (rotate the key to cut
  at once). The window and the cut are pinned:
  `scion-server/tests/access.rs::an_outstanding_ticket_survives_a_narrowed_access_file_until_the_key_rotates`
- TLS revocation, and hostname checks beyond what the CA signs: no
  test; revocation has no issue yet.
- A partial reader sees real changeset IDs for its view: a landing on
  hidden paths moves `main`, and the reader's `ResolveRef` answers the
  new ID over an unchanged view. The channel is pinned as documented
  behavior, not closed:
  `scion-server/tests/access.rs::a_partial_reader_sees_a_hidden_landing_through_the_ref_id`
- The page cache makes a shared blob fast, and the rename of a replaced
  blob frees the old file: two side channels the code does not hide. No
  test can exist here: both are behavior of the operating system and
  the hardware below the process, so no test of this repository can
  observe them, let alone refuse them. The data directory's file modes
  are the only wall, and they are tested.
- The mount's single-user rule rests on the operating system: the NFS
  server cannot tell local users apart, so the code has nothing to
  check, and a test of this repository cannot observe other local users
  either. It is out of profile, and the mount says so at start and in
  `--help`.
- `scn mount` serves any local user who can read the mount table: the
  secret is not a wall. Out of profile by design; no test.
