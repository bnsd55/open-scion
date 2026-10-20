# Design: history and writes for a principal with folder read rules

**Status: proposal. Nothing in this file is built, and no code ships
until the owner decides the open question in §2.** It answers
[#183](https://github.com/bnsd55/openscion/issues/183), the follow-up to
[#121](https://github.com/bnsd55/openscion/issues/121): today a principal
with folder read rules gets a checkout of its view and nothing else —
`Log`, `Blame`, `MergeBase`, `IsAncestor`, `GetChangeset` and `Diff`
answer `UNIMPLEMENTED`, and the write calls refuse
(`access.rs`, "partial readers cannot write in v0.1"). This design gives
that principal a projected history and a bounded write path, with no new
leak a checkout of the same view would not give.

## 1. The projection

A partial reader's history is **projected**: for every real changeset
that touches at least one covered path, the server holds a **view
changeset** — covered changes only, each parent mapped to its view
changeset.

- **All-hidden changesets are dropped and contracted.** A changeset
  whose changes are all hidden gets no view copy; its children parent
  onto the nearest visible ancestor. No merge edge of a hidden branch
  survives into any view.
- **Commit metadata.** See §2 — the owner decides; the projection works
  under either choice.
- **Ancestry answers (`MergeBase`, `IsAncestor`) come from the
  projection rows only.** The real graph is never consulted for a
  partial caller.
- **First read of a (repo, rule-set digest) projects the whole
  history.** Same shape as the `view_trees` derivation: the synchronous
  path projects only the page the call asked, a background job fills the
  rest in batched, resumable steps, and a bounded sweep drops the rows
  of rule sets that no live principal holds any more.

## 2. Commit metadata — owner decides

| | Option A: declared repo-global | Option B: constant metadata for partly hidden changesets |
|---|---|---|
| What the reader sees | Author, committer, message of every changeset its projected history holds, verbatim | Fixed strings (same author, same message) for every changeset that carries any hidden path |
| The leak | A message may name hidden paths; in a monorepo the folder name itself can be the secret | None beyond the fact that a changeset exists and touched visible paths |
| What it costs | One declared threat-model row; the truth in every message | History reads as machine noise; blame loses its "who wrote this"; a constant-metadata changeset cannot hash into a stable view ID without the redaction policy being part of the projection forever |
| Hidden changesets | Dropped and contracted, as §1 says | Same |

**Owner decides.** The projection is identical otherwise; switching
later means re-deriving every projection row, not a format change.

## 3. The base token

A view changeset ID hashes projected content, so it does not invert to
one real changeset. A partial writer needs a base, and the server needs
to know which real head that base names.

**Pick:** the server mints an opaque, authenticated **base token**
beside every projected reply (`Log` pages, `GetChangeset`, the view tip
of `ResolveRef`): a signed structure carrying

- a domain-separation prefix `scion\0base\0v1\0` before the signed
  bytes, so a ticket can never verify as a base token or the other way
  round, even under the same key;
- the principal's digest — the server refuses a base token presented by
  any other principal;
- the repo, the real changeset ID, the rule-set digest it was projected
  under, and an expiry (the ticket window).

**Same key as tickets, with the prefix as the wall.** Reason: the ticket
key is already managed — 0600, checked descriptor, rotation via
`--ticket-key-old` — and a second key doubles the operator ceremony for
a confusion the prefix already rules out (the signed bytes differ from
byte 0). Upgrade path: if a cross-type confusion bug class ever shows,
split the key; the token format carries a version field for it.

**Use.** The client sends the token back as the base of a land. The
server verifies the signature, the presenting principal, and the
rule-set digest; a stale or forged base answers as a re-projection
(the rules changed, so the view is rebuilt), never as a wrong merge
base.

## 4. No laundering

Content-addressing binds bytes, not actors: a blob that exists because
a hidden path holds it must not be nameable by a principal that never
saw it. **Pick:** every blob a partial writer's changeset names must be
covered by either

- an **upload receipt** — (principal, repo, blob ID), recorded at
  `PutBlob` for partial writers, checked at land; or
- a **copy from a visible path** — the blob is already reachable by
  this principal's rules in the base changeset.

The lander refuses everything else. The receipt row is written only for
partial writers, so the ordinary write path gains nothing.

## 5. What the archive carries for a view

Nothing. A restore rebuilds the real store; every view re-derives
lazily from the access file, which stays operator configuration outside
the archive (`archive-format.md`, "What an archive deliberately does
not hold"). The projected rows are derived data like the tree bodies.

## 6. The first slice

Read-only projected history: `Log`, `GetChangeset`,
`MergeBase`/`IsAncestor` from the projection. No writes, no `Blame`,
no `Diff`. Useful alone: a partial reader can follow its folders'
history and ask ancestry — the questions a checkout cannot answer.

**The leak test.** Seed a repo with hidden commits, hidden paths inside
visible commits, hidden merge edges, and messages naming hidden paths.
Then `a_partial_readers_history_leaks_nothing_its_checkout_would_not`:

- every returned changeset's changes are covered paths only;
- the parents of every returned changeset are view changesets of real
  ancestors, and no dropped changeset appears;
- the union of all returned content, materialized, equals a checkout of
  the view at each returned tip.

## 7. Cost on the ordinary paths

- **Land path, principals without folder rules: zero.** One
  in-memory `Access::partial` branch per land; no base tokens, no
  receipts, no projection rows exist for whole-repo writers.
- **`PutBlob`** gains the receipt write only for partial writers.
- **The projection** never runs for a whole-repo reader: every history
  call branches on the same `Access::partial` before any walk.

## 8. Threat-model rows this design adds or changes

- CHANGE "a partial reader reads only its view": history now answers,
  projected; the actor row names the projection.
- NEW invariant "a partial reader reads a projected history", with the
  test of §6.
- NEW "a base token outside its signature, principal, or digest
  refuses", with a test for each of the three.
- NEW declared leak, if Option A: "commit metadata is repo-global; a
  message may name paths outside the prefixes."
- CLOSE the pinned channel "a partial reader sees a hidden landing
  through the ref id": a partial reader resolves the view tip, and a
  hidden landing no longer moves its ref. It moves from
  "no test yet" to an invariant with a test.
