# The fuzz campaign: a wider target, a dispatchable budget

## The archive section readers gain their own target

The archive readers behind `verify` and `restore` are the door bytes
from an untrusted source enter by. `archive-verify` already fuzzed the
whole archive; `archive-read` feeds each section reader directly
(manifest, changesets, refs, lands, members, provenance), one
first-byte selector, the rest the file's bytes. An archive accepted
for verification keeps its verdict-or-refusal promise section by
section.

## A manual run can give every target an hour

`fuzz.yml` gains a `budget_scale` dispatch input (default 1) that
multiplies every target's budget; the nightly run is unchanged, and a
dispatched run can scale it.

## The seeds path is the crate's, and a missing seeds dir is not a failure

The first dispatched run failed on every target whose seeds directory
exists: the path was written from the repository root, while
`cargo fuzz run` works from the crate. The step now resolves the
seeds path from the crate, and a target without a seeds directory
starts with an empty corpus instead of failing.

## The seed target counts the records the file frames

The snapshot-seed target asserted every accepted seed report names at
least one object. A valid snapshot may hold zero records (magic, a
repo name, nothing more), and the first campaign run's crash was
exactly such a file: the report is honestly all zero, and the
assertion, not the reader, was wrong. The invariant is now exact: the
target parses the framing independently and asserts the report covers
exactly the records the file holds. The 12-byte empty snapshot joins
the target's seeds as the regression case.
