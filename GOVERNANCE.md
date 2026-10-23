# Governance

Who decides what in openscion, and how the project grows past one
person. This file is the short form; where a rule lives elsewhere, it
links to it.

## Roles today

One maintainer: the project owner. The maintainer reviews and lands
every pull request, cuts the releases, answers the security reports,
and owns the accounts the project runs on (GitHub, crates.io, the
release signing key).

## How decisions are made

- An issue first. A change starts as an issue that states the problem;
  the approach is agreed there before a large change is written.
- One topic per pull request. A pull request is reviewable in one
  sitting, adds a changelog fragment, and passes the same checks CI
  runs.
- The format is frozen. docs/format-v1.md defines the bytes that
  object IDs hash; a change there is a new format version and needs a
  discussion first, never a drive-by commit.
- CI is the merge bar. A pull request lands when its checks are green
  and the maintainer has reviewed it; the lead reviewer can refuse a
  change with a reason, and the reason lands in the pull request.
- Security reports follow SECURITY.md, privately, and the response
  process there is the project's.

## The staged ladder to a second maintainer

The project's continuity goal (docs/ROADMAP.md, v0.8.0) is two
maintainers who can each accept a change, handle a private report and
cut a release. The ladder to it, one stage at a time:

1. **Contributor.** Anyone whose pull requests land. No rights, no
   duties beyond the code of conduct.
2. **Triager.** The maintainer invites a consistent contributor to
   label and close issues, and to ask the clarifying questions on new
   ones. No merge rights.
3. **Maintainer.** The maintainer invites a triager who has landed a
   record of reviewed, non-reverted changes. A maintainer has merge
   rights after the owner's explicit, written (in the repository)
   invitation, and shares the duties: review, release, security
   response.

Owner decides: the number of landed changes stage 3 asks for, and the
names at every stage. The invitation itself is the owner's action; this
file records the rule, and the owner alone opens the door.

## Protected branches

`main` is protected: the required checks must pass, and a maintainer's
review is required. From v0.8.0, the review must be independent (not
the author's), as the roadmap's governance line says.

Owner decides: whether stale reviews are dismissed, whether
administrators are exempt, and when.

## Continuity

The project must survive the maintainer's absence: a second maintainer
(the ladder above), the release credentials recorded so a release can
be cut without one person's machine (docs/RELEASING.md), and the
recovery paths for the accounts.

Owner decides: who holds the second set of account credentials, and
where the recovery instructions live.
