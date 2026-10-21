# The git import refuses what git cannot write

## A date the source does not hold is not imported

The parser behind a git signature reads a timezone it cannot
understand as zero, so a commit with a broken timezone imported as a
changeset whose date differs from its source without a word. The
import now reads the raw date back through git's own grammar
(`<seconds> <sign><hhmm>`) and refuses a commit whose date does not
round-trip, with the commit and the raw line in the error. git's
written form of an unknown offset, `-0000`, still imports.

## A commit or tree object past the cap is refused on its header

Files were capped; the commit and tree objects the import decodes
whole were not, so a crafted repository could hand the import a
gigabyte message or a tree of millions of entries. Both are now
refused from the object's header, before any decode, with the object
and its size in the error: one commit or tree takes at most the byte
cap a file takes, and one tree takes at most a million entries.

## Why

The git import is a trust boundary: an operator imports someone
else's repository, and its objects need not be what git itself would
write. A field that does not read back as its source must be refused,
and an object the import decodes in one piece must be bounded before
the decode, not after.
