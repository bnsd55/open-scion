The release pipeline's dry run takes a commit SHA instead of a tag
name, so no tag is ever pushed for one: the dispatch refuses a commit
that is not an ancestor of main, the tag-identity verdicts moved to
`scripts/release-verify-tag.sh` (fed canned tag objects by
`scripts/test-release-verify-tag.sh`, since no CI run holds a signed
release tag), and everything after the checkout is the same code path a
real tag runs.
