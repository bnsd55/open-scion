# The two maintainer drills of #393

docs/drills/vulnerability-drill.md walks a mock private vulnerability
report end to end, from GitHub's private vulnerability reporting
through the private fork, the CVE decision, the published advisory,
and the credit, using only SECURITY.md and the Security tab - and
lists what SECURITY.md does not say for this (where a fix is
developed, when a CVE is requested, how a security fix ships, the
publication timeline, triage ownership), without filling the gaps.

docs/drills/release-drill.md runs RELEASING.md as a second maintainer,
starting with the dry run, and marks every step that today needs the
owner: the signed tag (their personal SSH key, D18), the refs/tags/v*
ruleset, and the immutable-releases setting - the v0.8 list. Both files
carry the record header, the done-whens, and the notes blanks, and
keep to what the runbooks already say.
