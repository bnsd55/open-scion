# How a report is handled: the draft the owner reviews

SECURITY.md gains a "How a report is handled" section between Reporting
and Scope, drafted from the six gaps the vulnerability drill found:
where the fix is developed (the advisory's private fork), triage
ownership (one maintainer, named as a limit), severity (a CVSS 4.0
score mapped to docs/support.md's levels), the CVE rule (medium or
higher through GitHub as CNA), the timeline (7 days to acknowledge, a
fix target of 30/60/90 days by severity, 90-day coordinated
disclosure), and the release mechanics (a patch release cut with the
release workflow from the earliest supported branch). Every number is
marked proposed; the owner decides. DRAFT: the PR leaves draft until
the owner has been through the six.
