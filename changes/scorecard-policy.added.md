The Scorecard run now enforces a pinned policy: floors are per check,
not an aggregate score, in .github/scorecard-policy.yml. Ten checks
carry a floor of 10 (Binary-Artifacts, CI-Tests, Dangerous-Workflow,
Dependency-Update-Tool, License, Maintained, Pinned-Dependencies,
Security-Policy, Token-Permissions, Vulnerabilities) and
Branch-Protection a floor of 8. Every check the project does not meet
yet - Signed-Releases, SBOM, Packaging, SAST, Fuzzing, Code-Review,
Contributors, CII-Best-Practices - is a dated exception, each with an
owner, a reason, and an expiry date; an expired exception fails the
job like a floor breach.

The job enforces the policy after each scan with
scripts/check-scorecard-policy.py, against a second Scorecard pass in
its JSON results format: the JSON lists every check with its score,
so a check that Scorecard skipped or errored on fails the policy
instead of hiding behind a missing SARIF entry, and a score of -1
(Scorecard's "could not run") fails unless an unexpired exception
covers it.
