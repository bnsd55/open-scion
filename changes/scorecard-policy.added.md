The Scorecard run now enforces a pinned policy: floors are per check,
not an aggregate score, in .github/scorecard-policy.yml. Ten checks
carry a floor of 10 (Binary-Artifacts, CI-Tests, Dangerous-Workflow,
Dependency-Update-Tool, License, Maintained, Pinned-Dependencies,
Security-Policy, Token-Permissions, Vulnerabilities) and
Branch-Protection a floor of 8. Every check the project does not meet
yet - Signed-Releases, SBOM, Packaging, SAST, Fuzzing, Code-Review,
Contributors, CII-Best-Practices - is a dated exception, each with an
owner, a reason, and an expiry date; an expired exception fails the
job like a floor breach. The scorecard job enforces the file after
each scan with scripts/check-scorecard-policy.py.
