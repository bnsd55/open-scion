# OpenSSF Best Practices: the evidence audit

This file maps the
[OpenSSF Best Practices badge](https://www.bestpractices.dev) criteria to
this repository's evidence, level by level, so the badge form can be
filled from it and the gaps can be worked. The criteria were read from
bestpractices.dev (levels passing and silver; gold is not this release's
goal). Each row says met, not met (gap), or N/A, with the evidence - a file,
a workflow, or a doc section - or the gap. A row marked Ben names a project
fact only the owner can supply. Nothing here submits the form or creates the
badge entry: that is the owner's account action, and the README badge is a
public claim the owner makes.

## Passing level

| criterion | status | evidence, gap, or justification |
|---|---|---|
| description_good | met | README.md: what this is, and why |
| interact | met | README.md: issues, contributing, the demo |
| contribution | met | CONTRIBUTING.md: issues first, PRs |
| contribution_requirements | met | CONTRIBUTING.md: code standards |
| floss_license | met | MIT OR Apache-2.0, both files |
| floss_license_osi | met | Both licenses are OSI-approved |
| license_location | met | LICENSE-MIT, LICENSE-APACHE |
| documentation_basics | met | README.md plus the docs/ set |
| documentation_interface | met | docs/protocol-v1.md, docs/cli.md |
| sites_https | met | GitHub serves everything over HTTPS |
| discussion | met | GitHub issues and pull requests |
| english | met | All docs and commits are English |
| maintained | met | Active main; docs/support.md |
| repo_public | met | The public GitHub repository |
| repo_track | met | git records what, who, when |
| repo_interim | met | Every change is a reviewed PR |
| repo_distributed | met | git |
| version_unique | met | vX.Y.Z per docs/RELEASING.md |
| version_semver | met | SemVer (CHANGELOG.md header) |
| version_tags | met | Signed annotated tags |
| release_notes | met | CHANGELOG.md, Keep a Changelog |
| release_notes_vulns | met | Advisories named in the rows |
| report_process | met | GitHub issues; CONTRIBUTING.md |
| report_tracker | met | GitHub issues |
| report_responses | Ben | The issue history answers this |
| enhancement_responses | Ben | The issue history answers this |
| report_archive | met | GitHub issues, public, searchable |
| vulnerability_report_process | met | SECURITY.md: private advisories |
| vulnerability_report_private | met | GitHub's private advisory form |
| vulnerability_report_response | met | SECURITY.md: seven days |
| build | met | cargo build; CI builds it |
| build_common_tools | met | cargo and rustc only |
| build_floss_tools | met | The toolchain is FLOSS |
| test | met | cargo test --workspace |
| test_invocation | met | The standard cargo test |
| test_most | met | The coverage gate (silver below) |
| test_continuous_integration | met | ci.yml on every PR and push |
| test_policy | met | CONTRIBUTING.md: tests that can fail |
| tests_are_added | met | The suites grew with the features |
| tests_documented_added | met | CONTRIBUTING.md's standards |
| warnings | met | clippy in CI; workspace lints |
| warnings_fixed | met | -D warnings blocks the merge |
| warnings_strict | met | -D warnings, both platforms |
| know_secure_design | Ben | docs/threat-model.md; the owner answers |
| know_common_errors | Ben | The threat model names the classes |
| crypto_published | met | rustls and Ed25519 |
| crypto_call | met | Calls rustls, dalek, blake3 only |
| crypto_floss | met | All three are FLOSS |
| crypto_keylength | met | rustls defaults; Ed25519 |
| crypto_working | met | No broken algorithms |
| crypto_weaknesses | met | No SHA-1, no CBC-SSH |
| crypto_pfs | met | rustls's ephemeral key exchange |
| crypto_password_storage | N/A | No passwords: tokens, blake3-hashed |
| crypto_random | met | getrandom-backed crates only |
| delivery_mitm | met | HTTPS; tiers refuse plain non-loopback |
| delivery_unsigned | met | No hashes over http |
| vulnerabilities_fixed_60_days | met | RUSTSEC fixes landed on arrival |
| vulnerabilities_critical_fixed | met | None open |
| no_leaked_credentials | met | The leak-pattern push gate |
| static_analysis | met | CodeQL and clippy |
| static_analysis_common_vulnerabilities | met | CodeQL's security queries |
| static_analysis_fixed | met | No open exploitable finding |
| static_analysis_often | met | CodeQL per PR and weekly |
| dynamic_analysis | met | The fuzz smoke and soak suites |
| dynamic_analysis_unsafe | met | Rust is safe; the FFI block is tested |
| dynamic_analysis_enable_assertions | met | CI runs the debug profile |
| dynamic_analysis_fixed | met | No open finding |

## Silver level

| criterion | status | evidence, gap, or justification |
|---|---|---|
| achieve_passing | met | Passing holds, less the Ben rows |
| contribution_requirements | met | CONTRIBUTING.md |
| dco | gap | A DCO section and a sign-off check |
| governance | gap | A GOVERNANCE.md; content Ben approves |
| code_of_conduct | met | CODE_OF_CONDUCT.md at the root |
| roles_responsibilities | gap | The same GOVERNANCE.md |
| access_continuity | Ben | A second access path is a fact |
| bus_factor | Ben | One maintainer today |
| documentation_roadmap | met | docs/ROADMAP.md |
| documentation_architecture | met | docs/architecture.md |
| documentation_security | met | SECURITY.md, docs/threat-model.md |
| documentation_quick_start | met | README.md Try it; smoke runs it |
| documentation_current | met | docs_ops and docs_links pin the docs |
| documentation_achievements | Ben | The README badge is the owner's call |
| accessibility_best_practices | gap | A scope statement; a SHOULD |
| internationalization | N/A | English messages by design |
| sites_password_security | N/A | The sites are GitHub's |
| maintenance_or_update | met | docs/compatibility.md, docs/support.md |
| report_tracker | met | GitHub issues |
| vulnerability_report_credit | met | SECURITY.md promises credit |
| vulnerability_response_process | met | SECURITY.md's process |
| coding_standards | met | CONTRIBUTING.md's code standards |
| coding_standards_enforced | met | fmt --check and clippy -D warnings |
| build_standard_variables | N/A | Pure Rust; no native build |
| build_preserve_debug | met | cargo keeps debuginfo |
| build_non_recursive | met | One cargo workspace |
| build_repeatable | met | Lockfile, pinned toolchain, fixed codegen |
| installation_common | met | cargo install; the Docker image |
| installation_standard_variables | N/A | cargo install --root is the way |
| installation_development_quick | met | CONTRIBUTING.md's setup |
| external_dependencies | met | Cargo.toml and Cargo.lock |
| dependency_monitoring | met | cargo-deny and Dependabot in CI |
| updateable_reused_components | met | The RUSTSEC-0382 lockfile update |
| interfaces_current | met | clippy flags deprecations |
| automated_integration_testing | met | ci.yml, per-job reports |
| regression_tests_added50 | Ben | The policy is set; the share is a fact |
| test_statement_coverage80 | met | The coverage gate: 85.7 percent lines |
| test_policy_mandated | gap | One line in CONTRIBUTING.md |
| tests_documented_added | gap | The same line, in the same file |
| warnings_strict | met | -D warnings, both platforms |
| implement_secure_design | met | docs/threat-model.md's invariants |
| crypto_weaknesses | met | As passing |
| crypto_algorithm_agility | met | rustls suites; the versioned ticket |
| crypto_credential_agility | met | Credentials in their own 0600 files |
| crypto_used_network | met | TLS end to end; plain refused |
| crypto_tls12 | met | rustls's minimum |
| crypto_certificate_verification | met | The TLS suite proves the refusal |
| crypto_verification_private | met | Verified before any private call |
| signed_releases | Ben | Tags SSH-signed; attestations are v0.8.0 |
| version_tags_signed | met | git tag -v, docs/RELEASING.md |
| input_validation | met | IDs verified; tickets signed; allowlists |
| hardening | met | Deny by default; mode-checked directories |
| assurance_case | met | docs/threat-model.md |
| static_analysis_common_vulnerabilities | met | CodeQL's security queries |
| dynamic_analysis_unsafe | met | Rust is safe; the FFI block is tested |

## The gaps, smallest first

1. `test_policy_mandated` and `tests_documented_added`: one line in
   CONTRIBUTING.md (a doc change).
2. `governance` and `roles_responsibilities`: GOVERNANCE.md, whose content
   the owner approves (a doc change; the facts are Ben's).
3. `dco`: a DCO section in CONTRIBUTING.md and a sign-off check (a doc
   change plus a small CI check).
4. `signed_releases`: the owner decides the release channels; the artifact
   attestations are already the v0.8.0 roadmap line (Ben).
5. `access_continuity` and `bus_factor`: a second person with access, or a
   documented recovery path (Ben).
6. `report_responses`, `enhancement_responses`, `regression_tests_added50`:
   facts from the issue tracker's history (Ben).
7. `know_secure_design` and `know_common_errors`: person claims the owner
   answers on the form (Ben).
8. `accessibility_best_practices`: a scope statement (a doc change; a SHOULD
   that does not block the level).
