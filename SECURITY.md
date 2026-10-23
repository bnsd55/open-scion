# Security policy

## Supported versions

openscion is alpha software (0.x). Only the latest commit on `main` is
supported, and it is not meant for production use yet. What each
severity level gets, and for how long:
[docs/support.md](docs/support.md).

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Report it privately through GitHub:
[**Report a vulnerability**](https://github.com/bnsd55/openscion/security/advisories/new)
(the *Security* tab of this repository).

Include what you found, how to reproduce it, and what an attacker could do with
it. You will get an answer within seven days. Once a fix is ready, the report
is published as a security advisory, with credit to you unless you prefer
otherwise.

## How a report is handled

The six paragraphs below are a **draft** the owner reviews: every number is
**proposed; the owner decides** (the drill that found the gaps is
[docs/drills/vulnerability-drill.md](docs/drills/vulnerability-drill.md)).

1. **Where the fix is developed.** A confirmed report is fixed in the
   temporary private fork GitHub offers from the draft advisory. Nothing of
   the fix lands on `main` before the advisory publishes. *(proposed; the
   owner decides)*
2. **Triage.** Today one maintainer triages a report: acknowledges it,
   reproduces it, and sets its severity - named as a limit of the project,
   not a promise of availability. A second reader for security reports is a
   v0.8 goal. *(proposed; the owner decides)*
3. **Severity.** The advisory carries a CVSS 4.0 base score, mapped to
   [docs/support.md](docs/support.md)'s four levels (critical, high, medium,
   low) for what the reporter can expect. *(proposed; the owner decides)*
4. **CVE.** A finding scored medium or higher gets a CVE ID, requested
   through GitHub as the CNA on the draft advisory; lower findings publish
   as advisories without one. *(proposed; the owner decides)*
5. **The timeline.** Acknowledge within **7 days** (as above); a fix
   target of **30 days** for critical and high, **60 days** for medium,
   **90 days** for low; coordinated disclosure at **90 days** from the
   report, so a report never waits on the calendar forever. *(all numbers
   proposed; the owner decides)*
6. **How the fix ships.** A security fix rides a patch release cut with the
   release workflow - a signed tag and an immutable release, per
   [RELEASING.md](docs/RELEASING.md) - from the earliest supported branch
   the fix applies to. While only the latest commit on `main` is supported,
   that branch is `main`, and the fix rides the next release; after the
   v0.4 N-1 window opens, the affected supported branches each get one.
   *(proposed; the owner decides)*

## Scope

Known limits of the current design are documented in the code as `SHORTCUT:`
comments, in the README, and in
[`docs/operations.md`](docs/operations.md). For example: an access file maps
one token to one principal with per-path rules, and the network is closed by
default — the server refuses a non-loopback address unless it serves TLS
(`--tls-cert` and `--tls-key`) or gets `--insecure-listen`. A report about a
documented limit is still welcome if it shows an impact the documentation
does not mention.
