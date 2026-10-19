# Security policy

## Supported versions

openscion is an early proof of concept. Only the latest commit on `main` is
supported, and it is not meant for production use yet.

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Report it privately through GitHub:
[**Report a vulnerability**](https://github.com/bnsd55/openscion/security/advisories/new)
(the *Security* tab of this repository).

Include what you found, how to reproduce it, and what an attacker could do with
it. You will get an answer within seven days. Once a fix is ready, the report
is published as a security advisory, with credit to you unless you prefer
otherwise.

## Scope

Known limits of the current design are documented in the code as `SHORTCUT:`
comments, in the README, and in
[`docs/operations.md`](docs/operations.md). For example: an access file maps
one token to one principal with per-path rules, and the network is closed by
default — the server refuses a non-loopback address unless it serves TLS
(`--tls-cert` and `--tls-key`) or gets `--insecure-listen`. A report about a
documented limit is still welcome if it shows an impact the documentation
does not mention.
