a pull request runs only the fast tier: fmt + clippy, the SQLite
tests with 500 proptest cases and the docs tests. A pull request
that touched only docs, markdown or changelog fragments runs the
docs tests alone; a draft pull request skips every job. Main, the
nightly and a dispatch run the full suite as before.
