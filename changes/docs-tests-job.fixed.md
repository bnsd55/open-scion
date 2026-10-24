A docs-only pull request never ran the docs tests: the test job skips
them, and the docs job ran only rustdoc, so a broken link or a stale
doc landed and failed main's first run. A docs-tests job now runs the
docs_*.rs integration tests on every pull request (and the same
events), and fails when a docs_*.rs file exists that it does not name.
