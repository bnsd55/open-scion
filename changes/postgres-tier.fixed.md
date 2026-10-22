The tier job that decides what a pull request runs now starts the
Postgres tests for any change under the server's `src/`, not only for
the Postgres, metadata, backend and db files: the server's correctness
depends on the Postgres transaction order. A test under
`crates/scion-server/tests/` opts in when its text names the shared
backend (`open_test_shared`) or the env var (`SCION_TEST_BACKEND`),
instead of a hand list.
