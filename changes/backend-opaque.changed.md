The server's Backend no longer says which database it holds: the
public Sqlite and Postgres variants are gone, and the backend is an
opaque struct with one private engine behind it, opened by the same
engine-agnostic constructors as before. A caller matches behavior
through the methods, never the engine, so a new engine (or a major
bump of one) is a private change of the server crate, not a break of
its API. The tests that build a backend from a store they prepared,
or read the data directory's own store out of one, find both in
test_support::backend (from_store, and BackendExt::sqlite), where the
test seams live since the facade became the API.
