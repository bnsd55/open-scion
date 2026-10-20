No production secret needs to appear in argv. The tokens and the
Postgres URL take a checked-file form (`--token-file`,
`--upstream-token-file`, `--postgres-url-file`, read the way the access
file is: the owner alone may read it, a symlink is refused), the
environment stays documented, `hash-token` reads stdin, and every argv
form prints a deprecation warning naming its replacement. The scripts'
throwaway tokens move to the environment.
