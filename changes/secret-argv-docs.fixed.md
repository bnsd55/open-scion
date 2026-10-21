The operations guide now shows every secret's deployment form first:
the Postgres example gives the URL through --postgres-url-file (or
SCION_POSTGRES_URL), not on the command line, and the flags and
environment section states the rule the flags follow - a secret has a
file form the owner alone may read, an environment form visible to
the same user and root, and an argv form that
is deprecated: it warns now, goes away in a later release, and ps
shows its value to every local account. No flag changes.
