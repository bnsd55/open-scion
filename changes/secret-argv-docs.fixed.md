The operations guide now shows every secret's deployment form first:
the Postgres example gives the URL through --postgres-url-file (or
SCION_POSTGRES_URL), not on the command line, and the flags and
environment section states the rule the flags follow - a secret has a
file form the owner alone may read, an environment form visible to
the same user and root, and an argv form that exists for tests,
warns, and is readable by every local account through ps. No flag
changes.
