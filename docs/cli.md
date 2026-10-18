# The scn command reference

`scn` is the command-line client of Scion. Every command talks to a server
over gRPC, prints human-readable text by default, and takes `--json` for
machine-readable output. An error exits 2; `0` is success and `1` is the
"no" of `scn is-ancestor`.

## Global flags and environment variables

Every command takes these flags, before or after the subcommand:

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--token TOKEN` | `SCION_TOKEN` | none, required | Bearer token for the server. Pass it in `SCION_TOKEN`, not here: the command line is readable by every local user (`ps`), the environment of your own process is not. |
| `--server ADDR` | `SCION_SERVER` | `http://127.0.0.1:7878` | Server address: `http://HOST:PORT`, or `unix:///PATH` for a `scion-daemon` on this node. |
| `--objects ADDR` | `SCION_OBJECTS` | none | Address of a `scion-cache` or `scion-daemon` that serves `GetObjects` with a ticket. Without it, object reads go to `--server`, which also has them. |
| `--tls-ca FILE` | `SCION_TLS_CA` | none | Trust this CA (PEM) for an https server. |
| `--repo NAME` | `SCION_REPO` | `main` | Repository name. |
| `--json` | none | off | Print machine-readable JSON. |

`scn commit` also reads `SCION_AUTHOR` for its `--author` flag.

## The commands

### Reading

- `scn cat REV:PATH` — print a file: `scn cat main:src/lib.rs`. `REV` is a
  ref name or a changeset ID.
- `scn ls REV[:DIR]` — list a directory: `scn ls main:src`, or `scn ls
  main` for the root. Each line shows the mode, the kind (`file`,
  `dir`, `symlink`), and the size that the tree carries; `--json` adds
  `size` to the entries that carry one.
- `scn log [REV]` (default `main`) — first-parent history, newest first,
  `-n/--limit` how many changesets to show (default 10). `--path PATH`
  shows only the changesets that change this file, or anything below
  this directory.
- `scn blame REV:PATH` — the changeset that last changed each line of a
  file, on first-parent history. It follows renames as `git blame` does;
  `--json` adds `path` to each range when some line came from another
  path.
- `scn show REV` — one changeset and its changed paths.

### Writing

- `scn checkout REV --into DIR` — write the files of a changeset into a
  new directory (`--into` is required). `--sparse DIR` writes only that
  directory and everything below it; repeat for more. An existing empty
  directory is kept; a signal removes what was written and then dies by
  the signal, so a shell reports 130 for Ctrl-C.
- `scn mount DIR [REV]` (default `main`) — serve the ref as an NFSv3
  filesystem that reads files on demand. `--no-mount` serves NFS but does
  not try to mount: it prints the mount command instead. `scn unmount
  DIR` stops the mount.
- `scn commit --message MESSAGE` (`-m`) — create a changeset on the server. It moves no
  ref; use `scn land` next. `--base REV` is the base (default `main`);
  `--put PATH=FILE` puts the content of local `FILE` at `PATH`
  (repeatable), `--delete PATH` deletes, `--executable` marks the put
  files executable, and `--author "Name <email>"` (default
  `scn <scn@localhost>`) names the author. `--from-mount DIR` takes the
  changes from the overlay of a `scn mount` on `DIR`, instead of `--put`
  and `--delete`.
- `scn land CHANGESET` — rebase a changeset onto a ref and move the ref
  (`--onto`, default `main`). `--request-id KEY` is the retry key; it
  defaults to the changeset ID and ref, so a retry of the same land is
  safe.
- `scn bench` — many agents commit and land at the same time and print
  throughput: `--agents` (default 200), `--lands` (default 10000),
  `--base main|fresh` (default `shared`: each agent re-reads `main`
  before every change only in `fresh`). It fails unless `main` gained
  each landed change exactly once.

### Queries

- `scn merge-base A B` — the best common ancestors of two revisions, over
  all parents, as `git merge-base --all`.
- `scn is-ancestor ANCESTOR DESCENDANT` — exit 0 if `ANCESTOR` is
  `DESCENDANT` or one of its ancestors, 1 if not. An error exits 2, so it
  never reads as the "no".
- `scn diff OLD NEW` — the paths whose file differs between two
  revisions, as `git diff --name-status`. A rename prints as `R`, a tab,
  and `old -> new`; `--path PATH` limits to a file or a directory.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | The "no" of `scn is-ancestor`. |
| 2 | An error: a failed call, a bad argument, a connection that did not open. |

A checkout that a signal stops removes what it wrote, and then dies by
the signal, so a shell reports 128 plus the signal number (130 for
Ctrl-C) and a shell loop stops too.
