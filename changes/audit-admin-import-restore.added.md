# import and restore write the audit's next admin lines

The import subcommand records one `admin`/`intent` line before its run
and one `admin`/`ok` after it, with the import's counts: the commits it
saw, the ones it stored, the ones it verified, and whether it moved the
target ref. The restore subcommand records the same pair around its
replay, in the restored store's own `audit.log`, with the counts the
restore reports: repos, refs, and pending lands. Both take
`--audit-file`, with the same default as `gc`. The golden's
`admin`/`intent` and `admin`/`ok` sections grow the fields of the new
lines. The fix that made this possible: `verify` and `restore` sat
behind an `unreachable!` in the store-open match since the archive
landed, so both subcommands panicked instead of running; they now run
before the store opens.
