# gc writes the audit's first admin lines

The collector subcommand records one `admin`/`intent` line before its
run and one result after it: `admin`/`ok` with the report's counts, or
`admin`/`failed` with the storage class of the error. `--audit-file`
puts the CLI's lines in the same file the server appends to, so one
audit log holds the whole history of a data directory. The golden pins
the new `admin`/`intent` kind and the `admin`/`ok` line's fields; a
source run that fails after its intent is declared undriven for now,
with its reason in the golden's census.
