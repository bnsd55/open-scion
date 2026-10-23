The fault scripts hand `--postgres-url` to the server root command
  before the subcommand again: `serve`, `import`, `gc` and `fsck` take
  no such flag of their own, so the NFS mount job's Postgres run failed
  to start since the fault-matrix change.
