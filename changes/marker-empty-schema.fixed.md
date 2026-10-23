A data directory whose marker names a store refuses a database
  schema that holds no store, so a `--postgres-url` whose search_path
  names the wrong or a new schema fails at the start, naming the
  directory's store, instead of adopting its identity into an empty
  store that every later call reads as "repo not found". A schema that
  holds a store's rows but no store UUID row - a dump taken before the
  UUID existed - is still adopted.
