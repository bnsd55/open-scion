# The archive's golden

The archive format joins the machine-checked surfaces: a checked-in
golden (`tests/archive_v1.golden`) pins every file of the export of
one fixed store by hash and path - the manifest's clock reading and
store UUID pin nothing - and pins the changeset ID the restored
store's `main` resolves to. The same test runs `verify` on the golden
with no store code and a restore into an empty directory, `fsck`
clean. A diff in the golden is a format change: a new archive version
beside v1 per docs/archive-format.md, named in a changes/ fragment.
Re-pin on purpose with `SCION_UPDATE_ARCHIVE_GOLDEN=1`.
