The fuzz campaign gets one more target, `archive-read`: the archive
section readers (`read_manifest`, `read_changesets`, `read_refs`,
`read_lands`, `read_members`, `read_provenance`), each fed raw bytes
through the test seam. An archive from an untrusted source is the door
the section readers face; the whole-archive door was already fuzzed
(`archive-verify`). The fuzz workflow gains a `budget_scale` dispatch
input that multiplies every target's budget, so a manual run can give
every target an hour instead of the nightly minutes.
