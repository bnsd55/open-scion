# The snapshot file's golden

The snapshot file joins the machine-checked surfaces: a checked-in
golden (`tests/snapshot_v1.golden`) pins the bytes today's writer
produces for a fixed object set, and the same golden seeds back into
an empty store. A diff in the golden is a format change: stability.md
says a new magic beside `scnsnap1\n`, read beside v1, and a changes/
fragment naming the break. The test header says how to re-pin on
purpose (`SCION_UPDATE_SNAPSHOT_GOLDEN=1`). The codec and proto
goldens get their on-purpose update paths written in their headers
too - the codec's is "a grammar change is a new format, never a
re-pin", the proto's is "re-pin additions only; a removal is v2".
