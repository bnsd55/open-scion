Add a mount-reps workflow: on demand only, on macos-15, it builds the
scn lib test binary once and runs the fork-gated mount tests the
number of times the reps input gives (100 by default), stopping at
the first failure and uploading that run's log. This is the repeated
evidence that the fork gate (#309) fixed the macOS mount flake.
