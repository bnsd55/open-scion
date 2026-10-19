The version matrix's dispatch could pass green without testing a pair:
the workflow resolved the N-1 ref in one step, and the step that ran
the matrix never received it, so the script resolved again, found no
release tag, and printed its skip. The run step gets the resolved ref
now, and the script fails when the resolve step left its ref file but
the run received no ref, so a wiring slip can never be green again.
