The failed-migration runbook (docs/operations.md, "A failed migration"):
the two failures an operator can see, each with the rule that names it.
A newer schema refuses the open with both versions named, and the fix is
the old binary or the restore of section 3 - never a hand-edited
user_version. A start that waits on other processes says which; stop
them. The Proof test forges the newer-schema case: the server refuses
the open with the error, the restore of the backup per section 3 brings
the store back, and the server serves. The fsck-failure runbook is its
own subsection of the routines, pointing at the failing-disk runbook's
Proof test, which runs the restore of section 3 end to end.
