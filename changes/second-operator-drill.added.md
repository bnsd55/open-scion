# The second operator's drill, in the record's shape

docs/drills/second-operator.md is the drill a human second operator
runs (#396): ten tasks from install to the alert runbooks, each with
its runbook section, the runbook's own commands verbatim, its
done-when, and a blank for the operator's notes; the header holds the
record of who ran it, when, and against which commit. A command that
the runbook does not print does not appear: the two steps the runbook
states in prose (creating the data directory, the gc first run's
`--dry-run`) say so and hand the choice to the operator's notes. The
database-container step is marked Linux. The guesses log stays: a
filled row is a gap in the runbook, not a failure.
