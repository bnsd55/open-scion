A land's verdict is final only against the head it was planned on: the
settle now re-reads the ref version inside its own transaction, and an
import that moved the ref between the plan and the settle makes the land
replan instead of recording a stale answer.
