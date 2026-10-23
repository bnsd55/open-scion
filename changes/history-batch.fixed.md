The import's history nodes are written in batches: the nodes a
transaction inserted answer its own reads, and the flush writes one
statement per 1,000 rows after the derived rows and before the git
import rows. An import of 1,000 commits costs about two history
statements per transaction instead of four per commit.
