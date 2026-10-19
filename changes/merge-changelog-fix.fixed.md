The changelog merge script refused every section that held more than
one fragment: its file-name check read the newlines between the paths
of the fragment list as whitespace, so a release whose fragments share
a section stopped at the merge. The check now runs on each fragment's
own path, and the merge script's fixture test holds two fragments in
one section, so the refusal cannot come back. Two changelog rows read
wrong: the release-smoke rows carried backticks around their issue
numbers and the older row said macOS stays out of CI for the job,
which the newer row made untrue; the stability row and the policies
row both introduced docs/stability.md, so the policies row now leads
with the toolchain floor; the backup row dropped its commit-style
prefix.
