CI stops re-saving a rust-cache for every pull-request branch, which
had grown to 66 caches and 10.6 GB - past GitHub's 10 GB ceiling, so
caches were evicting each other and hits were shrinking. Caches save
from main only now; a pull request restores main's. The three cargo
test jobs also cache the workspace crates' own artifacts
(cache-workspace-crates), so the postgres job stops rebuilding the
bins the SQLite job just built: about five minutes off the job that
finishes last in 13 of 20 runs. The fuzz smoke runs its ten targets
four at a time (20 s each) instead of serially (15 s each), which cuts
the job from ~11 minutes to ~7 on the runs where it sets the tail.
