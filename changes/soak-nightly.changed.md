The nightly soak runs 120 minutes instead of 10, so the latency trend's
thirds mean something and a leak that a short run hides has two hours to
show; the run stays bounded by the job's own timeout, and the soak's
150-minute cap keeps a hung client inside it. The three resource
ceilings are named in the workflow — 150 percent of the warm RSS, 100
open files over the start, a late p99 under twice the early one — and
soak.sh takes each from its environment, so a ceiling change is a diff
in one file. The workflow no longer runs on pull requests: the nightly
and a manual `workflow_dispatch` carry it, and its actions carry the
same commit-SHA pins the main workflow got.
