The flaky-issue bot of the v0.8.0 line, held out of main until Ben
decides on public automation: a scheduled `ci.yml` run that fails
records itself as one comment on the open issue titled "nightly: flaky
or real?" (or opens it), with the run URL and the failing tests' names.
A red run whose log names no failed test says so - a setup step or the
runner died, and a human should look before anyone calls it flaky.

The trigger is `workflow_run` on CI's completion, so no job list in
this file has to name every job of ci.yml. Permissions: the report job
alone holds `issues: write` and `actions: read`; the workflow's top
level holds none. The issue's closure is what lets the nightly streak
reach 30.
