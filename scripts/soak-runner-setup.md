# The 72-hour soak runner

The `soak-72h` workflow (`.github/workflows/soak-72h.yml`) runs the
72-hour soak on one rented VM. This file registers that runner and
deletes everything after. Nothing here runs until Ben rents the VM and
runs these steps himself.

## The public-repo risk, and the two doors it needs

The repository is public: anyone can open a fork pull request, and a
fork PR's workflow file is the fork's own - it can name
`runs-on: [self-hosted, scion-soak]` and wait for this runner. Two
doors close that:

1. The runner exists only for the run. It is registered at the start,
   and the last step removes it (`config.sh remove`), so the window is
   the one rental, not a standing machine.
2. Fork PR workflows need approval before they run. Before registering
   the runner, the owner tightens the policy so every outside
   contributor's workflow waits for an owner:

   ```sh
   gh api -X PUT repos/bnsd55/openscion/actions/permissions/fork-pr-contributor-approval \
     -f approval_policy=all_external_contributors
   ```

   The repository's standing policy is `first_time_contributors`; the
   tightened setting is for the rental window only, and step 4 puts it
   back.

## Before the run

1. Rent a fresh Ubuntu 24.04 VM with **about 300 GB of free disk for
   the store** (one 30-minute release soak grew the store 334 MB ->
   8.95 GB beside 22,930 lands, about 400 KB a land and slowing, and
   the 72-hour run lands about 3.3 million times). 2 vCPUs and 8 GB of
   RAM match a hosted runner. Sudo, and outbound HTTPS.
2. Install what the workflow and the soak assume: rustup stable,
   protoc, git, curl, `/usr/bin/time` (Ubuntu: `apt install time`),
   `lsof`.
3. The owner tightens the fork-PR policy (the command above).
4. Register the runner as a **normal runner, not ephemeral**: the
   workflow has two jobs on this runner (the 72-hour soak, then the
   upload), and an ephemeral runner takes exactly one job and
   de-registers, which would leave the upload job waiting forever
   ([GitHub's ephemeral
   docs](https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/about-ephemeral-runners)).
   A registration token (Settings -> Actions -> Runners -> New
   self-hosted runner, or the API; valid 1 h):

   ```sh
   curl -sS -X POST \
     -H "Authorization: Bearer $GH_TOKEN" \
     https://api.github.com/repos/bnsd55/openscion/actions/runners/registration-token \
     | jq -r .token

   ./config.sh --url https://github.com/bnsd55/openscion \
     --token "$TOKEN" \
     --labels scion-soak \
     --unattended \
     --work /home/runner/_work
   ./svc.sh install && ./svc.sh start   # or ./run.sh in a tmux
   ```

   No other workflow names the `scion-soak` label, and the two jobs of
   this one are serialized by `needs`, so the runner serves the soak
   and then the upload and nothing else.

## During

Nothing to watch but the run's own page. The samples stream to the
runner's disk as they are taken (`metrics.csv`, `metrics-store.csv`,
the logs) into `/home/runner/soak-72h/soak-artifacts/` - the run is
readable even if the VM dies at hour 71. The job's `GITHUB_TOKEN` is
dead after 24 h, which is why the upload is a separate short job with
a fresh token (`needs: soak-72h`, `if: always()`).

## After the run

1. The `upload` job puts `soak-72h-artifacts` on the run page. Save it.
2. Remove the runner from the VM and the repository:

   ```sh
   cd actions-runner && ./svc.sh uninstall && ./config.sh remove --token "$TOKEN"
   ```

3. Delete the VM.
4. The owner puts the fork-PR policy back:

   ```sh
   gh api -X PUT repos/bnsd55/openscion/actions/permissions/fork-pr-contributor-approval \
     -f approval_policy=first_time_contributors
   ```

5. If a runner entry lingers anyway (the VM died mid-job): the
   repository's Settings -> Actions -> Runners, remove the entry, or
   `DELETE /repos/bnsd55/openscion/actions/runners/{id}`.

Nothing is registered until Ben rents the VM and runs these steps; the
workflow and this file are only the preparation.
