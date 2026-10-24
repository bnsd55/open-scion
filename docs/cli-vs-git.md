# scn for GitHub and git users

Every `scn` command next to the closest `git` and `gh` operation, for readers
arriving from a GitHub workflow. `O/R` stands for an `owner/repo` pair, `B` for
a branch. The full `scn` reference is [cli.md](cli.md); the behavioral details
behind the mappings are in
[architecture.md](architecture.md), and the checkout cost comparison against
`git clone` is in [benchmarks.md](benchmarks.md).

| scn | git | gh | what it does |
|---|---|---|---|
| `scn cat REV:PATH` | `git show REV:PATH` | `gh api repos/O/R/contents/PATH?ref=REV` (a branch, a tag or a commit) | read one file, no clone |
| `scn ls REV[:DIR]` | `git ls-tree REV[:DIR]` | `gh api repos/O/R/contents/DIR?ref=REV` | mode, kind, size, name per entry |
| `scn log [REV]` | `git log --first-parent` | `gh api repos/O/R/commits?sha=B` | history, newest first; `scn log` is first-parent only, `gh` lists all parents |
| `scn blame REV:PATH` | `git blame --first-parent REV:PATH` | none (GitHub has no blame API) | per-line authorship, renames followed |
| `scn show REV` | `git show --stat REV` | `gh api repos/O/R/commits/SHA` | one changeset: message, author, changed paths |
| `scn checkout REV --into DIR` | `git clone O/R DIR && git -C DIR checkout REV` | `gh repo clone O/R DIR` | materialize files on disk |
| `scn checkout REV --sparse DIR` | `git clone --filter=blob:none` + `git sparse-checkout` | (partial-clone flags on `gh repo clone`) | materialize part of the tree |
| `scn mount DIR [REV]` | none (git has no lazy per-file mount) | none | serve a ref over NFS; files load when first read |
| `scn unmount DIR` | none | none | stop the mount |
| `scn commit --message M --put P=F` | `git add F && git commit -m M` | `gh api -X PUT repos/O/R/contents/P` (single file) | create a changeset; multi-file via the Git Data API chain (blobs, tree, commit), then push |
| `scn land CHANGESET` | `git fetch && git rebase && git push` | `gh pr create` + `gh pr merge --rebase` | server rebases the changeset onto the ref and moves it; conflicts name paths |
| `scn diff OLD NEW` | `git diff --name-status OLD NEW` | `gh api repos/O/R/compare/BASE...HEAD` | changed paths, renames found |
| `scn merge-base A B` | `git merge-base --all A B` | none | common ancestors |
| `scn is-ancestor A B` | `git merge-base --is-ancestor A B` | none | exit `0` for yes, `1` for no, same as git |
| `scn bench` | none | none | load generator: concurrent agents landing changesets |
| `--json` | none | `gh --json` | the stable machine-readable surface of each command |

## Differences that matter

**There is no clone, push, pull or branch of your own.** The server holds the
history; a client reads the paths it needs and sends the files it changed. The
branch-and-PR lifecycle maps onto one changeset plus `scn land`, and `main`
moves server-side with each land.

**Conflicts are path-level and never merge text.** A land conflicts when another
changeset changed the same path (or a directory above or below it) after your
base. `gh` would surface the same situation as a failing merge on the pull
request; `scn land` prints the paths and exits nonzero, and a retry of the same
land is safe: the server returns the first result.

**`scn log` is first-parent only**, by design: the history a client walks is the
landed sequence on the ref, not every parallel branch.

**Errors exit 2** on every `scn` command, `--json` output included; `0` is
success and `1` is the "no" of `scn is-ancestor`, matching `git
merge-base --is-ancestor`.
