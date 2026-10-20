The repository now runs the OpenSSF Scorecard itself, weekly and on
every push to main, and publishes the result: the public Scorecard REST
API (and any later badge) reads the score from these runs, and the
SARIF lands in code scanning next to the CodeQL results.

The analysis job carries the four grants the action's docs name —
security-events write, id-token write, actions read, contents read —
and nothing else; it checks out with no persisted credentials.

Dependabot already covers the other half of the gap: cargo and
github-actions updates are weekly and grouped, so the pinned action
SHAs stay current.
