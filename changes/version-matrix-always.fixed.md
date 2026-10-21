The version matrix always has an N-1, from the first nightly: `PREV_REF`
beats the release tags, the newest tag that is an ancestor of HEAD and
not HEAD itself wins next, and with neither, the newest of the stand-in
ladder - the two pinned commits that
[store-upgrade.sh](scripts/store-upgrade.md) runs, chosen where the
store schema changed - stands in until the first release tag replaces
them. A run that tests no pair fails with the reason; the matrix never
passes as a skip. Two windows of the N and N-1 pair are proved, both
directions: the ticket one version's server signs verifies on the other
version's tier, and a ticket of another key is refused with the ticket
reason; the snapshot one version's cache writes opens as the other
version's daemon seed, and a checkout through that daemon reads the
repo whole. The tier pair's read is a checkout, not a `cat`: `cat`
reads at the server and never reaches the tier, so only a checkout
exercises the GetObjects the ticket gates.
