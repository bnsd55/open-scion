The version matrix (#239): a nightly job runs the pairs of the
compatibility policy at versions N and N-1, building the tagged release
behind HEAD, so a release that breaks a listed pair is caught by CI.
The pairs: scn against the server, the cache tier against the server,
scn against the daemon over its socket, each in both directions, and
the store, which a newer server opens and migrates and an older one
refuses. A dispatch can name any commit or tag as N-1, so a pair can be
proven before the first release exists. Until a release is tagged the
job says it skipped, in the run summary.
