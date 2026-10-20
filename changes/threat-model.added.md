A threat model, docs/threat-model.md, replaces the one-line "one
trusted machine" with the profile the code holds: one active server,
its host and data directories trusted, the network and the token
holders not. It names the assets, the actors, the six trust boundaries,
and a security invariant for each promise the code makes, with the
test that proves it and the claims that have no test yet.
