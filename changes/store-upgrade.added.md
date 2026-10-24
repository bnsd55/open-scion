A CI job opens a store written by each tagged release and upgrades it
directly: the tagged store opens under this build, the migration runs
forward, reads and writes then work, and a store of this build is refused
by the tagged binary with the version reason. The direct upgrade must be
the same store that upgrading stepwise through every tag gives -- a
migration that is order-dependent fails the job. The stores are built in
CI from the tags and read back under their own tag's binary first; with
no release tag the job says so and starts at v0.1.0.
