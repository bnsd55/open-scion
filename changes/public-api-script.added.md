The crates publish only after their public APIs are on the record:
scripts/public-api.sh writes, for each of the five publishable crates,
what cargo-public-api sees as the crate's public API, one file per
crate under the folder the script takes as its argument. The step for
the crates.io publish of #21 is to run it on the tagged commit and
read the diff against the previous release's listings before anything
publishes. The script installs cargo-public-api and the nightly
toolchain it needs when they are missing, and prints one line per
crate with the line count of its listing.
