The scn library is the binary's pieces, not an API to build on: the
facade in lib.rs is what the scn binary names (the client's Ctx and its
repo-path parser, each command's run function and arguments), and
everything else is crate-internal. The materialize and vfs modules are
private, the mount session file's record moved out of the public
surface into a crate-internal module, and the one test seam is a
test_support module behind the test-hooks feature (default off), which
holds the client preset, the session-file writer and the NFS
filesystem that the integration tests drive. The encoded bytes, the
commands and their output are unchanged.
