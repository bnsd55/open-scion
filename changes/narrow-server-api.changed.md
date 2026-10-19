The scion-server modules are private except the fsck and gc
operations: the crate's API is the facade in lib.rs, and the test
seams sit in one test_support module behind the test-hooks feature.
