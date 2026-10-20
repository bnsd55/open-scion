CI fails if the test suite leaves the work tree dirty: after every
`cargo test` in the three test jobs, `git diff --exit-code` runs, and its
failure names the files a test wrote outside its sandbox.
