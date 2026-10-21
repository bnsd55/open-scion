ci.yml's macOS test job runs the same two clippy passes as the Linux
lint job (lib+bins and all-targets, -D warnings) before the tests.
The Linux job never compiles macOS-only code - the workspace's one
unsafe FFI module among it - so those lines were never linted; the
job's warm cache makes the added cost a couple of minutes, and no
second macOS runner is booked. The one macOS-only warning this
exposed (an unused mut in the ACL FFI walk, also fixed by #402) is
fixed here so the new gate is green.
