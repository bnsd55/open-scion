The unix-socket serving helper moved to scion_proto::runtime::listen,
next to the TCP listener it shares the bounds with: scion-daemon and
the tests that serve a daemon over its socket bind through the proto
runtime, and scion-tier no longer carries serving plumbing in its
public surface. The check the bind makes is unchanged: a real
directory, owned by this user, closed to others, under parents no
other user can change, and a dead socket file replaced.
