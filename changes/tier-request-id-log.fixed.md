The cache and the daemon log the call's request ID on their GetObjects
line, the way the server's call lines do: one command is followable
through every hop's log, which the operations guide and the README
promise. The reply runs outside the call, so it now stays in the call's
span, as the server's replies do. A test sends one ID through a daemon
and a cache and finds it in both logs and on the fetch upstream.
