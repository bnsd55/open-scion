# A client's connection notices a dead peer

## The product clients run the server's keepalive, on their side

`scn`'s client and a tier's upstream client now run HTTP/2 keepalive
pings (every 30 s, an answer within 10 s, while the connection is busy
or idle) and TCP keepalive probes (every 15 s), the mirror of the
server's own settings. A peer that died without a FIN or RST — a
frozen process, a host gone — ends every call of the client in about
40 s, where a call's own deadline was the only bound (`scn`'s is
600 s), and the tier's whole upstream window.

## Why

A kill that the kernel does not turn into a FIN or RST leaves the
connection half-dead: the socket is open and nothing answers. The
server already closed its side of such connections; the clients now
see the same thing from their side, and the values are named
constants beside the server's own, so the pair stays a pair.

A test that proves the dead-peer failure needs no 40 s of its own:
the `test-hooks` feature of the protocol crate shortens the keepalive
for the test's connection, the same failure is proved in seconds, and
compile-time asserts pin the product constants at their documented
values.
