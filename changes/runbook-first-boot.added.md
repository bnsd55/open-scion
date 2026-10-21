The first-boot runbook (docs/operations.md, "First boot"): a token file
and the three commands of a first start, each with its promise - the
token goes in a 0600 file (umask 077, then printf), not in argv, the
binary creates the data directory and serves, the health service
answers for the service name and for liveness, and the first import
makes a repo the client reads. The Proof test runs the commands in
order against a fresh data directory - the harness's server now takes
its token from the file too - and reads the imported ref back.

The certificate-rotation runbook (docs/operations.md, "Rotating the
server's certificate"): a new certificate of the same CA needs no client
change - the rotation is write the pair, restart, and the client's
unmoved CA file still connects. The Proof test rotates the server's
certificate under a connected client and reads back over https. A new
CA is the drill record of docs/drills/, since no test can hold a
running fleet's client CA files.
