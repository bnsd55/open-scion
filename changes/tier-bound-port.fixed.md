The cache's serving line now names the address the OS bound, not the
one the flag asked for: with `--listen 127.0.0.1:0` the log shows the
real port, so a starter can read the line and connect, the way the
release smoke parses the server's serving line.
