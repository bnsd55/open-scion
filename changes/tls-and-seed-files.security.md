The files a tier loads that must stay whole — the TLS certificate and
the client CA of a server, the CA, client certificate, and seed
snapshots of a cache or daemon — were read by path with no check: a
symlink, a file another user owns, or a mode that lets another user
rewrite the file all passed. A swapped certificate lets someone else
be the server, a swapped CA changes whose client certificates the
server accepts, and a swapped seed can seed what the daemon serves.
These reads go through one checked descriptor now (read_shared_file:
ancestors checked, O_NOFOLLOW, kind, owner, and a mode no other
account can write, checked on the open descriptor the bytes are read
from), as the secrets already did. The checked open is its own step
now, so a seed the size of a disk streams through a buffered reader
instead of being held whole in memory.
