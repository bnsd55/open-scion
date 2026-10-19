# open-scion-server

Server for the Scion version control system: storage, commits, lands
and reads.

The `scion-server` binary: one store per data directory, gRPC over TLS,
crash-safe lands and blob storage.

Part of [openscion](https://github.com/bnsd55/openscion), a version
control system for large monorepos. See the repository for the design
and the rest of the system.

```sh
cargo install open-scion-server
```
