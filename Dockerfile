# The builder is pinned to the MSRV, 1.88, the exact version the `check
# (MSRV)` CI job checks. The image then always builds with a toolchain the
# project supports, and a fixed tag cannot drift the way `rust:1` does.
# Bump this line together with the MSRV in ci.yml. The digest pins the
# multi-arch image index (verified 2026-10-21); the tag names it.
FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY proto proto

# The cargo caches live outside the image layers: they only speed up a
# rebuild on this machine. The binaries are copied to /out, because a cache
# mount's content does not survive into the next stage.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release -p open-scion-server -p open-scion-tier -p open-scion \
    && mkdir /out \
    && cp target/release/scion-server target/release/scion-cache target/release/scn /out/

# The runtime base, pinned by its multi-arch index digest (verified
# 2026-10-21); the tag names it.
FROM debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin scion \
    && mkdir -p /data /certs /work \
    && chown scion:scion /data /certs /work

COPY --from=build /out/scion-server /out/scion-cache /out/scn /usr/local/bin/

USER scion:scion

# One image carries all three binaries; each service in docker/compose.yml
# picks its own entrypoint. Data and certificates come from volumes.
