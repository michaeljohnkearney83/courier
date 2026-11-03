# syntax=docker/dockerfile:1
# Build any workspace binary: docker build --build-arg BIN=relay-node -t courier/relay-node .
FROM rust:1-bookworm AS build
ARG BIN=relay-node
WORKDIR /src
COPY . .
# Cache mounts keep the registry and build output between builds, so
# rebuilding, or building the other binary, only compiles what changed.
# `locked` makes parallel builds (docker compose builds both images at once)
# take turns instead of racing on the same cache.
# rust-toolchain.toml is in .dockerignore: the image's own stable toolchain
# builds release binaries without downloading clippy and rustfmt.
# COPY keeps the files' original mtimes, which can be older than the cached
# build output, so cargo would reuse a stale binary. Touching our own sources
# forces the workspace crates to rebuild; dependencies stay cached.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    find crates proto -type f -exec touch {} + \
    && cargo build --release --bin ${BIN} && cp target/release/${BIN} /app

FROM gcr.io/distroless/cc-debian12
COPY --from=build /app /app
ENTRYPOINT ["/app"]
