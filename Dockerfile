# Build any workspace binary: docker build --build-arg BIN=relay-node -t courier/relay-node .
FROM rust:1-bookworm AS build
ARG BIN=relay-node
WORKDIR /src
COPY . .
RUN cargo build --release --bin ${BIN} && cp target/release/${BIN} /app

FROM gcr.io/distroless/cc-debian12
COPY --from=build /app /app
ENTRYPOINT ["/app"]
