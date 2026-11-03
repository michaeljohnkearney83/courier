# Courier

A small Delay/Disruption-Tolerant Network (DTN) written in Rust. Relay nodes
store bundles and forward them when links come back up.

## Layout

| Path | What it is |
|---|---|
| `crates/bundle` | BPv7-lite bundle types and CBOR codec, with tests and benchmarks |
| `crates/proto` | Generated gRPC code from `proto/` (no `protoc` needed) |
| `crates/relay-node` | Tonic gRPC relay node |
| `crates/link-control` | Axum REST service that switches links on and off |
| `crates/courier-cli` | `courier` command-line client |
| `proto/` | Protobuf API, linted with Buf |
| `fuzz/` | cargo-fuzz targets (nightly only) |
| `deploy/helm/courier` | Helm chart: relay nodes as a StatefulSet |
| `deploy/kustomize`, `deploy/terraform` | Placeholders for week 5 |
| `dashboard/` | Placeholder for the React dashboard (week 6) |
| `docs/` | mdBook tutorial |

## Prerequisites

- Rust via [rustup](https://rustup.rs) (the toolchain in `rust-toolchain.toml` installs itself)
- On Windows: Visual Studio Build Tools with "Desktop development with C++"
- Docker, for NATS and container builds
- Optional: `kind`, `helm`, `buf`, `mdbook`, `cargo-fuzz`

> **Windows: `LNK1104 cannot open file` on every crate** means the build path
> is over 260 characters. Keep the repo in a short folder (e.g. `C:\dev\courier`)
> or point the build output somewhere short:
> `$env:CARGO_TARGET_DIR = "C:\dev\courier-target"` (PowerShell).

## Run it

```bash
cargo test --workspace
cargo run -p relay-node -- --node-id node-a
```

In a second terminal:

```bash
cargo run -p courier-cli -- listen dtn://node-a/inbox
```

In a third:

```bash
cargo run -p courier-cli -- send --from dtn://me/cli --to dtn://node-a/inbox "hello"
cargo run -p courier-cli -- status
```

Bundles addressed to another node are queued; forwarding comes in week 2.

## Other tasks

| Task | Command |
|---|---|
| Benchmarks | `cargo bench -p courier-bundle` |
| Fuzz the decoder | `cargo +nightly fuzz run decode_bundle` (from `fuzz/`'s parent) |
| Link control | `cargo run -p link-control` then `curl -X PUT localhost:8080/links/a--b -H 'content-type: application/json' -d '{"up":false}'` |
| NATS | `docker compose up -d` |
| Container image | `docker build --build-arg BIN=relay-node -t courier/relay-node:dev .` |
| Helm | `helm install courier deploy/helm/courier` |
| Lint proto | `buf lint` |
| Docs | `mdbook serve docs` |

## Configuration

| Variable | Used by | Default |
|---|---|---|
| `COURIER_NODE_ID` | relay-node | `node-a` |
| `COURIER_LISTEN` | relay-node | `0.0.0.0:50051` |
| `COURIER_LINK_LISTEN` | link-control | `0.0.0.0:8080` |
| `COURIER_NODE` | courier-cli | `http://127.0.0.1:50051` |
| `RUST_LOG` | all services | `info` |

## Roadmap

1. Bundle crate, CBOR, property tests, fuzzing, benchmarks
2. Store-and-forward between nodes
3. NATS events, link-control wired in, Tower retries, 3-node integration test
4. Docker, Helm on kind, Prometheus, Grafana, OpenTelemetry
5. Terraform on GKE/EKS, Argo CD, Linkerd
6. React dashboard, mdBook tutorial, demo GIF
