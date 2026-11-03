# Courier

A small Delay/Disruption-Tolerant Network (DTN) written in Rust. Relay nodes
store bundles and forward them when links come back up.

## Layout

| Path | What it is |
|---|---|
| `crates/bundle` | BPv7-lite bundle types and CBOR codec, with tests and benchmarks |
| `crates/proto` | Generated gRPC code from `proto/` (no `protoc` needed) |
| `crates/relay-node` | Tonic gRPC relay node |
| `crates/links` | Link names (`node-a--node-b`) and the NATS bucket that holds link state |
| `crates/link-control` | Axum REST service that switches links on and off |
| `crates/courier-cli` | `courier` command-line client |
| `proto/` | Protobuf API, linted with Buf |
| `fuzz/` | cargo-fuzz targets (nightly only) |
| `deploy/compose` | Prometheus config and the provisioned Grafana dashboard for the Compose stack |
| `deploy/helm/courier` | Helm chart: relay nodes as a StatefulSet, link-control and NATS |
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

### Two nodes: store and forward

Start node-a with node-b as a peer, but leave node-b down for now:

```bash
cargo run -p relay-node -- --node-id node-a --listen 127.0.0.1:50051   --peer node-b=http://127.0.0.1:50052 --store-dir ./data/node-a
cargo run -p courier-cli -- send --from dtn://me/cli --to dtn://node-b/inbox "hello b"
cargo run -p courier-cli -- status   # 1 queued; node-a logs "link down"
```

Then start node-b. Within a second node-a forwards the bundle:

```bash
cargo run -p relay-node -- --node-id node-b --listen 127.0.0.1:50052
cargo run -p courier-cli -- --node http://127.0.0.1:50052 status   # 1 delivered
```

Routing is static:

- a bundle for `dtn://<node>/...` goes straight to `<node>` if it is a `--peer`;
- otherwise it goes through the peer named by `--route <node>=<peer>`;
- `--route '*=<peer>'` sets a default route.

A peer that is briefly unavailable gets `--forward-retries` quick retries
(100 ms, then 200 ms, ...) before its link counts as down; after that it is
retried every `--forward-interval-ms`. Bundles with no
route stay queued until their lifetime runs out. Without `--store-dir`, the
queue is kept in memory only.

### Cutting links with link-control

link-control switches links on and off and writes their state to a NATS
JetStream key-value bucket (`courier-links`). Relay nodes started with
`--nats-url` watch that bucket. While a link is switched off, the nodes at either end don't forward
over it; when it comes back on they forward straight away. A node that starts
late still picks up links switched off before it came up.

```bash
docker compose up -d   # NATS with JetStream
cargo run -p link-control -- --nats-url nats://127.0.0.1:4222
# start the relay nodes as above, adding --nats-url nats://127.0.0.1:4222
curl -X PUT localhost:8080/links/node-a--node-b -H 'content-type: application/json' -d '{"up":false}'
curl localhost:8080/links   # {"node-a--node-b":false}
```

Links are undirected: `node-b--node-a` is stored as `node-a--node-b`. A link
with no entry is on. Without `--nats-url`, link-control keeps state in memory
only and relay nodes treat every link as on.

## Full stack in Docker

```bash
docker compose --profile stack up -d --build
```

This runs NATS, link-control, three relay nodes in a line
(`node-a -- node-b -- node-c`), Prometheus, Grafana and Jaeger. The first build
compiles the workspace inside Docker and takes a few minutes; later builds
reuse the cache.

| Service | Address |
|---|---|
| node-a, node-b, node-c (gRPC) | `localhost:50051`, `50052`, `50053` |
| link-control | `localhost:8080` |
| Prometheus | <http://localhost:9090> |
| Grafana, Courier dashboard | <http://localhost:3000> (anonymous viewer; `admin`/`admin` to edit) |
| Jaeger, traces | <http://localhost:16686> (service `relay-node`) |

Send a bundle from node-a to node-c, cut the `node-b--node-c` link and watch
node-b's queue in Grafana:

```bash
curl -X PUT localhost:8080/links/node-b--node-c -H 'content-type: application/json' -d '{"up":false}'
cargo run -p courier-cli -- send --from dtn://me/cli --to dtn://node-c/inbox "hello c"
curl -X PUT localhost:8080/links/node-b--node-c -H 'content-type: application/json' -d '{"up":true}'
```

`docker compose --profile stack down` stops everything; add `-v` to also
delete the nodes' stored bundles.

## Kubernetes with kind and Helm

The chart runs the relay nodes as a StatefulSet (`relay-node-0`, `-1`, `-2`),
each with its own volume for queued bundles, plus link-control and a NATS
server. Every node peers with every other node; cut links with link-control to
force bundles through a particular path.

```bash
kind create cluster --name courier
docker compose --profile stack build
for img in relay-node link-control; do
  docker save --platform linux/amd64 -o $img.tar courier/$img:dev
  kind load image-archive $img.tar --name courier
done
helm install courier deploy/helm/courier
kubectl rollout status statefulset/relay-node
```

The images go in as single-platform archives because `kind load docker-image`
fails against Docker Desktop's containerd image store ("content digest ...
not found"). On Apple silicon use `--platform linux/arm64`. After rebuilding
the images, load them again and run `kubectl rollout restart statefulset/relay-node deployment/link-control`.

Talk to it through port-forwards, each in its own terminal:

```bash
kubectl port-forward pod/relay-node-0 50051:50051
kubectl port-forward svc/link-control 8080:8080
```

Then cut the direct link from node 0 to node 2 and send a bundle. It waits on
`relay-node-0`, survives that pod being deleted (it is stored on the pod's
volume), and is forwarded once the link is back:

```bash
curl -X PUT localhost:8080/links/relay-node-0--relay-node-2 -H 'content-type: application/json' -d '{"up":false}'
cargo run -p courier-cli -- send --from dtn://me/cli --to dtn://relay-node-2/inbox "hello"
cargo run -p courier-cli -- status   # 1 queued
curl -X PUT localhost:8080/links/relay-node-0--relay-node-2 -H 'content-type: application/json' -d '{"up":true}'
```

Settings such as `relayNode.replicas`, `relayNode.storage` and
`nats.enabled` / `nats.externalUrl` are in
[`deploy/helm/courier/values.yaml`](deploy/helm/courier/values.yaml). Pods have
`prometheus.io/scrape` annotations for a cluster Prometheus. Remove everything
with `kind delete cluster --name courier`.

## Tracing

Set the standard `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g. `http://localhost:4318`)
and each relay node sends OpenTelemetry traces over OTLP/HTTP. A bundle carries
the W3C `traceparent` of the span that last handled it, so one trace follows a
bundle across every hop:

```
send_bundle (node-a)
└─ forward_bundle → node-b
   └─ receive_bundle (node-b)
      └─ forward_bundle → node-c     ← starts late if the link was down
         └─ receive_bundle (node-c)
```

The gap before a `forward_bundle` span is the time the bundle waited for a
link. Failed attempts show up as `forward_bundle` spans with
`outcome=link down`. Spans have `service.name=relay-node` and
`service.instance.id=<node id>`. In the Helm chart set
`relayNode.otlpEndpoint`. Without an endpoint, nodes only log.

## Metrics

Each relay node serves Prometheus metrics at `http://<node>:9464/metrics`. Every
series has a `node` label:

| Metric | Type | Meaning |
|---|---|---|
| `courier_queued_bundles` | gauge | Bundles waiting for a link |
| `courier_bundles_delivered_total` | counter | Delivered to a local endpoint |
| `courier_bundles_forwarded_total` | counter | Handed to a peer |
| `courier_bundles_expired_total` | counter | Dropped when their lifetime ran out |
| `courier_bundles_rejected_total` | counter | Dropped because a peer rejected them as invalid |
| `courier_peer_reachable{peer}` | gauge | 1 unless the last forward to the peer failed |
| `courier_link_switched_off{peer}` | gauge | 1 while link-control has the link switched off |

## Other tasks

| Task | Command |
|---|---|
| Benchmarks | `cargo bench -p courier-bundle` |
| Fuzz the decoder | `cargo +nightly fuzz run decode_bundle` (from `fuzz/`'s parent) |
| NATS | `docker compose up -d` |
| Tests that need NATS | `docker compose up -d` then `cargo test --workspace -- --include-ignored` |
| Container image | `docker build --build-arg BIN=relay-node -t courier/relay-node:dev .` (or `BIN=link-control`) |
| Helm lint | `helm lint deploy/helm/courier` |
| Lint proto | `buf lint` |
| Docs | `mdbook serve docs` |

## Configuration

| Variable | Used by | Default |
|---|---|---|
| `COURIER_NODE_ID` | relay-node | `node-a` |
| `COURIER_LISTEN` | relay-node | `0.0.0.0:50051` |
| `COURIER_PEERS` | relay-node (`--peer`) | none; comma-separated `node=http://host:port` |
| `COURIER_ROUTES` | relay-node (`--route`) | none; comma-separated `node=peer`, `*=peer` for default |
| `COURIER_STORE_DIR` | relay-node | none (queue kept in memory) |
| `COURIER_FORWARD_INTERVAL_MS` | relay-node | `1000` |
| `COURIER_FORWARD_RETRIES` | relay-node | `2` |
| `COURIER_METRICS_LISTEN` | relay-node | `0.0.0.0:9464` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | relay-node | none (no traces); standard `OTEL_*` exporter variables also apply |
| `COURIER_LINK_LISTEN` | link-control | `0.0.0.0:8080` |
| `COURIER_NATS_URL` | relay-node, link-control | none (link state not shared) |
| `COURIER_TEST_NATS_URL` | NATS tests | `nats://127.0.0.1:4222` |
| `COURIER_NODE` | courier-cli | `http://127.0.0.1:50051` |
| `RUST_LOG` | all services | `info` |

## Roadmap

1. Bundle crate, CBOR, property tests, fuzzing, benchmarks
2. Store-and-forward between nodes
3. NATS events, link-control wired in, Tower retries, 3-node integration test
4. Docker, Helm on kind, Prometheus, Grafana, OpenTelemetry
5. Terraform on GKE/EKS, Argo CD, Linkerd
6. React dashboard, mdBook tutorial, demo GIF
