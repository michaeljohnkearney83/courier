# Courier

Rust Cargo workspace for a small DTN relay network. See README.md for layout,
commands and configuration.

## Conventions

- Shared dependency versions live in `[workspace.dependencies]` in the root
  `Cargo.toml`; crates use `dep.workspace = true`.
- The API is defined in `proto/courier/v1/courier.proto`. `crates/proto`
  compiles it with protox (no protoc). Keep it passing `buf lint` (STANDARD):
  each RPC has its own `<Rpc>Request` / `<Rpc>Response` message.
- Endpoints are `dtn://<node>/<service>`; build them with `EndpointId::new`.
- Nodes hand bundles to each other with the `ForwardBundle` RPC, which carries
  the CBOR-encoded bundle, so id, creation time and lifetime survive each hop.
  Never rebuild a forwarded bundle with `Bundle::new`.
- relay-node routing is static (`--peer`, `--route`). A peer counts as down
  when the RPC fails; an `INVALID_ARGUMENT` reply drops the bundle instead of
  retrying it. A peer is also skipped while link-control has its link
  switched off.
- Link state lives in the NATS JetStream KV bucket `courier-links`, one
  `up`/`down` entry per link keyed `<node>--<node>` with the ends sorted. All
  access goes through `crates/links` (`courier-links`); build names with
  `LinkId`. A missing entry means the link is on.
- link-control is a lib (`app`, `Links`) plus a thin `main.rs`, so tests can
  run it in-process.
- Tests that need NATS are `#[ignore]`d with a reason. Run them with
  `docker compose up -d` and `cargo test --workspace -- --include-ignored`; CI
  does this. Use fresh node ids per run and purge the keys you create.
- Each peer's `ForwardBundle` call is a tower service with a retry layer
  (`crates/relay-node/src/retry.rs`). Only `UNAVAILABLE` is retried; other
  errors are final for that pass.
- relay-node metrics live in `crates/relay-node/src/metrics.rs`
  (prometheus-client, `courier_` prefix, `node` label on every series). The
  counters are the source of truth for `NodeStatus` too; don't add parallel
  atomics. The queue gauge is set when metrics are scraped.
- The Dockerfile caches `target/` with BuildKit mounts. Keep the `touch` before
  `cargo build` (COPY keeps old mtimes, so cargo would ship a stale binary) and
  keep `rust-toolchain.toml` in `.dockerignore` (otherwise parallel image builds
  race downloading clippy/rustfmt).
- `docker-compose.yml`: plain `up` starts NATS only (for tests); the `stack`
  profile adds the services. Grafana's dashboard is provisioned from
  `deploy/compose/grafana/dashboards/courier.json`; node colours are fixed per
  node, and link state uses text labels as well as colour.
- Helm chart (`deploy/helm/courier`): relay nodes are a StatefulSet whose pod
  names are the node ids. Every pod gets the same full-mesh `COURIER_PEERS`
  (relay-node skips its own entry). Pods run as uid 65532 with a read-only
  root filesystem; only `/var/lib/courier` (the PVC) is writable. Bump
  `version` in `Chart.yaml` when the chart changes.
- On kind with Docker Desktop, load images with `docker save --platform
  linux/amd64` + `kind load image-archive`; `kind load docker-image` fails.
- Tracing (`crates/relay-node/src/telemetry.rs`) is on only when
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set. A bundle's `trace_parent` field carries
  the W3C traceparent between hops: continue the trace from it when a node
  takes a bundle in or forwards it, then set it to the current span. Comparing
  bundles across hops in tests: use `untraced()`, since hops change it.
- Tests that check spans use the one global test subscriber
  (`exported_spans()`) and filter by bundle id. A thread-local subscriber
  misses spans when other tests run the same code in parallel.
- relay-node shuts down on Ctrl-C or SIGTERM and flushes traces on the way out.
- relay-node tests run real nodes in-process on `127.0.0.1:0`, with no quick
  retries, and call `forward_queued()` directly instead of waiting for the
  background loop.
- Services read config from CLI flags with `COURIER_*` env fallbacks.
- `fuzz/` is its own workspace and needs nightly.
- `courier-proto` allows `clippy::result_large_err` for the generated code.
- On Windows, a deep checkout path breaks linking (LNK1104); set
  `CARGO_TARGET_DIR` to a short folder.
- Before committing: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace -- --include-ignored` (with NATS up).
- When run/config/usage changes, update README.md and this file.
