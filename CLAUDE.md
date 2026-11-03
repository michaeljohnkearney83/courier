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
