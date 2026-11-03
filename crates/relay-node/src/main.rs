mod metrics;
mod retry;
mod store;
mod telemetry;

use anyhow::Context;
use clap::Parser;
use courier_bundle::{now_ms, Bundle, EndpointId};
use courier_links::LinkId;
use courier_proto::v1::relay_service_client::RelayServiceClient;
use courier_proto::v1::relay_service_server::{RelayService, RelayServiceServer};
use courier_proto::v1::{
    ForwardBundleRequest, ForwardBundleResponse, NodeStatusRequest, NodeStatusResponse,
    SendBundleRequest, SendBundleResponse, StreamReceivedRequest, StreamReceivedResponse,
};
use metrics::{LinkGauges, Metrics};
use retry::Forwarder;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::sync::{broadcast, Mutex, Notify};
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::{Endpoint, Server};
use tonic::{Code, Request, Response, Status};
use tower::ServiceExt;
use tracing::{field, info, info_span, warn, Instrument};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Parser)]
struct Args {
    /// Name of this node; endpoints dtn://<node-id>/... are delivered locally.
    #[arg(long, env = "COURIER_NODE_ID", default_value = "node-a")]
    node_id: String,

    #[arg(long, env = "COURIER_LISTEN", default_value = "0.0.0.0:50051")]
    listen: SocketAddr,

    /// A neighbouring node and its gRPC address, e.g. node-b=http://127.0.0.1:50052.
    /// An entry for this node itself is ignored.
    #[arg(long = "peer", env = "COURIER_PEERS", value_delimiter = ',', value_parser = parse_pair)]
    peers: Vec<(String, String)>,

    /// Reach a node through a peer, e.g. node-c=node-b. `*=node-b` is the default route.
    #[arg(long = "route", env = "COURIER_ROUTES", value_delimiter = ',', value_parser = parse_pair)]
    routes: Vec<(String, String)>,

    /// Keep queued bundles in this directory so they survive a restart.
    #[arg(long, env = "COURIER_STORE_DIR")]
    store_dir: Option<PathBuf>,

    /// How often to retry forwarding bundles whose next hop was unreachable.
    #[arg(long, env = "COURIER_FORWARD_INTERVAL_MS", default_value_t = 1000,
          value_parser = clap::value_parser!(u64).range(1..))]
    forward_interval_ms: u64,

    /// Quick retries, with backoff from 100 ms, when a peer is briefly
    /// unavailable, before counting its link as down until the next pass.
    #[arg(long, env = "COURIER_FORWARD_RETRIES", default_value_t = 2)]
    forward_retries: u32,

    /// Address for the Prometheus /metrics endpoint.
    #[arg(long, env = "COURIER_METRICS_LISTEN", default_value = "0.0.0.0:9464")]
    metrics_listen: SocketAddr,

    /// NATS server holding link-control's link state, e.g. nats://127.0.0.1:4222.
    /// Without it, every link counts as switched on.
    #[arg(long, env = "COURIER_NATS_URL")]
    nats_url: Option<String>,
}

fn parse_pair(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.trim().is_empty() && !v.trim().is_empty() => {
            Ok((k.trim().to_string(), v.trim().to_string()))
        }
        _ => Err(format!("expected <name>=<value>, got {s:?}")),
    }
}

/// Static routing table: which peer gets a bundle for each destination node.
struct Router {
    peers: HashMap<String, Forwarder>,
    routes: HashMap<String, String>,
    /// Peers that failed last time, so a link going down is logged once.
    down: std::sync::Mutex<HashSet<String>>,
    /// Peers whose link link-control has switched off.
    switched_off: std::sync::Mutex<HashSet<String>>,
    gauges: LinkGauges,
}

impl Router {
    fn new(
        peers: Vec<(String, String)>,
        routes: Vec<(String, String)>,
        retries: u32,
    ) -> anyhow::Result<Self> {
        let peers = peers
            .into_iter()
            .map(|(node, addr)| {
                // Lazy, so a node can start before its peers and reconnects on its own.
                let channel = Endpoint::from_shared(addr.clone())
                    .with_context(|| format!("invalid address for peer {node}: {addr:?}"))?
                    .connect_timeout(CONNECT_TIMEOUT)
                    .timeout(REQUEST_TIMEOUT)
                    .connect_lazy();
                let client = RelayServiceClient::new(channel);
                Ok((node, retry::forwarder(client, retries)))
            })
            .collect::<anyhow::Result<HashMap<_, _>>>()?;
        let gauges = LinkGauges::default();
        for peer in peers.keys() {
            gauges.set_reachable(peer, true);
            gauges.set_switched_off(peer, false);
        }
        for (node, hop) in &routes {
            anyhow::ensure!(
                peers.contains_key(hop),
                "route {node}={hop}: {hop} is not a --peer"
            );
        }
        Ok(Self {
            peers,
            routes: routes.into_iter().collect(),
            down: Default::default(),
            switched_off: Default::default(),
            gauges,
        })
    }

    /// The peer for bundles to `node`: the node itself if it is a peer, then
    /// an explicit route, then the default route.
    fn next_hop(&self, node: &str) -> Option<&str> {
        if let Some((peer, _)) = self.peers.get_key_value(node) {
            return Some(peer);
        }
        self.routes
            .get(node)
            .or_else(|| self.routes.get("*"))
            .map(String::as_str)
    }

    fn is_switched_off(&self, peer: &str) -> bool {
        self.switched_off.lock().unwrap().contains(peer)
    }

    /// Record link-control's state for the link to `peer`. Returns whether it changed.
    fn switch(&self, peer: &str, up: bool) -> bool {
        let mut off = self.switched_off.lock().unwrap();
        self.gauges.set_switched_off(peer, !up);
        if up {
            off.remove(peer)
        } else {
            off.insert(peer.to_string())
        }
    }

    async fn forward(&self, hop: &str, bundle: &Bundle) -> Forward {
        let bytes = match bundle.to_cbor() {
            Ok(bytes) => bytes,
            Err(e) => return Forward::Rejected(e.to_string()),
        };
        let request = ForwardBundleRequest { bundle: bytes };
        let outcome = match self.peers[hop].clone().oneshot(request).await {
            Ok(_) => Forward::Sent,
            // The peer is up but will never take this bundle, so retrying is pointless.
            Err(s) if s.code() == Code::InvalidArgument => Forward::Rejected(s.message().into()),
            Err(_) => Forward::LinkDown,
        };
        let link_down = matches!(outcome, Forward::LinkDown);
        self.gauges.set_reachable(hop, !link_down);
        let mut down = self.down.lock().unwrap();
        if link_down && down.insert(hop.to_string()) {
            warn!(peer = hop, "link down; holding bundles until it is back");
        } else if !link_down && down.remove(hop) {
            info!(peer = hop, "link up");
        }
        outcome
    }
}

enum Forward {
    Sent,
    Rejected(String),
    LinkDown,
}

struct Node {
    id: String,
    router: Router,
    store: Option<Store>,
    /// Bundles waiting for a link towards their destination.
    queue: Mutex<VecDeque<Bundle>>,
    /// Wakes the forwarder when a bundle is queued or a link is switched on.
    wake: Notify,
    metrics: Metrics,
    delivered: broadcast::Sender<Bundle>,
}

impl Node {
    fn new(id: String, router: Router, store: Option<Store>) -> Self {
        let (delivered, _) = broadcast::channel(1024);
        Self {
            metrics: Metrics::new(&id, &router.gauges),
            id,
            router,
            store,
            queue: Mutex::new(VecDeque::new()),
            wake: Notify::new(),
            delivered,
        }
    }

    /// Reload bundles queued before the last shutdown.
    async fn restore(&self) -> anyhow::Result<()> {
        if let Some(store) = &self.store {
            let bundles = store.load().context("failed to load stored bundles")?;
            info!(count = bundles.len(), "restored queued bundles");
            self.queue.lock().await.extend(bundles);
        }
        Ok(())
    }

    /// Deliver a bundle to a local endpoint, or queue it for its next hop.
    async fn accept(&self, bundle: Bundle) -> std::io::Result<()> {
        let bundle_id = bundle.id();
        if bundle.destination.node() == self.id {
            info!(%bundle_id, "delivering locally");
            self.metrics.delivered.inc();
            // No receivers is fine: nobody is listening on that endpoint yet.
            let _ = self.delivered.send(bundle);
            return Ok(());
        }

        match self.router.next_hop(bundle.destination.node()) {
            Some(hop) => info!(%bundle_id, next_hop = hop, "queued for forwarding"),
            None => {
                warn!(%bundle_id, node = bundle.destination.node(), "no route; holding until it expires")
            }
        }
        if let Some(store) = &self.store {
            store.save(&bundle).await?;
        }
        self.queue.lock().await.push_back(bundle);
        self.wake.notify_one();
        Ok(())
    }

    /// Apply a link change from link-control. Only links to a peer matter.
    fn set_link(&self, link: &LinkId, up: bool) {
        let Some(peer) = link.other(&self.id) else {
            return;
        };
        if !self.router.peers.contains_key(peer) || !self.router.switch(peer, up) {
            return;
        }
        if up {
            info!(peer, "link switched on");
            self.wake.notify_one();
        } else {
            info!(peer, "link switched off; holding bundles for it");
        }
    }

    /// One pass over the queue: drop expired bundles and hand the rest to their
    /// next hop. Bundles whose hop is unreachable stay queued for the next pass.
    async fn forward_queued(&self) {
        let now = now_ms();
        let (expired, pending): (VecDeque<_>, Vec<_>) = {
            let mut queue = self.queue.lock().await;
            let (expired, live) = std::mem::take(&mut *queue)
                .into_iter()
                .partition(|b: &Bundle| b.is_expired_at(now));
            *queue = live;
            (expired, queue.iter().cloned().collect())
        };

        for bundle in &expired {
            info!(bundle_id = %bundle.id(), "expired; dropping");
            self.metrics.expired.inc();
            self.unstore(bundle).await;
        }

        let mut done = HashSet::new();
        let mut unreachable = HashSet::new();
        for mut bundle in pending {
            let Some(hop) = self.router.next_hop(bundle.destination.node()) else {
                continue;
            };
            if unreachable.contains(hop) || self.router.is_switched_off(hop) {
                continue;
            }
            let bundle_id = bundle.id();
            let span = info_span!(
                "forward_bundle",
                %bundle_id,
                next_hop = hop,
                outcome = field::Empty,
                otel.status_code = field::Empty,
            );
            telemetry::continue_trace(&span, bundle.trace_parent.as_deref());
            let outcome = async {
                // The next hop's span becomes a child of this one.
                bundle.trace_parent =
                    telemetry::current_trace_parent().or(bundle.trace_parent.take());
                self.router.forward(hop, &bundle).await
            }
            .instrument(span.clone())
            .await;
            match outcome {
                Forward::Sent => {
                    span.record("outcome", "sent");
                    info!(%bundle_id, next_hop = hop, "forwarded");
                    self.metrics.forwarded.inc();
                }
                Forward::Rejected(error) => {
                    span.record("outcome", "rejected");
                    span.record("otel.status_code", "ERROR");
                    self.metrics.rejected.inc();
                    warn!(%bundle_id, next_hop = hop, %error, "rejected; dropping");
                }
                Forward::LinkDown => {
                    span.record("outcome", "link down");
                    span.record("otel.status_code", "ERROR");
                    unreachable.insert(hop);
                    continue;
                }
            }
            self.unstore(&bundle).await;
            done.insert(bundle_id);
        }

        if !done.is_empty() {
            self.queue.lock().await.retain(|b| !done.contains(&b.id()));
        }
    }

    async fn render_metrics(&self) -> String {
        self.metrics
            .queued
            .set(self.queue.lock().await.len() as i64);
        self.metrics.encode()
    }

    async fn unstore(&self, bundle: &Bundle) {
        if let Some(store) = &self.store {
            store.remove(bundle).await;
        }
    }
}

type ReceivedStream = Pin<Box<dyn Stream<Item = Result<StreamReceivedResponse, Status>> + Send>>;

#[tonic::async_trait]
impl RelayService for Node {
    async fn send_bundle(
        &self,
        request: Request<SendBundleRequest>,
    ) -> Result<Response<SendBundleResponse>, Status> {
        let req = request.into_inner();
        let source = EndpointId::new(req.source).map_err(invalid)?;
        let destination = EndpointId::new(req.destination).map_err(invalid)?;
        let mut bundle = Bundle::new(
            source,
            destination,
            Duration::from_millis(req.lifetime_ms),
            req.payload,
        );
        let bundle_id = bundle.id();
        let span = info_span!("send_bundle", %bundle_id, destination = %bundle.destination);
        async {
            bundle.trace_parent = telemetry::current_trace_parent();
            self.accept(bundle).await
        }
        .instrument(span)
        .await
        .map_err(store_failed)?;
        Ok(Response::new(SendBundleResponse { bundle_id }))
    }

    type StreamReceivedStream = ReceivedStream;

    async fn stream_received(
        &self,
        request: Request<StreamReceivedRequest>,
    ) -> Result<Response<Self::StreamReceivedStream>, Status> {
        let endpoint = request.into_inner().endpoint;
        let stream =
            BroadcastStream::new(self.delivered.subscribe()).filter_map(move |item| match item {
                Ok(b) if b.destination.as_str() == endpoint => Some(Ok(StreamReceivedResponse {
                    bundle_id: b.id(),
                    source: b.source.to_string(),
                    payload: b.payload,
                    created_at_ms: b.created_at_ms,
                })),
                _ => None,
            });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn node_status(
        &self,
        _request: Request<NodeStatusRequest>,
    ) -> Result<Response<NodeStatusResponse>, Status> {
        Ok(Response::new(NodeStatusResponse {
            node_id: self.id.clone(),
            queued_bundles: self.queue.lock().await.len() as u64,
            delivered_bundles: self.metrics.delivered.get(),
            forwarded_bundles: self.metrics.forwarded.get(),
            expired_bundles: self.metrics.expired.get(),
        }))
    }

    async fn forward_bundle(
        &self,
        request: Request<ForwardBundleRequest>,
    ) -> Result<Response<ForwardBundleResponse>, Status> {
        let mut bundle = Bundle::from_cbor(&request.into_inner().bundle).map_err(invalid)?;
        let span = info_span!("receive_bundle", bundle_id = %bundle.id(), node = %self.id);
        telemetry::continue_trace(&span, bundle.trace_parent.as_deref());
        async {
            if bundle.is_expired() {
                info!(bundle_id = %bundle.id(), "received expired; dropping");
                self.metrics.expired.inc();
                return Ok(());
            }
            bundle.trace_parent = telemetry::current_trace_parent().or(bundle.trace_parent.take());
            self.accept(bundle).await
        }
        .instrument(span)
        .await
        .map_err(store_failed)?;
        Ok(Response::new(ForwardBundleResponse {}))
    }
}

fn invalid(e: courier_bundle::BundleError) -> Status {
    Status::invalid_argument(e.to_string())
}

fn store_failed(e: std::io::Error) -> Status {
    Status::internal(format!("failed to store bundle: {e}"))
}

/// Forward whenever a bundle is queued, and retry on a timer for links that
/// were down.
async fn run_forwarder(node: Arc<Node>, interval: Duration) {
    let mut retry = tokio::time::interval(interval);
    retry.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = retry.tick() => {}
            _ = node.wake.notified() => {}
        }
        node.forward_queued().await;
    }
}

/// Follow link-control's link state in NATS, reconnecting if NATS goes away.
async fn follow_links(node: Arc<Node>, nats_url: String) {
    loop {
        match watch_links(&node, &nats_url).await {
            Ok(()) => warn!("link state watch ended; reconnecting"),
            Err(e) => warn!(error = %e, "cannot follow link state in NATS; retrying"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn watch_links(node: &Node, nats_url: &str) -> Result<(), courier_links::NatsError> {
    let store = courier_links::open(nats_url).await?;
    let mut changes = courier_links::watch(&store).await?;
    info!(nats_url, "following link state");
    while let Some(change) = changes.next().await {
        let (link, up) = change?;
        node.set_link(&link, up);
    }
    Ok(())
}

async fn serve_metrics(listener: tokio::net::TcpListener, node: Arc<Node>) {
    use axum::http::header::CONTENT_TYPE;
    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || async move {
            let content_type = "application/openmetrics-text; version=1.0.0; charset=utf-8";
            ([(CONTENT_TYPE, content_type)], node.render_metrics().await)
        }),
    );
    if let Err(e) = axum::serve(listener, app).await {
        warn!(error = %e, "metrics server stopped");
    }
}

/// Ctrl-C, or SIGTERM from Docker and Kubernetes.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
    info!("shutting down");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let tracer = telemetry::init(&args.node_id)?;
    let store = match &args.store_dir {
        Some(dir) => Some(
            Store::open(dir).with_context(|| format!("failed to open store {}", dir.display()))?,
        ),
        None => None,
    };
    // Skipping ourselves lets every node share one peer list, e.g. a Helm StatefulSet.
    let peers = args
        .peers
        .into_iter()
        .filter(|(node, _)| *node != args.node_id)
        .collect();
    let router = Router::new(peers, args.routes, args.forward_retries)?;
    let node = Arc::new(Node::new(args.node_id.clone(), router, store));
    node.restore().await?;
    tokio::spawn(run_forwarder(
        node.clone(),
        Duration::from_millis(args.forward_interval_ms),
    ));
    if let Some(url) = args.nats_url {
        tokio::spawn(follow_links(node.clone(), url));
    }

    let metrics = tokio::net::TcpListener::bind(args.metrics_listen)
        .await
        .with_context(|| format!("failed to bind metrics on {}", args.metrics_listen))?;
    tokio::spawn(serve_metrics(metrics, node.clone()));

    info!(node_id = %args.node_id, listen = %args.listen, metrics = %args.metrics_listen, "relay node starting");
    Server::builder()
        .add_service(RelayServiceServer::from_arc(node))
        .serve_with_shutdown(args.listen, shutdown_signal())
        .await?;
    // Send any spans still buffered.
    if let Some(tracer) = tracer {
        if let Err(e) = tracer.shutdown() {
            warn!(error = %e, "failed to flush traces");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    fn pair(a: &str, b: &str) -> (String, String) {
        (a.to_string(), b.to_string())
    }

    fn bundle_to(destination: &str, lifetime: Duration) -> Bundle {
        Bundle::new(
            EndpointId::new("dtn://test/cli").unwrap(),
            EndpointId::new(destination).unwrap(),
            lifetime,
            b"hello".to_vec(),
        )
    }

    /// Each hop updates `trace_parent`; everything else must arrive unchanged.
    fn untraced(bundle: &Bundle) -> Bundle {
        Bundle {
            trace_parent: None,
            ..bundle.clone()
        }
    }

    async fn listener() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        (listener, addr)
    }

    fn serve(listener: TcpListener, node: &Arc<Node>) {
        tokio::spawn(
            Server::builder()
                .add_service(RelayServiceServer::from_arc(node.clone()))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
    }

    fn node(id: &str, peers: Vec<(String, String)>, routes: Vec<(String, String)>) -> Arc<Node> {
        Arc::new(Node::new(
            id.to_string(),
            // No quick retries, so tests with a down link stay fast.
            Router::new(peers, routes, 0).unwrap(),
            None,
        ))
    }

    #[tokio::test]
    async fn picks_direct_peer_then_route_then_default() {
        let router = Router::new(
            vec![pair("b", "http://b:1"), pair("c", "http://c:1")],
            vec![pair("d", "c"), pair("*", "b")],
            0,
        )
        .unwrap();
        assert_eq!(router.next_hop("c"), Some("c"));
        assert_eq!(router.next_hop("d"), Some("c"));
        assert_eq!(router.next_hop("anything"), Some("b"));

        let no_default = Router::new(vec![pair("b", "http://b:1")], vec![], 0).unwrap();
        assert_eq!(no_default.next_hop("x"), None);
    }

    #[tokio::test]
    async fn rejects_route_through_unknown_peer() {
        assert!(Router::new(vec![], vec![pair("c", "b")], 0).is_err());
    }

    #[test]
    fn parses_pairs() {
        assert_eq!(parse_pair("b=http://x:1"), Ok(pair("b", "http://x:1")));
        assert!(parse_pair("b").is_err());
        assert!(parse_pair("=x").is_err());
    }

    #[tokio::test]
    async fn forwards_over_two_hops_keeping_the_bundle_id() {
        let (listen_b, addr_b) = listener().await;
        let (listen_c, addr_c) = listener().await;
        let a = node(
            "node-a",
            vec![pair("node-b", &addr_b)],
            vec![pair("node-c", "node-b")],
        );
        let b = node("node-b", vec![pair("node-c", &addr_c)], vec![]);
        let c = node("node-c", vec![], vec![]);
        serve(listen_b, &b);
        serve(listen_c, &c);
        let mut inbox = c.delivered.subscribe();

        let bundle = bundle_to("dtn://node-c/inbox", Duration::from_secs(60));
        a.accept(bundle.clone()).await.unwrap();
        a.forward_queued().await;
        b.forward_queued().await;

        let received = tokio::time::timeout(Duration::from_secs(5), inbox.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(untraced(&received), untraced(&bundle));
        assert!(a.queue.lock().await.is_empty());
        assert!(b.queue.lock().await.is_empty());
        assert_eq!(a.metrics.forwarded.get(), 1);
        assert_eq!(b.metrics.forwarded.get(), 1);
    }

    #[tokio::test]
    async fn holds_bundles_until_the_link_comes_up() {
        // Reserve a port for node-b, then leave it closed so the link is down.
        let (listen_b, addr_b) = listener().await;
        let bind_b = listen_b.local_addr().unwrap();
        drop(listen_b);
        let a = node("node-a", vec![pair("node-b", &addr_b)], vec![]);

        a.accept(bundle_to("dtn://node-b/inbox", Duration::from_secs(60)))
            .await
            .unwrap();
        a.forward_queued().await;
        assert_eq!(a.queue.lock().await.len(), 1);

        let b = node("node-b", vec![], vec![]);
        serve(TcpListener::bind(bind_b).await.unwrap(), &b);
        a.forward_queued().await;
        assert!(a.queue.lock().await.is_empty());
        assert_eq!(b.metrics.delivered.get(), 1);
    }

    #[tokio::test]
    async fn only_links_to_peers_are_switched() {
        let a = node("node-a", vec![pair("node-b", "http://b:1")], vec![]);
        a.set_link(&"node-a--node-b".parse().unwrap(), false);
        a.set_link(&"node-a--node-c".parse().unwrap(), false);
        a.set_link(&"node-b--node-c".parse().unwrap(), false);
        assert!(a.router.is_switched_off("node-b"));
        assert_eq!(a.router.switched_off.lock().unwrap().len(), 1);

        a.set_link(&"node-b--node-a".parse().unwrap(), true);
        assert!(!a.router.is_switched_off("node-b"));
    }

    async fn eventually(what: &str, check: impl AsyncFn() -> bool) {
        for _ in 0..100 {
            if check().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {what}");
    }

    async fn switch_link(link_control: &axum::Router, x: &str, y: &str, up: bool) {
        let request = axum::http::Request::put(format!("/links/{x}--{y}"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from(format!(r#"{{"up":{up}}}"#)))
            .unwrap();
        let response = link_control.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    /// a -> b -> c with link-control and NATS in the loop.
    #[tokio::test]
    #[ignore = "needs NATS with JetStream: docker compose up -d"]
    async fn three_nodes_hold_bundles_while_link_control_has_a_link_off() {
        let nats = std::env::var("COURIER_TEST_NATS_URL")
            .unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
        let store = courier_links::open(&nats)
            .await
            .expect("NATS is not running");
        let link_control =
            link_control::app(link_control::Links::with_nats(store.clone()).await.unwrap());

        // Fresh node ids per run keep their links apart in the shared bucket.
        let run = now_ms();
        let [id_a, id_b, id_c] = ["a", "b", "c"].map(|n| format!("node-{n}-{run}"));
        let (listen_b, addr_b) = listener().await;
        let (listen_c, addr_c) = listener().await;
        let a = node(&id_a, vec![pair(&id_b, &addr_b)], vec![pair(&id_c, &id_b)]);
        let b = node(&id_b, vec![pair(&id_c, &addr_c)], vec![]);
        let c = node(&id_c, vec![], vec![]);
        serve(listen_b, &b);
        serve(listen_c, &c);
        for n in [&a, &b, &c] {
            tokio::spawn(follow_links(n.clone(), nats.clone()));
            tokio::spawn(run_forwarder(n.clone(), Duration::from_millis(50)));
        }
        let mut inbox = c.delivered.subscribe();

        switch_link(&link_control, &id_b, &id_c, false).await;
        eventually("b to see b--c off", async || {
            b.router.is_switched_off(&id_c)
        })
        .await;

        let bundle = bundle_to(&format!("dtn://{id_c}/inbox"), Duration::from_secs(60));
        a.accept(bundle.clone()).await.unwrap();
        eventually("b to hold the bundle", async || {
            b.queue.lock().await.len() == 1
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(b.metrics.forwarded.get(), 0);
        assert_eq!(c.metrics.delivered.get(), 0);

        switch_link(&link_control, &id_b, &id_c, true).await;
        let received = tokio::time::timeout(Duration::from_secs(5), inbox.recv())
            .await
            .expect("bundle not delivered after the link came back")
            .unwrap();
        assert_eq!(untraced(&received), untraced(&bundle));
        // c can deliver before b's forward call returns and b dequeues it.
        eventually("b to dequeue", async || b.queue.lock().await.is_empty()).await;
        store.purge(format!("{id_b}--{id_c}")).await.unwrap();
    }

    #[tokio::test]
    async fn reports_metrics_per_node_and_peer() {
        let a = node("node-a", vec![pair("node-b", "http://b:1")], vec![]);
        a.accept(bundle_to("dtn://node-a/inbox", Duration::from_secs(60)))
            .await
            .unwrap();
        a.accept(bundle_to("dtn://node-b/inbox", Duration::from_secs(60)))
            .await
            .unwrap();
        a.set_link(&"node-a--node-b".parse().unwrap(), false);

        let text = a.render_metrics().await;
        for line in [
            r#"courier_bundles_delivered_total{node="node-a"} 1"#,
            r#"courier_queued_bundles{node="node-a"} 1"#,
            r#"courier_link_switched_off{node="node-a",peer="node-b"} 1"#,
            r#"courier_peer_reachable{node="node-a",peer="node-b"} 1"#,
        ] {
            assert!(
                text.contains(line),
                "missing {line:?} in:
{text}"
            );
        }
    }

    /// Spans from the whole test binary. The subscriber is global because a
    /// thread-local one misses span sites that other tests are using on
    /// other threads at the same time; tests pick out their own trace.
    fn exported_spans() -> &'static opentelemetry_sdk::trace::InMemorySpanExporter {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use std::sync::OnceLock;
        use tracing_subscriber::filter::Targets;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::Layer;

        static TRACING: OnceLock<(SdkTracerProvider, InMemorySpanExporter)> = OnceLock::new();
        let (_, exporter) = TRACING.get_or_init(|| {
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(exporter.clone())
                .build();
            let layer = tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("test"))
                .with_filter(Targets::new().with_target("relay_node", tracing::Level::INFO));
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
                .expect("no other global subscriber in tests");
            (provider, exporter)
        });
        exporter
    }

    #[tokio::test]
    async fn one_trace_follows_a_bundle_across_hops() {
        use opentelemetry_sdk::trace::SpanData;
        let exporter = exported_spans();

        let (listen_b, addr_b) = listener().await;
        let (listen_c, addr_c) = listener().await;
        let a = node(
            "node-a",
            vec![pair("node-b", &addr_b)],
            vec![pair("node-c", "node-b")],
        );
        let b = node("node-b", vec![pair("node-c", &addr_c)], vec![]);
        let c = node("node-c", vec![], vec![]);
        serve(listen_b, &b);
        serve(listen_c, &c);
        let mut inbox = c.delivered.subscribe();

        let bundle_id = a
            .send_bundle(Request::new(SendBundleRequest {
                source: "dtn://test/cli".into(),
                destination: "dtn://node-c/inbox".into(),
                lifetime_ms: 60_000,
                payload: b"traced".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner()
            .bundle_id;
        a.forward_queued().await;
        b.forward_queued().await;
        tokio::time::timeout(Duration::from_secs(5), inbox.recv())
            .await
            .unwrap()
            .unwrap();

        let ours = |s: &SpanData| {
            s.attributes
                .iter()
                .any(|kv| kv.key.as_str() == "bundle_id" && kv.value.as_str() == bundle_id)
        };
        let spans: Vec<_> = exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(ours)
            .collect();
        let find = |name: &str, key: &str, value: &str| -> SpanData {
            spans
                .iter()
                .find(|s| {
                    s.name == name
                        && s.attributes
                            .iter()
                            .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
                })
                .unwrap_or_else(|| panic!("no {name} span with {key}={value}"))
                .clone()
        };
        let send = find("send_bundle", "destination", "dtn://node-c/inbox");
        let chain = [
            send.clone(),
            find("forward_bundle", "next_hop", "node-b"),
            find("receive_bundle", "node", "node-b"),
            find("forward_bundle", "next_hop", "node-c"),
            find("receive_bundle", "node", "node-c"),
        ];
        for pair in chain.windows(2) {
            let (parent, child) = (&pair[0], &pair[1]);
            assert_eq!(child.span_context.trace_id(), send.span_context.trace_id());
            assert_eq!(
                child.parent_span_id,
                parent.span_context.span_id(),
                "{} should be the parent of {}",
                parent.name,
                child.name
            );
        }
    }

    #[tokio::test]
    async fn drops_expired_bundles() {
        let a = node("node-a", vec![], vec![]);
        a.accept(bundle_to("dtn://node-b/inbox", Duration::ZERO))
            .await
            .unwrap();
        a.forward_queued().await;
        assert!(a.queue.lock().await.is_empty());
        assert_eq!(a.metrics.expired.get(), 1);
    }
}
