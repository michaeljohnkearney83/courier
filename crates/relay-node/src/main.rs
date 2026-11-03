mod store;

use anyhow::Context;
use clap::Parser;
use courier_bundle::{now_ms, Bundle, EndpointId};
use courier_proto::v1::relay_service_client::RelayServiceClient;
use courier_proto::v1::relay_service_server::{RelayService, RelayServiceServer};
use courier_proto::v1::{
    ForwardBundleRequest, ForwardBundleResponse, NodeStatusRequest, NodeStatusResponse,
    SendBundleRequest, SendBundleResponse, StreamReceivedRequest, StreamReceivedResponse,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::sync::{broadcast, Mutex, Notify};
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Request, Response, Status};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

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
    peers: HashMap<String, RelayServiceClient<Channel>>,
    routes: HashMap<String, String>,
    /// Peers that failed last time, so a link going down is logged once.
    down: std::sync::Mutex<HashSet<String>>,
}

impl Router {
    fn new(peers: Vec<(String, String)>, routes: Vec<(String, String)>) -> anyhow::Result<Self> {
        let peers = peers
            .into_iter()
            .map(|(node, addr)| {
                // Lazy, so a node can start before its peers and reconnects on its own.
                let channel = Endpoint::from_shared(addr.clone())
                    .with_context(|| format!("invalid address for peer {node}: {addr:?}"))?
                    .connect_timeout(CONNECT_TIMEOUT)
                    .timeout(REQUEST_TIMEOUT)
                    .connect_lazy();
                Ok((node, RelayServiceClient::new(channel)))
            })
            .collect::<anyhow::Result<HashMap<_, _>>>()?;
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

    async fn forward(&self, hop: &str, bundle: &Bundle) -> Forward {
        let bytes = match bundle.to_cbor() {
            Ok(bytes) => bytes,
            Err(e) => return Forward::Rejected(e.to_string()),
        };
        let mut client = self.peers[hop].clone();
        let outcome = match client
            .forward_bundle(ForwardBundleRequest { bundle: bytes })
            .await
        {
            Ok(_) => Forward::Sent,
            // The peer is up but will never take this bundle, so retrying is pointless.
            Err(s) if s.code() == Code::InvalidArgument => Forward::Rejected(s.message().into()),
            Err(_) => Forward::LinkDown,
        };
        let link_down = matches!(outcome, Forward::LinkDown);
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
    /// Wakes the forwarder as soon as a bundle is queued.
    queued: Notify,
    delivered_count: AtomicU64,
    forwarded_count: AtomicU64,
    expired_count: AtomicU64,
    delivered: broadcast::Sender<Bundle>,
}

impl Node {
    fn new(id: String, router: Router, store: Option<Store>) -> Self {
        let (delivered, _) = broadcast::channel(1024);
        Self {
            id,
            router,
            store,
            queue: Mutex::new(VecDeque::new()),
            queued: Notify::new(),
            delivered_count: AtomicU64::new(0),
            forwarded_count: AtomicU64::new(0),
            expired_count: AtomicU64::new(0),
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
            self.delivered_count.fetch_add(1, Ordering::Relaxed);
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
        self.queued.notify_one();
        Ok(())
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
            self.expired_count.fetch_add(1, Ordering::Relaxed);
            self.unstore(bundle).await;
        }

        let mut done = HashSet::new();
        let mut unreachable = HashSet::new();
        for bundle in pending {
            let Some(hop) = self.router.next_hop(bundle.destination.node()) else {
                continue;
            };
            if unreachable.contains(hop) {
                continue;
            }
            let bundle_id = bundle.id();
            match self.router.forward(hop, &bundle).await {
                Forward::Sent => {
                    info!(%bundle_id, next_hop = hop, "forwarded");
                    self.forwarded_count.fetch_add(1, Ordering::Relaxed);
                }
                Forward::Rejected(error) => {
                    warn!(%bundle_id, next_hop = hop, %error, "rejected; dropping");
                }
                Forward::LinkDown => {
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
        let bundle = Bundle::new(
            source,
            destination,
            Duration::from_millis(req.lifetime_ms),
            req.payload,
        );
        let bundle_id = bundle.id();
        self.accept(bundle).await.map_err(store_failed)?;
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
            delivered_bundles: self.delivered_count.load(Ordering::Relaxed),
            forwarded_bundles: self.forwarded_count.load(Ordering::Relaxed),
            expired_bundles: self.expired_count.load(Ordering::Relaxed),
        }))
    }

    async fn forward_bundle(
        &self,
        request: Request<ForwardBundleRequest>,
    ) -> Result<Response<ForwardBundleResponse>, Status> {
        let bundle = Bundle::from_cbor(&request.into_inner().bundle).map_err(invalid)?;
        if bundle.is_expired() {
            info!(bundle_id = %bundle.id(), "received expired; dropping");
            self.expired_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.accept(bundle).await.map_err(store_failed)?;
        }
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
            _ = node.queued.notified() => {}
        }
        node.forward_queued().await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    let store = match &args.store_dir {
        Some(dir) => Some(
            Store::open(dir).with_context(|| format!("failed to open store {}", dir.display()))?,
        ),
        None => None,
    };
    let router = Router::new(args.peers, args.routes)?;
    let node = Arc::new(Node::new(args.node_id.clone(), router, store));
    node.restore().await?;
    tokio::spawn(run_forwarder(
        node.clone(),
        Duration::from_millis(args.forward_interval_ms),
    ));

    info!(node_id = %args.node_id, listen = %args.listen, "relay node starting");
    Server::builder()
        .add_service(RelayServiceServer::from_arc(node))
        .serve(args.listen)
        .await?;
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
            Router::new(peers, routes).unwrap(),
            None,
        ))
    }

    #[tokio::test]
    async fn picks_direct_peer_then_route_then_default() {
        let router = Router::new(
            vec![pair("b", "http://b:1"), pair("c", "http://c:1")],
            vec![pair("d", "c"), pair("*", "b")],
        )
        .unwrap();
        assert_eq!(router.next_hop("c"), Some("c"));
        assert_eq!(router.next_hop("d"), Some("c"));
        assert_eq!(router.next_hop("anything"), Some("b"));

        let no_default = Router::new(vec![pair("b", "http://b:1")], vec![]).unwrap();
        assert_eq!(no_default.next_hop("x"), None);
    }

    #[tokio::test]
    async fn rejects_route_through_unknown_peer() {
        assert!(Router::new(vec![], vec![pair("c", "b")]).is_err());
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
        assert_eq!(received, bundle);
        assert!(a.queue.lock().await.is_empty());
        assert!(b.queue.lock().await.is_empty());
        assert_eq!(a.forwarded_count.load(Ordering::Relaxed), 1);
        assert_eq!(b.forwarded_count.load(Ordering::Relaxed), 1);
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
        assert_eq!(b.delivered_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn drops_expired_bundles() {
        let a = node("node-a", vec![], vec![]);
        a.accept(bundle_to("dtn://node-b/inbox", Duration::ZERO))
            .await
            .unwrap();
        a.forward_queued().await;
        assert!(a.queue.lock().await.is_empty());
        assert_eq!(a.expired_count.load(Ordering::Relaxed), 1);
    }
}
