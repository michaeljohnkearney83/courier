use clap::Parser;
use courier_bundle::{Bundle, EndpointId};
use courier_proto::v1::relay_service_server::{RelayService, RelayServiceServer};
use courier_proto::v1::{
    NodeStatusRequest, NodeStatusResponse, SendBundleRequest, SendBundleResponse,
    StreamReceivedRequest, StreamReceivedResponse,
};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Args {
    /// Name of this node; endpoints dtn://<node-id>/... are delivered locally.
    #[arg(long, env = "COURIER_NODE_ID", default_value = "node-a")]
    node_id: String,

    #[arg(long, env = "COURIER_LISTEN", default_value = "0.0.0.0:50051")]
    listen: SocketAddr,
}

struct Node {
    id: String,
    /// Bundles waiting for a link towards their destination.
    queue: Mutex<VecDeque<Bundle>>,
    delivered_count: AtomicU64,
    delivered: broadcast::Sender<Bundle>,
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

        if bundle.destination.node() == self.id {
            info!(%bundle_id, "delivering locally");
            self.delivered_count.fetch_add(1, Ordering::Relaxed);
            // No receivers is fine: nobody is listening on that endpoint yet.
            let _ = self.delivered.send(bundle);
        } else {
            // TODO(week 2): forward to the next hop when its link is up.
            info!(%bundle_id, next = bundle.destination.node(), "queued for forwarding");
            self.queue.lock().await.push_back(bundle);
        }

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
        }))
    }
}

fn invalid(e: courier_bundle::BundleError) -> Status {
    Status::invalid_argument(e.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    let (delivered, _) = broadcast::channel(1024);
    let node = Node {
        id: args.node_id.clone(),
        queue: Mutex::new(VecDeque::new()),
        delivered_count: AtomicU64::new(0),
        delivered,
    };

    info!(node_id = %args.node_id, listen = %args.listen, "relay node starting");
    Server::builder()
        .add_service(RelayServiceServer::new(node))
        .serve(args.listen)
        .await?;
    Ok(())
}
