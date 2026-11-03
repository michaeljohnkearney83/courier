use clap::{Parser, Subcommand};
use courier_proto::v1::relay_service_client::RelayServiceClient;
use courier_proto::v1::{NodeStatusRequest, SendBundleRequest, StreamReceivedRequest};

#[derive(Parser)]
#[command(name = "courier", about = "Talk to a Courier relay node")]
struct Cli {
    /// gRPC address of the relay node.
    #[arg(long, env = "COURIER_NODE", default_value = "http://127.0.0.1:50051")]
    node: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send a text message as a bundle.
    Send {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value_t = 60_000)]
        lifetime_ms: u64,
        message: String,
    },
    /// Print bundles delivered to an endpoint as they arrive.
    Listen { endpoint: String },
    /// Show the node's queue depth and counters.
    Status,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut client = RelayServiceClient::connect(cli.node).await?;

    match cli.command {
        Command::Send {
            from,
            to,
            lifetime_ms,
            message,
        } => {
            let resp = client
                .send_bundle(SendBundleRequest {
                    source: from,
                    destination: to,
                    lifetime_ms,
                    payload: message.into_bytes(),
                })
                .await?
                .into_inner();
            println!("sent {}", resp.bundle_id);
        }
        Command::Listen { endpoint } => {
            let mut stream = client
                .stream_received(StreamReceivedRequest { endpoint })
                .await?
                .into_inner();
            while let Some(b) = stream.message().await? {
                println!(
                    "[{}] from {}: {}",
                    b.bundle_id,
                    b.source,
                    String::from_utf8_lossy(&b.payload)
                );
            }
        }
        Command::Status => {
            let s = client.node_status(NodeStatusRequest {}).await?.into_inner();
            println!(
                "node {}: {} queued, {} delivered",
                s.node_id, s.queued_bundles, s.delivered_bundles
            );
        }
    }
    Ok(())
}
