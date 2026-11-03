use anyhow::Context;
use clap::Parser;
use link_control::{app, Links};
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "COURIER_LINK_LISTEN", default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    /// NATS server to publish link state to, e.g. nats://127.0.0.1:4222.
    /// Without it, link state is kept in memory and relay nodes never see it.
    #[arg(long, env = "COURIER_NATS_URL")]
    nats_url: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    let links = match &args.nats_url {
        Some(url) => {
            let store = courier_links::open(url)
                .await
                .map_err(|e| anyhow::anyhow!(e))
                .with_context(|| format!("failed to open link state in NATS at {url}"))?;
            info!(%url, "publishing link state to NATS");
            Links::with_nats(store)
                .await
                .map_err(|e| anyhow::anyhow!(e))
                .context("failed to read link state from NATS")?
        }
        None => Links::in_memory(),
    };

    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    info!(listen = %args.listen, "link-control starting");
    axum::serve(listener, app(links)).await?;
    Ok(())
}
