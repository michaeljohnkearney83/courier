use axum::extract::{Path, State};
use axum::routing::{get, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Link name (e.g. "node-a--node-b") to whether it is up.
type Links = Arc<RwLock<HashMap<String, bool>>>;

#[derive(Clone, Serialize, Deserialize)]
struct LinkUpdate {
    up: bool,
}

async fn list_links(State(links): State<Links>) -> Json<HashMap<String, bool>> {
    Json(links.read().await.clone())
}

async fn set_link(
    State(links): State<Links>,
    Path(name): Path<String>,
    Json(update): Json<LinkUpdate>,
) -> Json<LinkUpdate> {
    info!(link = %name, up = update.up, "link changed");
    // TODO(week 3): publish link.up / link.down to NATS.
    links.write().await.insert(name, update.up);
    Json(update)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let listen = std::env::var("COURIER_LINK_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/links", get(list_links))
        .route("/links/:name", put(set_link))
        .with_state(Links::default());

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    info!(%listen, "link-control starting");
    axum::serve(listener, app).await?;
    Ok(())
}
