//! REST API for switching links between relay nodes on and off. With a NATS
//! bucket attached, every change is written there for the relay nodes to see.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::{Json, Router};
use courier_links::{LinkId, Store};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

pub struct Links {
    state: RwLock<BTreeMap<LinkId, bool>>,
    nats: Option<Store>,
}

impl Links {
    /// Link state kept in memory only.
    pub fn in_memory() -> Arc<Self> {
        Arc::new(Self {
            state: Default::default(),
            nats: None,
        })
    }

    /// Link state mirrored to NATS, starting from what the bucket already holds.
    pub async fn with_nats(store: Store) -> Result<Arc<Self>, courier_links::NatsError> {
        let state = courier_links::all(&store).await?.into_iter().collect();
        Ok(Arc::new(Self {
            state: RwLock::new(state),
            nats: Some(store),
        }))
    }
}

#[derive(Serialize, Deserialize)]
pub struct LinkUpdate {
    pub up: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct LinkState {
    pub link: String,
    pub up: bool,
}

pub fn app(links: Arc<Links>) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/links", get(list_links))
        .route("/links/:name", put(set_link))
        .with_state(links)
}

async fn list_links(State(links): State<Arc<Links>>) -> Json<BTreeMap<String, bool>> {
    let state = links.state.read().await;
    Json(state.iter().map(|(l, up)| (l.to_string(), *up)).collect())
}

async fn set_link(
    State(links): State<Arc<Links>>,
    Path(name): Path<String>,
    Json(update): Json<LinkUpdate>,
) -> Result<Json<LinkState>, (StatusCode, String)> {
    let link: LinkId = name
        .parse()
        .map_err(|e: courier_links::LinkError| (StatusCode::BAD_REQUEST, e.to_string()))?;
    if let Some(store) = &links.nats {
        courier_links::set(store, &link, update.up)
            .await
            .map_err(|e| {
                warn!(%link, error = %e, "failed to publish link state");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("failed to publish link state: {e}"),
                )
            })?;
    }
    info!(%link, up = update.up, "link changed");
    links.state.write().await.insert(link.clone(), update.up);
    Ok(Json(LinkState {
        link: link.to_string(),
        up: update.up,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tower::ServiceExt;

    async fn call(app: &Router, method: &str, uri: &str, body: &str) -> (StatusCode, String) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn stores_links_under_their_sorted_name() {
        let app = app(Links::in_memory());
        let (status, body) = call(&app, "PUT", "/links/node-b--node-a", r#"{"up":false}"#).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, r#"{"link":"node-a--node-b","up":false}"#);

        let (_, body) = call(&app, "GET", "/links", "").await;
        assert_eq!(body, r#"{"node-a--node-b":false}"#);
    }

    #[tokio::test]
    async fn rejects_bad_link_names() {
        let app = app(Links::in_memory());
        let (status, _) = call(&app, "PUT", "/links/node-a", r#"{"up":false}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
