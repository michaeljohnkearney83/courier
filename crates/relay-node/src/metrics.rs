//! Prometheus metrics for a relay node. Every series carries a `node` label.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::borrow::Cow;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PeerLabel {
    pub peer: String,
}

/// Per-peer link gauges. The router owns them; `Metrics` registers them.
#[derive(Clone, Default)]
pub struct LinkGauges {
    /// 1 unless the last forward to this peer failed.
    pub reachable: Family<PeerLabel, Gauge>,
    /// 1 while link-control has the link to this peer switched off.
    pub switched_off: Family<PeerLabel, Gauge>,
}

impl LinkGauges {
    pub fn set_reachable(&self, peer: &str, up: bool) {
        self.reachable.get_or_create(&label(peer)).set(up.into());
    }

    pub fn set_switched_off(&self, peer: &str, off: bool) {
        self.switched_off
            .get_or_create(&label(peer))
            .set(off.into());
    }
}

fn label(peer: &str) -> PeerLabel {
    PeerLabel {
        peer: peer.to_string(),
    }
}

pub struct Metrics {
    registry: Registry,
    pub delivered: Counter,
    pub forwarded: Counter,
    pub expired: Counter,
    pub rejected: Counter,
    /// Set from the queue length when metrics are scraped.
    pub queued: Gauge,
}

impl Metrics {
    pub fn new(node_id: &str, links: &LinkGauges) -> Self {
        let labels = [(Cow::Borrowed("node"), Cow::Owned(node_id.to_string()))];
        let mut registry = Registry::with_prefix_and_labels("courier", labels.into_iter());
        let mut counter = |name: &str, help: &str| {
            let counter = Counter::default();
            registry.register(name, help, counter.clone());
            counter
        };
        let delivered = counter("bundles_delivered", "Bundles delivered to a local endpoint");
        let forwarded = counter("bundles_forwarded", "Bundles handed to a peer");
        let expired = counter(
            "bundles_expired",
            "Bundles dropped because their lifetime ran out",
        );
        let rejected = counter(
            "bundles_rejected",
            "Bundles dropped because a peer rejected them as invalid",
        );

        let queued = Gauge::default();
        registry.register(
            "queued_bundles",
            "Bundles waiting for a link",
            queued.clone(),
        );
        registry.register(
            "peer_reachable",
            "1 unless the last forward to the peer failed",
            links.reachable.clone(),
        );
        registry.register(
            "link_switched_off",
            "1 while link-control has the link to the peer switched off",
            links.switched_off.clone(),
        );
        Self {
            registry,
            delivered,
            forwarded,
            expired,
            rejected,
            queued,
        }
    }

    /// The registry in the OpenMetrics text format.
    pub fn encode(&self) -> String {
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &self.registry)
            .expect("writing to a String cannot fail");
        text
    }
}
