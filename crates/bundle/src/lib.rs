//! A simplified take on the Bundle Protocol v7 (RFC 9171).
//!
//! A bundle is identified by its source endpoint, creation timestamp and
//! sequence number, and is dropped once its lifetime has elapsed.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SCHEME: &str = "dtn://";

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("invalid endpoint {0:?}: expected dtn://<node>/<service>")]
    InvalidEndpoint(String),
    #[error("failed to encode bundle: {0}")]
    Encode(String),
    #[error("failed to decode bundle: {0}")]
    Decode(String),
}

/// A DTN endpoint such as `dtn://node-a/inbox`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EndpointId(String);

impl EndpointId {
    pub fn new(value: impl Into<String>) -> Result<Self, BundleError> {
        let value = value.into();
        match value.strip_prefix(SCHEME) {
            Some(rest) if !rest.is_empty() && !rest.starts_with('/') => Ok(Self(value)),
            _ => Err(BundleError::InvalidEndpoint(value)),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The node part of the endpoint, e.g. `node-a` for `dtn://node-a/inbox`.
    pub fn node(&self) -> &str {
        let rest = &self.0[SCHEME.len()..];
        rest.split('/').next().unwrap_or(rest)
    }
}

impl fmt::Display for EndpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bundle {
    pub source: EndpointId,
    pub destination: EndpointId,
    pub created_at_ms: u64,
    pub sequence: u64,
    pub lifetime_ms: u64,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    /// W3C `traceparent` of the span that last handled the bundle, so a trace
    /// can follow it across hops. Not part of BPv7; omitted when empty, and
    /// bundles encoded without it still decode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_parent: Option<String>,
}

impl Bundle {
    pub fn new(
        source: EndpointId,
        destination: EndpointId,
        lifetime: Duration,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            source,
            destination,
            created_at_ms: now_ms(),
            sequence: SEQUENCE.fetch_add(1, Ordering::Relaxed),
            lifetime_ms: u64::try_from(lifetime.as_millis()).unwrap_or(u64::MAX),
            payload,
            trace_parent: None,
        }
    }

    pub fn id(&self) -> String {
        format!("{}#{}.{}", self.source, self.created_at_ms, self.sequence)
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.created_at_ms.saturating_add(self.lifetime_ms)
    }

    pub fn is_expired_at(&self, now_ms: u64) -> bool {
        now_ms >= self.expires_at_ms()
    }

    pub fn is_expired(&self) -> bool {
        self.is_expired_at(now_ms())
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, BundleError> {
        let mut buf = Vec::new();
        ciborium::into_writer(self, &mut buf).map_err(|e| BundleError::Encode(e.to_string()))?;
        Ok(buf)
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, BundleError> {
        let bundle: Self =
            ciborium::from_reader(bytes).map_err(|e| BundleError::Decode(e.to_string()))?;
        // Endpoints bypass `EndpointId::new` when deserialised, so validate them here.
        EndpointId::new(bundle.source.0.clone())?;
        EndpointId::new(bundle.destination.0.clone())?;
        Ok(bundle)
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn endpoint(s: &str) -> EndpointId {
        EndpointId::new(s).unwrap()
    }

    #[test]
    fn rejects_invalid_endpoints() {
        for bad in [
            "",
            "node-a/inbox",
            "dtn://",
            "dtn:///inbox",
            "http://node-a",
        ] {
            assert!(EndpointId::new(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn extracts_node_from_endpoint() {
        assert_eq!(endpoint("dtn://node-a/inbox").node(), "node-a");
        assert_eq!(endpoint("dtn://node-b").node(), "node-b");
    }

    #[test]
    fn expires_after_lifetime() {
        let b = Bundle::new(
            endpoint("dtn://a/x"),
            endpoint("dtn://b/y"),
            Duration::from_secs(10),
            vec![],
        );
        assert!(!b.is_expired_at(b.created_at_ms + 9_999));
        assert!(b.is_expired_at(b.created_at_ms + 10_000));
    }

    #[test]
    fn decodes_bundles_encoded_without_a_trace_parent() {
        #[derive(Serialize)]
        struct OldBundle<'a> {
            source: &'a str,
            destination: &'a str,
            created_at_ms: u64,
            sequence: u64,
            lifetime_ms: u64,
            #[serde(with = "serde_bytes")]
            payload: &'a [u8],
        }
        let mut old = Vec::new();
        let fields = OldBundle {
            source: "dtn://a/x",
            destination: "dtn://b/y",
            created_at_ms: 1,
            sequence: 2,
            lifetime_ms: 3,
            payload: b"hi",
        };
        ciborium::into_writer(&fields, &mut old).unwrap();
        let bundle = Bundle::from_cbor(&old).unwrap();
        assert_eq!(bundle.trace_parent, None);
        assert_eq!(bundle.payload, b"hi");
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(Bundle::from_cbor(&[0xff, 0x00, 0x13]).is_err());
    }

    proptest! {
        #[test]
        fn cbor_roundtrip(
            src in "[a-z]{1,8}",
            dst in "[a-z]{1,8}",
            lifetime_ms in any::<u64>(),
            payload in proptest::collection::vec(any::<u8>(), 0..512),
            trace_parent in proptest::option::of("[0-9a-f-]{55}"),
        ) {
            let b = Bundle {
                source: endpoint(&format!("dtn://{src}/svc")),
                destination: endpoint(&format!("dtn://{dst}/svc")),
                created_at_ms: 1,
                sequence: 2,
                lifetime_ms,
                payload,
                trace_parent,
            };
            let decoded = Bundle::from_cbor(&b.to_cbor().unwrap()).unwrap();
            prop_assert_eq!(decoded, b);
        }
    }
}
