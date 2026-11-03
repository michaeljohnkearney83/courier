//! Link state shared between link-control and the relay nodes.
//!
//! link-control writes each link's state to a NATS JetStream key-value
//! bucket and relay nodes watch it, so a node that starts late still sees
//! links that were switched off before it came up. A link with no entry is up.

use async_nats::jetstream::{self, kv};
use futures::{Stream, StreamExt};
use std::fmt;
use std::str::FromStr;

pub use async_nats::jetstream::kv::Store;
pub use async_nats::Error as NatsError;

/// Key-value bucket holding one `up` / `down` entry per link.
pub const BUCKET: &str = "courier-links";

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid link {0:?}: expected <node>--<node>, two different nodes made of letters, digits, '-' and '_'")]
pub struct LinkError(String);

/// An undirected link between two nodes, written `node-a--node-b`.
/// The ends are kept sorted, so `node-b--node-a` is the same link.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LinkId {
    a: String,
    b: String,
}

impl LinkId {
    pub fn new(x: &str, y: &str) -> Result<Self, LinkError> {
        let valid = |n: &str| {
            !n.is_empty()
                && !n.contains("--")
                && n.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        };
        if !valid(x) || !valid(y) || x == y {
            return Err(LinkError(format!("{x}--{y}")));
        }
        let (a, b) = if x < y { (x, y) } else { (y, x) };
        Ok(Self {
            a: a.to_string(),
            b: b.to_string(),
        })
    }

    /// The far end of the link from `node`, if `node` is on it.
    pub fn other(&self, node: &str) -> Option<&str> {
        if node == self.a {
            Some(&self.b)
        } else if node == self.b {
            Some(&self.a)
        } else {
            None
        }
    }
}

impl FromStr for LinkId {
    type Err = LinkError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (x, y) = s.split_once("--").ok_or_else(|| LinkError(s.to_string()))?;
        Self::new(x, y).map_err(|_| LinkError(s.to_string()))
    }
}

impl fmt::Display for LinkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}--{}", self.a, self.b)
    }
}

/// Connect to NATS and open the link bucket, creating it if needed.
pub async fn open(nats_url: &str) -> Result<Store, NatsError> {
    let client = async_nats::connect(nats_url).await?;
    let store = jetstream::new(client)
        .create_or_update_key_value(kv::Config {
            bucket: BUCKET.to_string(),
            history: 1,
            ..Default::default()
        })
        .await?;
    Ok(store)
}

pub async fn set(store: &Store, link: &LinkId, up: bool) -> Result<(), NatsError> {
    let value = if up { "up" } else { "down" };
    store.put(link.to_string(), value.into()).await?;
    Ok(())
}

/// The current state of every link that has an entry.
pub async fn all(store: &Store) -> Result<Vec<(LinkId, bool)>, NatsError> {
    let mut links = Vec::new();
    let mut keys = store.keys().await?;
    while let Some(key) = keys.next().await {
        let key = key?;
        if let (Ok(link), Some(value)) = (key.parse(), store.get(&key).await?) {
            links.push((link, value.as_ref() != b"down"));
        }
    }
    Ok(links)
}

/// The current state of every link, then each change as it happens.
/// Entries whose key is not a link name are skipped.
pub async fn watch(
    store: &Store,
) -> Result<impl Stream<Item = Result<(LinkId, bool), NatsError>>, NatsError> {
    let entries = store.watch_with_history(">").await?;
    Ok(entries.filter_map(|entry| {
        futures::future::ready(match entry {
            Err(e) => Some(Err(e.into())),
            Ok(entry) => entry.key.parse().ok().map(|link| {
                let up = entry.operation != kv::Operation::Put || entry.value.as_ref() != b"down";
                Ok((link, up))
            }),
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_ends_are_sorted() {
        let link: LinkId = "node-b--node-a".parse().unwrap();
        assert_eq!(link.to_string(), "node-a--node-b");
        assert_eq!(link, LinkId::new("node-a", "node-b").unwrap());
    }

    #[test]
    fn finds_the_far_end() {
        let link: LinkId = "node-a--node-b".parse().unwrap();
        assert_eq!(link.other("node-a"), Some("node-b"));
        assert_eq!(link.other("node-b"), Some("node-a"));
        assert_eq!(link.other("node-c"), None);
    }

    #[test]
    fn rejects_bad_names() {
        for bad in [
            "", "node-a", "node-a--", "--node-a", "a--a", "a--b--c", "a.b--c", "a--b c",
        ] {
            assert!(bad.parse::<LinkId>().is_err(), "{bad:?} should be rejected");
        }
    }
}
