//! Optional on-disk copy of the forwarding queue, so queued bundles survive a
//! restart. Each bundle is one CBOR file named after its id.

use courier_bundle::Bundle;
use std::io;
use std::path::PathBuf;
use tracing::warn;

pub struct Store {
    dir: PathBuf,
}

impl Store {
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    /// Every bundle in the store, oldest first. Unreadable files are skipped.
    pub fn load(&self) -> io::Result<Vec<Bundle>> {
        let mut bundles = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if path.extension().is_none_or(|ext| ext != "cbor") {
                continue;
            }
            match std::fs::read(&path)
                .map_err(|e| e.to_string())
                .and_then(|bytes| Bundle::from_cbor(&bytes).map_err(|e| e.to_string()))
            {
                Ok(bundle) => bundles.push(bundle),
                Err(e) => warn!(path = %path.display(), error = %e, "skipping stored bundle"),
            }
        }
        bundles.sort_by_key(|b| (b.created_at_ms, b.sequence));
        Ok(bundles)
    }

    pub async fn save(&self, bundle: &Bundle) -> io::Result<()> {
        let bytes = bundle
            .to_cbor()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        // Write then rename, so a crash never leaves a half-written bundle.
        let path = self.path(bundle);
        let tmp = path.with_extension("tmp");
        tokio::fs::write(&tmp, bytes).await?;
        tokio::fs::rename(&tmp, &path).await
    }

    pub async fn remove(&self, bundle: &Bundle) {
        let path = self.path(bundle);
        if let Err(e) = tokio::fs::remove_file(&path).await {
            if e.kind() != io::ErrorKind::NotFound {
                warn!(path = %path.display(), error = %e, "failed to remove stored bundle");
            }
        }
    }

    /// Bundle ids contain `/`, `:` and `#`, so the file name is the id in hex.
    fn path(&self, bundle: &Bundle) -> PathBuf {
        let name: String = bundle.id().bytes().map(|b| format!("{b:02x}")).collect();
        self.dir.join(format!("{name}.cbor"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use courier_bundle::EndpointId;
    use std::time::Duration;

    fn bundle(n: u8) -> Bundle {
        Bundle::new(
            EndpointId::new("dtn://node-a/cli").unwrap(),
            EndpointId::new("dtn://node-b/inbox").unwrap(),
            Duration::from_secs(60),
            vec![n],
        )
    }

    #[tokio::test]
    async fn saves_loads_and_removes_bundles() {
        let dir = std::env::temp_dir().join(format!(
            "courier-store-test-{}-{}",
            std::process::id(),
            courier_bundle::now_ms()
        ));
        let store = Store::open(&dir).unwrap();
        let (first, second) = (bundle(1), bundle(2));
        store.save(&second).await.unwrap();
        store.save(&first).await.unwrap();
        assert_eq!(store.load().unwrap(), vec![first.clone(), second.clone()]);

        store.remove(&first).await;
        assert_eq!(store.load().unwrap(), vec![second]);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
