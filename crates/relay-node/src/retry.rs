//! Quick retries for forwarding to a peer, so a brief network blip doesn't
//! count as the link going down and leave bundles waiting for the next pass.

use courier_proto::v1::relay_service_client::RelayServiceClient;
use courier_proto::v1::{ForwardBundleRequest, ForwardBundleResponse};
use std::time::Duration;
use tonic::transport::Channel;
use tonic::{Code, Response, Status};
use tower::retry::{Policy, RetryLayer};
use tower::util::BoxCloneSyncService;
use tower::ServiceBuilder;

const FIRST_BACKOFF: Duration = Duration::from_millis(100);

/// One peer's ForwardBundle call, with retries.
pub type Forwarder =
    BoxCloneSyncService<ForwardBundleRequest, Response<ForwardBundleResponse>, Status>;

pub fn forwarder(client: RelayServiceClient<Channel>, retries: u32) -> Forwarder {
    BoxCloneSyncService::new(
        ServiceBuilder::new()
            .layer(RetryLayer::new(RetryUnavailable::new(retries)))
            .service_fn(move |request| {
                let mut client = client.clone();
                async move { client.forward_bundle(request).await }
            }),
    )
}

/// Retry `UNAVAILABLE` up to `retries` times, doubling the wait each time.
/// Other errors are final: a rejected bundle will be rejected again, and a
/// timeout already took long enough.
#[derive(Clone)]
pub struct RetryUnavailable {
    retries_left: u32,
    backoff: Duration,
}

impl RetryUnavailable {
    pub fn new(retries: u32) -> Self {
        Self {
            retries_left: retries,
            backoff: FIRST_BACKOFF,
        }
    }
}

impl<Req: Clone, Res> Policy<Req, Res, Status> for RetryUnavailable {
    type Future = tokio::time::Sleep;

    fn retry(&mut self, _req: &mut Req, result: &mut Result<Res, Status>) -> Option<Self::Future> {
        match result {
            Err(status) if status.code() == Code::Unavailable && self.retries_left > 0 => {
                self.retries_left -= 1;
                let wait = self.backoff;
                self.backoff *= 2;
                Some(tokio::time::sleep(wait))
            }
            _ => None,
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(req.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use tower::ServiceExt;

    /// A service that fails with `code` for its first `failures` calls.
    async fn call(retries: u32, failures: u32, code: Code) -> (Result<(), Status>, u32) {
        let calls = Arc::new(AtomicU32::new(0));
        let counter = calls.clone();
        let service = ServiceBuilder::new()
            .layer(RetryLayer::new(RetryUnavailable::new(retries)))
            .service_fn(move |_: ()| {
                let n = counter.fetch_add(1, Ordering::Relaxed);
                async move {
                    if n < failures {
                        Err(Status::new(code, "nope"))
                    } else {
                        Ok(())
                    }
                }
            });
        let result = service.oneshot(()).await;
        (result, calls.load(Ordering::Relaxed))
    }

    #[tokio::test]
    async fn a_peer_that_is_not_listening_is_unavailable() {
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = closed.local_addr().unwrap();
        drop(closed);
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_lazy();
        let status = forwarder(RelayServiceClient::new(channel), 0)
            .oneshot(ForwardBundleRequest::default())
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unavailable);
    }

    #[tokio::test]
    async fn retries_unavailable_until_it_succeeds() {
        let (result, calls) = call(2, 2, Code::Unavailable).await;
        assert!(result.is_ok());
        assert_eq!(calls, 3);
    }

    #[tokio::test]
    async fn gives_up_after_the_last_retry() {
        let (result, calls) = call(1, 5, Code::Unavailable).await;
        assert_eq!(result.unwrap_err().code(), Code::Unavailable);
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn does_not_retry_other_errors() {
        for code in [
            Code::InvalidArgument,
            Code::DeadlineExceeded,
            Code::Internal,
        ] {
            let (result, calls) = call(3, 1, code).await;
            assert_eq!(result.unwrap_err().code(), code);
            assert_eq!(calls, 1, "{code:?} should not be retried");
        }
    }
}
