pub mod v1 {
    // tonic::Status is large; that's tonic's design, not something to fix here.
    #![allow(clippy::result_large_err)]
    tonic::include_proto!("courier.v1");
}
