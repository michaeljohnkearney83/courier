use courier_bundle::{Bundle, EndpointId};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::time::Duration;

fn codec(c: &mut Criterion) {
    let bundle = Bundle::new(
        EndpointId::new("dtn://node-a/inbox").unwrap(),
        EndpointId::new("dtn://node-b/inbox").unwrap(),
        Duration::from_secs(60),
        vec![0xab; 4096],
    );
    let encoded = bundle.to_cbor().unwrap();

    let mut group = c.benchmark_group("cbor");
    group.throughput(Throughput::Bytes(encoded.len() as u64));
    group.bench_function("encode_4k", |b| {
        b.iter(|| black_box(&bundle).to_cbor().unwrap())
    });
    group.bench_function("decode_4k", |b| {
        b.iter(|| Bundle::from_cbor(black_box(&encoded)).unwrap())
    });
    group.finish();
}

criterion_group!(benches, codec);
criterion_main!(benches);
