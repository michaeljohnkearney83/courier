#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = courier_bundle::Bundle::from_cbor(data);
});
