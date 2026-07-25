#![no_main]

use std::time::Duration;

use libfuzzer_sys::fuzz_target;
use market_squawk_data::Sha256Digest;
use market_squawk_modeling::{OnnxFallbackPolicy, OnnxModelPolicy, fuzz_parse_bundle_metadata};
use sha2::{Digest as _, Sha256};

const MAX_INPUT_BYTES: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }
    fuzz_parse_bundle_metadata(data);

    let mut digest: [u8; 32] = Sha256::digest(data).into();
    if digest == [0; 32] {
        digest[0] = 1;
    }
    let Ok(policy) = OnnxModelPolicy::try_new(
        Sha256Digest::new(digest),
        17,
        &[1, 1],
        &[1, 1],
        Duration::from_millis(5),
        OnnxFallbackPolicy::NoAction,
    ) else {
        return;
    };
    let _model = policy.preflight(data);
});
