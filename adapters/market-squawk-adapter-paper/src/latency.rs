//! Stable seeded latency sampling.

use market_squawk_domain::OrderId;
use rand_chacha::ChaCha12Rng;
use rand_core::{RngCore, SeedableRng};
use sha2::{Digest, Sha256};

pub(crate) fn sample_latency(
    seed: [u8; 32],
    configuration_version: u64,
    order_id: OrderId,
    minimum: u64,
    maximum: u64,
    domain: &[u8],
) -> u64 {
    if minimum == maximum {
        return minimum;
    }
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/paper-latency/v1\0");
    digest.update(seed);
    digest.update(configuration_version.to_be_bytes());
    digest.update(order_id.as_uuid().as_bytes());
    digest.update(domain);
    let mut rng = ChaCha12Rng::from_seed(digest.finalize().into());
    let span = maximum - minimum + 1;
    let rejection_ceiling = u64::MAX - (u64::MAX % span);
    loop {
        let candidate = rng.next_u64();
        if candidate < rejection_ceiling {
            return minimum + (candidate % span);
        }
    }
}
