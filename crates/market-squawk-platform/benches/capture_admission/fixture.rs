//! Frozen benchmark matrix and sustained-fixture constants.

use std::num::NonZeroUsize;
use std::time::Duration;

use super::producer_inventory::representative_producers;

pub(crate) const PAYLOAD_BYTES: [usize; 3] = [0, 1_024, 4_194_304];
pub(crate) const QUEUE_DEPTHS: [usize; 3] = [1, 64, 16_384];
pub(crate) const WRITER_QUEUE_DEPTHS: [usize; 1] = [64];
pub(crate) const EXPLICIT_PRODUCERS: [usize; 4] = [1, 2, 4, 8];
pub(crate) const MINIMUM_OPERATIONS: usize = 1_000_000;
pub(crate) const OPERATIONS_PER_PRODUCER: usize = 100_000;
pub(crate) const MAX_PAYLOAD_OPERATIONS: usize = 10_000;
pub(crate) const SUSTAINED_PAYLOAD_BYTES: usize = 1_024;
pub(crate) const SUSTAINED_QUEUE_DEPTH: usize = 16_384;
pub(crate) const WARM_EPOCHS: usize = 2;
pub(crate) const MEASURED_EPOCHS: usize = 10;
pub(crate) const WARM_EPOCH_DURATION: Duration = Duration::from_secs(5);
pub(crate) const MEASURED_EPOCH_DURATION: Duration = Duration::from_secs(10);
pub(crate) const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
pub(crate) const RSS_SAMPLE_JITTER_TOLERANCE: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SustainedFixture {
    pub(crate) warm_epochs: usize,
    pub(crate) warm_duration: Duration,
    pub(crate) measured_epochs: usize,
    pub(crate) measured_duration: Duration,
    pub(crate) payload_bytes: usize,
    pub(crate) queue_depth: usize,
    pub(crate) rss_interval: Duration,
}

pub(crate) const SUSTAINED_FIXTURE: SustainedFixture = SustainedFixture {
    warm_epochs: WARM_EPOCHS,
    warm_duration: WARM_EPOCH_DURATION,
    measured_epochs: MEASURED_EPOCHS,
    measured_duration: MEASURED_EPOCH_DURATION,
    payload_bytes: SUSTAINED_PAYLOAD_BYTES,
    queue_depth: SUSTAINED_QUEUE_DEPTH,
    rss_interval: RSS_SAMPLE_INTERVAL,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerCase {
    pub(crate) count: NonZeroUsize,
    pub(crate) representative: bool,
}

pub(crate) fn producer_cases() -> Result<Vec<ProducerCase>, Box<dyn std::error::Error>> {
    let representative = representative_producers()?;
    let mut cases = Vec::new();
    cases.try_reserve_exact(EXPLICIT_PRODUCERS.len().saturating_add(1))?;
    for count in EXPLICIT_PRODUCERS {
        let count = NonZeroUsize::new(count).ok_or("producer case must be nonzero")?;
        cases.push(ProducerCase {
            count,
            representative: count == representative,
        });
    }
    if !cases.iter().any(|case| case.count == representative) {
        cases.push(ProducerCase {
            count: representative,
            representative: true,
        });
    }
    Ok(cases)
}

pub(crate) fn requested_operations(
    payload_bytes: usize,
    producers: NonZeroUsize,
) -> Result<usize, &'static str> {
    if payload_bytes == *PAYLOAD_BYTES.last().ok_or("payload inventory is empty")? {
        return Ok(MAX_PAYLOAD_OPERATIONS);
    }
    producers
        .get()
        .checked_mul(OPERATIONS_PER_PRODUCER)
        .map(|scaled| scaled.max(MINIMUM_OPERATIONS))
        .ok_or("operation quota overflowed")
}
