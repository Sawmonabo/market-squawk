//! Fixed-capacity deterministic latency sampling and aggregation.

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub(crate) struct ProducerCollector {
    stride: usize,
    next_sample_ordinal: usize,
    samples: Vec<u64>,
    maximum_samples: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LatencySummary {
    pub(crate) samples: usize,
    pub(crate) p50_nanos: u64,
    pub(crate) p95_nanos: u64,
    pub(crate) p99_nanos: u64,
    pub(crate) maximum_nanos: u64,
}

impl ProducerCollector {
    pub(crate) fn try_new(
        producer_operations: usize,
        maximum_samples: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if producer_operations == 0 || maximum_samples == 0 {
            return Err("collector operations and sample capacity must be nonzero".into());
        }
        let stride = producer_operations.div_ceil(maximum_samples).max(1);
        let planned = producer_operations.div_ceil(stride).min(maximum_samples);
        let mut samples = Vec::new();
        samples.try_reserve_exact(planned)?;
        Ok(Self {
            stride,
            next_sample_ordinal: 0,
            samples,
            maximum_samples,
        })
    }

    pub(crate) fn observe(
        &mut self,
        ordinal: usize,
        elapsed_nanos: u64,
    ) -> Result<(), &'static str> {
        if ordinal == self.next_sample_ordinal {
            if self.samples.len() >= self.maximum_samples {
                return Err("benchmark sample collector overflowed");
            }
            self.samples.push(elapsed_nanos);
            self.next_sample_ordinal = self
                .next_sample_ordinal
                .checked_add(self.stride)
                .ok_or("benchmark sample ordinal overflowed")?;
        } else if ordinal > self.next_sample_ordinal {
            return Err("benchmark sample was written outside the deterministic stride");
        }
        Ok(())
    }

    pub(crate) fn into_samples(self) -> Vec<u64> {
        self.samples
    }
}

pub(crate) fn summarize(mut samples: Vec<u64>) -> Result<LatencySummary, &'static str> {
    if samples.is_empty() {
        return Err("benchmark produced zero latency samples");
    }
    samples.sort_unstable();
    let p50 = percentile_index(samples.len(), 50)?;
    let p95 = percentile_index(samples.len(), 95)?;
    let p99 = percentile_index(samples.len(), 99)?;
    let maximum = samples
        .last()
        .copied()
        .ok_or("benchmark lost its maximum sample")?;
    Ok(LatencySummary {
        samples: samples.len(),
        p50_nanos: samples[p50],
        p95_nanos: samples[p95],
        p99_nanos: samples[p99],
        maximum_nanos: maximum,
    })
}

pub(crate) fn summarize_partitioned(
    mut partitions: Vec<Vec<u64>>,
    expected_samples: usize,
) -> Result<LatencySummary, &'static str> {
    if expected_samples == 0 || partitions.is_empty() {
        return Err("benchmark produced zero latency samples");
    }
    let mut observed = 0_usize;
    let mut minimum = u64::MAX;
    let mut maximum = 0_u64;
    for partition in &mut partitions {
        if partition.is_empty() {
            return Err("benchmark produced an empty latency partition");
        }
        partition.sort_unstable();
        observed = observed
            .checked_add(partition.len())
            .ok_or("benchmark sample count overflowed")?;
        minimum = minimum.min(partition[0]);
        maximum = maximum.max(
            partition
                .last()
                .copied()
                .ok_or("benchmark lost a partition maximum")?,
        );
    }
    if observed != expected_samples {
        return Err("benchmark sample count differs from its exact quota");
    }
    Ok(LatencySummary {
        samples: observed,
        p50_nanos: select_nearest_rank(&partitions, observed, 50, minimum, maximum)?,
        p95_nanos: select_nearest_rank(&partitions, observed, 95, minimum, maximum)?,
        p99_nanos: select_nearest_rank(&partitions, observed, 99, minimum, maximum)?,
        maximum_nanos: maximum,
    })
}

fn select_nearest_rank(
    partitions: &[Vec<u64>],
    sample_count: usize,
    percentile: usize,
    mut lower: u64,
    mut upper: u64,
) -> Result<u64, &'static str> {
    let rank = percentile_index(sample_count, percentile)?
        .checked_add(1)
        .ok_or("benchmark percentile rank overflowed")?;
    while lower < upper {
        let midpoint = lower + (upper - lower) / 2;
        let at_or_below = partitions.iter().try_fold(0_usize, |count, partition| {
            count
                .checked_add(partition.partition_point(|sample| *sample <= midpoint))
                .ok_or("benchmark percentile count overflowed")
        })?;
        if at_or_below >= rank {
            upper = midpoint;
        } else {
            lower = midpoint
                .checked_add(1)
                .ok_or("benchmark percentile selection overflowed")?;
        }
    }
    Ok(lower)
}

fn percentile_index(length: usize, percentile: usize) -> Result<usize, &'static str> {
    let rank = length
        .checked_mul(percentile)
        .and_then(|value| value.checked_add(99))
        .ok_or("benchmark percentile rank overflowed")?
        / 100;
    rank.max(1)
        .checked_sub(1)
        .ok_or("benchmark percentile index underflowed")
}
