//! Fixed-operation workloads over the closed production-operation benchmark seam.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use std::hint::black_box;

use super::backend::{self, OfferedLoadOutcome, PreparedCase};
use super::collector::{ProducerCollector, summarize, summarize_partitioned};
use super::endpoints::Endpoint;
use super::fixture::{
    ProducerCase, RSS_SAMPLE_JITTER_TOLERANCE, SustainedFixture, requested_operations,
};
use super::schema::{
    ComparableFullResult, MatrixResult, OutcomeCounts, PostDrainAccounting, RESULT_SCHEMA_VERSION,
    RssSample, SustainedEpochEvidence, SustainedEpochPhase, SustainedResult,
};

#[derive(Debug)]
struct ProducerResult {
    completed: usize,
    samples: Vec<u64>,
}

pub(crate) fn run_matrix_case(
    endpoint: Endpoint,
    payload_bytes: usize,
    queue_depth: usize,
    producer_case: ProducerCase,
) -> Result<MatrixResult, Box<dyn std::error::Error>> {
    let requested = requested_operations(payload_bytes, producer_case.count)?;
    let queue_depth = NonZeroUsize::new(queue_depth).ok_or("queue depth must be nonzero")?;
    let maximum_samples = if endpoint.has_deferred_samples() {
        requested
    } else {
        0
    };
    let case = PreparedCase::try_new(endpoint, payload_bytes, queue_depth, maximum_samples)?;
    let configured_queue_depth = case.configured_queue_depth().get();
    let effective_capacity = case.effective_capacity().get();
    let mut producers = Vec::new();
    producers.try_reserve_exact(producer_case.count.get())?;
    for _ in 0..producer_case.count.get() {
        producers.push(case.try_producer()?);
    }
    let start_barrier = Arc::new(Barrier::new(
        producer_case
            .count
            .get()
            .checked_add(1)
            .ok_or("benchmark barrier participant count overflowed")?,
    ));
    let started = Instant::now();
    let scoped = thread::scope(|scope| -> Result<Vec<ProducerResult>, String> {
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(producers.len())
            .map_err(|error| error.to_string())?;
        let base_operations = requested / producer_case.count.get();
        let remainder = requested % producer_case.count.get();
        for (index, producer) in producers.into_iter().enumerate() {
            let barrier = Arc::clone(&start_barrier);
            let operations = base_operations + usize::from(index < remainder);
            handles.push(scope.spawn(move || -> Result<ProducerResult, String> {
                let mut collector = if endpoint.has_deferred_samples() {
                    None
                } else {
                    Some(
                        ProducerCollector::try_new(operations, operations)
                            .map_err(|error| error.to_string())?,
                    )
                };
                barrier.wait();
                for ordinal in 0..operations {
                    let prepared = producer
                        .try_prepare_operation()
                        .map_err(|error| error.to_string())?;
                    let attempt = prepared.execute().map_err(|error| error.to_string())?;
                    black_box(attempt.outcome());
                    if let Some(collector) = &mut collector {
                        collector
                            .observe(ordinal, attempt.latency_nanos())
                            .map_err(str::to_owned)?;
                    }
                }
                Ok(ProducerResult {
                    completed: operations,
                    samples: collector.map_or_else(Vec::new, ProducerCollector::into_samples),
                })
            }));
        }
        start_barrier.wait();
        let mut results = Vec::new();
        results
            .try_reserve_exact(handles.len())
            .map_err(|error| error.to_string())?;
        for handle in handles {
            results.push(
                handle
                    .join()
                    .map_err(|_panic| "benchmark producer panicked".to_owned())??,
            );
        }
        Ok(results)
    });
    // Reconciliation is mandatory even when a producer reports an error; otherwise a background
    // production owner could outlive the failed case and contaminate later measurements.
    let reconciliation = case.finish();
    let producer_results = scoped?;
    let reconciliation = reconciliation?;
    let elapsed_nanos = u64::try_from(started.elapsed().as_nanos())?;
    let completed = producer_results.iter().try_fold(0_usize, |total, result| {
        total
            .checked_add(result.completed)
            .ok_or("benchmark completion count overflowed")
    })?;
    if elapsed_nanos == 0 || completed != requested {
        return Err("benchmark case did not complete its exact successful-operation quota".into());
    }
    if reconciliation.accepted() != requested
        || reconciliation.consumed() != requested
        || reconciliation.queued_bytes() != 0
        || reconciliation.accounting_invariant_failures() != 0
    {
        return Err("benchmark case post-drain accounting did not reconcile".into());
    }
    let post_drain = PostDrainAccounting {
        accepted: reconciliation.accepted(),
        consumed: reconciliation.consumed(),
        queued_bytes: reconciliation.queued_bytes(),
        record_reservations: 0,
        accounting_invariant_failures: reconciliation.accounting_invariant_failures(),
    };
    let deferred_samples = reconciliation.into_samples();
    let latency = if endpoint.has_deferred_samples() {
        if producer_results
            .iter()
            .any(|result| !result.samples.is_empty())
        {
            return Err("deferred latency escaped its immutable observation order".into());
        }
        if deferred_samples.len() != requested {
            return Err("deferred endpoint did not return its exact observation quota".into());
        }
        summarize(deferred_samples)?
    } else {
        if !deferred_samples.is_empty() {
            return Err("producer-timed endpoint returned deferred writer samples".into());
        }
        summarize_partitioned(
            producer_results
                .into_iter()
                .map(|result| result.samples)
                .collect(),
            requested,
        )?
    };
    let throughput_per_second = (completed as f64) * 1_000_000_000.0 / (elapsed_nanos as f64);
    Ok(MatrixResult {
        schema_version: RESULT_SCHEMA_VERSION,
        backend: backend::EVIDENCE_BACKEND.to_owned(),
        endpoint,
        payload_bytes,
        configured_queue_depth,
        effective_capacity,
        producers: producer_case.count.get(),
        representative: producer_case.representative,
        requested_operations: requested,
        completed_operations: completed,
        outcomes: OutcomeCounts {
            accepted: completed,
            queue_full: 0,
            queue_contended: 0,
        },
        post_drain,
        elapsed_nanos,
        throughput_per_second,
        latency,
    })
}

pub(crate) fn run_comparable_full() -> Result<ComparableFullResult, Box<dyn std::error::Error>> {
    backend::verify_comparable_full()?;
    Ok(ComparableFullResult {
        schema_version: RESULT_SCHEMA_VERSION,
        backend: backend::EVIDENCE_BACKEND.to_owned(),
        queue_full: 1,
    })
}

pub(crate) fn run_sustained(
    fixture: SustainedFixture,
    producers: NonZeroUsize,
) -> Result<SustainedResult, Box<dyn std::error::Error>> {
    let SustainedFixture {
        warm_epochs,
        warm_duration,
        measured_epochs,
        measured_duration,
        payload_bytes,
        queue_depth,
        rss_interval,
    } = fixture;
    let queue_depth = NonZeroUsize::new(queue_depth).ok_or("queue depth must be nonzero")?;
    if rss_interval.is_zero() {
        return Err("RSS sample interval must be nonzero".into());
    }
    let total_started = Instant::now();
    let mut accepted = 0_usize;
    let mut queue_full = 0_usize;
    let mut rss_samples = Vec::new();
    let mut epochs = Vec::new();
    let epoch_count = warm_epochs
        .checked_add(measured_epochs)
        .ok_or("sustained epoch count overflowed")?;
    rss_samples.try_reserve_exact(
        measured_epochs
            .checked_mul(usize::try_from(
                measured_duration
                    .as_millis()
                    .div_ceil(rss_interval.as_millis()),
            )?)
            .ok_or("sustained RSS sample capacity overflowed")?,
    )?;
    epochs.try_reserve_exact(epoch_count)?;
    for epoch in 0..epoch_count {
        let duration = if epoch < warm_epochs {
            warm_duration
        } else {
            measured_duration
        };
        let case = backend::OfferedLoadCase::try_new(payload_bytes, queue_depth)?;
        let mut producer_handles = Vec::new();
        producer_handles.try_reserve_exact(producers.get())?;
        for _ in 0..producers.get() {
            producer_handles.push(case.try_producer()?);
        }
        let accepted_before_epoch = accepted;
        let rss_before_epoch = rss_samples.len();
        let stop = AtomicBool::new(false);
        let barrier = Barrier::new(
            producers
                .get()
                .checked_add(1)
                .ok_or("sustained barrier participant count overflowed")?,
        );
        let scoped = thread::scope(|scope| -> Result<(usize, usize, u64), String> {
            let mut handles = Vec::new();
            handles
                .try_reserve_exact(producer_handles.len())
                .map_err(|error| error.to_string())?;
            for producer in producer_handles {
                let stop = &stop;
                let barrier = &barrier;
                handles.push(scope.spawn(move || -> Result<(usize, usize), String> {
                    let mut accepted = 0_usize;
                    let mut queue_full = 0_usize;
                    barrier.wait();
                    while !stop.load(Ordering::Acquire) {
                        match producer.try_offer().map_err(|error| error.to_string())? {
                            OfferedLoadOutcome::Accepted => {
                                accepted = accepted.checked_add(1).ok_or_else(|| {
                                    "sustained accepted count overflowed".to_owned()
                                })?;
                            }
                            OfferedLoadOutcome::QueueFull => {
                                queue_full = queue_full.checked_add(1).ok_or_else(|| {
                                    "sustained QueueFull count overflowed".to_owned()
                                })?;
                            }
                        }
                    }
                    Ok((accepted, queue_full))
                }));
            }
            barrier.wait();
            let epoch_started = Instant::now();
            let mut next_rss = Duration::ZERO;
            if epoch >= warm_epochs {
                capture_rss_sample(&mut rss_samples, epoch + 1, epoch_started, next_rss)
                    .map_err(|error| error.to_string())?;
                next_rss = rss_interval;
            }
            while epoch_started.elapsed() < duration {
                if epoch >= warm_epochs && epoch_started.elapsed() >= next_rss {
                    if next_rss < duration {
                        capture_rss_sample(&mut rss_samples, epoch + 1, epoch_started, next_rss)
                            .map_err(|error| error.to_string())?;
                        next_rss = next_rss
                            .checked_add(rss_interval)
                            .ok_or_else(|| "RSS sample deadline overflowed".to_owned())?;
                    }
                } else {
                    thread::yield_now();
                }
            }
            if epoch >= warm_epochs {
                let expected_epoch_samples =
                    usize::try_from(duration.as_nanos().div_ceil(rss_interval.as_nanos()))
                        .map_err(|error| error.to_string())?;
                if rss_samples.len() - rss_before_epoch != expected_epoch_samples {
                    return Err("sustained RSS cadence missed its exact sample quota".to_owned());
                }
            }
            stop.store(true, Ordering::Release);
            let mut epoch_accepted = 0_usize;
            let mut epoch_full = 0_usize;
            for handle in handles {
                let (producer_accepted, producer_full) = handle
                    .join()
                    .map_err(|_panic| "sustained producer panicked".to_owned())??;
                epoch_accepted = epoch_accepted
                    .checked_add(producer_accepted)
                    .ok_or_else(|| "sustained accepted aggregation overflowed".to_owned())?;
                epoch_full = epoch_full
                    .checked_add(producer_full)
                    .ok_or_else(|| "sustained full aggregation overflowed".to_owned())?;
            }
            let elapsed_nanos = u64::try_from(epoch_started.elapsed().as_nanos())
                .map_err(|error| error.to_string())?;
            Ok((epoch_accepted, epoch_full, elapsed_nanos))
        })?;
        accepted = accepted
            .checked_add(scoped.0)
            .ok_or("sustained accepted count overflowed")?;
        queue_full = queue_full
            .checked_add(scoped.1)
            .ok_or("sustained QueueFull count overflowed")?;
        let reconciled = case.finish()?;
        if reconciled.accepted() != accepted - accepted_before_epoch
            || reconciled.consumed() != accepted - accepted_before_epoch
            || reconciled.queued_bytes() != 0
            || reconciled.accounting_invariant_failures() != 0
        {
            return Err("sustained epoch accepted count failed reconciliation".into());
        }
        let post_drain_rss_bytes = current_rss_bytes()?;
        epochs.push(SustainedEpochEvidence {
            ordinal: epoch + 1,
            phase: if epoch < warm_epochs {
                SustainedEpochPhase::Warm
            } else {
                SustainedEpochPhase::Measured
            },
            target_duration_nanos: u64::try_from(duration.as_nanos())?,
            elapsed_nanos: scoped.2,
            outcomes: OutcomeCounts {
                accepted: scoped.0,
                queue_full: scoped.1,
                queue_contended: 0,
            },
            active_rss_samples: rss_samples[rss_before_epoch..].to_vec(),
            post_drain_rss_bytes,
            post_drain: PostDrainAccounting {
                accepted: reconciled.accepted(),
                consumed: reconciled.consumed(),
                queued_bytes: reconciled.queued_bytes(),
                record_reservations: 0,
                accounting_invariant_failures: reconciled.accounting_invariant_failures(),
            },
        });
    }
    if accepted == 0 || queue_full == 0 || rss_samples.is_empty() {
        return Err("sustained fixture requires successes, QueueFull, and RSS samples".into());
    }
    Ok(SustainedResult {
        schema_version: RESULT_SCHEMA_VERSION,
        backend: backend::EVIDENCE_BACKEND.to_owned(),
        warm_epochs,
        warm_duration_nanos: u64::try_from(warm_duration.as_nanos())?,
        measured_epochs,
        measured_duration_nanos: u64::try_from(measured_duration.as_nanos())?,
        payload_bytes,
        configured_queue_depth: queue_depth.get(),
        producers: producers.get(),
        rss_interval_nanos: u64::try_from(rss_interval.as_nanos())?,
        accepted,
        queue_full,
        queue_contended: 0,
        elapsed_nanos: u64::try_from(total_started.elapsed().as_nanos())?,
        rss_samples,
        epochs,
    })
}

fn current_rss_bytes() -> Result<u64, Box<dyn std::error::Error>> {
    rss_bytes_from_stats(memory_stats::memory_stats())
}

fn rss_bytes_from_stats(
    stats: Option<memory_stats::MemoryStats>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let physical = stats
        .ok_or("current-process RSS is unavailable")?
        .physical_mem;
    let bytes = u64::try_from(physical).map_err(|_error| "current-process RSS exceeds u64")?;
    if bytes == 0 {
        return Err("current-process RSS is zero".into());
    }
    Ok(black_box(bytes))
}

fn capture_rss_sample(
    samples: &mut Vec<RssSample>,
    epoch_ordinal: usize,
    epoch_started: Instant,
    target_offset: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let observed_offset = epoch_started.elapsed();
    let latest = target_offset
        .checked_add(RSS_SAMPLE_JITTER_TOLERANCE)
        .ok_or("RSS sample jitter deadline overflowed")?;
    if observed_offset < target_offset || observed_offset > latest {
        return Err("RSS sample missed its fixed cadence tolerance".into());
    }
    samples.push(RssSample {
        epoch_ordinal,
        target_offset_nanos: u64::try_from(target_offset.as_nanos())?,
        observed_offset_nanos: u64::try_from(observed_offset.as_nanos())?,
        rss_bytes: current_rss_bytes()?,
    });
    Ok(())
}

pub(crate) fn self_check_rss_adapter() -> Result<(), Box<dyn std::error::Error>> {
    let exact = memory_stats::MemoryStats {
        physical_mem: 4_096,
        virtual_mem: 8_192,
    };
    let zero = memory_stats::MemoryStats {
        physical_mem: 0,
        virtual_mem: 8_192,
    };
    if rss_bytes_from_stats(Some(exact))? != 4_096
        || rss_bytes_from_stats(Some(zero)).is_ok()
        || rss_bytes_from_stats(None).is_ok()
    {
        return Err("typed current-process RSS adapter contract changed".into());
    }
    Ok(())
}
