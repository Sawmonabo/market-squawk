//! Adaptive Criterion engineering benchmark with zero baseline or candidate evidence authority.
//!
//! The fixed-quota `capture_admission_evidence` target is the only authoritative evidence runner.

#[path = "capture_admission/benchmark_identity.rs"]
mod benchmark_identity;

use std::num::NonZeroUsize;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use market_squawk_platform::capture_benchmark_support::{BenchmarkCase, BenchmarkOperation};

const ENGINEERING_PAYLOAD_BYTES: usize = 1_024;
const ENGINEERING_QUEUE_DEPTH: NonZeroUsize = match NonZeroUsize::new(64) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

fn capture_admission_criterion(criterion: &mut Criterion) {
    if benchmark_identity::verify_distinct_authority_labels().is_err() {
        eprintln!("benchmark authority identity self-check failed");
        std::process::exit(2);
    }
    let mut group = criterion.benchmark_group(benchmark_identity::CRITERION_TARGET);
    group.throughput(Throughput::Elements(1));
    for (name, operation) in [
        ("queue_push", BenchmarkOperation::QueuePush),
        ("queue_pop", BenchmarkOperation::QueuePop),
        ("capture_admission", BenchmarkOperation::CaptureAdmission),
        ("writer_append", BenchmarkOperation::WriterAppend),
        (
            "flush_inclusive_writer",
            BenchmarkOperation::FlushInclusiveWriter,
        ),
    ] {
        let mut failure = None;
        group.bench_with_input(
            BenchmarkId::new(name, "production_seam"),
            &operation,
            |b, op| {
                b.iter_custom(
                    |iterations| match measure_named_operations(*op, iterations) {
                        Ok(elapsed) => elapsed,
                        Err(error) => {
                            failure =
                                Some(format!("operation={op:?} iterations={iterations}: {error}"));
                            Duration::ZERO
                        }
                    },
                );
            },
        );
        if let Some(error) = failure {
            eprintln!("Criterion production-seam measurement failed: {error}");
            std::process::exit(2);
        }
    }
    group.finish();
}

fn measure_named_operations(
    operation: BenchmarkOperation,
    iterations: u64,
) -> Result<Duration, String> {
    let maximum_samples = if matches!(
        operation,
        BenchmarkOperation::QueuePop
            | BenchmarkOperation::WriterAppend
            | BenchmarkOperation::FlushInclusiveWriter
    ) {
        usize::try_from(iterations).map_err(|error| error.to_string())?
    } else {
        0
    };
    let case = BenchmarkCase::try_new(
        operation,
        ENGINEERING_PAYLOAD_BYTES,
        ENGINEERING_QUEUE_DEPTH,
        maximum_samples,
    )
    .map_err(|error| error.to_string())?;
    let producer = case.try_producer().map_err(|error| error.to_string())?;
    let mut elapsed_nanos = 0_u64;
    for _ in 0..iterations {
        let attempt = producer
            .try_prepare_operation()
            .and_then(market_squawk_platform::capture_benchmark_support::BenchmarkPreparedOperation::execute)
            .map_err(|error| error.to_string())?;
        if !matches!(
            operation,
            BenchmarkOperation::QueuePop
                | BenchmarkOperation::WriterAppend
                | BenchmarkOperation::FlushInclusiveWriter
        ) {
            elapsed_nanos = elapsed_nanos
                .checked_add(attempt.latency_nanos())
                .ok_or_else(|| "Criterion latency total overflowed".to_owned())?;
        }
        std::hint::black_box(attempt.outcome());
    }
    let reconciliation = case.finish().map_err(|error| error.to_string())?;
    if reconciliation.accepted()
        != usize::try_from(iterations).map_err(|error| error.to_string())?
        || reconciliation.consumed()
            != usize::try_from(iterations).map_err(|error| error.to_string())?
        || reconciliation.queued_bytes() != 0
        || reconciliation.accounting_invariant_failures() != 0
    {
        return Err("Criterion production seam failed post-drain reconciliation".to_owned());
    }
    let writer_samples = reconciliation.into_samples();
    if matches!(
        operation,
        BenchmarkOperation::QueuePop
            | BenchmarkOperation::WriterAppend
            | BenchmarkOperation::FlushInclusiveWriter
    ) {
        if writer_samples.len() != usize::try_from(iterations).map_err(|error| error.to_string())? {
            return Err("Criterion writer sample count failed reconciliation".to_owned());
        }
        for sample in writer_samples {
            elapsed_nanos = elapsed_nanos
                .checked_add(sample)
                .ok_or_else(|| "Criterion writer latency total overflowed".to_owned())?;
        }
    } else if !writer_samples.is_empty() {
        return Err("Criterion producer endpoint returned writer samples".to_owned());
    }
    Ok(Duration::from_nanos(elapsed_nanos))
}

criterion_group!(benches, capture_admission_criterion);
criterion_main!(benches);
