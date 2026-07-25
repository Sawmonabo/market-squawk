use std::hint::black_box;
use std::num::{NonZeroU16, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::time::Instant;

use anyhow::{Context as _, Result, bail};
use market_squawk_adapter_kraken::{
    KrakenDecodeOutcome, KrakenDecoder, KrakenDecoderState, KrakenDepth,
};
use market_squawk_analytics::{
    RollingFeatureState, RollingWindowConfig, TopOfBookView, TradeFeatureView, top_of_book_features,
};
use market_squawk_domain::{
    AggressorSide, ConnectionGeneration, IntegrityRule, PriceTicks, QuantityLots, RuleVersion,
    SequenceNumber, SequenceValidationRule, SourceIdentifier, Timestamp,
};
use market_squawk_live::{BookSide, DepthLimit, LevelUpdate, ScaledBook, SequenceTracker};
use market_squawk_modeling::ReleaseEvidenceInferenceFixture;
use market_squawk_platform::capture_benchmark_support::{
    BenchmarkAttemptOutcome, BenchmarkCase, BenchmarkOperation,
    benchmark_private_storage_accounting, benchmark_transport_identity, verify_comparable_full,
};
use market_squawk_sources::{
    ProviderBookLevel, ProviderDecimalLexeme, ProviderPrice, ProviderQuantity,
    ProviderSequenceEvidence, SequenceValidationProfile, kraken_v2_crc32,
};
use serde::{Deserialize, Serialize};

const MAXIMUM_COMPONENT_SAMPLES: u64 = 100_000;
const MAXIMUM_ONNX_SAMPLES: u64 = 10_000;
pub(super) const KRAKEN_CHECKSUM: u32 = 3_310_070_434;
const KRAKEN_FIXTURE: &[u8] = include_bytes!(
    "../../../../../adapters/market-squawk-adapter-kraken/fixtures/official_book_checksum.json"
);

pub(super) fn kraken_fixture() -> &'static [u8] {
    KRAKEN_FIXTURE
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ComponentEvidence {
    kraken_decoder_and_checksum: ComponentLatencyDistribution,
    sequence_validation: ComponentLatencyDistribution,
    checksum_canonicalization: ComponentLatencyDistribution,
    bounded_queue_push: QueueEvidence,
    order_book_update: ComponentLatencyDistribution,
    online_feature_update: ComponentLatencyDistribution,
    native_inference: ComponentLatencyDistribution,
    onnx_inference: ComponentLatencyDistribution,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ComponentLatencyDistribution {
    operations: u64,
    elapsed_nanos: u64,
    operations_per_second: u64,
    p50_nanos: u64,
    p95_nanos: u64,
    p99_nanos: u64,
    maximum_nanos: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QueueEvidence {
    latency: ComponentLatencyDistribution,
    configured_depth: usize,
    effective_capacity: usize,
    accepted: usize,
    consumed: usize,
    queued_bytes_after_drain: usize,
    accounting_invariant_failures: u64,
    queue_private_storage_bytes: Option<usize>,
    fixed_capture_bytes: Option<usize>,
    total_accounted_bytes: Option<usize>,
    transport: String,
    private_storage_accounting: String,
    real_full_refusal_verified: bool,
}

pub(super) fn measure_all(
    inference: &mut ReleaseEvidenceInferenceFixture,
    requested_events: u64,
) -> Result<ComponentEvidence> {
    let samples = requested_events.min(MAXIMUM_COMPONENT_SAMPLES);
    Ok(ComponentEvidence {
        kraken_decoder_and_checksum: measure_kraken(samples)?,
        sequence_validation: measure_sequence(samples)?,
        checksum_canonicalization: measure_checksum(samples)?,
        bounded_queue_push: measure_queue(samples)?,
        order_book_update: measure_book(samples)?,
        online_feature_update: measure_features(samples)?,
        native_inference: measure(samples, || {
            black_box(
                inference
                    .infer_native()
                    .context("native inference component failed")?,
            );
            Ok(())
        })?,
        onnx_inference: measure(requested_events.min(MAXIMUM_ONNX_SAMPLES), || {
            black_box(
                inference
                    .infer_onnx()
                    .context("ONNX inference component failed")?,
            );
            Ok(())
        })?,
    })
}

fn measure_kraken(iterations: u64) -> Result<ComponentLatencyDistribution> {
    let instrument =
        market_squawk_domain::InstrumentId::from_str("018f0000-0000-7000-8000-000000000091")?;
    let mut decoder = KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)?;
    measure(iterations, || {
        let outcome = decoder.decode_payload(KRAKEN_FIXTURE)?;
        let KrakenDecodeOutcome::Market(observations) = outcome else {
            bail!("Kraken fixture did not decode as market data");
        };
        if observations.len() != 1
            || decoder.state() != KrakenDecoderState::Healthy
            || decoder.last_checksum() != Some(KRAKEN_CHECKSUM)
        {
            bail!("Kraken fixture checksum or state was not committed");
        }
        black_box(observations);
        Ok(())
    })
}

fn measure_sequence(iterations: u64) -> Result<ComponentLatencyDistribution> {
    let rule = integrity_rule("release-performance-sequence")?;
    let profile = SequenceValidationProfile::Provided {
        rule: rule.clone(),
        progression: SequenceValidationRule::Consecutive,
    };
    let generation = ConnectionGeneration::new(1)?;
    let mut tracker = SequenceTracker::new(generation, &profile);
    tracker.validate_snapshot(&ProviderSequenceEvidence::Provided {
        value: SequenceNumber::new(1),
        rule: rule.clone(),
    })?;
    let mut sequence = 2_u64;
    measure(iterations, || {
        black_box(tracker.validate_delta(&ProviderSequenceEvidence::Provided {
            value: SequenceNumber::new(sequence),
            rule: rule.clone(),
        })?);
        sequence = sequence
            .checked_add(1)
            .context("sequence fixture exhausted")?;
        Ok(())
    })
}

fn measure_checksum(iterations: u64) -> Result<ComponentLatencyDistribution> {
    let asks = checksum_levels(&[
        ("45285.2", "0.00100000"),
        ("45286.4", "1.54571953"),
        ("45286.6", "1.54571109"),
        ("45289.6", "1.54560911"),
        ("45290.2", "0.15890660"),
        ("45291.8", "1.54553491"),
        ("45294.7", "0.04454749"),
        ("45296.1", "0.35380000"),
        ("45297.5", "0.09945542"),
        ("45299.5", "0.18772827"),
    ])?;
    let bids = checksum_levels(&[
        ("45283.5", "0.10000000"),
        ("45283.4", "1.54582015"),
        ("45282.1", "0.10000000"),
        ("45281.0", "0.10000000"),
        ("45280.3", "1.54592586"),
        ("45279.0", "0.07990000"),
        ("45277.6", "0.03310103"),
        ("45277.5", "0.30000000"),
        ("45277.3", "1.54602737"),
        ("45276.6", "0.15445238"),
    ])?;
    let levels = NonZeroU16::new(10).context("invalid checksum fixture depth")?;
    measure(iterations, || {
        let checksum = kraken_v2_crc32(&asks, &bids, levels)?;
        if checksum != KRAKEN_CHECKSUM {
            bail!("Kraken checksum canonicalization fixture changed");
        }
        black_box(checksum);
        Ok(())
    })
}

fn checksum_levels(values: &[(&str, &str)]) -> Result<Vec<ProviderBookLevel>> {
    values
        .iter()
        .map(|(price, quantity)| {
            Ok(ProviderBookLevel::new(
                ProviderPrice::new(ProviderDecimalLexeme::try_new(price)?),
                ProviderQuantity::new(ProviderDecimalLexeme::try_new(quantity)?),
            ))
        })
        .collect()
}

fn measure_queue(iterations: u64) -> Result<QueueEvidence> {
    let iterations = usize::try_from(iterations).context("queue sample count is too large")?;
    let depth = NonZeroUsize::new(64).context("invalid queue depth")?;
    let case = BenchmarkCase::try_new(BenchmarkOperation::QueuePush, 256, depth, 0)?;
    let effective_capacity = case.effective_capacity().get();
    let configured_depth = case.configured_queue_depth().get();
    let producer = case.try_producer()?;
    let started = Instant::now();
    let mut samples = Vec::new();
    samples.try_reserve_exact(iterations)?;
    for _ in 0..iterations {
        let attempt = producer.try_prepare_operation()?.execute()?;
        if attempt.outcome() != BenchmarkAttemptOutcome::Accepted {
            bail!("capacity-permitted queue operation was not accepted");
        }
        samples.push(attempt.latency_nanos());
    }
    let elapsed = nanos(started.elapsed());
    let reconciliation = case.finish()?;
    if reconciliation.accepted() != iterations
        || reconciliation.consumed() != iterations
        || reconciliation.queued_bytes() != 0
        || reconciliation.accounting_invariant_failures() != 0
    {
        bail!("bounded queue did not reconcile exactly");
    }
    verify_comparable_full()?;
    Ok(QueueEvidence {
        latency: distribution(samples, elapsed)?,
        configured_depth,
        effective_capacity,
        accepted: reconciliation.accepted(),
        consumed: reconciliation.consumed(),
        queued_bytes_after_drain: reconciliation.queued_bytes(),
        accounting_invariant_failures: reconciliation.accounting_invariant_failures(),
        queue_private_storage_bytes: reconciliation.queue_private_storage_bytes(),
        fixed_capture_bytes: reconciliation.fixed_capture_bytes(),
        total_accounted_bytes: reconciliation.total_accounted_bytes(),
        transport: benchmark_transport_identity().to_owned(),
        private_storage_accounting: benchmark_private_storage_accounting().to_owned(),
        real_full_refusal_verified: true,
    })
}

fn measure_book(iterations: u64) -> Result<ComponentLatencyDistribution> {
    let mut book = ScaledBook::new(DepthLimit::new(10)?);
    book.replace_snapshot(
        &[LevelUpdate::new(
            BookSide::Bid,
            PriceTicks::new(10_000),
            QuantityLots::new(100)?,
        )],
        &[LevelUpdate::new(
            BookSide::Ask,
            PriceTicks::new(10_100),
            QuantityLots::new(100)?,
        )],
    )?;
    let mut quantity = 101_i64;
    measure(iterations, || {
        book.apply_delta(&[LevelUpdate::new(
            BookSide::Bid,
            PriceTicks::new(10_000),
            QuantityLots::new(quantity)?,
        )])?;
        quantity = if quantity == 101 { 102 } else { 101 };
        black_box(book.best_bid());
        Ok(())
    })
}

fn measure_features(iterations: u64) -> Result<ComponentLatencyDistribution> {
    let config = RollingWindowConfig::try_new(
        NonZeroUsize::new(64).context("invalid rolling capacity")?,
        NonZeroUsize::new(3).context("invalid rolling warm-up")?,
        NonZeroU64::new(60_000_000_000).context("invalid rolling duration")?,
        NonZeroUsize::new(1024 * 1024).context("invalid rolling retained-byte limit")?,
    )?;
    let mut rolling = RollingFeatureState::try_new(config)?;
    let mut timestamp = 1_i64;
    measure(iterations, || {
        let observed_at = Timestamp::from_unix_nanos(timestamp);
        let top = TopOfBookView::try_new(
            PriceTicks::new(10_000),
            QuantityLots::new(101)?,
            PriceTicks::new(10_100),
            QuantityLots::new(99)?,
            observed_at,
        )?;
        black_box(top_of_book_features(top)?);
        black_box(rolling.update(TradeFeatureView::try_new(
            PriceTicks::new(10_050 + (timestamp % 3)),
            QuantityLots::new(1)?,
            AggressorSide::Buy,
            observed_at,
        )?)?);
        timestamp = timestamp
            .checked_add(1)
            .context("feature timestamp exhausted")?;
        Ok(())
    })
}

fn measure(
    iterations: u64,
    mut operation: impl FnMut() -> Result<()>,
) -> Result<ComponentLatencyDistribution> {
    if iterations == 0 || iterations > MAXIMUM_COMPONENT_SAMPLES {
        bail!("component sample count is outside its fixed bound");
    }
    let mut samples = Vec::new();
    samples.try_reserve_exact(usize::try_from(iterations)?)?;
    let started = Instant::now();
    for _ in 0..iterations {
        let operation_started = Instant::now();
        operation()?;
        samples.push(nanos(operation_started.elapsed()));
    }
    distribution(samples, nanos(started.elapsed()))
}

fn distribution(mut samples: Vec<u64>, elapsed_nanos: u64) -> Result<ComponentLatencyDistribution> {
    if samples.is_empty() || elapsed_nanos == 0 {
        bail!("latency distribution is empty");
    }
    samples.sort_unstable();
    let operations = u64::try_from(samples.len())?;
    Ok(ComponentLatencyDistribution {
        operations,
        elapsed_nanos,
        operations_per_second: throughput(operations, elapsed_nanos)?,
        p50_nanos: quantile(&samples, 50)?,
        p95_nanos: quantile(&samples, 95)?,
        p99_nanos: quantile(&samples, 99)?,
        maximum_nanos: *samples.last().context("latency distribution is empty")?,
    })
}

fn quantile(sorted: &[u64], percentile: usize) -> Result<u64> {
    let rank = sorted
        .len()
        .checked_mul(percentile)
        .and_then(|value| value.checked_add(99))
        .map(|value| value / 100)
        .and_then(|value| value.checked_sub(1))
        .context("latency quantile overflow")?;
    sorted
        .get(rank)
        .copied()
        .context("latency quantile is outside the sample set")
}

fn throughput(operations: u64, elapsed_nanos: u64) -> Result<u64> {
    let value = u128::from(operations)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_div(u128::from(elapsed_nanos)))
        .context("throughput calculation overflow")?;
    u64::try_from(value).context("throughput exceeds the report representation")
}

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn integrity_rule(value: &str) -> Result<IntegrityRule> {
    Ok(IntegrityRule::new(
        SourceIdentifier::try_from(value)?,
        RuleVersion::new(1)?,
    ))
}
