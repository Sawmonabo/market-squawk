#[path = "contracts.rs"]
mod contracts;
#[path = "golden.rs"]
mod golden;
#[path = "live_batch_parity.rs"]
mod live_batch_parity;
#[path = "properties.rs"]
mod properties;
#[path = "registry.rs"]
mod registry;

use std::{num::NonZeroU64, str::FromStr};

use market_squawk_analytics::{
    HARMONIC_TARGET_COUNT, HarmonicBar, HarmonicConfidenceAuthority, HarmonicDirection,
    HarmonicEvidenceBinding, HarmonicExecutionAuthority, HarmonicPatternError,
    HarmonicPatternInput, HarmonicPatternKind, HarmonicPatternQuality, KnownFeatureImplementation,
    MAX_HARMONIC_PARENT_MANIFESTS, classify_harmonic_pattern,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, InstrumentId, PriceTicks, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn harmonic_pattern_is_causal_and_rejects_a_future_confirmation() -> TestResult {
    let bars = bullish_bat_bars();
    let binding = harmonic_binding(
        InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?,
        NonZeroU64::new(60_000_000_000).ok_or("invalid timeframe")?,
        &[sha256(1)],
        [2, 3, 4, 5],
    )?;
    let cutoff = Timestamp::from_unix_nanos(120);
    let evidence = classify_harmonic_pattern(HarmonicPatternInput::new(binding, &bars, cutoff))?;

    assert_eq!(evidence.kind(), HarmonicPatternKind::Bat);
    assert_eq!(evidence.direction(), HarmonicDirection::Bullish);
    assert_eq!(evidence.quality(), HarmonicPatternQuality::Valid);
    assert!(evidence.completion_zone().contains(PriceTicks::new(1_114)));
    assert_eq!(evidence.entry_range(), evidence.completion_zone());
    assert_eq!(
        evidence.targets().map(PriceTicks::get),
        [1_325, 1_454, 1_664]
    );
    assert_eq!(evidence.invalidation(), PriceTicks::new(999));
    assert_eq!(
        evidence.pivots().map(|pivot| pivot.bar_index()),
        [1, 3, 5, 7, 9]
    );
    assert_eq!(
        evidence.observation_cutoff(),
        Timestamp::from_unix_nanos(110)
    );
    assert_eq!(
        evidence.confirmation_cutoff(),
        Timestamp::from_unix_nanos(111)
    );
    assert_eq!(
        evidence.expires_at(),
        Timestamp::from_unix_nanos(300_000_000_111)
    );
    assert_eq!(evidence.binding().parent_manifests().len(), 1);
    assert_eq!(
        evidence.implementation_identity(),
        KnownFeatureImplementation::BatchHarmonicPatterns.implementation_digest()?
    );
    assert_eq!(
        evidence.execution_authority(),
        HarmonicExecutionAuthority::None
    );
    assert_eq!(
        evidence.confidence_authority(),
        HarmonicConfidenceAuthority::None
    );

    let mut future_bars = bars;
    future_bars[10] = harmonic_bar(110, 121, 1_400, 1_200);
    assert_eq!(
        classify_harmonic_pattern(HarmonicPatternInput::new(binding, &future_bars, cutoff,)),
        Err(HarmonicPatternError::FutureInformation)
    );
    Ok(())
}

#[test]
fn harmonic_pattern_mirrors_bearish_price_plan_without_authority() -> TestResult {
    let bars = bullish_bat_bars().map(|bar| mirror_bar(bar, 3_000));
    let binding = harmonic_binding(
        InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?,
        NonZeroU64::new(60_000_000_000).ok_or("invalid timeframe")?,
        &[sha256(1)],
        [2, 3, 4, 5],
    )?;
    let evidence = classify_harmonic_pattern(HarmonicPatternInput::new(
        binding,
        &bars,
        Timestamp::from_unix_nanos(120),
    ))?;

    assert_eq!(evidence.kind(), HarmonicPatternKind::Bat);
    assert_eq!(evidence.direction(), HarmonicDirection::Bearish);
    assert!(evidence.entry_range().contains(PriceTicks::new(1_886)));
    assert_eq!(evidence.targets().len(), HARMONIC_TARGET_COUNT);
    assert_eq!(
        evidence.targets().map(PriceTicks::get),
        [1_675, 1_546, 1_336]
    );
    assert_eq!(evidence.invalidation(), PriceTicks::new(2_001));
    assert!(evidence.invalidation() > evidence.entry_range().upper());
    assert!(evidence.targets().windows(2).all(|pair| pair[0] > pair[1]));
    assert_eq!(
        evidence.execution_authority(),
        HarmonicExecutionAuthority::None
    );
    assert_eq!(
        evidence.confidence_authority(),
        HarmonicConfidenceAuthority::None
    );
    Ok(())
}

#[test]
fn harmonic_pattern_retains_latest_five_of_six_canonical_pivots() -> TestResult {
    let core = bullish_bat_bars();
    let bars = [
        harmonic_bar(-10, -9, 1_400, 1_200),
        harmonic_bar(0, 1, 1_600, 1_300),
        core[0],
        core[1],
        core[2],
        core[3],
        core[4],
        core[5],
        core[6],
        core[7],
        core[8],
        core[9],
        core[10],
    ];
    let binding = harmonic_binding(
        InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?,
        NonZeroU64::new(60_000_000_000).ok_or("invalid timeframe")?,
        &[sha256(1)],
        [2, 3, 4, 5],
    )?;
    let evidence = classify_harmonic_pattern(HarmonicPatternInput::new(
        binding,
        &bars,
        Timestamp::from_unix_nanos(120),
    ))?;

    assert_eq!(evidence.kind(), HarmonicPatternKind::Bat);
    assert_eq!(
        evidence.pivots().map(|pivot| pivot.bar_index()),
        [3, 5, 7, 9, 11]
    );
    assert_eq!(
        evidence.targets().map(PriceTicks::get),
        [1_325, 1_454, 1_664]
    );
    Ok(())
}

#[test]
fn harmonic_binding_is_bounded_and_digest_binds_each_policy() -> TestResult {
    let instrument_id = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?;
    let timeframe = NonZeroU64::new(60_000_000_000).ok_or("invalid timeframe")?;
    let empty = [];
    let duplicate = [sha256(1), sha256(1)];
    let unsorted = [sha256(2), sha256(1)];
    let zero = [sha256(0)];
    let wrong_algorithm = [EvidenceDigest::new(DigestAlgorithm::Blake3, [1; 32])];
    let over_bound: [EvidenceDigest; MAX_HARMONIC_PARENT_MANIFESTS + 1] =
        std::array::from_fn(|index| sha256(u8::try_from(index + 1).unwrap_or(u8::MAX)));

    for invalid in [
        empty.as_slice(),
        duplicate.as_slice(),
        unsorted.as_slice(),
        zero.as_slice(),
        wrong_algorithm.as_slice(),
        over_bound.as_slice(),
    ] {
        assert_eq!(
            harmonic_binding(instrument_id, timeframe, invalid, [2, 3, 4, 5]),
            Err(HarmonicPatternError::InvalidParentIdentity)
        );
    }

    let bars = bullish_bat_bars();
    let baseline = harmonic_binding(instrument_id, timeframe, &[sha256(1)], [2, 3, 4, 5])?;
    let baseline_digest = classify_harmonic_pattern(HarmonicPatternInput::new(
        baseline,
        &bars,
        Timestamp::from_unix_nanos(120),
    ))?
    .evidence_digest();
    let variants = [
        harmonic_binding(
            instrument_id,
            timeframe,
            &[sha256(1), sha256(6)],
            [2, 3, 4, 5],
        )?,
        harmonic_binding(instrument_id, timeframe, &[sha256(1)], [6, 3, 4, 5])?,
        harmonic_binding(instrument_id, timeframe, &[sha256(1)], [2, 6, 4, 5])?,
        harmonic_binding(instrument_id, timeframe, &[sha256(1)], [2, 3, 6, 5])?,
        harmonic_binding(instrument_id, timeframe, &[sha256(1)], [2, 3, 4, 6])?,
    ];
    for variant in variants {
        let digest = classify_harmonic_pattern(HarmonicPatternInput::new(
            variant,
            &bars,
            Timestamp::from_unix_nanos(120),
        ))?
        .evidence_digest();
        assert_ne!(digest, baseline_digest);
    }
    Ok(())
}

fn bullish_bat_bars() -> [HarmonicBar; 11] {
    [
        harmonic_bar(10, 11, 1_300, 1_100),
        harmonic_bar(20, 21, 1_200, 1_000),
        harmonic_bar(30, 31, 1_500, 1_300),
        harmonic_bar(40, 41, 2_000, 1_200),
        harmonic_bar(50, 51, 1_700, 1_500),
        harmonic_bar(60, 61, 1_550, 1_450),
        harmonic_bar(70, 71, 1_650, 1_600),
        harmonic_bar(80, 81, 1_664, 1_600),
        harmonic_bar(90, 91, 1_500, 1_300),
        harmonic_bar(100, 101, 1_200, 1_114),
        harmonic_bar(110, 111, 1_400, 1_200),
    ]
}

fn harmonic_binding(
    instrument_id: InstrumentId,
    timeframe_nanos: NonZeroU64,
    parent_manifests: &[EvidenceDigest],
    policies: [u8; 4],
) -> Result<HarmonicEvidenceBinding, HarmonicPatternError> {
    HarmonicEvidenceBinding::new(
        instrument_id,
        timeframe_nanos,
        parent_manifests,
        sha256(policies[0]),
        sha256(policies[1]),
        sha256(policies[2]),
        sha256(policies[3]),
    )
}

fn sha256(byte: u8) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
}

fn mirror_bar(bar: HarmonicBar, axis: i64) -> HarmonicBar {
    HarmonicBar::new(
        bar.observed_at(),
        bar.available_at(),
        PriceTicks::new(axis - bar.open().get()),
        PriceTicks::new(axis - bar.low().get()),
        PriceTicks::new(axis - bar.high().get()),
        PriceTicks::new(axis - bar.close().get()),
    )
}

fn harmonic_bar(observed_at: i64, available_at: i64, high: i64, low: i64) -> HarmonicBar {
    HarmonicBar::new(
        Timestamp::from_unix_nanos(observed_at),
        Timestamp::from_unix_nanos(available_at),
        PriceTicks::new(low + 1),
        PriceTicks::new(high),
        PriceTicks::new(low),
        PriceTicks::new(high - 1),
    )
}
