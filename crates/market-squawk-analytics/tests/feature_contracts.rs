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
    HarmonicBar, HarmonicConfidenceAuthority, HarmonicDirection, HarmonicEvidenceBinding,
    HarmonicExecutionAuthority, HarmonicPatternError, HarmonicPatternInput, HarmonicPatternKind,
    HarmonicPatternQuality, KnownFeatureImplementation, classify_harmonic_pattern,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, InstrumentId, PriceTicks, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn harmonic_pattern_is_causal_and_rejects_a_future_confirmation() -> TestResult {
    let bars = [
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
    ];
    let binding = HarmonicEvidenceBinding::new(
        InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?,
        NonZeroU64::new(60_000_000_000).ok_or("invalid timeframe")?,
        &[EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32])],
        EvidenceDigest::new(DigestAlgorithm::Sha256, [2; 32]),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [3; 32]),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [4; 32]),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [5; 32]),
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
