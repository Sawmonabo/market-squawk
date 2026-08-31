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
    HarmonicPivot, HarmonicPivotKind, classify_harmonic_pattern,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, InstrumentId, PriceTicks, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn harmonic_pattern_is_causal_and_rejects_a_future_confirmation() -> TestResult {
    let bars = [
        harmonic_bar(10, 11, 90, 86),
        harmonic_bar(20, 21, 91, 85),
        harmonic_bar(30, 31, 92, 90),
        harmonic_bar(40, 41, 100, 99),
        harmonic_bar(50, 51, 90, 85),
        harmonic_bar(60, 61, 81, 80),
        harmonic_bar(70, 71, 85, 84),
        harmonic_bar(80, 81, 90, 89),
        harmonic_bar(90, 91, 79, 75),
        harmonic_bar(100, 101, 71, 70),
        harmonic_bar(110, 111, 78, 74),
    ];
    let pivots = [
        HarmonicPivot::new(1, HarmonicPivotKind::Low, Timestamp::from_unix_nanos(35)),
        HarmonicPivot::new(3, HarmonicPivotKind::High, Timestamp::from_unix_nanos(55)),
        HarmonicPivot::new(5, HarmonicPivotKind::Low, Timestamp::from_unix_nanos(75)),
        HarmonicPivot::new(7, HarmonicPivotKind::High, Timestamp::from_unix_nanos(95)),
        HarmonicPivot::new(9, HarmonicPivotKind::Low, Timestamp::from_unix_nanos(115)),
    ];
    let binding = HarmonicEvidenceBinding::new(
        InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?,
        NonZeroU64::new(60_000_000_000).ok_or("invalid timeframe")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [2; 32]),
    );
    let cutoff = Timestamp::from_unix_nanos(120);
    let evidence = classify_harmonic_pattern(HarmonicPatternInput::new(
        binding,
        &bars,
        pivots,
        cutoff,
        Timestamp::from_unix_nanos(160),
    ))?;

    assert_eq!(evidence.kind(), HarmonicPatternKind::AbCd);
    assert_eq!(evidence.direction(), HarmonicDirection::Bullish);
    assert!(evidence.completion_zone().contains(PriceTicks::new(70)));
    assert_eq!(
        evidence.confirmation_cutoff(),
        Timestamp::from_unix_nanos(115)
    );
    assert_eq!(
        evidence.execution_authority(),
        HarmonicExecutionAuthority::None
    );
    assert_eq!(
        evidence.confidence_authority(),
        HarmonicConfidenceAuthority::None
    );

    let mut future_pivots = pivots;
    future_pivots[4] =
        HarmonicPivot::new(9, HarmonicPivotKind::Low, Timestamp::from_unix_nanos(121));
    assert_eq!(
        classify_harmonic_pattern(HarmonicPatternInput::new(
            binding,
            &bars,
            future_pivots,
            cutoff,
            Timestamp::from_unix_nanos(160),
        )),
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
