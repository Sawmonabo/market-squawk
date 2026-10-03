//! Reproducibility of an actual bounded price-pattern evaluation, including no-pattern results.

use market_squawk_analytics::{
    HarmonicPatternEvidence, HarmonicPivotKind, HarmonicRatioMeasurement,
    KnownFeatureImplementation, MAX_HARMONIC_BARS, MIN_HARMONIC_BARS,
};
use market_squawk_domain::{
    Currency, DigestAlgorithm, EvidenceDigest, InstrumentId, PriceTicks, TickSize, Timestamp,
};
use sha2::{Digest as _, Sha256};

use super::InvestmentProposalError;

/// Closed result of running the detector; missing source history is not an evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarmonicHistoryDisposition {
    Pattern,
    InsufficientBars,
    InsufficientPivots,
    NoMatchingPattern,
    Expired,
    Invalidated,
}

/// One actual confirmed pivot; positions/effective clocks remain separate from acquisition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicHistoryPivot {
    pub bar_index: u32,
    pub high: bool,
    pub observed_at: Timestamp,
    pub available_at: Timestamp,
    pub confirmed_at: Timestamp,
    pub price: PriceTicks,
}

/// Fixed-size exact geometry retained after the analytical input window is released.
/// Ratios follow AB/XA, BC/AB, CD/BC, CD/AB, AD/XA, XC/XA, CD/XC; tolerances and
/// accepted bands are fixed by the exact retained compiled implementation identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarmonicHistoryGeometry {
    pub pivots: [HarmonicHistoryPivot; 5],
    pub ratios: [Option<(u64, std::num::NonZeroU64)>; 7],
}

impl HarmonicHistoryGeometry {
    #[must_use]
    pub fn from_pattern(pattern: HarmonicPatternEvidence) -> Self {
        Self {
            pivots: pattern.pivots().map(|pivot| HarmonicHistoryPivot {
                bar_index: pivot.bar_index(),
                high: pivot.kind() == HarmonicPivotKind::High,
                observed_at: pivot.observed_at(),
                available_at: pivot.available_at(),
                confirmed_at: pivot.confirmed_at(),
                price: pivot.price(),
            }),
            ratios: [
                HarmonicRatioMeasurement::AbOverXa,
                HarmonicRatioMeasurement::BcOverAb,
                HarmonicRatioMeasurement::CdOverBc,
                HarmonicRatioMeasurement::CdOverAb,
                HarmonicRatioMeasurement::AdOverXa,
                HarmonicRatioMeasurement::XcOverXa,
                HarmonicRatioMeasurement::CdOverXc,
            ]
            .map(|key| {
                pattern
                    .ratios()
                    .get(key)
                    .map(|value| (value.numerator(), value.denominator()))
            }),
        }
    }
}

/// Exact audit inputs. Construction checks structure, never grants source or execution authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarmonicHistoryAuditInput {
    pub instrument_id: InstrumentId,
    pub currency: Currency,
    /// Original execution increment when the source supplies it; never an analytical grid.
    pub execution_tick: Option<TickSize>,
    pub analytical_tick: TickSize,
    pub source_cutoff: Timestamp,
    pub observed_through: Timestamp,
    pub observed_at: Timestamp,
    pub available_at: Timestamp,
    pub rights_decision_identity: EvidenceDigest,
    pub rights_graph_identity: EvidenceDigest,
    pub rights_checked_at: Timestamp,
    pub rights_expires_at: Timestamp,
    pub evaluated_at: Timestamp,
    pub source_identity: EvidenceDigest,
    pub selected_manifest: EvidenceDigest,
    pub origin_manifest: EvidenceDigest,
    pub adjustment_identity: EvidenceDigest,
    pub calendar_identity: EvidenceDigest,
    pub completeness_identity: EvidenceDigest,
    pub marketability_identity: EvidenceDigest,
    pub implementation_identity: EvidenceDigest,
    pub materialized_bars: u32,
    pub start_ordinal: u32,
    pub evaluated_bars: u32,
    pub disposition: HarmonicHistoryDisposition,
    pub pattern_digest: Option<EvidenceDigest>,
    pub geometry: Option<HarmonicHistoryGeometry>,
}

/// Immutable audit with exact pricing units and real knowledge/economic clocks.
///
/// The catalog remains the source authority. This record is retained with the proposal and
/// historical instruction so an evaluated absence cannot be confused with a skipped detector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarmonicHistoryAudit {
    input: HarmonicHistoryAuditInput,
    digest: EvidenceDigest,
}

impl HarmonicHistoryAudit {
    pub fn try_new(input: HarmonicHistoryAuditInput) -> Result<Self, InvestmentProposalError> {
        let invalid = || InvestmentProposalError::InvalidEvidenceMetric;
        let known = KnownFeatureImplementation::BatchHarmonicPatterns
            .implementation_digest()
            .map_err(|_| invalid())?;
        if input.observed_at > input.observed_through
            || input.observed_through > input.source_cutoff
            || input.observed_at > input.available_at
            || input.available_at > input.source_cutoff
            || input.source_cutoff > input.rights_checked_at
            || input.rights_checked_at > input.evaluated_at
            || input.evaluated_at >= input.rights_expires_at
            || input.evaluated_bars == 0
            || input.evaluated_bars as usize > MAX_HARMONIC_BARS
            || input
                .start_ordinal
                .checked_add(input.evaluated_bars)
                .is_none_or(|end| end > input.materialized_bars)
            || (input.disposition == HarmonicHistoryDisposition::Pattern)
                != input.pattern_digest.is_some()
            || input.pattern_digest.is_some() != input.geometry.is_some()
            || (input.disposition == HarmonicHistoryDisposition::InsufficientBars)
                != ((input.evaluated_bars as usize) < MIN_HARMONIC_BARS)
            || input.implementation_identity.bytes() != known.as_bytes()
        {
            return Err(invalid());
        }
        for digest in [
            input.rights_decision_identity,
            input.rights_graph_identity,
            input.source_identity,
            input.selected_manifest,
            input.origin_manifest,
            input.adjustment_identity,
            input.calendar_identity,
            input.completeness_identity,
            input.marketability_identity,
            input.implementation_identity,
        ]
        .into_iter()
        .chain(input.pattern_digest)
        {
            if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
                return Err(InvestmentProposalError::ReservedIdentity);
            }
        }
        if let Some(geometry) = input.geometry {
            for pivot in geometry.pivots {
                if pivot.bar_index >= input.evaluated_bars.saturating_sub(1)
                    || pivot.observed_at > input.observed_at
                    || pivot.observed_at > pivot.available_at
                    || pivot.available_at > pivot.confirmed_at
                    || pivot.confirmed_at > input.available_at
                    || pivot.price.get() <= 0
                {
                    return Err(invalid());
                }
            }
            if geometry.pivots.windows(2).any(|pair| {
                pair[0].bar_index >= pair[1].bar_index
                    || pair[0].observed_at >= pair[1].observed_at
                    || pair[0].confirmed_at > pair[1].confirmed_at
                    || pair[0].high == pair[1].high
            }) || geometry.ratios[..6].iter().any(Option::is_none)
            {
                return Err(invalid());
            }
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/harmonic-history-evaluation/v1\0");
        hash.update(input.instrument_id.as_uuid().as_bytes());
        hash.update(input.currency.as_str().as_bytes());
        match input.execution_tick {
            Some(step) => {
                hash.update([1]);
                let value = step.as_decimal().normalize();
                hash.update(value.mantissa().to_be_bytes());
                hash.update(value.scale().to_be_bytes());
            }
            None => hash.update([0]),
        }
        let analytical_step = input.analytical_tick.as_decimal().normalize();
        hash.update(analytical_step.mantissa().to_be_bytes());
        hash.update(analytical_step.scale().to_be_bytes());
        for clock in [
            input.source_cutoff,
            input.observed_through,
            input.observed_at,
            input.available_at,
            input.rights_checked_at,
            input.rights_expires_at,
            input.evaluated_at,
        ] {
            hash.update(clock.unix_nanos().to_be_bytes());
        }
        for identity in [
            input.rights_decision_identity,
            input.rights_graph_identity,
            input.source_identity,
            input.selected_manifest,
            input.origin_manifest,
            input.adjustment_identity,
            input.calendar_identity,
            input.completeness_identity,
            input.marketability_identity,
            input.implementation_identity,
        ] {
            hash.update(identity.bytes());
        }
        for count in [
            input.materialized_bars,
            input.start_ordinal,
            input.evaluated_bars,
        ] {
            hash.update(count.to_be_bytes());
        }
        hash.update([match input.disposition {
            HarmonicHistoryDisposition::Pattern => 0,
            HarmonicHistoryDisposition::InsufficientBars => 1,
            HarmonicHistoryDisposition::InsufficientPivots => 2,
            HarmonicHistoryDisposition::NoMatchingPattern => 3,
            HarmonicHistoryDisposition::Expired => 4,
            HarmonicHistoryDisposition::Invalidated => 5,
        }]);
        match input.pattern_digest {
            Some(digest) => {
                hash.update([1]);
                hash.update(digest.bytes());
            }
            None => hash.update([0]),
        }
        if let Some(geometry) = input.geometry {
            hash.update([1]);
            for pivot in geometry.pivots {
                hash.update(pivot.bar_index.to_be_bytes());
                hash.update([u8::from(pivot.high)]);
                for clock in [pivot.observed_at, pivot.available_at, pivot.confirmed_at] {
                    hash.update(clock.unix_nanos().to_be_bytes());
                }
                hash.update(pivot.price.get().to_be_bytes());
            }
            for ratio in geometry.ratios {
                match ratio {
                    Some((num, den)) => {
                        hash.update([1]);
                        hash.update(num.to_be_bytes());
                        hash.update(den.get().to_be_bytes());
                    }
                    None => hash.update([0]),
                }
            }
        } else {
            hash.update([0]);
        }
        let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
        Ok(Self { input, digest })
    }

    #[must_use]
    pub const fn input(&self) -> &HarmonicHistoryAuditInput {
        &self.input
    }

    #[must_use]
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}
