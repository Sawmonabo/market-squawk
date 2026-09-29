//! Closed durable codec for the actual harmonic detector evaluation and exact price units.

use market_squawk_decisions::{
    HarmonicHistoryAudit, HarmonicHistoryAuditInput, HarmonicHistoryDisposition,
    HarmonicHistoryGeometry, HarmonicHistoryPivot,
};
use market_squawk_domain::{
    Currency, EvidenceDigest, InstrumentId, PriceTicks, TickSize, Timestamp,
};
use serde::{Deserialize, Serialize};

use super::super::DecisionApplicationError;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HarmonicHistoryWire {
    instrument_id: InstrumentId,
    currency: Currency,
    #[serde(deserialize_with = "Option::deserialize")]
    execution_tick: Option<TickSize>,
    analytical_tick: TickSize,
    source_cutoff: Timestamp,
    observed_through: Timestamp,
    observed_at: Timestamp,
    available_at: Timestamp,
    rights_decision_identity: EvidenceDigest,
    rights_graph_identity: EvidenceDigest,
    rights_checked_at: Timestamp,
    rights_expires_at: Timestamp,
    evaluated_at: Timestamp,
    source_identity: EvidenceDigest,
    selected_manifest: EvidenceDigest,
    origin_manifest: EvidenceDigest,
    adjustment_identity: EvidenceDigest,
    calendar_identity: EvidenceDigest,
    completeness_identity: EvidenceDigest,
    marketability_identity: EvidenceDigest,
    implementation_identity: EvidenceDigest,
    materialized_bars: u32,
    start_ordinal: u32,
    evaluated_bars: u32,
    disposition: DispositionWire,
    evaluation_digest: EvidenceDigest,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum DispositionWire {
    Pattern {
        digest: EvidenceDigest,
        geometry: Box<GeometryWire>,
    },
    InsufficientBars,
    InsufficientPivots,
    NoMatchingPattern,
    Expired,
    Invalidated,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct GeometryWire {
    pivots: [PivotWire; 5],
    ratios: [Option<(u64, std::num::NonZeroU64)>; 7],
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PivotWire {
    bar_index: u32,
    high: bool,
    observed_at: Timestamp,
    available_at: Timestamp,
    confirmed_at: Timestamp,
    price: PriceTicks,
}

impl From<HarmonicHistoryGeometry> for GeometryWire {
    fn from(value: HarmonicHistoryGeometry) -> Self {
        Self {
            pivots: value.pivots.map(|p| PivotWire {
                bar_index: p.bar_index,
                high: p.high,
                observed_at: p.observed_at,
                available_at: p.available_at,
                confirmed_at: p.confirmed_at,
                price: p.price,
            }),
            ratios: value.ratios,
        }
    }
}

impl GeometryWire {
    fn decode(self) -> HarmonicHistoryGeometry {
        HarmonicHistoryGeometry {
            pivots: self.pivots.map(|p| HarmonicHistoryPivot {
                bar_index: p.bar_index,
                high: p.high,
                observed_at: p.observed_at,
                available_at: p.available_at,
                confirmed_at: p.confirmed_at,
                price: p.price,
            }),
            ratios: self.ratios,
        }
    }
}

impl From<&HarmonicHistoryAudit> for HarmonicHistoryWire {
    fn from(value: &HarmonicHistoryAudit) -> Self {
        let input = value.input();
        let disposition = match input.disposition {
            HarmonicHistoryDisposition::Pattern => DispositionWire::Pattern {
                geometry: match input.geometry {
                    Some(geometry) => Box::new(geometry.into()),
                    None => unreachable!("validated pattern audit retains exact geometry"),
                },
                digest: match input.pattern_digest {
                    Some(digest) => digest,
                    None => {
                        unreachable!("validated pattern audit always retains its pattern digest")
                    }
                },
            },
            HarmonicHistoryDisposition::InsufficientBars => DispositionWire::InsufficientBars,
            HarmonicHistoryDisposition::InsufficientPivots => DispositionWire::InsufficientPivots,
            HarmonicHistoryDisposition::NoMatchingPattern => DispositionWire::NoMatchingPattern,
            HarmonicHistoryDisposition::Expired => DispositionWire::Expired,
            HarmonicHistoryDisposition::Invalidated => DispositionWire::Invalidated,
        };
        Self {
            instrument_id: input.instrument_id,
            currency: input.currency,
            execution_tick: input.execution_tick,
            analytical_tick: input.analytical_tick,
            source_cutoff: input.source_cutoff,
            observed_through: input.observed_through,
            observed_at: input.observed_at,
            available_at: input.available_at,
            rights_decision_identity: input.rights_decision_identity,
            rights_graph_identity: input.rights_graph_identity,
            rights_checked_at: input.rights_checked_at,
            rights_expires_at: input.rights_expires_at,
            evaluated_at: input.evaluated_at,
            source_identity: input.source_identity,
            selected_manifest: input.selected_manifest,
            origin_manifest: input.origin_manifest,
            adjustment_identity: input.adjustment_identity,
            calendar_identity: input.calendar_identity,
            completeness_identity: input.completeness_identity,
            marketability_identity: input.marketability_identity,
            implementation_identity: input.implementation_identity,
            materialized_bars: input.materialized_bars,
            start_ordinal: input.start_ordinal,
            evaluated_bars: input.evaluated_bars,
            disposition,
            evaluation_digest: value.digest(),
        }
    }
}

impl HarmonicHistoryWire {
    pub(super) fn decode(self) -> Result<HarmonicHistoryAudit, DecisionApplicationError> {
        let (disposition, pattern_digest, geometry) = match self.disposition {
            DispositionWire::Pattern { digest, geometry } => (
                HarmonicHistoryDisposition::Pattern,
                Some(digest),
                Some((*geometry).decode()),
            ),
            DispositionWire::InsufficientBars => {
                (HarmonicHistoryDisposition::InsufficientBars, None, None)
            }
            DispositionWire::InsufficientPivots => {
                (HarmonicHistoryDisposition::InsufficientPivots, None, None)
            }
            DispositionWire::NoMatchingPattern => {
                (HarmonicHistoryDisposition::NoMatchingPattern, None, None)
            }
            DispositionWire::Expired => (HarmonicHistoryDisposition::Expired, None, None),
            DispositionWire::Invalidated => (HarmonicHistoryDisposition::Invalidated, None, None),
        };
        let audit = HarmonicHistoryAudit::try_new(HarmonicHistoryAuditInput {
            instrument_id: self.instrument_id,
            currency: self.currency,
            execution_tick: self.execution_tick,
            analytical_tick: self.analytical_tick,
            source_cutoff: self.source_cutoff,
            observed_through: self.observed_through,
            observed_at: self.observed_at,
            available_at: self.available_at,
            rights_decision_identity: self.rights_decision_identity,
            rights_graph_identity: self.rights_graph_identity,
            rights_checked_at: self.rights_checked_at,
            rights_expires_at: self.rights_expires_at,
            evaluated_at: self.evaluated_at,
            source_identity: self.source_identity,
            selected_manifest: self.selected_manifest,
            origin_manifest: self.origin_manifest,
            adjustment_identity: self.adjustment_identity,
            calendar_identity: self.calendar_identity,
            completeness_identity: self.completeness_identity,
            marketability_identity: self.marketability_identity,
            implementation_identity: self.implementation_identity,
            materialized_bars: self.materialized_bars,
            start_ordinal: self.start_ordinal,
            evaluated_bars: self.evaluated_bars,
            disposition,
            pattern_digest,
            geometry,
        })
        .map_err(invalid)?;
        if audit.digest() != self.evaluation_digest {
            return Err(invalid(()));
        }
        Ok(audit)
    }
}

fn invalid<T>(_: T) -> DecisionApplicationError {
    DecisionApplicationError::InvalidPersistentState
}

/// Shares the exact proposal audit codec with historical instruction persistence.
pub(crate) fn encode_harmonic_history_audit(
    audit: &HarmonicHistoryAudit,
) -> Result<serde_json::Value, DecisionApplicationError> {
    serde_json::to_value(HarmonicHistoryWire::from(audit)).map_err(invalid)
}

/// Strictly recovers and rehashes the same bounded audit; it confers no source authority.
pub(crate) fn decode_harmonic_history_audit(
    value: serde_json::Value,
) -> Result<HarmonicHistoryAudit, DecisionApplicationError> {
    serde_json::from_value::<HarmonicHistoryWire>(value)
        .map_err(invalid)?
        .decode()
}
