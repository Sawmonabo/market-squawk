//! Source-authenticated input epochs retained inside the existing feature publication.

#[path = "epoch_history.rs"]
mod history;
pub use history::{
    ForecastBasisHistory, ForecastBasisHistoryRow, ForecastBasisOhlc,
    ForecastCurrentShareConversion, ShareConversionRounding,
};

use super::financial::FinancialSourceInputs;
use crate::{
    ComponentAdjustmentEvidence, CorporateActionAdjustment, CorporateActionPolicy,
    DatasetBuildError, DatasetId, DatasetManifestRef, DatasetSchemaRef, Sha256Digest,
};
use market_squawk_domain::{
    EvidenceDigest, InstrumentId, MarketBarAdjustment, MarketBarObservation, SchemaVersion,
    Timestamp,
};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

pub(crate) const MAX_INPUT_EPOCH_BYTES: usize = 64 * 1024;

/// One closed source branch under the same immutable dataset admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureDatasetInputEpoch {
    source: EpochSource,
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum EpochSource {
    CompletedBarClose(CompletedBarCloseInputEpoch),
    NamedSessionCloseForNominalDailyBar(CompletedBarCloseInputEpoch),
    FinancialPeriod(FinancialPeriodInputEpoch),
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct FinancialPeriodInputEpoch {
    wire: FinancialEpochWire,
    manifest: DatasetManifestRef,
    binding: super::FinancialFiscalTargetBinding,
    decision: market_squawk_domain::ResearchTemporalCoordinate,
    retained_bytes: usize,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FinancialEpochWire {
    example_id: String,
    instrument_id: InstrumentId,
    source_selection_as_of: Timestamp,
    decision_coordinate: market_squawk_domain::ResearchTemporalCoordinate,
    basis: market_squawk_domain::HistoricalStudyBasis,
    purpose: super::DatasetBuildPurpose,
    snapshot_as_of: Timestamp,
    source_snapshot_digest: [u8; 32],
    calculated_at: Timestamp,
    current_inputs:
        super::financial::FinancialSourceInputs<market_squawk_domain::FundamentalObservation>,
    current_anchor: market_squawk_domain::FundamentalObservation,
    selection: super::FinancialAmountSelection,
    source_manifest: EpochManifest,
    source_evidence: [u8; 32],
    point_in_time_content: [u8; 32],
    point_in_time_audit: [u8; 32],
    universe_content: [u8; 32],
    universe_audit: [u8; 32],
    population_basis: super::DatasetPopulationBasis,
    period: serde_json::Value,
}
impl Eq for FinancialEpochWire {}
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "input",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum EpochWire {
    CompletedBarClose(InputEpochWire),
    NamedSessionCloseForNominalDailyBar(InputEpochWire),
    FinancialPeriod(FinancialEpochWire),
}

impl FeatureDatasetInputEpoch {
    /// Exact bounded source-sealed bytes for persistence and equality revalidation.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DatasetBuildError> {
        self.encode()
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "source clocks and lineage stay independently bound"
    )]
    pub(super) fn from_selected(
        example: &super::DatasetExample,
        bar: &MarketBarObservation,
        manifest: &DatasetManifestRef,
        source_evidence: Sha256Digest,
        content: Sha256Digest,
        audit: Sha256Digest,
        universe_content: Sha256Digest,
        universe_audit: Sha256Digest,
        population_basis: super::DatasetPopulationBasis,
        adjustment: &ComponentAdjustmentEvidence,
        calculated_at: Timestamp,
        actions: &crate::CorporateActionPlan,
        study: super::DatasetStudyPolicy,
        snapshot: Sha256Digest,
    ) -> Result<Self, DatasetBuildError> {
        let value = CompletedBarCloseInputEpoch::from_selected(
            example,
            bar,
            manifest,
            source_evidence,
            content,
            audit,
            universe_content,
            universe_audit,
            population_basis,
            adjustment,
            calculated_at,
            actions,
            study,
            snapshot,
        )?;
        Ok(Self {
            source: if example.nominal_daily_source().is_some() {
                EpochSource::NamedSessionCloseForNominalDailyBar(value)
            } else {
                EpochSource::CompletedBarClose(value)
            },
        })
    }
    pub(super) fn from_financial(
        example: &super::DatasetExample,
        study: super::DatasetStudyPolicy,
        snapshot: Sha256Digest,
        universe_content: Sha256Digest,
        universe_audit: Sha256Digest,
        population_basis: super::DatasetPopulationBasis,
        calculated_at: Timestamp,
    ) -> Result<Self, DatasetBuildError> {
        let financial = example
            .financial_source()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        let source = &financial.source;
        let binding = &financial.binding;
        let current_inputs = binding
            .observed_inputs()
            .try_map(|row| source.observation(row.row_ordinal()))?;
        let current_anchor = source.observation(binding.duration_chain()[0].row_ordinal())?;
        let super::FeatureLabelMeasurement::FinancialAmount {
            role,
            basis,
            share_convention,
            ..
        } = source.measurement
        else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        };
        let wire = FinancialEpochWire {
            example_id: example.example_id().into(),
            instrument_id: example.instrument_id(),
            source_selection_as_of: example.source_selection_as_of(),
            decision_coordinate: example.decision_coordinate().clone(),
            basis: study.basis(),
            purpose: study.purpose(),
            snapshot_as_of: study.snapshot_as_of(),
            source_snapshot_digest: snapshot.bytes(),
            calculated_at,
            current_inputs,
            current_anchor,
            selection: super::FinancialAmountSelection {
                role,
                basis,
                share_convention,
            },
            source_manifest: EpochManifest::from_manifest(&source.manifest),
            source_evidence: source.source_receipt.result_digest().bytes(),
            point_in_time_content: source
                .source_receipt
                .point_in_time_content_identity()
                .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?
                .bytes(),
            point_in_time_audit: source.source_receipt.point_in_time_audit_identity().bytes(),
            universe_content: universe_content.bytes(),
            universe_audit: universe_audit.bytes(),
            population_basis,
            period: serde_json::to_value(binding)
                .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?,
        };
        Self::financial_from_wire(wire)
    }
    fn financial_from_wire(wire: FinancialEpochWire) -> Result<Self, DatasetBuildError> {
        let binding = super::FinancialFiscalTargetBinding::decode(wire.period.clone())?;
        let policy = super::DatasetStudyPolicy::try_new(
            wire.basis,
            wire.purpose,
            wire.snapshot_as_of,
            if wire.basis == market_squawk_domain::HistoricalStudyBasis::RetrospectiveFrozenSnapshot
            {
                Some(std::time::Duration::ZERO)
            } else {
                None
            },
            binding.target_horizon()?,
        )?;
        wire.population_basis.validate_study_policy(Some(&policy))?;
        let primary = wire.current_inputs.primary();
        // Compute the amount from original observations, never from a forged derived fact.
        wire.current_inputs.amount(wire.selection)?;
        if !matches!(
            (&wire.current_inputs, binding.observed_inputs()),
            (
                FinancialSourceInputs::Reported { .. },
                FinancialSourceInputs::Reported { .. }
            ) | (
                FinancialSourceInputs::CommonBookEquity { .. },
                FinancialSourceInputs::CommonBookEquity { .. }
            )
        ) {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        for (fact, reference) in wire
            .current_inputs
            .iter()
            .zip(binding.observed_source_rows())
        {
            super::financial::validate_fact(fact, wire.instrument_id, wire.source_selection_as_of)?;
            if fact.fact_context() != reference.fact_context()
                || fact.context().provenance().ingested_at() > wire.snapshot_as_of
            {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        }
        super::financial::validate_fact(
            &wire.current_anchor,
            wire.instrument_id,
            wire.source_selection_as_of,
        )?;
        if wire.example_id.is_empty()
            || wire.example_id.len() > 256
            || wire.source_selection_as_of > wire.snapshot_as_of
            || wire.snapshot_as_of > wire.calculated_at
            || wire.current_anchor.context().provenance().ingested_at() > wire.snapshot_as_of
            || wire.current_anchor.fact_context() != binding.duration_chain()[0].fact_context()
            || !super::financial::same_scope(primary, &wire.current_anchor)
            || !super::financial::anchor_matches(
                primary.fact_context(),
                wire.current_anchor.fact_context(),
            )
            || super::financial::source_currency(
                wire.current_anchor.unit().as_str(),
                wire.current_anchor.unit().as_str().ends_with("/shares"),
            )? != super::financial::source_currency(
                primary.unit().as_str(),
                wire.selection.basis == super::FinancialAmountBasis::PerCommonShare,
            )?
            || [
                wire.source_snapshot_digest,
                wire.source_evidence,
                wire.point_in_time_content,
                wire.point_in_time_audit,
                wire.universe_content,
                wire.universe_audit,
            ]
            .contains(&[0; 32])
            || wire.source_evidence != binding.source_selection_digest().bytes()
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        match policy.basis() {
            market_squawk_domain::HistoricalStudyBasis::HistoricalAsKnown => {
                let decision = wire
                    .decision_coordinate
                    .exact_timestamp()
                    .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
                if wire.source_selection_as_of > decision
                    || decision
                        .utc_calendar_date()
                        .map_err(|_| DatasetBuildError::TemporalLeakage)?
                        < binding.observed_period().end()
                    || binding.target_period().is_some_and(|period| {
                        decision
                            .utc_calendar_date()
                            .is_ok_and(|date| date >= period.end())
                    })
                {
                    return Err(DatasetBuildError::TemporalLeakage);
                }
            }
            market_squawk_domain::HistoricalStudyBasis::RetrospectiveFrozenSnapshot => {
                if wire.source_selection_as_of != wire.snapshot_as_of
                    || wire.decision_coordinate.calendar_date_value()
                        != Some(binding.observed_period().end())
                {
                    return Err(DatasetBuildError::TemporalLeakage);
                }
            }
        }
        let manifest = wire.source_manifest.decode()?;
        let decision = wire.decision_coordinate.clone();
        let bytes =
            serde_json::to_vec(&wire).map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
        if bytes.len() > MAX_INPUT_EPOCH_BYTES {
            return Err(DatasetBuildError::LimitExceeded);
        }
        let retained_bytes = std::mem::size_of::<Self>()
            .checked_add(bytes.len() * 8)
            .ok_or(DatasetBuildError::LimitExceeded)?;
        Ok(Self {
            source: EpochSource::FinancialPeriod(FinancialPeriodInputEpoch {
                wire,
                manifest,
                binding,
                decision,
                retained_bytes,
            }),
        })
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DatasetBuildError> {
        if bytes.is_empty() || bytes.len() > MAX_INPUT_EPOCH_BYTES {
            return Err(DatasetBuildError::LimitExceeded);
        }
        let wire: EpochWire = serde_json::from_slice(bytes)
            .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
        let value = match wire {
            EpochWire::CompletedBarClose(wire) => Self {
                source: EpochSource::CompletedBarClose(CompletedBarCloseInputEpoch::from_wire(
                    wire,
                    super::FixedHorizonOriginBasis::CompletedBarClose,
                )?),
            },
            EpochWire::NamedSessionCloseForNominalDailyBar(wire) => Self {
                source: EpochSource::NamedSessionCloseForNominalDailyBar(
                    CompletedBarCloseInputEpoch::from_wire(
                        wire,
                        super::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar,
                    )?,
                ),
            },
            EpochWire::FinancialPeriod(wire) => Self::financial_from_wire(wire)?,
        };
        if value.encode()?.as_slice() != bytes {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        Ok(value)
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>, DatasetBuildError> {
        #[derive(Serialize)]
        #[serde(tag = "kind", content = "input", rename_all = "snake_case")]
        enum Ref<'a> {
            CompletedBarClose(&'a InputEpochWire),
            NamedSessionCloseForNominalDailyBar(&'a InputEpochWire),
            FinancialPeriod(&'a FinancialEpochWire),
        }
        let wire = match &self.source {
            EpochSource::CompletedBarClose(v) => Ref::CompletedBarClose(&v.wire),
            EpochSource::NamedSessionCloseForNominalDailyBar(v) => {
                Ref::NamedSessionCloseForNominalDailyBar(&v.wire)
            }
            EpochSource::FinancialPeriod(v) => Ref::FinancialPeriod(&v.wire),
        };
        let bytes =
            serde_json::to_vec(&wire).map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
        if bytes.len() > MAX_INPUT_EPOCH_BYTES {
            return Err(DatasetBuildError::LimitExceeded);
        }
        Ok(bytes)
    }
    pub fn example_id(&self) -> &str {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.example_id(),
            EpochSource::FinancialPeriod(v) => &v.wire.example_id,
        }
    }
    pub fn instrument_id(&self) -> InstrumentId {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.instrument_id(),
            EpochSource::FinancialPeriod(v) => v.wire.instrument_id,
        }
    }
    pub fn source_selection_as_of(&self) -> Timestamp {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.source_selection_as_of(),
            EpochSource::FinancialPeriod(v) => v.wire.source_selection_as_of,
        }
    }
    pub fn decision_at(&self) -> Option<Timestamp> {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => Some(v.decision_at()),
            EpochSource::FinancialPeriod(v) => v.decision.exact_timestamp(),
        }
    }
    pub fn decision_coordinate(&self) -> &market_squawk_domain::ResearchTemporalCoordinate {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => &v.decision,
            EpochSource::FinancialPeriod(v) => &v.decision,
        }
    }
    pub fn target_origin(&self) -> Option<Timestamp> {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => Some(v.target_origin()),
            EpochSource::FinancialPeriod(_) => None,
        }
    }
    pub fn target_at(&self) -> Option<Timestamp> {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => Some(v.target_at()),
            EpochSource::FinancialPeriod(_) => None,
        }
    }
    pub fn basis(&self) -> market_squawk_domain::HistoricalStudyBasis {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.basis(),
            EpochSource::FinancialPeriod(v) => v.wire.basis,
        }
    }
    /// Returns the genuine population qualification bound into this source epoch.
    pub fn population_basis(&self) -> super::DatasetPopulationBasis {
        match &self.source {
            EpochSource::CompletedBarClose(value)
            | EpochSource::NamedSessionCloseForNominalDailyBar(value) => {
                value.wire.population_basis
            }
            EpochSource::FinancialPeriod(value) => value.wire.population_basis,
        }
    }
    pub fn purpose(&self) -> super::DatasetBuildPurpose {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.purpose(),
            EpochSource::FinancialPeriod(v) => v.wire.purpose,
        }
    }
    pub fn snapshot_as_of(&self) -> Timestamp {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.snapshot_as_of(),
            EpochSource::FinancialPeriod(v) => v.wire.snapshot_as_of,
        }
    }
    pub fn source_snapshot_digest(&self) -> Sha256Digest {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.source_snapshot_digest(),
            EpochSource::FinancialPeriod(v) => Sha256Digest::new(v.wire.source_snapshot_digest),
        }
    }
    pub fn calculated_at(&self) -> Timestamp {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.calculated_at(),
            EpochSource::FinancialPeriod(v) => v.wire.calculated_at,
        }
    }
    pub fn source_manifest(&self) -> &DatasetManifestRef {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.source_manifest(),
            EpochSource::FinancialPeriod(v) => &v.manifest,
        }
    }
    pub fn market_bar(&self) -> Option<&MarketBarObservation> {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => Some(v.market_bar()),
            EpochSource::FinancialPeriod(_) => None,
        }
    }
    /// Exact origin qualification carried by the authenticated source branch.
    pub fn fixed_horizon_origin_basis(&self) -> Option<super::FixedHorizonOriginBasis> {
        match &self.source {
            EpochSource::CompletedBarClose(_) => {
                Some(super::FixedHorizonOriginBasis::CompletedBarClose)
            }
            EpochSource::NamedSessionCloseForNominalDailyBar(_) => {
                Some(super::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar)
            }
            EpochSource::FinancialPeriod(_) => None,
        }
    }
    pub fn named_session_origin(&self) -> Option<&super::nominal_daily::NamedSessionDailyOrigin> {
        match &self.source {
            EpochSource::NamedSessionCloseForNominalDailyBar(value) => {
                value.wire.named_session_origin.as_ref()
            }
            _ => None,
        }
    }
    pub fn financial_period(&self) -> Option<&super::FinancialFiscalTargetBinding> {
        match &self.source {
            EpochSource::FinancialPeriod(v) => Some(&v.binding),
            _ => None,
        }
    }
    pub fn financial_measurement(&self) -> Option<super::FeatureLabelMeasurement> {
        match &self.source {
            EpochSource::FinancialPeriod(v) => super::financial::source_currency(
                v.wire.current_inputs.primary().unit().as_str(),
                v.wire.selection.basis == super::FinancialAmountBasis::PerCommonShare,
            )
            .ok()
            .map(|c| v.wire.selection.measurement(c)),
            _ => None,
        }
    }
    /// Returns one original observation only for a direct source-reported amount.
    pub fn financial_current_fact(&self) -> Option<&market_squawk_domain::FundamentalObservation> {
        match &self.source {
            EpochSource::FinancialPeriod(v) => match &v.wire.current_inputs {
                super::financial::FinancialSourceInputs::Reported { amount } => Some(amount),
                super::financial::FinancialSourceInputs::CommonBookEquity { .. } => None,
            },
            EpochSource::CompletedBarClose(_)
            | EpochSource::NamedSessionCloseForNominalDailyBar(_) => None,
        }
    }
    /// Recomputes the exact signed current amount from the sealed original source inputs.
    pub fn current_financial_amount(&self) -> Result<rust_decimal::Decimal, DatasetBuildError> {
        match &self.source {
            EpochSource::FinancialPeriod(v) => v.wire.current_inputs.amount(v.wire.selection),
            EpochSource::CompletedBarClose(_)
            | EpochSource::NamedSessionCloseForNominalDailyBar(_) => {
                Err(DatasetBuildError::ComponentEvidenceMismatch)
            }
        }
    }
    pub fn financial_current_inputs(
        &self,
    ) -> impl Iterator<Item = &market_squawk_domain::FundamentalObservation> {
        let inputs = match &self.source {
            EpochSource::FinancialPeriod(v) => Some(&v.wire.current_inputs),
            EpochSource::CompletedBarClose(_)
            | EpochSource::NamedSessionCloseForNominalDailyBar(_) => None,
        };
        inputs
            .into_iter()
            .flat_map(super::financial::FinancialSourceInputs::iter)
    }
    pub fn retained_bytes(&self) -> usize {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.retained_bytes(),
            EpochSource::FinancialPeriod(v) => v.retained_bytes,
        }
    }
    pub fn current_unit_price(&self) -> Result<market_squawk_domain::Money, DatasetBuildError> {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.current_unit_price(),
            EpochSource::FinancialPeriod(_) => Err(DatasetBuildError::ComponentEvidenceMismatch),
        }
    }
    pub fn adjustment(&self) -> Option<&ComponentAdjustmentEvidence> {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => Some(v.adjustment()),
            EpochSource::FinancialPeriod(_) => None,
        }
    }
    pub fn source_evidence_digest(&self) -> Sha256Digest {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.source_evidence_digest(),
            EpochSource::FinancialPeriod(v) => Sha256Digest::new(v.wire.source_evidence),
        }
    }
    pub fn point_in_time_content(&self) -> Sha256Digest {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.point_in_time_content(),
            EpochSource::FinancialPeriod(v) => Sha256Digest::new(v.wire.point_in_time_content),
        }
    }
    pub fn point_in_time_audit(&self) -> Sha256Digest {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.point_in_time_audit(),
            EpochSource::FinancialPeriod(v) => Sha256Digest::new(v.wire.point_in_time_audit),
        }
    }
    pub fn universe_content(&self) -> Sha256Digest {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.universe_content(),
            EpochSource::FinancialPeriod(v) => Sha256Digest::new(v.wire.universe_content),
        }
    }
    pub fn universe_audit(&self) -> Sha256Digest {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.universe_audit(),
            EpochSource::FinancialPeriod(v) => Sha256Digest::new(v.wire.universe_audit),
        }
    }
    pub(crate) fn study_policy(&self) -> Result<super::DatasetStudyPolicy, DatasetBuildError> {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => v.study_policy(),
            EpochSource::FinancialPeriod(v) => super::DatasetStudyPolicy::try_new(
                v.wire.basis,
                v.wire.purpose,
                v.wire.snapshot_as_of,
                if v.wire.basis
                    == market_squawk_domain::HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                {
                    Some(std::time::Duration::ZERO)
                } else {
                    None
                },
                v.binding.target_horizon()?,
            ),
        }
    }
    pub fn limitations(&self) -> &[market_squawk_domain::HistoricalStudyLimitation] {
        // The code-owned closed qualification set is static for both source branches.
        super::study::limitations(self.basis())
    }
    pub(crate) fn validate_label_selection(
        &self,
        known: Option<Timestamp>,
        kind: u8,
        missing: bool,
    ) -> Result<(), DatasetBuildError> {
        match &self.source {
            EpochSource::CompletedBarClose(v)
            | EpochSource::NamedSessionCloseForNominalDailyBar(v) => {
                v.validate_label_selection(known, kind, missing)
            }
            EpochSource::FinancialPeriod(v) => match self.purpose() {
                super::DatasetBuildPurpose::StudyInputs if known.is_none() && kind == 1 => Ok(()),
                super::DatasetBuildPurpose::Training => {
                    let known = known.ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
                    if known>self.snapshot_as_of() || (kind==2 && missing)
                        || v.binding.target_period().is_none_or(|p|known.utc_calendar_date().map_or(true,|d|p.end()>d))
                        || match self.basis(){market_squawk_domain::HistoricalStudyBasis::HistoricalAsKnown=>known<=self.source_selection_as_of(),market_squawk_domain::HistoricalStudyBasis::RetrospectiveFrozenSnapshot=>known!=self.snapshot_as_of()} {
                        Err(DatasetBuildError::ComponentEvidenceMismatch)
                    }else{Ok(())}
                }
                _ => Err(DatasetBuildError::ComponentEvidenceMismatch),
            },
        }
    }
}

/// Original completed-close input evidence. Construction is private to the checked data builder.
#[derive(Clone, Debug, Eq, PartialEq)]
struct CompletedBarCloseInputEpoch {
    decision: market_squawk_domain::ResearchTemporalCoordinate,
    wire: InputEpochWire,
    source_manifest: DatasetManifestRef,
    adjustment: ComponentAdjustmentEvidence,
    retained_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InputEpochWire {
    example_id: String,
    instrument_id: InstrumentId,
    source_selection_as_of: Timestamp,
    decision_at: Timestamp,
    basis: market_squawk_domain::HistoricalStudyBasis,
    purpose: super::DatasetBuildPurpose,
    snapshot_as_of: Timestamp,
    source_snapshot_digest: [u8; 32],
    limitations: Vec<market_squawk_domain::HistoricalStudyLimitation>,
    decision_lag_nanos: Option<u64>,
    target_horizon_nanos: u64,
    target_origin: Timestamp,
    target_at: Timestamp,
    calculated_at: Timestamp,
    market_bar: MarketBarObservation,
    #[serde(deserialize_with = "required_named_session_origin")]
    named_session_origin: Option<super::nominal_daily::NamedSessionDailyOrigin>,
    source_manifest: EpochManifest,
    source_evidence: [u8; 32],
    point_in_time_content: [u8; 32],
    point_in_time_audit: [u8; 32],
    universe_content: [u8; 32],
    universe_audit: [u8; 32],
    population_basis: super::DatasetPopulationBasis,
    adjustment_plan_content: [u8; 32],
    adjustment_plan_audit: [u8; 32],
    adjustment_implementation: EvidenceDigest,
    origin_price_factors: Vec<(u32, u32)>,
}

fn required_named_session_origin<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<super::nominal_daily::NamedSessionDailyOrigin>, D::Error> {
    Option::deserialize(deserializer)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EpochManifest {
    dataset_id: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_sha256: [u8; 32],
    manifest_sha256: [u8; 32],
}

impl EpochManifest {
    fn from_manifest(value: &DatasetManifestRef) -> Self {
        Self {
            dataset_id: value.dataset_id().as_str().into(),
            manifest_version: value.manifest_version(),
            schema_name: value.schema().name().into(),
            schema_version: value.schema_version().get(),
            schema_sha256: value.schema().fingerprint(),
            manifest_sha256: value.content_hash().bytes(),
        }
    }
    fn decode(&self) -> Result<DatasetManifestRef, DatasetBuildError> {
        let schema = DatasetSchemaRef::try_new(
            &self.schema_name,
            SchemaVersion::new(self.schema_version)
                .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?,
            self.schema_sha256,
        )
        .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset_id.as_str())
                .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?,
            self.manifest_version,
            schema,
            Sha256Digest::new(self.manifest_sha256),
        )
        .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)
    }
}

impl CompletedBarCloseInputEpoch {
    #[allow(
        clippy::too_many_arguments,
        reason = "every source clock and lineage remains independently bound"
    )]
    pub(super) fn from_selected(
        example: &super::DatasetExample,
        bar: &MarketBarObservation,
        source_manifest: &DatasetManifestRef,
        source_evidence: Sha256Digest,
        point_in_time_content: Sha256Digest,
        point_in_time_audit: Sha256Digest,
        universe_content: Sha256Digest,
        universe_audit: Sha256Digest,
        population_basis: super::DatasetPopulationBasis,
        adjustment: &ComponentAdjustmentEvidence,
        calculated_at: Timestamp,
        action_plan: &crate::CorporateActionPlan,
        study: super::DatasetStudyPolicy,
        source_snapshot: Sha256Digest,
    ) -> Result<Self, DatasetBuildError> {
        let ComponentAdjustmentEvidence::Applied {
            policy,
            plan_content,
            plan_audit,
            implementation_evidence,
        } = adjustment
        else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        };
        if *policy
            != CorporateActionPolicy::new(CorporateActionAdjustment::SplitAdjusted, NonZeroU32::MIN)
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let target_origin = example
            .exact_target_coordinates()
            .map(|value| value.0)
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        let target_at = example
            .label_effective_cutoff()
            .and_then(market_squawk_domain::ResearchTemporalCoordinate::exact_timestamp)
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        let mut origin_price_factors = Vec::new();
        for step in action_plan.steps() {
            let crate::AdjustmentStep::Split {
                admitted_index,
                price_factor,
                ..
            } = step
            else {
                return Err(DatasetBuildError::ComponentAdjustmentMismatch);
            };
            let action = action_plan
                .admitted()
                .get(*admitted_index)
                .ok_or(DatasetBuildError::ComponentAdjustmentMismatch)?;
            if action.observation().context().provenance().instrument_id()
                == Some(example.instrument_id())
            {
                let effective = action
                    .application_at()
                    .ok_or(DatasetBuildError::ComponentAdjustmentMismatch)?;
                if target_origin <= effective {
                    if Some(effective) > example.decision_at() {
                        return Err(DatasetBuildError::ComponentAdjustmentMismatch);
                    }
                    if origin_price_factors.len() >= 1_024 {
                        return Err(DatasetBuildError::LimitExceeded);
                    }
                    origin_price_factors
                        .try_reserve(1)
                        .map_err(|_| DatasetBuildError::LimitExceeded)?;
                    origin_price_factors.push((
                        price_factor.numerator().get(),
                        price_factor.denominator().get(),
                    ));
                }
            }
        }
        let wire = InputEpochWire {
            example_id: example.example_id().into(),
            instrument_id: example.instrument_id(),
            source_selection_as_of: example.source_selection_as_of(),
            decision_at: example
                .decision_at()
                .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?,
            basis: study.basis(),
            purpose: study.purpose(),
            snapshot_as_of: study.snapshot_as_of(),
            source_snapshot_digest: source_snapshot.bytes(),
            limitations: study.limitations().to_vec(),
            decision_lag_nanos: study.decision_lag().map(|lag| lag.as_nanos() as u64),
            target_horizon_nanos: study
                .target_horizon()
                .exact_elapsed()
                .ok_or(DatasetBuildError::InvalidRequest)?
                .as_nanos() as u64,
            target_origin,
            target_at,
            calculated_at,
            market_bar: bar.clone(),
            named_session_origin: example.named_session_origin().cloned(),
            source_manifest: EpochManifest::from_manifest(source_manifest),
            source_evidence: source_evidence.bytes(),
            point_in_time_content: point_in_time_content.bytes(),
            point_in_time_audit: point_in_time_audit.bytes(),
            universe_content: universe_content.bytes(),
            universe_audit: universe_audit.bytes(),
            population_basis,
            adjustment_plan_content: plan_content.bytes(),
            adjustment_plan_audit: plan_audit.bytes(),
            adjustment_implementation: *implementation_evidence,
            origin_price_factors,
        };
        let origin_basis = if example.nominal_daily_source().is_some() {
            super::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar
        } else {
            super::FixedHorizonOriginBasis::CompletedBarClose
        };
        Self::from_wire(wire, origin_basis)
    }

    fn from_wire(
        wire: InputEpochWire,
        origin_basis: super::FixedHorizonOriginBasis,
    ) -> Result<Self, DatasetBuildError> {
        let bar = &wire.market_bar;
        let provenance = bar.context().provenance();
        let known = provenance
            .availability()
            .conservative_available_at()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        if wire.example_id.is_empty()
            || wire.example_id.len() > 256
            || provenance.instrument_id() != Some(wire.instrument_id)
            || bar.adjustment() != MarketBarAdjustment::Raw
            || match origin_basis {
                super::FixedHorizonOriginBasis::CompletedBarClose => {
                    wire.named_session_origin.is_some()
                        || bar.completed_at() != Some(wire.target_origin)
                }
                super::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar => wire
                    .named_session_origin
                    .is_none()
                    || bar.completed_at().is_some()
                    || !(wire.basis
                        == market_squawk_domain::HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                        || (wire.basis
                            == market_squawk_domain::HistoricalStudyBasis::HistoricalAsKnown
                            && wire.purpose == super::DatasetBuildPurpose::StudyInputs
                            && wire.source_selection_as_of == wire.snapshot_as_of
                            && wire.decision_at == wire.snapshot_as_of)),
                super::FixedHorizonOriginBasis::ExactEffectiveTimestamp => true,
            }
            || wire.target_at <= wire.target_origin
            || wire.origin_price_factors.len() > 1_024
            || wire.target_origin > known
            || known > wire.source_selection_as_of
            || wire.source_selection_as_of > wire.snapshot_as_of
            || wire.snapshot_as_of > wire.calculated_at
            || provenance.ingested_at() > wire.snapshot_as_of
            || wire.source_snapshot_digest == [0; 32]
            || [
                wire.source_evidence,
                wire.point_in_time_content,
                wire.point_in_time_audit,
                wire.universe_content,
                wire.universe_audit,
                wire.adjustment_plan_content,
                wire.adjustment_plan_audit,
                wire.adjustment_implementation.bytes(),
            ]
            .contains(&[0; 32])
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let policy = super::DatasetStudyPolicy::try_new(
            wire.basis,
            wire.purpose,
            wire.snapshot_as_of,
            wire.decision_lag_nanos.map(std::time::Duration::from_nanos),
            super::DatasetTargetHorizon::ExactElapsed(std::time::Duration::from_nanos(
                wire.target_horizon_nanos,
            )),
        )?;
        wire.population_basis.validate_study_policy(Some(&policy))?;
        if (wire.population_basis == super::DatasetPopulationBasis::CurrentListedSnapshot
            && wire.source_selection_as_of != wire.decision_at)
            || wire.limitations != policy.limitations()
            || wire.target_origin.unix_nanos().checked_add(
                i64::try_from(wire.target_horizon_nanos)
                    .map_err(|_| DatasetBuildError::InvalidRequest)?,
            ) != Some(wire.target_at.unix_nanos())
            || wire.decision_at < wire.target_origin
            || wire.decision_at >= wire.target_at
            || match wire.basis {
                market_squawk_domain::HistoricalStudyBasis::HistoricalAsKnown => {
                    wire.source_selection_as_of > wire.decision_at
                }
                market_squawk_domain::HistoricalStudyBasis::RetrospectiveFrozenSnapshot => {
                    wire.source_selection_as_of != wire.snapshot_as_of
                        || wire
                            .decision_lag_nanos
                            .and_then(|lag| i64::try_from(lag).ok())
                            .and_then(|lag| wire.target_origin.unix_nanos().checked_add(lag))
                            != Some(wire.decision_at.unix_nanos())
                }
            }
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let source_manifest = wire.source_manifest.decode()?;
        if let Some(origin) = &wire.named_session_origin {
            origin.validate(bar, &source_manifest, policy, wire.target_at)?;
            if origin.closes_at_exclusive() != wire.target_origin {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        } else if wire.population_basis == super::DatasetPopulationBasis::PresentDayFixedCohort {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let adjustment = ComponentAdjustmentEvidence::try_applied(
            CorporateActionPolicy::new(CorporateActionAdjustment::SplitAdjusted, NonZeroU32::MIN),
            Sha256Digest::new(wire.adjustment_plan_content),
            Sha256Digest::new(wire.adjustment_plan_audit),
            wire.adjustment_implementation,
        )?;
        let encoded =
            serde_json::to_vec(&wire).map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
        if encoded.len() > MAX_INPUT_EPOCH_BYTES {
            return Err(DatasetBuildError::LimitExceeded);
        }
        let retained_bytes = encoded
            .len()
            .checked_mul(8)
            .ok_or(DatasetBuildError::LimitExceeded)?;
        let value = Self {
            decision: market_squawk_domain::ResearchTemporalCoordinate::exact(wire.decision_at),
            wire,
            source_manifest,
            adjustment,
            retained_bytes,
        };
        value.current_unit_price()?;
        Ok(value)
    }

    pub(crate) fn study_policy(&self) -> Result<super::DatasetStudyPolicy, DatasetBuildError> {
        super::DatasetStudyPolicy::try_new(
            self.basis(),
            self.purpose(),
            self.snapshot_as_of(),
            self.wire
                .decision_lag_nanos
                .map(std::time::Duration::from_nanos),
            super::DatasetTargetHorizon::ExactElapsed(std::time::Duration::from_nanos(
                self.wire.target_horizon_nanos,
            )),
        )
    }
    pub(crate) fn validate_label_selection(
        &self,
        label_selection: Option<Timestamp>,
        component_kind: u8,
        missing: bool,
    ) -> Result<(), DatasetBuildError> {
        match self.purpose() {
            super::DatasetBuildPurpose::StudyInputs
                if label_selection.is_none() && component_kind == 1 =>
            {
                Ok(())
            }
            super::DatasetBuildPurpose::Training => {
                let known = label_selection.ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
                if known < self.target_at()
                    || known > self.snapshot_as_of()
                    || self.target_at() > self.snapshot_as_of()
                    || (component_kind == 2 && missing)
                    || match self.basis() {
                        market_squawk_domain::HistoricalStudyBasis::HistoricalAsKnown => {
                            known <= self.source_selection_as_of()
                        }
                        market_squawk_domain::HistoricalStudyBasis::RetrospectiveFrozenSnapshot => {
                            known != self.snapshot_as_of()
                        }
                    }
                {
                    Err(DatasetBuildError::ComponentEvidenceMismatch)
                } else {
                    Ok(())
                }
            }
            _ => Err(DatasetBuildError::ComponentEvidenceMismatch),
        }
    }
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    pub fn example_id(&self) -> &str {
        &self.wire.example_id
    }
    pub const fn instrument_id(&self) -> InstrumentId {
        self.wire.instrument_id
    }
    pub const fn source_selection_as_of(&self) -> Timestamp {
        self.wire.source_selection_as_of
    }
    pub const fn decision_at(&self) -> Timestamp {
        self.wire.decision_at
    }
    pub const fn basis(&self) -> market_squawk_domain::HistoricalStudyBasis {
        self.wire.basis
    }
    pub const fn purpose(&self) -> super::DatasetBuildPurpose {
        self.wire.purpose
    }
    pub const fn snapshot_as_of(&self) -> Timestamp {
        self.wire.snapshot_as_of
    }
    pub const fn source_snapshot_digest(&self) -> Sha256Digest {
        Sha256Digest::new(self.wire.source_snapshot_digest)
    }
    pub fn limitations(&self) -> &[market_squawk_domain::HistoricalStudyLimitation] {
        &self.wire.limitations
    }
    pub const fn target_origin(&self) -> Timestamp {
        self.wire.target_origin
    }
    pub const fn target_at(&self) -> Timestamp {
        self.wire.target_at
    }
    pub const fn calculated_at(&self) -> Timestamp {
        self.wire.calculated_at
    }
    pub const fn market_bar(&self) -> &MarketBarObservation {
        &self.wire.market_bar
    }
    pub const fn source_manifest(&self) -> &DatasetManifestRef {
        &self.source_manifest
    }
    pub const fn source_evidence_digest(&self) -> Sha256Digest {
        Sha256Digest::new(self.wire.source_evidence)
    }
    pub const fn point_in_time_content(&self) -> Sha256Digest {
        Sha256Digest::new(self.wire.point_in_time_content)
    }
    pub const fn point_in_time_audit(&self) -> Sha256Digest {
        Sha256Digest::new(self.wire.point_in_time_audit)
    }
    pub const fn universe_content(&self) -> Sha256Digest {
        Sha256Digest::new(self.wire.universe_content)
    }
    pub const fn universe_audit(&self) -> Sha256Digest {
        Sha256Digest::new(self.wire.universe_audit)
    }
    /// Returns the raw completed close transformed into the units of the authentic feature
    /// decision's split plan, including a split at the bar's exclusive completion boundary.
    pub fn current_unit_price(&self) -> Result<market_squawk_domain::Money, DatasetBuildError> {
        let mut amount = self.market_bar().close().amount();
        for (numerator, denominator) in &self.wire.origin_price_factors {
            if *numerator == 0 || *denominator == 0 {
                return Err(DatasetBuildError::ComponentAdjustmentMismatch);
            }
            amount = amount
                .checked_mul(rust_decimal::Decimal::from(*numerator))
                .and_then(|value| value.checked_div(rust_decimal::Decimal::from(*denominator)))
                .ok_or(DatasetBuildError::ComponentAdjustmentMismatch)?;
        }
        if amount <= rust_decimal::Decimal::ZERO {
            return Err(DatasetBuildError::ComponentAdjustmentMismatch);
        }
        Ok(market_squawk_domain::Money::new(
            amount.normalize(),
            self.market_bar().currency(),
        ))
    }

    pub const fn adjustment(&self) -> &ComponentAdjustmentEvidence {
        &self.adjustment
    }
}
