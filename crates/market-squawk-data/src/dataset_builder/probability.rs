//! Closed economic meanings for producer-derived binary event labels.
//!
//! These values describe an event; deserializing them grants no dataset, source or execution
//! authority. The existing composition-owned publisher must prove the original source derivation.

use market_squawk_domain::{Currency, DigestAlgorithm, EvidenceDigest, InstrumentId};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::DatasetBuildError;
use crate::Sha256Digest;

/// Exact source basis of the existing research execution simulator.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbabilityExecutionBasisV1 {
    ObservedQuoteDepth,
    CompletedDailyBar,
}

/// Existing deterministic depth precedence; no caller-selected alternative is admitted.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbabilityLiquidityPriorityV1 {
    SignalTimeThenOrderId,
}

/// Long entry-to-exit total wealth, including genuine distributions still owed at the exit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbabilityRoundTripConventionV1 {
    LongRoundTripTotalWealthIncludingEntitlements,
}

/// Complete immutable economic policy carried by an after-cost event target.
///
/// The simulation owner independently reconstructs its existing validated policy from these
/// fields. The descriptor is an inert commitment, never permission to simulate an invented fill.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProbabilityCostPolicyV1 {
    pub version: u32,
    pub execution_policy_version: u32,
    pub fee_basis_points: i32,
    pub slippage_basis_points: i32,
    pub maximum_random_slippage_basis_points: i32,
    pub maximum_participation_basis_points: i32,
    pub latency_nanos: i64,
    pub allow_partial_fills: bool,
    pub fee_decimal_scale: u32,
    pub reporting_currency: Currency,
    pub quantity_lots: i64,
    pub maximum_entry_lag_nanos: i64,
    pub maximum_exit_lag_nanos: i64,
    pub seed: u64,
    pub execution_basis: ProbabilityExecutionBasisV1,
    pub daily_bar_assumed_spread_basis_points: Option<i32>,
    pub liquidity_priority: ProbabilityLiquidityPriorityV1,
    pub convention: ProbabilityRoundTripConventionV1,
}

impl ProbabilityCostPolicyV1 {
    /// Rejects malformed persisted terms. The actual simulator additionally validates its policy.
    pub fn validate(&self) -> Result<(), DatasetBuildError> {
        if self.version != 1
            || self.execution_policy_version != 3
            || [
                self.fee_basis_points,
                self.slippage_basis_points,
                self.maximum_random_slippage_basis_points,
                self.maximum_participation_basis_points,
            ]
            .into_iter()
            .any(|value| !(0..=10_000).contains(&value))
            || self.maximum_participation_basis_points == 0
            || self.latency_nanos <= 0
            || self.fee_decimal_scale > 28
            || self.quantity_lots <= 0
            || self.maximum_entry_lag_nanos < self.latency_nanos
            || self.maximum_exit_lag_nanos < self.latency_nanos
            || !matches!(
                (
                    self.execution_basis,
                    self.daily_bar_assumed_spread_basis_points
                ),
                (ProbabilityExecutionBasisV1::ObservedQuoteDepth, None)
                    | (
                        ProbabilityExecutionBasisV1::CompletedDailyBar,
                        Some(0..=10_000)
                    )
            )
        {
            return Err(DatasetBuildError::InvalidRequest);
        }
        Ok(())
    }

    /// Stable allocation-free identity of all economic terms, independent of one sample's rows.
    pub fn digest(&self) -> Sha256Digest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/probability-cost-policy/v1\0");
        hash.update(self.version.to_be_bytes());
        hash.update(self.execution_policy_version.to_be_bytes());
        for value in [
            self.fee_basis_points,
            self.slippage_basis_points,
            self.maximum_random_slippage_basis_points,
            self.maximum_participation_basis_points,
        ] {
            hash.update(value.to_be_bytes());
        }
        hash.update(self.latency_nanos.to_be_bytes());
        hash.update([u8::from(self.allow_partial_fills)]);
        hash.update(self.fee_decimal_scale.to_be_bytes());
        hash.update(self.reporting_currency.as_str().as_bytes());
        hash.update(self.quantity_lots.to_be_bytes());
        hash.update(self.maximum_entry_lag_nanos.to_be_bytes());
        hash.update(self.maximum_exit_lag_nanos.to_be_bytes());
        hash.update(self.seed.to_be_bytes());
        hash.update([match self.execution_basis {
            ProbabilityExecutionBasisV1::ObservedQuoteDepth => 1,
            ProbabilityExecutionBasisV1::CompletedDailyBar => 2,
        }]);
        match self.daily_bar_assumed_spread_basis_points {
            None => hash.update([0]),
            Some(value) => {
                hash.update([1]);
                hash.update(value.to_be_bytes());
            }
        }
        hash.update([match self.liquidity_priority {
            ProbabilityLiquidityPriorityV1::SignalTimeThenOrderId => 1,
        }]);
        hash.update([match self.convention {
            ProbabilityRoundTripConventionV1::LongRoundTripTotalWealthIncludingEntitlements => 1,
        }]);
        Sha256Digest::new(hash.finalize().into())
    }
}

/// Three distinct owner-selected events. Equal outcomes are false; unavailable is not false.
///
/// Price events use split-adjusted price returns, excluding cash distributions. The after-cost
/// event uses the original simulator's total-wealth accounting under the complete retained policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProbabilityEventTarget {
    PriceHigher,
    BenchmarkOutperformance {
        benchmark_instrument_id: InstrumentId,
        benchmark_definition: EvidenceDigest,
    },
    ProfitAfterCosts {
        policy: ProbabilityCostPolicyV1,
    },
}

impl ProbabilityEventTarget {
    pub fn validate(&self) -> Result<(), DatasetBuildError> {
        match self {
            Self::PriceHigher => Ok(()),
            Self::BenchmarkOutperformance {
                benchmark_definition,
                ..
            } if benchmark_definition.bytes() != [0; 32] => Ok(()),
            Self::ProfitAfterCosts { policy } => policy.validate(),
            Self::BenchmarkOutperformance { .. } => Err(DatasetBuildError::InvalidRequest),
        }
    }

    pub const fn label_component_name(&self) -> &'static str {
        match self {
            Self::PriceHigher => "research.fixed-horizon-price-higher",
            Self::BenchmarkOutperformance { .. } => {
                "research.fixed-horizon-benchmark-outperformance"
            }
            Self::ProfitAfterCosts { .. } => "research.fixed-horizon-profit-after-costs",
        }
    }

    /// Includes the event meaning and every stable assumption. Original sample rows remain lineage.
    pub fn digest(&self) -> Sha256Digest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/probability-event-target/v1\0");
        match self {
            Self::PriceHigher => hash.update([1]),
            Self::BenchmarkOutperformance {
                benchmark_instrument_id,
                benchmark_definition,
            } => {
                hash.update([2]);
                hash.update(benchmark_instrument_id.as_uuid().as_bytes());
                hash.update([match benchmark_definition.algorithm() {
                    DigestAlgorithm::Sha256 => 1,
                    DigestAlgorithm::Blake3 => 2,
                }]);
                hash.update(benchmark_definition.bytes());
                hash.update(b"split-adjusted-price-return/strict-greater/v1");
            }
            Self::ProfitAfterCosts { policy } => {
                hash.update([3]);
                hash.update(policy.digest().bytes());
            }
        }
        Sha256Digest::new(hash.finalize().into())
    }
}

/// Original simulation facts attested by the composition-owned application producer.
/// This is inert evidence, not a source receipt or permission to publish a dataset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbabilityCostOutcomeAttestation {
    pub instrument_id: InstrumentId,
    pub origin: market_squawk_domain::Timestamp,
    pub target_at: market_squawk_domain::Timestamp,
    pub decision_at: market_squawk_domain::Timestamp,
    pub source_selection_as_of: market_squawk_domain::Timestamp,
    pub label_available_at: market_squawk_domain::Timestamp,
    pub source_manifest: crate::DatasetManifestRef,
    pub cohort_manifest: crate::DatasetManifestRef,
    pub source_epoch_digest: Sha256Digest,
    pub source_lineage_digest: Sha256Digest,
    pub cohort_digest: Sha256Digest,
    pub evaluation_digest: Sha256Digest,
    pub outcome_digest: Sha256Digest,
    pub action_content_digest: Sha256Digest,
    pub action_audit_digest: Sha256Digest,
    pub policy: ProbabilityCostPolicyV1,
    pub net_total_return: rust_decimal::Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProbabilityExampleDerivation {
    target: ProbabilityEventTarget,
    original_label: super::FeatureLabelComponentInput,
    comparison: ProbabilityComparison,
    outcome: bool,
    digest: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProbabilityComparison {
    Zero,
    Benchmark {
        original_feature: super::FeatureLabelComponentInput,
        original_label: super::FeatureLabelComponentInput,
        source_selection_as_of: market_squawk_domain::Timestamp,
        label_selection_as_of: market_squawk_domain::Timestamp,
        origin: market_squawk_domain::Timestamp,
        target_at: market_squawk_domain::Timestamp,
        named_session_digest: Option<Sha256Digest>,
    },
    Cost(Box<ProbabilityCostOutcomeAttestation>),
}

impl ProbabilityExampleDerivation {
    pub(super) fn target(&self) -> ProbabilityEventTarget {
        self.target
    }
    pub(super) fn original_label(&self) -> &super::FeatureLabelComponentInput {
        &self.original_label
    }
    pub(super) const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    pub(super) fn retained_bytes(&self) -> Result<usize, DatasetBuildError> {
        let mut bytes = std::mem::size_of::<Self>()
            .checked_add(self.original_label.retained_bytes()?)
            .ok_or(DatasetBuildError::LimitExceeded)?;
        match &self.comparison {
            ProbabilityComparison::Zero => {}
            ProbabilityComparison::Benchmark {
                original_feature,
                original_label,
                ..
            } => {
                bytes = bytes
                    .checked_add(original_feature.retained_bytes()?)
                    .and_then(|n| n.checked_add(original_label.retained_bytes().ok()?))
                    .ok_or(DatasetBuildError::LimitExceeded)?;
            }
            ProbabilityComparison::Cost(value) => {
                bytes = bytes
                    .checked_add(std::mem::size_of::<ProbabilityCostOutcomeAttestation>())
                    .and_then(|n| n.checked_add(value.source_manifest.dataset_id().as_str().len()))
                    .and_then(|n| n.checked_add(value.source_manifest.schema().name().len()))
                    .and_then(|n| n.checked_add(value.cohort_manifest.dataset_id().as_str().len()))
                    .and_then(|n| n.checked_add(value.cohort_manifest.schema().name().len()))
                    .ok_or(DatasetBuildError::LimitExceeded)?;
            }
        }
        Ok(bytes)
    }

    pub(super) fn derive(
        subject: &super::DatasetExample,
        target: ProbabilityEventTarget,
        benchmark: Option<&super::DatasetExample>,
        costs: Option<ProbabilityCostOutcomeAttestation>,
    ) -> Result<(Self, super::FeatureLabelComponentInput), DatasetBuildError> {
        use rust_decimal::Decimal;
        target.validate()?;
        if subject.probability_derivation().is_some() || subject.financial_source().is_some() {
            return Err(DatasetBuildError::InvalidRequest);
        }
        let original_label = original_return(subject)?;
        let subject_return = decimal_return(original_label)?;
        let (origin, terminal) = subject
            .exact_target_coordinates()
            .ok_or(DatasetBuildError::InvalidRequest)?;
        let known = subject
            .label_selection_as_of()
            .ok_or(DatasetBuildError::InvalidRequest)?;
        let (outcome, comparison) = match target {
            ProbabilityEventTarget::PriceHigher if benchmark.is_none() && costs.is_none() => {
                (subject_return > Decimal::ZERO, ProbabilityComparison::Zero)
            }
            ProbabilityEventTarget::BenchmarkOutperformance {
                benchmark_instrument_id,
                ..
            } if costs.is_none() => {
                let other = benchmark.ok_or(DatasetBuildError::InvalidRequest)?;
                if other.instrument_id() != benchmark_instrument_id
                    || other.instrument_id() == subject.instrument_id()
                    || other.exact_target_coordinates() != Some((origin, terminal))
                    || other.source_selection_as_of() != subject.source_selection_as_of()
                    || other.label_selection_as_of() != Some(known)
                    || other.decision_coordinate() != subject.decision_coordinate()
                    || other.probability_derivation().is_some()
                {
                    return Err(DatasetBuildError::ComponentEvidenceMismatch);
                }
                let other_label = original_return(other)?;
                let other_feature = other
                    .components()
                    .iter()
                    .find(|v| v.spec().name() == "research.price-return")
                    .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
                (
                    subject_return > decimal_return(other_label)?,
                    ProbabilityComparison::Benchmark {
                        original_feature: other_feature.clone(),
                        original_label: other_label.clone(),
                        source_selection_as_of: other.source_selection_as_of(),
                        label_selection_as_of: known,
                        origin,
                        target_at: terminal,
                        named_session_digest: other
                            .named_session_origin()
                            .map(|value| value.evidence_digest()),
                    },
                )
            }
            ProbabilityEventTarget::ProfitAfterCosts { policy } if benchmark.is_none() => {
                let value = costs.ok_or(DatasetBuildError::InvalidRequest)?;
                if value.policy != policy
                    || value.instrument_id != subject.instrument_id()
                    || value.origin != origin
                    || value.target_at != terminal
                    || Some(value.decision_at) != subject.decision_at()
                    || value.source_selection_as_of != subject.source_selection_as_of()
                    || value.label_available_at > known
                    || value.label_available_at < terminal
                    || [
                        value.source_epoch_digest,
                        value.source_lineage_digest,
                        value.cohort_digest,
                        value.evaluation_digest,
                        value.outcome_digest,
                        value.action_content_digest,
                        value.action_audit_digest,
                    ]
                    .iter()
                    .any(|digest| digest.bytes() == [0; 32])
                    || value.net_total_return < -Decimal::ONE
                {
                    return Err(DatasetBuildError::ComponentEvidenceMismatch);
                }
                (
                    value.net_total_return > Decimal::ZERO,
                    ProbabilityComparison::Cost(Box::new(value)),
                )
            }
            _ => return Err(DatasetBuildError::InvalidRequest),
        };
        let spec = super::FeatureLabelComponentSpec::try_new(
            super::ComponentKind::Label,
            super::ComponentScope::Instrument,
            super::CorporateActionSensitivity::RequiresAdjustment,
            target.label_component_name(),
            std::num::NonZeroU32::MIN,
        )?;
        let component = super::FeatureLabelComponentInput::try_new(
            spec,
            super::ComponentValue::decimal(
                Decimal::from(u8::from(outcome)),
                Some(
                    market_squawk_domain::SourceIdentifier::try_from(
                        super::FEATURE_LABEL_PROBABILITY_UNIT,
                    )
                    .map_err(|_| DatasetBuildError::InvalidRequest)?,
                ),
                None,
            )?,
            original_label.selectors().to_vec(),
            original_label.selection_effective_cutoff().clone(),
            original_label.label_selection_effective_cutoff().cloned(),
            original_label.adjustment().clone(),
        )?;
        let mut derived = Self {
            target,
            original_label: original_label.clone(),
            comparison,
            outcome,
            digest: Sha256Digest::new([0; 32]),
        };
        derived.digest = derived.compute_digest();
        Ok((derived, component))
    }

    pub(super) fn validate_parents(
        &self,
        parents: &[crate::DatasetManifestRef],
    ) -> Result<(), DatasetBuildError> {
        if let ProbabilityComparison::Cost(value) = &self.comparison {
            if value.source_manifest == value.cohort_manifest
                || !parents.contains(&value.source_manifest)
                || !parents.contains(&value.cohort_manifest)
            {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        }
        Ok(())
    }

    pub(super) fn validate(
        &self,
        example: &super::DatasetExample,
    ) -> Result<(), DatasetBuildError> {
        self.target.validate()?;
        let label = example
            .components()
            .iter()
            .find(|value| value.spec().name() == self.target.label_component_name())
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        let expected = rust_decimal::Decimal::from(u8::from(self.outcome));
        if !matches!(label.value(), super::ComponentValue::Decimal { value, unit: Some(unit), currency: None }
            if *value == expected && unit.as_str() == super::FEATURE_LABEL_PROBABILITY_UNIT)
            || label.selectors() != self.original_label.selectors()
            || label.selection_effective_cutoff()
                != self.original_label.selection_effective_cutoff()
            || label.label_selection_effective_cutoff()
                != self.original_label.label_selection_effective_cutoff()
            || label.adjustment() != self.original_label.adjustment()
            || self.compute_digest() != self.digest
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        Ok(())
    }

    fn compute_digest(&self) -> Sha256Digest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/probability-label-original-derivation/v1\0");
        hash.update(self.target.digest().bytes());
        super::canonical::encode_component(&mut hash, &self.original_label);
        hash.update([u8::from(self.outcome)]);
        match &self.comparison {
            ProbabilityComparison::Zero => hash.update([1]),
            ProbabilityComparison::Benchmark {
                original_feature,
                original_label,
                source_selection_as_of,
                label_selection_as_of,
                origin,
                target_at,
                named_session_digest,
            } => {
                hash.update([2]);
                super::canonical::encode_component(&mut hash, original_feature);
                super::canonical::encode_component(&mut hash, original_label);
                for clock in [
                    source_selection_as_of,
                    label_selection_as_of,
                    origin,
                    target_at,
                ] {
                    hash.update(clock.unix_nanos().to_be_bytes());
                }
                if let Some(digest) = named_session_digest {
                    hash.update([1]);
                    hash.update(digest.bytes());
                } else {
                    hash.update([0]);
                }
            }
            ProbabilityComparison::Cost(value) => {
                hash.update([3]);
                hash.update(value.policy.digest().bytes());
                hash.update(value.instrument_id.as_uuid().as_bytes());
                for clock in [
                    value.origin,
                    value.target_at,
                    value.decision_at,
                    value.source_selection_as_of,
                    value.label_available_at,
                ] {
                    hash.update(clock.unix_nanos().to_be_bytes());
                }
                for digest in [
                    value.source_epoch_digest,
                    value.source_lineage_digest,
                    value.cohort_digest,
                    value.evaluation_digest,
                    value.outcome_digest,
                    value.action_content_digest,
                    value.action_audit_digest,
                ] {
                    hash.update(digest.bytes());
                }
                super::canonical::encode_manifest(&mut hash, &value.source_manifest);
                super::canonical::encode_manifest(&mut hash, &value.cohort_manifest);
                hash.update(value.net_total_return.normalize().serialize());
            }
        }
        Sha256Digest::new(hash.finalize().into())
    }
}

pub(super) fn original_return(
    example: &super::DatasetExample,
) -> Result<&super::FeatureLabelComponentInput, DatasetBuildError> {
    let mut values = example
        .components()
        .iter()
        .filter(|v| v.spec().kind() == super::ComponentKind::Label);
    let value = values.next().ok_or(DatasetBuildError::InvalidRequest)?;
    if values.next().is_some()
        || value.spec().name() != "research.fixed-horizon-forward-return"
        || value.spec().version().get() != 1
    {
        return Err(DatasetBuildError::InvalidRequest);
    }
    decimal_return(value)?;
    Ok(value)
}
fn decimal_return(
    component: &super::FeatureLabelComponentInput,
) -> Result<rust_decimal::Decimal, DatasetBuildError> {
    match component.value() {
        super::ComponentValue::Decimal {
            value,
            unit: Some(unit),
            currency: None,
        } if unit.as_str() == super::FEATURE_LABEL_RETURN_UNIT
            && *value > -rust_decimal::Decimal::ONE =>
        {
            Ok(*value)
        }
        _ => Err(DatasetBuildError::ComponentEvidenceMismatch),
    }
}
