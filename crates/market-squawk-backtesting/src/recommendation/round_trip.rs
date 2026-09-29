//! Outcome-independent cohort evaluation through the existing fill and action-accounting owner.

use super::*;
use market_squawk_data::{
    DatasetManifestRef, FeatureDatasetInputEpoch, FixedHorizonOriginBasis, ProbabilityCostPolicyV1,
    ProbabilityExecutionBasisV1, ProbabilityLiquidityPriorityV1, ProbabilityRoundTripConventionV1,
};

/// Validated exact economic convention; input manifests belong to outcomes, not this target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AllOriginRoundTripPolicyV1 {
    target_policy: ProbabilityCostPolicyV1,
    horizon_nanos: i64,
    assumptions: ResearchExecutionAssumptions,
}

impl AllOriginRoundTripPolicyV1 {
    pub fn try_new(
        target_policy: ProbabilityCostPolicyV1,
        horizon_nanos: i64,
    ) -> Result<Self, RecommendationBacktestError> {
        target_policy
            .validate()
            .map_err(|_| RecommendationBacktestError::InvalidPolicy)?;
        if horizon_nanos <= 0
            || target_policy.maximum_entry_lag_nanos >= horizon_nanos
            || target_policy.maximum_entry_lag_nanos > HARD_MAX_EXECUTION_LAG_NANOS
            || target_policy.maximum_exit_lag_nanos > HARD_MAX_EXECUTION_LAG_NANOS
            || target_policy.liquidity_priority
                != ProbabilityLiquidityPriorityV1::SignalTimeThenOrderId
            || target_policy.convention
                != ProbabilityRoundTripConventionV1::LongRoundTripTotalWealthIncludingEntitlements
            || match target_policy.execution_basis {
                ProbabilityExecutionBasisV1::ObservedQuoteDepth => target_policy
                    .daily_bar_assumed_spread_basis_points
                    .is_some(),
                ProbabilityExecutionBasisV1::CompletedDailyBar => {
                    target_policy.daily_bar_assumed_spread_basis_points
                        != Some(DAILY_BAR_ASSUMED_FULL_SPREAD_BASIS_POINTS)
                }
            }
        {
            return Err(RecommendationBacktestError::InvalidPolicy);
        }
        let assumptions =
            ResearchExecutionAssumptions::try_new(ResearchExecutionAssumptionsInput {
                version: target_policy.execution_policy_version,
                fee_basis_points: BasisPoints::new(target_policy.fee_basis_points),
                slippage_basis_points: BasisPoints::new(target_policy.slippage_basis_points),
                maximum_random_slippage_basis_points: BasisPoints::new(
                    target_policy.maximum_random_slippage_basis_points,
                ),
                maximum_participation_basis_points: BasisPoints::new(
                    target_policy.maximum_participation_basis_points,
                ),
                liquidity_priority: ResearchLiquidityPriority::SignalTimeThenOrderId,
                latency_nanos: target_policy.latency_nanos,
                allow_partial_fills: target_policy.allow_partial_fills,
                fee_decimal_scale: target_policy.fee_decimal_scale,
            })
            .map_err(|_| RecommendationBacktestError::InvalidPolicy)?;
        if 5_000_i32
            .checked_add(assumptions.slippage_basis_points().get())
            .and_then(|n| n.checked_add(assumptions.maximum_random_slippage_basis_points().get()))
            .is_none_or(|n| n > 10_000)
        {
            return Err(RecommendationBacktestError::InvalidPolicy);
        }
        Ok(Self {
            target_policy,
            horizon_nanos,
            assumptions,
        })
    }
    pub const fn target_policy(self) -> ProbabilityCostPolicyV1 {
        self.target_policy
    }
    pub const fn horizon_nanos(self) -> i64 {
        self.horizon_nanos
    }
    /// Exact validated execution terms for equality with the original materialized recipe.
    pub const fn execution_assumptions(self) -> ResearchExecutionAssumptions {
        self.assumptions
    }
    fn execution(self) -> RoundTripExecutionPolicy {
        RoundTripExecutionPolicy {
            digest: self.target_policy.digest(),
            reason_code: "all-origin-round-trip-v1",
            reporting_currency: self.target_policy.reporting_currency,
            maximum_entry_lag_nanos: self.target_policy.maximum_entry_lag_nanos,
            maximum_exit_lag_nanos: self.target_policy.maximum_exit_lag_nanos,
            execution_assumptions: self.assumptions,
            seed: self.target_policy.seed,
        }
    }
}

/// Economic absence is retained separately from both a profitable and an unprofitable outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AllOriginRoundTripUnavailableV1 {
    OutsideHistoricalUniverse,
    MissingOriginPrice,
    TargetAfterSimulationCutoff,
    IncompleteCorporateActions,
    ReportingCurrencyMismatch,
    ExecutionTermsChanged,
    UnsupportedInstrument,
    InvalidArithmetic,
    InsufficientPointInTimeEvidence,
}

/// Original successful or unsuccessful simulated execution, with no manufactured binary loss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AllOriginRoundTripDispositionV1 {
    Completed(Box<RecommendationRoundTripOutcomeV1>),
    EntryUnfilled {
        gap: RecommendationExecutionGapV1,
        partial_fill: Option<ResearchFill>,
    },
    ExitUnfilled {
        entry_fill: ResearchFill,
        gap: RecommendationExecutionGapV1,
        partial_fill: Option<ResearchFill>,
    },
    Unavailable(AllOriginRoundTripUnavailableV1),
}

/// One immutable result for one original subject input epoch, including every unavailable origin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AllOriginRoundTripResultV1 {
    example_id: Box<str>,
    instrument_id: InstrumentId,
    source_manifest: DatasetManifestRef,
    source_epoch_digest: Sha256Digest,
    source_lineage_digest: Sha256Digest,
    decision_at: Timestamp,
    source_selection_as_of: Timestamp,
    feature_available_at: Timestamp,
    target_origin: Timestamp,
    target_at: Timestamp,
    origin_basis: FixedHorizonOriginBasis,
    label_available_at: Timestamp,
    disposition: AllOriginRoundTripDispositionV1,
    digest: Sha256Digest,
}
impl AllOriginRoundTripResultV1 {
    pub fn example_id(&self) -> &str {
        &self.example_id
    }
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn source_manifest(&self) -> &DatasetManifestRef {
        &self.source_manifest
    }
    pub const fn source_epoch_digest(&self) -> Sha256Digest {
        self.source_epoch_digest
    }
    pub const fn source_lineage_digest(&self) -> Sha256Digest {
        self.source_lineage_digest
    }
    pub const fn decision_at(&self) -> Timestamp {
        self.decision_at
    }
    pub const fn source_selection_as_of(&self) -> Timestamp {
        self.source_selection_as_of
    }
    pub const fn feature_available_at(&self) -> Timestamp {
        self.feature_available_at
    }
    pub const fn target_origin(&self) -> Timestamp {
        self.target_origin
    }
    pub const fn target_at(&self) -> Timestamp {
        self.target_at
    }
    pub const fn origin_basis(&self) -> FixedHorizonOriginBasis {
        self.origin_basis
    }
    /// Conservative original outcome knowledge, never rewritten to the historical decision.
    pub const fn label_available_at(&self) -> Timestamp {
        self.label_available_at
    }
    pub const fn disposition(&self) -> &AllOriginRoundTripDispositionV1 {
        &self.disposition
    }
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }
    /// Ties are false; missing outcomes remain None.
    pub fn profit_after_costs(&self) -> Option<bool> {
        match &self.disposition {
            AllOriginRoundTripDispositionV1::Completed(value) => {
                Some(value.cost_adjusted_total_return() > Decimal::ZERO)
            }
            _ => None,
        }
    }
}

/// Complete sealed-input cohort and its separately known realized outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AllOriginRoundTripEvaluationV1 {
    dataset_manifest: DatasetManifestRef,
    dataset_identity: Sha256Digest,
    cohort_digest: Sha256Digest,
    policy: AllOriginRoundTripPolicyV1,
    study_qualification: BacktestStudyQualification,
    raw_history_digest: Option<Sha256Digest>,
    action_content_digest: Sha256Digest,
    action_audit_digest: Sha256Digest,
    action_coverage_digest: Option<Sha256Digest>,
    simulation_cutoff: Timestamp,
    results: Box<[AllOriginRoundTripResultV1]>,
    digest: Sha256Digest,
}
impl AllOriginRoundTripEvaluationV1 {
    pub const fn dataset_manifest(&self) -> &DatasetManifestRef {
        &self.dataset_manifest
    }
    pub const fn dataset_identity(&self) -> Sha256Digest {
        self.dataset_identity
    }
    pub const fn cohort_digest(&self) -> Sha256Digest {
        self.cohort_digest
    }
    pub const fn policy(&self) -> AllOriginRoundTripPolicyV1 {
        self.policy
    }
    pub const fn study_qualification(&self) -> BacktestStudyQualification {
        self.study_qualification
    }
    pub const fn raw_history_digest(&self) -> Option<Sha256Digest> {
        self.raw_history_digest
    }
    pub const fn action_content_digest(&self) -> Sha256Digest {
        self.action_content_digest
    }
    pub const fn action_audit_digest(&self) -> Sha256Digest {
        self.action_audit_digest
    }
    pub const fn action_coverage_digest(&self) -> Option<Sha256Digest> {
        self.action_coverage_digest
    }
    pub const fn simulation_cutoff(&self) -> Timestamp {
        self.simulation_cutoff
    }
    pub fn results(&self) -> &[AllOriginRoundTripResultV1] {
        &self.results
    }
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }
}

/// Reuses the simulator at every original subject origin, without a strategy issuer or selection.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllOriginRoundTripEvaluatorV1;
impl AllOriginRoundTripEvaluatorV1 {
    pub fn evaluate(
        dataset: &BacktestDataset,
        corporate_actions: &CorporateActionPlan,
        subject: InstrumentId,
        policy: AllOriginRoundTripPolicyV1,
        limits: RecommendationBacktestLimits,
        cancellation: &CancellationToken,
    ) -> Result<AllOriginRoundTripEvaluationV1, RecommendationBacktestError> {
        let qualification = dataset
            .study_qualification
            .ok_or(RecommendationBacktestError::InvalidDataset)?;
        qualification
            .validate()
            .map_err(|_| RecommendationBacktestError::InvalidDataset)?;
        let basis = match policy.target_policy.execution_basis {
            ProbabilityExecutionBasisV1::ObservedQuoteDepth => {
                BacktestExecutionBasis::ObservedQuoteDepth
            }
            ProbabilityExecutionBasisV1::CompletedDailyBar => {
                BacktestExecutionBasis::CompletedDailyBar
            }
        };
        if dataset.execution_basis() != basis
            || corporate_actions.knowledge_cutoff() > qualification.snapshot_as_of()
            || corporate_actions.valuation_cutoff() > corporate_actions.knowledge_cutoff()
            || dataset
                .daily_history
                .as_ref()
                .is_some_and(|history| history.available_at > qualification.snapshot_as_of())
        {
            return Err(RecommendationBacktestError::InvalidDataset);
        }
        let mut count = 0_usize;
        let mut visits = 0_usize;
        let mut cohort = Sha256::new();
        cohort.update(b"market-squawk/all-origin-round-trip-cohort/v1\0");
        cohort.update(dataset.manifest.content_hash().bytes());
        cohort.update(dataset.object_graph_digest().bytes());
        cohort.update(dataset.point_in_time_content.bytes());
        cohort.update(dataset.point_in_time_audit.bytes());
        cohort.update(subject.as_uuid().as_bytes());
        // This first pass seals the entire input population before inspecting any realized fill.
        for observation in dataset.observations.iter() {
            let observation =
                observation.map_err(|_| RecommendationBacktestError::InvalidDataset)?;
            if cancellation.is_cancelled() {
                return Err(RecommendationBacktestError::Cancelled);
            }
            count_observation_visit(&mut visits, limits)?;
            if observation.instrument_id() != subject {
                continue;
            }
            let epoch = original_epoch(&observation, qualification, policy)?;
            count = count
                .checked_add(1)
                .filter(|n| *n <= limits.max_signals())
                .ok_or(RecommendationBacktestError::LimitExceeded)?;
            cohort.update(epoch_digest(&epoch)?.bytes());
            cohort.update(observation.lineage_digest.bytes());
        }
        if count == 0 {
            return Err(RecommendationBacktestError::InvalidDataset);
        }
        update_length(&mut cohort, count)?;
        let cohort_digest = Sha256Digest::new(cohort.finalize().into());
        let mut results = Vec::new();
        results
            .try_reserve_exact(count)
            .map_err(|_| RecommendationBacktestError::LimitExceeded)?;
        let coverage = corporate_actions.source_admission();
        let coverage_digest = coverage.map(|v| Sha256Digest::new(v.evidence_digest().bytes()));
        let action_scope = corporate_actions.policy().adjustment()
            == CorporateActionAdjustment::TotalReturn
            && corporate_actions.conflicts().is_empty()
            && coverage.is_some_and(|v| v.instruments().contains(&subject));
        let quantity = QuantityLots::new(policy.target_policy.quantity_lots)
            .map_err(|_| RecommendationBacktestError::InvalidPolicy)?;
        let mut total_equity_points = 0_usize;
        for observation in dataset.observations.iter() {
            let observation =
                observation.map_err(|_| RecommendationBacktestError::InvalidDataset)?;
            if cancellation.is_cancelled() {
                return Err(RecommendationBacktestError::Cancelled);
            }
            count_observation_visit(&mut visits, limits)?;
            if observation.instrument_id() != subject {
                continue;
            }
            let epoch = original_epoch(&observation, qualification, policy)?;
            let origin = epoch
                .target_origin()
                .ok_or(RecommendationBacktestError::InvalidDataset)?;
            let target = epoch
                .target_at()
                .ok_or(RecommendationBacktestError::InvalidDataset)?;
            let source_epoch_digest = epoch_digest(&epoch)?;
            let origin_basis = epoch
                .fixed_horizon_origin_basis()
                .ok_or(RecommendationBacktestError::InvalidDataset)?;
            let mut row = AllOriginRoundTripResultV1 {
                example_id: epoch.example_id().into(),
                instrument_id: subject,
                source_manifest: epoch.source_manifest().clone(),
                source_epoch_digest,
                source_lineage_digest: observation.lineage_digest,
                decision_at: observation.decision_at(),
                source_selection_as_of: observation.source_selection_as_of,
                feature_available_at: observation.available_at(),
                target_origin: origin,
                target_at: target,
                origin_basis,
                label_available_at: corporate_actions
                    .knowledge_cutoff()
                    .max(observation.available_at())
                    .max(
                        dataset
                            .daily_history
                            .as_ref()
                            .map_or(observation.available_at(), |h| h.available_at),
                    ),
                disposition: AllOriginRoundTripDispositionV1::Unavailable(
                    AllOriginRoundTripUnavailableV1::InsufficientPointInTimeEvidence,
                ),
                digest: Sha256Digest::new([0; 32]),
            };
            let action_complete = action_scope
                && coverage.is_some_and(|v| {
                    v.application_starts_at(subject)
                        .is_some_and(|start| start <= observation.decision_at())
                        && dataset.daily_history.as_ref().is_none_or(|history| {
                            history
                                .nominal_sources
                                .iter()
                                .filter(|s| s.instrument == subject)
                                .all(|source| {
                                    v.knowledge_cutoff() == source.knowledge_cutoff
                                        && v.history_input_manifests().contains(&source.manifest)
                                })
                        })
                });
            row.disposition = if observation.universe != HistoricalUniverseStatus::Eligible {
                unavailable(AllOriginRoundTripUnavailableV1::OutsideHistoricalUniverse)
            } else if observation.market_reference.is_none() && observation.mid_price.is_none() {
                unavailable(AllOriginRoundTripUnavailableV1::MissingOriginPrice)
            } else if observation.execution_terms.quote_currency()
                != policy.target_policy.reporting_currency
                || observation.execution_terms.settlement_denomination()
                    != Denomination::Currency(policy.target_policy.reporting_currency)
            {
                unavailable(AllOriginRoundTripUnavailableV1::ReportingCurrencyMismatch)
            } else if target > corporate_actions.valuation_cutoff() {
                unavailable(AllOriginRoundTripUnavailableV1::TargetAfterSimulationCutoff)
            } else if !action_complete {
                unavailable(AllOriginRoundTripUnavailableV1::IncompleteCorporateActions)
            } else {
                let execution_origin = RoundTripExecutionOrigin {
                    decision_at: observation.decision_at(),
                    entry_identity: cohort_leg_identity(
                        policy,
                        cohort_digest,
                        source_epoch_digest,
                        b"entry",
                    ),
                    exit_identity: cohort_leg_identity(
                        policy,
                        cohort_digest,
                        source_epoch_digest,
                        b"exit",
                    ),
                };
                match simulate_round_trip(
                    dataset,
                    policy.execution(),
                    corporate_actions,
                    cancellation,
                    subject,
                    quantity,
                    execution_origin,
                    target,
                    corporate_actions.valuation_cutoff(),
                    limits,
                    &mut total_equity_points,
                    &mut visits,
                ) {
                    Ok(RoundTripSimulation::Completed(value)) => {
                        row.label_available_at = value
                            .equity_path()
                            .iter()
                            .fold(row.label_available_at, |at, p| at.max(p.available_at()));
                        if row.label_available_at > qualification.snapshot_as_of() {
                            unavailable(
                                AllOriginRoundTripUnavailableV1::InsufficientPointInTimeEvidence,
                            )
                        } else {
                            AllOriginRoundTripDispositionV1::Completed(value)
                        }
                    }
                    Ok(RoundTripSimulation::EntryUnfilled { gap, partial_fill }) => {
                        AllOriginRoundTripDispositionV1::EntryUnfilled { gap, partial_fill }
                    }
                    Ok(RoundTripSimulation::ExitUnfilled {
                        entry_fill,
                        gap,
                        partial_fill,
                    }) => AllOriginRoundTripDispositionV1::ExitUnfilled {
                        entry_fill,
                        gap,
                        partial_fill,
                    },
                    Ok(RoundTripSimulation::Unavailable(reason)) => unavailable(match reason {
                        RecommendationSignalUnavailableReasonV1::ReportingCurrencyMismatch => {
                            AllOriginRoundTripUnavailableV1::ReportingCurrencyMismatch
                        }
                        RecommendationSignalUnavailableReasonV1::ExecutionTermsChanged => {
                            AllOriginRoundTripUnavailableV1::ExecutionTermsChanged
                        }
                        RecommendationSignalUnavailableReasonV1::UnsupportedInstrument => {
                            AllOriginRoundTripUnavailableV1::UnsupportedInstrument
                        }
                        _ => AllOriginRoundTripUnavailableV1::InsufficientPointInTimeEvidence,
                    }),
                    Err(RecommendationBacktestError::Arithmetic) => {
                        unavailable(AllOriginRoundTripUnavailableV1::InvalidArithmetic)
                    }
                    Err(RecommendationBacktestError::InvalidDataset) => {
                        unavailable(AllOriginRoundTripUnavailableV1::IncompleteCorporateActions)
                    }
                    Err(error) => return Err(error),
                }
            };
            row.digest = result_digest(&row, cohort_digest, policy, corporate_actions);
            results.push(row);
        }
        let mut evaluation = AllOriginRoundTripEvaluationV1 {
            dataset_manifest: dataset.manifest.clone(),
            dataset_identity: dataset.identity(),
            cohort_digest,
            policy,
            study_qualification: qualification,
            raw_history_digest: dataset.raw_execution_history_digest(),
            action_content_digest: corporate_actions.content_hash(),
            action_audit_digest: corporate_actions.audit_hash(),
            action_coverage_digest: coverage_digest,
            simulation_cutoff: corporate_actions.valuation_cutoff(),
            results: results.into_boxed_slice(),
            digest: Sha256Digest::new([0; 32]),
        };
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/all-origin-round-trip-evaluation/v1\0");
        hash.update(evaluation.dataset_identity.bytes());
        hash.update(evaluation.cohort_digest.bytes());
        hash.update(policy.target_policy.digest().bytes());
        hash.update(policy.horizon_nanos.to_be_bytes());
        qualification.hash_into(&mut hash);
        hash.update(evaluation.action_content_digest.bytes());
        hash.update(evaluation.action_audit_digest.bytes());
        for digest in [
            evaluation.raw_history_digest,
            evaluation.action_coverage_digest,
        ] {
            match digest {
                Some(v) => {
                    hash.update([1]);
                    hash.update(v.bytes());
                }
                None => hash.update([0]),
            }
        }
        hash.update(evaluation.simulation_cutoff.unix_nanos().to_be_bytes());
        for limit in persisted_limits(limits) {
            update_length(&mut hash, limit)?;
        }
        update_length(&mut hash, evaluation.results.len())?;
        for row in &evaluation.results {
            hash.update(row.digest.bytes());
        }
        evaluation.digest = Sha256Digest::new(hash.finalize().into());
        Ok(evaluation)
    }
}

fn original_epoch(
    observation: &BacktestObservation,
    qualification: BacktestStudyQualification,
    policy: AllOriginRoundTripPolicyV1,
) -> Result<FeatureDatasetInputEpoch, RecommendationBacktestError> {
    let coordinate = observation
        .input_coordinate
        .as_ref()
        .ok_or(RecommendationBacktestError::InvalidDataset)?
        .load()
        .map_err(|_| RecommendationBacktestError::InvalidDataset)?;
    let epoch = coordinate.epoch();
    let origin = epoch
        .target_origin()
        .ok_or(RecommendationBacktestError::InvalidDataset)?;
    let target = epoch
        .target_at()
        .ok_or(RecommendationBacktestError::InvalidDataset)?;
    if epoch.instrument_id() != observation.instrument_id()
        || epoch.decision_at() != Some(observation.decision_at())
        || epoch.source_selection_as_of() != observation.source_selection_as_of
        || epoch.source_snapshot_digest() != qualification.source_snapshot_digest()
        || epoch.snapshot_as_of() != qualification.snapshot_as_of()
        || epoch.basis() != qualification.basis()
        || observation.financial_target != Some((origin, target))
        || origin.checked_add_nanos(policy.horizon_nanos).ok() != Some(target)
        || target <= observation.decision_at()
        || !qualification.admits_clocks(
            origin,
            observation.available_at(),
            observation.source_selection_as_of,
            observation.decision_at(),
        )
    {
        return Err(RecommendationBacktestError::InvalidDataset);
    }
    Ok(epoch.clone())
}
fn epoch_digest(
    epoch: &FeatureDatasetInputEpoch,
) -> Result<Sha256Digest, RecommendationBacktestError> {
    let bytes = epoch
        .canonical_bytes()
        .map_err(|_| RecommendationBacktestError::InvalidDataset)?;
    Ok(Sha256Digest::new(Sha256::digest(bytes).into()))
}
fn unavailable(reason: AllOriginRoundTripUnavailableV1) -> AllOriginRoundTripDispositionV1 {
    AllOriginRoundTripDispositionV1::Unavailable(reason)
}
fn cohort_leg_identity(
    policy: AllOriginRoundTripPolicyV1,
    cohort: Sha256Digest,
    epoch: Sha256Digest,
    leg: &[u8],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/all-origin-round-trip-leg/v1\0");
    hash.update(policy.target_policy.digest().bytes());
    hash.update(policy.horizon_nanos.to_be_bytes());
    hash.update(cohort.bytes());
    hash.update(epoch.bytes());
    hash.update((leg.len() as u64).to_be_bytes());
    hash.update(leg);
    hash.finalize().into()
}
fn result_digest(
    row: &AllOriginRoundTripResultV1,
    cohort: Sha256Digest,
    policy: AllOriginRoundTripPolicyV1,
    actions: &CorporateActionPlan,
) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/all-origin-round-trip-result/v1\0");
    hash.update(cohort.bytes());
    hash.update(policy.target_policy.digest().bytes());
    hash.update(row.source_epoch_digest.bytes());
    hash.update(row.source_lineage_digest.bytes());
    hash.update(actions.content_hash().bytes());
    hash.update(actions.audit_hash().bytes());
    hash.update(row.label_available_at.unix_nanos().to_be_bytes());
    match &row.disposition {
        AllOriginRoundTripDispositionV1::Completed(value) => {
            hash.update([0]);
            hash.update(value.digest().bytes());
        }
        AllOriginRoundTripDispositionV1::EntryUnfilled { gap, partial_fill } => {
            hash.update([1, execution_gap_code(*gap)]);
            update_optional_fill(&mut hash, partial_fill.as_ref());
        }
        AllOriginRoundTripDispositionV1::ExitUnfilled {
            entry_fill,
            gap,
            partial_fill,
        } => {
            hash.update([2, execution_gap_code(*gap)]);
            update_fill(&mut hash, entry_fill);
            update_optional_fill(&mut hash, partial_fill.as_ref());
        }
        AllOriginRoundTripDispositionV1::Unavailable(reason) => hash.update([
            3,
            match reason {
                AllOriginRoundTripUnavailableV1::OutsideHistoricalUniverse => 0,
                AllOriginRoundTripUnavailableV1::MissingOriginPrice => 1,
                AllOriginRoundTripUnavailableV1::TargetAfterSimulationCutoff => 2,
                AllOriginRoundTripUnavailableV1::IncompleteCorporateActions => 3,
                AllOriginRoundTripUnavailableV1::ReportingCurrencyMismatch => 4,
                AllOriginRoundTripUnavailableV1::ExecutionTermsChanged => 5,
                AllOriginRoundTripUnavailableV1::UnsupportedInstrument => 6,
                AllOriginRoundTripUnavailableV1::InvalidArithmetic => 7,
                AllOriginRoundTripUnavailableV1::InsufficientPointInTimeEvidence => 8,
            },
        ]),
    }
    Sha256Digest::new(hash.finalize().into())
}
