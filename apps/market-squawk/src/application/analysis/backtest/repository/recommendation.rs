//! Governed recommendation studies retained in the existing backtest terminal authority.

use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_backtesting::{
    BacktestExecutionBasis, BacktestStudyQualification,
    RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1, RecommendationBacktestLimits,
    RecommendationBacktestLimitsInput, RecommendationBacktestPolicyV1,
    RecommendationBacktestPolicyV1Input, RecommendationBacktestPublicationV1,
    RecommendationBenchmarkPolicyV1, recommendation_conservative_execution_assumptions_v1,
};
use market_squawk_data::Sha256Digest;
use market_squawk_domain::{Currency, InstrumentId, QuantityLots, Timestamp};
use market_squawk_services::{ArtifactReference, RequestContext, ServiceError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::super::{
    GovernedBacktestCommand, GovernedBacktestPrepublishAuthority,
    ProductionGovernedBacktestInputAuthority,
    input_authority::{
        GovernedRecommendationBacktestEvidenceV1, GovernedRecommendationInputMaterializerV1,
        GovernedRecommendationSignalIssuerV1,
    },
};
use super::{
    ProductionGovernedBacktestRepository,
    index::CommandWire,
    lifecycle::{LinkedOperation, RepositoryLifecycle, await_blocking, ensure_operation_live},
    materialization::{MAXIMUM_MATERIALIZATION_BYTES, MaterializationReference},
};

/// Exact immutable study inputs. The issuer is installed separately by application composition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GovernedRecommendationBacktestRequestV1 {
    pub(crate) command: GovernedBacktestCommand,
    pub(crate) issuer_identity_digest: Sha256Digest,
    pub(crate) issuer_reference: Box<str>,
    pub(crate) policy: RecommendationBacktestPolicyV1,
    pub(crate) evaluation_starts_at: Timestamp,
    pub(crate) limits: RecommendationBacktestLimits,
}

impl GovernedRecommendationBacktestRequestV1 {
    pub(crate) fn digest(&self) -> Result<Sha256Digest, ServiceError> {
        let wire = RequestWire::from_request(self);
        if wire.clone().into_request()? != *self {
            return Err(ServiceError::InvalidRequest);
        }
        let bytes = serde_json::to_vec(&wire).map_err(|_| ServiceError::InvalidRequest)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/governed-recommendation-backtest-request/v1\0");
        hash.update(bytes);
        Ok(Sha256Digest::new(hash.finalize().into()))
    }
}

/// Reference to one exact durable study, never a caller-supplied performance summary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GovernedRecommendationBacktestReferenceV1 {
    pub(crate) request_digest: Sha256Digest,
    pub(crate) evidence_digest: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GovernedRecommendationBacktestReceiptV1 {
    pub(crate) reference: GovernedRecommendationBacktestReferenceV1,
    pub(crate) evidence: GovernedRecommendationBacktestEvidenceV1,
    pub(crate) materialization_artifact: ArtifactReference,
    pub(crate) fiscal_recipe_artifacts: Vec<ArtifactReference>,
}

impl GovernedRecommendationBacktestReceiptV1 {
    /// Projects retained observations and financial denominators without imputing missing returns.
    pub(crate) fn report(&self) -> serde_json::Value {
        use market_squawk_backtesting::{
            RecommendationAggregateEvidenceV1 as Aggregate,
            RecommendationAggregateUnavailableV1 as Gap,
            RecommendationBenchmarkAggregateV1 as Benchmark,
        };
        let study = self.evidence.study();
        let aggregate = match study.aggregate() {
            Aggregate::Available(value) => serde_json::json!({
                "status": "available", "observationCount": value.observation_count(),
                "independentFoldCount": value.trial_count(),
                "meanCostAdjustedReturn": value.cost_adjusted_total_return().to_string(),
                "worstMaximumDrawdown": value.worst_maximum_drawdown().to_string(),
                "positiveFoldCount": value.positive_fold_count(),
                "positiveFoldStability": value.positive_fold_stability().to_string(),
                "benchmark": match value.benchmark() {
                    Benchmark::Available { mean_cost_adjusted_total_return, mean_excess_return } =>
                        serde_json::json!({"status":"available",
                            "meanCostAdjustedReturn":mean_cost_adjusted_total_return.to_string(),
                            "meanExcessReturn":mean_excess_return.to_string()}),
                    Benchmark::Unavailable => serde_json::json!({"status":"unavailable"}),
                },
                "accompanyingBenchmark": match value.accompanying_benchmark() {
                    Benchmark::Available { mean_cost_adjusted_total_return, mean_excess_return } =>
                        serde_json::json!({"status":"available",
                            "meanCostAdjustedReturn":mean_cost_adjusted_total_return.to_string(),
                            "meanExcessReturn":mean_excess_return.to_string()}),
                    Benchmark::Unavailable => serde_json::json!({"status":"unavailable"}),
                },
            }),
            Aggregate::Unavailable(reason) => serde_json::json!({
                "status":"unavailable", "reason": match reason {
                    Gap::TruncatedSignalPopulation => "truncated-signal-population",
                    Gap::IncompleteDeclaredEntry { .. } => "incomplete-declared-entry",
                    Gap::MissingCompletedObservationInFold { .. } => "missing-completed-observation-in-fold",
                },
            }),
        };
        let folds: Vec<_> = study.folds().iter().enumerate().map(|(index, fold)| {
            serde_json::json!({"foldId":fold.fold_id().as_str(),
                "startsAtUnixNanos":fold.starts_at().unix_nanos().to_string(),
                "endsAtUnixNanos":fold.ends_at().unix_nanos().to_string(),
                "population":disposition_counts(study.results().iter().filter(|result| result.fold_index()==index)),
            })
        }).collect();
        let publication = study.publication();
        let costs = study.policy().execution_assumptions();
        let policy = study.policy();
        serde_json::json!({
            "requestDigest":crate::application::domain_support::encode_hex(self.reference.request_digest.bytes()),
            "evidenceDigest":crate::application::domain_support::encode_hex(self.reference.evidence_digest.bytes()),
            "studyBasis": study.basis(),
            "studyLimitations": study.limitations(),
            "snapshotAsOfUnixNanos": study.snapshot_as_of().unix_nanos().to_string(),
            "sourceSnapshotDigest": crate::application::domain_support::encode_hex(study.source_snapshot_digest().bytes()),
            "targetHorizonNanos":study.policy().target_horizon_nanos().to_string(),
            "simulationCutoffUnixNanos":publication.simulation_cutoff().unix_nanos().to_string(),
            "evaluatedAtUnixNanos":publication.evaluated_at().unix_nanos().to_string(),
            "publishedAtUnixNanos":publication.published_at().unix_nanos().to_string(),
            "availableAtUnixNanos":publication.available_at().unix_nanos().to_string(),
            "expiresAtUnixNanos":publication.expires_at().unix_nanos().to_string(),
            "population":disposition_counts(study.results().iter()), "folds":folds,
            "aggregate":aggregate,
            "methodology":{
                "policyDigest":crate::application::domain_support::encode_hex(policy.digest().bytes()),
                "subjectInstrumentId":policy.subject_instrument_id(),
                "reportingCurrency":policy.reporting_currency().as_str(),
                "priceBasis":"raw-with-corporate-action-ledger",
                "targetTiming":"financial-origin-plus-365-elapsed-days",
                "decisionLagNanos":policy.study_qualification().decision_lag_nanos().map(|value| value.to_string()),
                "executionPriceRounding":"adverse-tick-rounding-after-costs",
                "distributionTreatment":"cash-entitlement-without-reinvestment",
                "distributionTiming":"Dividend entitlement is valued from the source ex-date without reinvestment; it is distinct from cash settlement on the source payable date.",
                "executionBasis":match policy.execution_basis() {
                    BacktestExecutionBasis::ObservedQuoteDepth => "observed-quote-depth",
                    BacktestExecutionBasis::CompletedDailyBar => "completed-daily-bar",
                },
                "assumedFullSpreadBasisPoints":policy.daily_bar_assumed_spread_basis_points().map(|spread| spread.get()),
                "fillTiming":match policy.execution_basis() {
                    BacktestExecutionBasis::ObservedQuoteDepth => "next-eligible-observation",
                    BacktestExecutionBasis::CompletedDailyBar => "next-eligible-completed-bar-close",
                },
                "participationBasis":match policy.execution_basis() {
                    BacktestExecutionBasis::ObservedQuoteDepth => "observed-executable-depth",
                    BacktestExecutionBasis::CompletedDailyBar => "completed-bar-traded-volume",
                },
                "executionLimitations":match policy.execution_basis() {
                    BacktestExecutionBasis::ObservedQuoteDepth => Vec::<&str>::new(),
                    BacktestExecutionBasis::CompletedDailyBar => vec![
                        "Daily bars do not prove the bid/ask spread or liquidity available at the simulated fill instant.",
                        "Fills use the close of the first complete bar starting after the signal and latency, with an assumed spread and a participation cap on that bar's traded volume.",
                        "Realized bar prices and volume remain outside the information used to generate historical signals.",
                        "The equity path uses completed daily closes and does not measure intraday drawdown.",
                    ],
                },
                "rawPriceEvidenceDigest":crate::application::domain_support::encode_hex(policy.raw_price_evidence_digest().bytes()),
                "corporateActionContentDigest":crate::application::domain_support::encode_hex(policy.corporate_action_content_digest().bytes()),
                "corporateActionAuditDigest":crate::application::domain_support::encode_hex(policy.corporate_action_audit_digest().bytes()),
                "corporateActionCoverageStartsAtUnixNanos":policy.corporate_action_coverage_starts_at().unix_nanos().to_string(),
                "primaryBenchmark":{
                    "instrumentId":policy.benchmark().instrument_id(),
                    "approvalDigest":crate::application::domain_support::encode_hex(policy.benchmark().approval_digest().bytes()),
                },
                "accompanyingBenchmark":{
                    "instrumentId":policy.accompanying_benchmark().instrument_id(),
                    "approvalDigest":crate::application::domain_support::encode_hex(policy.accompanying_benchmark().approval_digest().bytes()),
                },
            },
            "executionAssumptions":{
                "feeBasisPointsPerLeg":costs.fee_basis_points().get(),
                "slippageBasisPointsPerLeg":costs.slippage_basis_points().get(),
                "maximumRandomSlippageBasisPointsPerLeg":costs.maximum_random_slippage_basis_points().get(),
                "maximumParticipationBasisPoints":costs.maximum_participation_basis_points().get(),
                "latencyNanos":costs.latency_nanos().to_string(),
                "allowPartialFills":costs.allow_partial_fills(),
                "digest":crate::application::domain_support::encode_hex(costs.digest().bytes()),
            },
        })
    }
}

fn disposition_counts<'a>(
    results: impl Iterator<Item = &'a market_squawk_backtesting::RecommendationSignalResultV1>,
) -> serde_json::Value {
    use market_squawk_backtesting::{
        RecommendationSignalCensorReasonV1 as Censor,
        RecommendationSignalDispositionV1 as Disposition,
    };
    let mut counts = [0_usize; 11];
    for result in results {
        counts[0] += 1;
        counts[match result.disposition() {
            Disposition::Completed { .. } => 1,
            Disposition::NoAction => 2,
            Disposition::Unavailable(_) => 3,
            Disposition::Censored(Censor::TargetAfterSimulationCutoff) => 4,
            Disposition::Censored(Censor::OutsideAuthorizedDataset) => 5,
            Disposition::EntryUnfilled { .. } => 6,
            Disposition::ExitUnfilled { .. } => 7,
            Disposition::BenchmarkUnavailable { .. } => 8,
        }] += 1;
        match result.accompanying_benchmark() {
            Some(Ok(_)) => counts[9] += 1,
            Some(Err(_)) => counts[10] += 1,
            None => {}
        }
    }
    serde_json::json!({"totalSignals":counts[0], "completedSubjectAndBenchmark":counts[1],
        "noAction":counts[2], "unavailable":counts[3],
        "censoredTargetAfterCutoff":counts[4], "censoredOutsideFold":counts[5],
        "entryUnfilled":counts[6], "exitUnfilled":counts[7], "benchmarkUnavailable":counts[8],
        "completedSubject":counts[1]+counts[8],
        "accompanyingBenchmarkCompleted":counts[9], "accompanyingBenchmarkUnavailable":counts[10],
    })
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredRecommendationTerminalV1 {
    request: RequestWire,
    request_digest: [u8; 32],
    evidence_digest: [u8; 32],
    publication: [Timestamp; 5],
    materialized_signal_plan: MaterializationReference,
    fiscal_artifacts: Vec<StoredFiscalArtifactV1>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredFiscalArtifactV1 {
    id: String,
    sha256: String,
    byte_count: usize,
}
impl StoredFiscalArtifactV1 {
    fn from_reference(reference: &ArtifactReference) -> Self {
        Self {
            id: reference.id().to_owned(),
            sha256: reference.sha256().to_owned(),
            byte_count: reference.byte_count(),
        }
    }
    fn reference(&self) -> Result<ArtifactReference, ServiceError> {
        ArtifactReference::try_new(
            self.id.as_str(),
            self.sha256.as_str(),
            self.byte_count,
            "application/json",
        )
        .map_err(|_| ServiceError::InvalidResult)
    }
}

impl StoredRecommendationTerminalV1 {
    pub(super) const fn evidence_digest(&self) -> [u8; 32] {
        self.evidence_digest
    }

    pub(super) fn validate(&self, _maximum_bytes: usize) -> Result<(), ServiceError> {
        let request = self.request.clone().into_request()?;
        let publication = self.publication()?;
        let fiscal = self.fiscal_artifact_references()?;
        let original: super::super::input_authority::HistoricalRecommendationAlphaProducerReference =
            serde_json::from_str(&request.issuer_reference).map_err(|_| ServiceError::InvalidResult)?;
        if fiscal.first() != Some(&original.fiscal_recipe_reference().artifact()?) {
            return Err(ServiceError::InvalidResult);
        }

        if request.digest()?.bytes() != self.request_digest
            || self.evidence_digest == [0; 32]
            || self.materialized_signal_plan.artifact().is_err()
            || request
                .evaluation_starts_at
                .checked_add_nanos(RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1)
                .map_err(|_| ServiceError::InvalidResult)?
                != publication.simulation_cutoff()
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(())
    }

    fn fiscal_artifact_references(&self) -> Result<Vec<ArtifactReference>, ServiceError> {
        if self.fiscal_artifacts.len() < 2
            || self.fiscal_artifacts.len() > 1 + crate::application::HISTORICAL_FISCAL_MAXIMUM_PAGES
        {
            return Err(ServiceError::InvalidResult);
        }
        let references = self
            .fiscal_artifacts
            .iter()
            .map(StoredFiscalArtifactV1::reference)
            .collect::<Result<Vec<_>, _>>()?;
        let mut ids = std::collections::BTreeSet::new();
        for (ordinal, reference) in references.iter().enumerate() {
            if !ids.insert(reference.id())
                || (ordinal > 0
                    && reference.byte_count()
                        > crate::application::HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES)
            {
                return Err(ServiceError::InvalidResult);
            }
        }
        Ok(references)
    }

    pub(super) fn backup_artifacts(&self) -> Result<Vec<ArtifactReference>, ServiceError> {
        self.validate(MAXIMUM_MATERIALIZATION_BYTES)?;
        let mut artifacts = vec![self.materialized_signal_plan.artifact()?];
        artifacts.extend(self.fiscal_artifact_references()?);
        Ok(artifacts)
    }

    fn publication(&self) -> Result<RecommendationBacktestPublicationV1, ServiceError> {
        RecommendationBacktestPublicationV1::try_new(
            self.publication[0],
            self.publication[1],
            self.publication[2],
            self.publication[3],
            self.publication[4],
        )
        .map_err(|_| ServiceError::InvalidResult)
    }
}

impl ProductionGovernedBacktestRepository {
    /// Reads the exact retained report for display, preserving historical publication timing.
    /// Analytical expiry is enforced separately when a new proposal consumes the evidence.
    pub(crate) async fn read_recommendation_report(
        &self,
        inputs: &ProductionGovernedBacktestInputAuthority,
        reference: GovernedRecommendationBacktestReferenceV1,
        as_of: Timestamp,
        historical_reader: &super::super::input_authority::HistoricalRecommendationAlphaProducerReadCapability,
        context: &RequestContext,
    ) -> Result<GovernedRecommendationBacktestReceiptV1, ServiceError> {
        let evidence = self
            .restore_recommendation(inputs, reference, as_of, false, historical_reader, context)
            .await?;
        Ok(GovernedRecommendationBacktestReceiptV1 {
            materialization_artifact: self.materialization_artifact(reference)?,
            fiscal_recipe_artifacts: self
                .fiscal_recipe_artifacts(reference, historical_reader, context)
                .await?,
            reference,
            evidence,
        })
    }

    /// Resolves an exact job-result study identity through the durable terminal index.
    pub(crate) async fn read_recommendation_receipt(
        &self,
        inputs: &ProductionGovernedBacktestInputAuthority,
        evidence_digest: Sha256Digest,
        as_of: Timestamp,
        historical_reader: &super::super::input_authority::HistoricalRecommendationAlphaProducerReadCapability,
        context: &RequestContext,
    ) -> Result<GovernedRecommendationBacktestReceiptV1, ServiceError> {
        let cancellation = context.cancellation().clone();
        let deadline = context.deadline();
        let _call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let reference = self
            .index
            .lock()
            .map_err(|_| ServiceError::Unavailable)?
            .recommendation_entries
            .iter()
            .find(|entry| entry.evidence_digest == evidence_digest.bytes())
            .map(|entry| GovernedRecommendationBacktestReferenceV1 {
                request_digest: Sha256Digest::new(entry.request_digest),
                evidence_digest,
            })
            .ok_or(ServiceError::NotFound)?;
        let evidence = self
            .restore_recommendation(inputs, reference, as_of, false, historical_reader, context)
            .await?;
        Ok(GovernedRecommendationBacktestReceiptV1 {
            materialization_artifact: self.materialization_artifact(reference)?,
            fiscal_recipe_artifacts: self
                .fiscal_recipe_artifacts(reference, historical_reader, context)
                .await?,
            reference,
            evidence,
        })
    }

    /// Executes the installed sequential alpha issuer and durably commits its complete study.
    pub(crate) async fn run_recommendation(
        &self,
        inputs: &ProductionGovernedBacktestInputAuthority,
        issuer: &GovernedRecommendationSignalIssuerV1,
        request: GovernedRecommendationBacktestRequestV1,
        fiscal_reader: &crate::application::research::HistoricalFiscalForecastReadCapability,
        context: &RequestContext,
        prepublish: Option<Arc<dyn GovernedBacktestPrepublishAuthority>>,
    ) -> Result<GovernedRecommendationBacktestReceiptV1, ServiceError> {
        let cancellation = context.cancellation().clone();
        let deadline = context.deadline();
        if issuer.identity().digest() != request.issuer_identity_digest
            || serde_json::to_string(issuer.reference())
                .map_err(|_| ServiceError::InvalidRequest)?
                != request.issuer_reference.as_ref()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let _call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let permit = Arc::new(self.recommendation_permit(&cancellation, deadline).await?);
        let fiscal_recipe_artifacts = fiscal_reader
            .recipe_artifacts(issuer.reference().fiscal_recipe_reference(), context)
            .await?;
        let materialized = inputs
            .materialize_recommendation_input(
                &request.command,
                request.policy,
                request.evaluation_starts_at,
                issuer,
                request.limits,
                Arc::clone(&permit),
                cancellation.clone(),
                deadline,
            )
            .await?;
        let signal_plan = materialized
            .signal_plan()
            .encode_persisted(MAXIMUM_MATERIALIZATION_BYTES)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let materialization_reference = self
            .publish_materialization(
                signal_plan,
                materialized.signal_plan().digest(),
                &cancellation,
                deadline,
            )
            .await?;
        let cutoff = materialized.signal_plan().evaluation_ends_at();
        let now = actual_time()?;
        let expiry = now
            .checked_add_nanos(24 * 60 * 60 * 1_000_000_000)
            .map_err(|_| ServiceError::InvalidResult)?;
        let publication =
            RecommendationBacktestPublicationV1::try_new(cutoff, now, now, now, expiry)
                .map_err(|_| ServiceError::InvalidRequest)?;
        let policy = request.policy;
        let call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let operation = LinkedOperation::new(
            cancellation.clone(),
            self.lifecycle.shutdown_token().clone(),
            deadline,
        );
        let worker_permit = Arc::clone(&permit);
        let worker = tokio::task::spawn_blocking(move || {
            let _call = call;
            let _permit = worker_permit;
            materialized.evaluate(policy, publication, operation.token())
        });
        let evidence = await_blocking(
            worker,
            &cancellation,
            self.lifecycle.shutdown_token(),
            deadline,
        )
        .await?;
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        let now = actual_time()?;
        let expiry = now
            .checked_add_nanos(24 * 60 * 60 * 1_000_000_000)
            .map_err(|_| ServiceError::InvalidResult)?;
        let evidence = evidence.with_publication(
            RecommendationBacktestPublicationV1::try_new(cutoff, now, now, now, expiry)
                .map_err(|_| ServiceError::InvalidResult)?,
        )?;
        let reference = GovernedRecommendationBacktestReferenceV1 {
            request_digest: request.digest()?,
            evidence_digest: evidence.digest(),
        };
        let terminal = StoredRecommendationTerminalV1 {
            request: RequestWire::from_request(&request),
            request_digest: reference.request_digest.bytes(),
            evidence_digest: reference.evidence_digest.bytes(),
            publication: [cutoff, now, now, now, expiry],
            materialized_signal_plan: materialization_reference,
            fiscal_artifacts: fiscal_recipe_artifacts
                .iter()
                .map(StoredFiscalArtifactV1::from_reference)
                .collect(),
        };
        self.publish_recommendation(
            terminal,
            cancellation,
            deadline,
            prepublish,
            Arc::clone(&permit),
        )
        .await?;
        *self
            .recommendation_cache
            .lock()
            .map_err(|_| ServiceError::Unavailable)? =
            Some((reference.evidence_digest.bytes(), evidence.clone()));
        Ok(GovernedRecommendationBacktestReceiptV1 {
            materialization_artifact: self.materialization_artifact(reference)?,
            fiscal_recipe_artifacts,
            reference,
            evidence,
        })
    }

    /// Re-pins the retained exact dataset and replays the retained signal population.
    /// No latest selection, reissuance, or caller performance values participate in this read.
    pub(crate) async fn read_recommendation(
        &self,
        inputs: &ProductionGovernedBacktestInputAuthority,
        reference: GovernedRecommendationBacktestReferenceV1,
        as_of: Timestamp,
        historical_reader: &super::super::input_authority::HistoricalRecommendationAlphaProducerReadCapability,
        context: &RequestContext,
    ) -> Result<GovernedRecommendationBacktestEvidenceV1, ServiceError> {
        self.restore_recommendation(inputs, reference, as_of, true, historical_reader, context)
            .await
    }

    async fn restore_recommendation(
        &self,
        inputs: &ProductionGovernedBacktestInputAuthority,
        reference: GovernedRecommendationBacktestReferenceV1,
        as_of: Timestamp,
        require_current: bool,
        historical_reader: &super::super::input_authority::HistoricalRecommendationAlphaProducerReadCapability,
        context: &RequestContext,
    ) -> Result<GovernedRecommendationBacktestEvidenceV1, ServiceError> {
        let cancellation = context.cancellation().clone();
        let deadline = context.deadline();
        let _call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let permit = Arc::new(self.recommendation_permit(&cancellation, deadline).await?);
        let retained = self
            .index
            .lock()
            .map_err(|_| ServiceError::Unavailable)?
            .recommendation_entries
            .iter()
            .find(|entry| entry.evidence_digest == reference.evidence_digest.bytes())
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        if retained.request_digest != reference.request_digest.bytes() {
            return Err(ServiceError::InvalidRequest);
        }
        retained.validate(self.limits.maximum_index_bytes)?;
        let publication = retained.publication()?;
        if publication.available_at() > as_of
            || (require_current && publication.expires_at() <= as_of)
        {
            return Err(ServiceError::Unavailable);
        }
        let expected_materialized_digest = retained.materialized_signal_plan.digest();
        let request = retained.request.clone().into_request()?;
        let original: super::super::input_authority::HistoricalRecommendationAlphaProducerReference =
            serde_json::from_str(&request.issuer_reference).map_err(|_|ServiceError::InvalidResult)?;
        let reopened = historical_reader.read_reference(&original, context).await?;
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        if reopened.reference() != &original
            || reopened.identity().digest() != request.issuer_identity_digest
            || serde_json::to_string(reopened.reference())
                .map_err(|_| ServiceError::InvalidResult)?
                != request.issuer_reference.as_ref()
        {
            return Err(ServiceError::InvalidResult);
        }
        // Reopening authenticates the original models, source pages and issuer clocks. It never
        // materializes replacement instructions; persisted original bytes are restored below.
        drop(reopened);
        let original_fiscal = historical_reader
            .fiscal_reader()
            .recipe_artifacts(original.fiscal_recipe_reference(), context)
            .await?;
        if original_fiscal != retained.fiscal_artifact_references()? {
            return Err(ServiceError::InvalidResult);
        }

        let payload = self
            .read_materialization(&retained.materialized_signal_plan, &cancellation, deadline)
            .await?;
        let materialized = inputs
            .restore_recommendation_input(
                &request.command,
                request.policy,
                request.limits,
                payload.content(),
                MAXIMUM_MATERIALIZATION_BYTES,
                Arc::clone(&permit),
                cancellation.clone(),
                deadline,
            )
            .await?;
        drop(payload);
        if materialized.signal_plan().digest() != expected_materialized_digest
            || materialized.signal_plan().issuer_identity().digest()
                != request.issuer_identity_digest
        {
            return Err(ServiceError::InvalidResult);
        }
        if let Some((identity, evidence)) = &*self
            .recommendation_cache
            .lock()
            .map_err(|_| ServiceError::Unavailable)?
        {
            if *identity == reference.evidence_digest.bytes()
                && evidence.digest() == reference.evidence_digest
                && evidence.materialized_signal_plan_digest() == materialized.signal_plan().digest()
                && evidence.dataset_identity() == materialized.dataset_identity()
            {
                return Ok(evidence.clone());
            }
        }
        let call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let operation = LinkedOperation::new(
            cancellation.clone(),
            self.lifecycle.shutdown_token().clone(),
            deadline,
        );
        let worker_permit = Arc::clone(&permit);
        let worker = tokio::task::spawn_blocking(move || {
            let _call = call;
            let _permit = worker_permit;
            materialized.evaluate(request.policy, publication, operation.token())
        });
        let evidence = await_blocking(
            worker,
            &cancellation,
            self.lifecycle.shutdown_token(),
            deadline,
        )
        .await?;
        if evidence.digest() != reference.evidence_digest {
            return Err(ServiceError::InvalidResult);
        }
        *self
            .recommendation_cache
            .lock()
            .map_err(|_| ServiceError::Unavailable)? =
            Some((reference.evidence_digest.bytes(), evidence.clone()));
        Ok(evidence)
    }

    fn materialization_artifact(
        &self,
        reference: GovernedRecommendationBacktestReferenceV1,
    ) -> Result<ArtifactReference, ServiceError> {
        let index = self.index.lock().map_err(|_| ServiceError::Unavailable)?;
        let retained = index
            .recommendation_entries
            .iter()
            .find(|entry| {
                entry.evidence_digest == reference.evidence_digest.bytes()
                    && entry.request_digest == reference.request_digest.bytes()
            })
            .ok_or(ServiceError::NotFound)?;
        retained.materialized_signal_plan.artifact()
    }

    async fn fiscal_recipe_artifacts(
        &self,
        reference: GovernedRecommendationBacktestReferenceV1,
        historical_reader: &super::super::input_authority::HistoricalRecommendationAlphaProducerReadCapability,
        context: &RequestContext,
    ) -> Result<Vec<ArtifactReference>, ServiceError> {
        let original = {
            let index = self.index.lock().map_err(|_| ServiceError::Unavailable)?;
            let retained = index
                .recommendation_entries
                .iter()
                .find(|entry| {
                    entry.evidence_digest == reference.evidence_digest.bytes()
                        && entry.request_digest == reference.request_digest.bytes()
                })
                .ok_or(ServiceError::NotFound)?;
            serde_json::from_str::<
                super::super::input_authority::HistoricalRecommendationAlphaProducerReference,
            >(&retained.request.issuer_reference)
            .map_err(|_| ServiceError::InvalidResult)?
        };
        original.validate_identity()?;
        historical_reader
            .fiscal_reader()
            .recipe_artifacts(original.fiscal_recipe_reference(), context)
            .await
    }

    async fn recommendation_permit(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, ServiceError> {
        tokio::select! {
            () = cancellation.cancelled() => Err(ServiceError::Cancelled),
            () = self.lifecycle.shutdown_token().cancelled() => Err(ServiceError::Unavailable),
            result = tokio::time::timeout_at(deadline.into(),
                Arc::clone(&self.recommendation_gate).acquire_owned()) => {
                result.map_err(|_| ServiceError::DeadlineExceeded)?
                    .map_err(|_| ServiceError::Unavailable)
            }
        }
    }

    async fn publish_recommendation(
        &self,
        terminal: StoredRecommendationTerminalV1,
        cancellation: CancellationToken,
        deadline: Instant,
        prepublish: Option<Arc<dyn GovernedBacktestPrepublishAuthority>>,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<(), ServiceError> {
        terminal.validate(self.limits.maximum_index_bytes)?;
        let call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let index = Arc::clone(&self.index);
        let store = Arc::clone(&self.store);
        let lifecycle = Arc::clone(&self.lifecycle);
        let resolver = Arc::clone(&self.resolver);
        let limits = self.limits;
        let worker_cancellation = cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _call = call;
            let _permit = permit;
            ensure_operation_live(&worker_cancellation, &lifecycle, deadline)?;
            let mut index = index.lock().map_err(|_| ServiceError::Unavailable)?;
            if let Some(existing) = index
                .recommendation_entries
                .iter()
                .find(|entry| entry.evidence_digest == terminal.evidence_digest)
            {
                if serde_json::to_vec(existing).map_err(|_| ServiceError::InvalidResult)?
                    != serde_json::to_vec(&terminal).map_err(|_| ServiceError::InvalidResult)?
                {
                    return Err(ServiceError::InvalidResult);
                }
                if let Some(authority) = &prepublish {
                    authority.validate_prepublish()?;
                    authority.commit_succeeded();
                }
                return Ok(());
            }
            if index.entries.len() + index.recommendation_entries.len() >= limits.maximum_terminals
            {
                return Err(ServiceError::ResourceExhausted);
            }
            let mut next = index.clone();
            next.recommendation_entries
                .try_reserve_exact(1)
                .map_err(|_| ServiceError::ResourceExhausted)?;
            next.recommendation_entries.push(terminal);
            next.recommendation_entries
                .sort_unstable_by_key(StoredRecommendationTerminalV1::evidence_digest);
            let bytes = next
                .encode(limits)
                .map_err(|_| ServiceError::ResourceExhausted)?;
            ensure_operation_live(&worker_cancellation, &lifecycle, deadline)?;
            if let Some(authority) = &prepublish {
                authority.validate_prepublish()?;
            }
            if store.store(&bytes).is_err() {
                lifecycle.begin_shutdown();
                resolver.begin_shutdown();
                return Err(ServiceError::Unavailable);
            }
            *index = next;
            if let Some(authority) = &prepublish {
                authority.commit_succeeded();
            }
            Ok(())
        });
        await_blocking(
            worker,
            &cancellation,
            self.lifecycle.shutdown_token(),
            deadline,
        )
        .await
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RequestWire {
    command: CommandWire,
    issuer_identity_digest: [u8; 32],
    issuer_reference: Box<str>,
    study_qualification: BacktestStudyQualification,
    subject: InstrumentId,
    benchmark: InstrumentId,
    benchmark_approval: [u8; 32],
    accompanying_benchmark: InstrumentId,
    accompanying_benchmark_approval: [u8; 32],
    raw_price_evidence_digest: [u8; 32],
    corporate_action_content_digest: [u8; 32],
    corporate_action_audit_digest: [u8; 32],
    corporate_action_coverage_starts_at: Timestamp,
    execution_basis: u8,
    currency: Currency,
    entry_lag_nanos: i64,
    exit_lag_nanos: i64,
    seed: u64,
    evaluation_starts_at: Timestamp,
    limits: [usize; 5],
}

impl RequestWire {
    fn from_request(request: &GovernedRecommendationBacktestRequestV1) -> Self {
        let policy = request.policy;
        let limits = request.limits;
        Self {
            command: CommandWire::from_command(&request.command),
            issuer_identity_digest: request.issuer_identity_digest.bytes(),
            issuer_reference: request.issuer_reference.clone(),
            study_qualification: policy.study_qualification(),
            subject: policy.subject_instrument_id(),
            benchmark: policy.benchmark().instrument_id(),
            benchmark_approval: policy.benchmark().approval_digest().bytes(),
            accompanying_benchmark: policy.accompanying_benchmark().instrument_id(),
            accompanying_benchmark_approval: policy
                .accompanying_benchmark()
                .approval_digest()
                .bytes(),
            raw_price_evidence_digest: policy.raw_price_evidence_digest().bytes(),
            corporate_action_content_digest: policy.corporate_action_content_digest().bytes(),
            corporate_action_audit_digest: policy.corporate_action_audit_digest().bytes(),
            corporate_action_coverage_starts_at: policy.corporate_action_coverage_starts_at(),
            execution_basis: match policy.execution_basis() {
                BacktestExecutionBasis::ObservedQuoteDepth => 1,
                BacktestExecutionBasis::CompletedDailyBar => 2,
            },
            currency: policy.reporting_currency(),
            entry_lag_nanos: policy.maximum_entry_lag_nanos(),
            exit_lag_nanos: policy.maximum_exit_lag_nanos(),
            seed: policy.seed(),
            evaluation_starts_at: request.evaluation_starts_at,
            limits: [
                limits.max_folds(),
                limits.max_signals(),
                limits.max_equity_points_per_outcome(),
                limits.max_total_equity_points(),
                limits.max_observation_visits(),
            ],
        }
    }

    fn into_request(self) -> Result<GovernedRecommendationBacktestRequestV1, ServiceError> {
        if self.issuer_identity_digest == [0; 32] || self.issuer_reference.len() > 64 * 1024 {
            return Err(ServiceError::InvalidResult);
        }
        let reference: super::super::input_authority::HistoricalRecommendationAlphaProducerReference =
            serde_json::from_str(&self.issuer_reference).map_err(|_|ServiceError::InvalidResult)?;
        reference.validate_identity()?;
        if reference.issuer_identity_digest().bytes() != self.issuer_identity_digest
            || serde_json::to_string(&reference).map_err(|_| ServiceError::InvalidResult)?
                != self.issuer_reference.as_ref()
        {
            return Err(ServiceError::InvalidResult);
        }
        let one_lot = QuantityLots::new(1).map_err(|_| ServiceError::InvalidResult)?;
        let policy = RecommendationBacktestPolicyV1::try_new(RecommendationBacktestPolicyV1Input {
            study_qualification: self.study_qualification,
            subject_instrument_id: self.subject,
            benchmark: RecommendationBenchmarkPolicyV1::try_new(
                self.benchmark,
                Sha256Digest::new(self.benchmark_approval),
            )
            .map_err(|_| ServiceError::InvalidResult)?,
            accompanying_benchmark: RecommendationBenchmarkPolicyV1::try_new(
                self.accompanying_benchmark,
                Sha256Digest::new(self.accompanying_benchmark_approval),
            )
            .map_err(|_| ServiceError::InvalidResult)?,
            reporting_currency: self.currency,
            raw_price_evidence_digest: Sha256Digest::new(self.raw_price_evidence_digest),
            corporate_action_content_digest: Sha256Digest::new(
                self.corporate_action_content_digest,
            ),
            corporate_action_audit_digest: Sha256Digest::new(self.corporate_action_audit_digest),
            corporate_action_coverage_starts_at: self.corporate_action_coverage_starts_at,
            execution_basis: match self.execution_basis {
                1 => BacktestExecutionBasis::ObservedQuoteDepth,
                2 => BacktestExecutionBasis::CompletedDailyBar,
                _ => return Err(ServiceError::InvalidResult),
            },
            subject_quantity: one_lot,
            benchmark_quantity: one_lot,
            maximum_entry_lag_nanos: self.entry_lag_nanos,
            maximum_exit_lag_nanos: self.exit_lag_nanos,
            execution_assumptions: recommendation_conservative_execution_assumptions_v1()
                .map_err(|_| ServiceError::InvalidResult)?,
            seed: self.seed,
        })
        .map_err(|_| ServiceError::InvalidResult)?;
        let limits = RecommendationBacktestLimits::try_new(RecommendationBacktestLimitsInput {
            max_folds: self.limits[0],
            max_signals: self.limits[1],
            max_equity_points_per_outcome: self.limits[2],
            max_total_equity_points: self.limits[3],
            max_observation_visits: self.limits[4],
        })
        .map_err(|_| ServiceError::InvalidResult)?;
        Ok(GovernedRecommendationBacktestRequestV1 {
            issuer_identity_digest: Sha256Digest::new(self.issuer_identity_digest),
            issuer_reference: self.issuer_reference,
            command: self
                .command
                .into_command()
                .map_err(|_| ServiceError::InvalidResult)?,
            policy,
            evaluation_starts_at: self.evaluation_starts_at,
            limits,
        })
    }
}

fn actual_time() -> Result<Timestamp, ServiceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Internal)?
        .as_nanos();
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(nanos).map_err(|_| ServiceError::Internal)?,
    ))
}
