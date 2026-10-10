//! Coordinate-confined historical alpha using the same admitted forecast and price-zone math.
//!
//! This producer receives neither held-out labels nor realized prices. Its instructions are
//! study evidence only; they cannot enter live proposal or execution admission.

use std::{
    fmt,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::ResearchService;
use market_squawk_backtesting::{
    RECOMMENDATION_OOS_FOLD_COUNT_V1, RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1,
    RECOMMENDATION_TARGET_HORIZON_NANOS_V1, RecommendationOosFoldV1,
    RecommendationSignalInformationSetV1, RecommendationSignalInstructionV1,
    RecommendationSignalIssuanceV1, RecommendationSignalIssuerIdentityV1,
    RecommendationSignalPlanMaterializerV1, RecommendationSignalUnavailableReasonV1,
};
use market_squawk_data::{
    DatasetBuildPurpose, ResearchUse, ResearchUseCatalogError, ResearchUseLimits,
    ResearchUseRequest, Sha256Digest,
};
use market_squawk_decisions::{InvestmentProposalAuthority, RecommendationAlphaDecision};
use market_squawk_domain::{
    AccountId, HistoricalStudyBasis, InstrumentId, Money, SourceIdentifier, Timestamp,
};
use market_squawk_services::{RequestContext, ServiceError};
use market_squawk_valuation::ActorId;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::application::{
    analytical_profile::{
        self, AnalyticalModelBundlePolicy, AnalyticalProfileResolution, ValidatedAnalyticalProfile,
    },
    fair_value::{
        AutomaticForecastValuationRequest, FairValueDomainService,
        HistoricalStudyValuationReadCapability,
    },
    lifecycle::WorkspaceRuntimeIdentity,
    model::{
        ForecastStudyRuntimeReference, SelectedForecastRuntime,
        forecast_preparation::ForecastPreparationAuthority, runtime::ProductionModelRuntime,
    },
    research::{
        RecommendationBenchmarkSelection, RecommendationBenchmarkSelectionReadCapability,
        RecommendationBenchmarkSelectionReference,
    },
};

use crate::application::decision::recommendation::monetary_forecast_cases;

const REFERENCE_VERSION: u16 = 1;
const MAXIMUM_RECEIPT_BYTES: usize = 16_384;
const PRODUCER: &str = "recommendation-financial-alpha";
const SEMANTICS: &str = "completed-close-365d-three-fold-all-methods-v1";

/// Exact restart coordinates. Bytes never grant model, benchmark, source-use or profile authority.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoricalRecommendationAlphaProducerReference {
    version: u16,
    runtimes: [ForecastStudyRuntimeReference; RECOMMENDATION_OOS_FOLD_COUNT_V1],
    benchmarks: RecommendationBenchmarkSelectionReference,
    profile: AnalyticalProfileResolution,
    subject_instrument_id: InstrumentId,
    account_id: AccountId,
    calculated_by: Box<str>,
    evaluation_starts_at: Timestamp,
    selected_at: Timestamp,
    fiscal_selection_digest: [u8; 32],
    fiscal_recipe: crate::application::research::HistoricalFiscalRecipeReference,
    issuer_identity_digest: [u8; 32],
}

impl HistoricalRecommendationAlphaProducerReference {
    pub(crate) const fn fiscal_recipe_reference(
        &self,
    ) -> &crate::application::research::HistoricalFiscalRecipeReference {
        &self.fiscal_recipe
    }

    /// Inert selected financial configuration, authenticated by the original retained request.
    pub(crate) const fn profile_resolution(&self) -> &AnalyticalProfileResolution {
        &self.profile
    }
    pub(crate) const fn subject_instrument_id(&self) -> InstrumentId {
        self.subject_instrument_id
    }
    pub(crate) const fn runtime_references(
        &self,
    ) -> &[ForecastStudyRuntimeReference; RECOMMENDATION_OOS_FOLD_COUNT_V1] {
        &self.runtimes
    }
    pub(crate) fn fiscal_recipe_artifact(
        &self,
    ) -> Result<market_squawk_services::ArtifactReference, ServiceError> {
        self.fiscal_recipe.artifact()
    }
    /// Checks the exact canonical reference commitment; this does not reopen model/source authority.
    pub(crate) fn validate_identity(&self) -> Result<(), ServiceError> {
        if self.version != REFERENCE_VERSION || self.issuer_identity_digest == [0; 32] {
            return Err(ServiceError::InvalidResult);
        }
        let mut reference = self.clone();
        reference.issuer_identity_digest = [0; 32];
        let bytes = serde_json::to_vec(&reference).map_err(|_| ServiceError::InvalidResult)?;
        if bytes.len() > 64 * 1024 {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/historical-alpha-selection/v1\0");
        hash.update(bytes);
        let identity = RecommendationSignalIssuerIdentityV1::try_new(
            SourceIdentifier::try_from(PRODUCER).map_err(|_| ServiceError::Internal)?,
            SourceIdentifier::try_from(SEMANTICS).map_err(|_| ServiceError::Internal)?,
            Sha256Digest::new(hash.finalize().into()),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        if identity.digest().bytes() != self.issuer_identity_digest {
            return Err(ServiceError::InvalidResult);
        }
        Ok(())
    }
    pub(crate) const fn issuer_identity_digest(&self) -> Sha256Digest {
        Sha256Digest::new(self.issuer_identity_digest)
    }
}

/// Concrete immutable selection for one requested study, not a global issuer or callback factory.
pub(crate) struct HistoricalRecommendationAlphaProducer {
    reference: HistoricalRecommendationAlphaProducerReference,
    identity: RecommendationSignalIssuerIdentityV1,
    runtimes: [SelectedForecastRuntime; RECOMMENDATION_OOS_FOLD_COUNT_V1],
    folds: [RecommendationOosFoldV1; RECOMMENDATION_OOS_FOLD_COUNT_V1],
    benchmarks: RecommendationBenchmarkSelection,
    profile: ValidatedAnalyticalProfile,
    research: Arc<ResearchService>,
    valuation: Arc<FairValueDomainService>,
    valuation_inputs: Arc<HistoricalStudyValuationReadCapability>,
    calendars: crate::application::market_calendar::CompletedMarketSessionReadCapability,
}

impl fmt::Debug for HistoricalRecommendationAlphaProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoricalRecommendationAlphaProducer")
            .field("identity", &self.identity)
            .field("folds", &self.folds)
            .finish_non_exhaustive()
    }
}

/// Reopens the same actual authorities through their existing production readers.
pub(crate) struct HistoricalRecommendationAlphaProducerReadCapability {
    runtime: Arc<ProductionModelRuntime>,
    forecast_preparation: Arc<ForecastPreparationAuthority>,
    workspace: WorkspaceRuntimeIdentity,
    benchmarks: RecommendationBenchmarkSelectionReadCapability,
    research: Arc<ResearchService>,
    valuation: Arc<FairValueDomainService>,
    fiscal_reader: Arc<crate::application::research::HistoricalFiscalForecastReadCapability>,
    macro_reader: crate::application::research::MacroContextReadCapability,
    calendars: crate::application::market_calendar::CompletedMarketSessionReadCapability,
}

impl HistoricalRecommendationAlphaProducerReadCapability {
    pub(crate) fn fiscal_reader(
        &self,
    ) -> &Arc<crate::application::research::HistoricalFiscalForecastReadCapability> {
        &self.fiscal_reader
    }

    pub(crate) fn new(
        runtime: Arc<ProductionModelRuntime>,
        forecast_preparation: Arc<ForecastPreparationAuthority>,
        workspace: WorkspaceRuntimeIdentity,
        benchmarks: RecommendationBenchmarkSelectionReadCapability,
        research: Arc<ResearchService>,
        valuation: Arc<FairValueDomainService>,
        fiscal_reader: Arc<crate::application::research::HistoricalFiscalForecastReadCapability>,
        macro_reader: crate::application::research::MacroContextReadCapability,
        calendars: crate::application::market_calendar::CompletedMarketSessionReadCapability,
    ) -> Self {
        Self {
            runtime,
            forecast_preparation,
            workspace,
            benchmarks,
            research,
            valuation,
            fiscal_reader,
            macro_reader,
            calendars,
        }
    }

    pub(crate) async fn read_reference(
        &self,
        reference: &HistoricalRecommendationAlphaProducerReference,
        context: &RequestContext,
    ) -> Result<HistoricalRecommendationAlphaProducer, ServiceError> {
        ensure_live(context)?;
        if reference.version != REFERENCE_VERSION || reference.selected_at > wall_now()? {
            return Err(ServiceError::InvalidRequest);
        }
        let catalog = match reference.profile.configuration.model_bundle_policy {
            AnalyticalModelBundlePolicy::BestAdmittedCalibratedMeanV1 => None,
            AnalyticalModelBundlePolicy::Exact { model_token } => Some(
                self.forecast_preparation
                    .catalog_for_model_token(
                        context.origin().ok_or(ServiceError::InvalidRequest)?,
                        self.workspace,
                        // Revalidate the current compatible model inventory, as financial settings
                        // does. Historical coordinate authority is reopened separately below; no
                        // current-feature selection exists in this original issuer reference.
                        wall_now()?,
                        None,
                        model_token,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(|_| ServiceError::Unavailable)?,
            ),
        };
        let profile = analytical_profile::revalidate(&reference.profile, catalog.as_ref())
            .map_err(|_| ServiceError::Unavailable)?;
        let valuation_inputs = Arc::new(
            HistoricalStudyValuationReadCapability::reopen(
                self.macro_reader.clone(),
                self.calendars.clone(),
                Arc::clone(&self.fiscal_reader),
                &reference.fiscal_recipe,
                &profile,
                context,
            )
            .await?,
        );
        if valuation_inputs.fiscal_identity()?.bytes() != reference.fiscal_selection_digest {
            return Err(ServiceError::InvalidResult);
        }
        let benchmarks = self
            .benchmarks
            .read_reference(
                &reference.benchmarks,
                context.deadline(),
                context.cancellation(),
            )?
            .ok_or(ServiceError::Unavailable)?;
        let [first, second, third] = &reference.runtimes;
        let runtimes = [
            self.runtime.read_forecast_runtime_reference(first)?,
            self.runtime.read_forecast_runtime_reference(second)?,
            self.runtime.read_forecast_runtime_reference(third)?,
        ];
        let actual = HistoricalRecommendationAlphaProducer::admit(
            runtimes,
            benchmarks,
            profile,
            reference.subject_instrument_id,
            reference.account_id,
            ActorId::try_from(reference.calculated_by.as_ref())
                .map_err(|_| ServiceError::InvalidRequest)?,
            reference.selected_at,
            Arc::clone(&self.research),
            Arc::clone(&self.valuation),
            valuation_inputs,
            self.calendars.clone(),
        )?;
        ensure_live(context)?;
        if actual.reference != *reference {
            return Err(ServiceError::InvalidResult);
        }
        Ok(actual)
    }
}

impl HistoricalRecommendationAlphaProducer {
    /// Called after the actual cohort, fixed benchmark pair, profile and three fold models have
    /// been selected. Evaluation boundaries are recovered from the admitted first model.
    #[allow(
        clippy::too_many_arguments,
        reason = "each existing authority remains explicit"
    )]
    pub(crate) fn try_new(
        runtimes: [SelectedForecastRuntime; RECOMMENDATION_OOS_FOLD_COUNT_V1],
        benchmarks: RecommendationBenchmarkSelection,
        profile: ValidatedAnalyticalProfile,
        subject_instrument_id: InstrumentId,
        account_id: AccountId,
        calculated_by: ActorId,
        research: Arc<ResearchService>,
        valuation: Arc<FairValueDomainService>,
        valuation_inputs: Arc<HistoricalStudyValuationReadCapability>,
        calendars: crate::application::market_calendar::CompletedMarketSessionReadCapability,
    ) -> Result<Self, ServiceError> {
        Self::admit(
            runtimes,
            benchmarks,
            profile,
            subject_instrument_id,
            account_id,
            calculated_by,
            wall_now()?,
            research,
            valuation,
            valuation_inputs,
            calendars,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "original selection clock is retained on reopen"
    )]
    fn admit(
        runtimes: [SelectedForecastRuntime; RECOMMENDATION_OOS_FOLD_COUNT_V1],
        benchmarks: RecommendationBenchmarkSelection,
        profile: ValidatedAnalyticalProfile,
        subject_instrument_id: InstrumentId,
        account_id: AccountId,
        calculated_by: ActorId,
        selected_at: Timestamp,
        research: Arc<ResearchService>,
        valuation: Arc<FairValueDomainService>,
        valuation_inputs: Arc<HistoricalStudyValuationReadCapability>,
        calendars: crate::application::market_calendar::CompletedMarketSessionReadCapability,
    ) -> Result<Self, ServiceError> {
        if profile.recommendation_policy().horizon_nanos() != RECOMMENDATION_TARGET_HORIZON_NANOS_V1
            || subject_instrument_id == benchmarks.primary().instrument_id()
            || subject_instrument_id == benchmarks.accompanying().instrument_id()
            || benchmarks.selected_at() > selected_at
        {
            return Err(ServiceError::InvalidRequest);
        }
        let first_dataset = runtimes[0].training_dataset();
        let study = first_dataset
            .study_policy()
            .ok_or(ServiceError::Unavailable)?;
        let snapshot = first_dataset
            .source_snapshot_digest()
            .ok_or(ServiceError::Unavailable)?;
        let [_, first_calibration_end, _] = first_dataset
            .split_policy()
            .timestamp_boundaries()
            .ok_or(ServiceError::Unavailable)?;
        let evaluation_starts_at = first_calibration_end
            .checked_add_nanos(1)
            .map_err(|_| ServiceError::InvalidResult)?;
        if study.purpose() != DatasetBuildPurpose::Training
            || study.snapshot_as_of() > selected_at
            || study
                .target_horizon()
                .exact_elapsed()
                .map(|horizon| horizon.as_nanos())
                != Some(RECOMMENDATION_TARGET_HORIZON_NANOS_V1 as u128)
            || (study.basis() == HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                && study.decision_lag() != Some(std::time::Duration::ZERO))
            || (study.basis() == HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                && !profile
                    .recommendation_policy()
                    .parameters()
                    .allow_retrospective_studies)
        {
            return Err(ServiceError::Unavailable);
        }
        let folds: [RecommendationOosFoldV1; RECOMMENDATION_OOS_FOLD_COUNT_V1] =
            RecommendationSignalPlanMaterializerV1::oos_folds(evaluation_starts_at)
                .map_err(|_| ServiceError::InvalidRequest)?
                .try_into()
                .map_err(|_| ServiceError::InvalidResult)?;
        for (runtime, fold) in runtimes.iter().zip(&folds) {
            let data = runtime.training_dataset();
            let policy = data.study_policy().ok_or(ServiceError::Unavailable)?;
            let [train_end, calibration_end, evaluation_end] = data
                .split_policy()
                .timestamp_boundaries()
                .ok_or(ServiceError::Unavailable)?;
            let expected_train_end = fold
                .starts_at()
                .checked_add_nanos(-RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1)
                .and_then(|at| at.checked_add_nanos(-1))
                .map_err(|_| ServiceError::InvalidRequest)?;
            let expected_calibration_end = fold
                .starts_at()
                .checked_add_nanos(-1)
                .map_err(|_| ServiceError::InvalidRequest)?;
            let expected_evaluation_end = fold
                .ends_at()
                .checked_add_nanos(-1)
                .map_err(|_| ServiceError::InvalidRequest)?;
            let calibration = runtime
                .calibration_window()
                .ok_or(ServiceError::Unavailable)?;
            let horizon = runtime
                .output_binding()
                .expected_terminal_price_horizon_nanos()
                .or_else(|| {
                    runtime
                        .output_binding()
                        .expected_arithmetic_return_horizon_nanos()
                });
            if policy != study
                || data.source_snapshot_digest() != Some(snapshot)
                || train_end != expected_train_end
                || calibration_end != expected_calibration_end
                || evaluation_end != expected_evaluation_end
                || runtime
                    .training_period()
                    .end()
                    .is_none_or(|end| end > train_end)
                || calibration.end().is_none_or(|end| end > fold.starts_at())
                || calibration.start().is_none_or(|start| start <= train_end)
                || runtime.selected_at() > selected_at
                || horizon.map(|value| value.get())
                    != u64::try_from(RECOMMENDATION_TARGET_HORIZON_NANOS_V1).ok()
            {
                return Err(ServiceError::Unavailable);
            }
        }
        let mut reference = HistoricalRecommendationAlphaProducerReference {
            version: REFERENCE_VERSION,
            runtimes: runtimes
                .each_ref()
                .map(|runtime| runtime.reference().clone()),
            benchmarks: benchmarks.reference().clone(),
            profile: profile.resolution().clone(),
            subject_instrument_id,
            account_id,
            calculated_by: calculated_by.as_str().into(),
            evaluation_starts_at,
            selected_at,
            fiscal_selection_digest: valuation_inputs.fiscal_identity()?.bytes(),
            fiscal_recipe: valuation_inputs.reference().clone(),
            issuer_identity_digest: [0; 32],
        };
        let bytes = serde_json::to_vec(&reference).map_err(|_| ServiceError::InvalidResult)?;
        if bytes.len() > 64 * 1024 {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/historical-alpha-selection/v1\0");
        hash.update(&bytes);
        let identity = RecommendationSignalIssuerIdentityV1::try_new(
            SourceIdentifier::try_from(PRODUCER).map_err(|_| ServiceError::Internal)?,
            SourceIdentifier::try_from(SEMANTICS).map_err(|_| ServiceError::Internal)?,
            Sha256Digest::new(hash.finalize().into()),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        reference.issuer_identity_digest = identity.digest().bytes();
        Ok(Self {
            reference,
            identity,
            runtimes,
            folds,
            benchmarks,
            profile,
            research,
            valuation,
            valuation_inputs,
            calendars,
        })
    }

    pub(super) fn source_snapshot_digest(&self) -> Result<Sha256Digest, ServiceError> {
        // The constructor admitted this nonempty identity for every exact fold.
        self.runtimes[0]
            .training_dataset()
            .source_snapshot_digest()
            .ok_or(ServiceError::InvalidResult)
    }
    pub(super) fn snapshot_as_of(&self) -> Result<Timestamp, ServiceError> {
        self.runtimes[0]
            .training_dataset()
            .study_policy()
            .map(|policy| policy.snapshot_as_of())
            .ok_or(ServiceError::InvalidResult)
    }
    pub(super) fn study_basis(&self) -> Result<HistoricalStudyBasis, ServiceError> {
        self.runtimes[0]
            .training_dataset()
            .study_policy()
            .map(|policy| policy.basis())
            .ok_or(ServiceError::InvalidResult)
    }
    pub(super) const fn account_id(&self) -> AccountId {
        self.reference.account_id
    }

    pub(crate) const fn subject_instrument_id(&self) -> InstrumentId {
        self.reference.subject_instrument_id
    }
    pub(crate) const fn evaluation_starts_at(&self) -> Timestamp {
        self.reference.evaluation_starts_at
    }
    pub(crate) const fn benchmarks(&self) -> &RecommendationBenchmarkSelection {
        &self.benchmarks
    }
    pub(crate) const fn identity(&self) -> &RecommendationSignalIssuerIdentityV1 {
        &self.identity
    }
    pub(crate) const fn reference(&self) -> &HistoricalRecommendationAlphaProducerReference {
        &self.reference
    }

    pub(crate) async fn issue(
        &self,
        information: &RecommendationSignalInformationSetV1<'_>,
        context: &RequestContext,
    ) -> Result<RecommendationSignalIssuanceV1, ServiceError> {
        ensure_live(context)?;
        let current = information.current();
        let subject = current.subject();
        let benchmark = current.benchmark();
        let coordinate = subject
            .input_coordinate()
            .ok_or(ServiceError::InvalidRequest)?;
        let epoch = coordinate.epoch();
        let origin = epoch.target_origin().ok_or(ServiceError::InvalidRequest)?;
        let target = epoch.target_at().ok_or(ServiceError::InvalidRequest)?;
        let decision_at = epoch.decision_at().ok_or(ServiceError::InvalidRequest)?;
        if epoch.market_bar().is_none() {
            return Err(ServiceError::InvalidRequest);
        }
        let benchmark_epoch = benchmark
            .input_epoch()
            .ok_or(ServiceError::InvalidRequest)?;
        if subject.instrument_id() != self.reference.subject_instrument_id
            || benchmark.instrument_id() != self.benchmarks.primary().instrument_id()
            || epoch.instrument_id() != subject.instrument_id()
            || epoch.basis() != information.basis()
            || epoch.snapshot_as_of() != information.snapshot_as_of()
            || epoch.source_snapshot_digest() != information.source_snapshot_digest()
            || epoch.limitations() != information.limitations()
            || epoch.source_selection_as_of() != information.source_selection_as_of()
            || decision_at != information.signal_at()
            || origin != information.target_origin()
            || target != information.target_at()
            || benchmark_epoch.basis() != epoch.basis()
            || benchmark_epoch.snapshot_as_of() != epoch.snapshot_as_of()
            || benchmark_epoch.source_snapshot_digest() != epoch.source_snapshot_digest()
            || benchmark_epoch.limitations() != epoch.limitations()
            || benchmark_epoch.source_selection_as_of() != epoch.source_selection_as_of()
            || benchmark_epoch.decision_at() != epoch.decision_at()
            || benchmark_epoch.target_origin() != epoch.target_origin()
            || benchmark_epoch.target_at() != epoch.target_at()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let fold_index = self
            .folds
            .iter()
            .position(|fold| {
                fold.starts_at() <= information.signal_at()
                    && information.signal_at() < fold.ends_at()
            })
            .ok_or(ServiceError::InvalidRequest)?;
        let runtime = &self.runtimes[fold_index];
        let training = runtime.training_dataset();
        let study = training
            .study_policy()
            .ok_or(ServiceError::InvalidRequest)?;
        if training.source_snapshot_digest() != Some(epoch.source_snapshot_digest())
            || training.universe_digest() != coordinate.dataset().universe_digest()
            || study.basis() != epoch.basis()
            || study.snapshot_as_of() != epoch.snapshot_as_of()
            || study.limitations() != epoch.limitations()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut receipt = HistoricalAlphaReceipt {
            version: 1,
            issuer_identity: self.identity.digest().bytes(),
            example_id: epoch.example_id().into(),
            fold_index,
            source_selection_as_of: epoch.source_selection_as_of(),
            decision_at,
            target_origin: origin,
            target_at: target,
            calculated_at: wall_now()?,
            forecast_rights: None,
            harmonic_audit: None,
            harmonic_unavailable: false,
            valuation_methods: None,
            evidence: AlphaEvidence::Unavailable {
                stage: MissingAlphaStage::Forecast,
                distribution_identity: None,
            },
            instruction: RecommendationSignalInstructionV1::Unavailable(
                RecommendationSignalUnavailableReasonV1::InsufficientPointInTimeEvidence,
            ),
        };
        // Source admission and historical qualification do not grant permission to calculate.
        // Authorize the actual transitive graph before inference, then consume this one-use permit.
        let mut roots = Vec::new();
        roots
            .try_reserve_exact(3)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for parent in [
            training.manifest(),
            coordinate.dataset().generation().manifest(),
            epoch.source_manifest(),
        ] {
            if !roots.contains(parent) {
                roots.push(parent.clone());
            }
        }
        let authorization_duration = context
            .deadline()
            .saturating_duration_since(Instant::now())
            .min(std::time::Duration::from_secs(5));
        if authorization_duration.is_zero() {
            return Err(ServiceError::DeadlineExceeded);
        }
        let authorization = self.research.analytical().authorize_research_use(
            ResearchUseRequest::try_new(
                roots.clone(),
                ResearchUse::LocalAnalysis,
                ResearchUseLimits::try_new(
                    3,
                    4096,
                    8192,
                    4096,
                    4 * 1024 * 1024,
                    authorization_duration,
                    std::time::Duration::from_secs(300),
                )
                .map_err(|_| ServiceError::Internal)?,
            )
            .map_err(|_| ServiceError::InvalidRequest)?,
            context.cancellation(),
        );
        let authorization = match authorization.map_err(map_rights_error) {
            Ok(authorization) => authorization,
            Err(ServiceError::Unavailable) => {
                ensure_live(context)?;
                receipt.evidence = AlphaEvidence::Unavailable {
                    stage: MissingAlphaStage::ForecastRights,
                    distribution_identity: None,
                };
                receipt.calculated_at = wall_now()?;
                return issue_receipt(information, receipt);
            }
            Err(error) => return Err(error),
        };
        if authorization.research_use() != ResearchUse::LocalAnalysis
            || authorization.graph().roots().len() != roots.len()
            || roots.iter().any(|root| {
                !authorization.graph().roots().contains(root)
                    || !authorization
                        .graph()
                        .nodes()
                        .iter()
                        .any(|node| node.manifest() == root)
            })
        {
            return Err(ServiceError::InvalidResult);
        }
        let authorization_expires_at = authorization.expires_at();
        let authorized_at = wall_now()?;
        if authorized_at >= authorization_expires_at {
            return Err(ServiceError::Unavailable);
        }
        receipt.forecast_rights = Some(ForecastCalculationRights {
            decision: authorization.decision_digest().bytes(),
            graph: authorization.graph().digest().bytes(),
            checked_at: authorized_at,
            expires_at: authorization_expires_at,
        });
        let _forecast_permit = authorization.into_permit();
        let forecast = match runtime.forecast_coordinate(coordinate, context) {
            Ok(value) => value,
            Err(ServiceError::Unavailable | ServiceError::NotFound) => {
                ensure_live(context)?;
                receipt.calculated_at = wall_now()?;
                return issue_receipt(information, receipt);
            }
            Err(error) => return Err(error),
        };
        let distribution = forecast.native_distribution();
        if wall_now()? >= authorization_expires_at {
            receipt.evidence = AlphaEvidence::Unavailable {
                stage: MissingAlphaStage::ForecastRights,
                distribution_identity: Some(distribution.identity().bytes()),
            };
            receipt.calculated_at = wall_now()?;
            return issue_receipt(information, receipt);
        }
        if forecast.epoch() != epoch
            || forecast.terminal().target_at() != target
            || forecast.calculated_at() < epoch.calculated_at()
            || forecast.calculated_at() < authorized_at
            || forecast.calculated_at() > wall_now()?
            || forecast.runtime_reopened_at().is_some_and(|reopened| {
                reopened > forecast.calculated_at() || reopened < forecast.runtime_selected_at()
            })
        {
            return Err(ServiceError::InvalidResult);
        }
        receipt.evidence = AlphaEvidence::Unavailable {
            stage: MissingAlphaStage::Valuation,
            distribution_identity: Some(distribution.identity().bytes()),
        };
        let now = wall_now()?;
        let expires_at = now
            .checked_add_nanos(
                self.profile
                    .recommendation_policy()
                    .proposal_lifetime_nanos(),
            )
            .map_err(|_| ServiceError::InvalidRequest)?;
        let evaluation = self
            .valuation
            .evaluate_historical_investment_valuations(
                &self.research,
                &forecast,
                &self.valuation_inputs,
                AutomaticForecastValuationRequest {
                    account_id: self.reference.account_id,
                    expires_at,
                    calculated_by: ActorId::try_from(self.reference.calculated_by.as_ref())
                        .map_err(|_| ServiceError::InvalidRequest)?,
                },
                context,
            )
            .await?;
        receipt.valuation_methods = Some(evaluation.audit().clone());
        let value = match evaluation.predictive() {
            Ok(value) => value,
            Err(ServiceError::Unavailable | ServiceError::NotFound) => {
                ensure_live(context)?;
                receipt.calculated_at = wall_now()?;
                return issue_receipt(information, receipt);
            }
            Err(error) => return Err(error),
        };
        ensure_live(context)?;
        let calculated_at = wall_now()?;
        if value.distribution_identity() != distribution.identity()
            || value.epoch() != epoch
            || value.account_id() != self.reference.account_id
            || value.calculated_by().as_str() != self.reference.calculated_by.as_ref()
            || value.runtime_generation() != forecast.runtime_generation()
            || value.runtime_selected_at() != forecast.runtime_selected_at()
            || value.forecast_calculated_at() != forecast.calculated_at()
            || value.calculated_at() < forecast.calculated_at()
            || value.calculated_at() > calculated_at
            || value.expires_at() <= calculated_at
        {
            return Err(ServiceError::InvalidResult);
        }
        let market = epoch
            .current_unit_price()
            .map_err(|_| ServiceError::InvalidResult)?;
        if subject.market_reference() != Some(market) {
            return Err(ServiceError::InvalidResult);
        }
        let harmonic = crate::application::research::MarketHistoryReadCapability::new(
            self.research.analytical_reader(),
        )
        .read_harmonic(
            self.research.as_ref(),
            &self.calendars,
            subject.instrument_id(),
            market.currency(),
            Some(subject.execution_terms().price_tick()),
            epoch.source_selection_as_of(),
            origin,
            context.deadline(),
            context.cancellation().clone(),
        )
        .await;
        match harmonic {
            Ok(Some(evaluation)) => {
                let audit =
                    crate::application::decision::encode_harmonic_history_audit(evaluation.audit())
                        .map_err(|_| ServiceError::InvalidResult)?;
                // Re-admit the shared canonical codec before binding the original audit bytes.
                let decoded =
                    crate::application::decision::decode_harmonic_history_audit(audit.clone())
                        .map_err(|_| ServiceError::InvalidResult)?;
                if decoded.digest() != evaluation.evaluation_digest() {
                    return Err(ServiceError::InvalidResult);
                }
                receipt.harmonic_audit = Some(audit);
            }
            Ok(None) => {
                receipt.harmonic_unavailable = true;
            }
            Err(crate::application::research::MarketHistoryUnavailableReason::Cancelled) => {
                return Err(ServiceError::Cancelled);
            }
            Err(crate::application::research::MarketHistoryUnavailableReason::DeadlineExceeded) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            Err(crate::application::research::MarketHistoryUnavailableReason::CapacityExceeded) => {
                return Err(ServiceError::ResourceExhausted);
            }
            Err(
                crate::application::research::MarketHistoryUnavailableReason::StorageUnavailable,
            ) => {
                return Err(ServiceError::Unavailable);
            }
            Err(
                crate::application::research::MarketHistoryUnavailableReason::IntegrityUnproven,
            ) => {
                return Err(ServiceError::InvalidResult);
            }
        }
        let (cases, ranges, _) = monetary_forecast_cases(
            market.currency(),
            forecast.terminal().central(),
            forecast
                .terminal()
                .intervals()
                .ok_or(ServiceError::Unavailable)?,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let (decision, _) = InvestmentProposalAuthority::calculate_research_entry_zones(
            market,
            cases,
            ranges,
            value.value(),
            self.profile.recommendation_policy(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        receipt.instruction = match decision {
            RecommendationAlphaDecision::Entry => RecommendationSignalInstructionV1::Entry,
            RecommendationAlphaDecision::NoAction(_) => RecommendationSignalInstructionV1::NoAction,
            RecommendationAlphaDecision::Unavailable(_) => return Err(ServiceError::InvalidResult),
        };
        receipt.calculated_at = calculated_at;
        receipt.evidence = AlphaEvidence::Calculated {
            distribution_identity: distribution.identity().bytes(),
            runtime_generation: forecast.runtime_generation().bytes(),
            runtime_selected_at: forecast.runtime_selected_at(),
            runtime_reopened_at: forecast.runtime_reopened_at(),
            forecast_calculated_at: forecast.calculated_at(),
            valuation_identity: value.identity().bytes(),
            valuation_method_policy_identity: value.method_policy_identity().bytes(),
            rights_decision: value.rights_decision().bytes(),
            rights_graph: value.rights_graph().bytes(),
            valuation_calculated_at: value.calculated_at(),
            expires_at: value.expires_at(),
            market,
            forecast_cases: [cases.downside(), cases.base(), cases.upside()],
            forecast_ranges: [
                ranges.downside().lower(),
                ranges.downside().upper(),
                ranges.base().lower(),
                ranges.base().upper(),
                ranges.upside().lower(),
                ranges.upside().upper(),
            ],
            valuation: value.value(),
        };
        issue_receipt(information, receipt)
    }
}

/// Retained in the existing bounded signal plan; common model/profile/source references live once
/// in its request. This payload records original calculation clocks even after later reopening.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoricalAlphaReceipt {
    version: u16,
    issuer_identity: [u8; 32],
    example_id: Box<str>,
    fold_index: usize,
    source_selection_as_of: Timestamp,
    decision_at: Timestamp,
    target_origin: Timestamp,
    target_at: Timestamp,
    calculated_at: Timestamp,
    forecast_rights: Option<ForecastCalculationRights>,
    harmonic_audit: Option<serde_json::Value>,
    harmonic_unavailable: bool,
    valuation_methods: Option<serde_json::Value>,
    evidence: AlphaEvidence,
    instruction: RecommendationSignalInstructionV1,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum AlphaEvidence {
    Unavailable {
        stage: MissingAlphaStage,
        distribution_identity: Option<[u8; 32]>,
    },
    Calculated {
        distribution_identity: [u8; 32],
        runtime_generation: [u8; 32],
        runtime_selected_at: Timestamp,
        runtime_reopened_at: Option<Timestamp>,
        forecast_calculated_at: Timestamp,
        valuation_identity: [u8; 32],
        valuation_method_policy_identity: [u8; 32],
        rights_decision: [u8; 32],
        rights_graph: [u8; 32],
        valuation_calculated_at: Timestamp,
        expires_at: Timestamp,
        market: Money,
        forecast_cases: [Money; 3],
        forecast_ranges: [Money; 6],
        valuation: Money,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum MissingAlphaStage {
    ForecastRights,
    Forecast,
    Valuation,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ForecastCalculationRights {
    decision: [u8; 32],
    graph: [u8; 32],
    checked_at: Timestamp,
    expires_at: Timestamp,
}

fn issue_receipt(
    information: &RecommendationSignalInformationSetV1<'_>,
    receipt: HistoricalAlphaReceipt,
) -> Result<RecommendationSignalIssuanceV1, ServiceError> {
    let bytes = serde_json::to_string(&receipt).map_err(|_| ServiceError::InvalidResult)?;
    if bytes.len() > MAXIMUM_RECEIPT_BYTES {
        return Err(ServiceError::ResourceExhausted);
    }
    let digest = Sha256Digest::new(Sha256::digest(bytes.as_bytes()).into());
    let mut signal = Sha256::new();
    signal.update(b"market-squawk/historical-alpha-coordinate/v1\0");
    signal.update(receipt.issuer_identity);
    signal.update((receipt.example_id.len() as u64).to_be_bytes());
    signal.update(receipt.example_id.as_bytes());
    signal.update((receipt.fold_index as u64).to_be_bytes());
    signal.update(receipt.decision_at.unix_nanos().to_be_bytes());
    signal.update(receipt.target_origin.unix_nanos().to_be_bytes());
    let mut id = String::from("alpha.");
    use std::fmt::Write as _;
    for byte in signal.finalize() {
        write!(id, "{byte:02x}").map_err(|_| ServiceError::Internal)?;
    }
    RecommendationSignalIssuanceV1::try_new(
        SourceIdentifier::try_from(id.as_str()).map_err(|_| ServiceError::InvalidResult)?,
        information.study_qualification(),
        information.source_selection_as_of(),
        information.target_origin(),
        information.target_at(),
        digest,
        receipt.instruction,
    )
    .and_then(|issuance| issuance.with_instruction_evidence_payload(bytes.into_boxed_str()))
    .map_err(|_| ServiceError::InvalidResult)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

pub(super) fn wall_now() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(elapsed.as_nanos()).map_err(|_| ServiceError::Unavailable)?,
    ))
}

fn map_rights_error(error: ResearchUseCatalogError) -> ServiceError {
    match error {
        ResearchUseCatalogError::Cancelled => ServiceError::Cancelled,
        ResearchUseCatalogError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ResearchUseCatalogError::LimitExceeded => ServiceError::ResourceExhausted,
        ResearchUseCatalogError::Denied { .. }
        | ResearchUseCatalogError::Expired
        | ResearchUseCatalogError::Revoked
        | ResearchUseCatalogError::UnknownGeneration => ServiceError::Unavailable,
        _ => ServiceError::InvalidResult,
    }
}
