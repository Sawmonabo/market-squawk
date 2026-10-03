//! Exact qualified study inputs, with source selection separate from simulated economics.

use market_squawk_data::{
    AnalyticalMarketBarReadLimit, AnalyticalMarketBarReadRequest, DatasetBuildPurpose,
    FeatureDatasetInputEpoch, LatestCanonicalMarketBarHistoryWindowRequest,
    MarketBarEffectiveRange, MarketHistorySelectionPolicy, QueryLimits, ResearchUseCatalogError,
    ResearchUseDecisionDigest, ResearchUseGraphDigest, Sha256Digest,
};
use market_squawk_domain::{HistoricalStudyBasis, MarketBarAdjustment, MarketBarObservation};
use market_squawk_modeling::ForecastMeasurement;
use market_squawk_valuation::ForecastValueArithmetic;

use crate::application::market_calendar::{
    CompletedMarketSessionReadCapability, CompletedMarketSessionReference,
};
use crate::application::model::HistoricalPriceForecast;

use super::*;

mod financial;
mod methods;
pub(crate) use methods::{
    HistoricalStudyValuationReadCapability, HistoricalValuationMethodEvaluation,
};

const FORECAST_STUDY_POLICY: &[u8] =
    b"market-squawk/study-predictive-terminal-price-expectation/v1\0";
const MAXIMUM_STUDY_OUTCOMES: usize = 126;
const MAXIMUM_MODEL_LIMITATION_BYTES: usize = 8 * 1024;

/// Economic assumptions retained separately from the source snapshot's study limitations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HistoricalForecastValuationAssumption {
    /// Fitted calibration residual masses describe modeled terminal outcomes.
    EmpiricalFitResidualMass,
    /// Minimum and maximum support are scenario bounds, not coverage probabilities.
    EmpiricalSupportBounds,
    /// Arithmetic returns use the original epoch price and only its selected action factors.
    OriginalEpochPriceAndSelectedActionFactors,
}

/// Research-only predictive terminal-price expectation for one authentic study epoch.
///
/// Only the issuer below can construct this result. It carries no accounting classification,
/// approval, or renewable authority; a fresh calculation obtains its own current-use permit.
#[derive(Clone, Debug)]
pub(crate) struct HistoricalForecastValuationReceipt {
    account_id: AccountId,
    calculated_by: ActorId,
    epoch: FeatureDatasetInputEpoch,
    distribution_identity: Sha256Digest,
    runtime_generation: Sha256Digest,
    runtime_selected_at: Timestamp,
    forecast_calculated_at: Timestamp,
    model_limitations: Box<[Box<str>]>,
    assumptions: Box<[HistoricalForecastValuationAssumption]>,
    authorized_roots: Box<[DatasetManifestRef]>,
    rights_decision: ResearchUseDecisionDigest,
    rights_graph: ResearchUseGraphDigest,
    method_policy_identity: EvidenceDigest,
    identity: EvidenceDigest,
    calculated_at: Timestamp,
    expires_at: Timestamp,
    value: Money,
    lower: Money,
    upper: Money,
}

impl HistoricalForecastValuationReceipt {
    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }
    pub(crate) const fn calculated_by(&self) -> &ActorId {
        &self.calculated_by
    }
    pub(crate) const fn epoch(&self) -> &FeatureDatasetInputEpoch {
        &self.epoch
    }
    pub(crate) const fn distribution_identity(&self) -> Sha256Digest {
        self.distribution_identity
    }
    pub(crate) const fn runtime_generation(&self) -> Sha256Digest {
        self.runtime_generation
    }
    pub(crate) const fn runtime_selected_at(&self) -> Timestamp {
        self.runtime_selected_at
    }
    pub(crate) const fn forecast_calculated_at(&self) -> Timestamp {
        self.forecast_calculated_at
    }
    pub(crate) fn model_limitations(&self) -> &[Box<str>] {
        &self.model_limitations
    }
    pub(crate) fn assumptions(&self) -> &[HistoricalForecastValuationAssumption] {
        &self.assumptions
    }
    pub(crate) fn authorized_roots(&self) -> &[DatasetManifestRef] {
        &self.authorized_roots
    }
    pub(crate) const fn rights_decision(&self) -> ResearchUseDecisionDigest {
        self.rights_decision
    }
    pub(crate) const fn rights_graph(&self) -> ResearchUseGraphDigest {
        self.rights_graph
    }
    pub(crate) const fn method_policy_identity(&self) -> EvidenceDigest {
        self.method_policy_identity
    }
    pub(crate) const fn identity(&self) -> EvidenceDigest {
        self.identity
    }
    pub(crate) const fn calculated_at(&self) -> Timestamp {
        self.calculated_at
    }
    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
    pub(crate) const fn value(&self) -> Money {
        self.value
    }
    pub(crate) const fn lower(&self) -> Money {
        self.lower
    }
    pub(crate) const fn upper(&self) -> Money {
        self.upper
    }
    pub(crate) const fn method_name(&self) -> &'static str {
        "predictive_terminal_price_expectation"
    }
}

impl FairValueDomainService {
    /// Uses actual model outcomes and the sealed original study unit basis, with current rights.
    pub(crate) async fn calculate_historical_forecast_valuation(
        &self,
        research: &ResearchService,
        forecast: &HistoricalPriceForecast,
        request: AutomaticForecastValuationRequest,
        context: &RequestContext,
    ) -> Result<HistoricalForecastValuationReceipt, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let distribution = forecast.native_distribution();
        let epoch = distribution.epoch();
        let path = distribution.path();
        let (Some(target_origin), Some(decision_at), Some(target_at), Some(_)) = (
            epoch.target_origin(),
            epoch.decision_at(),
            epoch.target_at(),
            epoch.market_bar(),
        ) else {
            return Err(ServiceError::Unavailable);
        };
        let now = calculation_clock()?;
        if epoch.purpose() != DatasetBuildPurpose::StudyInputs
            || forecast.calculated_at() > now
            || forecast.calculated_at() < epoch.calculated_at()
            || forecast.calculated_at() < epoch.snapshot_as_of()
            || forecast.runtime_selected_at() > forecast.calculated_at()
            || path.instrument_id() != epoch.instrument_id()
            || path.observed_cutoff() != Some(target_origin)
            || path.available_at() != epoch.source_selection_as_of()
            || forecast.terminal().target_at() != target_at
            || request.expires_at <= now
            || distribution.points().is_empty()
            || distribution.points().len() > MAXIMUM_STUDY_OUTCOMES
        {
            return Err(ServiceError::InvalidRequest);
        }
        let anchor = epoch
            .current_unit_price()
            .map_err(|_| ServiceError::Unavailable)?;
        let measurement = path.output_binding().measurement();
        let horizon_nanos = target_at
            .unix_nanos()
            .checked_sub(target_origin.unix_nanos())
            .and_then(|value| u64::try_from(value).ok())
            .and_then(std::num::NonZeroU64::new)
            .ok_or(ServiceError::InvalidResult)?;
        match measurement {
            ForecastMeasurement::Price { currency }
                if currency == anchor.currency()
                    && path
                        .output_binding()
                        .expected_terminal_price_horizon_nanos()
                        == Some(horizon_nanos) => {}
            ForecastMeasurement::Return
                if path
                    .output_binding()
                    .expected_arithmetic_return_horizon_nanos()
                    == Some(horizon_nanos) => {}
            _ => return Err(ServiceError::Unavailable),
        }
        let mut roots = Vec::new();
        roots
            .try_reserve_exact(3)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for manifest in [
            path.dataset().manifest(),
            distribution.input_manifest(),
            epoch.source_manifest(),
        ] {
            if !roots.contains(manifest) {
                roots.push(manifest.clone());
            }
        }
        let operation_duration = context
            .deadline()
            .saturating_duration_since(std::time::Instant::now())
            .min(Duration::from_secs(5));
        if operation_duration.is_zero() {
            return Err(ServiceError::DeadlineExceeded);
        }
        let authorization = research
            .analytical()
            .authorize_research_use(
                ResearchUseRequest::try_new(
                    roots.clone(),
                    ResearchUse::LocalAnalysis,
                    ResearchUseLimits::try_new(
                        3,
                        4096,
                        8192,
                        4096,
                        4 * 1024 * 1024,
                        operation_duration,
                        Duration::from_secs(300),
                    )
                    .map_err(|_| ServiceError::Internal)?,
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
                context.cancellation(),
            )
            .map_err(map_study_rights_error)?;
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
        let rights_decision = authorization.decision_digest();
        let rights_graph = authorization.graph().digest();
        let expires_at = request.expires_at.min(authorization.expires_at());
        let mut points = Vec::new();
        points
            .try_reserve_exact(distribution.points().len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for point in distribution.points() {
            ensure_request_live(context, &self.lifecycle)?;
            let native = point.value();
            let raw =
                Decimal::try_from_i128_with_scale(native.mantissa(), u32::from(native.scale()))
                    .map_err(|_| ServiceError::InvalidResult)?;
            let amount = match measurement {
                ForecastMeasurement::Price { .. } => raw,
                ForecastMeasurement::Return => Decimal::ONE
                    .checked_add(raw)
                    .and_then(|gross| anchor.amount().checked_mul(gross))
                    .ok_or(ServiceError::InvalidResult)?
                    .round_dp_with_strategy(
                        12,
                        rust_decimal::RoundingStrategy::MidpointNearestEven,
                    ),
                _ => return Err(ServiceError::Unavailable),
            };
            if amount <= Decimal::ZERO {
                return Err(ServiceError::Unavailable);
            }
            points.push((amount, point.probability_ppm().get()));
        }
        let arithmetic =
            ForecastValueArithmetic::calculate(&points).map_err(|_| ServiceError::InvalidResult)?;
        let round = |amount: Decimal, strategy| {
            let mut rounded = amount.round_dp_with_strategy(12, strategy);
            rounded.rescale(12);
            if rounded.scale() != 12 {
                return Err(ServiceError::InvalidResult);
            }
            Ok(Money::new(rounded, anchor.currency()))
        };
        let value = round(
            arithmetic.raw_value(),
            rust_decimal::RoundingStrategy::MidpointNearestEven,
        )?;
        let lower = round(
            arithmetic.lower(),
            rust_decimal::RoundingStrategy::ToNegativeInfinity,
        )?;
        let upper = round(
            arithmetic.upper(),
            rust_decimal::RoundingStrategy::ToPositiveInfinity,
        )?;
        if lower.amount() <= Decimal::ZERO
            || lower.amount() > value.amount()
            || value.amount() > upper.amount()
        {
            return Err(ServiceError::Unavailable);
        }
        let mut model_limitations = Vec::new();
        if path.limitations().len() > 32 {
            return Err(ServiceError::ResourceExhausted);
        }
        model_limitations
            .try_reserve_exact(path.limitations().len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let mut limitation_bytes = 0_usize;
        for limitation in path.limitations() {
            limitation_bytes = limitation_bytes
                .checked_add(limitation.len())
                .ok_or(ServiceError::ResourceExhausted)?;
            if limitation_bytes > MAXIMUM_MODEL_LIMITATION_BYTES {
                return Err(ServiceError::ResourceExhausted);
            }
            let mut retained = String::new();
            retained
                .try_reserve_exact(limitation.len())
                .map_err(|_| ServiceError::ResourceExhausted)?;
            retained.push_str(limitation);
            model_limitations.push(retained.into_boxed_str());
        }
        let mut assumptions = Vec::new();
        assumptions
            .try_reserve_exact(3)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        assumptions.extend([
            HistoricalForecastValuationAssumption::EmpiricalFitResidualMass,
            HistoricalForecastValuationAssumption::EmpiricalSupportBounds,
        ]);
        if measurement == ForecastMeasurement::Return {
            assumptions.push(
                HistoricalForecastValuationAssumption::OriginalEpochPriceAndSelectedActionFactors,
            );
        }
        let mut policy = Sha256::new();
        policy.update(FORECAST_STUDY_POLICY);
        policy.update(b"exact-ppm-mass;scale12;half-even-center;outward-support-bounds");
        policy.update(path.output_binding().identity().bytes());
        for assumption in &assumptions {
            policy.update([match assumption {
                HistoricalForecastValuationAssumption::EmpiricalFitResidualMass => 1,
                HistoricalForecastValuationAssumption::EmpiricalSupportBounds => 2,
                HistoricalForecastValuationAssumption::OriginalEpochPriceAndSelectedActionFactors => 3,
            }]);
        }
        let method_policy_identity =
            EvidenceDigest::new(DigestAlgorithm::Sha256, policy.finalize().into());
        ensure_request_live(context, &self.lifecycle)?;
        let calculated_at = calculation_clock()?;
        if calculated_at < now || calculated_at >= expires_at {
            return Err(ServiceError::Unavailable);
        }
        let mut hash = Sha256::new();
        hash.update(FORECAST_STUDY_POLICY);
        for bytes in [
            distribution.identity().bytes(),
            distribution.input_production_identity().bytes(),
            distribution.input_receipt_sha256().bytes(),
            method_policy_identity.bytes(),
            forecast.runtime_generation().bytes(),
            rights_decision.bytes(),
            rights_graph.bytes(),
            epoch.source_snapshot_digest().bytes(),
            epoch.source_evidence_digest().bytes(),
            epoch.point_in_time_content().bytes(),
            epoch.point_in_time_audit().bytes(),
            epoch.universe_content().bytes(),
            epoch.universe_audit().bytes(),
        ] {
            hash.update(bytes);
        }
        hash.update(request.account_id.as_uuid().as_bytes());
        hash.update(epoch.instrument_id().as_uuid().as_bytes());
        hash_study_bytes(&mut hash, epoch.example_id().as_bytes());
        hash.update([match epoch.basis() {
            HistoricalStudyBasis::HistoricalAsKnown => 1,
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot => 2,
        }]);
        hash.update((epoch.limitations().len() as u64).to_be_bytes());
        for limitation in epoch.limitations() {
            use market_squawk_domain::HistoricalStudyLimitation;
            hash.update([match limitation {
                HistoricalStudyLimitation::HistoricalRevisionCoverageUnproven => 1,
                HistoricalStudyLimitation::LaterVintageInputs => 2,
                HistoricalStudyLimitation::PresentDayFixedCohort => 3,
                HistoricalStudyLimitation::SimulatedAvailability => 4,
            }]);
        }
        hash_study_bytes(&mut hash, request.calculated_by.as_str().as_bytes());
        for time in [
            forecast.runtime_selected_at(),
            forecast.calculated_at(),
            calculated_at,
            expires_at,
            epoch.snapshot_as_of(),
            epoch.source_selection_as_of(),
            decision_at,
            target_origin,
            target_at,
            epoch.calculated_at(),
        ] {
            hash.update(time.unix_nanos().to_be_bytes());
        }
        for amount in [anchor, value, lower, upper] {
            hash.update(amount.amount().mantissa().to_be_bytes());
            hash.update(amount.amount().scale().to_be_bytes());
            hash_study_bytes(&mut hash, amount.currency().as_str().as_bytes());
        }
        hash.update((model_limitations.len() as u64).to_be_bytes());
        for limitation in &model_limitations {
            hash_study_bytes(&mut hash, limitation.as_bytes());
        }
        let identity = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
        ensure_request_live(context, &self.lifecycle)?;
        if calculation_clock()? >= expires_at {
            return Err(ServiceError::Unavailable);
        }
        let _consumed_calculation_permit = authorization.into_permit();
        Ok(HistoricalForecastValuationReceipt {
            account_id: request.account_id,
            calculated_by: request.calculated_by,
            epoch: epoch.clone(),
            distribution_identity: distribution.identity(),
            runtime_generation: forecast.runtime_generation(),
            runtime_selected_at: forecast.runtime_selected_at(),
            forecast_calculated_at: forecast.calculated_at(),
            model_limitations: model_limitations.into_boxed_slice(),
            assumptions: assumptions.into_boxed_slice(),
            authorized_roots: roots.into_boxed_slice(),
            rights_decision,
            rights_graph,
            method_policy_identity,
            identity,
            calculated_at,
            expires_at,
            value,
            lower,
            upper,
        })
    }
}

fn hash_study_bytes(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

fn map_study_rights_error(error: ResearchUseCatalogError) -> ServiceError {
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

/// Source inputs stay private until the method also proves their common share-unit basis.
struct HistoricalComparableSources {
    epoch: FeatureDatasetInputEpoch,
    subject: HistoricalComparableSource,
    peers: Box<[HistoricalComparableSource]>,
    cohort_evidence: Option<EvidenceDigest>,
}

struct HistoricalComparableSource {
    fundamentals: SelectedComparableFundamentals,
    price: HistoricalComparablePrice,
}

struct HistoricalComparablePrice {
    bar: MarketBarObservation,
    manifest: DatasetManifestRef,
    history_lookup: Option<Sha256Digest>,
    object_graph: EvidenceDigest,
    query_identity: EvidenceDigest,
    result_identity: EvidenceDigest,
    native_evidence: Option<EvidenceDigest>,
    native_parents: Box<[DatasetManifestRef]>,
}

impl FairValueDomainService {
    /// Reuses the live fundamental/cohort selectors, with the sealed epoch's actual source cutoff.
    async fn select_historical_comparable_sources(
        &self,
        research: &ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        epoch: &FeatureDatasetInputEpoch,
        mut request: ObservedComparableValuationRequest,
        context: &RequestContext,
    ) -> Result<HistoricalComparableSources, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let (Some(target_origin), Some(decision_at), Some(target_at), Some(_)) = (
            epoch.target_origin(),
            epoch.decision_at(),
            epoch.target_at(),
            epoch.market_bar(),
        ) else {
            return Err(ServiceError::Unavailable);
        };
        if epoch.purpose() != DatasetBuildPurpose::StudyInputs
            || request.subject != epoch.instrument_id()
            || request.knowledge_at != epoch.source_selection_as_of()
            || request.knowledge_at > epoch.snapshot_as_of()
            || target_origin > decision_at
            || decision_at >= target_at
            || epoch.calculated_at() > calculation_clock()?
            || i64::from(request.effective_date.days_since_unix_epoch())
                > target_origin.unix_nanos().div_euclid(86_400_000_000_000)
            || (!request.peers.is_empty() && !(2..=MAXIMUM_PEERS).contains(&request.peers.len()))
        {
            return Err(ServiceError::InvalidRequest);
        }
        match epoch.basis() {
            HistoricalStudyBasis::HistoricalAsKnown => {
                if epoch.source_selection_as_of() > decision_at {
                    return Err(ServiceError::InvalidRequest);
                }
            }
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot => {
                if epoch.source_selection_as_of() != epoch.snapshot_as_of() {
                    return Err(ServiceError::InvalidRequest);
                }
            }
        }
        request.peers.sort_unstable();
        if request.peers.iter().any(|peer| *peer == request.subject)
            || request.peers.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(ServiceError::InvalidRequest);
        }
        let fundamentals = Self::select_comparable_fundamentals(
            research,
            request.subject,
            request.knowledge_at,
            request.effective_date,
            context,
        )
        .await?;
        let cohort_evidence = if request.peers.is_empty() {
            let (peers, identity) = discover_peers(
                research,
                &fundamentals,
                request.subject,
                request.knowledge_at,
                context,
            )
            .await?;
            request.peers = peers;
            Some(identity)
        } else {
            None
        };
        let subject = HistoricalComparableSource {
            fundamentals,
            price: select_historical_price(research, calendars, epoch, request.subject, context)
                .await?,
        };
        let mut peers = Vec::with_capacity(request.peers.len());
        for instrument in &request.peers {
            ensure_request_live(context, &self.lifecycle)?;
            let fundamentals = Self::select_comparable_fundamentals(
                research,
                *instrument,
                request.knowledge_at,
                request.effective_date,
                context,
            )
            .await?;
            if fundamentals.industry != subject.fundamentals.industry
                || fundamentals.period != subject.fundamentals.period
                || fundamentals.metric.amount().money().currency()
                    != subject.fundamentals.metric.amount().money().currency()
            {
                return Err(ServiceError::Unavailable);
            }
            let price =
                select_historical_price(research, calendars, epoch, *instrument, context).await?;
            if price.bar.currency() != fundamentals.metric.amount().money().currency() {
                return Err(ServiceError::Unavailable);
            }
            peers.push(HistoricalComparableSource {
                fundamentals,
                price,
            });
        }
        if subject.price.bar.currency() != subject.fundamentals.metric.amount().money().currency() {
            return Err(ServiceError::Unavailable);
        }
        Ok(HistoricalComparableSources {
            epoch: epoch.clone(),
            subject,
            peers: peers.into_boxed_slice(),
            cohort_evidence,
        })
    }
}

async fn select_historical_price(
    research: &ResearchService,
    calendars: &CompletedMarketSessionReadCapability,
    epoch: &FeatureDatasetInputEpoch,
    instrument: InstrumentId,
    context: &RequestContext,
) -> Result<HistoricalComparablePrice, ServiceError> {
    let (Some(origin), Some(target_origin)) = (epoch.market_bar(), epoch.target_origin()) else {
        return Err(ServiceError::Unavailable);
    };
    let reader = research.analytical_reader();
    let (manifest, history_lookup, history_request) = if instrument == epoch.instrument_id() {
        (epoch.source_manifest().clone(), None, None)
    } else {
        let selection = reader
            .select_latest_canonical_market_bar_history_window(
                LatestCanonicalMarketBarHistoryWindowRequest::try_new(
                    instrument,
                    MarketHistorySelectionPolicy::COMPLETE_DAILY_RAW_V1,
                    epoch.source_selection_as_of(),
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(crate::application::research::corporate_actions::map_analytical_error)?
            .ok_or(ServiceError::Unavailable)?;
        let manifest = selection
            .exact_request()
            .exact_manifest()
            .ok_or(ServiceError::InvalidResult)?
            .clone();
        (
            manifest,
            Some(selection.lookup_digest()),
            Some(selection.into_exact_request()),
        )
    };
    // Query the original source coordinate, then independently require the economic completion.
    // The selection cannot read a later completed day merely because its snapshot is today.
    let nominal = if origin.time_semantics().nominal_daily_date().is_some() {
        Some(
            rejoin_historical_nominal_price(
                research,
                calendars,
                epoch,
                instrument,
                history_request,
                context,
            )
            .await?,
        )
    } else {
        None
    };
    let range = if let Some(date) = origin.time_semantics().nominal_daily_date() {
        MarketBarEffectiveRange::try_nominal_dates(date.date(), date.date())
    } else {
        let source_coordinate = origin
            .time_semantics()
            .provider_timestamp()
            .ok_or(ServiceError::InvalidResult)?;
        MarketBarEffectiveRange::try_new(source_coordinate, source_coordinate)
    }
    .map_err(|_| ServiceError::InvalidRequest)?;
    let request = AnalyticalMarketBarReadRequest::try_new(
        manifest.clone(),
        instrument,
        epoch.source_selection_as_of(),
        Some(range),
        AnalyticalMarketBarReadLimit::try_new(16).map_err(|_| ServiceError::Internal)?,
    )
    .map_err(|_| ServiceError::InvalidRequest)?;
    let limits = QueryLimits::try_new_with_inline_bytes(
        128,
        1024 * 1024,
        1024 * 1024,
        4 * 1024 * 1024,
        2,
        128,
        128,
        Duration::from_secs(5),
    )
    .map_err(|_| ServiceError::Internal)?;
    let output = reader
        .read_market_bars(
            request,
            limits,
            context.deadline(),
            context.cancellation().clone(),
        )
        .await
        .map_err(crate::application::research::corporate_actions::map_analytical_error)?;
    let [bar] = output.bars() else {
        return Err(ServiceError::Unavailable);
    };
    if (nominal.is_none() && bar.completed_at() != Some(target_origin))
        || nominal
            .as_ref()
            .is_some_and(|(original, _, _)| bar != original)
        || bar.adjustment() != MarketBarAdjustment::Raw
        || bar.context().provenance().source_id() != origin.context().provenance().source_id()
        || bar.context().provenance().venue_id() != origin.context().provenance().venue_id()
        || bar.interval() != origin.interval()
        || bar.feed() != origin.feed()
        || bar.time_semantics() != origin.time_semantics()
        || bar.close().amount() <= Decimal::ZERO
        || (instrument == epoch.instrument_id() && bar != origin)
    {
        return Err(ServiceError::Unavailable);
    }
    Ok(HistoricalComparablePrice {
        bar: bar.clone(),
        manifest,
        history_lookup,
        object_graph: output.output().object_graph_digest(),
        query_identity: output.output().query_identity(),
        result_identity: output.output().result_digest(),
        native_evidence: nominal.as_ref().map(|(_, _, evidence)| *evidence),
        native_parents: nominal.map_or_else(
            || Vec::new().into_boxed_slice(),
            |(_, parents, _)| parents.into_boxed_slice(),
        ),
    })
}

/// Reopens the genuine named session without turning a native date into a provider timestamp.
/// Peers also reopen their exact complete source history; its action fields remain source
/// evidence only and do not establish common share units for the reported EPS calculation.
async fn rejoin_historical_nominal_price(
    research: &ResearchService,
    calendars: &CompletedMarketSessionReadCapability,
    epoch: &FeatureDatasetInputEpoch,
    instrument: InstrumentId,
    history_request: Option<market_squawk_data::CanonicalMarketBarHistoryRequest>,
    context: &RequestContext,
) -> Result<
    (
        MarketBarObservation,
        Vec<DatasetManifestRef>,
        EvidenceDigest,
    ),
    ServiceError,
> {
    let origin = epoch.market_bar().ok_or(ServiceError::Unavailable)?;
    let named = epoch
        .named_session_origin()
        .ok_or(ServiceError::Unavailable)?;
    let economic_origin = epoch.target_origin().ok_or(ServiceError::Unavailable)?;
    let cutoff = epoch.source_selection_as_of();
    if !named.matches_origin_bar(origin, epoch.source_manifest(), economic_origin, cutoff) {
        return Err(ServiceError::InvalidResult);
    }
    let history = match history_request {
        Some(request) => {
            let (start, end) = request.requested_dates().ok_or(ServiceError::Unavailable)?;
            if request.instrument_id() != instrument
                || request.knowledge_cutoff() != cutoff
                || named.native_date() < start
                || named.native_date() > end
            {
                return Err(ServiceError::InvalidResult);
            }
            let exact = request
                .exact_manifest()
                .ok_or(ServiceError::InvalidResult)?
                .clone();
            let history = research
                .analytical_reader()
                .read_canonical_market_bar_history(
                    request,
                    context.deadline(),
                    context.cancellation().child_token(),
                )
                .await
                .map_err(crate::application::research::corporate_actions::map_analytical_error)?
                .ok_or(ServiceError::Unavailable)?;
            if history.selection().pinned().manifest() != &exact
                || history.read_receipt().knowledge_cutoff() != cutoff
            {
                return Err(ServiceError::InvalidResult);
            }
            Some(history)
        }
        None if instrument == epoch.instrument_id() => None,
        None => return Err(ServiceError::InvalidResult),
    };
    let (content, binding) = match history.as_ref() {
        Some(history) => {
            let calendar = history
                .selection()
                .receipt()
                .date_windows()
                .ok_or(ServiceError::InvalidResult)?
                .calendar();
            (
                calendar.origin_content_digest,
                calendar.capture_binding_digest,
            )
        }
        None => (
            named.calendar_origin_content_digest(),
            named.calendar_capture_binding_digest(),
        ),
    };
    let reference = CompletedMarketSessionReference::try_from_retained_digests(content, binding)
        .map_err(|_| ServiceError::InvalidResult)?;
    let calendar = calendars
        .read_reference(
            &reference,
            cutoff,
            context.deadline(),
            context.cancellation().child_token(),
        )
        .await
        .map_err(|error| {
            crate::application::research::EquityPremiumReadError::from(error).into_service_error()
        })?
        .ok_or(ServiceError::Unavailable)?;
    let session = calendar
        .date_session_on(named.native_date(), cutoff, cutoff)
        .ok_or(ServiceError::Unavailable)?;
    if session.closes_at_exclusive() != economic_origin
        || session.opens_at() != named.opens_at()
        || calendar.source_action_calendar().knowledge_cutoff() != cutoff
    {
        return Err(ServiceError::Unavailable);
    }
    let mut parents = vec![calendar.source_action_calendar().manifest().clone()];
    let mut evidence = Sha256::new();
    evidence.update(b"market-squawk/historical-comparable-original-native-price/v1\0");
    evidence.update(named.evidence_digest().bytes());
    evidence.update(calendar.source_action_calendar().evidence_digest().bytes());
    let bar = if let Some(history) = history {
        let history = research
            .rejoin_market_history_native_sessions_with_calendar(
                history,
                &calendar,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|error| {
                crate::application::research::EquityPremiumReadError::from(error)
                    .into_service_error()
            })?;
        let source = research
            .rejoin_tiingo_eod_history_actions(history, context.deadline(), context.cancellation())
            .await
            .map_err(|error| {
                crate::application::research::EquityPremiumReadError::from(error)
                    .into_service_error()
            })?;
        let history = source.history();
        let native = history
            .native_sessions()
            .ok_or(ServiceError::InvalidResult)?;
        let member = native
            .sessions()
            .find_date(named.native_date())
            .map_err(crate::application::research::corporate_actions::map_analytical_error)?
            .ok_or(ServiceError::Unavailable)?;
        if !member.bar_present()
            || member.provider_timestamp().is_some()
            || member.provider_period().is_some()
            || member.closes_at_exclusive() != economic_origin
            || member.opens_at() != session.opens_at()
        {
            return Err(ServiceError::InvalidResult);
        }
        let mut selected = None;
        for bar in history.bars() {
            let bar = bar.map_err(|_| ServiceError::InvalidResult)?;
            if bar
                .time_semantics()
                .nominal_daily_date()
                .is_some_and(|date| date.date() == named.native_date())
            {
                if selected.replace(bar).is_some() {
                    return Err(ServiceError::InvalidResult);
                }
            }
        }
        let bar = selected.ok_or(ServiceError::Unavailable)?;
        if bar.completed_at().is_some() {
            return Err(ServiceError::InvalidResult);
        }
        for parent in [
            history.selection().pinned().manifest(),
            history.read_receipt().origin_manifest(),
        ] {
            if !parents.contains(parent) {
                parents.push(parent.clone());
            }
        }
        evidence.update(native.mapping_digest().bytes());
        evidence.update(history.read_receipt().result_digest().bytes());
        evidence.update(source.binding().binding_digest().bytes());
        bar.clone()
    } else {
        if calendar.source_action_calendar().evidence_digest() != named.source_replay_digest() {
            return Err(ServiceError::InvalidResult);
        }
        origin.clone()
    };
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok((
        bar,
        parents,
        EvidenceDigest::new(DigestAlgorithm::Sha256, evidence.finalize().into()),
    ))
}
