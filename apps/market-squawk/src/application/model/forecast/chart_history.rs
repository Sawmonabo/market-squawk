//! Reopens original current forecast inputs, RAW history and source-owned Split authority.

use super::{ForecastServingEvidence, SelectedPriceForecast, current_input};
use crate::application::research::corporate_actions::{
    ApplicableActionPlanError, SourceAppliedCorporateActionPlanReference,
    SourceAppliedCorporateActionReadCapability,
};
use market_squawk_data::{
    CorporateActionAdjustment, CorporateActionPolicy, DatasetBuildError, ForecastBasisHistory,
};
use market_squawk_decisions::{ProposalForecastVintageId, SavedForecastChartEvidence};
use market_squawk_domain::{Currency, InstrumentId, Money, Timestamp};
use market_squawk_modeling::ForecastArtifactManifestRecord;
use market_squawk_services::{RequestContext, ServiceError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::num::NonZeroU32;

mod projection;
pub(crate) use projection::{
    authorize_projection_parents, quality as chart_quality, read_chart_display,
    read_chart_display_from_catalog, recheck_projection_parents,
    storage_error as chart_storage_error,
};

/// Original financial authority and immutable display projection retained with the decision.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SavedForecastChart {
    version: u16,
    instrument: InstrumentId,
    currency: Currency,
    source_cutoff: Timestamp,
    origin_at: Timestamp,
    origin_price: Money,
    vintage: [u8; 32],
    output_binding: [u8; 32],
    price_derivation: [u8; 32],
    serving_selection: [u8; 32],
    input_epoch: [u8; 32],
    basis: [u8; 32],
    history: [u8; 32],
    source_read: [u8; 32],
    calendar: [u8; 32],
    selected_manifest: ForecastArtifactManifestRecord,
    origin_manifest: ForecastArtifactManifestRecord,
    parents: Vec<ForecastArtifactManifestRecord>,
    projection: Option<market_squawk_data::ChartProjectionReference>,
    origin: serde_json::Value,
    forecast: serde_json::Value,
    source_action_reference: SourceAppliedCorporateActionPlanReference,
}
impl SavedForecastChart {
    /// Validates original reference bytes; this never promotes them into source authority.
    pub(crate) fn decode(saved: &SavedForecastChartEvidence) -> Result<Self, ServiceError> {
        let value: Self = serde_json::from_slice(saved.canonical_record())
            .map_err(|_| ServiceError::InvalidResult)?;
        value.validate()?;
        if value.instrument != saved.instrument_id()
            || value.currency != saved.currency()
            || value.source_cutoff != saved.source_cutoff()
            || value.origin_at != saved.origin_at()
            || value.basis != saved.basis_identity().evidence_digest().bytes()
            || value.history != saved.history_identity().evidence_digest().bytes()
            || value.vintage != saved.vintage_id().bytes()
            || serde_json::to_vec(&value).map_err(|_| ServiceError::InvalidResult)?
                != saved.canonical_record()
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(value)
    }
    pub(crate) const fn source_action_reference(
        &self,
    ) -> &SourceAppliedCorporateActionPlanReference {
        &self.source_action_reference
    }
    fn validate(&self) -> Result<(), ServiceError> {
        if self.version != 1
            || self.parents.is_empty()
            || self.parents.len() > 128
            || self.source_cutoff != self.source_action_reference.knowledge_cutoff()
            || !self
                .source_action_reference
                .requested_instruments()
                .contains(&self.instrument)
            || self.origin_at > self.source_cutoff
            || self.origin_price.currency() != self.currency
            || self.origin_price.amount() <= rust_decimal::Decimal::ZERO
            || [
                self.vintage,
                self.output_binding,
                self.price_derivation,
                self.serving_selection,
                self.input_epoch,
                self.basis,
                self.history,
                self.source_read,
                self.calendar,
            ]
            .contains(&[0; 32])
        {
            return Err(ServiceError::InvalidResult);
        }
        self.selected_manifest
            .typed()
            .map_err(|_| ServiceError::InvalidResult)?;
        self.origin_manifest
            .typed()
            .map_err(|_| ServiceError::InvalidResult)?;
        for (i, parent) in self.parents.iter().enumerate() {
            parent.typed().map_err(|_| ServiceError::InvalidResult)?;
            if self.parents[..i].contains(parent) {
                return Err(ServiceError::InvalidResult);
            }
        }
        if !self.parents.contains(&self.selected_manifest)
            || !self.parents.contains(&self.origin_manifest)
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(())
    }
    fn evidence(&self) -> Result<SavedForecastChartEvidence, ServiceError> {
        self.validate()?;
        SavedForecastChartEvidence::try_new(
            self.instrument,
            self.currency,
            self.source_cutoff,
            self.origin_at,
            ProposalForecastVintageId::try_from_bytes(self.vintage)
                .map_err(|_| ServiceError::InvalidResult)?,
            decision_digest(self.basis)?,
            decision_digest(self.history)?,
            serde_json::to_vec(self)
                .map_err(|_| ServiceError::InvalidResult)?
                .into_boxed_slice(),
        )
        .map_err(|_| ServiceError::InvalidResult)
    }
}

/// Source-owned inputs retained together for the monetary share projection.
pub(crate) struct ReplayedForecastPriceHistory {
    pub(crate) epoch: market_squawk_data::FeatureDatasetInputEpoch,
    pub(crate) original_plan: market_squawk_data::CorporateActionPlan,
    pub(crate) history: ForecastBasisHistory,
    pub(crate) saved: SavedForecastChartEvidence,
}

/// The price was authenticated by the existing exact forecast reader. Every source is reopened
/// here at its original coordinate; absence never authorizes a latest-generation substitution.
#[allow(
    clippy::too_many_arguments,
    reason = "independent existing read authorities"
)]
pub(crate) async fn replay_price_history_inputs(
    price: &SelectedPriceForecast,
    research: &crate::ResearchService,
    calendar: &crate::application::market_calendar::ForecastSessionReadCapability,
    source_actions: &SourceAppliedCorporateActionReadCapability,
    source_reference: &SourceAppliedCorporateActionPlanReference,
    saved: Option<&SavedForecastChartEvidence>,
    context: &RequestContext,
) -> Result<Option<ReplayedForecastPriceHistory>, ServiceError> {
    check(context)?;
    let retained = saved.map(SavedForecastChart::decode).transpose()?;
    if retained.as_ref().is_some_and(|record| {
        record.source_action_reference != *source_reference
            || record.vintage != price.vintage_id().bytes()
    }) {
        return Err(ServiceError::InvalidResult);
    }
    let serving = price.serving_evidence();
    let Some(input) = serving.current_price_input() else {
        return Ok(None);
    };
    if source_reference.knowledge_cutoff() != serving.knowledge_cutoff()
        || !source_reference
            .requested_instruments()
            .contains(&price.instrument_id())
    {
        return Err(ServiceError::InvalidResult);
    }
    let output = match current_input::reopen_current_price_input(
        &research.analytical_reader(),
        serving.manifest(),
        context.deadline(),
        context.cancellation().clone(),
    )
    .await
    {
        Ok(output) => output,
        Err(ServiceError::Unavailable | ServiceError::NotFound) => return Ok(None),
        Err(error) => return Err(error),
    };
    let index = current_input::current_price_coordinate_index(&output, &input.example_id)?;
    let coordinate = output
        .coordinate(index)
        .ok_or(ServiceError::InvalidResult)?;
    let cohort = current_input::current_price_cohort_reference(input)?;
    current_input::current_price_session_origin(
        Some(calendar),
        cohort.as_ref(),
        coordinate,
        context.deadline(),
        context.cancellation().clone(),
    )
    .await?;
    current_input::current_price_feature_values(price.model_metadata(), coordinate)?;
    if ForecastServingEvidence::from_current_price_output(&output, index, cohort.as_ref())
        .map_err(|_| ServiceError::InvalidResult)?
        != *serving
    {
        return Err(ServiceError::InvalidResult);
    }
    let epoch = coordinate.epoch();
    if epoch.instrument_id() != price.instrument_id()
        || epoch.target_origin() != Some(price.observed_through())
        || epoch.current_unit_price().map_err(build_error)?.currency() != price.currency()
    {
        return Err(ServiceError::InvalidResult);
    }
    let history = source_actions
        .read_history_reference(
            source_reference,
            price.instrument_id(),
            context.deadline(),
            context.cancellation().clone(),
            None,
        )
        .await
        .map_err(source_error);
    let history = match history {
        Ok(Some(history)) => history,
        Ok(None) | Err(ServiceError::Unavailable | ServiceError::NotFound) => return Ok(None),
        Err(error) => return Err(error),
    };
    let source = source_actions
        .read_price_reference_for_histories(
            source_reference,
            &[&history],
            context.deadline(),
            context.cancellation().clone(),
            None,
        )
        .await
        .map_err(source_error);
    let source = match source {
        Ok(Some(source)) => source,
        Ok(None) | Err(ServiceError::Unavailable | ServiceError::NotFound) => return Ok(None),
        Err(error) => return Err(error),
    };
    let original = source.covered_price_plan().map_err(source_error)?;
    let policy =
        CorporateActionPolicy::new(CorporateActionAdjustment::SplitAdjusted, NonZeroU32::MIN);
    let origin = epoch.target_origin().ok_or(ServiceError::InvalidResult)?;
    let limits = original
        .source_split_projection_limits(
            policy,
            epoch.instrument_id(),
            epoch.source_selection_as_of(),
            origin,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
    check(context)?;
    let plan = original
        .try_project_source_split_plan(
            policy,
            epoch.instrument_id(),
            epoch.source_selection_as_of(),
            origin,
            limits,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
    let proof = epoch
        .replay_price_history(&history, &plan, context.deadline(), context.cancellation())
        .map_err(build_error)?;
    let (permit, _) = crate::application::MarketHistoryReadCapability::authorize_forecast_history(
        research,
        &proof,
        context.deadline(),
        context.cancellation(),
    )
    .await
    .map_err(history_error)?;
    let origin = projection::original_origin(&proof)?;
    let forecast = projection::original_forecast(price, &origin)?;
    let mut record = SavedForecastChart {
        version: 1,
        instrument: proof.instrument_id(),
        currency: proof.origin_price().currency(),
        source_cutoff: proof.source_cutoff(),
        origin_at: proof.origin_at(),
        origin_price: proof.origin_price(),
        vintage: price.vintage_id().bytes(),
        output_binding: price.output_binding_identity().bytes(),
        price_derivation: price.price_derivation_identity().bytes(),
        serving_selection: serving.selection_sha256().bytes(),
        input_epoch: Sha256::digest(epoch.canonical_bytes().map_err(build_error)?).into(),
        basis: proof.basis_identity().bytes(),
        history: proof.history_identity().bytes(),
        source_read: proof.source_read_identity().bytes(),
        calendar: proof.calendar_identity().bytes(),
        selected_manifest: ForecastArtifactManifestRecord::from_manifest(proof.selected_manifest()),
        origin_manifest: ForecastArtifactManifestRecord::from_manifest(proof.origin_manifest()),
        parents: proof
            .parent_manifests()
            .iter()
            .map(ForecastArtifactManifestRecord::from_manifest)
            .collect(),
        projection: None,
        origin,
        forecast,
        source_action_reference: source_reference.clone(),
    };
    projection::publish_history(&mut record, &proof, research, context)?;
    let evidence = record.evidence()?;
    if saved.is_some_and(|saved| *saved != evidence)
        || retained.is_some_and(|saved| saved != record)
    {
        return Err(ServiceError::InvalidResult);
    }
    check(context)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|value| i64::try_from(value.as_nanos()).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Internal)?;
    if now >= permit.expires_at() {
        return Err(ServiceError::Unauthorized);
    }
    Ok(Some(ReplayedForecastPriceHistory {
        epoch: epoch.clone(),
        original_plan: plan,
        history: proof,
        saved: evidence,
    }))
}
fn check(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn build_error(error: DatasetBuildError) -> ServiceError {
    match error {
        DatasetBuildError::Cancelled => ServiceError::Cancelled,
        DatasetBuildError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        DatasetBuildError::LimitExceeded => ServiceError::ResourceExhausted,
        _ => ServiceError::InvalidResult,
    }
}
fn source_error(error: ApplicableActionPlanError) -> ServiceError {
    match error {
        ApplicableActionPlanError::SourceRead(error) => error,
        ApplicableActionPlanError::IncompleteOrdinaryCoverage
        | ApplicableActionPlanError::UnresolvedApplicableActions => ServiceError::Unavailable,
        ApplicableActionPlanError::InvalidEvidence => ServiceError::InvalidResult,
        ApplicableActionPlanError::Interrupted => ServiceError::Internal,
    }
}

fn decision_digest(
    bytes: [u8; 32],
) -> Result<market_squawk_decisions::DecisionContentDigest, ServiceError> {
    market_squawk_decisions::DecisionContentDigest::try_new(
        market_squawk_domain::EvidenceDigest::new(
            market_squawk_domain::DigestAlgorithm::Sha256,
            bytes,
        ),
    )
    .map_err(|_| ServiceError::InvalidResult)
}

fn history_error(error: crate::application::MarketHistoryUnavailableReason) -> ServiceError {
    use crate::application::MarketHistoryUnavailableReason as Error;
    match error {
        Error::Cancelled => ServiceError::Cancelled,
        Error::DeadlineExceeded => ServiceError::DeadlineExceeded,
        Error::CapacityExceeded => ServiceError::ResourceExhausted,
        Error::IntegrityUnproven => ServiceError::InvalidResult,
        Error::StorageUnavailable => ServiceError::Unavailable,
    }
}
