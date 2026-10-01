//! Installed-service adapter for evidence-derived forecast preparation.

mod fiscal;
pub(super) use fiscal::{
    GET_FISCAL_PREPARATION_PLAN, START_FISCAL_DATASET_BUILD, START_FISCAL_FORECAST,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use market_squawk_domain::{AssetClass, InstrumentId, Timestamp};
use market_squawk_modeling::{ForecastHorizon, ModelOutputSemantics};
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::{
    LocalProduct,
    application::{
        InstrumentContext, InstrumentContextOutcome, InstrumentContextReadCapability,
        InstrumentContextRequest,
        analytical_profile::{AnalyticalProfileError, AnalyticalProfileResolution, revalidate},
        lifecycle::WorkspaceRuntimeIdentity,
        market_calendar::{
            CompletedMarketSessionError, CompletedMarketSessionReadCapability,
            ForecastSessionCohort, ForecastSessionCohortReference,
        },
        model::forecast::{ForecastProductHorizon, ForecastProductIdentity, ForecastProductTarget},
        model::forecast_preparation::{
            ForecastCurrentFeatureInputSelection, ForecastEvidenceDataset, ForecastEvidencePolicy,
            ForecastInstrumentAvailability, ForecastModelSummary, ForecastPreparationAuthority,
            ForecastPreparationCatalog, ForecastPreparationError, ForecastPreparationPreview,
            ForecastPreparationSelection, PreparedForecast, PreparedForecastJobInput,
        },
        opaque_product_token,
    },
};

pub(super) const GET_FORECAST_PREPARATION: &str = "Model.GetForecastPreparation";
pub(super) const PREPARE_FORECAST: &str = "Model.PrepareForecast";
pub(super) const PREPARE_INVESTMENT_FORECAST: &str = "Model.PrepareInvestmentForecast";
pub(super) const START_PREPARED_FORECAST: &str = "Model.StartPreparedForecast";

const MAXIMUM_CATALOG_INSTRUMENTS: usize = 4_096;

/// One process-owned preparation authority, absent only when no model runtime is admitted.
pub(super) struct InstalledForecastPreparation {
    authority: Option<Arc<ForecastPreparationAuthority>>,
    research: Arc<crate::ResearchService>,
    instruments: Option<InstrumentContextReadCapability>,
    runtime: RuntimeIdentity,
    calendars: CompletedMarketSessionReadCapability,
}

impl InstalledForecastPreparation {
    pub(super) fn new(
        product: &LocalProduct,
        runtime: RuntimeIdentity,
        authority: Option<Arc<ForecastPreparationAuthority>>,
    ) -> Self {
        Self {
            authority,
            research: product.research(),
            instruments: product.instrument_context_read_capability(),
            calendars: CompletedMarketSessionReadCapability::new(
                product.research(),
                product.market_runtime(),
            ),
            runtime,
        }
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            GET_FORECAST_PREPARATION | PREPARE_FORECAST | PREPARE_INVESTMENT_FORECAST
        )
    }

    /// Resolves only the exact model pin required to validate this profile.
    pub(super) async fn financial_profile_catalog(
        &self,
        configuration: &crate::application::analytical_profile::AnalyticalProfileConfiguration,
        context: &RequestContext,
    ) -> Result<Option<ForecastPreparationCatalog>, ServiceError> {
        use crate::application::analytical_profile::AnalyticalModelBundlePolicy;
        ensure_live(context)?;
        let AnalyticalModelBundlePolicy::Exact { model_token } = configuration.model_bundle_policy
        else {
            return Ok(None);
        };
        let authority = self.authority.as_ref().ok_or(ServiceError::Unavailable)?;
        authority
            .catalog_for_model_token(
                context.origin().ok_or(ServiceError::Unauthorized)?,
                self.workspace()?,
                super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?,
                None,
                model_token,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map(Some)
            .map_err(map_preparation)
    }

    /// Reuses the request's typed financial configuration for exact model membership only.
    pub(super) async fn financial_profile_catalog_for_request(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Option<ForecastPreparationCatalog>, ServiceError> {
        let configuration = request
            .arguments()
            .get("financialProfile")
            .and_then(|profile| profile.get("configuration"))
            .ok_or(ServiceError::InvalidRequest)?;
        let configuration = serde_json::from_value(configuration.clone())
            .map_err(|_| ServiceError::InvalidRequest)?;
        self.financial_profile_catalog(&configuration, context)
            .await
    }

    pub(super) async fn revalidate_profile(
        &self,
        profile: &AnalyticalProfileResolution,
        context: &RequestContext,
    ) -> Result<crate::application::analytical_profile::ValidatedAnalyticalProfile, ServiceError>
    {
        let catalog = self
            .financial_profile_catalog(&profile.configuration, context)
            .await?;
        revalidate(profile, catalog.as_ref()).map_err(Into::into)
    }

    /// Produces one pinned page of choices; callers never collect the inventory.
    pub(super) async fn model_catalog_page(
        &self,
        cursor: Option<String>,
        limit: usize,
        context: &RequestContext,
    ) -> Result<Option<ForecastPreparationCatalog>, ServiceError> {
        ensure_live(context)?;
        let Some(authority) = self.authority.as_ref() else {
            return Ok(None);
        };
        authority
            .catalog(
                context.origin().ok_or(ServiceError::Unauthorized)?,
                self.workspace()?,
                super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?,
                None,
                cursor,
                limit,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map(Some)
            .map_err(map_preparation)
    }

    /// Folds bounded pages under the unchanged deterministic selection ordering.
    async fn best_investment_catalog(
        &self,
        input: &InvestmentForecastPreparationRequest,
        source_cutoff: Timestamp,
        current_feature_input: Option<&ForecastCurrentFeatureInputSelection>,
        cohort: Option<&ForecastSessionCohort>,
        context: &RequestContext,
    ) -> Result<ForecastPreparationCatalog, ServiceError> {
        let authority = self.authority.as_ref().ok_or(ServiceError::Unavailable)?;
        let profile = revalidate(&input.financial_profile, None)?;
        let mut cursor = None;
        let mut winner = None;
        loop {
            ensure_live(context)?;
            let page = authority
                .catalog(
                    context.origin().ok_or(ServiceError::Unauthorized)?,
                    self.workspace()?,
                    source_cutoff,
                    current_feature_input,
                    cursor,
                    25,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await
                .map_err(map_preparation)?;
            let next = page.next_cursor().map(str::to_owned);
            match profile.select_forecast_ranked(&page, input.instrument_id, source_cutoff, cohort)
            {
                Ok((_, rank)) => {
                    if winner.as_ref().is_none_or(|(prior, _)| rank > *prior) {
                        winner = Some((rank, page));
                    }
                }
                Err(AnalyticalProfileError::ModelUnavailable) => {
                    if next.is_none() && winner.is_none() {
                        return Ok(page);
                    }
                }
                Err(error) => return Err(error.into()),
            }
            let Some(next) = next else {
                break;
            };
            cursor = Some(next);
        }
        winner
            .map(|(_, page)| page)
            .ok_or(ServiceError::InvalidResult)
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let authority = self.authority.as_ref().ok_or(ServiceError::Unavailable)?;
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let workspace = self.workspace()?;
        let (content, item_count) = match request.name() {
            GET_FORECAST_PREPARATION => {
                let input: ForecastPreparationPageRequest =
                    decode(&super::business_arguments(request.arguments()))?;
                let limit = input.limit.unwrap_or(25);
                if !(1..=100).contains(&limit)
                    || input.cursor.as_ref().is_some_and(|cursor| {
                        cursor.is_empty()
                            || cursor.len() > 512
                            || cursor.chars().any(char::is_control)
                    })
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let catalog = authority
                    .catalog(
                        origin,
                        workspace,
                        super::runtime::current_timestamp()
                            .map_err(|_| ServiceError::Unavailable)?,
                        None,
                        input.cursor,
                        usize::from(limit),
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(map_preparation)?;
                let identities = self.identities_for_catalog(&catalog, context)?;
                catalog_value(&catalog, &identities)?
            }
            PREPARE_FORECAST | PREPARE_INVESTMENT_FORECAST => {
                let requested_at =
                    super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
                let arguments = super::business_arguments(request.arguments());
                let investment_input: Option<InvestmentForecastPreparationRequest> =
                    if request.name() == PREPARE_INVESTMENT_FORECAST {
                        Some(decode(&arguments)?)
                    } else {
                        None
                    };
                let forecast_input: Option<ForecastPreparationRequest> =
                    if request.name() == PREPARE_FORECAST {
                        Some(decode(&arguments)?)
                    } else {
                        None
                    };
                let catalog_cutoff = investment_input
                    .as_ref()
                    .map(InvestmentForecastPreparationRequest::source_cutoff)
                    .transpose()?
                    .unwrap_or(requested_at);
                if catalog_cutoff > requested_at {
                    return Err(ServiceError::InvalidRequest);
                }
                let cohort_reference = investment_input
                    .as_ref()
                    .and_then(|input| input.forecast_cohort.clone());
                let cohort = if let Some(reference) = &cohort_reference {
                    Some(
                        self.reopen_cohort(reference, catalog_cutoff, requested_at, context)
                            .await?,
                    )
                } else {
                    None
                };
                let current_feature_input = investment_input
                    .as_ref()
                    .and_then(|input| input.current_feature_input.clone())
                    .map(|input| match &cohort_reference {
                        Some(reference) => input.with_session_cohort(reference.clone()),
                        None => input,
                    });
                if cohort_reference.is_some() && current_feature_input.is_none() {
                    return Err(ServiceError::InvalidRequest);
                }
                let validated_profile = match &investment_input {
                    Some(input) => {
                        let profile_catalog = match input.financial_profile.configuration.model_bundle_policy {
                            crate::application::analytical_profile::AnalyticalModelBundlePolicy::Exact { model_token } => {
                                Some(authority.catalog_for_model_token(
                                    origin, workspace, catalog_cutoff, current_feature_input.as_ref(),
                                    model_token, context.deadline(), context.cancellation().child_token(),
                                ).await.map_err(map_preparation)?)
                            }
                            crate::application::analytical_profile::AnalyticalModelBundlePolicy::BestAdmittedCalibratedMeanV1 => None,
                        };
                        Some(revalidate(
                            &input.financial_profile,
                            profile_catalog.as_ref(),
                        )?)
                    }
                    None => None,
                };
                let selected_model_token = forecast_input.as_ref().map(|input| input.selection.model_token)
                    .or_else(|| investment_input.as_ref().and_then(|input| {
                        input.probability_selection.as_ref().map(|selection| selection.model_token)
                            .or_else(|| match input.financial_profile.configuration.model_bundle_policy {
                                crate::application::analytical_profile::AnalyticalModelBundlePolicy::Exact { model_token } => Some(model_token),
                                crate::application::analytical_profile::AnalyticalModelBundlePolicy::BestAdmittedCalibratedMeanV1 => None,
                            })
                    }));
                let catalog = if let Some(model_token) = selected_model_token {
                    authority
                        .catalog_for_model_token(
                            origin,
                            workspace,
                            catalog_cutoff,
                            current_feature_input.as_ref(),
                            model_token,
                            context.deadline(),
                            context.cancellation().child_token(),
                        )
                        .await
                        .map_err(map_preparation)?
                } else {
                    self.best_investment_catalog(
                        investment_input
                            .as_ref()
                            .ok_or(ServiceError::InvalidRequest)?,
                        catalog_cutoff,
                        current_feature_input.as_ref(),
                        cohort.as_ref(),
                        context,
                    )
                    .await?
                };
                let (mut selection, source_cutoff, profile_digest, instrument_id) = if request
                    .name()
                    == PREPARE_INVESTMENT_FORECAST
                {
                    let input = investment_input
                        .as_ref()
                        .ok_or(ServiceError::InvalidRequest)?;
                    let source_cutoff = input.source_cutoff()?;
                    if source_cutoff > requested_at {
                        return Err(ServiceError::InvalidRequest);
                    }
                    let selection = self.resolve_investment_selection(
                        &catalog,
                        input,
                        validated_profile
                            .as_ref()
                            .ok_or(ServiceError::InvalidRequest)?,
                        source_cutoff,
                        cohort.as_ref(),
                        context,
                    )?;
                    let Some(selection) = selection else {
                        // Only a completed, valid catalogue selection may prove absence.
                        // Failed reads and a rejected full profile never enter this branch.
                        ensure_live(context)?;
                        return TypedToolResult::try_new(
                                json!({
                                    "instrumentId": input.instrument_id,
                                    "availability": {
                                        "state": "unavailable",
                                        "reason": "compatible_forecast_selection_unavailable",
                                    },
                                    "forecast": null,
                                    "requestSha256": null,
                                    "forecastCohort": cohort_reference,
                                    "expectedObservedThroughUnixNanos": null,
                                    "financialProfileDigest": input.financial_profile.configuration_digest,
                                    "sourceCutoffUnixNanos": source_cutoff.unix_nanos().to_string(),
                                }),
                                1,
                                ToolResultMetadata::complete_not_applicable(),
                                context.limits(),
                            )
                            .map_err(ServiceError::from);
                    };
                    (
                        selection,
                        source_cutoff,
                        Some(input.financial_profile.configuration_digest.clone()),
                        Some(input.instrument_id),
                    )
                } else {
                    let input = forecast_input.ok_or(ServiceError::InvalidRequest)?;
                    (
                        resolve_selection(&catalog, input.selection)?,
                        requested_at,
                        None,
                        None,
                    )
                };
                if let Some(input) = current_feature_input {
                    selection.selection = selection.selection.with_current_feature_input(input);
                }
                let instruments = self.instruments.as_ref().ok_or(ServiceError::Unavailable)?;
                let prepared = authority
                    .prepare(
                        origin,
                        workspace,
                        selection.selection.clone(),
                        source_cutoff,
                        profile_digest
                            .as_deref()
                            .map(|digest| {
                                super::jobs::parse_sha256(digest).map(|value| value.bytes())
                            })
                            .transpose()
                            .map_err(|_| ServiceError::InvalidRequest)?,
                        |instrument_id, knowledge_at, effective_at| {
                            resolve_product_identity(
                                instruments,
                                instrument_id,
                                knowledge_at,
                                effective_at,
                                context,
                            )
                            .map_err(|error| match error {
                                ServiceError::Cancelled => ForecastPreparationError::Cancelled,
                                ServiceError::DeadlineExceeded => {
                                    ForecastPreparationError::DeadlineExceeded
                                }
                                ServiceError::ResourceExhausted => {
                                    ForecastPreparationError::Capacity
                                }
                                ServiceError::Unavailable => ForecastPreparationError::Unavailable,
                                _ => ForecastPreparationError::InvalidEvidence,
                            })
                        },
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(map_preparation)?;
                if selection
                    .expected_origin
                    .is_some_and(|origin| prepared.preview().observed_through() != origin)
                {
                    return Err(ServiceError::InvalidResult);
                }
                let value = prepared_value(&prepared, &selection)?;
                let value = if let Some(profile_digest) = profile_digest {
                    json!({
                        "instrumentId": instrument_id.ok_or(ServiceError::InvalidResult)?,
                        "availability": { "state": "ready", "reason": null },
                        "forecast": value,
                        "forecastCohort": cohort_reference,
                        "expectedObservedThroughUnixNanos": selection.expected_origin.map(|origin| origin.unix_nanos().to_string()),
                        "requestSha256": crate::application::model::forecast_preparation::hex(
                            prepared.preview().request_sha256()),
                        "financialProfileDigest": profile_digest,
                        "sourceCutoffUnixNanos": source_cutoff.unix_nanos().to_string(),
                    })
                } else {
                    value
                };
                (value, 1)
            }
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            content,
            item_count,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(ServiceError::from)
    }

    pub(super) async fn consume(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<PreparedForecastJobInput, ServiceError> {
        ensure_live(context)?;
        let authority = self.authority.as_ref().ok_or(ServiceError::Unavailable)?;
        let input: PreparedForecastStart = decode(&super::business_arguments(request.arguments()))?;
        authority
            .consume_token(
                context.origin().ok_or(ServiceError::Unauthorized)?,
                self.workspace()?,
                input.confirmation_token,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_preparation)
    }

    fn workspace(&self) -> Result<WorkspaceRuntimeIdentity, ServiceError> {
        WorkspaceRuntimeIdentity::try_from_runtime(self.runtime)
            .map_err(|_error| ServiceError::Unavailable)
    }

    async fn reopen_cohort(
        &self,
        reference: &ForecastSessionCohortReference,
        source_cutoff: Timestamp,
        evaluated_at: Timestamp,
        context: &RequestContext,
    ) -> Result<ForecastSessionCohort, ServiceError> {
        reference.validate().map_err(map_cohort)?;
        let knowledge = reference.knowledge_cutoff().map_err(map_cohort)?;
        if knowledge != source_cutoff {
            return Err(ServiceError::InvalidRequest);
        }
        let calendar = self
            .calendars
            .read_reference(
                reference.calendar(),
                knowledge,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_cohort)?
            .ok_or(ServiceError::Unavailable)?;
        calendar
            .reopen_forecast_session_cohort(
                reference,
                evaluated_at,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_cohort)?
            .ok_or(ServiceError::Unavailable)
    }

    fn resolve_investment_selection(
        &self,
        catalog: &ForecastPreparationCatalog,
        input: &InvestmentForecastPreparationRequest,
        profile: &crate::application::analytical_profile::ValidatedAnalyticalProfile,
        source_cutoff: Timestamp,
        cohort: Option<&ForecastSessionCohort>,
        context: &RequestContext,
    ) -> Result<Option<ResolvedForecastSelection>, ServiceError> {
        let identities = self.instruments.as_ref().ok_or(ServiceError::Unavailable)?;
        let identity = identities
            .read(
                InstrumentContextRequest::try_new(
                    input.instrument_id,
                    source_cutoff,
                    source_cutoff,
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(super::market_evidence::map_identity_error)?;
        let InstrumentContextOutcome::Exact(identity) = identity.outcome() else {
            return Err(ServiceError::Unavailable);
        };
        if !profile.admits_investment(identity.asset_class(), identity.exchange_traded_fund()) {
            return Err(ServiceError::InvalidRequest);
        }
        if let Some(probability) = &input.probability_selection {
            probability
                .event
                .validate()
                .map_err(|_| ServiceError::InvalidRequest)?;
            if input.current_feature_input.is_none() || probability.model_token.is_nil() {
                return Err(ServiceError::InvalidRequest);
            }
            let analysis = probability
                .analysis_manifest
                .typed()
                .map_err(|_| ServiceError::InvalidRequest)?;
            let mut models = catalog
                .models()
                .iter()
                .filter(|model| model_token(model) == probability.model_token);
            let Some(model) = models.next() else {
                return Ok(None);
            };
            if models.next().is_some() {
                return Err(ServiceError::InvalidResult);
            }
            let market_squawk_modeling::ForecastTargetMeaning::FixedHorizonEvent {
                horizon_nanos,
                event,
                ..
            } = model.output_binding().target()
            else {
                return Err(ServiceError::InvalidRequest);
            };
            if model.output_semantics() != ModelOutputSemantics::BinaryProbability
                || event != probability.event
                || profile.horizon().step_nanos() != Some(horizon_nanos)
                || profile.horizon().points().get() != 1
            {
                return Err(ServiceError::InvalidRequest);
            }
            let mut matches = Vec::new();
            for history in catalog.evidence().datasets().iter().filter(|dataset| {
                dataset_matches_model(dataset, model) && dataset.analysis_manifest() == &analysis
            }) {
                let Some(available) = history.instruments().iter().find(|value| {
                    value.instrument_id() == input.instrument_id
                        && value.available_at() <= source_cutoff
                }) else {
                    continue;
                };
                if let Some(cohort) = cohort {
                    if !available
                        .session_origin()
                        .is_some_and(|origin| cohort.matches_origin(origin))
                    {
                        continue;
                    }
                }
                for policy in history.policies().iter().filter(|policy| {
                    policy.maximum_horizon_points().get() == 1
                        && policy.horizon_step_nanos() == horizon_nanos
                }) {
                    matches.push(resolve_selection(
                        catalog,
                        ForecastSelectionWire {
                            model_token: probability.model_token,
                            history_token: history_token(history),
                            investment_token: investment_token(history, input.instrument_id),
                            horizon_token: horizon_token(history, *policy),
                        },
                    )?);
                }
            }
            if matches.len() > 1 {
                return Err(ServiceError::InvalidResult);
            }
            return Ok(matches.pop());
        }
        let selection =
            match profile.select_forecast(catalog, input.instrument_id, source_cutoff, cohort) {
                Ok(selection) => selection,
                Err(AnalyticalProfileError::ModelUnavailable) => return Ok(None),
                Err(error) => return Err(error.into()),
            };
        let history = catalog
            .evidence()
            .datasets()
            .iter()
            .find(|dataset| {
                dataset.dataset().manifest() == selection.dataset_manifest()
                    && dataset.analysis_manifest() == selection.analysis_manifest()
            })
            .ok_or(ServiceError::Unavailable)?;
        let available = history
            .instruments()
            .iter()
            .find(|value| value.instrument_id() == input.instrument_id)
            .ok_or(ServiceError::InvalidResult)?;
        let expected_origin = available
            .session_origin()
            .map(|origin| origin.observed_through());
        if let Some(cohort) = cohort {
            let origin = available
                .session_origin()
                .ok_or(ServiceError::InvalidResult)?;
            if !cohort.matches_origin(origin) {
                return Err(ServiceError::InvalidResult);
            }
        }
        let horizon = ForecastProductHorizon::try_from_horizon(selection.horizon())
            .map_err(|_| ServiceError::InvalidResult)?;
        Ok(Some(ResolvedForecastSelection {
            expected_origin,
            investment_token: investment_token(history, input.instrument_id),
            horizon_label: horizon.label().to_owned(),
            horizon_description: horizon.description().to_owned(),
            selection,
        }))
    }

    fn identities_for_catalog(
        &self,
        catalog: &ForecastPreparationCatalog,
        context: &RequestContext,
    ) -> Result<BTreeMap<(InstrumentId, i64, i64), ForecastProductIdentity>, ServiceError> {
        let coordinates = catalog
            .evidence()
            .datasets()
            .iter()
            .flat_map(|dataset| dataset.instruments().iter())
            .map(|instrument| {
                (
                    instrument.instrument_id(),
                    instrument.available_at().unix_nanos(),
                    instrument.observed_through().unix_nanos(),
                )
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if coordinates.len() > MAXIMUM_CATALOG_INSTRUMENTS {
            return Err(ServiceError::ResourceExhausted);
        }
        let instruments = self.instruments.as_ref().ok_or(ServiceError::Unavailable)?;
        coordinates
            .into_iter()
            .map(|coordinate @ (instrument_id, knowledge_at, effective_at)| {
                resolve_product_identity(
                    instruments,
                    instrument_id,
                    Timestamp::from_unix_nanos(knowledge_at),
                    Timestamp::from_unix_nanos(effective_at),
                    context,
                )
                .map(|identity| (coordinate, identity))
            })
            .collect()
    }
}

impl std::fmt::Debug for InstalledForecastPreparation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledForecastPreparation")
            .field(
                "authority",
                &self.authority.as_ref().map(|_| "[FORECAST AUTHORITY]"),
            )
            .field("instruments", &self.instruments)
            .field("runtime", &self.runtime)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastPreparationPageRequest {
    cursor: Option<String>,
    limit: Option<u16>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastPreparationRequest {
    selection: ForecastSelectionWire,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvestmentForecastPreparationRequest {
    instrument_id: InstrumentId,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
    #[serde(default)]
    forecast_cohort: Option<ForecastSessionCohortReference>,
    #[serde(default)]
    current_feature_input: Option<ForecastCurrentFeatureInputSelection>,
    #[serde(default)]
    probability_selection: Option<ProbabilityForecastSelection>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProbabilityForecastSelection {
    event: market_squawk_data::ProbabilityEventTarget,
    model_token: Uuid,
    analysis_manifest: market_squawk_modeling::ForecastArtifactManifestRecord,
}

impl InvestmentForecastPreparationRequest {
    fn source_cutoff(&self) -> Result<Timestamp, ServiceError> {
        let nanos = self
            .source_cutoff_unix_nanos
            .parse::<i64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if nanos.to_string() != self.source_cutoff_unix_nanos {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Timestamp::from_unix_nanos(nanos))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastSelectionWire {
    model_token: Uuid,
    history_token: Uuid,
    investment_token: Uuid,
    horizon_token: Uuid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreparedForecastStart {
    confirmation_token: Uuid,
}

#[derive(Clone)]
struct ResolvedForecastSelection {
    expected_origin: Option<Timestamp>,
    selection: ForecastPreparationSelection,
    investment_token: Uuid,
    horizon_label: String,
    horizon_description: String,
}

fn resolve_selection(
    catalog: &ForecastPreparationCatalog,
    selection: ForecastSelectionWire,
) -> Result<ResolvedForecastSelection, ServiceError> {
    let retained_investment_token = selection.investment_token;
    let mut models = catalog
        .models()
        .iter()
        .filter(|model| model_token(model) == selection.model_token);
    let model = models.next().ok_or(ServiceError::InvalidRequest)?;
    if models.next().is_some() {
        return Err(ServiceError::InvalidRequest);
    }
    let mut histories = catalog.evidence().datasets().iter().filter(|dataset| {
        dataset_matches_model(dataset, model) && history_token(dataset) == selection.history_token
    });
    let history = histories.next().ok_or(ServiceError::InvalidRequest)?;
    if histories.next().is_some() {
        return Err(ServiceError::InvalidRequest);
    }
    let mut investments = history.instruments().iter().filter(|investment| {
        investment_token(history, investment.instrument_id()) == selection.investment_token
    });
    let investment = investments.next().ok_or(ServiceError::InvalidRequest)?;
    if investments.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    let mut policies = history
        .policies()
        .iter()
        .copied()
        .filter(|policy| horizon_token(history, *policy) == selection.horizon_token);
    let policy = policies.next().ok_or(ServiceError::InvalidRequest)?;
    if policies.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    let horizon =
        ForecastHorizon::try_new(policy.maximum_horizon_points(), policy.horizon_step_nanos())
            .map_err(|_| ServiceError::InvalidResult)?;
    let product_horizon = ForecastProductHorizon::try_from_horizon(horizon)
        .map_err(|_| ServiceError::InvalidResult)?;
    let horizon_label = product_horizon.label().to_owned();
    let horizon_description = product_horizon.description().to_owned();
    let selection = ForecastPreparationSelection::try_new(
        model.model_id(),
        model.bundle_id().clone(),
        model.bundle_version(),
        history.dataset().manifest().clone(),
        history.analysis_manifest().clone(),
        investment.instrument_id(),
        horizon,
        policy.maximum_validity_nanos().get(),
    )
    .map_err(map_preparation)?;
    Ok(ResolvedForecastSelection {
        expected_origin: investment
            .session_origin()
            .map(|origin| origin.observed_through()),
        selection,
        investment_token: retained_investment_token,
        horizon_label,
        horizon_description,
    })
}

fn model_token(model: &ForecastModelSummary) -> Uuid {
    model.product_evidence().model_token()
}

fn history_token(dataset: &ForecastEvidenceDataset) -> Uuid {
    let training = dataset.dataset().manifest();
    let analysis = dataset.analysis_manifest();
    let training_manifest_version = training.manifest_version().to_be_bytes();
    let training_schema_version = training.schema().version().get().to_be_bytes();
    let training_schema_fingerprint = training.schema().fingerprint();
    let training_content_hash = training.content_hash().bytes();
    let analysis_manifest_version = analysis.manifest_version().to_be_bytes();
    let analysis_schema_version = analysis.schema().version().get().to_be_bytes();
    let analysis_schema_fingerprint = analysis.schema().fingerprint();
    let analysis_content_hash = analysis.content_hash().bytes();
    let components: [&[u8]; 12] = [
        training.dataset_id().as_str().as_bytes(),
        &training_manifest_version,
        training.schema().name().as_bytes(),
        &training_schema_version,
        &training_schema_fingerprint,
        &training_content_hash,
        analysis.dataset_id().as_str().as_bytes(),
        &analysis_manifest_version,
        analysis.schema().name().as_bytes(),
        &analysis_schema_version,
        &analysis_schema_fingerprint,
        &analysis_content_hash,
    ];
    opaque_product_token(b"market-squawk/forecast-history-choice/v1\0", &components)
}

fn investment_token(dataset: &ForecastEvidenceDataset, instrument_id: InstrumentId) -> Uuid {
    let history_token = history_token(dataset);
    opaque_product_token(
        b"market-squawk/forecast-investment-choice/v1\0",
        &[history_token.as_bytes(), instrument_id.as_uuid().as_bytes()],
    )
}

fn horizon_token(dataset: &ForecastEvidenceDataset, policy: ForecastEvidencePolicy) -> Uuid {
    let history_token = history_token(dataset);
    let maximum_horizon_points = policy.maximum_horizon_points().get().to_be_bytes();
    let horizon_step_nanos = policy.horizon_step_nanos().get().to_be_bytes();
    let maximum_validity_nanos = policy.maximum_validity_nanos().get().to_be_bytes();
    let minimum_observed_points = policy.minimum_observed_points().get().to_be_bytes();
    let components: [&[u8]; 5] = [
        history_token.as_bytes(),
        &maximum_horizon_points,
        &horizon_step_nanos,
        &maximum_validity_nanos,
        &minimum_observed_points,
    ];
    opaque_product_token(b"market-squawk/forecast-horizon-choice/v1\0", &components)
}

fn catalog_value(
    catalog: &ForecastPreparationCatalog,
    identities: &BTreeMap<(InstrumentId, i64, i64), ForecastProductIdentity>,
) -> Result<(Value, usize), ServiceError> {
    let models = catalog
        .models()
        .iter()
        .map(|model| {
            let datasets = catalog
                .evidence()
                .datasets()
                .iter()
                .filter(|dataset| dataset_matches_model(dataset, model))
                .map(|dataset| dataset_value(dataset, identities))
                .collect::<Result<Vec<_>, _>>()?;
            if datasets.is_empty() {
                Ok(None)
            } else {
                Ok(model_value(model, Some(datasets)))
            }
        })
        .collect::<Result<Vec<_>, ServiceError>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let item_count = models.len();
    Ok((
        json!({ "models": models, "nextCursor": catalog.next_cursor() }),
        item_count,
    ))
}

fn dataset_matches_model(dataset: &ForecastEvidenceDataset, model: &ForecastModelSummary) -> bool {
    dataset.model_id() == model.model_id()
        && dataset.bundle_id() == model.bundle_id()
        && dataset.bundle_version() == model.bundle_version()
        && dataset.dataset().manifest() == model.dataset_manifest()
}

fn dataset_value(
    dataset: &ForecastEvidenceDataset,
    identities: &BTreeMap<(InstrumentId, i64, i64), ForecastProductIdentity>,
) -> Result<Value, ServiceError> {
    let investments = dataset
        .instruments()
        .iter()
        .map(|instrument| {
            let identity = identities
                .get(&(
                    instrument.instrument_id(),
                    instrument.available_at().unix_nanos(),
                    instrument.observed_through().unix_nanos(),
                ))
                .ok_or(ServiceError::InvalidResult)?;
            Ok(json!({
                "investmentToken": investment_token(dataset, instrument.instrument_id()),
                "label": identity.display_name(),
                "observedFromUnixNanos": instrument.observed_from().unix_nanos().to_string(),
                "observedThroughUnixNanos": instrument.observed_through().unix_nanos().to_string(),
                "availableAtUnixNanos": instrument.available_at().unix_nanos().to_string(),
                "observationCount": instrument.observed_points().get(),
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    Ok(json!({
        "historyToken": history_token(dataset),
        "label": "Verified point-in-time investment history",
        "investments": investments,
        "horizons": dataset.policies().iter().filter_map(|policy| {
            let horizon = ForecastHorizon::try_new(
                policy.maximum_horizon_points(),
                policy.horizon_step_nanos(),
            ).ok()?;
            let product = ForecastProductHorizon::try_from_horizon(horizon).ok()?;
            Some(json!({
                "horizonToken": horizon_token(dataset, *policy),
                "label": product.label(),
                "description": product.description(),
            }))
        }).collect::<Vec<_>>(),
    }))
}

fn prepared_value(
    prepared: &PreparedForecast,
    resolved: &ResolvedForecastSelection,
) -> Result<Value, ServiceError> {
    let receipt = prepared.receipt();
    let preview = prepared.preview();
    let limitations = preview_limitations(preview);
    Ok(json!({
        "confirmationToken": receipt.receipt_id(),
        "expiresAtUnixNanos": receipt.expires_at().unix_nanos().to_string(),
        "model": model_value(preview.model(), None).ok_or(ServiceError::InvalidResult)?,
        "investmentToken": resolved.investment_token,
        "instrumentLabel": prepared.product_identity().display_name(),
        "observedFromUnixNanos": preview.observed_from().unix_nanos().to_string(),
        "observedThroughUnixNanos": preview.observed_through().unix_nanos().to_string(),
        "availableAtUnixNanos": preview.available_at().unix_nanos().to_string(),
        "observationCount": preview.observed_points(),
        "horizon": {
            "label": resolved.horizon_label,
            "description": resolved.horizon_description,
        },
        "limitations": limitations,
        "analysisOnly": true,
    }))
}

fn preview_limitations(preview: &ForecastPreparationPreview) -> Vec<String> {
    let mut limitations = preview
        .model()
        .limitations()
        .iter()
        .map(|limitation| limitation.to_string())
        .collect::<Vec<_>>();
    if preview.model().output_semantics() == ModelOutputSemantics::Regression
        && !preview.model().has_calibrated_intervals()
    {
        limitations.push(
            "Calibrated forecast ranges are unavailable, so this forecast must be treated as limited evidence."
                .to_owned(),
        );
    }
    limitations.push(
        "A forecast is uncertain investment research, not a promise of profit or permission to trade."
            .to_owned(),
    );
    limitations.sort_unstable();
    limitations.dedup();
    limitations
}

fn model_value(model: &ForecastModelSummary, datasets: Option<Vec<Value>>) -> Option<Value> {
    let (name, objective) = match model.output_semantics() {
        ModelOutputSemantics::Regression => ("Numeric outcome forecast", "numeric_outcome"),
        ModelOutputSemantics::BinaryProbability => ("Likelihood estimate", "likelihood"),
    };
    let mut value = serde_json::Map::from_iter([
        ("modelToken".to_owned(), json!(model_token(model))),
        ("name".to_owned(), json!(name)),
        ("objective".to_owned(), json!(objective)),
        ("target".to_owned(), target_value(model)?),
        (
            "modelEvidence".to_owned(),
            model.product_evidence().product_value(),
        ),
        ("intendedUse".to_owned(), json!(model.intended_use())),
        ("limitations".to_owned(), json!(model.limitations())),
        ("unavailableBehavior".to_owned(), json!("no_action")),
    ]);
    if let Some(datasets) = datasets {
        value.insert("histories".to_owned(), Value::Array(datasets));
    }
    Some(Value::Object(value))
}

fn resolve_product_identity(
    instruments: &InstrumentContextReadCapability,
    instrument_id: InstrumentId,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    context: &RequestContext,
) -> Result<ForecastProductIdentity, ServiceError> {
    let request = InstrumentContextRequest::try_new(instrument_id, knowledge_at, effective_at)
        .map_err(|_| ServiceError::InvalidResult)?;
    let read = instruments
        .read(request, context.deadline(), context.cancellation())
        .map_err(super::market_evidence::map_identity_error)?;
    let InstrumentContextOutcome::Exact(identity) = read.outcome() else {
        return Err(ServiceError::Unavailable);
    };
    ForecastProductIdentity::try_new(
        identity.display_name(),
        Some(identity.listed_symbol()),
        investment_description(identity),
        identity.quote_currency(),
        knowledge_at,
        effective_at,
    )
    .map_err(|_| ServiceError::InvalidResult)
}

fn investment_description(identity: &InstrumentContext) -> &'static str {
    if identity.exchange_traded_fund() {
        return "Exchange-traded fund with point-in-time verified listing identity.";
    }
    match identity.asset_class() {
        AssetClass::Equity => "Listed company investment with point-in-time verified identity.",
        AssetClass::FixedIncome => "Fixed-income investment with point-in-time verified identity.",
        AssetClass::Option => "Listed option with point-in-time verified identity.",
        AssetClass::Future => "Futures investment with point-in-time verified identity.",
        AssetClass::ForeignExchange => {
            "Foreign-exchange investment with point-in-time verified identity."
        }
        AssetClass::Crypto => "Crypto investment with point-in-time verified identity.",
        AssetClass::Commodity => "Commodity investment with point-in-time verified identity.",
        AssetClass::Fund => "Fund investment with point-in-time verified identity.",
        AssetClass::Index => "Market index with point-in-time verified identity.",
        AssetClass::Cash => "Cash investment with point-in-time verified identity.",
    }
}

fn target_value(model: &ForecastModelSummary) -> Option<Value> {
    let target = ForecastProductTarget::try_from_binding(model.output_binding()).ok()?;
    Some(json!({
        "label": target.label(),
        "meaning": target.meaning(),
        "valueKind": target.value_kind(),
        "unitLabel": target.unit_label(),
        "currencyCode": target.currency_code(),
        "event": target.event(),
    }))
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone()))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_preparation(error: ForecastPreparationError) -> ServiceError {
    match error {
        ForecastPreparationError::InvalidLimits
        | ForecastPreparationError::InvalidDescriptor
        | ForecastPreparationError::InvalidSelection
        | ForecastPreparationError::IncompatibleSelection
        | ForecastPreparationError::InvalidEvidence => ServiceError::InvalidRequest,
        ForecastPreparationError::ModelUnavailable
        | ForecastPreparationError::ReceiptUnavailable => ServiceError::NotFound,
        ForecastPreparationError::ReceiptMismatch => ServiceError::Unauthorized,
        ForecastPreparationError::Capacity => ServiceError::ResourceExhausted,
        ForecastPreparationError::Cancelled => ServiceError::Cancelled,
        ForecastPreparationError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ForecastPreparationError::TimeUnavailable | ForecastPreparationError::Unavailable => {
            ServiceError::Unavailable
        }
    }
}

fn map_cohort(error: CompletedMarketSessionError) -> ServiceError {
    match error {
        CompletedMarketSessionError::InvalidRequest => ServiceError::InvalidRequest,
        CompletedMarketSessionError::InvalidEvidence => ServiceError::InvalidResult,
        CompletedMarketSessionError::ResourceBoundExceeded => ServiceError::ResourceExhausted,
        CompletedMarketSessionError::Unavailable => ServiceError::Unavailable,
        CompletedMarketSessionError::Cancelled => ServiceError::Cancelled,
        CompletedMarketSessionError::DeadlineExceeded => ServiceError::DeadlineExceeded,
    }
}
