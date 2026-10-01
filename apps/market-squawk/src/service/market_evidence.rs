//! Shared selected market evidence over the existing identity, policy and durable data owners.

mod preparation;
pub(super) use preparation::InstalledInvestmentSourcePreparation;
use preparation::{SourcePreparationStep, clock, reference_digest};

use std::sync::Arc;

use futures_util::future::BoxFuture;
use market_squawk_domain::{AssetClass, Currency, InstrumentId, MarketEvent, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::{Deserialize, Serialize};

use crate::application::{
    InstrumentContext, InstrumentContextOutcome, InstrumentContextReadCapability,
    InstrumentContextReadError, InstrumentContextRequest,
    SourceAppliedCorporateActionPlanReference,
    analytical_profile::{AnalyticalProfileResolution, ValidatedAnalyticalProfile, revalidate},
    market_calendar::{
        CompletedMarketSessionError, CompletedMarketSessionReadCapability,
        CompletedMarketSessionReference,
    },
    market_selection::{
        MarketInvestmentMarkBasis, MarketInvestmentReadCapability, MarketInvestmentReadReceipt,
        MarketInvestmentReadReference, product::MarketProductSelectionReadCapability,
    },
    model::forecast_preparation::ForecastPreparationCatalog,
};

pub(super) const PREPARE: &str = "Market.PrepareInvestmentEvidence";
pub(super) const SELECT: &str = "Market.SelectInvestmentEvidence";
pub(super) const READ: &str = "Market.ReadInvestmentEvidence";

/// No private receipt registry is retained. Every call reads existing durable authorities.
pub(super) struct InstalledMarketEvidence {
    selections: MarketProductSelectionReadCapability,
    markets: MarketInvestmentReadCapability,
    identities: Arc<InstrumentContextReadCapability>,
    calendars: CompletedMarketSessionReadCapability,
    sources: InstalledInvestmentSourcePreparation,
    decisions: Arc<crate::application::decision::DecisionApplication>,
}

impl InstalledMarketEvidence {
    pub(super) const fn new(
        selections: MarketProductSelectionReadCapability,
        markets: MarketInvestmentReadCapability,
        identities: Arc<InstrumentContextReadCapability>,
        calendars: CompletedMarketSessionReadCapability,
        sources: InstalledInvestmentSourcePreparation,
        decisions: Arc<crate::application::decision::DecisionApplication>,
    ) -> Self {
        Self {
            selections,
            markets,
            identities,
            calendars,
            sources,
            decisions,
        }
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(operation, PREPARE | SELECT | READ)
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        models: Option<&ForecastPreparationCatalog>,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let arguments = serde_json::Value::Object(super::business_arguments(request.arguments()));
        if request.name() == PREPARE {
            let input =
                serde_json::from_value(arguments).map_err(|_| ServiceError::InvalidRequest)?;
            return self.prepare(input, context, models).await;
        }
        let (input, selection) = match request.name() {
            SELECT => {
                let input: SelectRequest =
                    serde_json::from_value(arguments).map_err(|_| ServiceError::InvalidRequest)?;
                (
                    EvidenceRequest {
                        source_cutoff_unix_nanos: input.source_cutoff_unix_nanos,
                        financial_profile: input.financial_profile,
                    },
                    Selection::Token(input.selection_token),
                )
            }
            READ => {
                let input: ReadRequest =
                    serde_json::from_value(arguments).map_err(|_| ServiceError::InvalidRequest)?;
                (
                    EvidenceRequest {
                        source_cutoff_unix_nanos: input.source_cutoff_unix_nanos,
                        financial_profile: input.financial_profile,
                    },
                    Selection::Reference(input.reference),
                )
            }
            _ => return Err(ServiceError::NotFound),
        };
        let profile = revalidate(&input.financial_profile, models)?;
        let source_cutoff = parse_cutoff(&input.source_cutoff_unix_nanos)?;
        if source_cutoff
            > super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?
        {
            return Err(ServiceError::InvalidRequest);
        }
        let maximum_mark_age = u64::try_from(
            profile
                .recommendation_policy()
                .parameters()
                .market_max_age_nanos,
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        let markets = self.markets.with_maximum_mark_age_nanos(maximum_mark_age)?;
        let instrument_id = match &selection {
            Selection::Token(token) => match self
                .selections
                .resolve(
                    token,
                    source_cutoff,
                    context.deadline(),
                    context.cancellation(),
                )
                .await
            {
                Ok(instrument) => instrument,
                Err(ServiceError::Unavailable) => {
                    return result(
                        unavailable(&input, None, None, UnavailableReason::SelectionChanged),
                        context,
                    );
                }
                Err(error) => return Err(error),
            },
            Selection::Reference(reference) => {
                if reference.source_cutoff()? != source_cutoff
                    || reference.maximum_mark_age_nanos()? != maximum_mark_age
                {
                    return Err(ServiceError::InvalidRequest);
                }
                reference.instrument_id()
            }
        };
        let identity_read = match self
            .identities
            .read(
                InstrumentContextRequest::try_new(instrument_id, source_cutoff, source_cutoff)
                    .map_err(map_identity_error)?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_identity_error)
        {
            Ok(identity) => identity,
            Err(ServiceError::Unavailable) => {
                return result(
                    unavailable(
                        &input,
                        Some(instrument_id),
                        None,
                        UnavailableReason::IdentityUnavailable,
                    ),
                    context,
                );
            }
            Err(error) => return Err(error),
        };
        let InstrumentContextOutcome::Exact(identity) = identity_read.outcome() else {
            return result(
                unavailable(
                    &input,
                    Some(instrument_id),
                    None,
                    UnavailableReason::IdentityUnavailable,
                ),
                context,
            );
        };
        let instrument = InvestmentIdentity::from(identity);
        if !profile.admits_investment(identity.asset_class(), identity.exchange_traded_fund()) {
            return result(
                unavailable(
                    &input,
                    Some(instrument_id),
                    Some(instrument),
                    UnavailableReason::UnsupportedInvestment,
                ),
                context,
            );
        }
        let receipt = match selection {
            Selection::Token(_) => {
                match markets
                    .read(
                        instrument_id,
                        source_cutoff,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(ServiceError::Unavailable | ServiceError::Unauthorized) => None,
                    Err(error) => return Err(error),
                }
            }
            Selection::Reference(reference) => match markets
                .read_reference(
                    &reference,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await
            {
                Ok(receipt) => Some(receipt),
                Err(ServiceError::Unavailable | ServiceError::Unauthorized) => None,
                Err(ServiceError::InvalidResult) => {
                    return result(
                        unavailable(
                            &input,
                            Some(instrument_id),
                            Some(instrument),
                            UnavailableReason::EvidenceChanged,
                        ),
                        context,
                    );
                }
                Err(error) => return Err(error),
            },
        };
        let Some(receipt) = receipt else {
            return result(
                unavailable(
                    &input,
                    Some(instrument_id),
                    Some(instrument),
                    UnavailableReason::MarketEvidenceUnavailable,
                ),
                context,
            );
        };
        if receipt.currency() != identity.quote_currency()
            || receipt.instrument_id() != identity.instrument_id()
        {
            return Err(ServiceError::InvalidResult);
        }
        self.identities
            .verify_restart(&identity_read, context.deadline(), context.cancellation())
            .map_err(map_identity_error)?;
        let content = available(&input, instrument, &receipt, &profile)?;
        result(content, context)
    }

    /// Complete source acquisition precedes the analysis cutoff. Final market refresh requests
    /// current-market preparation without reacquiring historical investment sources.
    async fn prepare(
        &self,
        input: PrepareRequest,
        context: &RequestContext,
        models: Option<&ForecastPreparationCatalog>,
    ) -> Result<TypedToolResult, ServiceError> {
        // Caller supplies only original membership; it cannot claim eligibility or a failure.
        let admitted = if let Some(member) = &input.find_member {
            if input.purpose != PreparationPurpose::InvestmentAnalysis {
                return Err(ServiceError::InvalidRequest);
            }
            revalidate(&input.financial_profile, models)?;
            Some(self.decisions.admit_find_member(
                member,
                &input.financial_profile,
                &input.selection_token,
                context,
            )?)
        } else {
            None
        };
        if let Some(admitted) = &admitted {
            // Immutable retries replay the actual completed attempt without using newer inputs.
            if let Some(saved) = self
                .decisions
                .find_member_source_assessment(admitted, context)?
            {
                return result(
                    saved.response().map_err(super::decision::map_application)?,
                    context,
                );
            }
            let original = self
                .selections
                .resolve(
                    &input.selection_token,
                    admitted.source_cutoff(),
                    context.deadline(),
                    context.cancellation(),
                )
                .await?;
            if original != admitted.instrument_id() {
                return Err(ServiceError::InvalidRequest);
            }
        }
        let started = clock()?;
        let response = self.prepare_sources(input, context, models).await?;
        if let Some(admitted) = admitted {
            let body = response.structured_content();
            if body.get("status").and_then(serde_json::Value::as_str) == Some("unavailable") {
                let required_input_unavailable =
                    match body.get("reason").and_then(serde_json::Value::as_str) {
                        Some("identity_unavailable") => true,
                        Some("source_evidence_unavailable") => body
                            .get("sourceActionReference")
                            .is_some_and(serde_json::Value::is_null),
                        // Stale selection/eligibility is never a completed member assessment.
                        Some("selection_changed" | "evidence_changed") => {
                            return Err(ServiceError::Unavailable);
                        }
                        _ => return Err(ServiceError::InvalidResult),
                    };
                // An optional premium or refresh audit cannot suppress independent valuation
                // methods. The native historical plan actually requires this original source ref.
                if required_input_unavailable {
                    let saved = self.decisions.retain_find_member_source_assessment(
                        admitted,
                        body.clone(),
                        started,
                        clock()?,
                        context,
                    )?;
                    return result(
                        saved.response().map_err(super::decision::map_application)?,
                        context,
                    );
                }
            }
            // A concurrent retry may have completed the same immutable member while this
            // attempt was running. The first retained assessment remains the terminal result.
            if let Some(saved) = self
                .decisions
                .find_member_source_assessment(&admitted, context)?
            {
                return result(
                    saved.response().map_err(super::decision::map_application)?,
                    context,
                );
            }
        }
        Ok(response)
    }

    // Construct the acquisition future in a returning frame. Its state must not be embedded
    // repeatedly in prepare, call and the installed dispatcher while they poll source evidence.
    fn prepare_sources<'a>(
        &'a self,
        input: PrepareRequest,
        context: &'a RequestContext,
        models: Option<&'a ForecastPreparationCatalog>,
    ) -> BoxFuture<'a, Result<TypedToolResult, ServiceError>> {
        Box::pin(self.prepare_sources_impl(input, context, models))
    }

    async fn prepare_sources_impl(
        &self,
        input: PrepareRequest,
        context: &RequestContext,
        models: Option<&ForecastPreparationCatalog>,
    ) -> Result<TypedToolResult, ServiceError> {
        let profile = revalidate(&input.financial_profile, models)?;
        if (input.share_origin_unix_nanos.is_some()
            || input.original_knowledge_at_unix_nanos.is_some())
            && input.purpose != PreparationPurpose::CurrentMarket
        {
            return Err(ServiceError::InvalidRequest);
        }
        let unavailable = |instrument_id, reason| InvestmentPreparationResult::Unavailable {
            scope: input.purpose.scope(),
            financial_configuration_digest: input.financial_profile.configuration_digest.clone(),
            instrument_id,
            reason,
            prepared_at_unix_nanos: None,
            reference: None,
            source_action_reference: None,
            fundamental_share_sources: None,
            sources: Vec::new(),
        };
        let selection_at =
            super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
        let instrument_id = match self
            .selections
            .resolve(
                &input.selection_token,
                selection_at,
                context.deadline(),
                context.cancellation(),
            )
            .await
        {
            Ok(instrument) => instrument,
            Err(ServiceError::Unavailable) => {
                return result(
                    unavailable(None, PreparationUnavailableReason::SelectionChanged),
                    context,
                );
            }
            Err(error) => return Err(error),
        };
        let identity_read = match self
            .identities
            .read(
                InstrumentContextRequest::try_new(instrument_id, selection_at, selection_at)
                    .map_err(map_identity_error)?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_identity_error)
        {
            Ok(identity) => identity,
            Err(ServiceError::Unavailable) => {
                return result(
                    unavailable(
                        Some(instrument_id),
                        PreparationUnavailableReason::IdentityUnavailable,
                    ),
                    context,
                );
            }
            Err(error) => return Err(error),
        };
        let InstrumentContextOutcome::Exact(identity) = identity_read.outcome() else {
            return result(
                unavailable(
                    Some(instrument_id),
                    PreparationUnavailableReason::IdentityUnavailable,
                ),
                context,
            );
        };
        if !profile.admits_investment(identity.asset_class(), identity.exchange_traded_fund()) {
            return result(
                unavailable(
                    Some(instrument_id),
                    PreparationUnavailableReason::UnsupportedInvestment,
                ),
                context,
            );
        }
        let started = clock()?;
        let calendar = self
            .calendars
            .preflight_current_session(context.deadline(), context.cancellation().clone())
            .await
            .map_err(map_calendar_error);
        ensure_live(context)?;
        let (reference, calendar_outcome) = match calendar {
            Ok(Some(reference)) => {
                let digest = reference_digest(&reference)?;
                (Some(reference), Ok(digest))
            }
            Ok(None) => (None, Err(ServiceError::Unavailable)),
            Err(error) => (None, Err(error)),
        };
        let mut sources = vec![SourcePreparationStep::from_attempt(
            "current_session",
            started,
            calendar_outcome,
            context,
        )?];
        // Options are bounded research context across the already selected investment horizon.
        // Acquire before source actions freeze the original analytical cutoff; reads stay pure.
        let option_range = if input.purpose == PreparationPurpose::InvestmentAnalysis
            && matches!(
                identity.asset_class(),
                AssetClass::Equity | AssetClass::Fund
            ) {
            use chrono::Datelike as _;
            let end = selection_at
                .checked_add_nanos(profile.recommendation_policy().horizon_nanos())
                .map_err(|_| ServiceError::InvalidRequest)?;
            let date = |at: Timestamp| {
                let civil = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(at.unix_nanos())
                    .with_timezone(&chrono_tz::America::New_York)
                    .date_naive();
                market_squawk_domain::CalendarDate::new(
                    u16::try_from(civil.year()).map_err(|_| ServiceError::InvalidRequest)?,
                    u8::try_from(civil.month()).map_err(|_| ServiceError::InvalidRequest)?,
                    u8::try_from(civil.day()).map_err(|_| ServiceError::InvalidRequest)?,
                )
                .map_err(|_| ServiceError::InvalidRequest)
            };
            Some(
                market_squawk_sources::OptionExpirationRange::try_new(
                    date(selection_at)?,
                    date(end)?,
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
            )
        } else {
            None
        };
        if let Some(range) = option_range {
            sources.push(
                self.sources
                    .acquire_options(
                        instrument_id,
                        range,
                        selection_at,
                        &input.financial_profile.configuration_digest,
                        context,
                    )
                    .await?,
            );
        }
        let mut fundamental_share_sources = None;
        let acquisition = if input.purpose == PreparationPurpose::InvestmentAnalysis {
            let acquired = self
                .sources
                .acquire(
                    &identity_read,
                    &self.calendars,
                    input.benchmark_instrument_id,
                    context,
                )
                .await?;
            sources.extend(acquired.steps);
            Some((acquired.source_cutoff, acquired.source_action_reference))
        } else if input.share_origin_unix_nanos.is_some()
            || input.original_knowledge_at_unix_nanos.is_some()
        {
            let origin = input
                .share_origin_unix_nanos
                .as_deref()
                .map(parse_cutoff)
                .transpose()?;
            let knowledge_at = input
                .original_knowledge_at_unix_nanos
                .as_deref()
                .map(parse_cutoff)
                .transpose()?;
            if knowledge_at.is_some_and(|at| at > selection_at) {
                return Err(ServiceError::InvalidRequest);
            }
            let acquired = self
                .sources
                .acquire_current_investment_sources(
                    &identity_read,
                    origin,
                    knowledge_at,
                    &self.markets,
                    &profile,
                    context,
                )
                .await?;
            sources.extend(acquired.steps);
            fundamental_share_sources = acquired.fundamental_share_sources;
            Some((acquired.cutoff, acquired.source_action_reference))
        } else {
            None
        };
        // Acquisition may take time. Re-admit the original identity and product selection before
        // retaining its preparation result; a completed capture cannot repair a changed token.
        match self.identities.verify_restart(
            &identity_read,
            context.deadline(),
            context.cancellation(),
        ) {
            Ok(_) => {}
            Err(
                InstrumentContextReadError::AuthorityUnavailable
                | InstrumentContextReadError::RestartConflict,
            ) => {
                return result(
                    unavailable(
                        Some(instrument_id),
                        PreparationUnavailableReason::EvidenceChanged,
                    ),
                    context,
                );
            }
            Err(error) => return Err(map_identity_error(error)),
        }
        let checked_at =
            super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
        match self
            .selections
            .resolve(
                &input.selection_token,
                checked_at,
                context.deadline(),
                context.cancellation(),
            )
            .await
        {
            Ok(current) if current == instrument_id => {}
            Ok(_) | Err(ServiceError::Unavailable) => {
                return result(
                    unavailable(
                        Some(instrument_id),
                        PreparationUnavailableReason::SelectionChanged,
                    ),
                    context,
                );
            }
            Err(error) => return Err(error),
        }
        // Successful source actions bind this exact cutoff. Later validation reads must not
        // replace it with a newer timestamp that would no longer reopen the same plan.
        let (prepared_at, source_action_reference) = match acquisition {
            Some(value) => value,
            None => (clock()?, None),
        };
        if let Some(range) = option_range {
            let index = sources
                .iter()
                .position(|step| step.source == "option_context")
                .ok_or(ServiceError::InvalidResult)?;
            if sources[index].is_available() {
                let context_step = self
                    .sources
                    .assess_options(instrument_id, range, prepared_at, context)
                    .await?;
                sources[index] = context_step;
            }
        }
        // Reopen the exact creating calendar; historical acquisition may have taken time.
        if let Some(reference) = &reference {
            match self
                .calendars
                .read_reference(
                    reference,
                    prepared_at,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await
                .map_err(map_calendar_error)
            {
                Ok(Some(read)) if read.reference() == reference => {}
                Ok(Some(_)) => return Err(ServiceError::InvalidResult),
                Ok(None) | Err(ServiceError::Unavailable) => {
                    sources[0] = SourcePreparationStep::from_attempt(
                        "current_session",
                        prepared_at,
                        Err(ServiceError::Unavailable),
                        context,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }
        if input.purpose == PreparationPurpose::InvestmentAnalysis {
            sources.push(
                self.sources
                    .assess_premium(&self.calendars, prepared_at, context)
                    .await?,
            );
        }
        ensure_live(context)?;
        // Options enrich equity/fund evidence; absent derivatives do not invalidate the underlying.
        // A failed original/identity/publication check has already returned a typed fatal error.
        if sources
            .iter()
            .all(|step| step.source == "option_context" || step.is_available())
        {
            result(
                InvestmentPreparationResult::Prepared {
                    scope: input.purpose.scope(),
                    financial_configuration_digest: input.financial_profile.configuration_digest,
                    instrument_id,
                    prepared_at_unix_nanos: prepared_at.unix_nanos().to_string(),
                    reference,
                    source_action_reference,
                    fundamental_share_sources,
                    sources,
                },
                context,
            )
        } else {
            result(
                InvestmentPreparationResult::Unavailable {
                    scope: input.purpose.scope(),
                    financial_configuration_digest: input.financial_profile.configuration_digest,
                    instrument_id: Some(instrument_id),
                    prepared_at_unix_nanos: Some(prepared_at.unix_nanos().to_string()),
                    reason: PreparationUnavailableReason::SourceEvidenceUnavailable,
                    reference,
                    source_action_reference,
                    fundamental_share_sources,
                    sources,
                },
                context,
            )
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PrepareRequest {
    original_knowledge_at_unix_nanos: Option<String>,
    share_origin_unix_nanos: Option<String>,
    benchmark_instrument_id: Option<InstrumentId>,
    find_member: Option<crate::application::decision::current_find::member::FindMemberContext>,
    #[serde(default)]
    purpose: PreparationPurpose,
    selection_token: String,
    financial_profile: AnalyticalProfileResolution,
}

#[derive(Clone, Copy, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum PreparationPurpose {
    #[default]
    InvestmentAnalysis,
    CurrentMarket,
}
impl PreparationPurpose {
    const fn scope(self) -> &'static str {
        match self {
            Self::InvestmentAnalysis => "investment_analysis",
            Self::CurrentMarket => "current_market",
        }
    }
}

#[derive(Serialize)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum InvestmentPreparationResult {
    Prepared {
        scope: &'static str,
        financial_configuration_digest: String,
        instrument_id: InstrumentId,
        prepared_at_unix_nanos: String,
        reference: Option<CompletedMarketSessionReference>,
        source_action_reference: Option<SourceAppliedCorporateActionPlanReference>,
        fundamental_share_sources: Option<String>,
        sources: Vec<SourcePreparationStep>,
    },
    Unavailable {
        scope: &'static str,
        financial_configuration_digest: String,
        instrument_id: Option<InstrumentId>,
        reason: PreparationUnavailableReason,
        prepared_at_unix_nanos: Option<String>,
        reference: Option<CompletedMarketSessionReference>,
        source_action_reference: Option<SourceAppliedCorporateActionPlanReference>,
        fundamental_share_sources: Option<String>,
        sources: Vec<SourcePreparationStep>,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum PreparationUnavailableReason {
    SelectionChanged,
    IdentityUnavailable,
    UnsupportedInvestment,
    SourceEvidenceUnavailable,
    EvidenceChanged,
}

struct EvidenceRequest {
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SelectRequest {
    selection_token: String,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReadRequest {
    reference: MarketInvestmentReadReference,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
}

enum Selection {
    Token(String),
    Reference(MarketInvestmentReadReference),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InvestmentIdentity {
    instrument_id: InstrumentId,
    symbol: String,
    name: String,
    asset_class: AssetClass,
    currency: Currency,
    exchange_traded_fund: bool,
}

impl From<&InstrumentContext> for InvestmentIdentity {
    fn from(value: &InstrumentContext) -> Self {
        Self {
            instrument_id: value.instrument_id(),
            symbol: value.listed_symbol().to_owned(),
            name: value.display_name().to_owned(),
            asset_class: value.asset_class(),
            currency: value.quote_currency(),
            exchange_traded_fund: value.exchange_traded_fund(),
        }
    }
}

#[derive(Serialize)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum InvestmentEvidenceResult {
    Available {
        source_cutoff_unix_nanos: String,
        financial_configuration_digest: String,
        instrument_id: InstrumentId,
        instrument: InvestmentIdentity,
        reference: MarketInvestmentReadReference,
        authorization: InvestmentAuthorization,
        mark: InvestmentMark,
        liquidity: InvestmentLiquidity,
    },
    Unavailable {
        source_cutoff_unix_nanos: String,
        financial_configuration_digest: String,
        instrument_id: Option<InstrumentId>,
        instrument: Option<InvestmentIdentity>,
        reason: UnavailableReason,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum UnavailableReason {
    SelectionChanged,
    IdentityUnavailable,
    UnsupportedInvestment,
    MarketEvidenceUnavailable,
    EvidenceChanged,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InvestmentMark {
    value: String,
    currency: Currency,
    basis: &'static str,
    observed_at_unix_nanos: String,
    available_at_unix_nanos: String,
    fresh_until_unix_nanos: String,
}

/// Current operation authority audit, kept separate from the original source reference.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InvestmentAuthorization {
    admitted_at_unix_nanos: String,
    expires_at_unix_nanos: String,
    decision_digest: String,
    selection_audit_digest: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DepthSide {
    price: String,
    quantity: String,
}

#[derive(Serialize)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum InvestmentLiquidity {
    Available {
        depth: &'static str,
        currency: Currency,
        contract_multiplier: String,
        bid: DepthSide,
        ask: DepthSide,
        observed_at_unix_nanos: String,
        available_at_unix_nanos: String,
        fresh_until_unix_nanos: String,
    },
    Unavailable {
        reason: LiquidityUnavailableReason,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum LiquidityUnavailableReason {
    SourceDoesNotSupplyDepth,
    NoPositiveDisplayedSize,
    StaleDepth,
}

fn available(
    input: &EvidenceRequest,
    instrument: InvestmentIdentity,
    receipt: &MarketInvestmentReadReceipt,
    profile: &ValidatedAnalyticalProfile,
) -> Result<InvestmentEvidenceResult, ServiceError> {
    let observation = receipt
        .observation()
        .map_err(|_| ServiceError::InvalidResult)?;
    let mark = observation.mark();
    let observed_at = observation
        .timestamps()
        .source_timestamp()
        .ok_or(ServiceError::InvalidResult)?;
    let available_at = observation
        .timestamps()
        .available_at()
        .max(receipt.publication().commit_available_at());
    let fresh_until = mark.fresh_until().ok_or(ServiceError::InvalidResult)?;
    let mark_result = InvestmentMark {
        value: mark.value().normalize().to_string(),
        currency: mark.currency(),
        basis: match mark.basis() {
            MarketInvestmentMarkBasis::FreshLastTrade => "last_trade",
            MarketInvestmentMarkBasis::FreshBidAskMidpoint => "bid_ask_midpoint",
        },
        observed_at_unix_nanos: observed_at.unix_nanos().to_string(),
        available_at_unix_nanos: available_at.unix_nanos().to_string(),
        fresh_until_unix_nanos: fresh_until.unix_nanos().to_string(),
    };
    let liquidity = match receipt.event().map_err(|_| ServiceError::InvalidResult)? {
        MarketEvent::Quote(quote) => {
            let (Some(bid), Some(ask)) = (quote.bid(), quote.ask()) else {
                return Err(ServiceError::InvalidResult);
            };
            let terms = receipt
                .execution_terms()
                .ok_or(ServiceError::InvalidResult)?;
            let depth_side =
                |level: market_squawk_domain::BookLevel| -> Result<DepthSide, ServiceError> {
                    Ok(DepthSide {
                        price: level
                            .price()
                            .checked_to_decimal(terms.price_tick())
                            .map_err(|_| ServiceError::InvalidResult)?
                            .normalize()
                            .to_string(),
                        quantity: level
                            .quantity()
                            .checked_to_decimal(terms.lot_size())
                            .map_err(|_| ServiceError::InvalidResult)?
                            .normalize()
                            .to_string(),
                    })
                };
            let liquidity_until = observed_at
                .checked_add_nanos(
                    profile
                        .recommendation_policy()
                        .parameters()
                        .liquidity_max_age_nanos,
                )
                .map_err(|_| ServiceError::InvalidResult)?
                .min(fresh_until);
            if liquidity_until < receipt.selection().selected_at() {
                InvestmentLiquidity::Unavailable {
                    reason: LiquidityUnavailableReason::StaleDepth,
                }
            } else if bid.quantity().get() == 0 || ask.quantity().get() == 0 {
                InvestmentLiquidity::Unavailable {
                    reason: LiquidityUnavailableReason::NoPositiveDisplayedSize,
                }
            } else {
                InvestmentLiquidity::Available {
                    depth: "top_of_book",
                    currency: terms.quote_currency(),
                    contract_multiplier: terms.contract_multiplier().normalize().to_string(),
                    bid: depth_side(bid)?,
                    ask: depth_side(ask)?,
                    observed_at_unix_nanos: observed_at.unix_nanos().to_string(),
                    available_at_unix_nanos: available_at.unix_nanos().to_string(),
                    fresh_until_unix_nanos: liquidity_until.unix_nanos().to_string(),
                }
            }
        }
        _ => InvestmentLiquidity::Unavailable {
            reason: LiquidityUnavailableReason::SourceDoesNotSupplyDepth,
        },
    };
    Ok(InvestmentEvidenceResult::Available {
        source_cutoff_unix_nanos: input.source_cutoff_unix_nanos.clone(),
        financial_configuration_digest: input.financial_profile.configuration_digest.clone(),
        instrument_id: instrument.instrument_id,
        instrument,
        reference: receipt.reference(),
        authorization: InvestmentAuthorization {
            admitted_at_unix_nanos: receipt.authorized_at().unix_nanos().to_string(),
            expires_at_unix_nanos: receipt.authorization_expires_at().unix_nanos().to_string(),
            decision_digest: encode_digest(receipt.authorization_decision_digest()),
            selection_audit_digest: encode_digest(receipt.selection().selection_digest().bytes()),
        },
        mark: mark_result,
        liquidity,
    })
}

fn unavailable(
    input: &EvidenceRequest,
    instrument_id: Option<InstrumentId>,
    instrument: Option<InvestmentIdentity>,
    reason: UnavailableReason,
) -> InvestmentEvidenceResult {
    InvestmentEvidenceResult::Unavailable {
        source_cutoff_unix_nanos: input.source_cutoff_unix_nanos.clone(),
        financial_configuration_digest: input.financial_profile.configuration_digest.clone(),
        instrument_id,
        instrument,
        reason,
    }
}

fn encode_digest(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(64);
    for byte in bytes {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 15)]));
    }
    result
}

fn result(
    content: impl Serialize,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    ensure_live(context)?;
    TypedToolResult::try_new(
        serde_json::to_value(content).map_err(|_| ServiceError::InvalidResult)?,
        1,
        ToolResultMetadata::complete_not_applicable(),
        context.limits(),
    )
    .map_err(ServiceError::from)
}

fn map_calendar_error(error: CompletedMarketSessionError) -> ServiceError {
    match error {
        CompletedMarketSessionError::InvalidRequest => ServiceError::InvalidRequest,
        CompletedMarketSessionError::InvalidEvidence => ServiceError::InvalidResult,
        CompletedMarketSessionError::ResourceBoundExceeded => ServiceError::ResourceExhausted,
        CompletedMarketSessionError::Unavailable => ServiceError::Unavailable,
        CompletedMarketSessionError::Cancelled => ServiceError::Cancelled,
        CompletedMarketSessionError::DeadlineExceeded => ServiceError::DeadlineExceeded,
    }
}

fn parse_cutoff(value: &str) -> Result<Timestamp, ServiceError> {
    let nanos = value
        .parse::<i64>()
        .map_err(|_| ServiceError::InvalidRequest)?;
    if nanos <= 0 || nanos.to_string() != value {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(Timestamp::from_unix_nanos(nanos))
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

pub(super) fn map_identity_error(error: InstrumentContextReadError) -> ServiceError {
    match error {
        InstrumentContextReadError::InvalidRequest => ServiceError::InvalidRequest,
        InstrumentContextReadError::Cancelled => ServiceError::Cancelled,
        InstrumentContextReadError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        InstrumentContextReadError::AuthorityUnavailable => ServiceError::Unavailable,
        InstrumentContextReadError::ResourceExhausted => ServiceError::ResourceExhausted,
        InstrumentContextReadError::EvidenceConflict
        | InstrumentContextReadError::RestartConflict => ServiceError::InvalidResult,
    }
}
