//! Bounded cross-domain discovery over installed-product read authorities.

use std::{collections::BTreeSet, sync::Arc};

use market_squawk_data::{
    AnalyticalReadCapability, AnalyticalReadLimit, CatalogError, CompanyIdentityReadCapability,
    CompanyIdentitySearchMatch, InstrumentDefinitionReadCapability, InstrumentSearchMatch,
};
use market_squawk_domain::{
    AssignmentVerification, ExternalIdentifier, IdentifierEntitlement, InstrumentDefinition,
    SymbolIdentityRecord,
};
use market_squawk_jobs::{JobListPageLimit, SqliteJobRepository};
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{
    LocalProduct,
    application::{
        decision::{DecisionApplication, DecisionApplicationError},
        job::{JobApplication, JobApplicationError},
    },
    jobs::InstalledJobAuthority,
    provider_onboarding::ProviderOnboardingService,
};

const LOOKUP: &str = "Analysis.Lookup";
const OVERVIEW: &str = "Analysis.GetDecisionOverview";
const MAXIMUM_LOOKUP_ITEMS: usize = 64;
const MAXIMUM_QUERY_BYTES: usize = 256;
const MAXIMUM_INSTRUMENT_MATCH_REASONS: usize = 8;
const ALL_CATEGORIES: [&str; 10] = [
    "company",
    "command",
    "dataset",
    "instrument",
    "job",
    "model",
    "portfolio",
    "provider",
    "screen",
    "target",
];

/// Closed cross-domain analysis surface shared by installed transports.
pub(super) struct InstalledAnalysisOperations {
    capabilities: ServiceCapabilities,
    providers: Arc<ProviderOnboardingService>,
    analytical: AnalyticalReadCapability,
    company_identities: CompanyIdentityReadCapability,
    instrument_definitions: InstrumentDefinitionReadCapability,
    decisions: Arc<DecisionApplication>,
    jobs: JobApplication<SqliteJobRepository>,
}

impl InstalledAnalysisOperations {
    pub(super) fn new(product: &LocalProduct, jobs: &InstalledJobAuthority) -> Self {
        Self {
            capabilities: product.application().capabilities(),
            providers: product.provider_onboarding(),
            analytical: product.research().analytical_reader(),
            company_identities: product.research().company_identities(),
            instrument_definitions: product.research().instrument_definitions(),
            decisions: product.decisions(),
            jobs: JobApplication::new(jobs.repository(), jobs.authority()),
        }
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(operation, LOOKUP | OVERVIEW)
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let (content, count) = match request.name() {
            LOOKUP => self.lookup(request.arguments(), context).await?,
            OVERVIEW => self.overview(context).await?,
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            content,
            count.max(1),
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    async fn lookup(
        &self,
        arguments: &Map<String, Value>,
        context: &RequestContext,
    ) -> Result<(Value, usize), ServiceError> {
        let request: LookupRequest = decode(arguments)?;
        let query = request.query.trim().to_ascii_lowercase();
        if query.is_empty() || query.len() > MAXIMUM_QUERY_BYTES {
            return Err(ServiceError::InvalidRequest);
        }
        let categories = requested_categories(request.categories)?;
        let maximum = context
            .limits()
            .maximum_result_items()
            .min(MAXIMUM_LOOKUP_ITEMS);
        if maximum == 0 {
            return Err(ServiceError::InvalidRequest);
        }
        let mut matches = Vec::new();
        let mut status = Vec::new();
        let mut truncated = false;

        for category in categories {
            ensure_live(context)?;
            match category.as_str() {
                "company" => {
                    let remaining = maximum.saturating_sub(matches.len());
                    if remaining == 0 {
                        truncated = true;
                    } else {
                        let page = self
                            .company_identities
                            .search(
                                &query,
                                remaining,
                                context.deadline(),
                                context.cancellation(),
                            )
                            .map_err(map_company_search)?;
                        truncated |= page.has_more();
                        for company in page.matches() {
                            matches.push(company_lookup_match(company)?);
                        }
                    }
                    status.push(available("company"));
                }
                "command" => {
                    for descriptor in self.capabilities.tools() {
                        if matches.len() >= maximum {
                            break;
                        }
                        let haystack = format!(
                            "{} {} {:?}",
                            descriptor.name(),
                            descriptor.description(),
                            descriptor.contract().domain()
                        )
                        .to_ascii_lowercase();
                        if haystack.contains(&query) {
                            matches.push(json!({
                                "category": "command",
                                "id": descriptor.name(),
                                "label": descriptor.description(),
                                "detail": {"domain": format!("{:?}", descriptor.contract().domain())}
                            }));
                        }
                    }
                    status.push(available("command"));
                }
                "provider" => {
                    for profile in self.providers.profiles() {
                        if matches.len() >= maximum {
                            break;
                        }
                        let value = encode(&profile)?;
                        if value.to_string().to_ascii_lowercase().contains(&query) {
                            matches.push(json!({
                                "category": "provider",
                                "id": profile.id(),
                                "label": profile.id(),
                                "detail": value
                            }));
                        }
                    }
                    status.push(available("provider"));
                }
                "dataset" => {
                    let page = self.dataset_page(context)?;
                    for generation in page.generations() {
                        if matches.len() >= maximum {
                            break;
                        }
                        let id = generation.manifest().dataset_id().as_str();
                        let source = generation.source_id().to_string();
                        if id.to_ascii_lowercase().contains(&query)
                            || source.to_ascii_lowercase().contains(&query)
                        {
                            matches.push(json!({
                                "category": "dataset",
                                "id": id,
                                "label": id,
                                "detail": {
                                    "manifestVersion": generation.manifest().manifest_version(),
                                    "sourceId": source,
                                    "rowCount": generation.row_count(),
                                    "totalBytes": generation.total_bytes()
                                }
                            }));
                        }
                    }
                    status.push(available("dataset"));
                }
                "instrument" => {
                    let remaining = maximum.saturating_sub(matches.len());
                    if remaining == 0 {
                        truncated = true;
                    } else {
                        let page = self
                            .instrument_definitions
                            .search(
                                &query,
                                remaining,
                                context.deadline(),
                                context.cancellation(),
                            )
                            .map_err(map_instrument_search)?;
                        truncated |= page.has_more();
                        for instrument in page.matches() {
                            matches.push(instrument_lookup_match(instrument, &query)?);
                        }
                    }
                    status.push(available("instrument"));
                }
                "screen" => {
                    for screen in self
                        .decisions
                        .list_screens(MAXIMUM_LOOKUP_ITEMS)
                        .map_err(map_decision)?
                    {
                        if matches.len() >= maximum {
                            break;
                        }
                        let id = screen.revision().id().as_str();
                        if id.to_ascii_lowercase().contains(&query) {
                            matches.push(json!({
                                "category": "screen",
                                "id": id,
                                "label": id,
                                "detail": {
                                    "revision": screen.revision().revision().get(),
                                    "maximumResults": screen.maximum_results().get()
                                }
                            }));
                        }
                    }
                    status.push(available("screen"));
                }
                "job" => {
                    let page = self.job_page().await?;
                    for job in page.jobs() {
                        if matches.len() >= maximum {
                            break;
                        }
                        let value = encode(job)?;
                        if value.to_string().to_ascii_lowercase().contains(&query) {
                            let id = value
                                .get("jobId")
                                .and_then(Value::as_str)
                                .ok_or(ServiceError::Internal)?;
                            matches.push(json!({
                                "category": "job",
                                "id": id,
                                "label": value.get("kind").cloned().unwrap_or(Value::String(id.to_owned())),
                                "detail": value
                            }));
                        }
                    }
                    status.push(available("job"));
                }
                unavailable => status.push(json!({
                    "category": unavailable,
                    "state": "unavailable",
                    "reason": "no bounded installed-product index is available for this category"
                })),
            }
        }
        truncated |= matches.len() == maximum;
        let count = matches.len();
        Ok((
            json!({
                "query": query,
                "matches": matches,
                "categories": status,
                "truncated": truncated
            }),
            count,
        ))
    }

    async fn overview(&self, context: &RequestContext) -> Result<(Value, usize), ServiceError> {
        let datasets = self.dataset_page(context)?;
        let screens = self
            .decisions
            .list_screens(MAXIMUM_LOOKUP_ITEMS)
            .map_err(map_decision)?;
        let jobs = self.job_page().await?;
        let providers = self.providers.profiles();
        Ok((
            json!({
                "providers": {
                    "state": "available",
                    "count": providers.len(),
                    "items": providers
                },
                "datasets": {
                    "state": "available",
                    "count": datasets.generations().len(),
                    "hasMore": datasets.has_more()
                },
                "screens": {
                    "state": "available",
                    "count": screens.len(),
                    "items": screens.iter().map(|screen| json!({
                        "id": screen.revision().id().as_str(),
                        "revision": screen.revision().revision().get(),
                        "maximumResults": screen.maximum_results().get()
                    })).collect::<Vec<_>>()
                },
                "jobs": {
                    "state": "available",
                    "count": jobs.jobs().len(),
                    "items": jobs.jobs()
                },
                "commands": {
                    "state": "available",
                    "count": self.capabilities.tools().len()
                },
                "unavailable": [
                    {"category": "model", "reason": "model bundles remain available through Model.ListBundles"},
                    {"category": "portfolio", "reason": "accounts remain available through Portfolio.ListAccounts"},
                    {"category": "target", "reason": "targets require a known target-series identity"}
                ]
            }),
            1,
        ))
    }

    fn dataset_page(
        &self,
        context: &RequestContext,
    ) -> Result<market_squawk_data::AnalyticalGenerationPage, ServiceError> {
        let limit = AnalyticalReadLimit::try_new(MAXIMUM_LOOKUP_ITEMS)
            .map_err(|_error| ServiceError::Internal)?;
        self.analytical
            .datasets(None, limit, context.deadline(), context.cancellation())
            .map_err(|_error| ServiceError::Unavailable)
    }

    async fn job_page(&self) -> Result<crate::application::job::JobViewPage, ServiceError> {
        let limit = JobListPageLimit::try_new(MAXIMUM_LOOKUP_ITEMS)
            .map_err(|_error| ServiceError::Internal)?;
        self.jobs.list(None, limit).await.map_err(map_job)
    }
}

impl std::fmt::Debug for InstalledAnalysisOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledAnalysisOperations")
            .field("capabilities", &self.capabilities)
            .field("providers", &"[PROVIDER AUTHORITY]")
            .field("analytical", &self.analytical)
            .field("company_identities", &self.company_identities)
            .field("instrument_definitions", &self.instrument_definitions)
            .field("decisions", &"[DECISION AUTHORITY]")
            .field("jobs", &"[JOB AUTHORITY]")
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LookupRequest {
    query: String,
    #[serde(default)]
    categories: Vec<String>,
}

fn requested_categories(categories: Vec<String>) -> Result<BTreeSet<String>, ServiceError> {
    let categories = if categories.is_empty() {
        ALL_CATEGORIES.iter().map(ToString::to_string).collect()
    } else {
        categories
    };
    if categories.len() > ALL_CATEGORIES.len()
        || categories
            .iter()
            .any(|category| !ALL_CATEGORIES.contains(&category.as_str()))
    {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(categories.into_iter().collect())
}

fn available(category: &str) -> Value {
    json!({"category": category, "state": "available"})
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone()))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn encode(value: impl serde::Serialize) -> Result<Value, ServiceError> {
    serde_json::to_value(value).map_err(|_error| ServiceError::Internal)
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

fn map_job(error: JobApplicationError) -> ServiceError {
    match error {
        JobApplicationError::NotFound => ServiceError::NotFound,
        JobApplicationError::WaitCancelled => ServiceError::Cancelled,
        JobApplicationError::WaitDeadlineExceeded => ServiceError::DeadlineExceeded,
        JobApplicationError::Contract => ServiceError::InvalidRequest,
        JobApplicationError::Repository | JobApplicationError::Authority => {
            ServiceError::Unavailable
        }
    }
}

fn map_decision(_error: DecisionApplicationError) -> ServiceError {
    ServiceError::Unavailable
}

fn map_instrument_search(error: CatalogError) -> ServiceError {
    match error {
        CatalogError::InstrumentDefinitionReadCancelled => ServiceError::Cancelled,
        CatalogError::InstrumentDefinitionReadDeadlineExceeded => ServiceError::DeadlineExceeded,
        CatalogError::InvalidLimit | CatalogError::InvalidRecord => ServiceError::InvalidRequest,
        _ => ServiceError::Unavailable,
    }
}

fn map_company_search(error: CatalogError) -> ServiceError {
    match error {
        CatalogError::CompanyIdentityReadCancelled => ServiceError::Cancelled,
        CatalogError::CompanyIdentityReadDeadlineExceeded => ServiceError::DeadlineExceeded,
        CatalogError::InvalidLimit | CatalogError::InvalidRecord => ServiceError::InvalidRequest,
        _ => ServiceError::Unavailable,
    }
}

fn company_lookup_match(search_match: &CompanyIdentitySearchMatch) -> Result<Value, ServiceError> {
    let observation = search_match.observation();
    let provider_company_id = observation.provider_company_id().as_str();
    let source_id = observation.source_id().as_str();
    let surface = observation.surface().database_name();
    let id = format!("{source_id}:{surface}:{provider_company_id}");
    let match_reasons = search_match
        .reasons()
        .iter()
        .map(|reason| {
            json!({
                "kind": reason.kind(),
                "value": reason.value(),
                "associationOrdinal": reason.association_ordinal()
            })
        })
        .collect::<Vec<_>>();
    let associations = observation
        .associations()
        .iter()
        .map(|association| {
            json!({
                "ticker": association.ticker(),
                "exchange": association.exchange(),
                "verification": "provider_reported_unverified"
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "category": "company",
        "id": id,
        "label": observation.conformed_name(),
        "destination": {
            "kind": "research_company",
            "sourceId": source_id,
            "providerCompanyId": provider_company_id,
            "surface": surface
        },
        "detail": {
            "currentName": observation.conformed_name(),
            "formerNames": observation.former_names(),
            "entityType": observation.entity_type(),
            "sic": observation.sic(),
            "sicDescription": observation.sic_description(),
            "providerReportedSecurityAssociations": associations,
            "sourceId": source_id,
            "providerCompanyId": provider_company_id,
            "surface": surface,
            "quality": observation.quality(),
            "receivedAt": observation.received_at(),
            "availability": observation.availability(),
            "ingestedAt": observation.ingested_at(),
            "publicationCompletedAt": search_match.completed_at(),
            "parentIngestPayloadEvidence": observation.parent_ingest_payload_evidence(),
            "identityPayloadEvidence": observation.identity_payload_evidence(),
            "matchReasons": match_reasons,
            "matchReasonsTruncated": search_match.reasons_truncated(),
            "executionEligible": false,
            "instrumentLinks": []
        }
    }))
}

fn instrument_lookup_match(
    search_match: &InstrumentSearchMatch,
    query: &str,
) -> Result<Value, ServiceError> {
    let definition = search_match.definition();
    let label = instrument_label(definition);
    let (reasons, reasons_truncated) = instrument_match_reasons(search_match, query)?;
    if reasons.is_empty() {
        return Err(ServiceError::Internal);
    }
    Ok(json!({
        "category": "instrument",
        "id": definition.instrument_id().to_string(),
        "label": label,
        "destination": {
            "kind": "market_instrument",
            "instrumentId": definition.instrument_id().to_string()
        },
        "detail": {
            "displayName": label,
            "companyName": null,
            "assetClass": encode(definition.asset_class())?,
            "tradingStatus": encode(definition.trading_status())?,
            "quoteCurrency": definition.quote_currency().to_string(),
            "definitionRevision": definition.definition_revision().get(),
            "definitionObservedAt": encode(search_match.definition_observed_at())?,
            "venueMappings": encode(definition.venue_mappings())?,
            "matchReasons": reasons,
            "matchReasonsTruncated": reasons_truncated || search_match.matching_symbols_truncated()
        }
    }))
}

fn instrument_label(definition: &InstrumentDefinition) -> String {
    definition
        .venue_mappings()
        .iter()
        .min_by_key(|mapping| (mapping.venue_symbol().as_str(), mapping.venue_id().as_str()))
        .map(|mapping| {
            format!(
                "{} · {}",
                mapping.venue_symbol().as_str(),
                mapping.venue_id().as_str()
            )
        })
        .unwrap_or_else(|| definition.instrument_id().to_string())
}

fn instrument_match_reasons(
    search_match: &InstrumentSearchMatch,
    query: &str,
) -> Result<(Vec<Value>, bool), ServiceError> {
    let definition = search_match.definition();
    let mut reasons = Vec::new();
    let mut truncated = false;
    let instrument_id = definition.instrument_id().to_string();
    if matches_query(&instrument_id, query) {
        reasons.push(json!({
            "kind": "stable_instrument_id",
            "label": "Stable instrument ID",
            "value": instrument_id,
            "current": true,
            "evidence": {
                "definitionRevision": definition.definition_revision().get(),
                "observedAt": encode(search_match.definition_observed_at())?
            }
        }));
    }
    for mapping in definition.venue_mappings() {
        if matches_query(mapping.venue_symbol().as_str(), query)
            || matches_query(mapping.venue_id().as_str(), query)
        {
            if reasons.len() == MAXIMUM_INSTRUMENT_MATCH_REASONS {
                truncated = true;
                break;
            }
            reasons.push(json!({
                "kind": "current_venue_symbol",
                "label": "Current market symbol",
                "value": mapping.venue_symbol().as_str(),
                "venueId": mapping.venue_id().as_str(),
                "current": true,
                "evidence": {
                    "definitionRevision": definition.definition_revision().get(),
                    "observedAt": encode(search_match.definition_observed_at())?
                }
            }));
        }
    }
    for symbol in search_match.matching_symbols() {
        if !is_current_symbol(definition, symbol) {
            if reasons.len() == MAXIMUM_INSTRUMENT_MATCH_REASONS {
                truncated = true;
                break;
            }
            reasons.push(json!({
                "kind": "historical_venue_symbol",
                "label": "Historical market symbol",
                "value": symbol.venue_symbol().as_str(),
                "venueId": symbol.venue_id().as_str(),
                "current": false,
                "evidence": {
                    "validFrom": encode(symbol.validity().starts_at())?,
                    "validUntil": encode(symbol.validity().ends_at())?
                }
            }));
        }
    }
    for record in definition.identifiers() {
        if record.assignment_verification() == AssignmentVerification::VerifiedUnassigned
            || record.rights_policy().entitlement() == IdentifierEntitlement::UnknownOrRestricted
        {
            continue;
        }
        let identifier = identifier_search_value(record.identifier())?;
        let kind = identifier_kind(record.identifier())?;
        if matches_query(&identifier, query) || matches_query(&kind, query) {
            if reasons.len() == MAXIMUM_INSTRUMENT_MATCH_REASONS {
                truncated = true;
                break;
            }
            reasons.push(json!({
                "kind": "external_identifier",
                "label": identifier_label(record.identifier()),
                "value": record.identifier().to_string(),
                "current": record.validity().ends_at().is_none(),
                "evidence": {
                    "identifierKind": kind,
                    "assignmentVerification": encode(record.assignment_verification())?,
                    "syntaxVerification": encode(record.syntax_verification())?,
                    "sourceId": record.source_id().as_str(),
                    "sourceEvidence": encode(record.source_evidence())?,
                    "sourceTimestamp": encode(record.source_timestamp())?,
                    "observedAt": encode(record.observed_at())?,
                    "validFrom": encode(record.validity().starts_at())?,
                    "validUntil": encode(record.validity().ends_at())?,
                    "rightsPolicy": encode(record.rights_policy())?
                }
            }));
        }
    }
    for record in definition.provider_identities() {
        if matches_query(record.provider_instrument_id().as_str(), query)
            || matches_query(record.source_id().as_str(), query)
        {
            if reasons.len() == MAXIMUM_INSTRUMENT_MATCH_REASONS {
                truncated = true;
                break;
            }
            reasons.push(json!({
                "kind": "accepted_provider_identity",
                "label": "Provider instrument ID",
                "value": record.provider_instrument_id().as_str(),
                "sourceId": record.source_id().as_str(),
                "current": record.validity().ends_at().is_none(),
                "evidence": encode(record)?
            }));
        }
    }
    Ok((reasons, truncated))
}

fn is_current_symbol(definition: &InstrumentDefinition, symbol: &SymbolIdentityRecord) -> bool {
    definition.venue_mappings().iter().any(|mapping| {
        mapping.venue_id() == symbol.venue_id() && mapping.venue_symbol() == symbol.venue_symbol()
    })
}

fn matches_query(value: &str, query: &str) -> bool {
    value.to_ascii_lowercase().contains(query)
}

fn identifier_search_value(identifier: &ExternalIdentifier) -> Result<String, ServiceError> {
    let value = encode(identifier)?;
    Ok(value
        .get("value")
        .map(Value::to_string)
        .unwrap_or_default()
        .to_ascii_lowercase())
}

fn identifier_kind(identifier: &ExternalIdentifier) -> Result<String, ServiceError> {
    encode(identifier)?
        .get("kind")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or(ServiceError::Internal)
}

const fn identifier_label(identifier: &ExternalIdentifier) -> &'static str {
    match identifier {
        ExternalIdentifier::Ticker(_) => "Ticker",
        ExternalIdentifier::Cusip(_) => "CUSIP",
        ExternalIdentifier::Isin(_) => "ISIN",
        ExternalIdentifier::Sedol(_) => "SEDOL",
        ExternalIdentifier::Figi(_) => "FIGI",
        ExternalIdentifier::OccOption(_) => "OCC option identity",
        ExternalIdentifier::Futures(_) => "Futures identity",
        ExternalIdentifier::CryptoPair(_) => "Crypto pair",
        ExternalIdentifier::ChainAddress(_) => "Chain address",
    }
}
