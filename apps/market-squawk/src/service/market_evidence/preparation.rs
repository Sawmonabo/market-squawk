//! Actual investment source acquisition on the existing provider and research owners.

use super::*;
use crate::{
    ResearchService,
    application::{
        BoardFullHistoryApplicationError, EquityPremiumReadError, InstrumentContextRead,
        MacroContextReadCapability, OptionsContextAvailability, OptionsContextError,
        OptionsContextReadCapability, OptionsContextRequest, OptionsContextUnavailableReason,
        ProductionResearchIngestCoordinator, RecommendationBenchmarkSelectionReadCapability,
        SourceActionPreparationCapability, SourceAppliedCorporateActionPlanReference,
    },
    provider_activation::ProviderAdapterActivation,
};
use market_squawk_domain::EvidenceDigest;
use market_squawk_sources::{
    OptionExpirationRange, OptionMarketBatchKind, OptionMarketRequestFilter,
};
use sha2::{Digest as _, Sha256};
use std::num::NonZeroU16;

pub(in crate::service) struct InstalledInvestmentSourcePreparation {
    research: Arc<ResearchService>,
    ingest: Arc<ProductionResearchIngestCoordinator>,
    activation: Arc<ProviderAdapterActivation>,
    macro_reader: MacroContextReadCapability,
    actions: SourceActionPreparationCapability,
    market_runtime: Arc<crate::application::MarketRuntimeRegistry>,
    options: OptionsContextReadCapability,
}

pub(super) struct AcquiredInvestmentSources {
    pub(super) steps: Vec<SourcePreparationStep>,
    pub(super) source_cutoff: Timestamp,
    pub(super) source_action_reference: Option<SourceAppliedCorporateActionPlanReference>,
}

impl InstalledInvestmentSourcePreparation {
    pub(in crate::service) const fn new(
        research: Arc<ResearchService>,
        ingest: Arc<ProductionResearchIngestCoordinator>,
        activation: Arc<ProviderAdapterActivation>,
        macro_reader: MacroContextReadCapability,
        actions: SourceActionPreparationCapability,
        market_runtime: Arc<crate::application::MarketRuntimeRegistry>,
        options: OptionsContextReadCapability,
    ) -> Self {
        Self {
            research,
            ingest,
            activation,
            macro_reader,
            actions,
            market_runtime,
            options,
        }
    }

    /// Acquire before sampling a quote, then bind the retained plan to that exact quote clock.
    pub(super) async fn acquire_current_share_sources(
        &self,
        identity: &InstrumentContextRead,
        origin: Timestamp,
        markets: &MarketInvestmentReadCapability,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<(Timestamp, SourceAppliedCorporateActionPlanReference), ServiceError> {
        let record = identity.canonical_record().ok_or(ServiceError::Unavailable)?;
        let prepared = self.actions.acquire_current_action_sources(
            std::slice::from_ref(record), origin, clock()?, context,
        ).await?;
        let cutoff = clock()?;
        let maximum_age = u64::try_from(profile.recommendation_policy().parameters().market_max_age_nanos)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let market = markets.with_maximum_mark_age_nanos(maximum_age)?
            .read(record.definition().instrument_id(), cutoff, context.deadline(), context.cancellation().clone())
            .await?.ok_or(ServiceError::Unavailable)?;
        let quote_at = market.observation().map_err(|_| ServiceError::Unavailable)?.timestamps().effective_at();
        let plan = self.actions.finish_current_action_sources(prepared, quote_at, context).await?;
        let reference = plan.reference().map_err(|_| ServiceError::InvalidResult)?;
        if reference.knowledge_cutoff() < cutoff || reference.valuation_cutoff() != quote_at {
            return Err(ServiceError::InvalidResult);
        }
        Ok((cutoff, reference))
    }

    /// Explicit current research acquisition; canonical reads and historical selectors stay pure.
    pub(super) async fn acquire_options(
        &self,
        instrument: InstrumentId,
        expiration_range: OptionExpirationRange,
        origin: Timestamp,
        profile: &str,
        context: &RequestContext,
    ) -> Result<SourcePreparationStep, ServiceError> {
        let started = clock()?;
        let demand = crate::application::OptionChainDemand::try_new(
            instrument,
            expiration_range.start(),
            expiration_range.end(),
        )
        .and_then(|demand| demand.with_research_scope(origin, profile.to_owned()))
        .map_err(|_| ServiceError::InvalidRequest)?;
        let outcome = self
            .market_runtime
            .acquire_option_chain(demand, context.deadline(), context.cancellation())
            .await
            .map_err(|error| match error {
                crate::application::OptionChainDemandError::Cancelled => ServiceError::Cancelled,
                crate::application::OptionChainDemandError::Deadline => {
                    ServiceError::DeadlineExceeded
                }
                crate::application::OptionChainDemandError::Capacity => {
                    ServiceError::ResourceExhausted
                }
                crate::application::OptionChainDemandError::InvalidDemand => {
                    ServiceError::InvalidRequest
                }
                crate::application::OptionChainDemandError::Revoked
                | crate::application::OptionChainDemandError::Permission
                | crate::application::OptionChainDemandError::Acquisition => {
                    ServiceError::Unavailable
                }
                crate::application::OptionChainDemandError::Authority
                | crate::application::OptionChainDemandError::PendingOriginalDemand
                | crate::application::OptionChainDemandError::Identity
                | crate::application::OptionChainDemandError::Custody
                | crate::application::OptionChainDemandError::Publication
                | crate::application::OptionChainDemandError::Read => ServiceError::InvalidResult,
            })
            .and_then(|read| {
                let batch = read.read().batch();
                let expected_filter = OptionMarketRequestFilter::try_new(
                    Some(expiration_range),
                    None,
                    None,
                    Vec::new(),
                )
                .map_err(|_| ServiceError::InvalidResult)?;
                if batch.publication_kind() != OptionMarketBatchKind::Snapshots
                    || batch.scope().underlying_instrument_id() != instrument
                    || batch.scope().filter() != &expected_filter
                {
                    return Err(ServiceError::InvalidResult);
                }
                Ok(batch.publication_digest())
            });
        SourcePreparationStep::from_attempt("option_context", started, outcome, context)
    }

    /// Reopens the admitted source through the neutral PIT selector at the final analysis cutoff.
    pub(super) async fn assess_options(
        &self,
        instrument: InstrumentId,
        expiration_range: OptionExpirationRange,
        prepared_at: Timestamp,
        context: &RequestContext,
    ) -> Result<SourcePreparationStep, ServiceError> {
        let started = clock()?;
        let request = OptionsContextRequest::try_all_strikes(
            instrument,
            prepared_at,
            prepared_at,
            prepared_at,
            expiration_range,
            NonZeroU16::new(512).ok_or(ServiceError::Internal)?,
        )
        .map_err(map_options_error)?;
        let read = self
            .options
            .read(&request, context.deadline(), context.cancellation())
            .await
            .map_err(map_options_error)?;
        match read.availability() {
            OptionsContextAvailability::Available | OptionsContextAvailability::Limited => {
                SourcePreparationStep::from_attempt(
                    "option_context",
                    started,
                    read.evidence_digest().map_err(map_options_error),
                    context,
                )
            }
            OptionsContextAvailability::Unavailable(reason) => {
                ensure_live(context)?;
                let completed = clock()?;
                if completed < started {
                    return Err(ServiceError::InvalidResult);
                }
                let failure = match reason {
                    OptionsContextUnavailableReason::SetupRequired => "setup_required",
                    OptionsContextUnavailableReason::EntitlementUnavailable => {
                        "entitlement_unavailable"
                    }
                    // A successful complete-chain publication must be selectable at this cutoff.
                    OptionsContextUnavailableReason::NoDataAtCutoff => {
                        return Err(ServiceError::InvalidResult);
                    }
                    OptionsContextUnavailableReason::NoContractsInWindow => "no_contracts_in_window",
                };
                Ok(SourcePreparationStep::unavailable(
                    "option_context",
                    started,
                    failure.to_owned(),
                    completed,
                ))
            }
        }
    }

    /// Source identities are selected before outcomes; their analytical cutoff is frozen later.
    /// No private cache or replacement source authority is created here.
    pub(super) async fn acquire(
        &self,
        identity: &InstrumentContextRead,
        calendars: &CompletedMarketSessionReadCapability,
        benchmark_instrument_id: Option<InstrumentId>,
        context: &RequestContext,
    ) -> Result<AcquiredInvestmentSources, ServiceError> {
        ensure_live(context)?;
        let InstrumentContextOutcome::Exact(subject) = identity.outcome() else {
            return Err(ServiceError::Unavailable);
        };
        let instrument = subject.instrument_id();
        let mut steps = Vec::with_capacity(4);
        let started = clock()?;
        let h15 = self
            .ingest
            .prepare_default_h15_full_history(context)
            .await
            .map(|reference| reference.binding_digest())
            .map_err(map_board_error);
        steps.push(SourcePreparationStep::from_attempt(
            "government_history",
            started,
            h15,
            context,
        )?);

        let started = clock()?;
        let selection_at = clock()?;
        let selection = RecommendationBenchmarkSelectionReadCapability::new(
            self.research.market_data_instruments(),
        );
        let deadline = context.deadline();
        let selected = self
            .research
            .run_owned_research_io(deadline, context.cancellation(), move |cancellation| {
                selection.select(selection_at, selection_at, deadline, &cancellation)
            })
            .await
            .map_err(|error| EquityPremiumReadError::History(error).into_service_error())?;
        ensure_live(context)?;
        let mut source_action_reference = None;
        let mut action_cutoff = None;
        let benchmark = match selected {
            Ok(Some(benchmark)) => Some(benchmark),
            outcome => {
                let error = match outcome {
                    Ok(None) => ServiceError::NotFound,
                    Err(error) => error,
                    Ok(Some(_)) => return Err(ServiceError::InvalidResult),
                };
                steps.push(SourcePreparationStep::from_attempt(
                    "benchmark_history",
                    started,
                    Err(error),
                    context,
                )?);
                None
            }
        };
        if let Some(benchmark) = benchmark {
            // The pair is independently useful to valuation even if subject acquisition fails.
            let published = self
                .activation
                .prepare_default_equity_premium_history(&benchmark, calendars, context)
                .await;
            let published = match published {
                Ok(published) => {
                    steps.push(SourcePreparationStep::from_attempt(
                        "benchmark_history",
                        started,
                        Ok(published.evidence_digest()),
                        context,
                    )?);
                    Some(published)
                }
                Err(error) => {
                    steps.push(SourcePreparationStep::from_attempt(
                        "benchmark_history",
                        started,
                        Err(error),
                        context,
                    )?);
                    None
                }
            };
            if let Some(published) = published {
                let started = clock()?;
                let histories = self
                    .activation
                    .prepare_selected_investment_histories(
                        identity, &benchmark, &published, calendars, context,
                    )
                    .await;
                let histories = match histories {
                    Ok(histories) => {
                        steps.push(SourcePreparationStep::from_attempt(
                            "selected_history",
                            started,
                            Ok(histories.evidence_digest()),
                            context,
                        )?);
                        Some(histories)
                    }
                    Err(error) => {
                        steps.push(SourcePreparationStep::from_attempt(
                            "selected_history",
                            started,
                            Err(error),
                            context,
                        )?);
                        None
                    }
                };
                if let Some(histories) = histories {
                    let (histories, interval) = histories.into_parts();
                    let started = clock()?;
                    let prepared = self
                        .actions
                        .prepare_for_histories(
                            histories,
                            &benchmark,
                            interval,
                            selection_at,
                            context,
                        )
                        .await;
                    let outcome = match prepared {
                        Ok(prepared) => {
                            let (histories, actions, cutoff) = prepared.into_parts();
                            let reference = actions
                                .reference()
                                .map_err(|_| ServiceError::InvalidResult)?;
                            if reference.knowledge_cutoff() != cutoff
                                || cutoff < started
                                || !reference.requested_instruments().contains(&instrument)
                            {
                                return Err(ServiceError::InvalidResult);
                            }
                            // Ownership ends here. The reference retains original history/calendar
                            // coordinates; downstream consumers reopen them through their owner.
                            drop(histories);
                            let bytes = serde_json::to_vec(&reference)
                                .map_err(|_| ServiceError::InvalidResult)?;
                            let digest = EvidenceDigest::new(
                                market_squawk_domain::DigestAlgorithm::Sha256,
                                Sha256::digest(bytes).into(),
                            );
                            source_action_reference = Some(reference);
                            action_cutoff = Some(cutoff);
                            Ok(digest)
                        }
                        Err(error) => Err(error),
                    };
                    steps.push(SourcePreparationStep::from_attempt(
                        "source_actions",
                        started,
                        outcome,
                        context,
                    )?);
                }
            }
        }
        if source_action_reference.as_ref().is_none_or(|reference| benchmark_instrument_id.is_some_and(|id| !reference.requested_instruments().contains(&id))) {
        // Independent subject acquisition does not require the fixed premium pair.
        steps.retain(|step| step.source != "selected_history" && step.source != "source_actions");
        let started = clock()?;
        let subject_record = identity.canonical_record().ok_or(ServiceError::Unavailable)?.clone();
        let acquisition_at = clock()?;
        let population_start = acquisition_at.checked_sub_nanos(i64::from(market_squawk_adapter_alpaca::ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS - 367) * 86_400_000_000_000)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let mut records = vec![subject_record.clone()];
        // Keep the caller's choice unchanged. Absence selects the admitted default; an explicit
        // unavailable comparison stays unavailable and never becomes the default instrument.
        let comparison = RecommendationBenchmarkSelectionReadCapability::new(
            self.research.market_data_instruments(),
        )
        .select_comparison(
            benchmark_instrument_id,
            acquisition_at,
            acquisition_at,
            context.deadline(),
            context.cancellation(),
        )?;
        if let Some(comparison) = comparison.as_ref().filter(|value| value.instrument_id() != instrument) {
            let benchmark_id = comparison.instrument_id();
            let query = market_squawk_data::MarketDataInstrumentPopulationQuery::try_new(
                vec![benchmark_id], acquisition_at, acquisition_at).map_err(|_| ServiceError::InvalidRequest)?;
            let reader = self.research.market_data_instruments();
            let selection = reader.pin_population_as_of(query, context.deadline(), context.cancellation())
                .map_err(crate::application::map_market_definition_read_error)?;
            if selection.disposition() == market_squawk_data::MarketDataInstrumentPopulationDisposition::Complete
                && selection.exclusions().is_empty()
            {
                let [record] = selection.records() else { return Err(ServiceError::InvalidResult); };
                if record.definition().instrument_id() != benchmark_id
                    || record.revision_digest() != comparison.reference_revision_digest()
                {
                    return Err(ServiceError::InvalidResult);
                }
                records.push(record.clone());
            }
        }
        let mut prepared = self.actions.prepare_selected_complete_histories(&records, population_start, acquisition_at, context).await;
        if records.len() == 2 && matches!(prepared, Err(ServiceError::Unavailable | ServiceError::NotFound)) {
            // An unavailable comparison never suppresses genuine subject-only higher/cost events.
            prepared = self.actions.prepare_selected_complete_histories(&[subject_record], population_start, acquisition_at, context).await;
        }
        // Acquisition may cross a canonical revision. The returned source-action cutoff is
        // also the later probability selector's cutoff, so compare the exact same selection now.
        let prepared = prepared.and_then(|prepared| {
            let cutoff = prepared.cutoff();
            let final_comparison = RecommendationBenchmarkSelectionReadCapability::new(
                self.research.market_data_instruments(),
            )
            .select_comparison(
                benchmark_instrument_id,
                cutoff,
                cutoff,
                context.deadline(),
                context.cancellation(),
            )?;
            let selected = |value: &crate::application::SelectedRecommendationBenchmark| {
                (value.instrument_id(), value.reference_revision_digest())
            };
            if comparison.as_ref().map(selected) != final_comparison.as_ref().map(selected) {
                return Err(ServiceError::Unavailable);
            }
            Ok(prepared)
        });
        let outcome = match prepared {
            Ok(prepared) => {
                let reference = prepared.plan().price_reference().map_err(|_| ServiceError::InvalidResult)?;
                let cutoff = prepared.cutoff();
                if reference.knowledge_cutoff() != cutoff || cutoff < started || !reference.requested_instruments().contains(&instrument) {
                    return Err(ServiceError::InvalidResult);
                }
                let bytes = serde_json::to_vec(&reference).map_err(|_| ServiceError::InvalidResult)?;
                let digest = EvidenceDigest::new(market_squawk_domain::DigestAlgorithm::Sha256, Sha256::digest(bytes).into());
                source_action_reference = Some(reference); action_cutoff = Some(cutoff);
                Ok(digest)
            }
            Err(error) => Err(error),
        };
        steps.push(SourcePreparationStep::from_attempt("selected_history", started, outcome, context)?);
        steps.push(match &source_action_reference {
            Some(reference) => SourcePreparationStep::from_attempt("source_actions", started,
                Ok(EvidenceDigest::new(market_squawk_domain::DigestAlgorithm::Sha256,
                    Sha256::digest(serde_json::to_vec(reference).map_err(|_| ServiceError::InvalidResult)?).into())), context)?,
            None => SourcePreparationStep::unavailable("source_actions", started, "source_prerequisite_unavailable".to_owned(), clock()?),
        });
        }
        // Explicit unavailable prerequisite outcomes do not masquerade as provider attempts.
        for source in ["selected_history", "source_actions"] {
            if !steps.iter().any(|step| step.source == source) {
                let at = clock()?;
                steps.push(SourcePreparationStep::unavailable(
                    source,
                    at,
                    "source_prerequisite_unavailable".to_owned(),
                    at,
                ));
            }
        }
        ensure_live(context)?;
        let source_cutoff = match action_cutoff {
            Some(cutoff) => cutoff,
            None => clock()?,
        };
        Ok(AcquiredInvestmentSources {
            steps,
            source_cutoff,
            source_action_reference,
        })
    }

    /// Runs the genuine source reader after acquisition at one final cutoff. A failed premium
    /// leaves independent valuation methods available and never becomes a caller-supplied rate.
    pub(super) async fn assess_premium(
        &self,
        calendars: &CompletedMarketSessionReadCapability,
        source_cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<SourcePreparationStep, ServiceError> {
        let started = clock()?;
        let premium = self
            .macro_reader
            .read_default_equity_premium_from_store(
                &self.research,
                calendars,
                source_cutoff,
                source_cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await;
        ensure_live(context)?;
        match premium {
            Ok(premium) => SourcePreparationStep::from_attempt(
                "equity_premium",
                started,
                Ok(premium.reference().evidence_digest()),
                context,
            ),
            Err(EquityPremiumReadError::Unavailable(reason)) => {
                Ok(SourcePreparationStep::unavailable(
                    "equity_premium",
                    started,
                    reason.to_string(),
                    clock()?,
                ))
            }
            Err(error) => SourcePreparationStep::from_attempt(
                "equity_premium",
                started,
                Err(error.into_service_error()),
                context,
            ),
        }
    }
}

fn map_options_error(error: OptionsContextError) -> ServiceError {
    match error {
        OptionsContextError::Cancelled => ServiceError::Cancelled,
        OptionsContextError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        OptionsContextError::CapacityExceeded => ServiceError::ResourceExhausted,
        OptionsContextError::AnalyticalEvidenceUnavailable => ServiceError::Unavailable,
        OptionsContextError::InvalidRequest
        | OptionsContextError::InvalidEvidence
        | OptionsContextError::ConflictingCanonicalTerms
        | OptionsContextError::ConflictingCanonicalIdentity
        | OptionsContextError::ReferenceEvidenceUnavailable => ServiceError::InvalidResult,
    }
}

/// A bounded audit of a real attempt, not a reusable source receipt or permission grant.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SourcePreparationStep {
    pub(super) source: &'static str,
    status: &'static str,
    evidence_digest: Option<String>,
    failure: Option<String>,
    started_at_unix_nanos: String,
    completed_at_unix_nanos: String,
}

impl SourcePreparationStep {
    pub(super) fn from_attempt(
        source: &'static str,
        started_at: Timestamp,
        outcome: Result<EvidenceDigest, ServiceError>,
        context: &RequestContext,
    ) -> Result<Self, ServiceError> {
        ensure_live(context)?;
        let completed_at = clock()?;
        if completed_at < started_at {
            return Err(ServiceError::InvalidResult);
        }
        match outcome {
            Ok(digest) => Ok(Self {
                source,
                status: "available",
                evidence_digest: Some(encode_digest(digest.bytes())),
                failure: None,
                started_at_unix_nanos: started_at.unix_nanos().to_string(),
                completed_at_unix_nanos: completed_at.unix_nanos().to_string(),
            }),
            Err(error @ (ServiceError::Unavailable | ServiceError::NotFound)) => Ok(Self::unavailable(
                source,
                started_at,
                service_failure(error).to_owned(),
                completed_at,
            )),
            Err(error) => Err(error),
        }
    }

    fn unavailable(
        source: &'static str,
        started_at: Timestamp,
        failure: String,
        completed_at: Timestamp,
    ) -> Self {
        Self {
            source,
            status: "unavailable",
            evidence_digest: None,
            failure: Some(failure),
            started_at_unix_nanos: started_at.unix_nanos().to_string(),
            completed_at_unix_nanos: completed_at.unix_nanos().to_string(),
        }
    }

    pub(super) fn is_available(&self) -> bool {
        self.status == "available"
    }
}

pub(super) fn reference_digest(
    reference: &impl serde::Serialize,
) -> Result<EvidenceDigest, ServiceError> {
    let bytes = serde_json::to_vec(reference).map_err(|_| ServiceError::InvalidResult)?;
    Ok(EvidenceDigest::new(
        market_squawk_domain::DigestAlgorithm::Sha256,
        Sha256::digest(bytes).into(),
    ))
}

pub(super) fn clock() -> Result<Timestamp, ServiceError> {
    super::super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)
}

fn service_failure(error: ServiceError) -> &'static str {
    match error {
        ServiceError::InvalidRequest => "invalid_request",
        ServiceError::NotFound => "source_not_found",
        ServiceError::Unauthorized => "source_not_authorized",
        ServiceError::ResourceExhausted => "resource_bound_exceeded",
        ServiceError::Unavailable => "source_unavailable",
        ServiceError::InvalidResult => "source_evidence_invalid",
        ServiceError::Internal => "source_internal_error",
        ServiceError::Cancelled => "cancelled",
        ServiceError::DeadlineExceeded => "deadline_exceeded",
    }
}

fn map_board_error(error: BoardFullHistoryApplicationError) -> ServiceError {
    use crate::ResearchServiceError;
    use market_squawk_adapter_federal_reserve::{
        BoardAdapterError as Parse, BoardExtractionError as Extraction,
        BoardFullHistoryError as Full, BoardSourceError as Board,
    };
    use market_squawk_data::{CatalogError, IngestError, ManifestCatalogError, ParquetStoreError};
    use market_squawk_platform::{
        ResearchObjectControlError as Control, SealedResearchJournalStoreError as Store,
    };
    use market_squawk_sources::{
        ExtractionAuthorityError as Authority, ExtractionError as Contract,
        ExtractionSourceError as Source, ObservedRevisionError,
        ProviderLogicalPublicationError as Logical, SourceError,
    };

    fn store(error: Store) -> ServiceError {
        match error {
            Store::ObjectControl(Control::Cancelled) => ServiceError::Cancelled,
            Store::ObjectControl(Control::DeadlineExceeded) => ServiceError::DeadlineExceeded,
            Store::FrameLimitExceeded { .. }
            | Store::ByteLimitExceeded { .. }
            | Store::ObjectByteLimitExceeded { .. }
            | Store::ObjectChunkLimitExceeded { .. }
            | Store::ObjectAllocationFailed => ServiceError::ResourceExhausted,
            // Unavailable trusted control and indeterminate/corrupt storage remain fatal.
            _ => ServiceError::Internal,
        }
    }
    fn parse(error: Parse) -> ServiceError {
        match error {
            Parse::ControlledRead(Control::Cancelled) => ServiceError::Cancelled,
            Parse::ControlledRead(Control::DeadlineExceeded) => ServiceError::DeadlineExceeded,
            Parse::ControlledRead(Control::Unavailable) => ServiceError::Internal,
            Parse::ByteLimitExceeded
            | Parse::StructuralLimitExceeded
            | Parse::AllocationFailed
            | Parse::CompressionRatioExceeded
            | Parse::CountOverflow => ServiceError::ResourceExhausted,
            _ => ServiceError::InvalidResult,
        }
    }
    fn extraction(error: Source) -> ServiceError {
        match error {
            Source::Cancelled | Source::Source(SourceError::Cancelled) => ServiceError::Cancelled,
            Source::DeadlineExceeded => ServiceError::DeadlineExceeded,
            Source::Authority(
                Authority::NotCurrent | Authority::NotEffective | Authority::NetworkDenied,
            )
            | Source::Source(SourceError::Unauthorized) => ServiceError::Unauthorized,
            Source::Contract(
                Contract::LimitTooLarge { .. }
                | Contract::DiscoveryLimitExceeded { .. }
                | Contract::RecordTooLarge { .. }
                | Contract::RecordLimitExceeded { .. }
                | Contract::ByteLimitExceeded { .. }
                | Contract::ByteCountOverflow
                | Contract::AllocationFailed,
            ) => ServiceError::ResourceExhausted,
            Source::Source(SourceError::ProviderUnavailable | SourceError::Network) => {
                ServiceError::Unavailable
            }
            Source::Source(
                SourceError::FrameTooLarge { .. } | SourceError::FrameIdentityExhausted,
            ) => ServiceError::ResourceExhausted,
            Source::Contract(_) | Source::Source(SourceError::InvalidProtocolState) => {
                ServiceError::InvalidResult
            }
            // Requires the paired transport correction: oversized bodies retain FrameTooLarge
            // with the actual request bound; Network represents only transport unavailability.
            _ => ServiceError::Internal,
        }
    }
    fn manifest(error: ManifestCatalogError) -> ServiceError {
        match error {
            ManifestCatalogError::Cancelled => ServiceError::Cancelled,
            ManifestCatalogError::DeadlineExceeded => ServiceError::DeadlineExceeded,
            _ => ServiceError::Internal,
        }
    }
    fn catalog(error: CatalogError) -> ServiceError {
        match error {
            CatalogError::QueryArtifactCancelled
            | CatalogError::InstrumentDefinitionReadCancelled
            | CatalogError::CompanyIdentityReadCancelled
            | CatalogError::MarketRecoveryReadCancelled
            | CatalogError::AnalyticalEvidenceCancelled => ServiceError::Cancelled,
            CatalogError::OnboardingDeadlineExceeded
            | CatalogError::QueryArtifactDeadlineExceeded
            | CatalogError::InstrumentDefinitionReadDeadlineExceeded
            | CatalogError::CompanyIdentityReadDeadlineExceeded
            | CatalogError::MarketRecoveryReadDeadlineExceeded => ServiceError::DeadlineExceeded,
            CatalogError::AnalyticalEvidenceLimitExceeded | CatalogError::Allocation => {
                ServiceError::ResourceExhausted
            }
            _ => ServiceError::Internal,
        }
    }
    fn ingest(error: IngestError) -> ServiceError {
        match error {
            IngestError::Cancelled => ServiceError::Cancelled,
            IngestError::DeadlineExceeded => ServiceError::DeadlineExceeded,
            IngestError::SealedProviderCapture(error) => store(error),
            IngestError::Manifest(error) => manifest(error),
            IngestError::Catalog(error) => catalog(error),
            IngestError::Parquet(error) => match error {
                ParquetStoreError::Cancelled => ServiceError::Cancelled,
                ParquetStoreError::RecoveryDeadlineExceeded
                | ParquetStoreError::ReadDeadlineExceeded => ServiceError::DeadlineExceeded,
                ParquetStoreError::StagingLimitExceeded
                | ParquetStoreError::ReadLimitExceeded
                | ParquetStoreError::SizeOverflow
                | ParquetStoreError::BlockingTaskLimitExceeded
                | ParquetStoreError::RecoveryScanLimit => ServiceError::ResourceExhausted,
                _ => ServiceError::Internal,
            },
            IngestError::RevisionAuthority(error) => match error {
                ObservedRevisionError::Cancelled => ServiceError::Cancelled,
                ObservedRevisionError::DeadlineExceeded => ServiceError::DeadlineExceeded,
                _ => ServiceError::InvalidResult,
            },
            IngestError::PublicationAuthorityRevoked
            | IngestError::AuthorityTransitionRejected
            | IngestError::PersistRightsRequired => ServiceError::Unauthorized,
            _ => ServiceError::Internal,
        }
    }
    match error {
        BoardFullHistoryApplicationError::Service(error) => error,
        BoardFullHistoryApplicationError::Data(error) => ingest(error),
        BoardFullHistoryApplicationError::Research(error) => match error {
            ResearchServiceError::Ingest(error) => ingest(error),
            ResearchServiceError::ProviderCaptureStore(error) => store(error),
            ResearchServiceError::Manifest(error) => manifest(error),
            ResearchServiceError::Catalog(error) => catalog(error),
            ResearchServiceError::Rights(_) => ServiceError::Unauthorized,
            ResearchServiceError::IngestAuthorityMismatch => ServiceError::InvalidResult,
            _ => ServiceError::Internal,
        },
        BoardFullHistoryApplicationError::Source(error) => match error {
            Full::Source(error) => extraction(error),
            Full::Board(error) => match error {
                Board::Cancelled => ServiceError::Cancelled,
                Board::DeadlineExceeded => ServiceError::DeadlineExceeded,
                Board::Network => ServiceError::Unavailable,
                Board::BodyTooLarge | Board::PartitionedExtractionRequired => {
                    ServiceError::ResourceExhausted
                }
                Board::Protocol(error) => parse(error),
                Board::InvalidMetadata => ServiceError::Unauthorized,
                Board::HealthUnavailable => ServiceError::Internal,
                Board::InvalidProfile | Board::InvalidValidator | Board::CanonicalMapping => {
                    ServiceError::InvalidResult
                }
            },
            Full::Parse(error) => parse(error),
            Full::Store(error) => store(error),
            Full::Logical(error) => match error {
                Logical::ObjectStore(error) => store(error),
                Logical::Allocation
                | Logical::FrameLimitExceeded
                | Logical::PartitionLimitExceeded
                | Logical::OrdinalOverflow
                | Logical::CountOverflow
                | Logical::CatalogMetadataLimitExceeded => ServiceError::ResourceExhausted,
                Logical::Io(_) | Logical::Poisoned | Logical::StateConflict => {
                    ServiceError::Internal
                }
                _ => ServiceError::InvalidResult,
            },
            Full::Extraction(error) => match error {
                Extraction::Source(error) => extraction(error),
                Extraction::CaptureBodyTooLarge { .. } => ServiceError::ResourceExhausted,
                _ => ServiceError::InvalidResult,
            },
            Full::ResourceBound => ServiceError::ResourceExhausted,
            Full::InvalidEvidence => ServiceError::InvalidResult,
            Full::Io(_) => ServiceError::Internal,
        },
        BoardFullHistoryApplicationError::OriginalMismatch => ServiceError::InvalidResult,
        BoardFullHistoryApplicationError::Composition(_) => ServiceError::Internal,
    }
}
