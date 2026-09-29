//! First source acquisition for the fixed canonical benchmark pair.
//!
//! Source generation leases, raw capture, native fields and calendar authority stay with their
//! existing owners. The caller establishes its final knowledge cutoff after this method returns.

use super::*;
use crate::application::{
    InstrumentContextOutcome, InstrumentContextRead, RecommendationBenchmarkSelection,
    SelectedRecommendationBenchmark, TiingoEodHistoryPublicationReceipt,
    market_calendar::{
        CompletedMarketSessionReadCapability, TiingoCalendarExpectedSessionAuthority,
    },
    required_annual_source_dates,
};
use market_squawk_adapter_tiingo::{
    TiingoCapturedPage, TiingoEodCashUnitEvidence, TiingoEodInstrumentKind, TiingoExchangeCode,
    TiingoHistoryCheckpointReceipt, TiingoHistoryPlan, TiingoMetadataReceipt,
    tiingo_eod_native_schema_evidence,
};
use market_squawk_data::{CompleteMarketBarHistoryCursor, MarketDataInstrumentRecord};
use market_squawk_domain::{
    AssetClass, CalendarDate, DigestAlgorithm, ExactPayloadEvidence, MetadataRevision,
    ProviderInstrumentId, RevisionBoundPayloadEvidence, VenueId,
};
use market_squawk_services::{RequestContext, ServiceError};
use sha2::{Digest as _, Sha256};
use std::num::NonZeroU64;
use std::time::{SystemTime, UNIX_EPOCH};

/// All fields originate from the exact activation, admitted identity, actual metadata body and
/// calendar read. There is deliberately no public constructor accepting caller-authored digests.
pub(crate) struct TiingoEodHistoryOperation {
    pub(crate) plan: TiingoHistoryPlan,
    pub(crate) checkpoint: TiingoHistoryCheckpointReceipt,
    pub(crate) captured_metadata: TiingoHistoryMetadataInput,
    pub(crate) original_context: TiingoHistoryOriginalContext,
    pub(crate) instrument: TiingoEodInstrumentAuthority,
    pub(crate) contract: TiingoEodContractEvidence,
    pub(crate) expected_session_authority: Arc<TiingoCalendarExpectedSessionAuthority>,
    pub(crate) cash_unit: Option<TiingoEodCashUnitEvidence>,
    pub(crate) admitted_plan_digest: EvidenceDigest,
    pub(crate) analytical_dataset: DatasetId,
}

/// Original metadata is either newly captured or physically reopened from existing raw custody.
pub(crate) enum TiingoHistoryMetadataInput {
    Fresh(TiingoCapturedPage<TiingoMetadataReceipt>),
    Original {
        original: market_squawk_data::ProviderCaptureOriginalReceipt,
        decoded: TiingoMetadataReceipt,
    },
}
impl TiingoHistoryMetadataInput {
    pub(crate) fn decoded(&self) -> &TiingoMetadataReceipt {
        match self {
            Self::Fresh(value) => value.decoded(),
            Self::Original { decoded, .. } => decoded,
        }
    }
}
/// Inert original caller coordinates; source replay, canonical identity and calendar reopen remain required.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct TiingoHistoryOriginalContext {
    pub(crate) session: EvidenceDigest,
    pub(crate) instrument_revision: EvidenceDigest,
    pub(crate) source_metadata_digest: [u8; 32],
    pub(crate) credential_generation: u64,
    pub(crate) admitted_plan_digest: EvidenceDigest,
    pub(crate) calendar: crate::application::market_calendar::CompletedMarketSessionReference,
    pub(crate) calendar_cutoff: Timestamp,
    pub(crate) calendar_resolved_at: Timestamp,
    pub(crate) calendar_expected_digest: EvidenceDigest,
    pub(crate) native_contract_revision: SourceIdentifier,
    pub(crate) entitlement_generation: SourceIdentifier,
}

/// Two genuine immutable source publications. The digest is a bounded audit identifier, never a
/// substitute for reopening either complete history and its original source calendar.
#[derive(Debug)]
pub(crate) struct TiingoEquityPremiumHistoryPreparation {
    primary: TiingoEodHistoryPublicationReceipt,
    accompanying: TiingoEodHistoryPublicationReceipt,
    evidence_digest: EvidenceDigest,
    dates: (CalendarDate, CalendarDate),
    selection_digest: market_squawk_data::Sha256Digest,
}
impl TiingoEquityPremiumHistoryPreparation {
    pub(crate) const fn primary(&self) -> &TiingoEodHistoryPublicationReceipt {
        &self.primary
    }
    pub(crate) const fn accompanying(&self) -> &TiingoEodHistoryPublicationReceipt {
        &self.accompanying
    }
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

impl ProviderAdapterActivation {
    /// Acquires both fixed instruments before a new product cutoff is created. An original saved
    /// cutoff never enters acquisition; source replay uses only the subsequently frozen cutoff.
    pub(crate) async fn prepare_default_equity_premium_history(
        &self,
        benchmarks: &RecommendationBenchmarkSelection,
        calendars: &CompletedMarketSessionReadCapability,
        context: &RequestContext,
    ) -> Result<TiingoEquityPremiumHistoryPreparation, ServiceError> {
        check_context(context)?;
        let started_at = current_time()?;
        let dates = completed_source_dates(calendars, started_at, context).await?;
        let activation = self
            .tiingo
            .read()
            .map_err(|_| operation_error(context))?
            .as_ref()
            .cloned()
            .ok_or(ServiceError::Unavailable)?;
        let primary = self
            .prepare_benchmark_history(
                &activation,
                benchmarks,
                benchmarks.primary(),
                calendars,
                dates,
                context,
            )
            .await?;
        let accompanying = self
            .prepare_benchmark_history(
                &activation,
                benchmarks,
                benchmarks.accompanying(),
                calendars,
                dates,
                context,
            )
            .await?;
        check_context(context)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/tiingo-fixed-benchmark-history-preparation/v1\0");
        hash.update(benchmarks.selection_digest().bytes());
        for receipt in [&primary, &accompanying] {
            hash.update(receipt.manifest().content_hash().bytes());
            hash.update(receipt.binding_digest().bytes());
            hash.update(receipt.manifest().manifest_version().to_be_bytes());
        }
        Ok(TiingoEquityPremiumHistoryPreparation {
            primary,
            accompanying,
            evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
            dates,
            selection_digest: benchmarks.selection_digest(),
        })
    }

    /// Acquires only the selected subject after independent benchmark preparation. All three
    /// outputs reopen the exact supplied publications, including subject/benchmark overlap.
    pub(crate) async fn prepare_selected_investment_histories(
        &self,
        identity: &InstrumentContextRead,
        benchmarks: &RecommendationBenchmarkSelection,
        prepared: &TiingoEquityPremiumHistoryPreparation,
        calendars: &CompletedMarketSessionReadCapability,
        context: &RequestContext,
    ) -> Result<TiingoSelectedInvestmentHistories, ServiceError> {
        check_context(context)?;
        if prepared.selection_digest != benchmarks.selection_digest() {
            return Err(ServiceError::InvalidRequest);
        }
        let InstrumentContextOutcome::Exact(subject) = identity.outcome() else {
            return Err(ServiceError::Unavailable);
        };
        let record = identity
            .canonical_record()
            .ok_or(ServiceError::Unavailable)?;
        let subject_publication;
        let subject_publication = if subject.instrument_id() == benchmarks.primary().instrument_id()
        {
            &prepared.primary
        } else if subject.instrument_id() == benchmarks.accompanying().instrument_id() {
            &prepared.accompanying
        } else {
            subject_publication = self
                .prepare_instrument_eod_history(
                    record,
                    subject.listing_venue(),
                    calendars,
                    prepared.dates,
                    context,
                )
                .await?;
            &subject_publication
        };
        let cutoff = current_time()?;
        let subject_history = self
            .research
            .read_complete_tiingo_eod_publication(
                subject_publication,
                record,
                subject.listing_venue(),
                prepared.dates,
                calendars,
                cutoff,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|error| history_error(&error, context))?;
        let primary = self
            .reopen_benchmark_history(
                benchmarks,
                benchmarks.primary(),
                &prepared.primary,
                prepared.dates,
                calendars,
                cutoff,
                context,
            )
            .await?;
        let accompanying = self
            .reopen_benchmark_history(
                benchmarks,
                benchmarks.accompanying(),
                &prepared.accompanying,
                prepared.dates,
                calendars,
                cutoff,
                context,
            )
            .await?;
        let histories = [subject_history, primary, accompanying];
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/selected-investment-native-histories/v1\0");
        hash.update(benchmarks.selection_digest().bytes());
        for history in &histories {
            hash.update(history.selection().receipt().receipt_digest().bytes());
            hash.update(history.read_receipt().source_result_digest().bytes());
        }
        check_context(context)?;
        Ok(TiingoSelectedInvestmentHistories {
            histories,
            dates: prepared.dates,
            evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact identity, publication and calendar are independent"
    )]
    async fn reopen_benchmark_history(
        &self,
        benchmarks: &RecommendationBenchmarkSelection,
        benchmark: &SelectedRecommendationBenchmark,
        publication: &TiingoEodHistoryPublicationReceipt,
        dates: (CalendarDate, CalendarDate),
        calendars: &CompletedMarketSessionReadCapability,
        cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<CompleteMarketBarHistoryCursor, ServiceError> {
        let record = benchmarks
            .source_definition(benchmark)
            .ok_or(ServiceError::Unavailable)?;
        let mut mappings = record
            .definition()
            .venue_mappings()
            .iter()
            .filter(|mapping| mapping.venue_symbol().as_str() == benchmark.display_symbol());
        let mapping = mappings.next().ok_or(ServiceError::Unavailable)?;
        if mappings.next().is_some() {
            return Err(ServiceError::Unavailable);
        }
        self.research
            .read_complete_tiingo_eod_publication(
                publication,
                record,
                mapping.venue_id(),
                dates,
                calendars,
                cutoff,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|error| history_error(&error, context))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "source, identity, calendar and request authority are independent"
    )]
    async fn prepare_benchmark_history(
        &self,
        activation: &Arc<TiingoProductActivation>,
        benchmarks: &RecommendationBenchmarkSelection,
        benchmark: &SelectedRecommendationBenchmark,
        calendars: &CompletedMarketSessionReadCapability,
        dates: (CalendarDate, CalendarDate),
        context: &RequestContext,
    ) -> Result<TiingoEodHistoryPublicationReceipt, ServiceError> {
        check_context(context)?;
        let record = benchmarks
            .source_definition(benchmark)
            .ok_or(ServiceError::Unavailable)?;
        let mut mappings = record
            .definition()
            .venue_mappings()
            .iter()
            .filter(|mapping| mapping.venue_symbol().as_str() == benchmark.display_symbol());
        let mapping = mappings.next().ok_or(ServiceError::Unavailable)?;
        if mappings.next().is_some() {
            return Err(ServiceError::Unavailable);
        }
        self.prepare_instrument_eod_history_with_activation(
            activation,
            record,
            mapping.venue_id(),
            Some(benchmark),
            calendars,
            dates,
            context,
        )
        .await
    }

    /// Acquires the exact retained catalog listing and original civil-date horizon. The caller
    /// supplies no ticker or mapping evidence; both come from this source-owned record.
    pub(crate) async fn prepare_instrument_eod_history(
        &self,
        record: &MarketDataInstrumentRecord,
        venue: &VenueId,
        calendars: &CompletedMarketSessionReadCapability,
        dates: (CalendarDate, CalendarDate),
        context: &RequestContext,
    ) -> Result<TiingoEodHistoryPublicationReceipt, ServiceError> {
        check_context(context)?;
        let activation = self
            .tiingo
            .read()
            .map_err(|_| operation_error(context))?
            .as_ref()
            .cloned()
            .ok_or(ServiceError::Unavailable)?;
        self.prepare_instrument_eod_history_with_activation(
            &activation,
            record,
            venue,
            None,
            calendars,
            dates,
            context,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "source, actual catalog listing, dates and calendar have independent authority"
    )]
    async fn prepare_instrument_eod_history_with_activation(
        &self,
        activation: &Arc<TiingoProductActivation>,
        record: &MarketDataInstrumentRecord,
        venue: &VenueId,
        benchmark: Option<&SelectedRecommendationBenchmark>,
        calendars: &CompletedMarketSessionReadCapability,
        dates: (CalendarDate, CalendarDate),
        context: &RequestContext,
    ) -> Result<TiingoEodHistoryPublicationReceipt, ServiceError> {
        check_context(context)?;
        let _history_owner = activation
            .source
            .acquire_history_operation(context.deadline(), context.cancellation())
            .await
            .map_err(|_| operation_error(context))?;
        let definition = record.definition();
        let instrument_kind = match definition.asset_class() {
            AssetClass::Fund => TiingoEodInstrumentKind::ExchangeTradedFund,
            AssetClass::Equity => TiingoEodInstrumentKind::Equity,
            _ => return Err(ServiceError::Unavailable),
        };
        let mut mappings = definition
            .venue_mappings()
            .iter()
            .filter(|mapping| mapping.venue_id() == venue);
        let mapping = mappings.next().ok_or(ServiceError::Unavailable)?;
        if mappings.next().is_some() {
            return Err(ServiceError::Unavailable);
        }
        let ticker = TiingoTicker::try_new(mapping.venue_symbol().as_str())
            .map_err(|_| ServiceError::InvalidResult)?;
        let plan = TiingoHistoryPlan::try_new(ticker.clone(), dates.0, dates.1)
            .map_err(|_| ServiceError::InvalidResult)?;

        // This exact runtime lease covers metadata acquisition and every subsequent history page.
        // Rotation can revoke it; its same-generation precommit remains retained through commit.
        let onboarding = self.onboarding.acquire_runtime_mutation_authority().await;
        onboarding
            .require_active(&activation.lease)
            .map_err(|_| operation_error(context))?;
        let publication = self
            .research
            .acquire_provider_publication_operation(
                activation.generation(),
                context.cancellation().clone(),
                context.deadline(),
            )
            .await
            .map_err(|_| operation_error(context))?;
        drop(onboarding);
        let source_deadline = wall_deadline(context)?;
        publication
            .validate_precommit()
            .map_err(|_| operation_error(context))?;
        if let Some(retained) = self
            .research
            .reuse_complete_tiingo_eod_history(
                record,
                venue,
                dates,
                activation.metadata.source_id(),
                benchmark.map(|_| reviewed_cash_unit_payload()),
                calendars,
                current_time()?,
                context.deadline(),
                publication.cancellation(),
            )
            .await
            .map_err(|error| history_error(&error, context))?
        {
            publication
                .validate_precommit()
                .map_err(|_| operation_error(context))?;
            return Ok(retained);
        }
        let mut session_hash = Sha256::new();
        session_hash.update(b"market-squawk/tiingo-original-history-session/v1\0");
        session_hash.update(plan.request_set_identity().bytes());
        session_hash.update(definition.instrument_id().as_uuid().as_bytes());
        let session = EvidenceDigest::new(DigestAlgorithm::Sha256, session_hash.finalize().into());
        let source_metadata_digest: [u8; 32] = Sha256::digest(
            serde_json::to_vec(&activation.metadata).map_err(|_| ServiceError::InvalidResult)?,
        )
        .into();
        let recovered = self
            .research
            .recover_tiingo_history_metadata(
                session,
                activation.metadata.source_id(),
                &plan,
                context.deadline(),
                publication.cancellation(),
            )
            .await
            .map_err(|error| history_error(&error, context))?;
        let checkpoint = activation
            .source
            .prepare_history_plan(&plan)
            .map_err(|error| source_error(&error, context))?;
        let (captured_metadata, original_context) = match recovered {
            Some((input, original)) => {
                if (checkpoint.next_page_index() == 0
                    && checkpoint.receipt_identity() != original.admitted_plan_digest)
                    || original.instrument_revision != record.revision_digest()
                    || original.source_metadata_digest != source_metadata_digest
                    || original.credential_generation != activation.credential_generation
                    || original.native_contract_revision.as_str() != TIINGO_NATIVE_CONTRACT_REVISION
                    || original.entitlement_generation != activation.entitlement_generation
                {
                    return Err(ServiceError::Unavailable);
                }
                (input, Some(original))
            }
            None if checkpoint.next_page_index() != 0 => return Err(ServiceError::Unavailable),
            None => (
                TiingoHistoryMetadataInput::Fresh(
                    activation
                        .source
                        .fetch_metadata(ticker.clone(), source_deadline, publication.cancellation())
                        .await
                        .map_err(|error| source_error(&error, context))?,
                ),
                None,
            ),
        };
        let metadata = captured_metadata.decoded();
        let native = metadata.metadata();
        let mapping_at = metadata.evidence().decoded_at();
        if definition.effective_interval().starts_at() > mapping_at
            || definition
                .effective_interval()
                .ends_at()
                .is_some_and(|end| mapping_at >= end)
            || native.ticker() != &ticker
            || !native.coverage().contains(dates.0)
            || !native.coverage().contains(dates.1)
            || metadata.evidence().native_contract_revision().as_str()
                != TIINGO_NATIVE_CONTRACT_REVISION
            || metadata.evidence().entitlement_generation() != &activation.entitlement_generation
        {
            return Err(ServiceError::Unavailable);
        }
        // Explicit source exchange names are matched to the source-attested canonical venue.
        // A provider's mutable display name never establishes the instrument identity.
        let expected_exchange = match mapping.venue_id().as_str() {
            "ARCX" => "NYSE ARCA",
            "XNYS" => "NYSE",
            "XNAS" => "NASDAQ",
            _ => return Err(ServiceError::Unavailable),
        };
        if native.exchange_code() != expected_exchange {
            return Err(ServiceError::Unavailable);
        }
        let mut mapping_hash = Sha256::new();
        mapping_hash.update(b"market-squawk/tiingo-canonical-listing-metadata-join/v1\0");
        mapping_hash.update(record.revision_digest().bytes());
        mapping_hash.update(
            definition
                .reference_evidence()
                .payload_evidence()
                .content_digest()
                .bytes(),
        );
        mapping_hash.update(metadata.evidence().body_digest().bytes());
        mapping_hash.update(mapping.venue_id().as_str().as_bytes());
        mapping_hash.update(ticker.as_str().as_bytes());
        let mapping_evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            mapping_hash.finalize().into(),
        ));
        let instrument = TiingoEodInstrumentAuthority::try_new(
            definition.instrument_id(),
            mapping.venue_id().clone(),
            ProviderInstrumentId::try_from(ticker.as_str())
                .map_err(|_| ServiceError::InvalidResult)?,
            ticker,
            TiingoExchangeCode::try_from(native.exchange_code())
                .map_err(|_| ServiceError::InvalidResult)?,
            instrument_kind,
            definition.reference_evidence().clone(),
            mapping_evidence,
            metadata.evidence().decoded_at(),
            definition.quote_currency(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let contract = TiingoEodContractEvidence::try_new(
            activation.metadata.revision().clone(),
            activation
                .metadata
                .revision_evidence()
                .payload_evidence()
                .clone(),
            SourceIdentifier::try_from(TIINGO_NATIVE_CONTRACT_REVISION)
                .map_err(|_| ServiceError::InvalidResult)?,
            tiingo_eod_native_schema_evidence(),
            NonZeroU64::new(activation.credential_generation).ok_or(ServiceError::Unavailable)?,
            activation.entitlement_generation.clone(),
            activation
                .metadata
                .authorization()
                .evidence()
                .content_digest(),
            reviewed_adjusted_surface_evidence(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;

        // The native calendar market is preserved. A bounded reviewed exchange relationship is
        // admitted in the Tiingo calendar bridge, not by changing the calendar's source venue.
        let calendar_venue = crate::application::market_calendar::tiingo_calendar_source_venue(
            instrument.venue_id(),
            dates,
        )
        .ok_or(ServiceError::Unavailable)?;
        let (calendar_reference, calendar_cutoff) = if let Some(original) = &original_context {
            (original.calendar.clone(), original.calendar_cutoff)
        } else {
            use chrono::Datelike as _;
            // Retain closed-day coverage through acquisition, even when the last price is from
            // an earlier session. These are calendar query bounds, never Tiingo bar timestamps.
            let acquisition_date =
                chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(current_time()?.unix_nanos())
                    .with_timezone(&chrono_tz::America::New_York)
                    .date_naive();
            let acquisition_date = CalendarDate::new(
                u16::try_from(acquisition_date.year()).map_err(|_| ServiceError::InvalidRequest)?,
                u8::try_from(acquisition_date.month()).map_err(|_| ServiceError::InvalidRequest)?,
                u8::try_from(acquisition_date.day()).map_err(|_| ServiceError::InvalidRequest)?,
            )
            .map_err(|_| ServiceError::InvalidRequest)?;
            let reference = calendars
                .preflight(
                    &calendar_venue,
                    dates.0,
                    dates.1.max(acquisition_date),
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await
                .map_err(|error| calendar_error(&error, context))?
                .ok_or(ServiceError::Unavailable)?;
            (reference, current_time()?)
        };
        let calendar = calendars
            .read_reference(
                &calendar_reference,
                calendar_cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(|error| calendar_error(&error, context))?
            .ok_or(ServiceError::Unavailable)?;
        let expected_session_authority = if original_context.is_some() {
            let TiingoHistoryMetadataInput::Original { original, .. } = &captured_metadata else {
                return Err(ServiceError::InvalidResult);
            };
            calendar.reopen_tiingo_expected_session_authority(
                &plan,
                &instrument,
                original,
                context.deadline(),
                context.cancellation().clone(),
            )
        } else {
            calendar.tiingo_expected_session_authority(
                &plan,
                &instrument,
                calendar_cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
        }
        .map_err(|error| calendar_error(&error, context))?;
        let admitted_plan_digest = original_context
            .as_ref()
            .map_or(checkpoint.receipt_identity(), |original| {
                original.admitted_plan_digest
            });
        let original_context = original_context.unwrap_or(TiingoHistoryOriginalContext {
            session,
            instrument_revision: record.revision_digest(),
            source_metadata_digest,
            credential_generation: activation.credential_generation,
            admitted_plan_digest,
            calendar: calendar_reference,
            calendar_cutoff,
            calendar_resolved_at: expected_session_authority.expected_evidence().resolved_at(),
            calendar_expected_digest: expected_session_authority
                .expected_evidence()
                .evidence_identity(),
            native_contract_revision: SourceIdentifier::try_from(TIINGO_NATIVE_CONTRACT_REVISION)
                .map_err(|_| ServiceError::InvalidResult)?,
            entitlement_generation: activation.entitlement_generation.clone(),
        });
        let analytical_dataset = DatasetId::try_from(
            format!("tiingo-eod-history-{}", instrument.instrument_id(),).as_str(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let cash_unit = inferred_cash_unit(record, benchmark, &instrument, &contract)?;
        let operation = TiingoEodHistoryOperation {
            plan,
            checkpoint,
            captured_metadata,
            original_context,
            instrument,
            contract,
            expected_session_authority,
            cash_unit,
            admitted_plan_digest,
            analytical_dataset,
        };
        self.research
            .acquire_and_publish_tiingo_eod_history(
                Arc::clone(&activation.source),
                operation,
                publication,
                source_deadline,
                context.deadline(),
            )
            .await
            .map_err(|error| history_error(&error, context))
    }
}

fn reviewed_adjusted_surface_evidence() -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(include_bytes!("history_contract.md")).into(),
    ))
}
fn check_context(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}
fn operation_error(context: &RequestContext) -> ServiceError {
    check_context(context)
        .err()
        .unwrap_or(ServiceError::Internal)
}

// Only explicit source absence/availability reaches an unavailable source step. A shared
// authority, settlement, capture or decoding failure is never an availability substitute.
pub(super) fn source_error(
    error: &TiingoHttpSourceError,
    context: &RequestContext,
) -> ServiceError {
    use market_squawk_adapter_tiingo::TiingoTransportFailureKind as Transport;
    let classified = match error {
        TiingoHttpSourceError::Provider(failure) => {
            provider_status_error(failure.provider().status())
        }
        TiingoHttpSourceError::Adapter(error) => adapter_error(error),
        TiingoHttpSourceError::Decode(failure) => adapter_error(failure.error()),
        TiingoHttpSourceError::Transport(failure) => match failure.kind() {
            Transport::Cancelled => ServiceError::Cancelled,
            Transport::DeadlineExceeded => ServiceError::DeadlineExceeded,
            Transport::Network => ServiceError::Unavailable,
            Transport::BodyTooLarge => ServiceError::ResourceExhausted,
            Transport::InvalidResponseHeaders => ServiceError::InvalidResult,
            Transport::ClockUnavailable => ServiceError::Internal,
        },
        TiingoHttpSourceError::QuotaDenied(_) => ServiceError::ResourceExhausted,
        TiingoHttpSourceError::InvalidHttpResponse(_) => ServiceError::InvalidResult,
        TiingoHttpSourceError::HistoryEvidence(
            market_squawk_adapter_tiingo::TiingoHistoryEvidenceError::Allocation,
        ) => ServiceError::ResourceExhausted,
        TiingoHttpSourceError::HistoryEvidence(_) => ServiceError::InvalidResult,
        // Includes BudgetUnavailable and all settlement-persistence errors, even when they
        // contain a network/5xx failure. Their durable authority did not finish safely.
        _ => ServiceError::Internal,
    };
    check_context(context).err().unwrap_or(classified)
}

fn provider_status_error(status: u16) -> ServiceError {
    match status {
        401 | 403 => ServiceError::Unauthorized,
        404 => ServiceError::NotFound,
        408 => ServiceError::DeadlineExceeded,
        429 => ServiceError::ResourceExhausted,
        500..=599 => ServiceError::Unavailable,
        400 | 422 => ServiceError::InvalidRequest,
        _ => ServiceError::InvalidResult,
    }
}

fn adapter_error(error: &TiingoAdapterError) -> ServiceError {
    match error {
        TiingoAdapterError::Provider(failure) => provider_status_error(failure.status()),
        TiingoAdapterError::InvalidToken => ServiceError::Unauthorized,
        TiingoAdapterError::InvalidTicker | TiingoAdapterError::InvalidDateRange => {
            ServiceError::InvalidRequest
        }
        TiingoAdapterError::HistoryTooLarge | TiingoAdapterError::BodyTooLarge => {
            ServiceError::ResourceExhausted
        }
        TiingoAdapterError::RequestBuild => ServiceError::Internal,
        _ => ServiceError::InvalidResult,
    }
}

fn calendar_error(
    error: &crate::application::market_calendar::CompletedMarketSessionError,
    context: &RequestContext,
) -> ServiceError {
    use crate::application::market_calendar::CompletedMarketSessionError as Calendar;
    let classified = match error {
        Calendar::InvalidRequest => ServiceError::InvalidRequest,
        Calendar::InvalidEvidence => ServiceError::InvalidResult,
        Calendar::ResourceBoundExceeded => ServiceError::ResourceExhausted,
        Calendar::Unavailable => ServiceError::Unavailable,
        Calendar::Cancelled => ServiceError::Cancelled,
        Calendar::DeadlineExceeded => ServiceError::DeadlineExceeded,
    };
    check_context(context).err().unwrap_or(classified)
}

fn history_error(
    error: &crate::application::TiingoHistoryApplicationError,
    context: &RequestContext,
) -> ServiceError {
    use crate::application::{
        TiingoHistoryApplicationError as History, TiingoLatestApplicationError as Latest,
    };
    use market_squawk_adapter_tiingo::{TiingoEodMapError, TiingoHistoryEvidenceError};
    let classified = match error {
        History::Source(error) => source_error(error, context),
        History::Calendar(error) => calendar_error(error, context),
        History::Research(error) | History::Latest(Latest::Research(error)) => {
            research_error(error)
        }
        History::Ingest(error) | History::Latest(Latest::Ingest(error)) => ingest_error(error),
        History::Latest(Latest::Rights(error)) => match error {
            // A rights denial is not absent market data, even if a lower boundary named it
            // unavailable. All other original service errors remain fatal and unchanged.
            ServiceError::Unavailable | ServiceError::NotFound => ServiceError::Unauthorized,
            error => *error,
        },
        History::Eod(TiingoEodMapError::Allocation)
        | History::Evidence(TiingoHistoryEvidenceError::Allocation) => {
            ServiceError::ResourceExhausted
        }
        History::Read(error) | History::Latest(Latest::AnalyticalRead(error)) => read_error(error),
        History::Admission
        | History::OriginalContinuationRequired
        | History::Eod(_)
        | History::Evidence(_)
        | History::Capture(_)
        | History::CaptureMaterial(_)
        | History::Publication(_)
        | History::Extraction(_)
        | History::Latest(
            Latest::FamilyMismatch
            | Latest::RestartInvalid
            | Latest::Adapter(_)
            | Latest::Capture(_),
        ) => ServiceError::InvalidResult,
        History::Latest(Latest::AuthorityInvalid) => ServiceError::Unauthorized,
    };
    check_context(context).err().unwrap_or(classified)
}

fn ingest_error(error: &market_squawk_data::IngestError) -> ServiceError {
    use market_squawk_data::IngestError as Ingest;
    match error {
        Ingest::Cancelled => ServiceError::Cancelled,
        Ingest::DeadlineExceeded => ServiceError::DeadlineExceeded,
        Ingest::PublicationAuthorityRevoked
        | Ingest::AuthorityTransitionRejected
        | Ingest::PersistRightsRequired => ServiceError::Unauthorized,
        _ => ServiceError::Internal,
    }
}

fn research_error(error: &crate::ResearchServiceError) -> ServiceError {
    use crate::ResearchServiceError as Research;
    use market_squawk_platform::{
        ResearchObjectControlError as Control, SealedResearchJournalStoreError as Store,
    };
    match error {
        Research::Ingest(error) => ingest_error(error),
        Research::ProviderCaptureStore(Store::ObjectControl(Control::Cancelled)) => {
            ServiceError::Cancelled
        }
        Research::ProviderCaptureStore(Store::ObjectControl(Control::DeadlineExceeded)) => {
            ServiceError::DeadlineExceeded
        }
        Research::IngestAuthorityMismatch => ServiceError::InvalidResult,
        Research::Rights(_) => ServiceError::Unauthorized,
        _ => ServiceError::Internal,
    }
}

fn read_error(error: &market_squawk_data::AnalyticalReadError) -> ServiceError {
    use market_squawk_data::{AnalyticalReadError as Read, ParquetStoreError, QueryError};
    use market_squawk_platform::ResearchObjectControlError as Control;
    match error {
        Read::NativeSessionControl(Control::Cancelled)
        | Read::Query(QueryError::Cancelled)
        | Read::Parquet(ParquetStoreError::Cancelled) => ServiceError::Cancelled,
        Read::NativeSessionControl(Control::DeadlineExceeded)
        | Read::Query(QueryError::DeadlineExceeded)
        | Read::Parquet(ParquetStoreError::ReadDeadlineExceeded) => ServiceError::DeadlineExceeded,
        _ => ServiceError::Internal,
    }
}

fn current_time() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos()).map_err(|_| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
fn wall_deadline(context: &RequestContext) -> Result<Timestamp, ServiceError> {
    check_context(context)?;
    let remaining = context.deadline().saturating_duration_since(Instant::now());
    let nanos = i64::try_from(remaining.as_nanos()).map_err(|_| ServiceError::InvalidRequest)?;
    current_time()?
        .checked_add_nanos(nanos)
        .map_err(|_| ServiceError::InvalidRequest)
}

/// Genuine owning reads from three exact publications, in subject/SPY/VTI order.
pub(crate) struct TiingoSelectedInvestmentHistories {
    histories: [CompleteMarketBarHistoryCursor; 3],
    dates: (CalendarDate, CalendarDate),
    evidence_digest: EvidenceDigest,
}
impl TiingoSelectedInvestmentHistories {
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        [CompleteMarketBarHistoryCursor; 3],
        (CalendarDate, CalendarDate),
    ) {
        (self.histories, self.dates)
    }
}

/// Keeps the established completed-year start; only source-authored completed sessions extend
/// acquisition. The annual premium evaluator independently retains its completed-year endpoints.
async fn completed_source_dates(
    calendars: &CompletedMarketSessionReadCapability,
    started_at: Timestamp,
    context: &RequestContext,
) -> Result<(CalendarDate, CalendarDate), ServiceError> {
    let annual = required_annual_source_dates(started_at).map_err(|_| operation_error(context))?;
    let upper = started_at
        .utc_calendar_date()
        .map_err(|_| ServiceError::InvalidRequest)?;
    let venue = VenueId::try_from("XNYS").map_err(|_| ServiceError::Internal)?;
    let reference = calendars
        .preflight(
            &venue,
            annual.0,
            upper,
            context.deadline(),
            context.cancellation().clone(),
        )
        .await
        .map_err(|error| calendar_error(&error, context))?
        .ok_or(ServiceError::Unavailable)?;
    let cutoff = current_time()?;
    let calendar = calendars
        .read_reference(
            &reference,
            cutoff,
            context.deadline(),
            context.cancellation().clone(),
        )
        .await
        .map_err(|error| calendar_error(&error, context))?
        .ok_or(ServiceError::Unavailable)?;
    let completed = calendar
        .native_session_replay()
        .sessions()
        .iter()
        .rev()
        .find_map(|day| {
            calendar
                .date_session_on(day.date(), cutoff, cutoff)
                .filter(|session| session.closes_at_exclusive() <= cutoff)
                .map(|session| session.date())
        })
        .ok_or(ServiceError::Unavailable)?;
    check_context(context)?;
    Ok((annual.0, annual.1.max(completed)))
}

const CASH_UNIT_REVISION: &str = "tiingo-spy-vti-inferred-usd-original-share-v1";
fn reviewed_cash_unit_payload() -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(include_bytes!("cash_unit_interpretation.md")).into(),
    ))
}
fn inferred_cash_unit(
    record: &MarketDataInstrumentRecord,
    benchmark: Option<&SelectedRecommendationBenchmark>,
    instrument: &TiingoEodInstrumentAuthority,
    contract: &TiingoEodContractEvidence,
) -> Result<Option<TiingoEodCashUnitEvidence>, ServiceError> {
    let Some(benchmark) = benchmark else {
        return Ok(None);
    };
    if record.definition().instrument_id() != benchmark.instrument_id()
        || record.revision_digest() != benchmark.reference_revision_digest()
        || record.definition().asset_class() != AssetClass::Fund
        || record.definition().quote_currency().as_str() != "USD"
        || instrument.venue_id().as_str() != "ARCX"
        || instrument.ticker().as_str() != benchmark.display_symbol()
        || !matches!(benchmark.display_symbol(), "SPY" | "VTI")
    {
        return Err(ServiceError::Unavailable);
    }
    let assertion = RevisionBoundPayloadEvidence::new(
        MetadataRevision::new(
            SourceIdentifier::try_from(CASH_UNIT_REVISION).map_err(|_| ServiceError::Internal)?,
        ),
        reviewed_cash_unit_payload(),
    );
    TiingoEodCashUnitEvidence::try_new_with_status(
        instrument.instrument_id(),
        contract.mapping_identity(),
        record.definition().quote_currency(),
        assertion,
        current_time()?,
        market_squawk_sources::MarketHistoryCashUnitStatus::ReviewedInference,
    )
    .map(Some)
    .map_err(|_| ServiceError::InvalidResult)
}
