//! Reconstructs original native fields through the existing controlled raw-generation reader.

use super::*;
use market_squawk_data::CompleteMarketBarHistoryRequest;
use market_squawk_domain::MarketBarAdjustment;
use market_squawk_domain::{CalendarDate, InstrumentId, ProviderInstrumentId, VenueId};
use serde::{Deserialize, Serialize};

/// Exact immutable selected history and its original native publication. This reference conveys
/// reconstruction coordinates; only the controlled reader can return completed source evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    try_from = "TiingoCompletedEodHistoryReferenceWire",
    into = "TiingoCompletedEodHistoryReferenceWire"
)]
pub(crate) struct TiingoCompletedEodHistoryReference {
    version: u16,
    selected_manifest: DatasetManifestRef,
    publication_digest: market_squawk_data::Sha256Digest,
    binding_digest: EvidenceDigest,
    knowledge_cutoff: Timestamp,
    read_digest: market_squawk_data::Sha256Digest,
    instrument_id: InstrumentId,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    surface_requirement: market_squawk_data::MarketHistoryPriceSurfaceRequirement,
    start_date: CalendarDate,
    end_date: CalendarDate,
    ruleset: SourceIdentifier,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TiingoCompletedEodHistoryReferenceWire {
    version: u16,
    selected_dataset: String,
    selected_version: u64,
    selected_schema_name: String,
    selected_schema_version: u16,
    selected_schema_fingerprint: [u8; 32],
    selected_content_digest: [u8; 32],
    publication_digest: [u8; 32],
    binding_digest: EvidenceDigest,
    knowledge_cutoff: Timestamp,
    read_digest: [u8; 32],
    instrument_id: InstrumentId,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    surface_requirement: market_squawk_data::MarketHistoryPriceSurfaceRequirement,
    start_date: CalendarDate,
    end_date: CalendarDate,
    ruleset: SourceIdentifier,
}
impl From<TiingoCompletedEodHistoryReference> for TiingoCompletedEodHistoryReferenceWire {
    fn from(value: TiingoCompletedEodHistoryReference) -> Self {
        Self {
            selected_dataset: value.selected_manifest.dataset_id().as_str().to_owned(),
            selected_version: value.selected_manifest.manifest_version(),
            selected_schema_name: value.selected_manifest.schema().name().to_owned(),
            selected_schema_version: value.selected_manifest.schema_version().get(),
            selected_schema_fingerprint: value.selected_manifest.schema().fingerprint(),
            selected_content_digest: value.selected_manifest.content_hash().bytes(),
            publication_digest: value.publication_digest.bytes(),
            read_digest: value.read_digest.bytes(),
            version: value.version,
            binding_digest: value.binding_digest,
            knowledge_cutoff: value.knowledge_cutoff,
            instrument_id: value.instrument_id,
            provider_instrument_id: value.provider_instrument_id,
            venue_id: value.venue_id,
            feed: value.feed,
            interval: value.interval,
            adjustment: value.adjustment,
            surface_requirement: value.surface_requirement,
            start_date: value.start_date,
            end_date: value.end_date,
            ruleset: value.ruleset,
        }
    }
}
impl TryFrom<TiingoCompletedEodHistoryReferenceWire> for TiingoCompletedEodHistoryReference {
    type Error = &'static str;
    fn try_from(value: TiingoCompletedEodHistoryReferenceWire) -> Result<Self, Self::Error> {
        let invalid = || "invalid Tiingo history reference";
        if value.selected_dataset.len() > 256
            || value.selected_schema_name.len() > 256
            || value.selected_content_digest == [0; 32]
            || value.selected_schema_fingerprint == [0; 32]
        {
            return Err(invalid());
        }
        let schema = market_squawk_data::DatasetSchemaRef::try_new(
            &value.selected_schema_name,
            market_squawk_domain::SchemaVersion::new(value.selected_schema_version)
                .map_err(|_| invalid())?,
            value.selected_schema_fingerprint,
        )
        .map_err(|_| invalid())?;
        let selected_manifest = DatasetManifestRef::try_new_with_schema(
            market_squawk_data::DatasetId::try_from(value.selected_dataset.as_str())
                .map_err(|_| invalid())?,
            value.selected_version,
            schema,
            market_squawk_data::Sha256Digest::new(value.selected_content_digest),
        )
        .map_err(|_| invalid())?;
        let reference = Self {
            selected_manifest,
            publication_digest: market_squawk_data::Sha256Digest::new(value.publication_digest),
            read_digest: market_squawk_data::Sha256Digest::new(value.read_digest),
            version: value.version,
            binding_digest: value.binding_digest,
            knowledge_cutoff: value.knowledge_cutoff,
            instrument_id: value.instrument_id,
            provider_instrument_id: value.provider_instrument_id,
            venue_id: value.venue_id,
            feed: value.feed,
            interval: value.interval,
            adjustment: value.adjustment,
            surface_requirement: value.surface_requirement,
            start_date: value.start_date,
            end_date: value.end_date,
            ruleset: value.ruleset,
        };
        reference.validate().map_err(|_| invalid())?;
        Ok(reference)
    }
}

impl TiingoCompletedEodHistoryReference {
    /// Closed structural schema for the exact inert V1 wire below. Consumers also use this
    /// type's strict Deserialize validation; schema acceptance never grants source authority.
    pub(crate) fn json_schema() -> serde_json::Value {
        use serde_json::{Value, json};
        fn object<const N: usize>(fields: [(&str, Value); N]) -> Value {
            let required: Vec<_> = fields.iter().map(|(name, _)| *name).collect();
            let properties: serde_json::Map<_, _> = fields
                .iter()
                .map(|(name, value)| ((*name).to_owned(), value.clone()))
                .collect();
            json!({"type":"object", "additionalProperties":false,
                "required":required, "properties":properties})
        }
        fn digest_bytes() -> Value {
            let items = json!({"type":"integer", "minimum":0, "maximum":255});
            let zero = [0_u8; 32];
            json!({"type":"array", "minItems":32, "maxItems":32, "items":items,
                "not":{"type":"array", "items":items, "const":zero}})
        }
        fn text(maximum: usize) -> Value {
            json!({"type":"string", "minLength":1, "maxLength":maximum})
        }
        fn date() -> Value {
            object([
                (
                    "year",
                    json!({"type":"integer", "minimum":1, "maximum":u16::MAX}),
                ),
                (
                    "month",
                    json!({"type":"integer", "minimum":1, "maximum":12}),
                ),
                ("day", json!({"type":"integer", "minimum":1, "maximum":31})),
            ])
        }
        object([
            ("version", json!({"type":"integer", "const":1})),
            ("selected_dataset", text(256)),
            (
                "selected_version",
                json!({"type":"integer", "minimum":1, "maximum":u64::MAX}),
            ),
            ("selected_schema_name", text(128)),
            (
                "selected_schema_version",
                json!({"type":"integer", "minimum":1, "maximum":u16::MAX}),
            ),
            ("selected_schema_fingerprint", digest_bytes()),
            ("selected_content_digest", digest_bytes()),
            ("publication_digest", digest_bytes()),
            (
                "binding_digest",
                object([
                    ("algorithm", json!({"type":"string", "const":"sha256"})),
                    ("bytes", digest_bytes()),
                ]),
            ),
            (
                "knowledge_cutoff",
                json!({"type":"integer", "minimum":i64::MIN, "maximum":i64::MAX}),
            ),
            ("read_digest", digest_bytes()),
            (
                "instrument_id",
                json!({"type":"string", "format":"uuid", "minLength":36, "maxLength":36,
                "not":{"type":"string", "const":"00000000-0000-0000-0000-000000000000"}}),
            ),
            (
                "provider_instrument_id",
                text(ProviderInstrumentId::MAX_LENGTH),
            ),
            ("venue_id", text(VenueId::MAX_LENGTH)),
            ("feed", text(SourceIdentifier::MAX_LENGTH)),
            (
                "interval",
                json!({"type":"string", "const":"tiingo-calendar-day"}),
            ),
            ("adjustment", json!({"type":"string", "enum":["raw","all"]})),
            (
                "surface_requirement",
                json!({"type":"string", "enum":["selected_only","raw_with_all"]}),
            ),
            ("start_date", date()),
            ("end_date", date()),
            (
                "ruleset",
                json!({"type":"string", "const":"tiingo-eod-native-nominal-date-v1"}),
            ),
        ])
    }
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.selected_manifest
    }
    pub(crate) const fn publication_digest(&self) -> market_squawk_data::Sha256Digest {
        self.publication_digest
    }
    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    pub(crate) const fn read_digest(&self) -> market_squawk_data::Sha256Digest {
        self.read_digest
    }
    /// Bounded inert locator bytes. Actual source authority is reconstructed only by raw replay.
    pub(crate) fn canonical_bytes(&self) -> Result<Box<[u8]>, ResearchServiceError> {
        self.validate()?;
        let bytes =
            serde_json::to_vec(self).map_err(|_| ResearchServiceError::IngestAuthorityMismatch)?;
        if bytes.len() > 64 * 1024 {
            return Err(ResearchServiceError::IngestAuthorityMismatch);
        }
        Ok(bytes.into_boxed_slice())
    }
    fn decode_canonical_bytes(bytes: &[u8]) -> Result<Self, ResearchServiceError> {
        let invalid = || ResearchServiceError::IngestAuthorityMismatch;
        if bytes.is_empty() || bytes.len() > 64 * 1024 {
            return Err(invalid());
        }
        let reference: Self = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        if reference.canonical_bytes()?.as_ref() != bytes {
            return Err(invalid());
        }
        Ok(reference)
    }
    fn validate(&self) -> Result<(), ResearchServiceError> {
        if self.version != 1
            || self.start_date > self.end_date
            || self.publication_digest.bytes() == [0; 32]
            || self.binding_digest.algorithm() != market_squawk_domain::DigestAlgorithm::Sha256
            || self.binding_digest.bytes() == [0; 32]
            || self.read_digest.bytes() == [0; 32]
            || (self.surface_requirement
                == market_squawk_data::MarketHistoryPriceSurfaceRequirement::RawWithAll
                && self.adjustment != MarketBarAdjustment::Raw)
            || self.interval.as_str() != "tiingo-calendar-day"
            || self.ruleset.as_str() != "tiingo-eod-native-nominal-date-v1"
            || !matches!(
                self.adjustment,
                MarketBarAdjustment::Raw | MarketBarAdjustment::All
            )
        {
            return Err(ResearchServiceError::IngestAuthorityMismatch);
        }
        Ok(())
    }
}

impl TiingoCompletedEodActionRead {
    pub(crate) const fn reference(&self) -> &TiingoCompletedEodHistoryReference {
        &self.reference
    }
}

impl ResearchService {
    /// Reopens the exact prior selection, cutoff and original native publication without a latest
    /// fallback. Source identity/contract values are checked against the retained graph in replay.
    pub(crate) async fn read_tiingo_eod_history_action_reference(
        &self,
        reference: &TiingoCompletedEodHistoryReference,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCompletedEodActionRead, ResearchServiceError> {
        let history = self
            .read_tiingo_eod_history_reference(reference, deadline, cancellation)
            .await?;
        self.rejoin_tiingo_eod_history_actions(history, deadline, cancellation)
            .await
    }

    /// Exact owning canonical read. Native action authority is completed only by the same
    /// controlled rejoin after any independently authenticated original calendar is attached.
    pub(crate) async fn read_tiingo_eod_history_reference(
        &self,
        reference: &TiingoCompletedEodHistoryReference,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompleteMarketBarHistoryOutput, ResearchServiceError> {
        let invalid = || ResearchServiceError::IngestAuthorityMismatch;
        reference.validate()?;
        let request = CompleteMarketBarHistoryRequest::try_exact_nominal(
            reference.instrument_id,
            reference.start_date,
            reference.end_date,
            reference.provider_instrument_id.clone(),
            reference.venue_id.clone(),
            reference.feed.clone(),
            reference.interval.clone(),
            reference.adjustment,
            reference.ruleset.clone(),
            reference.knowledge_cutoff,
            reference.selected_manifest.clone(),
        )
        .and_then(|request| request.try_with_surface_requirement(reference.surface_requirement))
        .map_err(|_| invalid())?;
        let history = self
            .analytical_reader()
            .read_complete_market_bar_history(request, deadline, cancellation.clone())
            .await
            .map_err(map_history_read_error)?
            .ok_or_else(invalid)?;
        if history.selection().receipt().receipt_digest() != reference.publication_digest
            || history.selection().receipt().binding_digest().bytes()
                != reference.binding_digest.bytes()
            || history.read_receipt().source_result_digest() != reference.read_digest
        {
            return Err(invalid());
        }
        Ok(history)
    }

    /// Decodes only an inert source locator, then performs exact canonical and physical replay.
    pub(crate) async fn read_tiingo_eod_history_action_reference_bytes(
        &self,
        bytes: &[u8],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCompletedEodActionRead, ResearchServiceError> {
        let reference = TiingoCompletedEodHistoryReference::decode_canonical_bytes(bytes)?;
        self.read_tiingo_eod_history_action_reference(&reference, deadline, cancellation)
            .await
    }

    /// Preserves owning exact recovery without cloning an already shared history.
    pub(crate) async fn read_tiingo_eod_history_reference_bytes(
        &self,
        bytes: &[u8],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompleteMarketBarHistoryOutput, ResearchServiceError> {
        let reference = TiingoCompletedEodHistoryReference::decode_canonical_bytes(bytes)?;
        self.read_tiingo_eod_history_reference(&reference, deadline, cancellation)
            .await
    }

    /// Attaches the genuine original named-calendar mapping before the action read owns the
    /// canonical history through Arc. The original source-result digest remains unchanged.
    pub(crate) async fn read_tiingo_eod_history_action_reference_with_calendar(
        &self,
        reference: &TiingoCompletedEodHistoryReference,
        calendar: &crate::application::market_calendar::CompletedMarketSessionRead,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCompletedEodActionRead, ResearchServiceError> {
        self.read_tiingo_eod_history_action_reference_with_calendar_with_job_context(
            reference,
            calendar,
            deadline,
            cancellation,
            None,
        )
        .await
    }

    pub(crate) async fn read_tiingo_eod_history_action_reference_with_calendar_with_job_context(
        &self,
        reference: &TiingoCompletedEodHistoryReference,
        calendar: &crate::application::market_calendar::CompletedMarketSessionRead,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<TiingoCompletedEodActionRead, ResearchServiceError> {
        let history = self
            .read_tiingo_eod_history_reference(reference, deadline, cancellation)
            .await?;
        let history = self
            .rejoin_market_history_native_sessions_with_calendar_with_job_context(
                history,
                calendar,
                deadline,
                cancellation,
                job,
            )
            .await?;
        self.rejoin_tiingo_eod_history_actions_with_job_context(
            history,
            deadline,
            cancellation,
            job,
        )
        .await
    }

    pub(crate) async fn read_tiingo_eod_history_action_reference_bytes_with_calendar(
        &self,
        bytes: &[u8],
        calendar: &crate::application::market_calendar::CompletedMarketSessionRead,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCompletedEodActionRead, ResearchServiceError> {
        let reference = TiingoCompletedEodHistoryReference::decode_canonical_bytes(bytes)?;
        self.read_tiingo_eod_history_action_reference_with_calendar(
            &reference,
            calendar,
            deadline,
            cancellation,
        )
        .await
    }

    /// Joins only an existing sealed canonical output to its exact physically replayed source
    /// pages. Deserialized summaries, native payloads and action projections cannot mint this read.
    pub(crate) async fn rejoin_tiingo_eod_history_actions(
        &self,
        history: CompleteMarketBarHistoryOutput,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCompletedEodActionRead, ResearchServiceError> {
        self.rejoin_tiingo_eod_history_actions_with_job_context(
            history,
            deadline,
            cancellation,
            None,
        )
        .await
    }

    pub(crate) async fn rejoin_tiingo_eod_history_actions_with_job_context(
        &self,
        history: CompleteMarketBarHistoryOutput,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<TiingoCompletedEodActionRead, ResearchServiceError> {
        let manifest = history.selection().receipt().origin_manifest().clone();
        self.read_provider_capture_generation_with_job_context(
            job,
            manifest,
            deadline,
            cancellation,
            move |owned, store, control, analytical, _| {
                let source = std::sync::Arc::new(
                    analytical.rejoin_tiingo_eod_action_history(history, &owned, store, control)?,
                );
                let history = source.history();
                let receipt = history.selection().receipt();
                let invalid = || ResearchServiceError::IngestAuthorityMismatch;
                let graph = receipt.date_windows().ok_or_else(invalid)?;
                let (start, end) = graph.requested_dates();
                let cutoff = source.knowledge_cutoff();
                let first_date = graph
                    .sessions()
                    .first()
                    .and_then(|session| session.time.nominal_daily_date())
                    .ok_or_else(invalid)?;
                let reference = TiingoCompletedEodHistoryReference {
                    version: 1,
                    selected_manifest: history.selection().pinned().manifest().clone(),
                    publication_digest: receipt.receipt_digest(),
                    binding_digest: source.binding().binding_digest(),
                    knowledge_cutoff: cutoff,
                    read_digest: history.read_receipt().source_result_digest(),
                    instrument_id: receipt.instrument_id(),
                    provider_instrument_id: receipt.provider_instrument_id().clone(),
                    venue_id: receipt.venue_id().clone(),
                    feed: receipt.feed().clone(),
                    interval: receipt.interval().clone(),
                    adjustment: receipt.adjustment(),
                    surface_requirement: history.selection().surface_requirement(),
                    start_date: start,
                    end_date: end,
                    ruleset: first_date.ruleset().clone(),
                };
                reference.validate()?;
                Ok(TiingoCompletedEodActionRead { reference, source })
            },
        )
        .await
    }
}

fn map_history_read_error(error: market_squawk_data::AnalyticalReadError) -> ResearchServiceError {
    use market_squawk_data::{AnalyticalReadError as E, DatasetBuildError, IngestError, QueryError as Q};
    match error {
        E::NativeSessionControl(control) => {
            market_squawk_platform::SealedResearchJournalStoreError::ObjectControl(control).into()
        }
        E::Manifest(error) => error.into(),
        E::Parquet(error) => IngestError::Parquet(error).into(),
        E::PythonDataset(error) => DatasetBuildError::PythonDataset(error).into(),
        E::InvalidLimit | E::InstrumentLimitExceeded | E::InvalidMarketBarLimit
        | E::MarketBarResultRequiresInline | E::InputEpochResultRequiresInline => {
            DatasetBuildError::LimitExceeded.into()
        }
        E::Query(error) => match error {
            Q::Cancelled => IngestError::Cancelled.into(),
            Q::DeadlineExceeded => IngestError::DeadlineExceeded.into(),
            Q::Artifact(error) => IngestError::Parquet(error).into(),
            Q::Catalog(error) => error.into(),
            Q::ArrowConversion(error) => IngestError::Arrow(error).into(),
            // ResearchServiceError has no query carrier. Its existing dataset-bound variant
            // preserves resource classification without inventing a Parquet/source failure.
            Q::InvalidLimits | Q::AstLimitExceeded | Q::PlanLimitExceeded | Q::PartitionLimitExceeded
            | Q::RowLimitExceeded { .. } | Q::ByteLimitExceeded { .. } | Q::MemoryLimitExceeded { .. }
            | Q::SizeOverflow | Q::DependencyAllocationContract | Q::BlockingTaskLimitExceeded
            | Q::ReaderMemoryBoundExceeded | Q::ArtifactStoreRequired | Q::ArtifactAuthorityRequired => {
                DatasetBuildError::LimitExceeded.into()
            }
            _ => ResearchServiceError::IngestAuthorityMismatch,
        },
        _ => ResearchServiceError::IngestAuthorityMismatch,
    }
}


impl ProductionResearchIngestCoordinator {
    /// Reuses only a complete exact-source/date publication after reopening its original native
    /// fields and calendar. No source checkpoint is reset and no seal is made from a digest.
    #[allow(clippy::too_many_arguments, reason = "independent identity, source, dates and calendar bounds")]
    pub(crate) async fn reuse_complete_tiingo_eod_history(
        &self,
        record: &market_squawk_data::MarketDataInstrumentRecord,
        venue: &VenueId,
        dates: (CalendarDate, CalendarDate),
        source_id: &SourceId,
        required_cash_assertion: Option<ExactPayloadEvidence>,
        calendars: &crate::application::market_calendar::CompletedMarketSessionReadCapability,
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<TiingoEodHistoryPublicationReceipt>, TiingoHistoryApplicationError> {
        let request = history_request(record, venue, dates, cutoff, None)?;
        let Some(history) = self.research.analytical_reader()
            .read_complete_market_bar_history(request, deadline, cancellation.clone()).await?
        else { return Ok(None); };
        if history.selection().receipt().source_id() != source_id {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        let history = attach_original_calendar(&self.research, history, calendars, cutoff, deadline, cancellation).await?;
        let read = self.research.rejoin_tiingo_eod_history_actions(history, deadline, cancellation).await?;
        if let Some(assertion) = required_cash_assertion {
            let Some(unit) = read.actions().cash_unit() else { return Ok(None); };
            if unit.status() != market_squawk_sources::MarketHistoryCashUnitStatus::ReviewedInference
                || unit.assertion().payload_evidence() != &assertion
                || unit.instrument() != record.definition().instrument_id()
                || unit.currency() != record.definition().quote_currency()
            { return Ok(None); }
        }
        let binding = read.binding();
        let source = read.history().selection().receipt();
        // Reuse may not replace the source definition retained by the original history graph.
        let graph = source.date_windows().ok_or(TiingoHistoryApplicationError::Admission)?;
        if graph.instrument_revision_digest() != record.definition().reference_evidence().payload_evidence().content_digest() {
            return Ok(None);
        }
        Ok(Some(TiingoEodHistoryPublicationReceipt {
            restart: TiingoLatestRestartBinding {
                manifest: read.history().selection().pinned().manifest().clone(),
                binding_digest: binding.binding_digest(),
                source_id: binding.capture().source_id().clone(),
                expected_record_count: binding.record_count(),
                native_schema_version: binding.native_lineage().version(),
                native_schema_fingerprint: binding.native_lineage().fingerprint(),
            },
        }))
    }

    /// Exact immutable raw publication read with its original calendar attached. The returned
    /// data-owned output is consumed directly by source action preparation, never reconstructed.
    #[allow(clippy::too_many_arguments, reason = "exact publication and independent source coordinates")]
    pub(crate) async fn read_complete_tiingo_eod_publication(
        &self,
        publication: &TiingoEodHistoryPublicationReceipt,
        record: &market_squawk_data::MarketDataInstrumentRecord,
        venue: &VenueId,
        dates: (CalendarDate, CalendarDate),
        calendars: &crate::application::market_calendar::CompletedMarketSessionReadCapability,
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompleteMarketBarHistoryOutput, TiingoHistoryApplicationError> {
        let request = history_request(record, venue, dates, cutoff, Some(publication.manifest().clone()))?;
        let history = self.research.analytical_reader()
            .read_complete_market_bar_history(request, deadline, cancellation.clone()).await?
            .ok_or(TiingoHistoryApplicationError::Admission)?;
        if history.selection().receipt().binding_digest().bytes() != publication.binding_digest().bytes()
            || history.selection().receipt().source_id() != &publication.restart.source_id
        { return Err(TiingoHistoryApplicationError::Admission); }
        let graph = history.selection().receipt().date_windows().ok_or(TiingoHistoryApplicationError::Admission)?;
        if graph.instrument_revision_digest() != record.definition().reference_evidence().payload_evidence().content_digest() {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        attach_original_calendar(&self.research, history, calendars, cutoff, deadline, cancellation).await
    }
}

fn history_request(
    record: &market_squawk_data::MarketDataInstrumentRecord,
    venue: &VenueId,
    dates: (CalendarDate, CalendarDate),
    cutoff: Timestamp,
    exact: Option<DatasetManifestRef>,
) -> Result<CompleteMarketBarHistoryRequest, TiingoHistoryApplicationError> {
    let mut mappings = record.definition().venue_mappings().iter().filter(|mapping| mapping.venue_id() == venue);
    let mapping = mappings.next().ok_or(TiingoHistoryApplicationError::Admission)?;
    if mappings.next().is_some() { return Err(TiingoHistoryApplicationError::Admission); }
    let provider = ProviderInstrumentId::try_from(mapping.venue_symbol().as_str()).map_err(|_| TiingoHistoryApplicationError::Admission)?;
    let identifier = |value| SourceIdentifier::try_from(value).map_err(|_| TiingoHistoryApplicationError::Admission);
    let request = if let Some(manifest) = exact {
        CompleteMarketBarHistoryRequest::try_exact_nominal(
            record.definition().instrument_id(), dates.0, dates.1, provider, venue.clone(),
            identifier("tiingo-starter-daily-eod-raw")?, identifier("tiingo-calendar-day")?,
            MarketBarAdjustment::Raw, identifier("tiingo-eod-native-nominal-date-v1")?, cutoff, manifest,
        )
    } else {
        CompleteMarketBarHistoryRequest::try_latest_nominal(
            record.definition().instrument_id(), dates.0, dates.1, provider, venue.clone(),
            identifier("tiingo-starter-daily-eod-raw")?, identifier("tiingo-calendar-day")?,
            MarketBarAdjustment::Raw, identifier("tiingo-eod-native-nominal-date-v1")?, cutoff,
        )
    }.map_err(|_| TiingoHistoryApplicationError::Admission)?;
    Ok(request)
}

async fn attach_original_calendar(
    research: &ResearchService,
    history: CompleteMarketBarHistoryOutput,
    calendars: &crate::application::market_calendar::CompletedMarketSessionReadCapability,
    cutoff: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CompleteMarketBarHistoryOutput, TiingoHistoryApplicationError> {
    use crate::application::market_calendar::CompletedMarketSessionReference;
    let retained = history.selection().receipt().date_windows()
        .ok_or(TiingoHistoryApplicationError::Admission)?.calendar();
    let reference = CompletedMarketSessionReference::try_from_retained_digests(
        retained.origin_content_digest, retained.capture_binding_digest,
    )?;
    let calendar = calendars.read_reference(&reference, cutoff, deadline, cancellation.clone()).await?
        .ok_or(TiingoHistoryApplicationError::Admission)?;
    Ok(research.rejoin_market_history_native_sessions_with_calendar(history, &calendar, deadline, cancellation).await?)
}
