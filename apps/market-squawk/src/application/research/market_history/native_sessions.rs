//! Controlled original-calendar replay for both guided selection and exact recipe reopening.

use crate::{ResearchService, ResearchServiceError};
use chrono::Datelike as _;
use market_squawk_adapter_alpaca::{
    AlpacaAuthenticatedCalendarRequest, AlpacaRetainedCalendarSessions, AlpacaTradingApiEnvironment,
};
use market_squawk_data::{
    AnalyticalReadError, CompleteMarketBarHistoryCursor, CompleteMarketBarHistoryOutput,
    CompleteMarketBarHistoryReadReceipt, CompleteMarketBarHistorySelection,
    RetainedCorporateActionCalendar, RetainedHistoryNativeSessions,
};
use market_squawk_domain::{CalendarDate, DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_sources::SealedProviderCaptureSetReceipt;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Existing sealed data reads share the same native-source validators; no caller rows enter here.
pub(crate) trait NativeSessionHistory: Send + Sync + 'static + Sized {
    fn selection(&self) -> &CompleteMarketBarHistorySelection;
    fn read_receipt(&self) -> &CompleteMarketBarHistoryReadReceipt;
    fn native_sessions(&self) -> Option<&RetainedHistoryNativeSessions>;
    fn into_native_cursor(
        self,
        service: &market_squawk_data::AnalyticalDataService,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CompleteMarketBarHistoryCursor, AnalyticalReadError>;
    fn bar_count(&self) -> usize;
    fn bars(
        &self,
    ) -> Box<
        dyn Iterator<Item = Result<market_squawk_domain::MarketBarObservation, AnalyticalReadError>>
            + '_,
    >;
    fn source_actions(
        &self,
    ) -> Box<
        dyn Iterator<
                Item = Result<
                    market_squawk_domain::CorporateActionObservation,
                    AnalyticalReadError,
                >,
            > + '_,
    >;
    fn ordinary_evidence(
        &self,
    ) -> Result<
        market_squawk_data::CompletedOrdinaryHistoryEvidence,
        market_squawk_data::CorporateActionError,
    >;

    fn try_with_native_sessions(
        self,
        replay: AlpacaRetainedCalendarSessions,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError>;
    fn try_with_nominal_native_sessions(
        self,
        calendar: &RetainedCorporateActionCalendar,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError>;
}
impl NativeSessionHistory for CompleteMarketBarHistoryOutput {
    fn selection(&self) -> &CompleteMarketBarHistorySelection {
        CompleteMarketBarHistoryOutput::selection(self)
    }
    fn read_receipt(&self) -> &CompleteMarketBarHistoryReadReceipt {
        CompleteMarketBarHistoryOutput::read_receipt(self)
    }
    fn native_sessions(&self) -> Option<&RetainedHistoryNativeSessions> {
        CompleteMarketBarHistoryOutput::native_sessions(self)
    }
    fn bar_count(&self) -> usize {
        self.bars().len()
    }
    fn bars(
        &self,
    ) -> Box<
        dyn Iterator<Item = Result<market_squawk_domain::MarketBarObservation, AnalyticalReadError>>
            + '_,
    > {
        Box::new(self.bars().iter().cloned().map(Ok))
    }
    fn source_actions(
        &self,
    ) -> Box<
        dyn Iterator<
                Item = Result<
                    market_squawk_domain::CorporateActionObservation,
                    AnalyticalReadError,
                >,
            > + '_,
    > {
        Box::new(self.source_actions().iter().cloned().map(Ok))
    }
    fn ordinary_evidence(
        &self,
    ) -> Result<
        market_squawk_data::CompletedOrdinaryHistoryEvidence,
        market_squawk_data::CorporateActionError,
    > {
        market_squawk_data::CompletedOrdinaryHistoryEvidence::try_from_history(self)
    }
    fn into_native_cursor(
        self,
        service: &market_squawk_data::AnalyticalDataService,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<CompleteMarketBarHistoryCursor, AnalyticalReadError> {
        self.into_cursor(
            std::sync::Arc::new(service.operation_scratch()?),
            deadline,
            cancellation,
        )
    }
    fn try_with_native_sessions(
        self,
        replay: AlpacaRetainedCalendarSessions,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError> {
        CompleteMarketBarHistoryOutput::try_with_native_sessions(self, replay, control)
    }
    fn try_with_nominal_native_sessions(
        self,
        calendar: &RetainedCorporateActionCalendar,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError> {
        CompleteMarketBarHistoryOutput::try_with_nominal_native_sessions(self, calendar, control)
    }
}
impl NativeSessionHistory for CompleteMarketBarHistoryCursor {
    fn selection(&self) -> &CompleteMarketBarHistorySelection {
        CompleteMarketBarHistoryCursor::selection(self)
    }
    fn read_receipt(&self) -> &CompleteMarketBarHistoryReadReceipt {
        CompleteMarketBarHistoryCursor::read_receipt(self)
    }
    fn native_sessions(&self) -> Option<&RetainedHistoryNativeSessions> {
        CompleteMarketBarHistoryCursor::native_sessions(self)
    }
    fn bar_count(&self) -> usize {
        self.bar_count()
    }
    fn bars(
        &self,
    ) -> Box<
        dyn Iterator<Item = Result<market_squawk_domain::MarketBarObservation, AnalyticalReadError>>
            + '_,
    > {
        Box::new(self.bars())
    }
    fn source_actions(
        &self,
    ) -> Box<
        dyn Iterator<
                Item = Result<
                    market_squawk_domain::CorporateActionObservation,
                    AnalyticalReadError,
                >,
            > + '_,
    > {
        Box::new(self.source_actions())
    }
    fn ordinary_evidence(
        &self,
    ) -> Result<
        market_squawk_data::CompletedOrdinaryHistoryEvidence,
        market_squawk_data::CorporateActionError,
    > {
        market_squawk_data::CompletedOrdinaryHistoryEvidence::try_from_cursor(self)
    }
    fn into_native_cursor(
        self,
        _service: &market_squawk_data::AnalyticalDataService,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> Result<CompleteMarketBarHistoryCursor, AnalyticalReadError> {
        Ok(self)
    }
    fn try_with_native_sessions(
        self,
        replay: AlpacaRetainedCalendarSessions,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError> {
        CompleteMarketBarHistoryCursor::try_with_native_sessions(self, replay, control)
    }
    fn try_with_nominal_native_sessions(
        self,
        calendar: &RetainedCorporateActionCalendar,
        control: &dyn market_squawk_platform::ResearchObjectControl,
    ) -> Result<Self, AnalyticalReadError> {
        CompleteMarketBarHistoryCursor::try_with_nominal_native_sessions(self, calendar, control)
    }
}

/// Borrowed immutable coordinates from one of the two privately constructed calendar reads.
struct NativeSessionCalendar<'a> {
    reference: &'a crate::application::market_calendar::CompletedMarketSessionReference,
    calendar_id: &'a market_squawk_domain::SourceIdentifier,
    calendar_revision: &'a market_squawk_domain::RevisionBoundPayloadEvidence,
    available_at: Timestamp,
    venue_id: &'a market_squawk_domain::VenueId,
    action_calendar: &'a std::sync::Arc<market_squawk_data::RetainedCorporateActionCalendar>,
}

impl ResearchService {
    /// Associates source-native dates only with the exact already-reopened calendar retained by
    /// the original price publication. This supplies named-session simulated clocks, never a
    /// provider observation timestamp or a historical ticker-continuity assertion.
    pub(crate) async fn rejoin_market_history_native_sessions_with_calendar<
        H: NativeSessionHistory,
    >(
        &self,
        output: H,
        calendar: &crate::application::market_calendar::CompletedMarketSessionRead,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<H, ResearchServiceError> {
        self.rejoin_market_history_native_sessions_with_calendar_with_job_context(
            output,
            calendar,
            deadline,
            cancellation,
            None,
        )
        .await
    }

    pub(crate) async fn rejoin_market_history_native_sessions_with_calendar_for_job<
        H: NativeSessionHistory,
    >(
        &self,
        output: H,
        calendar: &crate::application::market_calendar::CompletedMarketSessionRead,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: &market_squawk_jobs::JobRunContext,
    ) -> Result<H, ResearchServiceError> {
        self.rejoin_market_history_native_sessions_with_calendar_with_job_context(
            output,
            calendar,
            deadline,
            cancellation,
            Some(job),
        )
        .await
    }

    pub(crate) async fn rejoin_market_history_native_sessions_with_calendar_with_job_context<
        H: NativeSessionHistory,
    >(
        &self,
        output: H,
        calendar: &crate::application::market_calendar::CompletedMarketSessionRead,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<H, ResearchServiceError> {
        let calendar = NativeSessionCalendar {
            reference: calendar.reference(),
            calendar_id: calendar.calendar_id(),
            calendar_revision: calendar.calendar_revision(),
            available_at: calendar.available_at(),
            venue_id: calendar.venue_id(),
            action_calendar: calendar.source_action_calendar(),
        };
        self.rejoin_market_history_native_sessions_with_original_calendar(
            output,
            calendar,
            deadline,
            cancellation,
            job,
        )
        .await
    }

    /// Original historical replay only; this accepts no live authority or publication guard.
    pub(crate) async fn rejoin_market_history_native_sessions_with_retained_calendar_with_job_context<
        H: NativeSessionHistory,
    >(
        &self,
        output: H,
        calendar: &crate::application::market_calendar::RetainedMarketSessionRead,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<H, ResearchServiceError> {
        let calendar = NativeSessionCalendar {
            reference: calendar.reference(),
            calendar_id: calendar.calendar_id(),
            calendar_revision: calendar.calendar_revision(),
            available_at: calendar.available_at(),
            venue_id: calendar.venue_id(),
            action_calendar: calendar.source_action_calendar(),
        };
        self.rejoin_market_history_native_sessions_with_original_calendar(
            output,
            calendar,
            deadline,
            cancellation,
            job,
        )
        .await
    }

    async fn rejoin_market_history_native_sessions_with_original_calendar<
        H: NativeSessionHistory,
    >(
        &self,
        output: H,
        calendar: NativeSessionCalendar<'_>,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<H, ResearchServiceError> {
        let invalid = || ResearchServiceError::IngestAuthorityMismatch;
        let receipt = output.selection().receipt();
        if receipt.requested_range().is_some() {
            return self
                .rejoin_market_history_native_sessions_with_job_context(
                    output,
                    deadline,
                    cancellation,
                    job,
                )
                .await;
        }
        let graph = receipt.date_windows().ok_or_else(invalid)?;
        let original = graph.calendar();
        if calendar.reference.origin_content_digest() != original.origin_content_digest
            || calendar.reference.capture_binding_digest() != original.capture_binding_digest
            || calendar.calendar_id != &original.calendar_id
            || calendar.calendar_revision != &original.calendar_revision
            || calendar.available_at != original.calendar_available_at
            || !original.relationship.matches(
                calendar.venue_id,
                receipt.venue_id(),
                graph.requested_dates(),
            )
            || calendar.action_calendar.knowledge_cutoff()
                != output.read_receipt().knowledge_cutoff()
        {
            return Err(invalid());
        }
        if let Some(attached) = output.native_sessions() {
            if attached.source_replay_digest() != calendar.action_calendar.evidence_digest() {
                return Err(invalid());
            }
            return Ok(output);
        }
        let manifest = self
            .analytical_reader()
            .provider_capture_origin(
                original.capture_binding_digest,
                market_squawk_data::Sha256Digest::new(original.origin_content_digest.bytes()),
                output.read_receipt().knowledge_cutoff(),
                deadline,
                cancellation,
            )
            .map_err(map_native_history_error)?
            .ok_or_else(invalid)?;
        let original_calendar = std::sync::Arc::clone(calendar.action_calendar);
        self.read_provider_capture_generation_with_job_context(
            job,
            manifest,
            deadline,
            cancellation,
            move |owned, _, control, _, _| {
                if owned.pinned().manifest() != original_calendar.manifest()
                    || owned.published_at() > output.read_receipt().knowledge_cutoff()
                {
                    return Err(ResearchServiceError::IngestAuthorityMismatch);
                }
                output
                    .try_with_nominal_native_sessions(&original_calendar, control)
                    .map_err(map_native_history_error)
            },
        )
        .await
    }

    /// Rejoins native sessions from the exact creating generation through the existing bounded
    /// raw worker. Unsupported source associations remain explicitly unattached; consumers that
    /// require native execution sessions must reject that state.
    pub(crate) async fn rejoin_market_history_native_sessions<H: NativeSessionHistory>(
        &self,
        output: H,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<H, ResearchServiceError> {
        self.rejoin_market_history_native_sessions_with_job_context(
            output,
            deadline,
            cancellation,
            None,
        )
        .await
    }

    pub(crate) async fn rejoin_market_history_native_sessions_for_job<H: NativeSessionHistory>(
        &self,
        output: H,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: &market_squawk_jobs::JobRunContext,
    ) -> Result<H, ResearchServiceError> {
        self.rejoin_market_history_native_sessions_with_job_context(
            output,
            deadline,
            cancellation,
            Some(job),
        )
        .await
    }

    pub(crate) async fn rejoin_market_history_native_sessions_with_job_context<
        H: NativeSessionHistory,
    >(
        &self,
        output: H,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<H, ResearchServiceError> {
        if output.native_sessions().is_some() {
            return Ok(output);
        }
        if output.selection().receipt().source_id().as_str() != "alpaca-basic-iex-market-data" {
            return Ok(output);
        }
        let manifest = output.selection().receipt().origin_manifest().clone();
        self.read_provider_capture_generation_with_job_context(
            job,
            manifest,
            deadline,
            cancellation,
            move |owned, store, control, _, _| {
                let invalid = |stage: &'static str| {
                    tracing::warn!(stage, "retained history native-session rejoin rejected");
                    ResearchServiceError::IngestAuthorityMismatch
                };
                let receipt = output.selection().receipt();
                if owned.pinned().manifest() != receipt.origin_manifest()
                    || owned.source_id() != receipt.source_id()
                    || owned.origin_created_at() != receipt.published_at()
                    || owned.published_at() < receipt.published_at()
                    || owned.published_at() > output.read_receipt().knowledge_cutoff()
                {
                    tracing::warn!(
                        stage = "origin",
                        manifest_mismatch = owned.pinned().manifest() != receipt.origin_manifest(),
                        source_mismatch = owned.source_id() != receipt.source_id(),
                        publication_clock_mismatch =
                            owned.origin_created_at() != receipt.published_at(),
                        availability_before_publication =
                            owned.published_at() < receipt.published_at(),
                        availability_after_cutoff =
                            owned.published_at() > output.read_receipt().knowledge_cutoff(),
                        "retained history native-session coordinates differ"
                    );
                    return Err(ResearchServiceError::IngestAuthorityMismatch);
                }
                let object = owned
                    .objects()
                    .iter()
                    .find(|object| {
                        object.generation_object_ordinal()
                            == usize::from(receipt.origin_object_ordinal())
                    })
                    .ok_or_else(|| invalid("object_missing"))?;
                if object.object().artifact_id() != receipt.origin_artifact_id()
                    || object.inputs().len() != 1
                {
                    tracing::warn!(
                        stage = "object",
                        artifact_mismatch =
                            object.object().artifact_id() != receipt.origin_artifact_id(),
                        input_count_mismatch = object.inputs().len() != 1,
                        "retained history native-session coordinates differ"
                    );
                    return Err(invalid("object"));
                }
                let binding = object.inputs()[0].binding();
                let (component_ordinal, digest, page_count) = receipt
                    .session_calendar_component()
                    .ok_or_else(|| invalid("calendar_coordinate_missing"))?;
                let component = binding
                    .capture()
                    .request_graph_components()
                    .get(usize::from(component_ordinal))
                    .ok_or_else(|| invalid("calendar_component_missing"))?;
                if binding.binding_digest().bytes() != receipt.binding_digest().bytes()
                    || binding.sealed_capture_receipt_digest().bytes()
                        != receipt.capture_receipt_digest().bytes()
                    || binding.capture().content_digest().bytes()
                        != receipt.capture_graph_digests().0.bytes()
                    || binding.capture().observation_digest().bytes()
                        != receipt.capture_graph_digests().1.bytes()
                    || component.content_digest().bytes() != digest.bytes()
                    || component.page_count().get() != page_count
                    || binding.layout() != "whole_single_segment"
                    || binding.physical_claims().len() != 1
                {
                    tracing::warn!(
                        stage = "graph",
                        binding_mismatch =
                            binding.binding_digest().bytes() != receipt.binding_digest().bytes(),
                        capture_receipt_mismatch = binding.sealed_capture_receipt_digest().bytes()
                            != receipt.capture_receipt_digest().bytes(),
                        content_mismatch = binding.capture().content_digest().bytes()
                            != receipt.capture_graph_digests().0.bytes(),
                        observation_mismatch = binding.capture().observation_digest().bytes()
                            != receipt.capture_graph_digests().1.bytes(),
                        component_mismatch = component.content_digest().bytes() != digest.bytes(),
                        page_count_mismatch = component.page_count().get() != page_count,
                        layout_mismatch = binding.layout() != "whole_single_segment",
                        claim_count_mismatch = binding.physical_claims().len() != 1,
                        "retained history native-session coordinates differ"
                    );
                    return Err(invalid("graph"));
                }
                let (start, end) = receipt
                    .requested_range()
                    .ok_or_else(|| invalid("requested_range_missing"))?;
                let start_date = utc_date(start).inspect_err(|_| {
                    tracing::warn!(
                        stage = "request_start_date",
                        "retained history native-session rejoin rejected"
                    );
                })?;
                let end_date = utc_date(end).inspect_err(|_| {
                    tracing::warn!(
                        stage = "request_end_date",
                        "retained history native-session rejoin rejected"
                    );
                })?;
                let mut request = None;
                for environment in [
                    AlpacaTradingApiEnvironment::Live,
                    AlpacaTradingApiEnvironment::Paper,
                ] {
                    let candidate = AlpacaAuthenticatedCalendarRequest::try_new(
                        environment,
                        start_date,
                        end_date,
                    )
                    .map_err(|_| invalid("request_construction"))?;
                    if candidate
                        .capture_request_identity()
                        .map_err(|_| invalid("request_identity"))?
                        == component.request_set_identity()
                    {
                        if request.replace(candidate).is_some() {
                            return Err(invalid("request_ambiguous"));
                        }
                    }
                }
                let request = request.ok_or_else(|| invalid("request_unmatched"))?;
                let segment = store
                    .open_verified_claim_with_control(binding.physical_claims()[0].claim(), control)
                    .inspect_err(|error| {
                        tracing::warn!(stage = "raw_reopen", kind = ?std::mem::discriminant(error),
                        "retained history native-session rejoin rejected");
                    })?;
                let sealed = SealedProviderCaptureSetReceipt::try_bind(
                    binding.capture().clone(),
                    segment.receipt().clone(),
                )
                .map_err(|_| invalid("sealed_binding"))?;
                if sealed.receipt_digest()
                    != EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        receipt.capture_receipt_digest().bytes(),
                    )
                {
                    return Err(invalid("sealed_receipt"));
                }
                let replay = AlpacaRetainedCalendarSessions::try_replay(
                    &request, &sealed, &segment, control,
                )
                .map_err(map_calendar_replay_error)?;
                output
                    .try_with_native_sessions(replay, control)
                    .map_err(map_native_history_error)
            },
        )
        .await
        .inspect_err(|error| {
            tracing::warn!(stage = "capture_generation_read", kind = ?std::mem::discriminant(error),
                "retained history native-session rejoin unavailable");
        })
    }
}

fn utc_date(timestamp: Timestamp) -> Result<CalendarDate, ResearchServiceError> {
    let instant = chrono::DateTime::from_timestamp_nanos(timestamp.unix_nanos());
    CalendarDate::new(
        u16::try_from(instant.year()).map_err(|_| ResearchServiceError::IngestAuthorityMismatch)?,
        u8::try_from(instant.month()).map_err(|_| ResearchServiceError::IngestAuthorityMismatch)?,
        u8::try_from(instant.day()).map_err(|_| ResearchServiceError::IngestAuthorityMismatch)?,
    )
    .map_err(|_| ResearchServiceError::IngestAuthorityMismatch)
}

fn map_calendar_replay_error(
    error: market_squawk_adapter_alpaca::AlpacaCalendarDecodeError,
) -> ResearchServiceError {
    tracing::warn!(
        stage = "calendar_replay",
        ?error,
        "retained history native-session rejoin rejected"
    );
    match error {
        market_squawk_adapter_alpaca::AlpacaCalendarDecodeError::Control(control) => {
            market_squawk_platform::SealedResearchJournalStoreError::ObjectControl(control).into()
        }
        _ => ResearchServiceError::IngestAuthorityMismatch,
    }
}
fn map_native_history_error(
    error: market_squawk_data::AnalyticalReadError,
) -> ResearchServiceError {
    tracing::warn!(stage = "native_period_join", kind = ?std::mem::discriminant(&error),
        "retained history native-session rejoin rejected");
    match error {
        market_squawk_data::AnalyticalReadError::NativeSessionControl(control) => {
            market_squawk_platform::SealedResearchJournalStoreError::ObjectControl(control).into()
        }
        _ => ResearchServiceError::IngestAuthorityMismatch,
    }
}
