//! Per-analysis selection and exact reopening of a durably published source calendar.

mod retained;
pub(crate) use retained::{
    RetainedMarketSessionDateReceipt, RetainedMarketSessionRead,
    RetainedMarketSessionReadCapability,
};

mod tiingo;
mod tiingo_calendar_relation;
pub(crate) use tiingo::TiingoCalendarExpectedSessionAuthority;
pub(crate) use tiingo_calendar_relation::tiingo_calendar_source_venue;

use super::alpaca::{
    publish_alpaca_market_calendar_with_job_context,
    read_alpaca_completed_calendar_with_job_context,
};
use super::{CompletedMarketSessionAuthority, CompletedMarketSessionError};
use crate::application::market_runtime::{
    AlpacaHistoricalRuntimeCapability, MarketRuntimeRegistry,
};
use crate::{ResearchService, ResearchServiceError};
use chrono::{DateTime, Datelike as _, Utc};
use chrono_tz::America::New_York;
use market_squawk_adapter_alpaca::{AlpacaAuthenticatedCalendarRequest, AlpacaCalendarMarket};
use market_squawk_data::{
    AnalyticalReadLimit, DatasetId, DatasetManifestRef, ResearchArrowBatch, Sha256Digest,
};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, MarketCalendarCompleteness,
    MarketCalendarObservation, MarketCalendarPayload, ResearchObservation, Timestamp,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const MAXIMUM_ORIGIN_CANDIDATES: usize = 64;
const MAXIMUM_CALENDAR_QUERY_ROWS: u64 = 100_000;
const MAXIMUM_CALENDAR_QUERY_BYTES: u64 = 64 * 1024 * 1024;
// Bound physical decoding and transient projection reconstruction before retaining Arrow rows.
const CALENDAR_DECODE_CHUNK_ROWS: usize = 256;

/// Public references retain only immutable content commitments. They contain no provider choice,
/// request builder, source ID, or physical path and are not publication or account authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "CalendarReferenceWire", into = "CalendarReferenceWire")]
pub struct CompletedMarketSessionReference {
    origin_content_digest: EvidenceDigest,
    capture_binding_digest: EvidenceDigest,
}

impl CompletedMarketSessionReference {
    /// Reconstructs only an inert original locator; an exact controlled reopen is still required.
    pub(crate) fn try_from_retained_digests(
        origin_content_digest: EvidenceDigest,
        capture_binding_digest: EvidenceDigest,
    ) -> Result<Self, CompletedMarketSessionError> {
        if [origin_content_digest, capture_binding_digest]
            .into_iter()
            .any(|digest| {
                digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32]
            })
        {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        Ok(Self {
            origin_content_digest,
            capture_binding_digest,
        })
    }
    pub(crate) const fn origin_content_digest(&self) -> EvidenceDigest {
        self.origin_content_digest
    }
    pub(crate) const fn capture_binding_digest(&self) -> EvidenceDigest {
        self.capture_binding_digest
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CalendarReferenceWire {
    origin_content_digest: String,
    capture_binding_digest: String,
}
impl TryFrom<CalendarReferenceWire> for CompletedMarketSessionReference {
    type Error = &'static str;
    fn try_from(wire: CalendarReferenceWire) -> Result<Self, Self::Error> {
        Ok(Self {
            origin_content_digest: decode_digest(&wire.origin_content_digest)?,
            capture_binding_digest: decode_digest(&wire.capture_binding_digest)?,
        })
    }
}
impl From<CompletedMarketSessionReference> for CalendarReferenceWire {
    fn from(value: CompletedMarketSessionReference) -> Self {
        Self {
            origin_content_digest: encode_digest(value.origin_content_digest),
            capture_binding_digest: encode_digest(value.capture_binding_digest),
        }
    }
}

#[derive(Clone)]
pub(crate) struct CompletedMarketSessionRead {
    reference: CompletedMarketSessionReference,
    authority: Arc<CompletedMarketSessionAuthority>,
    source: Arc<super::alpaca::AlpacaCompletedSessionEvidence>,
    action_calendar: Arc<market_squawk_data::RetainedCorporateActionCalendar>,
}
impl CompletedMarketSessionRead {
    pub(crate) const fn source_action_calendar(
        &self,
    ) -> &Arc<market_squawk_data::RetainedCorporateActionCalendar> {
        &self.action_calendar
    }
    /// Exact source-requested nominal coverage; omitted sessions are proved by complete replay.
    pub(crate) fn requested_dates(&self) -> (CalendarDate, CalendarDate) {
        self.source.native_session_replay().requested_dates()
    }
    /// Original opaque physical replay, shared without cloning the bounded native array.
    /// Creating publication, rights and cutoff checks remain required by the consuming reader.
    pub(crate) fn native_session_replay(
        &self,
    ) -> &Arc<market_squawk_adapter_alpaca::AlpacaRetainedCalendarSessions> {
        self.source.native_session_replay()
    }
    pub(crate) fn calendar_id(&self) -> &market_squawk_domain::SourceIdentifier {
        self.source.calendar_id()
    }
    pub(crate) fn calendar_revision(&self) -> &market_squawk_domain::RevisionBoundPayloadEvidence {
        self.source.calendar_revision()
    }
    pub(crate) fn available_at(&self) -> Timestamp {
        self.source.available_at()
    }

    /// Original retained currentness expiry, not a caller TTL or implied bar period.
    pub(crate) fn currentness_expires_at(&self) -> Timestamp {
        self.source.currentness_expires_at()
    }

    pub(crate) const fn reference(&self) -> &CompletedMarketSessionReference {
        &self.reference
    }
    pub(crate) const fn authority(&self) -> &Arc<CompletedMarketSessionAuthority> {
        &self.authority
    }
    /// Exact native market of the retained calendar; callers must match their resolved listing.
    pub(crate) fn venue_id(&self) -> &market_squawk_domain::VenueId {
        self.source.venue_id()
    }
    /// Returns an authentic regular-session completion and the same original source's
    /// currentness receipt. This does not reinterpret an IEX aggregation endpoint.
    pub(crate) fn latest_completed_regular_session(
        &self,
        knowledge_cutoff: Timestamp,
        completion_cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Result<
        (
            Option<CompletedMarketSessionDateReceipt>,
            super::CompletedMarketSessionCurrentnessReceipt,
        ),
        CompletedMarketSessionError,
    > {
        let (session, currentness) = self.source.latest_completed_regular_session(
            knowledge_cutoff,
            completion_cutoff,
            evaluated_at,
        )?;
        Ok((
            session.map(|session| CompletedMarketSessionDateReceipt {
                reference: self.reference.clone(),
                session,
            }),
            currentness,
        ))
    }

    /// Selects the first source-reported regular opening at or after the exact eligible instant.
    /// This proves calendar order only; liquidity and executable prices require separate evidence.
    pub(crate) fn next_session_starting_at_or_after(
        &self,
        eligible_at: Timestamp,
        knowledge_cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Option<CompletedMarketSessionDateReceipt> {
        self.source
            .next_session_starting_at_or_after(eligible_at, knowledge_cutoff, evaluated_at)
            .map(|session| CompletedMarketSessionDateReceipt {
                reference: self.reference.clone(),
                session,
            })
    }

    /// Resolves the exact native calendar date. Absent dates remain absent; payment/application
    /// conventions belong to the action product and cannot be inferred by calendar rolling.
    pub(crate) fn date_session_on(
        &self,
        date: CalendarDate,
        knowledge_cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Option<CompletedMarketSessionDateReceipt> {
        self.source
            .session_on(date, knowledge_cutoff, evaluated_at)
            .map(|session| CompletedMarketSessionDateReceipt {
                reference: self.reference.clone(),
                session,
            })
    }
}

/// Exact provider-reported regular-session endpoints paired with the immutable calendar reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletedMarketSessionDateReceipt {
    reference: CompletedMarketSessionReference,
    session: super::alpaca::AlpacaCalendarSessionDateReceipt,
}
impl CompletedMarketSessionDateReceipt {
    pub(crate) const fn reference(&self) -> &CompletedMarketSessionReference {
        &self.reference
    }
    pub(crate) fn date(&self) -> CalendarDate {
        self.session.date()
    }
    pub(crate) fn opens_at(&self) -> Timestamp {
        self.session.opens_at()
    }
    pub(crate) fn closes_at_exclusive(&self) -> Timestamp {
        self.session.closes_at_exclusive()
    }
    pub(crate) fn available_at(&self) -> Timestamp {
        self.session.available_at()
    }
    pub(crate) fn evidence_digest(&self) -> EvidenceDigest {
        self.session.calendar_evidence()
    }
    /// Exact admitted Alpaca IEX daily aggregation semantics. Native core endpoints remain
    /// separate; None means the source calendar establishes no provider aggregation interval.
    pub(crate) fn provider_period(&self) -> Option<&market_squawk_domain::BarTimeSemantics> {
        self.session.provider_period()
    }
}

/// Existing store and runtime owners supply all authority. This value has no calendar cache,
/// background task, private registry, or per-holding acquisition path.
#[derive(Clone)]
pub(crate) struct CompletedMarketSessionReadCapability {
    research: Arc<ResearchService>,
    market_runtime: Arc<MarketRuntimeRegistry>,
}
impl fmt::Debug for CompletedMarketSessionReadCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletedMarketSessionReadCapability")
            .finish_non_exhaustive()
    }
}

impl CompletedMarketSessionReadCapability {
    /// Shares immutable original calendar reads without account/runtime currentness authority.
    pub(crate) fn retained_read_capability(&self) -> RetainedMarketSessionReadCapability {
        RetainedMarketSessionReadCapability::new(Arc::clone(&self.research))
    }

    pub(crate) fn new(
        research: Arc<ResearchService>,
        market_runtime: Arc<MarketRuntimeRegistry>,
    ) -> Self {
        Self {
            research,
            market_runtime,
        }
    }

    /// Acquires the existing source policy's minimum current-session request window. Thirty
    /// civil days is a query bound only; it asserts no number or existence of sessions and does
    /// not authorize a longer historical study range.
    pub(crate) async fn preflight_current_session(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<CompletedMarketSessionReference>, CompletedMarketSessionError> {
        self.preflight_current_session_with_job_context(deadline, cancellation, None)
            .await
    }

    pub(crate) async fn preflight_current_session_for_job(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
        job: &market_squawk_jobs::JobRunContext,
    ) -> Result<Option<CompletedMarketSessionReference>, CompletedMarketSessionError> {
        self.preflight_current_session_with_job_context(deadline, cancellation, Some(job))
            .await
    }

    pub(crate) async fn preflight_current_session_with_job_context(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Option<CompletedMarketSessionReference>, CompletedMarketSessionError> {
        use super::MarketCalendarClock as _;
        check(deadline, &cancellation)?;
        let now = super::SystemMarketCalendarClock.now().map_err(invalid)?;
        let end = DateTime::<Utc>::from_timestamp_nanos(now.unix_nanos())
            .with_timezone(&New_York)
            .date_naive();
        let start = end
            .checked_sub_days(chrono::Days::new(u64::from(
                market_squawk_adapter_alpaca::ALPACA_HISTORICAL_MIN_LOOKBACK_DAYS,
            )))
            .ok_or(CompletedMarketSessionError::InvalidRequest)?;
        let calendar_date = |date: chrono::NaiveDate| {
            CalendarDate::new(
                u16::try_from(date.year()).map_err(invalid)?,
                u8::try_from(date.month()).map_err(invalid)?,
                u8::try_from(date.day()).map_err(invalid)?,
            )
            .map_err(invalid)
        };
        self.preflight_with_job_context(
            &market_squawk_domain::VenueId::try_from("iex").map_err(invalid)?,
            calendar_date(start)?,
            calendar_date(end)?,
            deadline,
            cancellation,
            job,
        )
        .await
    }

    /// Performs one shared workflow acquisition before that workflow freezes its cutoff. The
    /// caller supplies its actual bounded history date range, never a fabricated session list.
    pub(crate) async fn preflight(
        &self,
        venue: &market_squawk_domain::VenueId,
        start_date: CalendarDate,
        end_date: CalendarDate,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<CompletedMarketSessionReference>, CompletedMarketSessionError> {
        self.preflight_with_job_context(venue, start_date, end_date, deadline, cancellation, None)
            .await
    }

    pub(crate) async fn preflight_for_job(
        &self,
        venue: &market_squawk_domain::VenueId,
        start_date: CalendarDate,
        end_date: CalendarDate,
        deadline: Instant,
        cancellation: CancellationToken,
        job: &market_squawk_jobs::JobRunContext,
    ) -> Result<Option<CompletedMarketSessionReference>, CompletedMarketSessionError> {
        self.preflight_with_job_context(
            venue,
            start_date,
            end_date,
            deadline,
            cancellation,
            Some(job),
        )
        .await
    }

    pub(crate) async fn preflight_with_job_context(
        &self,
        venue: &market_squawk_domain::VenueId,
        start_date: CalendarDate,
        end_date: CalendarDate,
        deadline: Instant,
        cancellation: CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Option<CompletedMarketSessionReference>, CompletedMarketSessionError> {
        check(deadline, &cancellation)?;
        let market = match venue.as_str() {
            "iex" => AlpacaCalendarMarket::Iex,
            "XNYS" => AlpacaCalendarMarket::Nyse,
            "XNAS" => AlpacaCalendarMarket::Nasdaq,
            _ => return Ok(None),
        };
        let Some(runtime) = self.runtime(deadline, &cancellation).await? else {
            return Ok(None);
        };
        let published = publish_alpaca_market_calendar_with_job_context(
            &self.research,
            &runtime,
            market,
            start_date,
            end_date,
            deadline,
            &cancellation,
            job,
        )
        .await?;
        // Calendar refresh uses the existing display-preparation owner. That worker treats
        // publication wakes as retained-only, so this cannot recursively acquire calendars.
        self.research.history_publications().notify_one();
        Ok(Some(reference(
            published.manifest(),
            published.binding_digest(),
        )))
    }

    /// Selects once for the original cutoff from genuine creating generations, newest first.
    /// Pages are bounded, but an unrelated recent history calendar cannot hide a covering
    /// current calendar. The existing deadline/cancellation bounds the preparation traversal.
    pub(crate) async fn select(
        &self,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<CompletedMarketSessionRead>, CompletedMarketSessionError> {
        check(deadline, &cancellation)?;
        let Some(runtime) = self.runtime(deadline, &cancellation).await? else {
            return Ok(None);
        };
        let dataset = DatasetId::try_from("alpaca-market-calendar-iex").map_err(invalid)?;
        let mut before_version = None;
        loop {
            let (candidates, has_more) = self
                .research
                .analytical_reader()
                .provider_capture_origin_candidates(
                    &dataset,
                    as_of,
                    before_version,
                    AnalyticalReadLimit::try_new(MAXIMUM_ORIGIN_CANDIDATES).map_err(invalid)?,
                    deadline,
                    &cancellation,
                )
                .inspect_err(|error| calendar_read_failure("calendar-origin-candidates", error))
                .map_err(|_| controlled_error(deadline, &cancellation))?;
            if candidates.is_empty() {
                return Ok(None);
            }
            before_version = candidates.last().map(DatasetManifestRef::manifest_version);
            for manifest in candidates {
                check(deadline, &cancellation)?;
                match self
                    .read_origin(
                        runtime.clone(),
                        manifest,
                        None,
                        true,
                        as_of,
                        deadline,
                        &cancellation,
                        None,
                    )
                    .await
                {
                    Ok(Some(read)) => return Ok(Some(read)),
                    Ok(None) => {}
                    Err(CompletedMarketSessionError::Unavailable) => {
                        tracing::warn!(
                            stage = "calendar-origin-read",
                            "completed market calendar selection unavailable"
                        );
                        return Ok(None);
                    }
                    Err(error) => {
                        tracing::warn!(
                            ?error,
                            stage = "calendar-origin-selection",
                            "completed market calendar selection unavailable"
                        );
                        return Err(error);
                    }
                }
            }
            if !has_more {
                return Ok(None);
            }
        }
    }

    /// Reopens only the original content/binding pair. It never substitutes a newer calendar.
    pub(crate) async fn read_reference(
        &self,
        original: &CompletedMarketSessionReference,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<CompletedMarketSessionRead>, CompletedMarketSessionError> {
        self.read_reference_with_job_context(original, as_of, deadline, cancellation, None)
            .await
    }

    pub(crate) async fn read_reference_for_job(
        &self,
        original: &CompletedMarketSessionReference,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
        job: &market_squawk_jobs::JobRunContext,
    ) -> Result<Option<CompletedMarketSessionRead>, CompletedMarketSessionError> {
        self.read_reference_with_job_context(original, as_of, deadline, cancellation, Some(job))
            .await
    }

    pub(crate) async fn read_reference_with_job_context(
        &self,
        original: &CompletedMarketSessionReference,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Option<CompletedMarketSessionRead>, CompletedMarketSessionError> {
        check(deadline, &cancellation)?;
        let Some(runtime) = self.runtime(deadline, &cancellation).await? else {
            return Ok(None);
        };
        let manifest = self
            .research
            .analytical_reader()
            .provider_capture_origin(
                original.capture_binding_digest,
                Sha256Digest::new(original.origin_content_digest.bytes()),
                as_of,
                deadline,
                &cancellation,
            )
            .map_err(|_| controlled_error(deadline, &cancellation))?;
        let Some(manifest) = manifest else {
            return Ok(None);
        };
        let read = match self
            .read_origin(
                runtime,
                manifest,
                Some(original.capture_binding_digest),
                false,
                as_of,
                deadline,
                &cancellation,
                job,
            )
            .await
        {
            Ok(read) => read,
            Err(CompletedMarketSessionError::Unavailable) => return Ok(None),
            Err(error) => return Err(error),
        };
        if read
            .as_ref()
            .is_some_and(|read| read.reference() != original)
        {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        Ok(read)
    }

    async fn runtime(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<AlpacaHistoricalRuntimeCapability>, CompletedMarketSessionError> {
        match self
            .market_runtime
            .current_alpaca_calendar_runtime(deadline, cancellation)
            .await
        {
            Ok(runtime) => Ok(Some(runtime)),
            Err(error) => {
                tracing::warn!(
                    ?error,
                    stage = "calendar-runtime-lookup",
                    "completed market calendar runtime unavailable"
                );
                check(deadline, cancellation)?;
                Ok(None)
            }
        }
    }

    async fn read_origin(
        &self,
        runtime: AlpacaHistoricalRuntimeCapability,
        manifest: DatasetManifestRef,
        expected_binding: Option<EvidenceDigest>,
        require_current_date: bool,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Option<CompletedMarketSessionRead>, CompletedMarketSessionError> {
        let source = runtime.calendar_metadata().source_id().clone();
        let Some(CalendarOriginRead {
            binding,
            published_at: _,
            retained_metadata,
            calendar_rows,
        }) = read_calendar_origin(
            &self.research,
            manifest.clone(),
            expected_binding,
            Some(source),
            as_of,
            deadline,
            cancellation,
            job,
        )
        .await
        .inspect_err(|error| {
            tracing::warn!(
                ?error,
                stage = "calendar-origin-canonical-read",
                "completed market calendar selection unavailable"
            );
        })?
        else {
            return Ok(None);
        };
        let binding_digest = binding.binding_digest();
        let capture = binding.capture();
        let coverage = calendar_rows
            .first()
            .ok_or(CompletedMarketSessionError::InvalidEvidence)?;
        let date = DateTime::<Utc>::from_timestamp_nanos(as_of.unix_nanos())
            .with_timezone(&New_York)
            .date_naive();
        let date = CalendarDate::new(
            u16::try_from(date.year()).map_err(invalid)?,
            u8::try_from(date.month()).map_err(invalid)?,
            u8::try_from(date.day()).map_err(invalid)?,
        )
        .map_err(invalid)?;
        let scope = coverage.scope();
        let market = match scope.native_product.as_str() {
            "IEX" => AlpacaCalendarMarket::Iex,
            "XNYS" => AlpacaCalendarMarket::Nyse,
            "XNAS" => AlpacaCalendarMarket::Nasdaq,
            _ => return Err(CompletedMarketSessionError::InvalidEvidence),
        };
        if require_current_date
            && (market != AlpacaCalendarMarket::Iex
                || scope.date_scope.start_date() > date
                || scope.date_scope.end_date() < date)
        {
            return Ok(None);
        }
        let source_age = as_of
            .unix_nanos()
            .checked_sub(coverage.observed_at().unix_nanos())
            .ok_or(CompletedMarketSessionError::InvalidEvidence)?;
        if source_age < 0
            || u64::try_from(source_age).map_err(invalid)?
                > retained_metadata.freshness_policy().max_source_age_nanos()
        {
            // Latest applicable source evidence is stale; do not fall back behind it.
            tracing::warn!(
                stage = "calendar-origin-source-age",
                source_age_nanos = source_age,
                maximum_source_age_nanos =
                    retained_metadata.freshness_policy().max_source_age_nanos(),
                "completed market calendar selection unavailable"
            );
            return Err(CompletedMarketSessionError::Unavailable);
        }
        let request = AlpacaAuthenticatedCalendarRequest::try_for_market(
            runtime.trading_api_environment(),
            market,
            scope.date_scope.start_date(),
            scope.date_scope.end_date(),
        )
        .map_err(invalid)?;
        if request.capture_request_identity().map_err(invalid)? != capture.request_set_identity() {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        let source = read_alpaca_completed_calendar_with_job_context(
            &self.research,
            runtime,
            manifest.clone(),
            request,
            binding_digest,
            calendar_rows.into_boxed_slice(),
            as_of,
            deadline,
            cancellation,
            job,
        )
        .await
        .inspect_err(|error| {
            tracing::warn!(
                ?error,
                stage = "calendar-origin-native-read",
                "completed market calendar selection unavailable"
            );
        })?;
        let replay = source.native_session_replay().clone();
        let action_calendar = self
            .research
            .read_provider_capture_generation_with_job_context(
                job,
                manifest.clone(),
                deadline,
                cancellation,
                move |generation, _, _, analytical, read_cancel| {
                    analytical
                        .rejoin_corporate_action_calendar(
                            &generation,
                            replay,
                            as_of,
                            deadline,
                            read_cancel,
                        )
                        .map(Arc::new)
                        .map_err(ResearchServiceError::from)
                },
            )
            .await
            .inspect_err(|error| calendar_worker_failure("calendar-origin-action-rejoin", error))
            .map_err(|_| controlled_error(deadline, cancellation))?;
        let authority = Arc::new(CompletedMarketSessionAuthority::new(source.clone()));
        Ok(Some(CompletedMarketSessionRead {
            action_calendar,
            source,
            reference: reference(&manifest, binding_digest),
            authority,
        }))
    }
}

struct CalendarOriginRead {
    binding: market_squawk_data::PersistedProviderCaptureBindingEvidence,
    published_at: Timestamp,
    retained_metadata: market_squawk_sources::SourceMetadata,
    calendar_rows: Vec<MarketCalendarObservation>,
}

/// Shared immutable physical and canonical checks. Live runtime/freshness admission remains
/// in read_origin; retained history never constructs that live capability.
async fn read_calendar_origin(
    research: &Arc<ResearchService>,
    manifest: DatasetManifestRef,
    expected_binding: Option<EvidenceDigest>,
    expected_source: Option<market_squawk_domain::SourceId>,
    as_of: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
    job: Option<&market_squawk_jobs::JobRunContext>,
) -> Result<Option<CalendarOriginRead>, CompletedMarketSessionError> {
    check(deadline, cancellation)?;
    let coordinates = research
        .read_provider_capture_generation_with_job_context(
            job,
            manifest.clone(),
            deadline,
            cancellation,
            move |generation, _, _, analytical, read_cancellation| {
                if generation.published_at() > as_of
                    || expected_source
                        .as_ref()
                        .is_some_and(|source| generation.source_id() != source)
                {
                    return Ok(None);
                }
                if generation.objects().len() != 1 || generation.objects()[0].inputs().len() != 1 {
                    return Err(ResearchServiceError::IngestAuthorityMismatch);
                }
                let binding = generation.objects()[0].inputs()[0].binding();
                if expected_binding.is_some_and(|expected| expected != binding.binding_digest()) {
                    return Err(ResearchServiceError::IngestAuthorityMismatch);
                }
                if binding.native_lineage().implementation() != "alpaca_calendar_v1" {
                    return Err(ResearchServiceError::IngestAuthorityMismatch);
                }
                let retained_metadata = analytical
                    .retained_source_metadata(
                        binding.capture().source_id(),
                        binding.capture().metadata_revision(),
                        as_of,
                        deadline,
                        read_cancellation,
                    )?
                    .ok_or(ResearchServiceError::IngestAuthorityMismatch)?;
                Ok(Some((
                    binding.clone(),
                    generation.published_at(),
                    retained_metadata,
                    generation.pinned().clone(),
                    generation.objects()[0].object().artifact_id(),
                    generation.objects()[0].generation_object_ordinal(),
                )))
            },
        )
        .await
        .inspect_err(|error| calendar_worker_failure("calendar-origin-coordinates", error))
        .map_err(|_| controlled_error(deadline, cancellation))?;
    let Some((binding, published_at, retained_metadata, pinned, artifact_id, object_ordinal)) =
        coordinates
    else {
        return Ok(None);
    };
    let capture = binding.capture();
    if capture.pages().len() != 1 {
        return Err(CompletedMarketSessionError::InvalidEvidence);
    }
    if u64::try_from(binding.record_count()).map_err(invalid)? > MAXIMUM_CALENDAR_QUERY_ROWS {
        return Err(CompletedMarketSessionError::ResourceBoundExceeded);
    }
    let read_deadline = deadline.min(
        Instant::now()
            .checked_add(Duration::from_secs(60))
            .ok_or(CompletedMarketSessionError::ResourceBoundExceeded)?,
    );
    // The verified creating output excludes inherited calendar objects. Read fresh bounded
    // batches from that exact object; slicing a whole-generation query retains its buffers.
    let mut cursor = research
        .analytical()
        .object_store()
        .pinned_object_batch_cursor(
            &pinned,
            artifact_id,
            object_ordinal,
            CALENDAR_DECODE_CHUNK_ROWS,
            MAXIMUM_CALENDAR_QUERY_BYTES as usize,
            cancellation,
        )
        .map_err(calendar_cursor_error)?;
    let mut remaining_read_bytes = (MAXIMUM_CALENDAR_QUERY_BYTES as usize)
        .checked_sub(
            binding
                .record_count()
                .checked_mul(size_of::<(u32, MarketCalendarObservation)>())
                .ok_or(CompletedMarketSessionError::ResourceBoundExceeded)?,
        )
        .ok_or(CompletedMarketSessionError::ResourceBoundExceeded)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(binding.record_count())
        .map_err(invalid)?;
    let control = CalendarReadControl {
        deadline,
        cancellation,
    };
    loop {
        check(read_deadline, cancellation)?;
        let batch = tokio::time::timeout_at(
            tokio::time::Instant::from_std(read_deadline),
            cursor.next_batch(),
        )
        .await
        .map_err(|_| CompletedMarketSessionError::DeadlineExceeded)?
        .map_err(calendar_cursor_error)?;
        let Some(batch) = batch else {
            break;
        };
        let batch_rows = batch.num_rows();
        let (records, retained_bytes) =
            ResearchArrowBatch::decode_query_capture_binding_rows_bounded(
                batch,
                &binding,
                remaining_read_bytes,
                &control,
            )
            .map_err(|error| {
                // ArrowConversionError Display exposes fixed variants and numeric bounds only.
                tracing::warn!(
                    stage = "calendar-origin-row-decode",
                    error = %error,
                    batch_rows,
                    remaining_bytes = remaining_read_bytes,
                    binding = %encode_digest(binding.binding_digest()),
                    "completed market calendar row decoding failed"
                );
                use market_squawk_data::ArrowConversionError;
                use market_squawk_platform::ResearchObjectControlError;
                match error {
                    ArrowConversionError::RetainedLimitExceeded { .. }
                    | ArrowConversionError::RetainedSizeOverflow
                    | ArrowConversionError::AllocationFailure => {
                        CompletedMarketSessionError::ResourceBoundExceeded
                    }
                    ArrowConversionError::ObjectControl(ResearchObjectControlError::Cancelled) => {
                        CompletedMarketSessionError::Cancelled
                    }
                    ArrowConversionError::ObjectControl(
                        ResearchObjectControlError::DeadlineExceeded,
                    ) => CompletedMarketSessionError::DeadlineExceeded,
                    _ => CompletedMarketSessionError::InvalidEvidence,
                }
            })?;
        remaining_read_bytes = remaining_read_bytes
            .checked_sub(retained_bytes)
            .ok_or(CompletedMarketSessionError::ResourceBoundExceeded)?;
        for (ordinal, record) in records {
            check(deadline, cancellation)?;
            let ResearchObservation::MarketCalendar(calendar) = record else {
                return Err(calendar_invalid("calendar-origin-row-type"));
            };
            let provenance = calendar.context().provenance();
            if provenance.received_at() > as_of
                || provenance.ingested_at() > as_of
                || provenance.ingested_at() > published_at
                || calendar.observed_at() > as_of
                || provenance
                    .availability()
                    .conservative_available_at()
                    .is_none_or(|available| available > as_of)
                || rows.len() >= binding.record_count()
            {
                return Err(calendar_invalid("calendar-origin-row-clock-or-count"));
            }
            rows.push((ordinal, calendar));
        }
    }
    check(deadline, cancellation)?;
    rows.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    if rows.len() != binding.record_count()
        || rows
            .iter()
            .enumerate()
            .any(|(index, (ordinal, _))| usize::try_from(*ordinal).ok() != Some(index))
    {
        return Err(calendar_invalid("calendar-origin-row-ordinals"));
    }
    let calendar_rows: Vec<MarketCalendarObservation> =
        rows.into_iter().map(|(_, row)| row).collect();
    let coverage = calendar_rows
        .first()
        .ok_or(CompletedMarketSessionError::InvalidEvidence)?;
    let MarketCalendarPayload::Coverage {
        completeness,
        reported_day_count,
        ..
    } = coverage.payload()
    else {
        return Err(calendar_invalid("calendar-origin-coverage-type"));
    };
    if *completeness != MarketCalendarCompleteness::CompleteSessionEnumeration
        || calendar_rows.len()
            != usize::try_from(*reported_day_count)
                .map_err(invalid)?
                .checked_add(1)
                .ok_or(CompletedMarketSessionError::ResourceBoundExceeded)?
    {
        return Err(calendar_invalid("calendar-origin-coverage-count"));
    }
    Ok(Some(CalendarOriginRead {
        binding,
        published_at,
        retained_metadata,
        calendar_rows,
    }))
}

fn calendar_invalid(stage: &'static str) -> CompletedMarketSessionError {
    tracing::warn!(stage, "completed market calendar evidence invalid");
    CompletedMarketSessionError::InvalidEvidence
}

fn calendar_cursor_error(
    error: market_squawk_data::ParquetStoreError,
) -> CompletedMarketSessionError {
    use market_squawk_data::ParquetStoreError;
    let (failure, result) = match error {
        ParquetStoreError::Cancelled => ("cancelled", CompletedMarketSessionError::Cancelled),
        ParquetStoreError::ReadDeadlineExceeded | ParquetStoreError::RecoveryDeadlineExceeded => (
            "deadline-exceeded",
            CompletedMarketSessionError::DeadlineExceeded,
        ),
        ParquetStoreError::ReadLimitExceeded
        | ParquetStoreError::SizeOverflow
        | ParquetStoreError::BlockingTaskLimitExceeded => (
            "resource-limit",
            CompletedMarketSessionError::ResourceBoundExceeded,
        ),
        _ => ("object-read", CompletedMarketSessionError::InvalidEvidence),
    };
    tracing::warn!(
        stage = "calendar-origin-object-read",
        failure,
        "completed market calendar read failed"
    );
    result
}

fn calendar_read_failure(stage: &'static str, error: &market_squawk_data::AnalyticalReadError) {
    use market_squawk_data::{AnalyticalReadError, ManifestCatalogError, QueryError};
    let failure = match error {
        AnalyticalReadError::Manifest(ManifestCatalogError::LockPoisoned) => "catalog-lock",
        AnalyticalReadError::Manifest(ManifestCatalogError::Cancelled) => "catalog-cancelled",
        AnalyticalReadError::Manifest(ManifestCatalogError::DeadlineExceeded) => "catalog-deadline",
        AnalyticalReadError::Manifest(_) => "catalog-other",
        AnalyticalReadError::Query(QueryError::MemoryLimitExceeded { .. }) => "query-memory",
        AnalyticalReadError::Query(QueryError::ReaderMemoryBoundExceeded) => "query-reader-memory",
        AnalyticalReadError::Query(QueryError::RowLimitExceeded { .. }) => "query-rows",
        AnalyticalReadError::Query(QueryError::ByteLimitExceeded { .. }) => "query-bytes",
        AnalyticalReadError::Query(QueryError::BlockingTaskLimitExceeded) => "query-workers",
        AnalyticalReadError::Query(QueryError::Cancelled) => "query-cancelled",
        AnalyticalReadError::Query(QueryError::DeadlineExceeded) => "query-deadline",
        AnalyticalReadError::Query(QueryError::UnsupportedSourceSchema) => "query-schema",
        AnalyticalReadError::Query(QueryError::DependencyAllocationContract) => "query-allocation",
        AnalyticalReadError::Query(QueryError::DataFusion(_)) => "query-datafusion",
        AnalyticalReadError::Query(_) => "query-other",
        AnalyticalReadError::Parquet(_) => "parquet-read",
        _ => "analytical-read",
    };
    tracing::warn!(stage, failure, "completed market calendar read failed");
}

pub(super) fn calendar_worker_failure(stage: &'static str, error: &ResearchServiceError) {
    use market_squawk_data::{CatalogError, IngestError};
    use market_squawk_platform::SealedResearchJournalStoreError as StoreError;
    let failure = match error {
        ResearchServiceError::Ingest(IngestError::AuthorityBusy)
        | ResearchServiceError::Ingest(IngestError::Catalog(CatalogError::AuthorityBusy))
        | ResearchServiceError::Catalog(CatalogError::AuthorityBusy) => "catalog-busy",
        ResearchServiceError::Ingest(IngestError::AuthorityLockPoisoned) => "catalog-lock-poisoned",
        ResearchServiceError::Ingest(IngestError::Cancelled) => "cancelled",
        ResearchServiceError::Ingest(IngestError::DeadlineExceeded) => "deadline-exceeded",
        ResearchServiceError::Ingest(IngestError::ProviderCaptureRequired) => {
            "capture-evidence-mismatch"
        }
        ResearchServiceError::Ingest(IngestError::Manifest(_))
        | ResearchServiceError::Manifest(_) => "manifest-error",
        ResearchServiceError::Ingest(IngestError::Catalog(_))
        | ResearchServiceError::Catalog(_) => "catalog-error",
        ResearchServiceError::Ingest(IngestError::SealedProviderCapture(StoreError::Io {
            ..
        }))
        | ResearchServiceError::ProviderCaptureStore(StoreError::Io { .. }) => "capture-store-io",
        ResearchServiceError::Ingest(IngestError::SealedProviderCapture(_))
        | ResearchServiceError::ProviderCaptureStore(_) => "capture-store-error",
        ResearchServiceError::Ingest(_) => "ingest-error",
        ResearchServiceError::IngestAuthorityMismatch => "ingest-authority-mismatch",
        ResearchServiceError::ProviderCaptureSealWorkerUnavailable => "worker-unavailable",
        ResearchServiceError::Path(_) => "path-unavailable",
        _ => "research-error",
    };
    tracing::warn!(stage, failure, "completed market calendar worker failed");
}

struct CalendarReadControl<'a> {
    deadline: Instant,
    cancellation: &'a CancellationToken,
}
impl market_squawk_platform::ResearchObjectControl for CalendarReadControl<'_> {
    fn checkpoint(
        &self,
        _: market_squawk_platform::ResearchObjectControlPoint,
    ) -> Result<(), market_squawk_platform::ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            Err(market_squawk_platform::ResearchObjectControlError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(market_squawk_platform::ResearchObjectControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

fn reference(
    manifest: &DatasetManifestRef,
    binding: EvidenceDigest,
) -> CompletedMarketSessionReference {
    CompletedMarketSessionReference {
        origin_content_digest: EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            manifest.content_hash().bytes(),
        ),
        capture_binding_digest: binding,
    }
}
fn check(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CompletedMarketSessionError> {
    if cancellation.is_cancelled() {
        Err(CompletedMarketSessionError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(CompletedMarketSessionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn controlled_error(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> CompletedMarketSessionError {
    check(deadline, cancellation)
        .err()
        .unwrap_or(CompletedMarketSessionError::InvalidEvidence)
}
fn invalid<T>(_: T) -> CompletedMarketSessionError {
    CompletedMarketSessionError::InvalidEvidence
}
fn decode_digest(value: &str) -> Result<EvidenceDigest, &'static str> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("calendar reference digest is invalid");
    }
    let mut bytes = [0_u8; 32];
    for (index, output) in bytes.iter_mut().enumerate() {
        *output = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "calendar reference digest is invalid")?;
    }
    if bytes == [0; 32] {
        return Err("calendar reference digest is empty");
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}
fn encode_digest(value: EvidenceDigest) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(64);
    for byte in value.bytes() {
        let _ = write!(output, "{byte:02x}");
    }
    output
}
