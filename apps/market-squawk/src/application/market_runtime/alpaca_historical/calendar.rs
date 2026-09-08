//! Exact retained range-calendar composition over returned Alpaca daily-bar coordinates.

use std::{sync::Arc, time::Instant};

use chrono::{DateTime, Datelike as _, LocalResult, NaiveDate, TimeZone as _, Utc};
use chrono_tz::America::New_York;

use market_squawk_adapter_alpaca::{
    ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES, ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS,
    AlpacaAdjustment, AlpacaAuthenticatedCalendarExecutor, AlpacaAuthenticatedCalendarRequest,
    AlpacaAuthenticatedCalendarResponse, AlpacaError, AlpacaHistoricalBarTimeAuthority,
    AlpacaHistoricalBarTimeRequest, AlpacaHistoricalEquityConfig,
    AlpacaHistoricalEquityPreflightReceipt, AlpacaHistoricalReturnedBarTime,
    AlpacaHistoricalSeriesSemantics, AlpacaTradingApiEnvironment,
};
use market_squawk_domain::{
    AvailabilityEvidence, BarTimeSemantics, BarTimestampBasis, CalendarDate, DigestAlgorithm,
    EvidenceDigest, ExactPayloadEvidence, InstrumentId, MarketBarAdjustment,
    MarketBarSessionEvidence, MarketBarSessionKind, ProviderInstrumentId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_sources::{
    BudgetDecision, BudgetDispatchDecision, BudgetPermit, BudgetReservationDecision,
    CompleteMarketBarHistoryV1, HttpRequestBounds, ProviderCaptureMaterial, SharedProviderBudget,
    apply_http_retry_after,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::market_calendar::{
    MarketCalendarClock, SystemMarketCalendarClock,
    alpaca::{
        AlpacaCalendarApiEnvironment, AlpacaIexUtcCalendarFetchRequest,
        AlpacaIexUtcCalendarFetchResult, AlpacaPreauthorizedBarTimeAuthority,
        MAXIMUM_ALPACA_CALENDAR_RESPONSE_BYTES, try_produce_alpaca_iex_utc_daily_calendar,
    },
};

use super::{AlpacaHistoricalCapabilityError, AlpacaHistoricalRuntimeCapability, ensure_before};

const COMPOSITE_RULESET_ID: &str = "alpaca-v3-iex-utc-range-returned-dates-v2";
const MAXIMUM_COMPOSITE_CALENDAR_BYTES: usize = 32 * 1024 * 1024;
const MAXIMUM_CALENDAR_HTTP_ATTEMPTS: usize = 3;
const MAXIMUM_RANGE_CALENDAR_ROWS: usize = ALPACA_HISTORICAL_MAX_LOOKBACK_DAYS as usize + 2;

struct RetainedCalendarRangeResponse {
    response: Arc<AlpacaAuthenticatedCalendarResponse>,
    received_at: Timestamp,
    body_digest: EvidenceDigest,
}

struct CalendarRangeRow {
    date: CalendarDate,
    derived_body: Box<[u8]>,
}

struct ExactCalendarSession {
    date: CalendarDate,
    authority: Arc<AlpacaPreauthorizedBarTimeAuthority>,
    series_semantics: AlpacaHistoricalSeriesSemantics,
}

struct ExactCalendarFragment {
    returned: AlpacaHistoricalReturnedBarTime,
    authority: Arc<AlpacaPreauthorizedBarTimeAuthority>,
    original_semantics: BarTimeSemantics,
}

/// Secret-free, revocable exact resolver for every returned bar in one retained preflight graph.
///
/// The plan directory retains this value behind an [`Arc`]. Exact range responses and all parsed
/// session authorities are therefore reused by discovery and extraction without another network
/// request. This is deliberately a runtime-generation cache, not a cross-generation or durable
/// restart checkpoint.
pub(crate) struct AlpacaHistoricalCompositeCalendarAuthority {
    runtime: AlpacaHistoricalRuntimeCapability,
    preflight_digest: EvidenceDigest,
    instrument_id: InstrumentId,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    timeframe: SourceIdentifier,
    requested_start: Timestamp,
    requested_end: Timestamp,
    adjustment: MarketBarAdjustment,
    range_responses: Box<[RetainedCalendarRangeResponse]>,
    sessions: Box<[ExactCalendarSession]>,
    fragments: Box<[ExactCalendarFragment]>,
    series_semantics: AlpacaHistoricalSeriesSemantics,
    expected_provider_timestamps: Box<[Timestamp]>,
    retained_response_bytes: usize,
}

impl AlpacaHistoricalCompositeCalendarAuthority {
    pub(crate) const fn series_semantics(&self) -> &AlpacaHistoricalSeriesSemantics {
        &self.series_semantics
    }

    pub(crate) const fn preflight_digest(&self) -> EvidenceDigest {
        self.preflight_digest
    }

    pub(crate) const fn retained_response_bytes(&self) -> usize {
        self.retained_response_bytes
    }

    pub(crate) fn history_capture_semantic(
        &self,
        instrument_revision_digest: EvidenceDigest,
        admitted_plan_digest: EvidenceDigest,
    ) -> Result<CompleteMarketBarHistoryV1, AlpacaHistoricalCalendarError> {
        let mut expected_provider_timestamps = Vec::new();
        expected_provider_timestamps
            .try_reserve_exact(self.expected_provider_timestamps.len())
            .map_err(|_| AlpacaHistoricalCalendarError::Allocation)?;
        expected_provider_timestamps.extend_from_slice(&self.expected_provider_timestamps);
        CompleteMarketBarHistoryV1::try_new(
            self.requested_start,
            self.requested_end,
            self.instrument_id,
            instrument_revision_digest,
            admitted_plan_digest,
            self.provider_instrument_id.clone(),
            self.venue_id.clone(),
            self.feed.clone(),
            self.timeframe.clone(),
            self.adjustment,
            self.series_semantics.timestamp_basis(),
            self.series_semantics.session().kind(),
            self.series_semantics.session().ruleset().clone(),
            SourceIdentifier::try_from("alpaca-iex-historical-bars-and-calendar/v1")
                .map_err(|_| AlpacaHistoricalCalendarError::Identity)?,
            0,
            1,
            expected_provider_timestamps,
            self.series_semantics.session().evidence(),
        )
        .map_err(|_| AlpacaHistoricalCalendarError::ConflictingFragment)
    }

    /// Returns the accepted exact range-calendar response as source-neutral capture material.
    ///
    /// Refusal attempts remain bounded telemetry in this authority and are intentionally excluded.
    /// The common integration lane must seal this material together with the matching bar capture
    /// before publishing any canonical historical rows.
    pub(crate) fn provider_capture_material(
        &self,
        config: &AlpacaHistoricalEquityConfig,
        preflight: &AlpacaHistoricalEquityPreflightReceipt,
    ) -> Result<ProviderCaptureMaterial, AlpacaHistoricalCalendarError> {
        self.validate_current()?;
        let start_date = utc_calendar_date(preflight.plan().start())?;
        let end_date = utc_calendar_date(preflight.plan().end())?;
        let timeframe = preflight.plan().timeframe().provider_identifier()?;
        if preflight.digest() != self.preflight_digest
            || preflight.plan().mapping().instrument() != self.instrument_id
            || preflight.plan().mapping().symbol() != self.provider_instrument_id.as_str()
            || timeframe != self.timeframe
        {
            return Err(AlpacaHistoricalCalendarError::RequestMismatch);
        }
        let successful = self
            .range_responses
            .last()
            .ok_or(AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
        validate_range_responses(&self.range_responses, successful.response.request())?;
        if successful.response.status() != 200
            || successful.received_at != successful.response.received_at()
            || successful.response.request().start_date() != start_date
            || successful.response.request().end_date() != end_date
        {
            return Err(AlpacaHistoricalCalendarError::InvalidRangeResponse);
        }
        let mut datasets = config.provider_dataset_identifiers();
        let dataset = datasets
            .next()
            .ok_or(AlpacaHistoricalCalendarError::RequestMismatch)?
            .clone();
        if datasets.next().is_some() {
            return Err(AlpacaHistoricalCalendarError::RequestMismatch);
        }
        let material = successful.response.provider_capture_material(
            config.metadata().source_id().clone(),
            config.metadata().revision().clone(),
            dataset,
        )?;
        self.validate_current()?;
        Ok(material)
    }
}

impl std::fmt::Debug for AlpacaHistoricalCompositeCalendarAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalCompositeCalendarAuthority")
            .field("preflight_digest", &self.preflight_digest)
            .field("instrument_id", &self.instrument_id)
            .field("provider_instrument_id", &self.provider_instrument_id)
            .field("venue_id", &self.venue_id)
            .field("timeframe", &self.timeframe)
            .field("range_response_count", &self.range_responses.len())
            .field("parsed_session_count", &self.sessions.len())
            .field("fragment_count", &self.fragments.len())
            .field("revoked", &self.runtime.is_revoked())
            .finish_non_exhaustive()
    }
}

impl AlpacaHistoricalBarTimeAuthority for AlpacaHistoricalCompositeCalendarAuthority {
    fn validate_current(&self) -> Result<(), AlpacaError> {
        self.runtime
            .validate_current_now()
            .map_err(|_error| AlpacaError::Protocol)?;
        for session in &self.sessions {
            session.authority.validate_current()?;
        }
        self.runtime
            .validate_current_now()
            .map_err(|_error| AlpacaError::Protocol)
    }

    fn resolve(
        &self,
        request: &AlpacaHistoricalBarTimeRequest,
    ) -> Result<BarTimeSemantics, AlpacaError> {
        self.validate_current()?;
        if request.instrument_id() != self.instrument_id
            || request.provider_instrument_id() != &self.provider_instrument_id
            || request.venue_id() != &self.venue_id
            || request.timeframe() != &self.timeframe
        {
            return Err(AlpacaError::Protocol);
        }
        let index = self
            .fragments
            .binary_search_by_key(&request.provider_timestamp(), |fragment| {
                fragment.returned.provider_timestamp()
            })
            .map_err(|_| AlpacaError::Protocol)?;
        let fragment = self.fragments.get(index).ok_or(AlpacaError::Protocol)?;
        let resolved = fragment.authority.resolve(request)?;
        if resolved != fragment.original_semantics
            || resolved.provider_timestamp() != fragment.returned.provider_timestamp()
            || resolved.timestamp_basis() != self.series_semantics.timestamp_basis()
        {
            return Err(AlpacaError::Protocol);
        }
        let rebound = BarTimeSemantics::try_new(
            resolved.period_start(),
            resolved.period_end_exclusive(),
            resolved.timestamp_basis(),
            self.series_semantics.session().clone(),
        )
        .map_err(|_| AlpacaError::Protocol)?;
        self.validate_current()?;
        Ok(rebound)
    }
}

impl AlpacaHistoricalRuntimeCapability {
    /// Fetches one bounded inclusive calendar range, parses every returned session, and binds the
    /// exact subset needed by the returned daily bars into a stable plan-specific receipt.
    #[allow(
        clippy::too_many_arguments,
        reason = "preflight identity, exact canonical provider identity, transport bounds, and cancellation stay explicit"
    )]
    pub(crate) async fn compose_returned_bar_calendar(
        &self,
        preflight: &Arc<AlpacaHistoricalEquityPreflightReceipt>,
        instrument_id: InstrumentId,
        provider_instrument_id: ProviderInstrumentId,
        request_bounds: HttpRequestBounds,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Arc<AlpacaHistoricalCompositeCalendarAuthority>, AlpacaHistoricalCalendarError>
    {
        ensure_before(deadline, cancellation)?;
        if preflight.returned_bar_times().is_empty()
            || preflight.plan().timeframe().provider_identifier()?.as_str() != "1Day"
        {
            return Err(AlpacaHistoricalCalendarError::UnsupportedOrEmptyPlan);
        }
        validate_returned_bar_times(preflight.returned_bar_times())?;
        let _operation = self.inner.admit()?;
        self.validate_current(cancellation).await?;
        let (credentials, budget) = self.inner.historical_authority()?;
        let executor = AlpacaAuthenticatedCalendarExecutor::try_new(credentials, request_bounds)?;
        let clock: Arc<dyn MarketCalendarClock> = Arc::new(SystemMarketCalendarClock);
        let venue_id =
            VenueId::try_from("iex").map_err(|_| AlpacaHistoricalCalendarError::Identity)?;
        let timeframe = SourceIdentifier::try_from("1Day")
            .map_err(|_| AlpacaHistoricalCalendarError::Identity)?;
        let returned_bar_times = preflight.returned_bar_times();
        let start_date = utc_calendar_date(preflight.plan().start())?;
        let end_date = utc_calendar_date(preflight.plan().end())?;
        let environment = self.trading_api_environment();
        let transport_request =
            AlpacaAuthenticatedCalendarRequest::try_new(environment, start_date, end_date)?;
        let producer_environment = producer_environment(environment);

        let range_responses = tokio::select! {
            biased;
            () = self.inner.cancellation.cancelled() => {
                return Err(AlpacaHistoricalCapabilityError::Revoked.into());
            }
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalCapabilityError::Cancelled.into());
            }
            result = execute_rate_accounted_calendar(
                &executor,
                &transport_request,
                &budget,
                deadline,
                cancellation,
            ) => result?,
        };
        validate_range_responses(&range_responses, &transport_request)?;
        let successful = range_responses
            .last()
            .ok_or(AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
        if successful.response.status() != 200 {
            return Err(AlpacaHistoricalCalendarError::RangeHttpStatus(
                successful.response.status(),
            ));
        }
        let range_rows = parse_calendar_range_rows(
            successful.response.body(),
            transport_request.start_date(),
            transport_request.end_date(),
        )?;

        let mut sessions = Vec::new();
        sessions
            .try_reserve_exact(range_rows.len())
            .map_err(|_| AlpacaHistoricalCalendarError::Allocation)?;
        let mut fragments = Vec::new();
        fragments
            .try_reserve_exact(returned_bar_times.len())
            .map_err(|_| AlpacaHistoricalCalendarError::Allocation)?;
        let mut expected_provider_timestamps = Vec::new();
        expected_provider_timestamps
            .try_reserve_exact(range_rows.len())
            .map_err(|_| AlpacaHistoricalCalendarError::Allocation)?;
        let mut retained_response_bytes =
            range_responses
                .iter()
                .try_fold(0_usize, |total, retained| {
                    total
                        .checked_add(retained.response.body().len())
                        .filter(|bytes| *bytes <= MAXIMUM_COMPOSITE_CALENDAR_BYTES)
                        .ok_or(AlpacaHistoricalCalendarError::ResponseBoundExceeded)
                })?;

        for (row_index, row) in range_rows.into_iter().enumerate() {
            ensure_before(deadline, cancellation)?;
            self.validate_current(cancellation).await?;
            let producer_request =
                AlpacaIexUtcCalendarFetchRequest::try_new(producer_environment, row.date)?;
            if producer_request.method() != transport_request.method()
                || producer_request.origin() != transport_request.origin()
                || producer_request.requested_date() != row.date
                || row.date < transport_request.start_date()
                || row.date > transport_request.end_date()
            {
                return Err(AlpacaHistoricalCalendarError::RequestMismatch);
            }
            retained_response_bytes = retained_response_bytes
                .checked_add(row.derived_body.len())
                .filter(|bytes| *bytes <= MAXIMUM_COMPOSITE_CALENDAR_BYTES)
                .ok_or(AlpacaHistoricalCalendarError::ResponseBoundExceeded)?;
            let retrieval_evidence = calendar_retrieval_evidence(
                self,
                preflight.digest(),
                &transport_request,
                &range_responses,
                &producer_request,
                u32::try_from(row_index)
                    .map_err(|_| AlpacaHistoricalCalendarError::ResponseBoundExceeded)?,
                &row.derived_body,
            )?;
            let fetch_result = AlpacaIexUtcCalendarFetchResult::try_new(
                producer_request,
                successful.response.status(),
                row.derived_body,
                retrieval_evidence,
                AvailabilityEvidence::local_first_observed(successful.received_at),
                successful.received_at,
            )?;
            let calendar = Arc::new(try_produce_alpaca_iex_utc_daily_calendar(fetch_result)?);
            let expected_provider_timestamp = alpaca_daily_provider_timestamp(row.date)?;
            let resolved_semantics = calendar.resolve_at(
                &venue_id,
                &timeframe,
                expected_provider_timestamp,
                clock.now()?,
            )?;
            let expected_in_plan = expected_provider_timestamp >= preflight.plan().start()
                && expected_provider_timestamp <= preflight.plan().end()
                && resolved_semantics.period_end_exclusive() <= preflight.plan().end();
            let returned = returned_bar_times
                .binary_search_by_key(&row.date, |returned| returned.calendar_date())
                .ok()
                .and_then(|index| returned_bar_times.get(index));
            if expected_in_plan {
                expected_provider_timestamps.push(expected_provider_timestamp);
            }
            if expected_in_plan != returned.is_some()
                || returned.is_some_and(|returned| {
                    returned.provider_timestamp() != expected_provider_timestamp
                })
            {
                return Err(AlpacaHistoricalCalendarError::MissingReturnedSession);
            }
            let original_semantics = expected_in_plan.then_some(resolved_semantics);
            let authority = Arc::new(AlpacaPreauthorizedBarTimeAuthority::try_new(
                calendar,
                Arc::clone(&clock),
            )?);
            let session_semantics = authority.series_semantics(&timeframe)?;
            sessions.push(ExactCalendarSession {
                date: row.date,
                authority: Arc::clone(&authority),
                series_semantics: session_semantics,
            });
            if let (Some(returned), Some(original_semantics)) = (returned, original_semantics) {
                fragments.push(ExactCalendarFragment {
                    returned: *returned,
                    authority,
                    original_semantics,
                });
            }
        }
        drop(executor);
        drop(budget);
        self.validate_current(cancellation).await?;
        validate_sessions(&sessions)?;
        validate_fragments(&fragments)?;
        if expected_provider_timestamps.len() != returned_bar_times.len()
            || expected_provider_timestamps
                .iter()
                .zip(returned_bar_times)
                .any(|(expected, returned)| *expected != returned.provider_timestamp())
            || fragments.len() != returned_bar_times.len()
        {
            return Err(AlpacaHistoricalCalendarError::MissingReturnedSession);
        }
        let session_digest = composite_session_digest(
            self,
            preflight.digest(),
            instrument_id,
            &provider_instrument_id,
            &venue_id,
            &timeframe,
            &transport_request,
            &range_responses,
            &sessions,
            &fragments,
        )?;
        let session = MarketBarSessionEvidence::try_new(
            MarketBarSessionKind::ProviderDefined,
            SourceIdentifier::try_from(COMPOSITE_RULESET_ID)
                .map_err(|_| AlpacaHistoricalCalendarError::Identity)?,
            session_digest,
        )
        .map_err(|_| AlpacaHistoricalCalendarError::Identity)?;
        let feed = SourceIdentifier::try_from("iex")
            .map_err(|_| AlpacaHistoricalCalendarError::Identity)?;
        let adjustment = market_bar_adjustment(preflight.plan().adjustment());
        let series_semantics =
            AlpacaHistoricalSeriesSemantics::new(BarTimestampBasis::PeriodStart, session);
        Ok(Arc::new(AlpacaHistoricalCompositeCalendarAuthority {
            runtime: self.clone(),
            preflight_digest: preflight.digest(),
            instrument_id,
            provider_instrument_id,
            venue_id,
            feed,
            timeframe,
            requested_start: preflight.plan().start(),
            requested_end: preflight.plan().end(),
            adjustment,
            range_responses,
            sessions: sessions.into_boxed_slice(),
            fragments: fragments.into_boxed_slice(),
            series_semantics,
            expected_provider_timestamps: expected_provider_timestamps.into_boxed_slice(),
            retained_response_bytes,
        }))
    }
}

async fn execute_rate_accounted_calendar(
    executor: &AlpacaAuthenticatedCalendarExecutor,
    request: &AlpacaAuthenticatedCalendarRequest,
    budget: &SharedProviderBudget,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Box<[RetainedCalendarRangeResponse]>, AlpacaHistoricalCalendarError> {
    let mut responses = Vec::new();
    responses
        .try_reserve_exact(MAXIMUM_CALENDAR_HTTP_ATTEMPTS)
        .map_err(|_| AlpacaHistoricalCalendarError::Allocation)?;
    for attempt in 0..MAXIMUM_CALENDAR_HTTP_ATTEMPTS {
        ensure_before(deadline, cancellation)?;
        let permit = commit_calendar_dispatch(budget, deadline, cancellation).await?;
        let response = executor
            .execute(request.clone(), deadline, cancellation)
            .await?;
        let governed_refusal = matches!(response.status(), 429 | 503);
        let retry_decision =
            governed_refusal.then(|| apply_http_retry_after(budget, response.retry_after(), 1_000));
        let received_at = response.received_at();
        let body_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(response.body()).into(),
        );
        if response.status() == 200 {
            budget
                .record_success()
                .map_err(|_| AlpacaHistoricalCalendarError::BudgetUnavailable)?;
        }
        permit.release();
        responses.push(RetainedCalendarRangeResponse {
            response: Arc::new(response),
            received_at,
            body_digest,
        });
        ensure_before(deadline, cancellation)?;
        if !governed_refusal {
            return Ok(responses.into_boxed_slice());
        }
        let retry_decision =
            retry_decision.ok_or(AlpacaHistoricalCalendarError::BudgetUnavailable)?;
        if attempt + 1 == MAXIMUM_CALENDAR_HTTP_ATTEMPTS {
            return match retry_decision {
                BudgetDecision::WaitUntil(_wait_until) => {
                    Err(AlpacaHistoricalCalendarError::RetryLimitExceeded)
                }
                BudgetDecision::Ready(permit) => {
                    permit.release();
                    Err(AlpacaHistoricalCalendarError::BudgetUnavailable)
                }
                BudgetDecision::Unavailable(_reason) => {
                    Err(AlpacaHistoricalCalendarError::BudgetUnavailable)
                }
            };
        }
        wait_for_budget(budget, retry_decision, deadline, cancellation).await?;
    }
    Err(AlpacaHistoricalCalendarError::RetryLimitExceeded)
}

async fn commit_calendar_dispatch(
    budget: &SharedProviderBudget,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<BudgetPermit, AlpacaHistoricalCalendarError> {
    loop {
        ensure_before(deadline, cancellation)?;
        let reservation = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => reservation,
            BudgetReservationDecision::WaitUntil(wait_until) => {
                wait_until_budget(budget, wait_until, deadline, cancellation).await?;
                continue;
            }
            BudgetReservationDecision::Unavailable(_reason) => {
                return Err(AlpacaHistoricalCalendarError::BudgetUnavailable);
            }
        };
        ensure_before(deadline, cancellation)?;
        match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => return Ok(permit),
            BudgetDispatchDecision::WaitUntil(wait_until) => {
                wait_until_budget(budget, wait_until, deadline, cancellation).await?;
            }
            BudgetDispatchDecision::Unavailable(_reason) => {
                return Err(AlpacaHistoricalCalendarError::BudgetUnavailable);
            }
        }
    }
}

async fn wait_for_budget(
    budget: &SharedProviderBudget,
    decision: BudgetDecision,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaHistoricalCalendarError> {
    let wait_until = match decision {
        BudgetDecision::WaitUntil(wait_until) => wait_until,
        BudgetDecision::Ready(permit) => {
            permit.release();
            return Err(AlpacaHistoricalCalendarError::BudgetUnavailable);
        }
        BudgetDecision::Unavailable(_reason) => {
            return Err(AlpacaHistoricalCalendarError::BudgetUnavailable);
        }
    };
    wait_until_budget(budget, wait_until, deadline, cancellation).await
}

async fn wait_until_budget(
    budget: &SharedProviderBudget,
    wait_until: market_squawk_sources::MonotonicInstant,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaHistoricalCalendarError> {
    let wait = budget
        .remaining_wait(wait_until)
        .map_err(|_| AlpacaHistoricalCalendarError::BudgetUnavailable)?;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(AlpacaHistoricalCapabilityError::DeadlineExceeded)?;
    if wait > remaining {
        return Err(AlpacaHistoricalCapabilityError::DeadlineExceeded.into());
    }
    let deadline_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(deadline_sleep);
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(AlpacaHistoricalCapabilityError::Cancelled.into()),
        () = &mut deadline_sleep => Err(AlpacaHistoricalCapabilityError::DeadlineExceeded.into()),
        () = tokio::time::sleep(wait) => {
            ensure_before(deadline, cancellation).map_err(Into::into)
        },
    }
}

fn validate_returned_bar_times(
    returned_bar_times: &[AlpacaHistoricalReturnedBarTime],
) -> Result<(), AlpacaHistoricalCalendarError> {
    if returned_bar_times.is_empty() || returned_bar_times.len() > MAXIMUM_RANGE_CALENDAR_ROWS {
        return Err(AlpacaHistoricalCalendarError::UnsupportedOrEmptyPlan);
    }
    for pair in returned_bar_times.windows(2) {
        if pair[0] >= pair[1] || pair[0].calendar_date() >= pair[1].calendar_date() {
            return Err(AlpacaHistoricalCalendarError::ConflictingFragment);
        }
    }
    Ok(())
}

fn validate_range_responses(
    responses: &[RetainedCalendarRangeResponse],
    request: &AlpacaAuthenticatedCalendarRequest,
) -> Result<(), AlpacaHistoricalCalendarError> {
    if responses.is_empty() || responses.len() > MAXIMUM_CALENDAR_HTTP_ATTEMPTS {
        return Err(AlpacaHistoricalCalendarError::InvalidRangeResponse);
    }
    let mut previous_received_at: Option<Timestamp> = None;
    for (index, retained) in responses.iter().enumerate() {
        let exact_body_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(retained.response.body()).into(),
        );
        if retained.response.request() != request
            || retained.response.body().len() > ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES
            || retained.body_digest != exact_body_digest
            || retained.response.received_at() != retained.received_at
            || previous_received_at.is_some_and(|previous| previous > retained.received_at)
            || (index + 1 < responses.len() && !matches!(retained.response.status(), 429 | 503))
        {
            return Err(AlpacaHistoricalCalendarError::InvalidRangeResponse);
        }
        previous_received_at = Some(retained.received_at);
    }
    Ok(())
}

fn parse_calendar_range_rows(
    body: &[u8],
    start_date: CalendarDate,
    end_date: CalendarDate,
) -> Result<Vec<CalendarRangeRow>, AlpacaHistoricalCalendarError> {
    if body.is_empty() || body.len() > ALPACA_HISTORICAL_CALENDAR_MAX_RESPONSE_BYTES {
        return Err(AlpacaHistoricalCalendarError::InvalidRangeResponse);
    }
    let envelope: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
    let object = envelope
        .as_object()
        .ok_or(AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
    let market = object
        .get("market")
        .filter(|market| market.is_object())
        .ok_or(AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
    let calendar = object
        .get("calendar")
        .and_then(serde_json::Value::as_array)
        .ok_or(AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
    if calendar.is_empty() || calendar.len() > MAXIMUM_RANGE_CALENDAR_ROWS {
        return Err(AlpacaHistoricalCalendarError::InvalidRangeResponse);
    }

    let mut rows = Vec::new();
    rows.try_reserve_exact(calendar.len())
        .map_err(|_| AlpacaHistoricalCalendarError::Allocation)?;
    let mut previous_date: Option<CalendarDate> = None;
    for row in calendar {
        let date = row
            .as_object()
            .and_then(|row| row.get("date"))
            .and_then(serde_json::Value::as_str)
            .ok_or(AlpacaHistoricalCalendarError::InvalidRangeResponse)
            .and_then(parse_calendar_date)?;
        if date < start_date
            || date > end_date
            || previous_date.is_some_and(|previous| previous >= date)
        {
            return Err(AlpacaHistoricalCalendarError::InvalidRangeResponse);
        }
        previous_date = Some(date);

        let mut derived = serde_json::Map::new();
        derived.insert("market".to_owned(), market.clone());
        derived.insert(
            "calendar".to_owned(),
            serde_json::Value::Array(vec![row.clone()]),
        );
        let derived_body = serde_json::to_vec(&serde_json::Value::Object(derived))
            .map_err(|_| AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
        if derived_body.is_empty() || derived_body.len() > MAXIMUM_ALPACA_CALENDAR_RESPONSE_BYTES {
            return Err(AlpacaHistoricalCalendarError::ResponseBoundExceeded);
        }
        rows.push(CalendarRangeRow {
            date,
            derived_body: derived_body.into_boxed_slice(),
        });
    }
    Ok(rows)
}

fn parse_calendar_date(value: &str) -> Result<CalendarDate, AlpacaHistoricalCalendarError> {
    let bytes = value.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| index != 4 && index != 7 && !byte.is_ascii_digit())
    {
        return Err(AlpacaHistoricalCalendarError::InvalidRangeResponse);
    }
    let year = value[0..4]
        .parse::<u16>()
        .map_err(|_| AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
    let month = value[5..7]
        .parse::<u8>()
        .map_err(|_| AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
    let day = value[8..10]
        .parse::<u8>()
        .map_err(|_| AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
    CalendarDate::new(year, month, day)
        .map_err(|_| AlpacaHistoricalCalendarError::InvalidRangeResponse)
}

fn utc_calendar_date(value: Timestamp) -> Result<CalendarDate, AlpacaHistoricalCalendarError> {
    let date = DateTime::<Utc>::from_timestamp_nanos(value.unix_nanos()).date_naive();
    CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| AlpacaHistoricalCalendarError::RequestMismatch)?,
        u8::try_from(date.month()).map_err(|_| AlpacaHistoricalCalendarError::RequestMismatch)?,
        u8::try_from(date.day()).map_err(|_| AlpacaHistoricalCalendarError::RequestMismatch)?,
    )
    .map_err(|_| AlpacaHistoricalCalendarError::RequestMismatch)
}

fn alpaca_daily_provider_timestamp(
    date: CalendarDate,
) -> Result<Timestamp, AlpacaHistoricalCalendarError> {
    let local_date = NaiveDate::from_ymd_opt(
        i32::from(date.year()),
        u32::from(date.month()),
        u32::from(date.day()),
    )
    .ok_or(AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
    let local_midnight = local_date
        .and_hms_opt(0, 0, 0)
        .ok_or(AlpacaHistoricalCalendarError::InvalidRangeResponse)?;
    let local = match New_York.from_local_datetime(&local_midnight) {
        LocalResult::Single(value) => value,
        LocalResult::Ambiguous(_, _) | LocalResult::None => {
            return Err(AlpacaHistoricalCalendarError::InvalidRangeResponse);
        }
    };
    local
        .with_timezone(&Utc)
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(AlpacaHistoricalCalendarError::InvalidRangeResponse)
}

const fn market_bar_adjustment(adjustment: AlpacaAdjustment) -> MarketBarAdjustment {
    match adjustment {
        AlpacaAdjustment::Raw => MarketBarAdjustment::Raw,
        AlpacaAdjustment::Split => MarketBarAdjustment::Split,
        AlpacaAdjustment::Dividend => MarketBarAdjustment::Dividend,
        AlpacaAdjustment::SpinOff => MarketBarAdjustment::SpinOff,
        AlpacaAdjustment::All => MarketBarAdjustment::All,
    }
}

fn validate_sessions(
    sessions: &[ExactCalendarSession],
) -> Result<(), AlpacaHistoricalCalendarError> {
    if sessions.is_empty() || sessions.len() > MAXIMUM_RANGE_CALENDAR_ROWS {
        return Err(AlpacaHistoricalCalendarError::InvalidRangeResponse);
    }
    for pair in sessions.windows(2) {
        if pair[0].date >= pair[1].date {
            return Err(AlpacaHistoricalCalendarError::InvalidRangeResponse);
        }
    }
    for session in sessions {
        if session.series_semantics.timestamp_basis() != BarTimestampBasis::PeriodStart
            || session.series_semantics.session().kind() != MarketBarSessionKind::ProviderDefined
        {
            return Err(AlpacaHistoricalCalendarError::ConflictingFragment);
        }
    }
    Ok(())
}

fn validate_fragments(
    fragments: &[ExactCalendarFragment],
) -> Result<(), AlpacaHistoricalCalendarError> {
    if fragments.is_empty() {
        return Err(AlpacaHistoricalCalendarError::UnsupportedOrEmptyPlan);
    }
    for pair in fragments.windows(2) {
        if pair[0].returned >= pair[1].returned
            || pair[0].returned.calendar_date() >= pair[1].returned.calendar_date()
        {
            return Err(AlpacaHistoricalCalendarError::ConflictingFragment);
        }
    }
    for fragment in fragments {
        if fragment.original_semantics.provider_timestamp()
            != fragment.returned.provider_timestamp()
            || fragment.original_semantics.timestamp_basis() != BarTimestampBasis::PeriodStart
            || fragment.original_semantics.session().kind() != MarketBarSessionKind::ProviderDefined
        {
            return Err(AlpacaHistoricalCalendarError::ConflictingFragment);
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "account generation, preflight, canonical identity, and every exact fragment are independent evidence"
)]
fn composite_session_digest(
    runtime: &AlpacaHistoricalRuntimeCapability,
    preflight_digest: EvidenceDigest,
    instrument_id: InstrumentId,
    provider_instrument_id: &ProviderInstrumentId,
    venue_id: &VenueId,
    timeframe: &SourceIdentifier,
    transport_request: &AlpacaAuthenticatedCalendarRequest,
    range_responses: &[RetainedCalendarRangeResponse],
    sessions: &[ExactCalendarSession],
    fragments: &[ExactCalendarFragment],
) -> Result<EvidenceDigest, AlpacaHistoricalCalendarError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-historical-composite-calendar/v2\0");
    hash_evidence(&mut digest, preflight_digest);
    hash_text(&mut digest, runtime.surface_id().as_str())?;
    digest.update(runtime.onboarding_session_id().as_bytes());
    digest.update(runtime.credential_generation().get().to_be_bytes());
    hash_evidence(&mut digest, runtime.account_digest());
    hash_evidence(&mut digest, runtime.public_configuration_digest());
    hash_evidence(&mut digest, runtime.runtime_evidence_digest());
    digest.update([environment_tag(runtime.trading_api_environment())]);
    digest.update(instrument_id.as_uuid().as_bytes());
    hash_text(&mut digest, provider_instrument_id.as_str())?;
    hash_text(&mut digest, venue_id.as_str())?;
    hash_text(&mut digest, timeframe.as_str())?;
    hash_range_request(&mut digest, transport_request)?;
    hash_range_responses(&mut digest, range_responses)?;
    digest.update(
        u16::try_from(sessions.len())
            .map_err(|_| AlpacaHistoricalCalendarError::ResponseBoundExceeded)?
            .to_be_bytes(),
    );
    for session in sessions {
        hash_date(&mut digest, session.date);
        digest.update([timestamp_basis_tag(
            session.series_semantics.timestamp_basis(),
        )]);
        digest.update([session_kind_tag(session.series_semantics.session().kind())]);
        hash_text(
            &mut digest,
            session.series_semantics.session().ruleset().as_str(),
        )?;
        hash_evidence(&mut digest, session.series_semantics.session().evidence());
    }
    digest.update(
        u16::try_from(fragments.len())
            .map_err(|_| AlpacaHistoricalCalendarError::ResponseBoundExceeded)?
            .to_be_bytes(),
    );
    for fragment in fragments {
        let semantics = &fragment.original_semantics;
        digest.update(fragment.returned.calendar_date().year().to_be_bytes());
        digest.update([
            fragment.returned.calendar_date().month(),
            fragment.returned.calendar_date().day(),
        ]);
        digest.update(
            fragment
                .returned
                .provider_timestamp()
                .unix_nanos()
                .to_be_bytes(),
        );
        digest.update(semantics.period_start().unix_nanos().to_be_bytes());
        digest.update(semantics.period_end_exclusive().unix_nanos().to_be_bytes());
        digest.update([timestamp_basis_tag(semantics.timestamp_basis())]);
        digest.update([session_kind_tag(semantics.session().kind())]);
        hash_text(&mut digest, semantics.session().ruleset().as_str())?;
        hash_evidence(&mut digest, semantics.session().evidence());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn calendar_retrieval_evidence(
    runtime: &AlpacaHistoricalRuntimeCapability,
    preflight_digest: EvidenceDigest,
    transport_request: &AlpacaAuthenticatedCalendarRequest,
    range_responses: &[RetainedCalendarRangeResponse],
    producer_request: &AlpacaIexUtcCalendarFetchRequest,
    row_index: u32,
    derived_body: &[u8],
) -> Result<ExactPayloadEvidence, AlpacaHistoricalCalendarError> {
    if derived_body.is_empty() || derived_body.len() > MAXIMUM_ALPACA_CALENDAR_RESPONSE_BYTES {
        return Err(AlpacaHistoricalCalendarError::ResponseBoundExceeded);
    }
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-historical-calendar-range-retrieval/v2\0");
    hash_evidence(&mut digest, preflight_digest);
    hash_text(&mut digest, runtime.surface_id().as_str())?;
    digest.update(runtime.onboarding_session_id().as_bytes());
    digest.update(runtime.credential_generation().get().to_be_bytes());
    hash_evidence(&mut digest, runtime.account_digest());
    hash_evidence(&mut digest, runtime.public_configuration_digest());
    hash_evidence(&mut digest, runtime.runtime_evidence_digest());
    digest.update([environment_tag(runtime.trading_api_environment())]);
    hash_range_request(&mut digest, transport_request)?;
    hash_range_responses(&mut digest, range_responses)?;
    hash_text(&mut digest, producer_request.method())?;
    hash_text(&mut digest, producer_request.origin())?;
    hash_text(&mut digest, producer_request.path_and_query().as_str())?;
    hash_date(&mut digest, producer_request.requested_date());
    digest.update(row_index.to_be_bytes());
    digest.update(
        u32::try_from(derived_body.len())
            .map_err(|_| AlpacaHistoricalCalendarError::ResponseBoundExceeded)?
            .to_be_bytes(),
    );
    digest.update(Sha256::digest(derived_body));
    Ok(ExactPayloadEvidence::from_content_digest(
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
    ))
}

fn hash_range_request(
    digest: &mut Sha256,
    request: &AlpacaAuthenticatedCalendarRequest,
) -> Result<(), AlpacaHistoricalCalendarError> {
    hash_text(digest, request.method())?;
    hash_text(digest, request.origin())?;
    hash_text(digest, request.path_and_query())?;
    hash_date(digest, request.start_date());
    hash_date(digest, request.end_date());
    Ok(())
}

fn hash_range_responses(
    digest: &mut Sha256,
    responses: &[RetainedCalendarRangeResponse],
) -> Result<(), AlpacaHistoricalCalendarError> {
    digest.update(
        u8::try_from(responses.len())
            .map_err(|_| AlpacaHistoricalCalendarError::ResponseBoundExceeded)?
            .to_be_bytes(),
    );
    for response in responses {
        digest.update(response.response.status().to_be_bytes());
        digest.update(
            u32::try_from(response.response.body().len())
                .map_err(|_| AlpacaHistoricalCalendarError::ResponseBoundExceeded)?
                .to_be_bytes(),
        );
        hash_evidence(digest, response.body_digest);
        match response.response.retry_after() {
            Some(retry_after) => {
                digest.update([1]);
                digest.update(
                    u16::try_from(retry_after.len())
                        .map_err(|_| AlpacaHistoricalCalendarError::ResponseBoundExceeded)?
                        .to_be_bytes(),
                );
                digest.update(retry_after);
            }
            None => digest.update([0]),
        }
        digest.update(response.received_at.unix_nanos().to_be_bytes());
    }
    Ok(())
}

fn hash_date(digest: &mut Sha256, date: CalendarDate) {
    digest.update(date.year().to_be_bytes());
    digest.update([date.month(), date.day()]);
}

const fn producer_environment(
    environment: AlpacaTradingApiEnvironment,
) -> AlpacaCalendarApiEnvironment {
    match environment {
        AlpacaTradingApiEnvironment::Live => AlpacaCalendarApiEnvironment::Live,
        AlpacaTradingApiEnvironment::Paper => AlpacaCalendarApiEnvironment::Paper,
    }
}

const fn environment_tag(environment: AlpacaTradingApiEnvironment) -> u8 {
    match environment {
        AlpacaTradingApiEnvironment::Live => 1,
        AlpacaTradingApiEnvironment::Paper => 2,
    }
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), AlpacaHistoricalCalendarError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| AlpacaHistoricalCalendarError::Allocation)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn hash_evidence(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

const fn timestamp_basis_tag(basis: BarTimestampBasis) -> u8 {
    match basis {
        BarTimestampBasis::PeriodStart => 1,
        BarTimestampBasis::PeriodEnd => 2,
    }
}

const fn session_kind_tag(kind: MarketBarSessionKind) -> u8 {
    match kind {
        MarketBarSessionKind::Regular => 1,
        MarketBarSessionKind::Extended => 2,
        MarketBarSessionKind::Continuous => 3,
        MarketBarSessionKind::ProviderDefined => 4,
    }
}

/// Closed failure classes for exact returned-date calendar composition.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaHistoricalCalendarError {
    #[error(transparent)]
    Capability(#[from] AlpacaHistoricalCapabilityError),
    #[error(transparent)]
    Adapter(#[from] AlpacaError),
    #[error(transparent)]
    Producer(#[from] crate::application::market_calendar::alpaca::AlpacaIexUtcCalendarError),
    #[error(transparent)]
    Calendar(#[from] crate::application::market_calendar::MarketCalendarError),
    #[error("the historical plan has no supported returned daily bars")]
    UnsupportedOrEmptyPlan,
    #[error("the authenticated and producer calendar request coordinates differ")]
    RequestMismatch,
    #[error("Alpaca calendar range returned HTTP status {0}")]
    RangeHttpStatus(u16),
    #[error("the Alpaca calendar range response graph is invalid")]
    InvalidRangeResponse,
    #[error("the bounded Alpaca calendar refusal retry limit was exhausted")]
    RetryLimitExceeded,
    #[error("the shared Alpaca provider-rate authority is unavailable")]
    BudgetUnavailable,
    #[error("calendar response retention crossed a code-owned bound")]
    ResponseBoundExceeded,
    #[error("duplicate or conflicting exact calendar fragments were returned")]
    ConflictingFragment,
    #[error("the exact requested calendar-session set and returned historical-bar set differ")]
    MissingReturnedSession,
    #[error("calendar identity construction failed")]
    Identity,
    #[error("calendar bounded allocation failed")]
    Allocation,
}
