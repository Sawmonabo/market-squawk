//! Actual first history lookup for the fixed source-owned benchmark and annual premium.

use super::super::{RecommendationBenchmarkSelectionReadCapability, map_read_error};
use super::*;
use crate::application::market_calendar::{
    CompletedMarketSessionReadCapability, CompletedMarketSessionReference,
};
use market_squawk_data::{
    CompleteMarketBarHistoryOutput, LatestCanonicalMarketBarHistoryWindowRequest,
    MarketHistorySelectionPolicy,
};

impl MacroContextReadCapability {
    /// Selects the owner-fixed SPY/VTI pair through the existing actual instrument catalog before
    /// reading price outcomes. A caller with the original workflow benchmark receipt should use
    /// `read_selected_default_equity_premium` after its exact source-owned reference reopen.
    pub(crate) async fn read_default_equity_premium_from_store(
        &self,
        research: &crate::ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        knowledge_cutoff: Timestamp,
        effective_at: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<HistoricalEquityPremiumRead, EquityPremiumReadError> {
        let benchmarks =
            RecommendationBenchmarkSelectionReadCapability::new(research.market_data_instruments());
        let selected = research
            .run_owned_research_io(deadline, &cancellation, move |worker_cancellation| {
                benchmarks.select(
                    knowledge_cutoff,
                    effective_at,
                    deadline,
                    &worker_cancellation,
                )
            })
            .await?;
        check_selection_control(deadline, &cancellation)?;
        let benchmark = selected?.ok_or(EquityPremiumUnavailable::BenchmarkSelectionMissing)?;
        self.read_selected_default_equity_premium(
            research,
            calendars,
            benchmark,
            deadline,
            cancellation,
        )
        .await
    }

    /// Genuine lookup and physical source replay, not a placeholder unavailable argument. The
    /// primary instrument comes only from the existing sealed fixed benchmark selection. A source
    /// acquisition must complete before this selection's original analytical cutoff is frozen;
    /// this read never widens that cutoff to admit later acquired information.
    pub(crate) async fn read_selected_default_equity_premium(
        &self,
        research: &crate::ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        benchmark: RecommendationBenchmarkSelection,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<HistoricalEquityPremiumRead, EquityPremiumReadError> {
        check_selection_control(deadline, &cancellation)?;
        let knowledge_cutoff = benchmark.knowledge_at();
        let (start, end) = required_annual_source_dates(knowledge_cutoff)?;
        let history = read_annual_source_superset(
            research,
            benchmark.primary().instrument_id(),
            knowledge_cutoff,
            start,
            end,
            deadline,
            &cancellation,
        )
        .await?;
        check_selection_control(deadline, &cancellation)?;
        let source = rejoin_source_with_original_calendar(
            research,
            calendars,
            history,
            deadline,
            &cancellation,
        )
        .await?;
        self.read_default_equity_premium(research, source, benchmark, deadline, cancellation)
            .await
    }
}

/// Fixed inclusive source query, selected independently of observed return outcomes. The source
/// expected-session owner must prove its actual final annual closing dates inside this window.
pub(crate) fn required_annual_source_dates(
    cutoff: Timestamp,
) -> Result<(CalendarDate, CalendarDate), EquityPremiumUnavailable> {
    let mismatch = EquityPremiumUnavailable::IncompleteTenYearHistory;
    let year = calendar_date(cutoff)?.year();
    let opening = year
        .checked_sub((market_squawk_valuation::EQUITY_PREMIUM_SAMPLE_YEARS + 1) as u16)
        .ok_or(mismatch)?;
    Ok((
        CalendarDate::new(opening, 12, 24).map_err(|_| mismatch)?,
        CalendarDate::new(year - 1, 12, 31).map_err(|_| mismatch)?,
    ))
}

pub(in crate::application::research) fn check_selection_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}

/// An original calendar locator is reopened through its genuine source capability before the
/// data owner can mint native membership. A structurally valid graph alone is not this proof.
pub(super) async fn rejoin_source_with_original_calendar(
    research: &crate::ResearchService,
    calendars: &CompletedMarketSessionReadCapability,
    history: CompleteMarketBarHistoryOutput,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<TiingoCompletedEodActionRead, EquityPremiumReadError> {
    check_selection_control(deadline, cancellation)?;
    let original = history
        .selection()
        .receipt()
        .date_windows()
        .ok_or(EquityPremiumUnavailable::NativeDateAuthorityMissing)?
        .calendar();
    let reference = CompletedMarketSessionReference::try_from_retained_digests(
        original.origin_content_digest,
        original.capture_binding_digest,
    )
    .map_err(|_| EquityPremiumUnavailable::SourceIdentityMismatch)?;
    let calendar = calendars
        .read_reference(
            &reference,
            history.read_receipt().knowledge_cutoff(),
            deadline,
            cancellation.child_token(),
        )
        .await?
        .ok_or(EquityPremiumUnavailable::OriginalCalendarPublicationMissing)?;
    check_selection_control(deadline, cancellation)?;
    let history = research
        .rejoin_market_history_native_sessions_with_calendar(
            history,
            &calendar,
            deadline,
            cancellation,
        )
        .await?;
    let source = research
        .rejoin_tiingo_eod_history_actions(history, deadline, cancellation)
        .await?;
    check_selection_control(deadline, cancellation)?;
    Ok(source)
}

/// Select the genuine completed source window, then check that it contains the predeclared
/// annual endpoints. The exact manifest-pinned request is read intact; no subset publication,
/// calendar receipt, source date or knowledge cutoff is fabricated.
pub(super) async fn read_annual_source_superset(
    research: &crate::ResearchService,
    instrument: market_squawk_domain::InstrumentId,
    cutoff: Timestamp,
    start: CalendarDate,
    end: CalendarDate,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CompleteMarketBarHistoryOutput, EquityPremiumReadError> {
    check_selection_control(deadline, cancellation)?;
    let reader = research.analytical_reader();
    let request = LatestCanonicalMarketBarHistoryWindowRequest::try_new(
        instrument,
        MarketHistorySelectionPolicy::COMPLETE_DAILY_RAW_V1,
        cutoff,
    )
    .map_err(|_| EquityPremiumUnavailable::SourceIdentityMismatch)?;
    let selection = research
        .run_owned_research_io(deadline, cancellation, move |token| {
            reader.select_latest_canonical_market_bar_history_window(request, deadline, &token)
        })
        .await?
        .map_err(map_read_error)?
        .ok_or(EquityPremiumUnavailable::AnnualHistoryNotPublished)?;
    let (source_start, source_end) = selection
        .requested_dates()
        .ok_or(EquityPremiumUnavailable::NativeDateAuthorityMissing)?;
    if source_start > start
        || source_end < end
        || selection.knowledge_cutoff() != cutoff
        || selection.instrument_id() != instrument
    {
        return Err(EquityPremiumUnavailable::IncompleteTenYearHistory.into());
    }
    let exact = selection.into_exact_request();
    let expected_manifest = exact
        .exact_manifest()
        .ok_or(EquityPremiumUnavailable::SourceIdentityMismatch)?
        .clone();
    let history = research
        .analytical_reader()
        .read_canonical_market_bar_history(exact, deadline, cancellation.child_token())
        .await
        .map_err(map_read_error)?
        .ok_or(EquityPremiumUnavailable::AnnualHistoryNotPublished)?;
    if history.selection().pinned().manifest() != &expected_manifest
        || history.read_receipt().knowledge_cutoff() != cutoff
    {
        return Err(EquityPremiumUnavailable::SourceIdentityMismatch.into());
    }
    check_selection_control(deadline, cancellation)?;
    Ok(history)
}
