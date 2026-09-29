//! Complete source acquisition, exact restart replay and original calendar/rights admission.

use super::super::unavailable_reason;
use super::{
    BenchmarkHistoryCoordinate, BenchmarkHistoryObservation, BenchmarkSourceReference, Error,
    MAX_POINTS, MarketHistoryReadCapability, SourceSeries, check, reserved,
};
use crate::application::research::corporate_actions::map_research_error;
use crate::{
    ResearchService,
    application::market_calendar::{
        CompletedMarketSessionError, CompletedMarketSessionReadCapability,
        CompletedMarketSessionReference,
    },
};
use market_squawk_data::{
    CompleteMarketBarHistoryOutput, LatestCanonicalMarketBarHistoryWindowRequest,
    MAX_RESEARCH_USE_EDGES, MAX_RESEARCH_USE_GRAPH_NODES, MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS,
    MAX_RESEARCH_USE_RETAINED_BYTES, MAX_RESEARCH_USE_SOURCES,
    MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS, MarketHistorySelectionPolicy, ResearchUse,
    ResearchUseCatalogError, ResearchUseLimits, ResearchUseRequest, Sha256Digest,
};
use market_squawk_domain::{
    Currency, InstrumentId, MarketBarAdjustment, MarketBarObservation, Timestamp,
};
use market_squawk_services::ServiceError;
use rust_decimal::Decimal;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

// These are genuine source observations, including expected-session gaps. They remain private
// and cannot become a financial comparison before the current LocalAnalysis permit is issued.
type SourcePoints = Vec<(
    BenchmarkHistoryCoordinate,
    Option<BenchmarkHistoryObservation>,
)>;

impl MarketHistoryReadCapability {
    #[allow(
        clippy::too_many_arguments,
        reason = "exact source financial and lifecycle coordinates remain independent"
    )]
    pub(super) async fn benchmark_source(
        &self,
        research: &ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        instrument: InstrumentId,
        currency: Currency,
        cutoff: Timestamp,
        observed: Timestamp,
        saved: Option<&BenchmarkSourceReference>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<SourceSeries>, Error> {
        let policy = MarketHistorySelectionPolicy::COMPLETE_DAILY_SPLIT_ADJUSTED_V1;
        let request = if let Some(saved) = saved {
            self.reader
                .exact_canonical_market_bar_history_window(
                    instrument,
                    Sha256Digest::new(saved.selected_manifest),
                    policy,
                    cutoff,
                    deadline,
                    cancellation,
                )
                .map_err(|error| unavailable_reason(&error))?
        } else {
            let request =
                LatestCanonicalMarketBarHistoryWindowRequest::try_new(instrument, policy, cutoff)
                    .map_err(|_| Error::IntegrityUnproven)?;
            self.reader
                .select_latest_canonical_market_bar_history_window(request, deadline, cancellation)
                .map_err(|error| unavailable_reason(&error))?
                .map(|value| value.into_exact_request())
        };
        let Some(request) = request else {
            return Ok(None);
        };
        let Some(output) = self
            .reader
            .read_canonical_market_bar_history(request, deadline, cancellation.clone())
            .await
            .map_err(|error| unavailable_reason(&error))?
        else {
            return Ok(None);
        };
        check(deadline, cancellation)?;
        let output = if let Some(graph) = output.selection().receipt().date_windows() {
            let retained = graph.calendar();
            let reference = CompletedMarketSessionReference::try_from_retained_digests(
                retained.origin_content_digest,
                retained.capture_binding_digest,
            )
            .map_err(calendar_error)?;
            let calendar = calendars
                .read_reference(&reference, cutoff, deadline, cancellation.clone())
                .await
                .map_err(calendar_error)?
                .ok_or(Error::StorageUnavailable)?;
            research
                .rejoin_market_history_native_sessions_with_calendar(
                    output,
                    &calendar,
                    deadline,
                    cancellation,
                )
                .await
                .map_err(|error| source_error(map_research_error(error)))?
        } else {
            research
                .rejoin_market_history_native_sessions(output, deadline, cancellation)
                .await
                .map_err(|error| source_error(map_research_error(error)))?
        };
        check(deadline, cancellation)?;
        if output.native_sessions().is_none() {
            return Ok(None);
        }
        // A valid foreign-currency source is an unavailable comparison, not an FX conversion.
        if output.selection().receipt().currency() != currency {
            return if saved.is_some() {
                Err(Error::IntegrityUnproven)
            } else {
                Ok(None)
            };
        }
        let mut roots = reserved(2)?;
        for manifest in [
            output.selection().pinned().manifest(),
            output.read_receipt().origin_manifest(),
        ] {
            if !roots.contains(manifest) {
                roots.push(manifest.clone());
            }
        }
        // Retain only the bounded original observations, then drop the complete decoded
        // history before rights traversal. No financial calculation or caller-visible output
        // occurs until LocalAnalysis authorization succeeds.
        let (reference, points) = project_source(
            output,
            instrument,
            currency,
            cutoff,
            observed,
            deadline,
            cancellation,
        )?;
        let duration = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(
                MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS,
            ));
        if duration.is_zero() {
            return Err(Error::DeadlineExceeded);
        }
        // Rights metadata is independent of price-row count. Use the data owner's full
        // 100,000-node / 400,000-edge / 100,000-source, 64 MiB ceilings, not a chart-local
        // 4 MiB ancestry restriction. The large decoded history has already been dropped;
        // only <=4096 compact original observations survive while this graph is loaded.
        let request = ResearchUseRequest::try_new(
            roots.clone(),
            ResearchUse::LocalAnalysis,
            ResearchUseLimits::try_new(
                2,
                MAX_RESEARCH_USE_GRAPH_NODES,
                MAX_RESEARCH_USE_EDGES,
                MAX_RESEARCH_USE_SOURCES,
                MAX_RESEARCH_USE_RETAINED_BYTES,
                duration,
                Duration::from_secs(MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS),
            )
            .map_err(|_| Error::IntegrityUnproven)?,
        )
        .map_err(|_| Error::IntegrityUnproven)?;
        let authorization = research
            .authorize_research_use(request, deadline, cancellation)
            .await
            .map_err(|error| source_error(map_research_error(error)))?
            .map_err(|error| match error {
                ResearchUseCatalogError::Cancelled => Error::Cancelled,
                ResearchUseCatalogError::DeadlineExceeded => Error::DeadlineExceeded,
                ResearchUseCatalogError::LimitExceeded => Error::CapacityExceeded,
                _ => Error::IntegrityUnproven,
            })?;
        check(deadline, cancellation)?;
        if authorization.research_use() != ResearchUse::LocalAnalysis
            || authorization.graph().roots().len() != roots.len()
            || roots.iter().any(|root| {
                !authorization.graph().roots().contains(root)
                    || !authorization
                        .graph()
                        .nodes()
                        .iter()
                        .any(|node| node.manifest() == root)
            })
        {
            return Err(Error::IntegrityUnproven);
        }
        Ok(Some(SourceSeries {
            reference,
            points,
            permit: authorization.into_permit(),
        }))
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "source receipts, clocks and cancellation remain explicit"
)]
fn project_source(
    output: CompleteMarketBarHistoryOutput,
    instrument: InstrumentId,
    currency: Currency,
    cutoff: Timestamp,
    observed: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(BenchmarkSourceReference, SourcePoints), Error> {
    let publication = output.selection().receipt();
    let receipt = output.read_receipt();
    let native = output.native_sessions().ok_or(Error::IntegrityUnproven)?;
    if publication.instrument_id() != instrument
        || publication.currency() != currency
        || publication.adjustment() != MarketBarAdjustment::Split
        || !publication.current_research_eligible()
        || publication.published_at() > cutoff
        || receipt.knowledge_cutoff() != cutoff
        || output.bars().len() != publication.bar_count()
        || output.bars().len() > market_squawk_sources::MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS
        || native.sessions().len()
            > market_squawk_sources::MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS
        || native.published_at() > cutoff
        || native.received_at() > cutoff
    {
        return Err(Error::IntegrityUnproven);
    }
    let mut points = reserved(native.sessions().len().min(MAX_POINTS))?;
    // The source reader bounds full rows. Retain only a suffix after inspecting every original
    // native mapping; no interior missing session is removed from this suffix.
    let eligible = native.sessions().partition_point(|session| {
        session.closes_at_exclusive() <= observed
            && session
                .provider_period()
                .is_none_or(|(_, end)| end <= observed)
    });
    let start = eligible.saturating_sub(MAX_POINTS);
    let mut bar_index = 0;
    let mut previous = None;
    for (index, session) in native.sessions().iter().enumerate() {
        check(deadline, cancellation)?;
        let coordinate = BenchmarkHistoryCoordinate {
            date: session.native_date(),
            session_close: session.closes_at_exclusive(),
        };
        if session.opens_at() >= coordinate.session_close
            || previous.is_some_and(|prior: BenchmarkHistoryCoordinate| {
                prior >= coordinate || prior.date >= coordinate.date
            })
        {
            return Err(Error::IntegrityUnproven);
        }
        previous = Some(coordinate);
        let observation = if session.bar_present() {
            let bar = output
                .bars()
                .get(bar_index)
                .ok_or(Error::IntegrityUnproven)?;
            bar_index += 1;
            validate_bar(bar, session, instrument, currency, cutoff)?;
            // Keep the original as-of boundary explicit at the actual row-retention seam.
            // completed_at is the bar's period end; validate_bar independently equates
            // that period with the retained native session's provider period.
            if (start..eligible).contains(&index)
                && bar
                    .completed_at()
                    .is_some_and(|completed| completed > observed)
            {
                return Err(Error::IntegrityUnproven);
            }
            let provenance = bar.context().provenance();
            Some(BenchmarkHistoryObservation {
                close: bar.close().amount(),
                price_index: Decimal::ZERO,
                available_at: provenance
                    .availability()
                    .conservative_available_at()
                    .ok_or(Error::IntegrityUnproven)?
                    .max(provenance.ingested_at())
                    .max(publication.published_at())
                    .max(native.received_at())
                    .max(native.published_at()),
                provider_completed_at: bar.completed_at(),
                quality: provenance.quality(),
            })
        } else {
            None
        };
        if (start..eligible).contains(&index) {
            points.push((coordinate, observation));
        }
    }
    if bar_index != output.bars().len() {
        return Err(Error::IntegrityUnproven);
    }
    let reference = BenchmarkSourceReference {
        selected_manifest: output
            .selection()
            .pinned()
            .manifest()
            .content_hash()
            .bytes(),
        origin_manifest: receipt.origin_manifest().content_hash().bytes(),
        selection_sha256: receipt.selection_digest().bytes(),
        publication_sha256: receipt.publication_receipt_digest().bytes(),
        capture_sha256: publication.capture_receipt_digest().bytes(),
        history_sha256: receipt.history_content_digest().bytes(),
        result_sha256: receipt.result_digest().bytes(),
        native_sessions_sha256: native.mapping_digest().bytes(),
    };
    Ok((reference, points))
}

fn validate_bar(
    bar: &MarketBarObservation,
    session: &market_squawk_data::RetainedHistoryNativeSession,
    instrument: InstrumentId,
    currency: Currency,
    cutoff: Timestamp,
) -> Result<(), Error> {
    let provenance = bar.context().provenance();
    let available = provenance
        .availability()
        .conservative_available_at()
        .ok_or(Error::IntegrityUnproven)?;
    let coordinate_matches = if let Some(date) = bar.time_semantics().nominal_daily_date() {
        session.provider_timestamp().is_none()
            && session.provider_period().is_none()
            && date.date() == session.native_date()
            && bar.context().time().effective().calendar_date_value() == Some(date.date())
            && session.closes_at_exclusive() <= available
    } else {
        bar.time_semantics()
            .timestamped_period()
            .is_some_and(|period| {
                session.provider_period()
                    == Some((period.period_start(), period.period_end_exclusive()))
                    && period.period_end_exclusive() <= available
            })
    };
    if !coordinate_matches
        || provenance.instrument_id() != Some(instrument)
        || bar.currency() != currency
        || bar.adjustment() != MarketBarAdjustment::Split
        || bar.close().amount() <= Decimal::ZERO
        || available > cutoff
        || provenance.ingested_at() > cutoff
    {
        return Err(Error::IntegrityUnproven);
    }
    Ok(())
}

fn calendar_error(error: CompletedMarketSessionError) -> Error {
    match error {
        CompletedMarketSessionError::Cancelled => Error::Cancelled,
        CompletedMarketSessionError::DeadlineExceeded => Error::DeadlineExceeded,
        CompletedMarketSessionError::ResourceBoundExceeded => Error::CapacityExceeded,
        CompletedMarketSessionError::Unavailable => Error::StorageUnavailable,
        CompletedMarketSessionError::InvalidRequest
        | CompletedMarketSessionError::InvalidEvidence => Error::IntegrityUnproven,
    }
}
fn source_error(error: ServiceError) -> Error {
    match error {
        ServiceError::Cancelled => Error::Cancelled,
        ServiceError::DeadlineExceeded => Error::DeadlineExceeded,
        ServiceError::ResourceExhausted => Error::CapacityExceeded,
        ServiceError::Unavailable | ServiceError::NotFound => Error::StorageUnavailable,
        _ => Error::IntegrityUnproven,
    }
}
