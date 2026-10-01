//! Provider-neutral, bounded complete-market-history reads for product consumers.
//!
//! This leaf delegates source selection to the durable analytical catalog. Callers name only a
//! canonical instrument and financial-series policy; provider, native symbol, feed, manifest, and
//! capture coordinates remain sealed inside [`MarketHistoryEvidenceReceipt`].

mod harmonic;
mod native_sessions;
pub(crate) use native_sessions::NativeSessionHistory;
mod previous_close;
mod viewport;
pub(crate) use harmonic::HarmonicHistoryEvaluation;
pub(crate) use viewport::MarketHistoryViewport;

pub(crate) mod benchmark;

use std::{fmt, num::NonZeroU32, time::Instant};

use market_squawk_data::{
    AnalyticalReadCapability, AnalyticalReadError, CanonicalMarketBarHistoryRequest,
    CompleteMarketBarHistoryOutput, DatasetManifestRef,
    LatestCanonicalMarketBarHistoryWindowRequest, ManifestCatalogError,
    MarketHistorySelectionPolicy, ParquetStoreError, QueryError, Sha256Digest,
};
use market_squawk_domain::{
    BarTimeSemantics, CalendarDate, Currency, DataQuality, InstrumentId, MarketBarAdjustment,
    MarketBarObservation, Money, ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp,
    VenueId,
};
use market_squawk_sources::MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS;
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

/// Hard output ceiling inherited from complete-history publication.
pub(crate) const MAX_MARKET_HISTORY_BARS: u32 = MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS as u32;

/// Financial range at its authentic source precision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketHistoryInterval {
    Timestamped {
        start: Timestamp,
        end_exclusive: Timestamp,
    },
    NominalDates {
        start: CalendarDate,
        end_inclusive: CalendarDate,
    },
}

impl MarketHistoryInterval {
    pub(crate) fn try_new(
        start: Timestamp,
        end_exclusive: Timestamp,
    ) -> Result<Self, MarketHistoryRequestError> {
        if start >= end_exclusive {
            return Err(MarketHistoryRequestError::InvalidInterval);
        }
        Ok(Self::Timestamped {
            start,
            end_exclusive,
        })
    }

    pub(crate) fn try_nominal_dates(
        start: CalendarDate,
        end_inclusive: CalendarDate,
    ) -> Result<Self, MarketHistoryRequestError> {
        if start > end_inclusive {
            return Err(MarketHistoryRequestError::InvalidInterval);
        }
        Ok(Self::NominalDates {
            start,
            end_inclusive,
        })
    }

    pub(crate) const fn start(self) -> Option<Timestamp> {
        match self {
            Self::Timestamped { start, .. } => Some(start),
            Self::NominalDates { .. } => None,
        }
    }

    pub(crate) const fn end_exclusive(self) -> Option<Timestamp> {
        match self {
            Self::Timestamped { end_exclusive, .. } => Some(end_exclusive),
            Self::NominalDates { .. } => None,
        }
    }

    pub(crate) const fn nominal_dates(self) -> Option<(CalendarDate, CalendarDate)> {
        match self {
            Self::NominalDates {
                start,
                end_inclusive,
            } => Some((start, end_inclusive)),
            Self::Timestamped { .. } => None,
        }
    }

    /// Timestamped provider bounds are inclusive; a nominal source retains its own date bounds.
    fn provider_range(self) -> Option<(Timestamp, Timestamp)> {
        match self {
            Self::Timestamped {
                start,
                end_exclusive,
            } => Some((
                start,
                Timestamp::from_unix_nanos(end_exclusive.unix_nanos() - 1),
            )),
            Self::NominalDates { .. } => None,
        }
    }

    fn ended_by(self, cutoff: Timestamp) -> bool {
        match self {
            Self::Timestamped { end_exclusive, .. } => end_exclusive <= cutoff,
            Self::NominalDates { end_inclusive, .. } => {
                // This only bounds a date query; it does not assign an instant to a source day.
                cutoff
                    .utc_calendar_date()
                    .is_ok_and(|date| end_inclusive <= date)
            }
        }
    }

    fn contains(self, time: &BarTimeSemantics) -> bool {
        match self {
            Self::Timestamped {
                start,
                end_exclusive,
            } => time.timestamped_period().is_some_and(|period| {
                period.period_start() >= start && period.period_end_exclusive() <= end_exclusive
            }),
            Self::NominalDates {
                start,
                end_inclusive,
            } => time
                .nominal_daily_date()
                .is_some_and(|date| date.date() >= start && date.date() <= end_inclusive),
        }
    }

    fn from_bars(first: &MarketBarObservation, last: &MarketBarObservation) -> Option<Self> {
        match (first.time_semantics(), last.time_semantics()) {
            (
                BarTimeSemantics::TimestampedPeriod(first),
                BarTimeSemantics::TimestampedPeriod(last),
            ) => Self::try_new(first.period_start(), last.period_end_exclusive()).ok(),
            (
                BarTimeSemantics::NominalDailyDate(first),
                BarTimeSemantics::NominalDailyDate(last),
            ) => Self::try_nominal_dates(first.date(), last.date()).ok(),
            _ => None,
        }
    }
}

/// Product-level bar duration. It deliberately carries no provider-native interval string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketHistoryTimeframe {
    Daily,
}

/// Product-level session policy. Calendar ownership remains below this boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketHistorySessionPolicy {
    /// Include every session proven complete by the selected market calendar.
    CompletedTradingSessions,
}

/// Corporate-action treatment requested by the financial consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketHistoryAdjustmentPolicy {
    Unadjusted,
    SplitAdjusted,
    DividendAdjusted,
    SpinOffAdjusted,
    FullyAdjusted,
}

impl MarketHistoryAdjustmentPolicy {
    fn selection_policy(self) -> Option<MarketHistorySelectionPolicy> {
        match self {
            Self::Unadjusted => Some(MarketHistorySelectionPolicy::COMPLETE_DAILY_RAW_V1),
            Self::SplitAdjusted => {
                Some(MarketHistorySelectionPolicy::COMPLETE_DAILY_SPLIT_ADJUSTED_V1)
            }
            Self::FullyAdjusted => Some(MarketHistorySelectionPolicy::COMPLETE_DAILY_ADJUSTED_V1),
            _ => None,
        }
    }

    const fn canonical(self) -> MarketBarAdjustment {
        match self {
            Self::Unadjusted => MarketBarAdjustment::Raw,
            Self::SplitAdjusted => MarketBarAdjustment::Split,
            Self::DividendAdjusted => MarketBarAdjustment::Dividend,
            Self::SpinOffAdjusted => MarketBarAdjustment::SpinOff,
            Self::FullyAdjusted => MarketBarAdjustment::All,
        }
    }
}

/// Nonzero result ceiling under the durable complete-history maximum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MarketHistoryReadLimit(NonZeroU32);

impl MarketHistoryReadLimit {
    pub(crate) fn try_new(value: u32) -> Result<Self, MarketHistoryRequestError> {
        NonZeroU32::new(value)
            .filter(|value| value.get() <= MAX_MARKET_HISTORY_BARS)
            .map(Self)
            .ok_or(MarketHistoryRequestError::InvalidLimit)
    }

    const fn get(self) -> usize {
        self.0.get() as usize
    }
}

/// Provider-neutral immutable input to one complete-history selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketHistoryReadRequest {
    instrument_id: InstrumentId,
    interval: MarketHistoryInterval,
    timeframe: MarketHistoryTimeframe,
    session: MarketHistorySessionPolicy,
    adjustment: MarketHistoryAdjustmentPolicy,
    knowledge_cutoff: Timestamp,
    limit: MarketHistoryReadLimit,
}

/// Provider-neutral request for the latest immutable complete financial window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LatestMarketHistoryReadRequest {
    instrument_id: InstrumentId,
    timeframe: MarketHistoryTimeframe,
    session: MarketHistorySessionPolicy,
    adjustment: MarketHistoryAdjustmentPolicy,
    knowledge_cutoff: Timestamp,
    limit: MarketHistoryReadLimit,
}

impl LatestMarketHistoryReadRequest {
    pub(crate) const fn new(
        instrument_id: InstrumentId,
        timeframe: MarketHistoryTimeframe,
        session: MarketHistorySessionPolicy,
        adjustment: MarketHistoryAdjustmentPolicy,
        knowledge_cutoff: Timestamp,
        limit: MarketHistoryReadLimit,
    ) -> Self {
        Self {
            instrument_id,
            timeframe,
            session,
            adjustment,
            knowledge_cutoff,
            limit,
        }
    }

    fn supported_by_current_catalog_policy(&self) -> bool {
        self.timeframe == MarketHistoryTimeframe::Daily
            && self.session == MarketHistorySessionPolicy::CompletedTradingSessions
            && self.adjustment.selection_policy().is_some()
    }
}

impl MarketHistoryReadRequest {
    #[allow(
        clippy::too_many_arguments,
        reason = "canonical identity, financial policy, PIT cutoff, and output bound stay explicit"
    )]
    pub(crate) fn try_new(
        instrument_id: InstrumentId,
        interval: MarketHistoryInterval,
        timeframe: MarketHistoryTimeframe,
        session: MarketHistorySessionPolicy,
        adjustment: MarketHistoryAdjustmentPolicy,
        knowledge_cutoff: Timestamp,
        limit: MarketHistoryReadLimit,
    ) -> Result<Self, MarketHistoryRequestError> {
        if !interval.ended_by(knowledge_cutoff) {
            return Err(MarketHistoryRequestError::KnowledgeBeforeIntervalEnd);
        }
        Ok(Self {
            instrument_id,
            interval,
            timeframe,
            session,
            adjustment,
            knowledge_cutoff,
            limit,
        })
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn interval(&self) -> MarketHistoryInterval {
        self.interval
    }

    pub(crate) const fn timeframe(&self) -> MarketHistoryTimeframe {
        self.timeframe
    }

    pub(crate) const fn session(&self) -> MarketHistorySessionPolicy {
        self.session
    }

    pub(crate) const fn adjustment(&self) -> MarketHistoryAdjustmentPolicy {
        self.adjustment
    }

    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    fn supported_by_current_catalog_policy(&self) -> bool {
        self.timeframe == MarketHistoryTimeframe::Daily
            && self.session == MarketHistorySessionPolicy::CompletedTradingSessions
            && self.adjustment.selection_policy().is_some()
    }
}

/// Pure request validation failure; no source was consulted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketHistoryRequestError {
    InvalidInterval,
    KnowledgeBeforeIntervalEnd,
    InvalidLimit,
}

/// Exact provider-neutral OHLCV value returned to financial consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketHistoryBar {
    time_semantics: BarTimeSemantics,
    open: Money,
    high: Money,
    low: Money,
    close: Money,
    volume: Decimal,
    trade_count: Option<u64>,
    vwap: Option<Money>,
}

impl MarketHistoryBar {
    pub(crate) const fn period_start(&self) -> Option<Timestamp> {
        self.time_semantics.period_start()
    }

    pub(crate) const fn period_end_exclusive(&self) -> Option<Timestamp> {
        self.time_semantics.period_end_exclusive()
    }

    pub(crate) const fn nominal_date(&self) -> Option<CalendarDate> {
        match self.time_semantics.nominal_daily_date() {
            Some(date) => Some(date.date()),
            None => None,
        }
    }

    pub(crate) const fn time_semantics(&self) -> &BarTimeSemantics {
        &self.time_semantics
    }

    pub(crate) const fn open(&self) -> Money {
        self.open
    }

    pub(crate) const fn high(&self) -> Money {
        self.high
    }

    pub(crate) const fn low(&self) -> Money {
        self.low
    }

    pub(crate) const fn close(&self) -> Money {
        self.close
    }

    pub(crate) const fn volume(&self) -> Decimal {
        self.volume
    }

    pub(crate) const fn trade_count(&self) -> Option<u64> {
        self.trade_count
    }

    pub(crate) const fn vwap(&self) -> Option<Money> {
        self.vwap
    }
}

/// Requested, materialized, and returned financial coverage without source plumbing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MarketHistoryCoverage {
    requested: MarketHistoryInterval,
    materialized: MarketHistoryInterval,
    returned: MarketHistoryInterval,
    materialized_bars: usize,
    returned_bars: usize,
}

impl MarketHistoryCoverage {
    pub(crate) const fn requested(&self) -> MarketHistoryInterval {
        self.requested
    }

    pub(crate) const fn materialized(&self) -> MarketHistoryInterval {
        self.materialized
    }

    pub(crate) const fn returned(&self) -> MarketHistoryInterval {
        self.returned
    }

    pub(crate) const fn materialized_bars(&self) -> usize {
        self.materialized_bars
    }

    pub(crate) const fn returned_bars(&self) -> usize {
        self.returned_bars
    }
}

/// Financial quality and admissible use of the selected immutable history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MarketHistoryQuality {
    observation_quality: DataQuality,
    complete_trading_sessions: bool,
    current_research_eligible: bool,
    point_in_time_backtest_eligible: bool,
    retrospective_training_eligible: bool,
    realized_outcome_eligible: bool,
}

impl MarketHistoryQuality {
    pub(crate) const fn observation_quality(self) -> DataQuality {
        self.observation_quality
    }

    pub(crate) const fn complete_trading_sessions(self) -> bool {
        self.complete_trading_sessions
    }

    pub(crate) const fn current_research_eligible(self) -> bool {
        self.current_research_eligible
    }

    pub(crate) const fn point_in_time_backtest_eligible(self) -> bool {
        self.point_in_time_backtest_eligible
    }

    pub(crate) const fn retrospective_training_eligible(self) -> bool {
        self.retrospective_training_eligible
    }

    pub(crate) const fn realized_outcome_eligible(self) -> bool {
        self.realized_outcome_eligible
    }
}

/// Opaque lineage retained for audit and downstream immutable-input binding.
///
/// No field is serializable or exposed to ordinary product projections. The private coordinates
/// intentionally include provider/source identity so Settings and logs can audit the exact read.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct MarketHistoryEvidenceReceipt {
    source_id: SourceId,
    provider_dataset: SourceIdentifier,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    native_interval: SourceIdentifier,
    selected_manifest: DatasetManifestRef,
    origin_manifest: DatasetManifestRef,
    publication_receipt_digest: Sha256Digest,
    capture_receipt_digest: Sha256Digest,
    capture_graph_digests: (Sha256Digest, Sha256Digest),
    selection_digest: Sha256Digest,
    history_content_digest: Sha256Digest,
    result_digest: Sha256Digest,
    projected_start_ordinal: usize,
    projected_bar_count: usize,
}

impl MarketHistoryEvidenceReceipt {
    /// Stable verified result identity for downstream immutable-input binding.
    pub(crate) const fn verified_result_digest(&self) -> Sha256Digest {
        self.result_digest
    }
}

impl fmt::Debug for MarketHistoryEvidenceReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketHistoryEvidenceReceipt")
            .field("lineage", &"[OPAQUE VERIFIED HISTORY EVIDENCE]")
            .finish()
    }
}

/// Typed financial history ready for product or analytical consumers.
#[derive(Debug)]
pub(crate) struct MarketHistorySeries {
    instrument_id: InstrumentId,
    timeframe: MarketHistoryTimeframe,
    session: MarketHistorySessionPolicy,
    adjustment: MarketHistoryAdjustmentPolicy,
    currency: Currency,
    knowledge_cutoff: Timestamp,
    coverage: MarketHistoryCoverage,
    quality: MarketHistoryQuality,
    bars: Box<[MarketHistoryBar]>,
    evidence: MarketHistoryEvidenceReceipt,
}

impl MarketHistorySeries {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn timeframe(&self) -> MarketHistoryTimeframe {
        self.timeframe
    }

    pub(crate) const fn session(&self) -> MarketHistorySessionPolicy {
        self.session
    }

    pub(crate) const fn adjustment(&self) -> MarketHistoryAdjustmentPolicy {
        self.adjustment
    }

    pub(crate) const fn currency(&self) -> Currency {
        self.currency
    }

    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub(crate) const fn coverage(&self) -> MarketHistoryCoverage {
        self.coverage
    }

    pub(crate) const fn quality(&self) -> MarketHistoryQuality {
        self.quality
    }

    pub(crate) fn bars(&self) -> &[MarketHistoryBar] {
        &self.bars
    }

    pub(crate) const fn evidence(&self) -> &MarketHistoryEvidenceReceipt {
        &self.evidence
    }
}

/// Honest bounded result of one provider-neutral durable history read.
#[derive(Debug)]
pub(crate) enum MarketHistoryReadOutcome {
    Complete(MarketHistorySeries),
    Partial {
        series: MarketHistorySeries,
        reason: MarketHistoryPartialReason,
    },
    Missing(MarketHistoryMissingReason),
    Unavailable(MarketHistoryUnavailableReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketHistoryPartialReason {
    OutputLimit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketHistoryMissingReason {
    PolicyNotMaterialized,
    NoCompleteWindowAtKnowledgeCutoff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketHistoryUnavailableReason {
    Cancelled,
    DeadlineExceeded,
    CapacityExceeded,
    StorageUnavailable,
    IntegrityUnproven,
}

/// Read-only provider-neutral history capability over the existing rich analytical store.
///
/// Multiple installed sources remain catalog inputs, not application routes. The data-owned
/// canonical selector resolves source evidence under its versioned policy; this capability never
/// names a preferred provider or constructs a second store.
#[derive(Clone)]
pub(crate) struct MarketHistoryReadCapability {
    reader: AnalyticalReadCapability,
}

impl MarketHistoryReadCapability {
    pub(crate) const fn new(reader: AnalyticalReadCapability) -> Self {
        Self { reader }
    }

    pub(crate) async fn read(
        &self,
        request: MarketHistoryReadRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> MarketHistoryReadOutcome {
        if !request.supported_by_current_catalog_policy() {
            return MarketHistoryReadOutcome::Missing(
                MarketHistoryMissingReason::PolicyNotMaterialized,
            );
        }
        match self
            .read_complete_output(&request, deadline, cancellation)
            .await
        {
            Ok(Some(output)) => project_output(output, request),
            Ok(None) => MarketHistoryReadOutcome::Missing(
                MarketHistoryMissingReason::NoCompleteWindowAtKnowledgeCutoff,
            ),
            Err(error) => MarketHistoryReadOutcome::Unavailable(error),
        }
    }

    /// Returns the existing sealed complete raw receipt directly to financial outcome admission.
    /// It neither projects caller-authored bars nor qualifies these later-acquired outcomes as
    /// historical signal information. The same canonical source selector owns this read.
    pub(crate) async fn read_raw_outcomes(
        &self,
        request: MarketHistoryReadRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<CompleteMarketBarHistoryOutput>, MarketHistoryUnavailableReason> {
        if request.adjustment != MarketHistoryAdjustmentPolicy::Unadjusted
            || !request.supported_by_current_catalog_policy()
        {
            return Err(MarketHistoryUnavailableReason::IntegrityUnproven);
        }
        let output = self
            .read_complete_output(&request, deadline, cancellation)
            .await?;
        if let Some(output) = &output {
            if !output.selection().receipt().realized_outcome_eligible() {
                return Err(MarketHistoryUnavailableReason::IntegrityUnproven);
            }
            if output.bars().len() > request.limit.get() {
                return Err(MarketHistoryUnavailableReason::CapacityExceeded);
            }
        }
        Ok(output)
    }

    async fn read_complete_output(
        &self,
        request: &MarketHistoryReadRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<CompleteMarketBarHistoryOutput>, MarketHistoryUnavailableReason> {
        let policy = request
            .adjustment
            .selection_policy()
            .ok_or(MarketHistoryUnavailableReason::IntegrityUnproven)?;
        let selection = match request.interval {
            MarketHistoryInterval::Timestamped {
                start,
                end_exclusive,
            } => CanonicalMarketBarHistoryRequest::try_latest(
                request.instrument_id(),
                start,
                Timestamp::from_unix_nanos(end_exclusive.unix_nanos() - 1),
                policy,
                request.knowledge_cutoff(),
            ),
            MarketHistoryInterval::NominalDates {
                start,
                end_inclusive,
            } => CanonicalMarketBarHistoryRequest::try_latest_nominal(
                request.instrument_id(),
                start,
                end_inclusive,
                policy,
                request.knowledge_cutoff(),
            ),
        }
        .map_err(|_| MarketHistoryUnavailableReason::IntegrityUnproven)?;
        self.reader
            .read_canonical_market_bar_history(selection, deadline, cancellation)
            .await
            .map_err(|error| unavailable_reason(&error))
    }

    /// Selects and reads the latest complete immutable window known at the trusted cutoff.
    pub(crate) async fn read_latest(
        &self,
        request: LatestMarketHistoryReadRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> MarketHistoryReadOutcome {
        if !request.supported_by_current_catalog_policy() {
            return MarketHistoryReadOutcome::Missing(
                MarketHistoryMissingReason::PolicyNotMaterialized,
            );
        }
        let Some(policy) = request.adjustment.selection_policy() else {
            return MarketHistoryReadOutcome::Missing(
                MarketHistoryMissingReason::PolicyNotMaterialized,
            );
        };
        let lookup = match LatestCanonicalMarketBarHistoryWindowRequest::try_new(
            request.instrument_id,
            policy,
            request.knowledge_cutoff,
        ) {
            Ok(lookup) => lookup,
            Err(_error) => {
                return MarketHistoryReadOutcome::Unavailable(
                    MarketHistoryUnavailableReason::IntegrityUnproven,
                );
            }
        };
        let selection = match self
            .reader
            .select_latest_canonical_market_bar_history_window(lookup, deadline, &cancellation)
        {
            Ok(Some(selection)) => selection,
            Ok(None) => {
                return MarketHistoryReadOutcome::Missing(
                    MarketHistoryMissingReason::NoCompleteWindowAtKnowledgeCutoff,
                );
            }
            Err(error) => {
                return MarketHistoryReadOutcome::Unavailable(unavailable_reason(&error));
            }
        };
        let interval = match (selection.requested_range(), selection.requested_dates()) {
            (Some((start, end_inclusive)), None) => end_inclusive
                .checked_add_nanos(1)
                .ok()
                .and_then(|end_exclusive| {
                    MarketHistoryInterval::try_new(start, end_exclusive).ok()
                }),
            (None, Some((start, end_inclusive))) => {
                MarketHistoryInterval::try_nominal_dates(start, end_inclusive).ok()
            }
            _ => None,
        };
        let Some(interval) = interval else {
            return MarketHistoryReadOutcome::Unavailable(
                MarketHistoryUnavailableReason::IntegrityUnproven,
            );
        };
        let exact_request = match MarketHistoryReadRequest::try_new(
            request.instrument_id,
            interval,
            request.timeframe,
            request.session,
            request.adjustment,
            request.knowledge_cutoff,
            request.limit,
        ) {
            Ok(request) => request,
            Err(_) => {
                return MarketHistoryReadOutcome::Unavailable(
                    MarketHistoryUnavailableReason::IntegrityUnproven,
                );
            }
        };
        match self
            .reader
            .read_canonical_market_bar_history(
                selection.into_exact_request(),
                deadline,
                cancellation,
            )
            .await
        {
            Ok(Some(output)) => project_output(output, exact_request),
            Ok(None) => MarketHistoryReadOutcome::Missing(
                MarketHistoryMissingReason::NoCompleteWindowAtKnowledgeCutoff,
            ),
            Err(error) => MarketHistoryReadOutcome::Unavailable(unavailable_reason(&error)),
        }
    }
}

impl fmt::Debug for MarketHistoryReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketHistoryReadCapability")
            .field("reader", &"[DURABLE PROVIDER-NEUTRAL HISTORY READER]")
            .finish()
    }
}

fn project_output(
    output: CompleteMarketBarHistoryOutput,
    request: MarketHistoryReadRequest,
) -> MarketHistoryReadOutcome {
    let publication = output.selection().receipt();
    let bars = output.bars();
    let requested = request.interval();
    let expected_adjustment = request.adjustment().canonical();
    let valid = !bars.is_empty()
        && bars.len() == publication.bar_count()
        && bars.len() <= MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS
        && publication.instrument_id() == request.instrument_id()
        && publication.requested_range() == requested.provider_range()
        && publication.requested_dates() == requested.nominal_dates()
        && publication.current_research_eligible()
        && !publication.point_in_time_eligible()
        && !publication.backtest_eligible()
        && bars.iter().all(|bar| {
            let period = bar.time_semantics();
            bar.context().provenance().instrument_id() == Some(request.instrument_id())
                && bar.adjustment() == expected_adjustment
                && requested.contains(period)
                && bar
                    .context()
                    .provenance()
                    .availability()
                    .conservative_available_at()
                    .is_some_and(|available_at| available_at <= request.knowledge_cutoff())
        });
    if !valid {
        return MarketHistoryReadOutcome::Unavailable(
            MarketHistoryUnavailableReason::IntegrityUnproven,
        );
    }

    let quality = bars[0].context().provenance().quality();
    let currency = bars[0].currency();
    if bars
        .iter()
        .any(|bar| bar.context().provenance().quality() != quality || bar.currency() != currency)
    {
        return MarketHistoryReadOutcome::Unavailable(
            MarketHistoryUnavailableReason::IntegrityUnproven,
        );
    }

    let start_ordinal = bars.len().saturating_sub(request.limit.get());
    let selected = &bars[start_ordinal..];
    let Some(materialized_first) = bars.first() else {
        return MarketHistoryReadOutcome::Unavailable(
            MarketHistoryUnavailableReason::IntegrityUnproven,
        );
    };
    let Some(materialized_last) = bars.last() else {
        return MarketHistoryReadOutcome::Unavailable(
            MarketHistoryUnavailableReason::IntegrityUnproven,
        );
    };
    let Some(returned_first) = selected.first() else {
        return MarketHistoryReadOutcome::Unavailable(
            MarketHistoryUnavailableReason::IntegrityUnproven,
        );
    };
    let Some(returned_last) = selected.last() else {
        return MarketHistoryReadOutcome::Unavailable(
            MarketHistoryUnavailableReason::IntegrityUnproven,
        );
    };

    let projected = selected
        .iter()
        .map(|bar| MarketHistoryBar {
            time_semantics: bar.time_semantics().clone(),
            open: bar.open(),
            high: bar.high(),
            low: bar.low(),
            close: bar.close(),
            volume: bar.volume(),
            trade_count: bar.trade_count(),
            vwap: bar.vwap(),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let (Some(materialized), Some(returned)) = (
        MarketHistoryInterval::from_bars(materialized_first, materialized_last),
        MarketHistoryInterval::from_bars(returned_first, returned_last),
    ) else {
        return MarketHistoryReadOutcome::Unavailable(
            MarketHistoryUnavailableReason::IntegrityUnproven,
        );
    };
    let coverage = MarketHistoryCoverage {
        requested,
        materialized,
        returned,
        materialized_bars: bars.len(),
        returned_bars: projected.len(),
    };
    let read_receipt = output.read_receipt();
    let evidence = MarketHistoryEvidenceReceipt {
        source_id: publication.source_id().clone(),
        provider_dataset: publication.provider_dataset().clone(),
        provider_instrument_id: publication.provider_instrument_id().clone(),
        venue_id: publication.venue_id().clone(),
        feed: publication.feed().clone(),
        native_interval: publication.interval().clone(),
        selected_manifest: output.selection().pinned().manifest().clone(),
        origin_manifest: read_receipt.origin_manifest().clone(),
        publication_receipt_digest: read_receipt.publication_receipt_digest(),
        capture_receipt_digest: publication.capture_receipt_digest(),
        capture_graph_digests: publication.capture_graph_digests(),
        selection_digest: read_receipt.selection_digest(),
        history_content_digest: read_receipt.history_content_digest(),
        result_digest: read_receipt.result_digest(),
        projected_start_ordinal: start_ordinal,
        projected_bar_count: projected.len(),
    };
    let series = MarketHistorySeries {
        instrument_id: request.instrument_id(),
        timeframe: request.timeframe(),
        session: request.session(),
        adjustment: request.adjustment(),
        currency,
        knowledge_cutoff: request.knowledge_cutoff(),
        coverage,
        quality: MarketHistoryQuality {
            observation_quality: quality,
            complete_trading_sessions: true,
            current_research_eligible: true,
            point_in_time_backtest_eligible: false,
            retrospective_training_eligible: false,
            realized_outcome_eligible: publication.realized_outcome_eligible(),
        },
        bars: projected,
        evidence,
    };
    if start_ordinal == 0 {
        MarketHistoryReadOutcome::Complete(series)
    } else {
        MarketHistoryReadOutcome::Partial {
            series,
            reason: MarketHistoryPartialReason::OutputLimit,
        }
    }
}

fn unavailable_reason(error: &AnalyticalReadError) -> MarketHistoryUnavailableReason {
    match error {
        AnalyticalReadError::Manifest(ManifestCatalogError::Cancelled)
        | AnalyticalReadError::Query(QueryError::Cancelled)
        | AnalyticalReadError::Parquet(ParquetStoreError::Cancelled) => {
            MarketHistoryUnavailableReason::Cancelled
        }
        AnalyticalReadError::Manifest(ManifestCatalogError::DeadlineExceeded)
        | AnalyticalReadError::Query(QueryError::DeadlineExceeded)
        | AnalyticalReadError::Parquet(
            ParquetStoreError::ReadDeadlineExceeded | ParquetStoreError::RecoveryDeadlineExceeded,
        ) => MarketHistoryUnavailableReason::DeadlineExceeded,
        AnalyticalReadError::Manifest(
            ManifestCatalogError::ObjectLimitExceeded { .. }
            | ManifestCatalogError::CaptureInputLimitExceeded { .. }
            | ManifestCatalogError::MarketBarHistoryInputLimitExceeded { .. }
            | ManifestCatalogError::ReferenceWorkLimitExceeded { .. }
            | ManifestCatalogError::CountOverflow
            | ManifestCatalogError::AllocationContract,
        )
        | AnalyticalReadError::Query(
            QueryError::RowLimitExceeded { .. }
            | QueryError::ByteLimitExceeded { .. }
            | QueryError::MemoryLimitExceeded { .. }
            | QueryError::SizeOverflow
            | QueryError::BlockingTaskLimitExceeded
            | QueryError::ReaderMemoryBoundExceeded,
        )
        | AnalyticalReadError::Parquet(
            ParquetStoreError::StagingLimitExceeded
            | ParquetStoreError::ReadLimitExceeded
            | ParquetStoreError::SizeOverflow
            | ParquetStoreError::BlockingTaskLimitExceeded
            | ParquetStoreError::RecoveryScanLimit,
        ) => MarketHistoryUnavailableReason::CapacityExceeded,
        AnalyticalReadError::Parquet(_) => MarketHistoryUnavailableReason::StorageUnavailable,
        _ => MarketHistoryUnavailableReason::IntegrityUnproven,
    }
}
