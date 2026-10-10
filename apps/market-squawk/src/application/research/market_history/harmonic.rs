//! Actual price-pattern evaluation over the existing sealed canonical history authority.

use crate::ResearchService;
use crate::application::market_calendar::{
    CompletedMarketSessionError, CompletedMarketSessionReadCapability,
    CompletedMarketSessionReference,
};
use crate::application::research::corporate_actions::map_research_error;
use market_squawk_services::ServiceError;
use std::{
    num::NonZeroU64,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_analytics::{
    HarmonicBar, HarmonicDirection, HarmonicEvidenceBinding, HarmonicPatternError,
    HarmonicPatternEvidence, HarmonicPatternInput, KnownFeatureImplementation, MAX_HARMONIC_BARS,
    MIN_HARMONIC_BARS, classify_harmonic_pattern,
};
use market_squawk_data::{
    CompleteMarketBarHistoryCursor, ForecastBasisHistory, ForecastBasisHistoryRow,
    LatestCanonicalMarketBarHistoryWindowRequest, MarketHistorySelectionPolicy, ResearchUse,
    ResearchUseCatalogError, ResearchUseLimits, ResearchUsePermit, ResearchUseRequest,
};
use market_squawk_decisions::{
    HarmonicHistoryAudit, HarmonicHistoryAuditInput, HarmonicHistoryDisposition,
    HarmonicHistoryGeometry, HarmonicPatternEvidenceReceipt,
};
use market_squawk_domain::{
    Currency, DigestAlgorithm, EvidenceDigest, InstrumentId, MarketBarAdjustment,
    MarketBarObservation, PriceTicks, TickSize, Timestamp,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{MarketHistoryReadCapability, MarketHistoryUnavailableReason, unavailable_reason};

const DAILY_NANOS: u64 = 86_400_000_000_000;

/// A real detector invocation, including an evaluated absence of a qualifying pattern.
#[derive(Clone, Debug)]
pub(crate) struct HarmonicHistoryEvaluation {
    pattern: Option<HarmonicPatternEvidenceReceipt>,
    audit: HarmonicHistoryAudit,
    chart_bars: Vec<HarmonicHistoryChartBar>,
}

/// Original evaluated closes. A native date remains a date; any plot instant is explicitly the
/// independently retained regular-session close, never an invented provider timestamp.
#[derive(Clone, Debug)]
pub(crate) struct HarmonicHistoryChartBar {
    pub(crate) nominal_date: Option<market_squawk_domain::CalendarDate>,
    pub(crate) observed_at: Timestamp,
    pub(crate) available_at: Timestamp,
    pub(crate) close: market_squawk_domain::Money,
    pub(crate) quality: market_squawk_domain::DataQuality,
}

impl HarmonicHistoryEvaluation {
    pub(crate) fn chart_bars(&self) -> &[HarmonicHistoryChartBar] {
        &self.chart_bars
    }

    pub(crate) fn pattern_receipt(&self) -> Option<&HarmonicPatternEvidenceReceipt> {
        self.pattern.as_ref()
    }

    pub(crate) const fn audit(&self) -> &HarmonicHistoryAudit {
        &self.audit
    }

    pub(crate) const fn evaluation_digest(&self) -> EvidenceDigest {
        self.audit.digest()
    }
}

impl MarketHistoryReadCapability {
    /// Rechecks every original chart parent before an aligned history is saved or read.
    pub(crate) async fn authorize_forecast_history(
        research: &ResearchService,
        history: &ForecastBasisHistory,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(ResearchUsePermit, Timestamp), MarketHistoryUnavailableReason> {
        authorize_history_parents(
            research,
            history.parent_manifests(),
            history.source_cutoff(),
            deadline,
            cancellation,
        )
        .await
    }

    /// Evaluates the exact original forecast split-plan units; source replay remains data-owned.
    /// `saved` compares original financial evidence while independently renewing current rights.
    /// A saved All-adjusted audit cannot pass this comparison and is never relabelled.
    #[allow(
        clippy::too_many_arguments,
        reason = "sealed input, original audit and controls are independent"
    )]
    pub(crate) async fn read_forecast_basis_harmonic(
        &self,
        research: &ResearchService,
        history: &ForecastBasisHistory,
        execution_tick: Option<TickSize>,
        saved: Option<&HarmonicHistoryAudit>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<HarmonicHistoryEvaluation, MarketHistoryUnavailableReason> {
        check_control(deadline, &cancellation)?;
        let invalid = || MarketHistoryUnavailableReason::IntegrityUnproven;
        let source_cutoff = history.source_cutoff();
        let observed_through = history.origin_at();
        let currency = history.origin_price().currency();
        if observed_through > source_cutoff {
            return Err(invalid());
        }
        let (permit, checked_at) = authorize_history_parents(
            research,
            history.parent_manifests(),
            source_cutoff,
            deadline,
            &cancellation,
        )
        .await?;
        let prepared = prepare_basis_bars(
            history.rows(),
            currency,
            source_cutoff,
            observed_through,
            deadline,
            &cancellation,
        )?;
        if prepared
            .chart_bars
            .last()
            .is_none_or(|bar| bar.close != history.origin_price())
        {
            return Err(invalid());
        }
        let source_identity = digest(history.source_read_identity().bytes());
        let adjustment_identity = digest(history.basis_identity().bytes());
        let completeness_identity = digest(history.history_identity().bytes());
        // The history digest retains every original gap. The detector evaluates only its final
        // contiguous suffix, never an apparent leg across a missing source session.
        let marketability_identity = policy_digest(
            b"forecast-split-final-contiguous-suffix-research-only-five-day-expiry/v1",
            &[adjustment_identity.bytes(), completeness_identity.bytes()],
            &[
                (prepared.start_ordinal as u64).to_be_bytes().as_slice(),
                source_cutoff.unix_nanos().to_be_bytes().as_slice(),
                observed_through.unix_nanos().to_be_bytes().as_slice(),
            ],
        );
        let evaluation = finish_evaluation(
            &prepared.bars,
            prepared.chart_bars,
            EvaluationCoordinates {
                instrument_id: history.instrument_id(),
                currency,
                execution_tick,
                analytical_tick: prepared.analytical_tick,
                source_cutoff,
                observed_through,
                source_identity,
                selected_manifest: digest(history.selected_manifest().content_hash().bytes()),
                origin_manifest: digest(history.origin_manifest().content_hash().bytes()),
                adjustment_identity,
                calendar_identity: digest(history.calendar_identity().bytes()),
                completeness_identity,
                marketability_identity,
                materialized_bars: u32::try_from(history.row_count()).map_err(|_| invalid())?,
                start_ordinal: u32::try_from(prepared.start_ordinal).map_err(|_| invalid())?,
            },
            permit,
            checked_at,
            deadline,
            &cancellation,
        )?;
        if let Some(saved) = saved {
            let actual = evaluation.audit().input();
            let mut expected = saved.input().clone();
            expected.rights_decision_identity = actual.rights_decision_identity;
            expected.rights_graph_identity = actual.rights_graph_identity;
            expected.rights_checked_at = actual.rights_checked_at;
            expected.rights_expires_at = actual.rights_expires_at;
            expected.evaluated_at = actual.evaluated_at;
            if &expected != actual {
                return Err(invalid());
            }
        }
        Ok(evaluation)
    }

    /// Reads an actual immutable adjusted generation, then evaluates only economically closed
    /// bars through `observed_through`, preserving every actual source-knowledge clock.
    ///
    /// `None` means missing eligible history. `Some` with no pattern is an audited detector
    /// result. A retrospective study must separately retain its frozen-snapshot qualification;
    /// this method never converts acquisition today into historical-as-known information.
    /// `execution_tick` retains only an original source increment when present. Exact adjusted
    /// OHLC values independently determine the analytical grid for every source.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact identity, price units, both clocks and work controls remain explicit"
    )]
    pub(crate) async fn read_harmonic(
        &self,
        research: &ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        instrument_id: InstrumentId,
        currency: Currency,
        execution_tick: Option<TickSize>,
        source_cutoff: Timestamp,
        observed_through: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<HarmonicHistoryEvaluation>, MarketHistoryUnavailableReason> {
        self.read_harmonic_bound(
            research,
            calendars,
            instrument_id,
            currency,
            execution_tick,
            source_cutoff,
            observed_through,
            None,
            deadline,
            cancellation,
        )
        .await
    }

    /// Reopens one saved evaluation by exact content hash. No latest-generation selection occurs.
    pub(crate) async fn read_saved_harmonic(
        &self,
        research: &ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        saved: &HarmonicHistoryAudit,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<HarmonicHistoryEvaluation>, MarketHistoryUnavailableReason> {
        let input = saved.input();
        let evaluation = self
            .read_harmonic_bound(
                research,
                calendars,
                input.instrument_id,
                input.currency,
                input.execution_tick,
                input.source_cutoff,
                input.observed_through,
                Some(input.selected_manifest),
                deadline,
                cancellation,
            )
            .await?;
        if let Some(evaluation) = &evaluation {
            let actual = evaluation.audit().input();
            // A read obtains a fresh, independently checked permission. Every original economic,
            // source, calendar, implementation and geometric coordinate must remain identical.
            let mut expected = input.clone();
            expected.rights_decision_identity = actual.rights_decision_identity;
            expected.rights_graph_identity = actual.rights_graph_identity;
            expected.rights_checked_at = actual.rights_checked_at;
            expected.rights_expires_at = actual.rights_expires_at;
            expected.evaluated_at = actual.evaluated_at;
            if &expected != actual {
                return Err(MarketHistoryUnavailableReason::IntegrityUnproven);
            }
        }
        Ok(evaluation)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "original authority coordinates stay explicit"
    )]
    async fn read_harmonic_bound(
        &self,
        research: &ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        instrument_id: InstrumentId,
        currency: Currency,
        execution_tick: Option<TickSize>,
        source_cutoff: Timestamp,
        observed_through: Timestamp,
        saved_content_hash: Option<EvidenceDigest>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<HarmonicHistoryEvaluation>, MarketHistoryUnavailableReason> {
        check_control(deadline, &cancellation)?;
        if observed_through > source_cutoff {
            return Err(MarketHistoryUnavailableReason::IntegrityUnproven);
        }
        let exact_request = if let Some(hash) = saved_content_hash {
            self.reader
                .exact_canonical_market_bar_history_window(
                    instrument_id,
                    market_squawk_data::Sha256Digest::new(hash.bytes()),
                    MarketHistorySelectionPolicy::COMPLETE_DAILY_ADJUSTED_V1,
                    source_cutoff,
                    deadline,
                    &cancellation,
                )
                .map_err(|error| unavailable_reason(&error))?
        } else {
            let lookup = LatestCanonicalMarketBarHistoryWindowRequest::try_new(
                instrument_id,
                MarketHistorySelectionPolicy::COMPLETE_DAILY_ADJUSTED_V1,
                source_cutoff,
            )
            .map_err(|_| MarketHistoryUnavailableReason::IntegrityUnproven)?;
            self.reader
                .select_latest_canonical_market_bar_history_window(lookup, deadline, &cancellation)
                .map_err(|error| unavailable_reason(&error))?
                .map(|selection| selection.into_exact_request())
        };
        let Some(exact_request) = exact_request else {
            return Ok(None);
        };
        let Some(output) = self
            .reader
            .read_canonical_market_bar_history_cursor(exact_request, deadline, cancellation.clone())
            .await
            .map_err(|error| unavailable_reason(&error))?
        else {
            return Ok(None);
        };
        // Nominal dates have no provider completion instant. Reopen the exact physical
        // calendar retained by this publication, at the same original knowledge cutoff.
        // Its genuine regular closes are analytical economic coordinates, not bar timestamps.
        let output = if let Some(graph) = output.selection().receipt().date_windows() {
            let retained = graph.calendar();
            let reference = CompletedMarketSessionReference::try_from_retained_digests(
                retained.origin_content_digest,
                retained.capture_binding_digest,
            )
            .map_err(calendar_error)?;
            let calendar = calendars
                .read_reference(&reference, source_cutoff, deadline, cancellation.clone())
                .await
                .map_err(calendar_error)?
                .ok_or(MarketHistoryUnavailableReason::IntegrityUnproven)?;
            let result = research
                .rejoin_market_history_native_sessions_with_calendar(
                    output,
                    &calendar,
                    deadline,
                    &cancellation,
                )
                .await;
            check_control(deadline, &cancellation)?;
            result.map_err(|error| source_error(map_research_error(error)))?
        } else {
            output
        };
        let mut roots = Vec::new();
        roots
            .try_reserve_exact(2)
            .map_err(|_| MarketHistoryUnavailableReason::CapacityExceeded)?;
        for manifest in [
            output.selection().pinned().manifest(),
            output.read_receipt().origin_manifest(),
        ] {
            if !roots.contains(manifest) {
                roots.push(manifest.clone());
            }
        }
        let (permit, checked_at) =
            authorize_history_parents(research, &roots, source_cutoff, deadline, &cancellation)
                .await?;
        evaluate_history(
            &output,
            permit,
            checked_at,
            instrument_id,
            currency,
            execution_tick,
            source_cutoff,
            observed_through,
            deadline,
            &cancellation,
        )
    }
}

async fn authorize_history_parents(
    research: &ResearchService,
    roots: &[market_squawk_data::DatasetManifestRef],
    source_cutoff: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(ResearchUsePermit, Timestamp), MarketHistoryUnavailableReason> {
    if roots.is_empty() || roots.len() > 128 {
        return Err(MarketHistoryUnavailableReason::CapacityExceeded);
    }
    let authorization_duration = deadline
        .saturating_duration_since(Instant::now())
        .min(Duration::from_secs(5));
    if authorization_duration.is_zero() {
        return Err(MarketHistoryUnavailableReason::DeadlineExceeded);
    }
    let request = ResearchUseRequest::try_new(
        roots.to_vec(),
        ResearchUse::LocalAnalysis,
        ResearchUseLimits::try_new(
            128,
            4096,
            8192,
            4096,
            4 * 1024 * 1024,
            authorization_duration,
            Duration::from_secs(300),
        )
        .map_err(|_| MarketHistoryUnavailableReason::IntegrityUnproven)?,
    )
    .map_err(|_| MarketHistoryUnavailableReason::IntegrityUnproven)?;
    let authorization = research
        .authorize_research_use(request, deadline, cancellation)
        .await;
    check_control(deadline, cancellation)?;
    let authorization = authorization
        .map_err(|error| source_error(map_research_error(error)))?
        .map_err(|error| match error {
            ResearchUseCatalogError::Cancelled => MarketHistoryUnavailableReason::Cancelled,
            ResearchUseCatalogError::DeadlineExceeded => {
                MarketHistoryUnavailableReason::DeadlineExceeded
            }
            ResearchUseCatalogError::LimitExceeded => {
                MarketHistoryUnavailableReason::CapacityExceeded
            }
            _ => MarketHistoryUnavailableReason::IntegrityUnproven,
        })?;
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
        return Err(MarketHistoryUnavailableReason::IntegrityUnproven);
    }
    let checked_at = wall_now()?;
    if checked_at >= authorization.expires_at() || checked_at < source_cutoff {
        return Err(MarketHistoryUnavailableReason::IntegrityUnproven);
    }
    let permit = authorization.into_permit();
    Ok((permit, checked_at))
}

#[allow(
    clippy::too_many_arguments,
    reason = "source receipt, financial identity and causal coordinates are independent"
)]
fn evaluate_history(
    output: &CompleteMarketBarHistoryCursor,
    permit: ResearchUsePermit,
    rights_checked_at: Timestamp,
    instrument_id: InstrumentId,
    currency: Currency,
    execution_tick: Option<TickSize>,
    source_cutoff: Timestamp,
    observed_through: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<HarmonicHistoryEvaluation>, MarketHistoryUnavailableReason> {
    let invalid = || MarketHistoryUnavailableReason::IntegrityUnproven;
    let publication = output.selection().receipt();
    if publication.instrument_id() != instrument_id
        || publication.currency() != currency
        || publication.adjustment() != MarketBarAdjustment::All
        || !publication.current_research_eligible()
        || publication.published_at() > source_cutoff
        || output.bar_count() != publication.bar_count()
    {
        return Err(invalid());
    }

    // The complete canonical reader has already verified each row against its exact raw-derived
    // publication receipt. Do not drop an unavailable interior bar and join its neighbours.
    let mut eligible_count: usize = 0;
    let mut previous_end = None;
    let mut selected = std::collections::VecDeque::new();
    selected
        .try_reserve_exact(MAX_HARMONIC_BARS)
        .map_err(|_| MarketHistoryUnavailableReason::CapacityExceeded)?;
    for bar in output.bars() {
        check_control(deadline, cancellation)?;
        let bar = bar.map_err(|error| unavailable_reason(&error))?;
        let completed = source_completion(output, &bar)?;
        if previous_end.is_some_and(|prior| prior >= completed) {
            return Err(invalid());
        }
        previous_end = Some(completed);
        if completed > observed_through {
            break;
        }
        let provenance = bar.context().provenance();
        let available = provenance
            .availability()
            .conservative_available_at()
            .ok_or_else(invalid)?;
        if provenance.instrument_id() != Some(instrument_id)
            || bar.currency() != currency
            || bar.adjustment() != MarketBarAdjustment::All
            || completed > available
            || available > source_cutoff
            || provenance.ingested_at() > source_cutoff
        {
            return Err(invalid());
        }
        eligible_count = eligible_count.checked_add(1).ok_or_else(invalid)?;
        if selected.len() == MAX_HARMONIC_BARS {
            selected.pop_front();
        }
        selected.push_back(bar);
    }
    if eligible_count == 0 {
        return Ok(None);
    }
    let start_ordinal = eligible_count.saturating_sub(MAX_HARMONIC_BARS);

    // Adjusted prices can have sub-market-tick precision. Derive the exact decimal grid from
    // the selected values and retain any genuine execution increment separately. Never round OHLC
    // or label the analytical grid executable. PriceTicks' checked exact conversion is unchanged.
    let scale = selected
        .iter()
        .flat_map(|bar| [bar.open(), bar.high(), bar.low(), bar.close()])
        .map(|price| price.amount().normalize().scale())
        .max()
        .ok_or_else(invalid)?;
    let analytical_tick = TickSize::power_of_ten(scale).map_err(|_| invalid())?;
    let mut bars = Vec::new();
    bars.try_reserve_exact(selected.len())
        .map_err(|_| MarketHistoryUnavailableReason::CapacityExceeded)?;
    for bar in &selected {
        check_control(deadline, cancellation)?;
        let provenance = bar.context().provenance();
        let available_at = provenance
            .availability()
            .conservative_available_at()
            .ok_or_else(invalid)?
            .max(provenance.ingested_at())
            .max(publication.published_at());
        let [open, high, low, close] = [bar.open(), bar.high(), bar.low(), bar.close()]
            .map(|price| PriceTicks::try_from_decimal(price.amount(), analytical_tick));
        bars.push(HarmonicBar::new(
            source_completion(output, bar)?,
            available_at,
            open.map_err(|_| invalid())?,
            high.map_err(|_| invalid())?,
            low.map_err(|_| invalid())?,
            close.map_err(|_| invalid())?,
        ));
    }
    let selected_manifest = digest(
        output
            .selection()
            .pinned()
            .manifest()
            .content_hash()
            .bytes(),
    );
    let origin_manifest = digest(
        output
            .read_receipt()
            .origin_manifest()
            .content_hash()
            .bytes(),
    );
    let source_identity = digest(output.read_receipt().result_digest().bytes());
    // Explicit absence is evidence, not permission to invent an execution increment.
    let execution_tick_bytes = execution_tick.map(|tick| {
        let value = tick.as_decimal().normalize();
        (value.mantissa().to_be_bytes(), value.scale().to_be_bytes())
    });
    let adjustment_identity = policy_digest(
        b"exact-adjusted-ohlc-no-rounding/v1",
        &[
            publication.receipt_digest().bytes(),
            source_identity.bytes(),
        ],
        &[
            &[u8::from(execution_tick.is_some())],
            execution_tick_bytes
                .as_ref()
                .map_or(&[][..], |(mantissa, _)| mantissa.as_slice()),
            execution_tick_bytes
                .as_ref()
                .map_or(&[][..], |(_, scale)| scale.as_slice()),
            analytical_tick
                .as_decimal()
                .mantissa()
                .to_be_bytes()
                .as_slice(),
            analytical_tick
                .as_decimal()
                .scale()
                .to_be_bytes()
                .as_slice(),
            currency.as_str().as_bytes(),
        ],
    );
    let calendar_identity = match (
        publication.session_calendar_component(),
        publication.date_windows(),
    ) {
        (Some((_, component, _)), None) => policy_digest(
            b"complete-canonical-daily-sessions/v1",
            &[
                component.bytes(),
                publication.completeness_evidence_digest().bytes(),
            ],
            &[publication.session_ruleset().as_str().as_bytes()],
        ),
        (None, Some(graph)) => {
            let native = output.native_sessions().ok_or_else(invalid)?;
            policy_digest(
                b"complete-canonical-nominal-regular-sessions/v1",
                &[
                    native.mapping_digest().bytes(),
                    native.source_replay_digest().bytes(),
                    native.calendar_origin_content_digest().bytes(),
                    native.calendar_capture_binding_digest().bytes(),
                    graph.calendar().relationship.relationship_digest().bytes(),
                    publication.completeness_evidence_digest().bytes(),
                ],
                &[publication.session_ruleset().as_str().as_bytes()],
            )
        }
        _ => return Err(invalid()),
    };
    let completeness_identity = policy_digest(
        b"bounded-economic-prefix-no-interior-drops/v1",
        &[
            source_identity.bytes(),
            publication.expected_timestamp_set_digest().bytes(),
        ],
        &[
            (start_ordinal as u64).to_be_bytes().as_slice(),
            (selected.len() as u64).to_be_bytes().as_slice(),
            observed_through.unix_nanos().to_be_bytes().as_slice(),
        ],
    );
    // This binds real feed/volume/quality and explicit freshness policy. It grants no liquidity,
    // confidence, historical-as-known, or execution authority; those consumers remain separate.
    let marketability_identity = policy_digest(
        b"research-only-no-execution-five-day-economic-expiry-invalidation/v1",
        &[
            source_identity.bytes(),
            publication.instrument_revision_digest().bytes(),
        ],
        &[
            source_cutoff.unix_nanos().to_be_bytes().as_slice(),
            observed_through.unix_nanos().to_be_bytes().as_slice(),
        ],
    );
    let mut chart_bars = Vec::new();
    chart_bars
        .try_reserve_exact(selected.len())
        .map_err(|_| MarketHistoryUnavailableReason::CapacityExceeded)?;
    for (bar, evaluated) in selected.iter().zip(&bars) {
        chart_bars.push(HarmonicHistoryChartBar {
            nominal_date: bar
                .time_semantics()
                .nominal_daily_date()
                .map(|date| date.date()),
            observed_at: evaluated.observed_at(),
            available_at: evaluated.available_at(),
            close: bar.close(),
            quality: bar.context().provenance().quality(),
        });
    }
    finish_evaluation(
        &bars,
        chart_bars,
        EvaluationCoordinates {
            instrument_id,
            currency,
            execution_tick,
            analytical_tick,
            source_cutoff,
            observed_through,
            source_identity,
            selected_manifest,
            origin_manifest,
            adjustment_identity,
            calendar_identity,
            completeness_identity,
            marketability_identity,
            materialized_bars: u32::try_from(output.bar_count()).map_err(|_| invalid())?,
            start_ordinal: u32::try_from(start_ordinal).map_err(|_| invalid())?,
        },
        permit,
        rights_checked_at,
        deadline,
        cancellation,
    )
    .map(Some)
}

struct PreparedBasisBars {
    start_ordinal: usize,
    analytical_tick: TickSize,
    bars: Vec<HarmonicBar>,
    chart_bars: Vec<HarmonicHistoryChartBar>,
}

/// Preserve every source clock and exact price. Only the final uninterrupted native-session run
/// can describe a currently active pattern; earlier runs remain in the sealed chart history.
fn prepare_basis_bars(
    rows: impl IntoIterator<
        Item = Result<ForecastBasisHistoryRow, market_squawk_data::DatasetBuildError>,
    >,
    currency: Currency,
    source_cutoff: Timestamp,
    observed_through: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PreparedBasisBars, MarketHistoryUnavailableReason> {
    let invalid = || MarketHistoryUnavailableReason::IntegrityUnproven;
    check_control(deadline, cancellation)?;
    // The detector already evaluates at most MAX_HARMONIC_BARS. Retain the same final
    // contiguous suffix while the full original history remains in the immutable projection.
    let mut selected = std::collections::VecDeque::with_capacity(MAX_HARMONIC_BARS);
    let mut start_ordinal = 0;
    for (ordinal, row) in rows.into_iter().enumerate() {
        check_control(deadline, cancellation)?;
        let row = row.map_err(|_| invalid())?;
        if row.prices.is_none() {
            selected.clear();
            start_ordinal = ordinal.checked_add(1).ok_or_else(invalid)?;
        } else {
            if selected.len() == MAX_HARMONIC_BARS {
                selected.pop_front();
                start_ordinal = start_ordinal.checked_add(1).ok_or_else(invalid)?;
            }
            selected.push_back(row);
        }
    }
    if selected
        .back()
        .is_none_or(|row| row.observed_at != observed_through)
    {
        return Err(invalid());
    }
    let scale = selected
        .iter()
        .filter_map(|row| row.prices.as_ref())
        .flat_map(|prices| [prices.open, prices.high, prices.low, prices.close])
        .map(|price| price.amount().normalize().scale())
        .max()
        .ok_or_else(invalid)?;
    let analytical_tick = TickSize::power_of_ten(scale).map_err(|_| invalid())?;
    let mut bars = Vec::new();
    let mut chart_bars = Vec::new();
    bars.try_reserve_exact(selected.len())
        .map_err(|_| MarketHistoryUnavailableReason::CapacityExceeded)?;
    chart_bars
        .try_reserve_exact(selected.len())
        .map_err(|_| MarketHistoryUnavailableReason::CapacityExceeded)?;
    let mut previous = None;
    for row in &selected {
        check_control(deadline, cancellation)?;
        let prices = row.prices.as_ref().ok_or_else(invalid)?;
        let available_at = row.available_at.ok_or_else(invalid)?;
        let quality = row.quality.ok_or_else(invalid)?;
        let values = [prices.open, prices.high, prices.low, prices.close];
        if row.observed_at > available_at
            || available_at > source_cutoff
            || row.observed_at > observed_through
            || previous.is_some_and(|prior| prior >= row.observed_at)
            || values.iter().any(|price| price.currency() != currency)
            || row.original_bar_identity.is_none()
        {
            return Err(invalid());
        }
        previous = Some(row.observed_at);
        let [open, high, low, close] =
            values.map(|price| PriceTicks::try_from_decimal(price.amount(), analytical_tick));
        let bar = HarmonicBar::new(
            row.observed_at,
            available_at,
            open.map_err(|_| invalid())?,
            high.map_err(|_| invalid())?,
            low.map_err(|_| invalid())?,
            close.map_err(|_| invalid())?,
        );
        // Short suffixes still receive complete OHLC validation before an InsufficientBars audit.
        if bar.low().get() <= 0
            || bar.low() > bar.open()
            || bar.low() > bar.close()
            || bar.high() < bar.open()
            || bar.high() < bar.close()
        {
            return Err(invalid());
        }
        bars.push(bar);
        chart_bars.push(HarmonicHistoryChartBar {
            nominal_date: row.nominal_date,
            observed_at: row.observed_at,
            available_at,
            close: prices.close,
            quality,
        });
    }
    Ok(PreparedBasisBars {
        start_ordinal,
        analytical_tick,
        bars,
        chart_bars,
    })
}

struct EvaluationCoordinates {
    instrument_id: InstrumentId,
    currency: Currency,
    execution_tick: Option<TickSize>,
    analytical_tick: TickSize,
    source_cutoff: Timestamp,
    observed_through: Timestamp,
    source_identity: EvidenceDigest,
    selected_manifest: EvidenceDigest,
    origin_manifest: EvidenceDigest,
    adjustment_identity: EvidenceDigest,
    calendar_identity: EvidenceDigest,
    completeness_identity: EvidenceDigest,
    marketability_identity: EvidenceDigest,
    materialized_bars: u32,
    start_ordinal: u32,
}

/// Both source adapters use one detector, expiry/invalidation rule and persisted audit.
fn finish_evaluation(
    bars: &[HarmonicBar],
    chart_bars: Vec<HarmonicHistoryChartBar>,
    coordinates: EvaluationCoordinates,
    permit: ResearchUsePermit,
    rights_checked_at: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<HarmonicHistoryEvaluation, MarketHistoryUnavailableReason> {
    let invalid = || MarketHistoryUnavailableReason::IntegrityUnproven;
    let EvaluationCoordinates {
        instrument_id,
        currency,
        execution_tick,
        analytical_tick,
        source_cutoff,
        observed_through,
        source_identity,
        selected_manifest,
        origin_manifest,
        adjustment_identity,
        calendar_identity,
        completeness_identity,
        marketability_identity,
        materialized_bars,
        start_ordinal,
    } = coordinates;
    let mut parents = [selected_manifest, origin_manifest];
    parents.sort_by_key(|value| value.bytes());
    let parent_count = if parents[0] == parents[1] { 1 } else { 2 };
    let binding = HarmonicEvidenceBinding::new(
        instrument_id,
        NonZeroU64::new(DAILY_NANOS).ok_or_else(invalid)?,
        &parents[..parent_count],
        adjustment_identity,
        calendar_identity,
        completeness_identity,
        marketability_identity,
    )
    .map_err(|_| invalid())?;
    let (disposition, classified) = classify(bars, binding, source_cutoff, observed_through)?;
    let geometry = classified.map(HarmonicHistoryGeometry::from_pattern);
    let pattern = classified
        .map(HarmonicPatternEvidenceReceipt::try_from_pattern)
        .transpose()
        .map_err(|_| invalid())?;
    check_control(deadline, cancellation)?;
    let observed_at = bars.last().ok_or_else(invalid)?.observed_at();
    let available_at = bars
        .iter()
        .map(|bar| bar.available_at())
        .max()
        .ok_or_else(invalid)?;
    let implementation_identity = digest(
        KnownFeatureImplementation::BatchHarmonicPatterns
            .implementation_digest()
            .map_err(|_| invalid())?
            .as_bytes(),
    );
    let evaluated_at = wall_now()?;
    if evaluated_at >= permit.expires_at() || evaluated_at < rights_checked_at {
        return Err(invalid());
    }
    let audit = HarmonicHistoryAudit::try_new(HarmonicHistoryAuditInput {
        rights_decision_identity: digest(permit.decision_digest().bytes()),
        rights_graph_identity: digest(permit.graph_digest().bytes()),
        rights_checked_at,
        rights_expires_at: permit.expires_at(),
        evaluated_at,
        instrument_id,
        currency,
        execution_tick,
        analytical_tick,
        source_cutoff,
        observed_through,
        observed_at,
        available_at,
        source_identity,
        selected_manifest,
        origin_manifest,
        adjustment_identity,
        calendar_identity,
        completeness_identity,
        marketability_identity,
        implementation_identity,
        materialized_bars,
        start_ordinal,
        evaluated_bars: u32::try_from(bars.len()).map_err(|_| invalid())?,
        disposition,
        geometry,
        pattern_digest: pattern
            .as_ref()
            .map(HarmonicPatternEvidenceReceipt::evidence_digest),
    })
    .map_err(|_| invalid())?;
    Ok(HarmonicHistoryEvaluation {
        pattern,
        audit,
        chart_bars,
    })
}

/// Keeps native dates intact; only the original attached calendar supplies a regular close.
fn source_completion(
    output: &CompleteMarketBarHistoryCursor,
    bar: &MarketBarObservation,
) -> Result<Timestamp, MarketHistoryUnavailableReason> {
    let invalid = || MarketHistoryUnavailableReason::IntegrityUnproven;
    if let Some(completed) = bar.completed_at() {
        if output.selection().receipt().date_windows().is_some() {
            return Err(invalid());
        }
        return Ok(completed);
    }
    let nominal = bar
        .time_semantics()
        .nominal_daily_date()
        .ok_or_else(invalid)?;
    let native = output.native_sessions().ok_or_else(invalid)?;
    let session = native
        .sessions()
        .find_date(nominal.date())
        .map_err(|error| unavailable_reason(&error))?
        .ok_or_else(invalid)?;
    if !session.bar_present()
        || session.provider_timestamp().is_some()
        || session.provider_period().is_some()
        || session.opens_at() >= session.closes_at_exclusive()
        || bar.context().time().effective().calendar_date_value() != Some(nominal.date())
    {
        return Err(invalid());
    }
    Ok(session.closes_at_exclusive())
}

fn calendar_error(error: CompletedMarketSessionError) -> MarketHistoryUnavailableReason {
    match error {
        CompletedMarketSessionError::Cancelled => MarketHistoryUnavailableReason::Cancelled,
        CompletedMarketSessionError::DeadlineExceeded => {
            MarketHistoryUnavailableReason::DeadlineExceeded
        }
        CompletedMarketSessionError::ResourceBoundExceeded => {
            MarketHistoryUnavailableReason::CapacityExceeded
        }
        CompletedMarketSessionError::Unavailable => {
            MarketHistoryUnavailableReason::StorageUnavailable
        }
        CompletedMarketSessionError::InvalidRequest
        | CompletedMarketSessionError::InvalidEvidence => {
            MarketHistoryUnavailableReason::IntegrityUnproven
        }
    }
}

fn source_error(error: ServiceError) -> MarketHistoryUnavailableReason {
    match error {
        ServiceError::Cancelled => MarketHistoryUnavailableReason::Cancelled,
        ServiceError::DeadlineExceeded => MarketHistoryUnavailableReason::DeadlineExceeded,
        ServiceError::ResourceExhausted => MarketHistoryUnavailableReason::CapacityExceeded,
        ServiceError::Unavailable | ServiceError::NotFound => {
            MarketHistoryUnavailableReason::StorageUnavailable
        }
        _ => MarketHistoryUnavailableReason::IntegrityUnproven,
    }
}

fn classify(
    bars: &[HarmonicBar],
    binding: HarmonicEvidenceBinding,
    source_cutoff: Timestamp,
    observed_through: Timestamp,
) -> Result<
    (HarmonicHistoryDisposition, Option<HarmonicPatternEvidence>),
    MarketHistoryUnavailableReason,
> {
    let invalid = || MarketHistoryUnavailableReason::IntegrityUnproven;
    if bars.len() < MIN_HARMONIC_BARS {
        return Ok((HarmonicHistoryDisposition::InsufficientBars, None));
    }
    let pattern =
        match classify_harmonic_pattern(HarmonicPatternInput::new(binding, bars, source_cutoff)) {
            Ok(pattern) => pattern,
            Err(HarmonicPatternError::InsufficientPivots) => {
                return Ok((HarmonicHistoryDisposition::InsufficientPivots, None));
            }
            Err(HarmonicPatternError::NoMatchingPattern) => {
                return Ok((HarmonicHistoryDisposition::NoMatchingPattern, None));
            }
            Err(_) => return Err(invalid()),
        };
    let final_pivot = pattern.pivots()[4];
    let confirmation_index = usize::try_from(final_pivot.bar_index())
        .map_err(|_| invalid())?
        .checked_add(1)
        .ok_or_else(invalid)?;
    let economic_confirmation = bars
        .get(confirmation_index)
        .ok_or_else(invalid)?
        .observed_at();
    let lifetime = pattern
        .expires_at()
        .unix_nanos()
        .checked_sub(pattern.confirmation_cutoff().unix_nanos())
        .ok_or_else(invalid)?;
    let economic_expiry = economic_confirmation
        .checked_add_nanos(lifetime)
        .map_err(|_| invalid())?;
    if observed_through >= economic_expiry || source_cutoff >= pattern.expires_at() {
        return Ok((HarmonicHistoryDisposition::Expired, None));
    }
    if bars[confirmation_index..]
        .iter()
        .any(|bar| match pattern.direction() {
            HarmonicDirection::Bullish => bar.low() <= pattern.invalidation(),
            HarmonicDirection::Bearish => bar.high() >= pattern.invalidation(),
        })
    {
        return Ok((HarmonicHistoryDisposition::Invalidated, None));
    }
    Ok((HarmonicHistoryDisposition::Pattern, Some(pattern)))
}

fn digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn policy_digest(domain: &[u8], identities: &[[u8; 32]], values: &[&[u8]]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/harmonic-history-policy/v1\0");
    hash.update((domain.len() as u64).to_be_bytes());
    hash.update(domain);
    hash.update((identities.len() as u64).to_be_bytes());
    for value in identities {
        hash.update(value);
    }
    for value in values {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    digest(hash.finalize().into())
}

fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), MarketHistoryUnavailableReason> {
    if cancellation.is_cancelled() {
        Err(MarketHistoryUnavailableReason::Cancelled)
    } else if Instant::now() >= deadline {
        Err(MarketHistoryUnavailableReason::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn wall_now() -> Result<Timestamp, MarketHistoryUnavailableReason> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MarketHistoryUnavailableReason::IntegrityUnproven)?;
    i64::try_from(duration.as_nanos())
        .map(Timestamp::from_unix_nanos)
        .map_err(|_| MarketHistoryUnavailableReason::IntegrityUnproven)
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_data::ForecastBasisOhlc;
    use market_squawk_domain::{CalendarDate, DataQuality, Money};
    use rust_decimal::Decimal;

    #[test]
    fn forecast_basis_harmonics_preserve_gaps_precision_and_knowledge()
    -> Result<(), Box<dyn std::error::Error>> {
        let currency = Currency::try_from("USD")?;
        let mut rows = Vec::new();
        for day in 1..=4 {
            let observed_at = Timestamp::from_unix_nanos(i64::from(day) * 100);
            let date = CalendarDate::new(2026, 9, day)?;
            let price = Money::new(Decimal::new(12_345, 3), currency);
            rows.push(ForecastBasisHistoryRow {
                native_date: date,
                session_open: Timestamp::from_unix_nanos(i64::from(day) * 100 - 10),
                session_close: observed_at,
                nominal_date: Some(date),
                provider_timestamp: None,
                completed_at: None,
                observed_at,
                raw_available_at: Some(Timestamp::from_unix_nanos(i64::from(day) * 100 + 1)),
                available_at: Some(Timestamp::from_unix_nanos(500)),
                quality: Some(DataQuality::OfficialDelayed),
                original_bar_identity: Some([day; 32]),
                prices: Some(ForecastBasisOhlc {
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                }),
            });
        }
        rows[1].prices = None;
        rows[1].raw_available_at = None;
        rows[1].available_at = None;
        rows[1].quality = None;
        rows[1].original_bar_identity = None;
        let deadline = Instant::now() + Duration::from_secs(5);
        let cancellation = CancellationToken::new();
        let cutoff = Timestamp::from_unix_nanos(500);
        let origin = Timestamp::from_unix_nanos(400);
        let prepared = prepare_basis_bars(
            rows.iter().cloned().map(Ok),
            currency,
            cutoff,
            origin,
            deadline,
            &cancellation,
        )
        .map_err(|_| "valid split-basis suffix rejected")?;
        assert_eq!(prepared.start_ordinal, 2);
        assert_eq!(prepared.bars.len(), 2);
        assert_eq!(prepared.analytical_tick.as_decimal(), Decimal::new(1, 3));
        assert_eq!(prepared.bars[0].close().get(), 12_345);
        assert_eq!(
            prepared.bars[0].observed_at(),
            Timestamp::from_unix_nanos(300)
        );
        assert_eq!(
            prepared.bars[0].available_at(),
            Timestamp::from_unix_nanos(500)
        );
        assert_eq!(
            prepared.chart_bars[0].nominal_date,
            Some(rows[2].native_date)
        );
        // The detector's bounded suffix must not reject the retained full browse history.
        let original_count = MAX_HARMONIC_BARS + 7;
        let long_rows = (0..original_count).map(|index| {
            let mut row = rows[0].clone();
            row.observed_at = Timestamp::from_unix_nanos(index as i64 + 1);
            row.available_at = Some(Timestamp::from_unix_nanos(10_000));
            Ok(row)
        });
        let prepared = prepare_basis_bars(
            long_rows,
            currency,
            Timestamp::from_unix_nanos(10_000),
            Timestamp::from_unix_nanos(original_count as i64),
            deadline,
            &cancellation,
        )
        .map_err(|_| "complete history incorrectly constrained by detector limit")?;
        assert_eq!(prepared.start_ordinal, 7);
        assert_eq!(prepared.bars.len(), MAX_HARMONIC_BARS);
        // A later-known input is rejected, never dropped and joined to its neighbours.
        rows[2].available_at = Some(Timestamp::from_unix_nanos(501));
        assert!(matches!(
            prepare_basis_bars(
                rows.iter().cloned().map(Ok),
                currency,
                cutoff,
                origin,
                deadline,
                &cancellation
            ),
            Err(MarketHistoryUnavailableReason::IntegrityUnproven)
        ));
        // Missing origin cannot turn an earlier historical shape into current evidence.
        rows[3].prices = None;
        assert!(
            prepare_basis_bars(
                rows.iter().cloned().map(Ok),
                currency,
                cutoff,
                origin,
                deadline,
                &cancellation
            )
            .is_err()
        );
        Ok(())
    }
}
