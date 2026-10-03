//! Current-weight historical scenarios from exact canonical adjusted daily observations.
//!
//! A scenario reuses today's marked exposures, including shorts, against simultaneous historical
//! adjusted price changes. Reporting-currency cash stays constant. It is neither experienced
//! performance, a reinvested total-return index, nor historical-as-known signal evidence.

use market_squawk_data::{
    AnalyticalReadCapability, CompleteMarketBarHistoryOutput,
    LatestCanonicalMarketBarHistoryWindowRequest, MarketHistorySelectionPolicy,
};
use market_squawk_domain::{MarketBarAdjustment, MarketBarObservation};

use super::*;
use crate::ResearchService;
use crate::application::market_calendar::{
    CompletedMarketSessionError, CompletedMarketSessionReadCapability,
    CompletedMarketSessionReference, HistoryCurrentSessionQualification,
    qualify_history_current_session,
};
use crate::application::{
    map_source_analytical_error as map_analytical_error,
    map_source_research_error as map_research_error,
};
use market_squawk_services::ServiceError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ScenarioPeriod {
    pub(super) opening_at: Timestamp,
    pub(super) closing_at: Timestamp,
    pub(super) weighted_return: Decimal,
}

/// Bounded sample and exact source commitments; full histories remain in their immutable store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioHistoricalScenarioEvidence {
    periods: Box<[ScenarioPeriod]>,
    holdings: usize,
    source_observations: usize,
    cash: Money,
    receivables: Money,
    marked_equity: Money,
    source_set_digest: EvidenceDigest,
    source_histories: Box<[(InstrumentId, EvidenceDigest)]>,
    evidence_digest: EvidenceDigest,
    current_session_covered: bool,
    session_qualifications: Box<[HistoryCurrentSessionQualification]>,
}

impl PortfolioHistoricalScenarioEvidence {
    pub(super) fn periods(&self) -> &[ScenarioPeriod] {
        &self.periods
    }
    pub(super) fn source_history(&self, instrument: InstrumentId) -> Option<EvidenceDigest> {
        self.source_histories
            .iter()
            .find(|(id, _)| *id == instrument)
            .map(|(_, digest)| *digest)
    }
    pub(crate) const fn current_session_covered(&self) -> bool {
        self.current_session_covered
    }
    pub(super) async fn recheck_current_session(
        &self,
        authority: Option<&CompletedMarketSessionAuthority>,
        calendars: &CompletedMarketSessionReadCapability,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError> {
        if self.is_cash_only() {
            return Ok(());
        }
        if !self.current_session_covered || self.session_qualifications.len() != self.holdings {
            return Err(PortfolioApplicationServiceError::StateChanged);
        }
        for qualification in &self.session_qualifications {
            ensure_live(deadline, cancellation)?;
            if let Some(reference) = qualification.nominal_calendar_reference() {
                let calendar = calendars
                    .read_reference(reference, as_of, deadline, cancellation.clone())
                    .await
                    .map_err(calendar_error)?
                    .ok_or(PortfolioApplicationServiceError::StateChanged)?;
                qualification
                    .recheck_nominal(&calendar, as_of)
                    .map_err(calendar_error)?;
            } else {
                let authority = authority.ok_or(PortfolioApplicationServiceError::StateChanged)?;
                qualification
                    .recheck(authority, as_of)
                    .map_err(calendar_error)?;
            }
        }
        ensure_live(deadline, cancellation)
    }

    pub(crate) const fn is_cash_only(&self) -> bool {
        self.holdings == 0
    }
    pub(crate) fn observations(&self) -> usize {
        self.periods.len()
    }
    pub(crate) const fn source_observations(&self) -> usize {
        self.source_observations
    }
    pub(crate) const fn holdings(&self) -> usize {
        self.holdings
    }
    pub(crate) fn sample_start(&self) -> Option<Timestamp> {
        self.periods.first().map(|period| period.opening_at)
    }
    pub(crate) fn sample_end(&self) -> Option<Timestamp> {
        self.periods.last().map(|period| period.closing_at)
    }
    pub(crate) const fn receivables(&self) -> Money {
        self.receivables
    }
    pub(crate) const fn cash(&self) -> Money {
        self.cash
    }
    pub(crate) const fn digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

pub(super) async fn calculate(
    reader: &AnalyticalReadCapability,
    research: &ResearchService,
    calendars: &CompletedMarketSessionReadCapability,
    completed_sessions: Option<&CompletedMarketSessionAuthority>,
    portfolio: &PortfolioAnalysisPortfolioSnapshot,
    marked: &PortfolioAnalysisMarkedPortfolioEvidence,
    as_of: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PortfolioAnalysisRiskAvailability, PortfolioApplicationServiceError> {
    let mut periods: Vec<ScenarioPeriod> = Vec::new();
    let mut source_observations = 0_usize;
    let mut holdings = 0_usize;
    let mut current_session_covered = true;
    let mut source_histories = Vec::new();
    source_histories
        .try_reserve_exact(marked.holdings().len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    let mut session_qualifications = Vec::new();
    session_qualifications
        .try_reserve_exact(marked.holdings().len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    let mut sources = Sha256::new();
    sources.update(b"market-squawk/portfolio-current-weight-source-set/v1\0");
    canonical_evidence(&mut sources, marked.evidence_digest());
    let maximum = portfolio
        .policy()
        .maximum_historical_return_observations()
        .get();
    for holding in marked
        .holdings()
        .iter()
        .filter(|holding| !holding.quantity().is_zero())
    {
        ensure_live(deadline, cancellation)?;
        let instrument_id = holding.instrument_id();
        let source = match read_source_history(
            reader,
            research,
            calendars,
            completed_sessions,
            instrument_id,
            portfolio.reporting_currency(),
            as_of,
            deadline,
            cancellation,
        )
        .await?
        {
            Ok(source) => source,
            Err(reason) => return Ok(PortfolioAnalysisRiskAvailability::Unavailable(reason)),
        };
        canonical_evidence(&mut sources, source.digest);
        source_histories.push((instrument_id, source.digest));
        if let Some(qualification) = source.qualification {
            current_session_covered &= qualification.current_session_covered();
            session_qualifications.push(qualification);
        } else {
            current_session_covered = false;
        }
        let output = source.output;
        let bars = output.bars();
        let count = bars.len().saturating_sub(1).min(maximum);
        source_observations = source_observations
            .checked_add(count)
            .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
        let bars = &bars[bars.len().saturating_sub(maximum.saturating_add(1))..];
        let mut contribution = Vec::new();
        contribution
            .try_reserve_exact(count)
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
        for pair in bars.windows(2) {
            ensure_live(deadline, cancellation)?;
            let opening = &pair[0];
            let closing = &pair[1];
            let opening_at = source_session_close(&output, opening)?;
            let closing_at = source_session_close(&output, closing)?;
            if opening_at >= closing_at
                || closing_at > as_of
                || opening.currency() != portfolio.reporting_currency()
                || closing.currency() != portfolio.reporting_currency()
                || opening.adjustment() != MarketBarAdjustment::All
                || closing.adjustment() != MarketBarAdjustment::All
                || opening.close().amount() <= Decimal::ZERO
                || closing.close().amount() <= Decimal::ZERO
            {
                return Err(PortfolioApplicationServiceError::CorruptPublication);
            }
            // Compute dollar contribution first: signed current exposure preserves shorts and
            // leverage. Divide once by complete marked equity, which includes real signed cash.
            let weighted_return = closing
                .close()
                .amount()
                .checked_sub(opening.close().amount())
                .and_then(|change| change.checked_mul(holding.marked_value().amount()))
                .and_then(|change| change.checked_div(opening.close().amount()))
                .and_then(|impact| impact.checked_div(marked.marked_equity().amount()))
                .map(|value| value.normalize())
                .ok_or(PortfolioApplicationServiceError::Analytics)?;
            contribution.push(ScenarioPeriod {
                opening_at,
                closing_at,
                weighted_return,
            });
        }
        if holdings == 0 {
            periods = contribution;
        } else {
            // Intersect exact adjacent session pairs, never pair unrelated dates or fill a
            // missing instrument return with zero. Keep only one aggregate and one source window.
            let mut next = 0_usize;
            let mut retained = 0_usize;
            for index in 0..periods.len() {
                let key = (periods[index].opening_at, periods[index].closing_at);
                while next < contribution.len()
                    && (contribution[next].opening_at, contribution[next].closing_at) < key
                {
                    next += 1;
                }
                if let Some(other) = contribution
                    .get(next)
                    .filter(|other| (other.opening_at, other.closing_at) == key)
                {
                    let mut combined = periods[index].clone();
                    combined.weighted_return = combined
                        .weighted_return
                        .checked_add(other.weighted_return)
                        .ok_or(PortfolioApplicationServiceError::Analytics)?;
                    periods[retained] = combined;
                    retained += 1;
                }
            }
            periods.truncate(retained);
        }
        holdings += 1;
    }
    let required = portfolio
        .policy()
        .minimum_historical_return_observations()
        .get();
    if holdings != 0 && periods.len() < required {
        return Ok(PortfolioAnalysisRiskAvailability::Unavailable(
            PortfolioAnalysisRiskUnavailableReason::InsufficientHistory {
                required,
                available: periods.len(),
            },
        ));
    }
    let mut scenario = PortfolioHistoricalScenarioEvidence {
        periods: periods.into_boxed_slice(),
        holdings,
        source_observations,
        cash: marked.source_cash_balance(),
        receivables: marked.source_receivable_value(),
        marked_equity: marked.marked_equity(),
        source_set_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, sources.finalize().into()),
        source_histories: source_histories.into_boxed_slice(),
        evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        current_session_covered,
        session_qualifications: session_qualifications.into_boxed_slice(),
    };
    scenario.evidence_digest = scenario_digest(&scenario);
    let mut losses = Vec::new();
    losses
        .try_reserve_exact(scenario.periods.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    losses.extend(
        scenario
            .periods
            .iter()
            .map(|period| (-period.weighted_return).max(Decimal::ZERO)),
    );
    let budget = portfolio
        .setup()
        .setup()
        .profile()
        .maximum_downside_loss_bps_of_marked_equity();
    let (
        value_at_risk,
        expected_shortfall,
        expected_shortfall_basis_points_ceil,
        risk_capacity_ppm,
    ) = if scenario.is_cash_only() {
        // Exact market-price shock of reporting-currency cash is zero. This is an accounting
        // identity with zero historical observations, never fabricated daily market history.
        (Decimal::ZERO, Decimal::ZERO, 0, 1_000_000)
    } else {
        tail_risk(&losses, budget)?
    };
    let mut risk = PortfolioAnalysisRiskEvidence {
        account_id: portfolio.account_id(),
        portfolio_revision: portfolio.revision().clone(),
        profile_digest: portfolio.setup().setup().profile().digest(),
        returns: Box::new([]),
        scenario: Some(scenario),
        confidence_basis_points: 9_500,
        value_at_risk,
        expected_shortfall,
        expected_shortfall_basis_points_ceil,
        user_downside_budget_basis_points: budget,
        risk_capacity_ppm,
        policy_digest: portfolio.policy().digest(),
        evaluated_at: as_of,
        evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
    };
    risk.evidence_digest = risk_evidence_digest(&risk);
    ensure_live(deadline, cancellation)?;
    Ok(PortfolioAnalysisRiskAvailability::Available(risk))
}

pub(super) struct SourceHistory {
    pub(super) output: CompleteMarketBarHistoryOutput,
    pub(super) qualification: Option<HistoryCurrentSessionQualification>,
    pub(super) digest: EvidenceDigest,
}

#[allow(
    clippy::too_many_arguments,
    reason = "original independent financial read authorities and lifecycle remain explicit"
)]
pub(super) async fn read_source_history(
    reader: &AnalyticalReadCapability,
    research: &ResearchService,
    calendars: &CompletedMarketSessionReadCapability,
    completed_sessions: Option<&CompletedMarketSessionAuthority>,
    instrument_id: InstrumentId,
    currency: Currency,
    as_of: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<
    Result<SourceHistory, PortfolioAnalysisRiskUnavailableReason>,
    PortfolioApplicationServiceError,
> {
    let request = LatestCanonicalMarketBarHistoryWindowRequest::try_new(
        instrument_id,
        MarketHistorySelectionPolicy::COMPLETE_DAILY_ADJUSTED_V1,
        as_of,
    )
    .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    let selected = reader
        .select_latest_canonical_market_bar_history_window(request, deadline, cancellation)
        .map_err(|error| source_error(map_analytical_error(error)))?;
    let Some(selected) = selected else {
        return Ok(Err(
            PortfolioAnalysisRiskUnavailableReason::MarketHistoryUnavailable { instrument_id },
        ));
    };
    let output = reader
        .read_canonical_market_bar_history(
            selected.into_exact_request(),
            deadline,
            cancellation.clone(),
        )
        .await
        .map_err(|error| source_error(map_analytical_error(error)))?;
    let Some(output) = output else {
        return Err(PortfolioApplicationServiceError::StateChanged);
    };
    // Native dates are joined only to the exact calendar kept by this publication.
    // This is a controlled replay, never a provider call or replacement-calendar selection.
    let calendar = if let Some(graph) = output.selection().receipt().date_windows() {
        let retained = graph.calendar();
        let reference = CompletedMarketSessionReference::try_from_retained_digests(
            retained.origin_content_digest,
            retained.capture_binding_digest,
        )
        .map_err(calendar_error)?;
        Some(
            calendars
                .read_reference(&reference, as_of, deadline, cancellation.clone())
                .await
                .map_err(calendar_error)?
                .ok_or(PortfolioApplicationServiceError::StateChanged)?,
        )
    } else {
        None
    };
    let output = if let Some(calendar) = &calendar {
        let joined = research
            .rejoin_market_history_native_sessions_with_calendar(
                output,
                calendar,
                deadline,
                cancellation,
            )
            .await;
        ensure_live(deadline, cancellation)?;
        joined.map_err(|error| source_error(map_research_error(error)))?
    } else {
        output
    };
    let receipt = output.selection().receipt();
    if receipt.instrument_id() != instrument_id || !receipt.current_research_eligible() {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    if receipt.currency() != currency {
        return Ok(Err(
            PortfolioAnalysisRiskUnavailableReason::MarketHistoryCurrencyMismatch { instrument_id },
        ));
    }
    if receipt.adjustment() != MarketBarAdjustment::All {
        return Ok(Err(
            PortfolioAnalysisRiskUnavailableReason::MarketHistoryAdjustmentUnsupported {
                instrument_id,
            },
        ));
    }
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-risk-source-history/v1\0");
    digest.update(instrument_id.as_uuid().as_bytes());
    digest.update(output.selection().selection_digest().bytes());
    digest.update(receipt.receipt_digest().bytes());
    digest.update(output.read_receipt().result_digest().bytes());
    digest.update(output.read_receipt().history_content_digest().bytes());
    digest.update([
        u8::from(receipt.point_in_time_eligible()),
        u8::from(receipt.current_research_eligible()),
    ]);
    let qualification = if let Some(calendar) = &calendar {
        Some(
            HistoryCurrentSessionQualification::qualify_nominal(&output, calendar, as_of)
                .map_err(calendar_error)?,
        )
    } else if let Some(authority) = completed_sessions {
        Some(qualify_history_current_session(&output, authority, as_of).map_err(calendar_error)?)
    } else {
        None
    };
    if let Some(qualification) = &qualification {
        digest.update([1]);
        canonical_evidence(&mut digest, qualification.evidence_digest());
    } else {
        digest.update([0]);
    }
    Ok(Ok(SourceHistory {
        output,
        qualification,
        digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
    }))
}

/// A nominal source date has no provider completion instant. Its economic session coordinate
/// comes only from the exact original history's independently evidenced calendar association.
pub(super) fn source_session_close(
    output: &CompleteMarketBarHistoryOutput,
    bar: &MarketBarObservation,
) -> Result<Timestamp, PortfolioApplicationServiceError> {
    if let Some(completed_at) = bar.completed_at() {
        return Ok(completed_at);
    }
    let nominal = bar
        .time_semantics()
        .nominal_daily_date()
        .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
    let sessions = output
        .native_sessions()
        .ok_or(PortfolioApplicationServiceError::CorruptPublication)?
        .sessions();
    let session = sessions
        .find_date(nominal.date())
        .map_err(|error| source_error(map_analytical_error(error)))?
        .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
    if !session.bar_present()
        || session.provider_timestamp().is_some()
        || session.provider_period().is_some()
        || session.opens_at() >= session.closes_at_exclusive()
        || bar.context().time().effective().calendar_date_value() != Some(nominal.date())
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    Ok(session.closes_at_exclusive())
}

fn scenario_digest(scenario: &PortfolioHistoricalScenarioEvidence) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-current-weight-adjusted-return-scenarios/v1\0");
    canonical_evidence(&mut digest, scenario.source_set_digest);
    digest.update([u8::from(scenario.current_session_covered)]);
    canonical_money(&mut digest, scenario.cash);
    canonical_money(&mut digest, scenario.receivables);
    canonical_money(&mut digest, scenario.marked_equity);
    digest.update((scenario.holdings as u64).to_be_bytes());
    digest.update((scenario.source_observations as u64).to_be_bytes());
    digest.update((scenario.periods.len() as u64).to_be_bytes());
    for period in &scenario.periods {
        digest.update(period.opening_at.unix_nanos().to_be_bytes());
        digest.update(period.closing_at.unix_nanos().to_be_bytes());
        canonical_decimal(&mut digest, period.weighted_return);
    }
    canonical_text(
        &mut digest,
        "one_trading_session;fixed_current_weights;source_all_adjustments;constant_reporting_currency_cash_and_receivables;no_cash_interest;no_inflation;not_experienced_returns;not_total_return_index;not_historical_pit",
    );
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

pub(super) fn ensure_live(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), PortfolioApplicationServiceError> {
    if cancellation.is_cancelled() {
        Err(PortfolioApplicationServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(PortfolioApplicationServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn source_error(error: ServiceError) -> PortfolioApplicationServiceError {
    match error {
        ServiceError::Cancelled => PortfolioApplicationServiceError::Cancelled,
        ServiceError::DeadlineExceeded => PortfolioApplicationServiceError::DeadlineExceeded,
        ServiceError::ResourceExhausted => PortfolioApplicationServiceError::ResourceExhausted,
        ServiceError::InvalidRequest => PortfolioApplicationServiceError::InvalidRequest,
        ServiceError::NotFound => PortfolioApplicationServiceError::NotFound,
        ServiceError::Unavailable | ServiceError::Unauthorized => {
            PortfolioApplicationServiceError::Authority
        }
        _ => PortfolioApplicationServiceError::CorruptPublication,
    }
}

pub(super) fn calendar_error(
    error: CompletedMarketSessionError,
) -> PortfolioApplicationServiceError {
    match error {
        CompletedMarketSessionError::InvalidRequest => {
            PortfolioApplicationServiceError::InvalidRequest
        }
        CompletedMarketSessionError::InvalidEvidence => {
            PortfolioApplicationServiceError::CorruptPublication
        }
        CompletedMarketSessionError::ResourceBoundExceeded => {
            PortfolioApplicationServiceError::ResourceExhausted
        }
        CompletedMarketSessionError::Unavailable => PortfolioApplicationServiceError::StateChanged,
        CompletedMarketSessionError::Cancelled => PortfolioApplicationServiceError::Cancelled,
        CompletedMarketSessionError::DeadlineExceeded => {
            PortfolioApplicationServiceError::DeadlineExceeded
        }
    }
}
