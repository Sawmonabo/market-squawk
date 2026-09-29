//! Measured raw-price returns from original epochs and later source-owned share coverage.

use super::*;
use crate::application::research::corporate_actions::source_split_adjusted_close;
use market_squawk_data::{
    AnalyticalReadCapability, DatasetManifestRef, FeatureDatasetInputCoordinate,
    OutcomeMarketBarRequest, OutcomeMarketBarSelection, OutcomeMarketBarSeries,
};
use market_squawk_domain::{DataQuality, MarketBarAdjustment, MarketBarObservation};
use market_squawk_modeling::ForecastArtifactManifestRecord;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Inert closed evidence. Only read_current_forecast_outcome can reproduce its source authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SourceForecastOutcomeEvidence {
    pub source_action_reference: SourceAppliedCorporateActionPlanReference,
    pub outcome_manifest: ForecastArtifactManifestRecord,
    pub original_origin: MarketBarObservation,
    pub reread_origin: MarketBarObservation,
    pub target: MarketBarObservation,
    pub origin_at: Timestamp,
    pub target_at: Timestamp,
    pub origin_session_date: CalendarDate,
    pub target_session_date: CalendarDate,
    pub origin_receipt_digest: EvidenceDigest,
    pub target_receipt_digest: EvidenceDigest,
    pub input_epoch_sha256: [u8; 32],
    pub adjusted_origin_close: Decimal,
    pub adjusted_target_close: Decimal,
}
impl SourceForecastOutcomeEvidence {
    pub(crate) fn measured_return(&self) -> Result<Decimal, ApplicableActionPlanError> {
        self.adjusted_target_close
            .checked_div(self.adjusted_origin_close)
            .and_then(|ratio| ratio.checked_sub(Decimal::ONE))
            .ok_or(ApplicableActionPlanError::InvalidEvidence)
    }
    pub(crate) fn available_at(&self) -> Timestamp {
        // The complete source query, all raw observations and calendar applications were actually
        // available at this retained later seal. Bar publication alone cannot predate action proof.
        self.source_action_reference.knowledge_cutoff()
    }
}

impl SourceAppliedCorporateActionReadCapability {
    /// Reopens all original captures/calendars before selecting an exact completed target. A
    /// caller's reference is only a lookup coordinate, never a declaration of complete actions.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_current_forecast_outcome(
        &self,
        reference: &SourceAppliedCorporateActionPlanReference,
        coordinate: FeatureDatasetInputCoordinate<'_>,
        outcome_manifest: &DatasetManifestRef,
        as_of: Timestamp,
        analytical: &AnalyticalReadCapability,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<SourceForecastOutcomeEvidence>, ApplicableActionPlanError> {
        check(deadline, &cancellation)?;
        let epoch = coordinate.epoch();
        let (Some(origin), Some(origin_at), Some(target_at)) =
            (epoch.market_bar(), epoch.target_origin(), epoch.target_at())
        else {
            return Ok(None);
        };
        let cutoff = reference.knowledge_cutoff();
        if target_at > cutoff
            || cutoff > as_of
            || epoch.source_selection_as_of() >= target_at
            || origin.adjustment() != MarketBarAdjustment::Raw
        {
            return Ok(None);
        }
        let Some(source) = self
            .read_reference(reference, deadline, cancellation.clone())
            .await?
        else {
            return Ok(None);
        };
        let plan = match source.covered_price_plan() {
            Ok(plan) => plan,
            Err(
                ApplicableActionPlanError::IncompleteOrdinaryCoverage
                | ApplicableActionPlanError::UnresolvedApplicableActions,
            ) => return Ok(None),
            Err(error) => return Err(error),
        };
        let policy = CorporateActionPolicy::new(
            market_squawk_data::CorporateActionAdjustment::SplitAdjusted,
            std::num::NonZeroU32::MIN,
        );
        let coverage = source
            .ordinary_coverage()
            .ok_or(ApplicableActionPlanError::IncompleteOrdinaryCoverage)?;
        let mut histories = coverage.reads().iter().filter(|(read, _)| {
            read.history().selection().receipt().instrument_id() == epoch.instrument_id()
        });
        let Some((ordinary, _)) = histories.next() else {
            return Ok(None);
        };
        if histories.next().is_some() {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let history = ordinary.history();
        let (reread_origin, target, origin_date, target_date, origin_digest, target_digest) =
            if let Some(native) = epoch.named_session_origin() {
                if history.selection().pinned().manifest() != outcome_manifest {
                    return Ok(None);
                }
                let sessions = history
                    .native_sessions()
                    .ok_or(ApplicableActionPlanError::InvalidEvidence)?
                    .sessions();
                let mut origin_sessions = sessions
                    .iter()
                    .filter(|session| session.closes_at_exclusive() == origin_at);
                let mut target_sessions = sessions
                    .iter()
                    .filter(|session| session.closes_at_exclusive() == target_at);
                let (Some(os), Some(ts)) = (origin_sessions.next(), target_sessions.next()) else {
                    return Ok(None);
                };
                if origin_sessions.next().is_some()
                    || target_sessions.next().is_some()
                    || !os.bar_present()
                    || !ts.bar_present()
                    || os.provider_timestamp().is_some()
                    || ts.provider_timestamp().is_some()
                    || os.provider_period().is_some()
                    || ts.provider_period().is_some()
                    || native.closes_at_exclusive() != origin_at
                    || native.opens_at() != os.opens_at()
                    || native.native_date() != os.native_date()
                {
                    return Ok(None);
                }
                let Some(left) = unique_nominal_bar(history.bars(), origin, os.native_date())?
                else {
                    return Ok(None);
                };
                let Some(right) = unique_nominal_bar(history.bars(), origin, ts.native_date())?
                else {
                    return Ok(None);
                };
                let receipt = EvidenceDigest::new(
                    market_squawk_domain::DigestAlgorithm::Sha256,
                    history.selection().receipt().receipt_digest().bytes(),
                );
                (
                    left.clone(),
                    right.clone(),
                    os.native_date(),
                    ts.native_date(),
                    receipt,
                    receipt,
                )
            } else {
                let Some(exact) = origin.time_semantics().timestamped_period() else {
                    return Ok(None);
                };
                let Some(venue) = origin.context().provenance().venue_id() else {
                    return Ok(None);
                };
                if venue != source.calendar().venue_id() {
                    return Ok(None);
                }
                let series = OutcomeMarketBarSeries::new(
                    epoch.instrument_id(),
                    origin.context().provenance().source_id().clone(),
                    venue.clone(),
                    origin.provider_instrument_id().clone(),
                    origin.feed().clone(),
                    origin.interval().clone(),
                    MarketBarAdjustment::Raw,
                    exact.timestamp_basis(),
                    exact.session().kind(),
                    exact.session().ruleset().clone(),
                );
                let select = |at| {
                    OutcomeMarketBarRequest::try_new(
                        outcome_manifest.clone(),
                        series.clone(),
                        cutoff,
                        at,
                        at,
                    )
                    .map_err(|error| ApplicableActionPlanError::SourceRead(map_analytical_error(error)))
                };
                let left = analytical
                    .select_outcome_market_bar(select(origin_at)?, deadline, cancellation.clone())
                    .await
                    .map_err(|error| ApplicableActionPlanError::SourceRead(map_analytical_error(error)))?;
                let right = analytical
                    .select_outcome_market_bar(select(target_at)?, deadline, cancellation.clone())
                    .await
                    .map_err(|error| ApplicableActionPlanError::SourceRead(map_analytical_error(error)))?;
                let (
                    OutcomeMarketBarSelection::Selected(left),
                    OutcomeMarketBarSelection::Selected(right),
                ) = (left, right)
                else {
                    return Ok(None);
                };
                let Some(od) =
                    timestamp_session(&source, left.bar(), cutoff, deadline, &cancellation)?
                else {
                    return Ok(None);
                };
                let Some(td) =
                    timestamp_session(&source, right.bar(), cutoff, deadline, &cancellation)?
                else {
                    return Ok(None);
                };
                (
                    left.bar().clone(),
                    right.bar().clone(),
                    od,
                    td,
                    left.receipt_digest(),
                    right.receipt_digest(),
                )
            };
        if origin_date < source.interval().0
            || target_date > source.interval().1
            || origin_date >= target_date
            || !same_raw_series(origin, &reread_origin)
            || !same_raw_series(origin, &target)
            || !same_raw_coordinate(origin, &reread_origin)
            || !anchor::same_reported_values(origin, &reread_origin)
            || !eligible(&reread_origin, cutoff)
            || !eligible(&target, cutoff)
        {
            return Ok(None);
        }
        // Reproduce the original unit frame using the SAME admitted later pool. A revised raw
        // origin or changed pre-decision split frame cannot silently become the original forecast.
        let decision = epoch
            .decision_at()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        let limits = plan
            .source_split_projection_limits(policy, epoch.instrument_id(), cutoff, decision)
            .map_err(|error| map_source_plan_error(error, deadline, &cancellation))?;
        let original_plan = plan
            .try_project_source_split_plan(policy, epoch.instrument_id(), cutoff, decision, limits)
            .map_err(|error| map_source_plan_error(error, deadline, &cancellation))?;
        if source_split_adjusted_close(&original_plan, &reread_origin, origin_at)?
            != epoch
                .current_unit_price()
                .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?
                .amount()
        {
            return Ok(None);
        }
        let limits = plan
            .source_split_projection_limits(policy, epoch.instrument_id(), cutoff, target_at)
            .map_err(|error| map_source_plan_error(error, deadline, &cancellation))?;
        let target_plan = plan
            .try_project_source_split_plan(policy, epoch.instrument_id(), cutoff, target_at, limits)
            .map_err(|error| map_source_plan_error(error, deadline, &cancellation))?;
        let adjusted_origin_close =
            source_split_adjusted_close(&target_plan, &reread_origin, origin_at)?;
        let adjusted_target_close = source_split_adjusted_close(&target_plan, &target, target_at)?;
        if adjusted_origin_close <= Decimal::ZERO || adjusted_target_close <= Decimal::ZERO {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        use sha2::Digest as _;
        let input_epoch_sha256 = sha2::Sha256::digest(
            epoch
                .canonical_bytes()
                .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?,
        )
        .into();
        check(deadline, &cancellation)?;
        Ok(Some(SourceForecastOutcomeEvidence {
            source_action_reference: source.price_reference()?,
            outcome_manifest: ForecastArtifactManifestRecord::from_manifest(outcome_manifest),
            original_origin: origin.clone(),
            reread_origin,
            target,
            origin_at,
            target_at,
            origin_session_date: origin_date,
            target_session_date: target_date,
            origin_receipt_digest: origin_digest,
            target_receipt_digest: target_digest,
            input_epoch_sha256,
            adjusted_origin_close,
            adjusted_target_close,
        }))
    }
}

fn unique_nominal_bar<'a>(
    bars: &'a [MarketBarObservation],
    series: &MarketBarObservation,
    date: CalendarDate,
) -> Result<Option<&'a MarketBarObservation>, ApplicableActionPlanError> {
    let mut rows = bars.iter().filter(|bar| {
        same_raw_series(series, bar)
            && bar
                .time_semantics()
                .nominal_daily_date()
                .is_some_and(|value| value.date() == date)
    });
    let selected = rows.next();
    if rows.next().is_some() {
        return Err(ApplicableActionPlanError::InvalidEvidence);
    }
    Ok(selected)
}
fn same_raw_series(a: &MarketBarObservation, b: &MarketBarObservation) -> bool {
    a.context().provenance().instrument_id() == b.context().provenance().instrument_id()
        && a.context().provenance().source_id() == b.context().provenance().source_id()
        && a.context().provenance().venue_id() == b.context().provenance().venue_id()
        && a.provider_instrument_id() == b.provider_instrument_id()
        && a.feed() == b.feed()
        && a.interval() == b.interval()
        && a.currency() == b.currency()
        && a.adjustment() == MarketBarAdjustment::Raw
        && b.adjustment() == MarketBarAdjustment::Raw
        && match (a.time_semantics(), b.time_semantics()) {
            (
                market_squawk_domain::BarTimeSemantics::TimestampedPeriod(a),
                market_squawk_domain::BarTimeSemantics::TimestampedPeriod(b),
            ) => a.session().kind() == b.session().kind()
                && a.session().ruleset() == b.session().ruleset(),
            (
                market_squawk_domain::BarTimeSemantics::NominalDailyDate(a),
                market_squawk_domain::BarTimeSemantics::NominalDailyDate(b),
            ) => a.ruleset() == b.ruleset(),
            _ => false,
        }
}
fn same_raw_coordinate(a: &MarketBarObservation, b: &MarketBarObservation) -> bool {
    match (
        a.time_semantics().nominal_daily_date(),
        b.time_semantics().nominal_daily_date(),
    ) {
        // Each source payload is independently replayed. Tiingo's row digest also includes
        // adjusted values, which can change after a real later split while raw values stay exact.
        (Some(a), Some(b)) => a.date() == b.date() && a.ruleset() == b.ruleset(),
        (None, None) => anchor::same_coordinate(a, b),
        _ => false,
    }
}
fn eligible(bar: &MarketBarObservation, cutoff: Timestamp) -> bool {
    let p = bar.context().provenance();
    !matches!(
        p.quality(),
        DataQuality::Modeled | DataQuality::Stale | DataQuality::Quarantined
    ) && p
        .availability()
        .conservative_available_at()
        .is_some_and(|at| at <= cutoff)
        && p.received_at() <= cutoff
        && p.ingested_at() <= cutoff
}
fn timestamp_session(
    source: &SourceAppliedCorporateActionPlan,
    bar: &MarketBarObservation,
    cutoff: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<CalendarDate>, ApplicableActionPlanError> {
    let Some(exact) = bar.time_semantics().timestamped_period() else {
        return Ok(None);
    };
    let mut found = None;
    for native in source.calendar().native_session_replay().sessions() {
        check(deadline, cancellation)?;
        let Some(session) = source
            .calendar()
            .date_session_on(native.date(), cutoff, cutoff)
        else {
            continue;
        };
        let Some(period) = session.provider_period().and_then(|p| p.timestamped_period()) else {
            continue;
        };
        if period.period_start() == exact.period_start()
            && period.period_end_exclusive() == exact.period_end_exclusive()
            && period.provider_timestamp() == exact.provider_timestamp()
            && period.timestamp_basis() == exact.timestamp_basis()
            && period.session().kind() == exact.session().kind()
            && period.session().ruleset() == exact.session().ruleset()
        {
            if found.replace(session.date()).is_some() {
                return Err(ApplicableActionPlanError::InvalidEvidence);
            }
        }
    }
    Ok(found)
}
