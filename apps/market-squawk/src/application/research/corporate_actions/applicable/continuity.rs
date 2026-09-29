//! Source-as-known continuity of one immutable forecast's native share-price frame.
//!
//! A fresh, unchanged native Split anchor supplies the current source adjustment frame. Actual
//! Raw and Split price/volume fields and exact original coordinates remain retained. No saved
//! forecast value is divided, multiplied, or silently moved into a replacement source epoch.

use super::*;
use crate::application::{
    market_selection::{MarketInvestmentReadReceipt, MarketInvestmentReadReference},
    model::forecast::{ExactHorizonPriceForecastProjection, LatestValidForecast},
};
use market_squawk_domain::{CorporateActionSourceCategory, DigestAlgorithm, MarketBarAdjustment};
use sha2::{Digest as _, Sha256};

/// The source-scoped, explicitly versioned interpretation, including provider delay/correction
/// limitations. A future correction may invalidate a later read; it does not rewrite this one.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum UnitContinuityScope {
    AlpacaFreshSplitFrameAndObservedLifecycleQueryV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceForecastUnitContinuityReference {
    version: u16,
    scope: UnitContinuityScope,
    instrument_id: InstrumentId,
    currency: market_squawk_domain::Currency,
    source_plan: SourceAppliedCorporateActionPlanReference,
    forecast_vintage: EvidenceDigest,
    forecast_distribution: EvidenceDigest,
    forecast_price_derivation: EvidenceDigest,
    forecast_artifact: EvidenceDigest,
    original_source_result: EvidenceDigest,
    original_origin: EvidenceDigest,
    original_knowledge_cutoff: Timestamp,
    origin_completed_at: Timestamp,
    origin_native_date: CalendarDate,
    final_market: MarketInvestmentReadReference,
    final_observation_identity: EvidenceDigest,
    final_source_cutoff: Timestamp,
    final_market_policy: EvidenceDigest,
    maximum_capture_age_nanos: u64,
    raw_origin_received_at: Timestamp,
    split_origin_received_at: Timestamp,
    lifecycle_summary_received_at: Timestamp,
    final_native_date: CalendarDate,
    final_session: EvidenceDigest,
}
/// Private construction requires all genuine source, forecast, mark and calendar reads.
pub(crate) struct SourceForecastUnitContinuity {
    reference: SourceForecastUnitContinuityReference,
    evidence_digest: EvidenceDigest,
}
impl SourceForecastUnitContinuity {
    pub(crate) const fn reference(&self) -> &SourceForecastUnitContinuityReference {
        &self.reference
    }
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.reference.instrument_id
    }
    pub(crate) const fn currency(&self) -> market_squawk_domain::Currency {
        self.reference.currency
    }
    pub(crate) const fn forecast_vintage(&self) -> EvidenceDigest {
        self.reference.forecast_vintage
    }
    pub(crate) const fn price_derivation_identity(&self) -> EvidenceDigest {
        self.reference.forecast_price_derivation
    }
    pub(crate) const fn original_source_cutoff(&self) -> Timestamp {
        self.reference.original_knowledge_cutoff
    }
    pub(crate) const fn final_observation_identity(&self) -> EvidenceDigest {
        self.reference.final_observation_identity
    }
    pub(crate) const fn final_source_cutoff(&self) -> Timestamp {
        self.reference.final_source_cutoff
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum SourceForecastUnitContinuityError {
    InvalidEvidence,
    MissingFreshSourceAnchor,
    OriginalCoordinateUnavailable,
    FinalSessionUnavailable,
    SourceCaptureOutsideFreshnessBound,
    OriginalSourceFrameChanged,
    SourceShareRelationUnavailable,
    KnownShareOrLifecycleChange,
    UnresolvedApplicableLifecycle,
    Interrupted,
}

impl SourceAppliedCorporateActionPlan {
    /// Shared finite preparation/final-read check using the existing market policy age. All page
    /// clocks are genuine retained observations, including empty pages. This is not unit authority.
    pub(crate) fn require_capture_freshness(
        &self,
        original_knowledge_cutoff: Timestamp,
        maximum_capture_age_nanos: u64,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceForecastUnitContinuityError> {
        live(deadline, cancellation)?;
        let invalid = SourceForecastUnitContinuityError::InvalidEvidence;
        let final_cutoff = self.source.knowledge_cutoff();
        if maximum_capture_age_nanos == 0 || original_knowledge_cutoff > final_cutoff {
            return Err(invalid);
        }
        let earliest_capture = final_cutoff
            .checked_sub_nanos(i64::try_from(maximum_capture_age_nanos).map_err(|_| invalid)?)
            .map_err(|_| invalid)?;
        let anchor = self
            .anchor
            .as_ref()
            .ok_or(SourceForecastUnitContinuityError::MissingFreshSourceAnchor)?;
        for &received_at in anchor
            .raw_page_received_at
            .iter()
            .chain(anchor.split_page_received_at.iter())
            .chain(self.source.capture_page_received_at().iter())
        {
            live(deadline, cancellation)?;
            if received_at < earliest_capture
                || received_at > final_cutoff
                || received_at < original_knowledge_cutoff
            {
                return Err(SourceForecastUnitContinuityError::SourceCaptureOutsideFreshnessBound);
            }
        }
        Ok(())
    }

    /// Compares the actual immutable forecast and mark with freshly re-read source adjustment
    /// evidence. An unchanged catalog revision is never sufficient; every accepted result retains
    /// the two original source publications, actual native values and original forecast identity.
    pub(crate) fn check_forecast_unit_continuity(
        &self,
        forecast: &LatestValidForecast,
        projection: ExactHorizonPriceForecastProjection<'_>,
        market: &MarketInvestmentReadReceipt,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SourceForecastUnitContinuity, SourceForecastUnitContinuityError> {
        live(deadline, cancellation)?;
        let invalid = SourceForecastUnitContinuityError::InvalidEvidence;
        let distribution = forecast.selected_distribution().ok_or(invalid)?;
        let serving = distribution.serving_binding();
        let origin = serving
            .origin_bar()
            .ok_or(SourceForecastUnitContinuityError::OriginalCoordinateUnavailable)?;
        let origin_time = origin
            .time_semantics()
            .timestamped_period()
            .ok_or(SourceForecastUnitContinuityError::OriginalCoordinateUnavailable)?;
        let observation = market.observation().map_err(|_| invalid)?;
        let mark = observation.mark();
        let instrument = projection.instrument_id();
        let final_cutoff = observation.selected_at();
        let mark_at = observation.timestamps().effective_at();
        let final_market = market.reference();
        let maximum_capture_age_nanos =
            final_market.maximum_mark_age_nanos().map_err(|_| invalid)?;
        if distribution.vintage_id() != projection.vintage_id()
            || forecast.selection_receipt().receipt_digest()
                != projection.selection_receipt_digest()
            || serving.knowledge_cutoff() != projection.source_knowledge_cutoff()
            || serving.observed_through() != Some(projection.observed_through())
            || origin_time.period_end_exclusive() != projection.observed_through()
            || origin.context().provenance().source_id() != serving.source_id()
            || origin.context().provenance().instrument_id() != Some(instrument)
            || origin.adjustment() != MarketBarAdjustment::Split
            || origin.currency() != projection.currency()
            || mark.currency() != projection.currency()
            || observation.instrument_id() != instrument
            || market.instrument_id() != instrument
            || market.currency() != projection.currency()
            || !matches!(
                market.asset_class(),
                market_squawk_domain::AssetClass::Equity | market_squawk_domain::AssetClass::Fund
            )
            || self.source.knowledge_cutoff() != final_cutoff
            || self.evaluated_at < final_cutoff
            || serving.knowledge_cutoff() > mark_at
            || mark_at > final_cutoff
            || !self.requested_instruments.contains(&instrument)
        {
            return Err(invalid);
        }
        let anchor = self
            .anchor
            .as_ref()
            .ok_or(SourceForecastUnitContinuityError::MissingFreshSourceAnchor)?;
        let index = anchor
            .raw
            .bars()
            .iter()
            .position(|bar| anchor::same_coordinate(origin, bar))
            .ok_or(SourceForecastUnitContinuityError::OriginalCoordinateUnavailable)?;
        let raw = &anchor.raw.bars()[index];
        let split = &anchor.split.bars()[index];
        // These are independently acquired source snapshots. Reuse the exact final market
        // configuration's age bound at one immutable cutoff; never chase a newer tick or imply
        // that the captures and market event happened atomically. The original manifests retain
        // the complete physical page clocks, including pages without an economic observation.
        self.require_capture_freshness(
            serving.knowledge_cutoff(),
            maximum_capture_age_nanos,
            deadline,
            cancellation,
        )?;
        if !anchor::same_reported_values(origin, split) {
            return Err(SourceForecastUnitContinuityError::OriginalSourceFrameChanged);
        }
        if !anchor::reciprocal_price_volume(raw, split) {
            return Err(SourceForecastUnitContinuityError::SourceShareRelationUnavailable);
        }
        let native = anchor
            .raw
            .native_sessions()
            .ok_or(invalid)?
            .sessions()
            .iter()
            .find(|session| {
                session.provider_timestamp() == Some(origin_time.provider_timestamp())
                    && session.provider_period()
                        == Some((
                            origin_time.period_start(),
                            origin_time.period_end_exclusive(),
                        ))
                    && session.bar_present()
            })
            .ok_or(SourceForecastUnitContinuityError::OriginalCoordinateUnavailable)?;
        let origin_date = native.native_date();
        // The exact captured calendar supplies the nominal current date. No timezone conversion,
        // guessed midnight, holiday rolling, or daily aggregation timestamp is used as an open.
        let final_session = session_containing(
            &self.calendar,
            self.interval,
            mark_at,
            final_cutoff,
            self.evaluated_at,
            deadline,
            cancellation,
        )?
        .ok_or(SourceForecastUnitContinuityError::FinalSessionUnavailable)?;
        let final_date = final_session.date();
        if self.calendar.venue_id() != anchor.raw.selection().receipt().venue_id()
            || origin_date > final_date
            || origin_date < self.interval.0
            || final_date > self.interval.1
        {
            return Err(invalid);
        }
        // Complete Tiingo native share fields corroborate every retained completed date when
        // present. The fresh provider Split frame covers its as-known adjustment relation through
        // the current capture; it does not pretend an unfinished EOD row already exists.
        if let Some(coverage) = &self.ordinary {
            for (read, _) in coverage.reads() {
                if read.history().selection().receipt().instrument_id() != instrument {
                    continue;
                }
                for row in read.actions().rows() {
                    if row.date <= origin_date || row.date > final_date {
                        continue;
                    }
                    match row.shares {
                        market_squawk_adapter_tiingo::TiingoEodActionFieldDisposition::ExplicitNoEvent => {},
                        market_squawk_adapter_tiingo::TiingoEodActionFieldDisposition::Normalized { .. } =>
                            return Err(SourceForecastUnitContinuityError::KnownShareOrLifecycleChange),
                        _ => return Err(SourceForecastUnitContinuityError::UnresolvedApplicableLifecycle),
                    }
                }
            }
        }
        // Exact observed lifecycle query, including every retained unresolved family. Cash-only
        // denomination/payment/ordinary classification is unrelated to share-unit continuity and
        // cannot create an artificial currency dependency for an otherwise valid source anchor.
        for descriptor in self.source.source_actions() {
            live(deadline, cancellation)?;
            let CorporateActionSourcePayload::ReturnedAction {
                category, dates, ..
            } = descriptor.payload()
            else {
                return Err(invalid);
            };
            if descriptor
                .context()
                .provenance()
                .instrument_id()
                .is_some_and(|id| id != instrument)
            {
                continue;
            }
            if matches!(
                category,
                CorporateActionSourceCategory::CashDividend
                    | CorporateActionSourceCategory::CapitalGainsDistribution
                    | CorporateActionSourceCategory::NameChange
            ) {
                continue;
            }
            let date = dates
                .economic_date()
                .ok_or(SourceForecastUnitContinuityError::UnresolvedApplicableLifecycle)?;
            if date <= origin_date || date > final_date {
                continue;
            }
            if descriptor.context().provenance().instrument_id() != Some(instrument) {
                return Err(SourceForecastUnitContinuityError::UnresolvedApplicableLifecycle);
            }
            return Err(SourceForecastUnitContinuityError::KnownShareOrLifecycleChange);
        }
        let reference = SourceForecastUnitContinuityReference {
            version: 1,
            scope: UnitContinuityScope::AlpacaFreshSplitFrameAndObservedLifecycleQueryV1,
            instrument_id: instrument,
            currency: projection.currency(),
            source_plan: self.source_reference().map_err(|_| invalid)?,
            forecast_vintage: digest(projection.vintage_id().bytes()),
            forecast_distribution: digest(distribution.identity().bytes()),
            forecast_price_derivation: digest(projection.price_derivation_identity().bytes()),
            forecast_artifact: digest(serving.forecast_artifact_hash().bytes()),
            original_source_result: digest(serving.result_sha256().bytes()),
            original_origin: hash_value(
                b"market-squawk/original-forecast-unit-origin/v1\0",
                origin,
            )?,
            original_knowledge_cutoff: serving.knowledge_cutoff(),
            origin_completed_at: origin_time.period_end_exclusive(),
            origin_native_date: origin_date,
            final_market,
            final_observation_identity: mark.evidence_identity(),
            final_source_cutoff: final_cutoff,
            final_market_policy: market.selection().policy_digest(),
            maximum_capture_age_nanos,
            raw_origin_received_at: raw.context().provenance().received_at(),
            split_origin_received_at: split.context().provenance().received_at(),
            lifecycle_summary_received_at: self
                .source
                .summary()
                .context()
                .provenance()
                .received_at(),
            final_native_date: final_date,
            final_session: final_session.evidence_digest(),
        };
        let evidence_digest = hash_value(
            b"market-squawk/source-forecast-unit-continuity/v1\0",
            &reference,
        )?;
        live(deadline, cancellation)?;
        Ok(SourceForecastUnitContinuity {
            reference,
            evidence_digest,
        })
    }
}
impl SourceAppliedCorporateActionReadCapability {
    /// Reopens exact source roots before checking current borrowed forecast/market authorities.
    pub(crate) async fn read_forecast_unit_continuity(
        &self,
        reference: &SourceAppliedCorporateActionPlanReference,
        forecast: &LatestValidForecast,
        projection: ExactHorizonPriceForecastProjection<'_>,
        market: &MarketInvestmentReadReceipt,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SourceForecastUnitContinuity, SourceForecastUnitContinuityError> {
        let plan = self
            .read_reference(reference, deadline, cancellation.clone())
            .await
            .map_err(|_| SourceForecastUnitContinuityError::InvalidEvidence)?
            .ok_or(SourceForecastUnitContinuityError::InvalidEvidence)?;
        plan.check_forecast_unit_continuity(forecast, projection, market, deadline, &cancellation)
    }
    /// A decoded continuity reference is inert. Serving it requires the exact original forecast,
    /// final market and both source publications to be reopened and to reproduce every field.
    pub(crate) async fn revalidate_forecast_unit_continuity(
        &self,
        reference: &SourceForecastUnitContinuityReference,
        forecast: &LatestValidForecast,
        projection: ExactHorizonPriceForecastProjection<'_>,
        market: &MarketInvestmentReadReceipt,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SourceForecastUnitContinuity, SourceForecastUnitContinuityError> {
        let read = self
            .read_forecast_unit_continuity(
                &reference.source_plan,
                forecast,
                projection,
                market,
                deadline,
                cancellation,
            )
            .await?;
        if read.reference != *reference {
            return Err(SourceForecastUnitContinuityError::InvalidEvidence);
        }
        Ok(read)
    }
}
/// Iterate bounded civil-date query keys, admitting only an actual captured session receipt.
/// The keys carry no source authority and are never converted into invented timestamps.
fn session_containing(
    calendar: &SourcePlanCalendar,
    interval: (CalendarDate, CalendarDate),
    instant: Timestamp,
    cutoff: Timestamp,
    evaluated_at: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<SourcePlanCalendarDate>, SourceForecastUnitContinuityError> {
    use chrono::Datelike as _;
    let invalid = SourceForecastUnitContinuityError::InvalidEvidence;
    if interval.0 > interval.1
        || interval.1.days_since_unix_epoch() - interval.0.days_since_unix_epoch() > 64_000
    {
        return Err(invalid);
    }
    let mut key = chrono::NaiveDate::from_ymd_opt(
        i32::from(interval.0.year()),
        u32::from(interval.0.month()),
        u32::from(interval.0.day()),
    )
    .ok_or(invalid)?;
    loop {
        live(deadline, cancellation)?;
        let date = CalendarDate::new(
            u16::try_from(key.year()).map_err(|_| invalid)?,
            u8::try_from(key.month()).map_err(|_| invalid)?,
            u8::try_from(key.day()).map_err(|_| invalid)?,
        )
        .map_err(|_| invalid)?;
        if let Some(session) = calendar.date_session_on(date, cutoff, evaluated_at)
            && session.opens_at() <= instant
            && instant < session.closes_at_exclusive()
        {
            return Ok(Some(session));
        }
        if date == interval.1 {
            return Ok(None);
        }
        key = key.succ_opt().ok_or(invalid)?;
    }
}
fn digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}
fn hash_value<T: serde::Serialize>(
    namespace: &[u8],
    value: &T,
) -> Result<EvidenceDigest, SourceForecastUnitContinuityError> {
    let mut hash = Sha256::new();
    hash.update(namespace);
    hash.update(
        serde_json::to_vec(value)
            .map_err(|_| SourceForecastUnitContinuityError::InvalidEvidence)?,
    );
    Ok(digest(hash.finalize().into()))
}
fn live(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), SourceForecastUnitContinuityError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(SourceForecastUnitContinuityError::Interrupted)
    } else {
        Ok(())
    }
}
