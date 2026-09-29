//! Genuine common economic sessions with distinct source-native forecast origins.

use super::{
    CompletedMarketSessionError as Error, CompletedMarketSessionRead,
    CompletedMarketSessionReference,
};
use market_squawk_data::{
    CompleteMarketBarHistoryOutput, DatasetManifestRef, FeatureDatasetInputCoordinate,
    FixedHorizonOriginBasis, NominalDailyCurrentSource, Sha256Digest,
};
use market_squawk_domain::{
    BarTimeSemantics, CalendarDate, InstrumentId, MarketBarObservation, Timestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Inert transport coordinates. Only reopening the original physical calendar issues a cohort.
/// Nanosecond fields use decimal strings so Desktop transport cannot round the original values.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ForecastSessionCohortReference {
    calendar: CompletedMarketSessionReference,
    session_date: CalendarDate,
    regular_opens_at_unix_nanos: String,
    regular_closes_at_unix_nanos: String,
    aggregation_starts_at_unix_nanos: String,
    aggregation_ends_at_unix_nanos: String,
    aggregation_provider_timestamp_unix_nanos: String,
    aggregation_semantics_sha256: [u8; 32],
    calendar_session_evidence_sha256: [u8; 32],
    knowledge_cutoff_unix_nanos: String,
    horizon_nanos: String,
}
impl ForecastSessionCohortReference {
    pub(crate) const fn session_date(&self) -> CalendarDate { self.session_date }
    pub(crate) fn calendar(&self) -> &CompletedMarketSessionReference {
        &self.calendar
    }
    /// Inert economic equality for the acquisition barrier. Each reference still requires its
    /// own original physical calendar reopen; fresh capture identity and knowledge stay separate.
    pub(crate) fn same_economic_session(&self, other: &Self) -> bool {
        self.session_date == other.session_date
            && self.regular_opens_at_unix_nanos == other.regular_opens_at_unix_nanos
            && self.regular_closes_at_unix_nanos == other.regular_closes_at_unix_nanos
            && self.aggregation_starts_at_unix_nanos == other.aggregation_starts_at_unix_nanos
            && self.aggregation_ends_at_unix_nanos == other.aggregation_ends_at_unix_nanos
            && self.aggregation_provider_timestamp_unix_nanos == other.aggregation_provider_timestamp_unix_nanos
            && self.aggregation_semantics_sha256 == other.aggregation_semantics_sha256
            && self.horizon_nanos == other.horizon_nanos
    }
    pub(crate) fn knowledge_cutoff(&self) -> Result<Timestamp, Error> {
        Ok(Timestamp::from_unix_nanos(integer(
            &self.knowledge_cutoff_unix_nanos,
        )?))
    }
    pub(crate) fn horizon_nanos(&self) -> Result<i64, Error> {
        integer(&self.horizon_nanos)
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let open = integer(&self.regular_opens_at_unix_nanos)?;
        let close = integer(&self.regular_closes_at_unix_nanos)?;
        let start = integer(&self.aggregation_starts_at_unix_nanos)?;
        let end = integer(&self.aggregation_ends_at_unix_nanos)?;
        integer(&self.aggregation_provider_timestamp_unix_nanos)?;
        let cutoff = self.knowledge_cutoff()?.unix_nanos();
        let horizon = self.horizon_nanos()?;
        if open >= close
            || start > open
            || close > end
            || start >= end
            || end > cutoff
            || horizon <= 0
            || end.checked_add(horizon).is_none()
            || close.checked_add(horizon).is_none()
            || self.aggregation_semantics_sha256 == [0; 32]
            || self.calendar_session_evidence_sha256 == [0; 32]
        {
            return Err(Error::InvalidEvidence);
        }
        Ok(())
    }
    pub(crate) fn digest(&self) -> Result<Sha256Digest, Error> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| Error::InvalidEvidence)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/forecast-economic-session-cohort/v1\0");
        hash.update(bytes);
        Ok(Sha256Digest::new(hash.finalize().into()))
    }
}

/// Calendar-issued common session. No public constructor or Deserialize can issue this value.
#[derive(Clone, Debug)]
pub(crate) struct ForecastSessionCohort {
    reference: ForecastSessionCohortReference,
    provider_period: BarTimeSemantics,
    opens_at: Timestamp,
    closes_at: Timestamp,
    aggregation_end: Timestamp,
}

/// A source-issued origin mapped to the common economic session. The native timestamp is never
/// changed to the regular-session endpoint (or vice versa), and never replaced by acquisition time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastSessionOrigin {
    cohort: ForecastSessionCohortReference,
    instrument_id: InstrumentId,
    basis: FixedHorizonOriginBasis,
    observed_through: Timestamp,
    source_manifest: DatasetManifestRef,
    source_read_digest: Sha256Digest,
    source_origin_evidence_digest: Sha256Digest,
}
impl ForecastSessionOrigin {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub(crate) const fn observed_through(&self) -> Timestamp {
        self.observed_through
    }
    pub(crate) const fn basis(&self) -> FixedHorizonOriginBasis {
        self.basis
    }
    pub(crate) fn cohort(&self) -> &ForecastSessionCohortReference {
        &self.cohort
    }
    pub(crate) fn source_manifest(&self) -> &DatasetManifestRef {
        &self.source_manifest
    }
    pub(crate) const fn source_read_digest(&self) -> Sha256Digest {
        self.source_read_digest
    }
    pub(crate) const fn source_origin_evidence_digest(&self) -> Sha256Digest {
        self.source_origin_evidence_digest
    }
    pub(crate) fn retained_dynamic_bytes(&self) -> Result<usize, Error> {
        serde_json::to_vec(&self.cohort)
            .map_err(|_| Error::InvalidEvidence)?
            .len()
            .checked_mul(8)
            .and_then(|n| n.checked_add(self.source_manifest.dataset_id().as_str().len()))
            .and_then(|n| n.checked_add(self.source_manifest.schema().name().len()))
            .ok_or(Error::ResourceBoundExceeded)
    }
}

impl CompletedMarketSessionRead {
    /// Selects one original session whose genuine regular session AND admitted timestamp-bar
    /// aggregation are complete. A same-day regular close cannot stand in for an unfinished daily
    /// aggregation. No UTC/local date conversion or fixed-hours calendar rule is introduced here.
    pub(crate) fn latest_forecast_session_cohort(
        &self,
        knowledge_cutoff: Timestamp,
        horizon_nanos: i64,
        evaluated_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ForecastSessionCohort>, Error> {
        latest_forecast_session_cohort(self, knowledge_cutoff, horizon_nanos, evaluated_at, deadline, cancellation)
    }

    /// Call only after existing calendar capability reopens `expected.calendar()` physically.
    /// Reproduces the original economic selection; parsed coordinates alone cannot grant it.
    pub(crate) fn reopen_forecast_session_cohort(
        &self,
        expected: &ForecastSessionCohortReference,
        evaluated_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ForecastSessionCohort>, Error> {
        expected.validate()?;
        if self.reference() != expected.calendar() {
            return Err(Error::InvalidEvidence);
        }
        let Some(actual) = self.latest_forecast_session_cohort(
            expected.knowledge_cutoff()?,
            expected.horizon_nanos()?,
            evaluated_at,
            deadline,
            cancellation,
        )?
        else {
            return Ok(None);
        };
        if actual.reference != *expected {
            return Err(Error::InvalidEvidence);
        }
        Ok(Some(actual))
    }
}

/// Historical cohort reads share the exact original native-calendar selection algorithm.
#[derive(Clone)]
pub(crate) enum ForecastSessionReadCapability {
    Current(super::CompletedMarketSessionReadCapability),
    Retained(super::RetainedMarketSessionReadCapability),
}
impl ForecastSessionReadCapability {
    pub(crate) fn retained(&self) -> Self {
        match self {
            Self::Current(reader) => Self::Retained(reader.retained_read_capability()),
            Self::Retained(reader) => Self::Retained(reader.clone()),
        }
    }

    pub(crate) async fn read_cohort(
        &self, expected: &ForecastSessionCohortReference, deadline: Instant, cancellation: CancellationToken,
    ) -> Result<Option<ForecastSessionCohort>, Error> {
        let cutoff = expected.knowledge_cutoff()?;
        match self {
            Self::Current(reader) => {
                let Some(read) = reader.read_reference(expected.calendar(), cutoff, deadline, cancellation.clone()).await? else { return Ok(None); };
                read.reopen_forecast_session_cohort(expected, cutoff, deadline, &cancellation)
            }
            Self::Retained(reader) => {
                let Some(read) = reader.read_reference_with_job_context(expected.calendar(), cutoff, deadline, cancellation.clone(), None).await? else { return Ok(None); };
                expected.validate()?;
                if read.reference() != expected.calendar() { return Err(Error::InvalidEvidence); }
                let actual = latest_forecast_session_cohort(&read, cutoff, expected.horizon_nanos()?, cutoff, deadline, &cancellation)?;
                if actual.as_ref().is_some_and(|actual| actual.reference != *expected) { return Err(Error::InvalidEvidence); }
                Ok(actual)
            }
        }
    }
}

trait ForecastCalendarRead {
    fn reference(&self) -> &CompletedMarketSessionReference;
    fn native_session_replay(&self) -> &market_squawk_adapter_alpaca::AlpacaRetainedCalendarSessions;
    fn date_session_on(&self, date: CalendarDate, cutoff: Timestamp, evaluated: Timestamp) -> Option<ForecastCalendarDay>;
}
struct ForecastCalendarDay {
    date: CalendarDate, opens: Timestamp, closes: Timestamp,
    digest: market_squawk_domain::EvidenceDigest, period: Option<BarTimeSemantics>,
}
impl ForecastCalendarDay {
    fn date(&self) -> CalendarDate { self.date }
    fn opens_at(&self) -> Timestamp { self.opens }
    fn closes_at_exclusive(&self) -> Timestamp { self.closes }
    fn evidence_digest(&self) -> market_squawk_domain::EvidenceDigest { self.digest }
    fn provider_period(&self) -> Option<&BarTimeSemantics> { self.period.as_ref() }
}
impl ForecastCalendarRead for CompletedMarketSessionRead {
    fn reference(&self) -> &CompletedMarketSessionReference { self.reference() }
    fn native_session_replay(&self) -> &market_squawk_adapter_alpaca::AlpacaRetainedCalendarSessions { self.native_session_replay() }
    fn date_session_on(&self, date: CalendarDate, cutoff: Timestamp, evaluated: Timestamp) -> Option<ForecastCalendarDay> {
        let day = self.date_session_on(date, cutoff, evaluated)?;
        Some(ForecastCalendarDay { date: day.date(), opens: day.opens_at(), closes: day.closes_at_exclusive(), digest: day.evidence_digest(), period: day.provider_period().cloned() })
    }
}
impl ForecastCalendarRead for super::RetainedMarketSessionRead {
    fn reference(&self) -> &CompletedMarketSessionReference { self.reference() }
    fn native_session_replay(&self) -> &market_squawk_adapter_alpaca::AlpacaRetainedCalendarSessions { self.native_session_replay() }
    fn date_session_on(&self, date: CalendarDate, cutoff: Timestamp, evaluated: Timestamp) -> Option<ForecastCalendarDay> {
        let day = self.date_session_on(date, cutoff, evaluated)?;
        Some(ForecastCalendarDay { date: day.date(), opens: day.opens_at(), closes: day.closes_at_exclusive(), digest: day.evidence_digest(), period: day.provider_period().cloned() })
    }
}
fn latest_forecast_session_cohort(
    reader: &impl ForecastCalendarRead, knowledge_cutoff: Timestamp, horizon_nanos: i64,
    evaluated_at: Timestamp, deadline: Instant, cancellation: &CancellationToken,
) -> Result<Option<ForecastSessionCohort>, Error> {
        check(deadline, cancellation)?;
        if horizon_nanos <= 0 || evaluated_at < knowledge_cutoff {
            return Err(Error::InvalidRequest);
        }
        for native in reader.native_session_replay().sessions().iter().rev() {
            check(deadline, cancellation)?;
            if native.closes_at_exclusive() > knowledge_cutoff {
                continue;
            }
            let Some(session) = reader.date_session_on(native.date(), knowledge_cutoff, evaluated_at)
            else {
                continue;
            };
            let Some(period) = session.provider_period() else {
                continue;
            };
            let Some(end) = period.period_end_exclusive() else {
                return Err(Error::InvalidEvidence);
            };
            if end > knowledge_cutoff {
                continue;
            }
            let start = period.period_start().ok_or(Error::InvalidEvidence)?;
            let provider_timestamp = period.provider_timestamp().ok_or(Error::InvalidEvidence)?;
            let semantics: [u8; 32] =
                Sha256::digest(serde_json::to_vec(period).map_err(|_| Error::InvalidEvidence)?)
                    .into();
            let reference = ForecastSessionCohortReference {
                calendar: reader.reference().clone(),
                session_date: session.date(),
                regular_opens_at_unix_nanos: session.opens_at().unix_nanos().to_string(),
                regular_closes_at_unix_nanos: session
                    .closes_at_exclusive()
                    .unix_nanos()
                    .to_string(),
                aggregation_starts_at_unix_nanos: start.unix_nanos().to_string(),
                aggregation_ends_at_unix_nanos: end.unix_nanos().to_string(),
                aggregation_provider_timestamp_unix_nanos: provider_timestamp
                    .unix_nanos()
                    .to_string(),
                aggregation_semantics_sha256: semantics,
                calendar_session_evidence_sha256: session.evidence_digest().bytes(),
                knowledge_cutoff_unix_nanos: knowledge_cutoff.unix_nanos().to_string(),
                horizon_nanos: horizon_nanos.to_string(),
            };
            reference.validate()?;
            return Ok(Some(ForecastSessionCohort {
                reference,
                provider_period: period.clone(),
                opens_at: session.opens_at(),
                closes_at: session.closes_at_exclusive(),
                aggregation_end: end,
            }));
        }
        Ok(None)

}

impl ForecastSessionCohort {
    pub(crate) fn reference(&self) -> &ForecastSessionCohortReference {
        &self.reference
    }
    pub(crate) fn matches_origin(&self, origin: &ForecastSessionOrigin) -> bool {
        origin.cohort == self.reference
            && match origin.basis {
                FixedHorizonOriginBasis::CompletedBarClose => {
                    origin.observed_through == self.aggregation_end
                }
                FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar => {
                    origin.observed_through == self.closes_at
                }
                FixedHorizonOriginBasis::ExactEffectiveTimestamp => false,
            }
    }

    /// Actual complete timestamp history is the authority; equality of a scalar alone is not.
    pub(crate) fn bind_timestamp_history(
        &self,
        history: &CompleteMarketBarHistoryOutput,
    ) -> Result<Option<ForecastSessionOrigin>, Error> {
        let cutoff = self.reference.knowledge_cutoff()?;
        let Some(bar) = history.bars().last() else {
            return Ok(None);
        };
        if history.selection().receipt().date_windows().is_some()
            || !history.selection().receipt().current_research_eligible()
            || history.selection().receipt().published_at() > cutoff
            || history.selection().receipt().capture_recorded_at() > cutoff
            || {
                let (available, received, ingested) =
                    history.selection().receipt().knowledge_clocks();
                available > cutoff || received > cutoff || ingested > cutoff
            }
            || bar
                .context()
                .provenance()
                .availability()
                .conservative_available_at()
                .is_none_or(|available| available > cutoff)
            || !self.matches_timestamp_bar(bar)
        {
            return Ok(None);
        }
        self.origin(
            bar,
            history.selection().pinned().manifest(),
            history.read_receipt().result_digest(),
            history.read_receipt().result_digest(),
            FixedHorizonOriginBasis::CompletedBarClose,
            self.aggregation_end,
        )
        .map(Some)
    }

    /// Actual nominal history already binds the raw native date and original independently
    /// replayed calendar. Its calendar can be a different capture of the same source session.
    pub(crate) fn bind_nominal_source(
        &self,
        source: &NominalDailyCurrentSource,
    ) -> Result<Option<ForecastSessionOrigin>, Error> {
        let native = source.named_session_origin();
        if source.source_cutoff() > self.reference.knowledge_cutoff()?
            || native.native_date() != self.reference.session_date
            || native.opens_at() != self.opens_at
            || native.closes_at_exclusive() != self.closes_at
            || !native.matches_origin_bar(
                source.current_bar(),
                source.manifest(),
                source.current_close(),
                source.source_cutoff(),
            )
        {
            return Ok(None);
        }
        self.origin(
            source.current_bar(),
            source.manifest(),
            native.history_read_digest(),
            native.evidence_digest(),
            FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar,
            source.current_close(),
        )
        .map(Some)
    }

    /// A published native epoch can also project its own original input; deserialized epoch
    /// coordinates are not accepted in place of the source-authenticated data-reader capability.
    pub(crate) fn bind_input_epoch(
        &self,
        coordinate: FeatureDatasetInputCoordinate<'_>,
    ) -> Result<Option<ForecastSessionOrigin>, Error> {
        let epoch = coordinate.epoch();
        if epoch.source_selection_as_of() > self.reference.knowledge_cutoff()? {
            return Ok(None);
        }
        let Some(bar) = epoch.market_bar() else {
            return Ok(None);
        };
        let Some(basis) = epoch.fixed_horizon_origin_basis() else {
            return Ok(None);
        };
        let expected = match basis {
            FixedHorizonOriginBasis::CompletedBarClose if self.matches_timestamp_bar(bar) => {
                self.aggregation_end
            }
            FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar => {
                let Some(native) = epoch.named_session_origin() else {
                    return Ok(None);
                };
                if native.native_date() != self.reference.session_date
                    || native.opens_at() != self.opens_at
                    || native.closes_at_exclusive() != self.closes_at
                    || !native.matches_origin_bar(
                        bar,
                        epoch.source_manifest(),
                        self.closes_at,
                        epoch.source_selection_as_of(),
                    )
                {
                    return Ok(None);
                }
                self.closes_at
            }
            _ => return Ok(None),
        };
        if epoch.target_origin() != Some(expected) {
            return Ok(None);
        }
        self.origin(
            bar,
            epoch.source_manifest(),
            epoch.source_evidence_digest(),
            epoch
                .named_session_origin()
                .map_or(epoch.source_evidence_digest(), |origin| {
                    origin.evidence_digest()
                }),
            basis,
            expected,
        )
        .map(Some)
    }

    fn matches_timestamp_bar(&self, bar: &MarketBarObservation) -> bool {
        bar.time_semantics() == &self.provider_period
            && bar.completed_at() == Some(self.aggregation_end)
    }
    fn origin(
        &self,
        bar: &MarketBarObservation,
        manifest: &DatasetManifestRef,
        source_read_digest: Sha256Digest,
        source_origin_evidence_digest: Sha256Digest,
        basis: FixedHorizonOriginBasis,
        observed_through: Timestamp,
    ) -> Result<ForecastSessionOrigin, Error> {
        if source_read_digest.bytes() == [0; 32] {
            return Err(Error::InvalidEvidence);
        }
        Ok(ForecastSessionOrigin {
            cohort: self.reference.clone(),
            instrument_id: bar
                .context()
                .provenance()
                .instrument_id()
                .ok_or(Error::InvalidEvidence)?,
            basis,
            observed_through,
            source_manifest: manifest.clone(),
            source_read_digest,
            source_origin_evidence_digest,
        })
    }
}

fn integer(value: &str) -> Result<i64, Error> {
    let parsed = value.parse::<i64>().map_err(|_| Error::InvalidEvidence)?;
    if parsed.to_string() != value {
        return Err(Error::InvalidEvidence);
    }
    Ok(parsed)
}
fn check(deadline: Instant, cancellation: &CancellationToken) -> Result<(), Error> {
    if cancellation.is_cancelled() {
        Err(Error::Cancelled)
    } else if Instant::now() >= deadline {
        Err(Error::DeadlineExceeded)
    } else {
        Ok(())
    }
}
