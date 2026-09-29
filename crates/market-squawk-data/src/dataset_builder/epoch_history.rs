//! Causal RAW history replay in one authenticated forecast epoch's original share units.

use super::{EpochSource, FeatureDatasetInputEpoch};
use crate::{
    CompleteMarketBarHistoryCursor, ComponentAdjustmentEvidence, CorporateActionPlan,
    DatasetBuildError, DatasetBuildPurpose, Sha256Digest,
};
use market_squawk_domain::{
    CalendarDate, DataQuality, HistoricalStudyBasis, MarketBarAdjustment, MarketBarObservation,
    Money, Timestamp,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{io, time::Instant};
use tokio_util::sync::CancellationToken;

#[path = "epoch_history/rows.rs"]
mod rows;
use rows::HistoryRows;

/// Sealed producer result. Serialized bytes are retained evidence, not a reread capability.
/// There is deliberately no Deserialize or public constructor.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ForecastBasisHistory {
    instrument_id: market_squawk_domain::InstrumentId,
    #[serde(serialize_with = "serialize_manifest")]
    selected_manifest: crate::DatasetManifestRef,
    #[serde(serialize_with = "serialize_manifest")]
    origin_manifest: crate::DatasetManifestRef,
    #[serde(serialize_with = "serialize_manifests")]
    parent_manifests: Vec<crate::DatasetManifestRef>,
    basis_identity: [u8; 32],
    history_identity: [u8; 32],
    input_epoch_identity: [u8; 32],
    source_history_identity: [u8; 32],
    source_read_identity: [u8; 32],
    action_coverage_identity: [u8; 32],
    calendar_identity: [u8; 32],
    source_cutoff: Timestamp,
    origin_at: Timestamp,
    origin_price: Money,
    first_session_ordinal: usize,
    rows: HistoryRows,
}
impl ForecastBasisHistory {
    pub const fn instrument_id(&self) -> market_squawk_domain::InstrumentId {
        self.instrument_id
    }
    pub const fn selected_manifest(&self) -> &crate::DatasetManifestRef {
        &self.selected_manifest
    }
    pub const fn origin_manifest(&self) -> &crate::DatasetManifestRef {
        &self.origin_manifest
    }
    pub fn parent_manifests(&self) -> &[crate::DatasetManifestRef] {
        &self.parent_manifests
    }
    pub fn source_read_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.source_read_identity)
    }
    pub fn calendar_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.calendar_identity)
    }
    pub fn basis_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.basis_identity)
    }
    pub fn history_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.history_identity)
    }
    pub fn rows(
        &self,
    ) -> impl Iterator<Item = Result<ForecastBasisHistoryRow, DatasetBuildError>> + '_ {
        self.rows.iter()
    }
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
    pub fn last_row(&self) -> Option<&ForecastBasisHistoryRow> {
        self.rows.last()
    }
    pub const fn origin_at(&self) -> Timestamp {
        self.origin_at
    }
    pub const fn origin_price(&self) -> Money {
        self.origin_price
    }
    pub const fn source_cutoff(&self) -> Timestamp {
        self.source_cutoff
    }
}

/// Native session coordinates remain distinct from provider observation/completion timestamps.
/// An absent original bar stays a real gap; consumers must not join across it for detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForecastBasisHistoryRow {
    pub native_date: CalendarDate,
    pub session_open: Timestamp,
    pub session_close: Timestamp,
    pub nominal_date: Option<CalendarDate>,
    pub provider_timestamp: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub observed_at: Timestamp,
    /// Original untransformed source availability; adjusted series knowledge is separate.
    pub raw_available_at: Option<Timestamp>,
    pub available_at: Option<Timestamp>,
    pub quality: Option<DataQuality>,
    pub original_bar_identity: Option<[u8; 32]>,
    pub prices: Option<ForecastBasisOhlc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForecastBasisOhlc {
    pub open: Money,
    pub high: Money,
    pub low: Money,
    pub close: Money,
}

impl FeatureDatasetInputEpoch {
    /// Replays only the original generation and source-admitted split plan at the saved cutoff.
    /// Callers must freshly authorize all original history/action/calendar parents before use.
    /// Retains every authenticated session through the genuine forecast origin, including gaps.
    /// Display range and resolution are applied only after immutable projection publication.
    pub fn replay_price_history(
        &self,
        history: &CompleteMarketBarHistoryCursor,
        plan: &CorporateActionPlan,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ForecastBasisHistory, DatasetBuildError> {
        check(deadline, cancellation)?;
        let epoch = match &self.source {
            EpochSource::CompletedBarClose(value)
            | EpochSource::NamedSessionCloseForNominalDailyBar(value) => value,
            EpochSource::FinancialPeriod(_) => return Err(invalid()),
        };
        let publication = history.selection().receipt();
        let native = history.native_sessions().ok_or_else(invalid)?;
        let coverage = plan.source_split_admission().ok_or_else(invalid)?;
        let ComponentAdjustmentEvidence::Applied {
            policy,
            plan_content,
            plan_audit,
            implementation_evidence,
        } = epoch.adjustment()
        else {
            return Err(invalid());
        };
        if self.basis() != HistoricalStudyBasis::HistoricalAsKnown
            || self.purpose() != DatasetBuildPurpose::StudyInputs
            || history.selection().pinned().manifest() != self.source_manifest()
            || publication.instrument_id() != self.instrument_id()
            || publication.currency() != epoch.market_bar().currency()
            || publication.adjustment() != MarketBarAdjustment::Raw
            || !publication.current_research_eligible()
            || history.read_receipt().knowledge_cutoff() != self.source_selection_as_of()
            || publication.published_at() > self.source_selection_as_of()
            || native.published_at() > self.source_selection_as_of()
            || native.received_at() > native.published_at()
            || plan.steps().len() > 1_024
            || plan.policy() != *policy
            || plan.content_hash() != *plan_content
            || plan.audit_hash() != *plan_audit
            || plan.knowledge_cutoff() != self.source_selection_as_of()
            || plan.valuation_cutoff() != epoch.target_origin()
            || !coverage.instruments().contains(&self.instrument_id())
            || !coverage
                .history_input_manifests()
                .contains(self.source_manifest())
        {
            return Err(invalid());
        }
        if let Some(saved) = self.named_session_origin() {
            if saved.history_content_digest() != history.read_receipt().history_content_digest()
                || saved.history_read_digest() != history.read_receipt().result_digest()
                || saved.mapping_digest() != native.mapping_digest()
                || saved.source_replay_digest() != native.source_replay_digest()
            {
                return Err(invalid());
            }
        } else {
            let actual = crate::CompletedOrdinaryHistoryEvidence::try_from_cursor(history)
                .map_err(|_| invalid())?;
            if !coverage.admits_completed_history(&actual) {
                return Err(invalid());
            }
        }
        if coverage.source_manifests().len() > 126 {
            return Err(DatasetBuildError::LimitExceeded);
        }
        let mut parents = Vec::new();
        parents
            .try_reserve_exact(coverage.source_manifests().len() + 2)
            .map_err(|_| DatasetBuildError::LimitExceeded)?;
        for parent in coverage.source_manifests().iter().chain([
            history.selection().pinned().manifest(),
            history.read_receipt().origin_manifest(),
        ]) {
            check(deadline, cancellation)?;
            if let Some(existing) = parents
                .iter()
                .find(|existing: &&crate::DatasetManifestRef| {
                    existing.dataset_id() == parent.dataset_id()
                        && existing.manifest_version() == parent.manifest_version()
                })
            {
                if existing != parent {
                    return Err(invalid());
                }
            } else {
                parents.push(parent.clone());
            }
        }
        let origin = epoch.target_origin();
        let mut eligible = 0_usize;
        let mut previous = None;
        let mut first = None;
        let mut last = None;
        for session in native.sessions().iter() {
            let session = session.map_err(|_| invalid())?;
            check(deadline, cancellation)?;
            let observation = session
                .provider_period()
                .map_or(session.closes_at_exclusive(), |(_, end)| end);
            if session.opens_at() >= session.closes_at_exclusive()
                || previous.is_some_and(|prior| prior >= observation)
            {
                return Err(invalid());
            }
            previous = Some(observation);
            if observation <= origin {
                eligible += 1;
                if first.is_none() {
                    first = Some(session.clone());
                }
                last = Some(session);
            }
        }
        let start = 0;
        let first = first.ok_or_else(invalid)?;
        let last = last.ok_or_else(invalid)?;
        if first.native_date() < coverage.interval().0
            || last.native_date() > coverage.interval().1
            || coverage
                .application_starts_at(self.instrument_id())
                .is_none_or(|at| at > first.opens_at())
        {
            return Err(invalid());
        }
        let mut rows =
            HistoryRows::new(history.operation_scratch(), deadline, cancellation.clone())?;
        let mut bars = history.bars();
        let mut next_bar = bars.next().transpose().map_err(|_| invalid())?;
        let mut origin_matched = false;
        for session in native.sessions().iter().take(eligible) {
            let session = session.map_err(|_| invalid())?;
            check(deadline, cancellation)?;
            // Canonical originals and native sessions are ordered by their actual source key.
            // Consume at most one original at a time; no source history binary-search vector.
            let key = session
                .provider_timestamp()
                .map(|at| at.unix_nanos())
                .unwrap_or_else(|| native_date_key(session.native_date()));
            while next_bar.as_ref().is_some_and(|bar| original_key(bar) < key) {
                next_bar = bars.next().transpose().map_err(|_| invalid())?;
            }
            let bar = next_bar.as_ref().filter(|bar| original_key(bar) == key);
            if session.bar_present() != bar.is_some() {
                return Err(invalid());
            }
            let observed_at = session
                .provider_period()
                .map_or(session.closes_at_exclusive(), |(_, end)| end);
            let mut row = ForecastBasisHistoryRow {
                native_date: session.native_date(),
                session_open: session.opens_at(),
                session_close: session.closes_at_exclusive(),
                nominal_date: None,
                provider_timestamp: session.provider_timestamp(),
                completed_at: None,
                observed_at,
                raw_available_at: None,
                available_at: None,
                quality: None,
                original_bar_identity: None,
                prices: None,
            };
            if let Some(bar) = bar {
                let provenance = bar.context().provenance();
                let (prices, adjustment_available_at) =
                    transform(bar, observed_at, self.instrument_id(), plan)?;
                let available = provenance
                    .availability()
                    .conservative_available_at()
                    .ok_or_else(invalid)?
                    .max(provenance.received_at())
                    .max(provenance.ingested_at())
                    .max(publication.published_at())
                    .max(native.published_at())
                    .max(adjustment_available_at.unwrap_or(observed_at))
                    // Complete split/non-event coverage is admitted only as of this saved
                    // cutoff. Do not backdate the reconstructed common basis to raw knowledge.
                    .max(plan.knowledge_cutoff());
                if bar.adjustment() != MarketBarAdjustment::Raw
                    || provenance.instrument_id() != Some(self.instrument_id())
                    || bar.currency() != publication.currency()
                    || available > self.source_selection_as_of()
                    || observed_at > available
                    || bar.completed_at() != session.provider_period().map(|(_, end)| end)
                {
                    return Err(invalid());
                }
                if observed_at == origin {
                    if bar != epoch.market_bar() || prices.close != epoch.current_unit_price()? {
                        return Err(invalid());
                    }
                    origin_matched = true;
                }
                row.nominal_date = bar
                    .time_semantics()
                    .nominal_daily_date()
                    .map(|date| date.date());
                row.completed_at = bar.completed_at();
                row.raw_available_at = provenance.availability().conservative_available_at();
                row.available_at = Some(available);
                row.quality = Some(provenance.quality());
                row.original_bar_identity = Some(hash(b"original-raw-history-bar/v1", bar)?);
                row.prices = Some(prices);
            }
            rows.push(row)?;
        }
        if !origin_matched {
            return Err(invalid());
        }
        rows.finish()?;
        let epoch_identity = hash(b"forecast-history-input-epoch/v1", &self.canonical_bytes()?)?;
        // Whole original epoch binds currency, raw source origin, both clocks, original action
        // content/audit/implementation and current-unit price. This is an equality authority,
        // not a claim that any other same-currency Split series has these share units.
        let basis_identity = hash(
            b"forecast-original-split-share-units/v1",
            &(
                epoch_identity,
                plan_content.bytes(),
                plan_audit.bytes(),
                implementation_evidence,
                epoch.current_unit_price()?,
            ),
        )?;
        let history_identity = hash(
            b"forecast-basis-history/v1",
            &(
                basis_identity,
                history.read_receipt().history_content_digest().bytes(),
                history.read_receipt().result_digest().bytes(),
                coverage.evidence_digest(),
                native.mapping_digest(),
                parents
                    .iter()
                    .map(super::EpochManifest::from_manifest)
                    .collect::<Vec<_>>(),
                start,
                &rows,
            ),
        )?;
        check(deadline, cancellation)?;
        Ok(ForecastBasisHistory {
            instrument_id: self.instrument_id(),
            selected_manifest: history.selection().pinned().manifest().clone(),
            origin_manifest: history.read_receipt().origin_manifest().clone(),
            parent_manifests: parents,
            basis_identity,
            history_identity,
            input_epoch_identity: epoch_identity,
            source_history_identity: history.read_receipt().history_content_digest().bytes(),
            source_read_identity: history.read_receipt().result_digest().bytes(),
            action_coverage_identity: coverage.evidence_digest().bytes(),
            calendar_identity: native.mapping_digest().bytes(),
            source_cutoff: self.source_selection_as_of(),
            origin_at: origin,
            origin_price: epoch.current_unit_price()?,
            first_session_ordinal: start,
            rows,
        })
    }
}

fn native_date_key(date: CalendarDate) -> i64 {
    i64::from(date.year()) * 10_000 + i64::from(date.month()) * 100 + i64::from(date.day())
}
fn original_key(bar: &MarketBarObservation) -> i64 {
    bar.time_semantics()
        .provider_timestamp()
        .map(|at| at.unix_nanos())
        .or_else(|| {
            bar.time_semantics()
                .nominal_daily_date()
                .map(|date| native_date_key(date.date()))
        })
        .unwrap_or(i64::MIN)
}

fn transform(
    bar: &MarketBarObservation,
    at: Timestamp,
    instrument: market_squawk_domain::InstrumentId,
    plan: &CorporateActionPlan,
) -> Result<(ForecastBasisOhlc, Option<Timestamp>), DatasetBuildError> {
    let (prices, adjustment_available_at) = adjust_prices(
        [bar.open(), bar.high(), bar.low(), bar.close()].map(|price| price.amount()),
        at,
        instrument,
        plan,
    )?;
    let [open, high, low, close] =
        prices.map(|value| Money::new(value.normalize(), bar.currency()));
    Ok((
        ForecastBasisOhlc {
            open,
            high,
            low,
            close,
        },
        adjustment_available_at,
    ))
}
fn adjust_prices(
    mut prices: [Decimal; 4],
    at: Timestamp,
    instrument: market_squawk_domain::InstrumentId,
    plan: &CorporateActionPlan,
) -> Result<([Decimal; 4], Option<Timestamp>), DatasetBuildError> {
    let mut adjustment_available_at: Option<Timestamp> = None;
    for step in plan.steps() {
        let crate::AdjustmentStep::Split {
            admitted_index,
            price_factor,
            ..
        } = step
        else {
            return Err(invalid());
        };
        let action = plan.admitted().get(*admitted_index).ok_or_else(invalid)?;
        if action.observation().context().provenance().instrument_id() != Some(instrument) {
            continue;
        }
        let effective = action.application_at().ok_or_else(invalid)?;
        if at <= effective {
            if effective > plan.valuation_cutoff() {
                return Err(invalid());
            }
            // Split-basis geometry cannot predate the action observation or the independent
            // calendar/application evidence that makes its effective boundary knowable.
            let provenance = action.observation().context().provenance();
            let known = provenance
                .availability()
                .conservative_available_at()
                .ok_or_else(invalid)?
                .max(provenance.received_at())
                .max(provenance.ingested_at());
            let known = action
                .application()
                .map_or(known, |application| known.max(application.available_at()));
            if known > plan.knowledge_cutoff() {
                return Err(invalid());
            }
            adjustment_available_at =
                Some(adjustment_available_at.map_or(known, |prior| prior.max(known)));
            for price in &mut prices {
                *price = price
                    .checked_mul(Decimal::from(price_factor.numerator().get()))
                    .and_then(|value| {
                        value.checked_div(Decimal::from(price_factor.denominator().get()))
                    })
                    .ok_or_else(invalid)?;
            }
        }
    }
    if prices.iter().any(|price| *price <= Decimal::ZERO)
        || prices[2] > prices[0]
        || prices[2] > prices[3]
        || prices[1] < prices[0]
        || prices[1] < prices[3]
    {
        return Err(invalid());
    }
    Ok((prices, adjustment_available_at))
}

fn invalid() -> DatasetBuildError {
    DatasetBuildError::ComponentEvidenceMismatch
}
fn check(deadline: Instant, cancellation: &CancellationToken) -> Result<(), DatasetBuildError> {
    if cancellation.is_cancelled() {
        Err(DatasetBuildError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(DatasetBuildError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn hash(domain: &[u8], value: &impl Serialize) -> Result<[u8; 32], DatasetBuildError> {
    struct Writer {
        hash: Sha256,
        bytes: usize,
    }
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("forecast history evidence size overflow"))?;
            self.hash.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        hash: Sha256::new(),
        bytes: 0,
    };
    writer.hash.update(b"market-squawk/forecast-history/");
    writer.hash.update(domain);
    writer.hash.update([0]);
    serde_json::to_writer(&mut writer, value).map_err(|_| invalid())?;
    Ok(writer.hash.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CorporateActionAdjustment, CorporateActionLimits, CorporateActionPolicy,
        CorporateActionRecord, DatasetId, DatasetManifestRef, DatasetSchemaRegistry,
    };
    use market_squawk_domain::{
        AvailabilityEvidence, CorporateActionKind, CorporateActionObservation, Currency,
        DigestAlgorithm, EvidenceDigest, InstrumentId, PayloadReference, ResearchContext,
        ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber, SourceId,
        SourceIdentifier, VenueId,
    };
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        str::FromStr,
    };

    #[test]
    fn exact_share_conversion_rounds_once_and_preserves_outward_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        use ShareConversionRounding::{Central, Lower, Upper};
        assert_eq!(
            exact_scaled_ratio(Decimal::from(120), 1, 2, 2, Central)?,
            Decimal::from(60)
        );
        assert_eq!(
            exact_scaled_ratio(Decimal::from(120), 1, 1, 2, Central)?,
            Decimal::from(120)
        );
        assert_eq!(
            exact_scaled_ratio(Decimal::ONE, 1, 3, 2, Lower)?,
            Decimal::new(33, 2)
        );
        assert_eq!(
            exact_scaled_ratio(Decimal::ONE, 1, 3, 2, Upper)?,
            Decimal::new(34, 2)
        );
        assert_eq!(
            exact_scaled_ratio(
                Decimal::from_str("0.0050000000000000000000000001")?,
                1,
                1,
                2,
                Central
            )?,
            Decimal::new(1, 2)
        );
        assert_eq!(
            exact_scaled_ratio(Decimal::new(5, 3), 1, 1, 2, Central)?,
            Decimal::ZERO
        );
        assert!(exact_scaled_ratio(Decimal::MAX, 2, 1, 2, Central).is_err());
        Ok(())
    }

    #[test]
    fn history_split_replay_preserves_boundary_and_excludes_cash_and_future_actions()
    -> Result<(), Box<dyn std::error::Error>> {
        let instrument = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1")?;
        let currency = Currency::try_from("USD")?;
        let mut records = Vec::new();
        for (effective, action) in [
            (
                80,
                CorporateActionKind::Split {
                    numerator: NonZeroU32::new(2).ok_or("ratio")?,
                    denominator: NonZeroU32::MIN,
                },
            ),
            (
                100,
                CorporateActionKind::Split {
                    numerator: NonZeroU32::new(3).ok_or("ratio")?,
                    denominator: NonZeroU32::MIN,
                },
            ),
            (
                120,
                CorporateActionKind::Split {
                    numerator: NonZeroU32::new(5).ok_or("ratio")?,
                    denominator: NonZeroU32::MIN,
                },
            ),
            (
                70,
                CorporateActionKind::CashDividend {
                    amount: Money::new(Decimal::from(10), currency),
                },
            ),
        ] {
            let identifier = SourceIdentifier::try_from(format!("history-action-{effective}"))?;
            let context = ResearchContext::new(
                ResearchProvenance::try_new(ResearchProvenanceInput {
                    source_id: SourceId::try_from("official-corporate-actions")?,
                    instrument_id: Some(instrument),
                    venue_id: Some(VenueId::try_from("XNYS")?),
                    source_identifier: identifier.clone(),
                    source_timestamp: Some(Timestamp::from_unix_nanos(effective)),
                    received_at: Timestamp::from_unix_nanos(effective + 45),
                    ingested_at: Timestamp::from_unix_nanos(effective + 50),
                    quality: DataQuality::OfficialDelayed,
                    payload_reference: PayloadReference::SourceReference(identifier.clone()),
                    availability: AvailabilityEvidence::evidenced(
                        Timestamp::from_unix_nanos(effective + 40),
                        identifier,
                    ),
                })?,
                ResearchTime::new(
                    Timestamp::from_unix_nanos(effective),
                    None,
                    RevisionNumber::new(1)?,
                    None,
                )?,
            )?;
            let manifest = DatasetManifestRef::try_new_with_schema(
                DatasetId::try_from("history-actions")?,
                effective as u64,
                DatasetSchemaRegistry::local().canonical_research_observations()?,
                Sha256Digest::new([effective as u8; 32]),
            )?;
            records.push(CorporateActionRecord::new(
                CorporateActionObservation::new(context, action)?,
                manifest,
                EvidenceDigest::new(DigestAlgorithm::Sha256, [effective as u8; 32]),
            ));
        }
        let plan = CorporateActionPlan::try_build(
            CorporateActionPolicy::new(CorporateActionAdjustment::SplitAdjusted, NonZeroU32::MIN),
            Timestamp::from_unix_nanos(150),
            Timestamp::from_unix_nanos(100),
            records,
            CorporateActionLimits::try_new(
                NonZeroUsize::new(4).ok_or("count")?,
                NonZeroUsize::new(1024 * 1024).ok_or("bytes")?,
            )?,
        )?;
        let prices = [120, 180, 60, 150].map(Decimal::from);
        assert_eq!(
            adjust_prices(prices, Timestamp::from_unix_nanos(80), instrument, &plan)?,
            (
                [20, 30, 10, 25].map(Decimal::from),
                Some(Timestamp::from_unix_nanos(150))
            )
        );
        assert_eq!(
            adjust_prices(prices, Timestamp::from_unix_nanos(100), instrument, &plan)?,
            (
                [40, 60, 20, 50].map(Decimal::from),
                Some(Timestamp::from_unix_nanos(150))
            )
        );
        assert_eq!(
            adjust_prices(prices, Timestamp::from_unix_nanos(101), instrument, &plan)?,
            (prices, None)
        );
        let earlier = CorporateActionPlan::try_build(
            plan.policy(),
            Timestamp::from_unix_nanos(145),
            plan.valuation_cutoff(),
            plan.admitted().to_vec(),
            CorporateActionLimits::try_new(
                NonZeroUsize::new(4).ok_or("count")?,
                NonZeroUsize::new(1024 * 1024).ok_or("bytes")?,
            )?,
        )?;
        assert!(
            adjust_prices(
                prices,
                Timestamp::from_unix_nanos(100),
                instrument,
                &earlier
            )
            .is_err()
        );
        // Decoding the same financial recipe cannot recreate live source coverage for chart use.
        let bytes = plan.encode_recovery_material()?;
        let decoded = CorporateActionPlan::decode_recovery_material(
            &bytes,
            CorporateActionLimits::try_new(
                NonZeroUsize::new(4).ok_or("count")?,
                NonZeroUsize::new(1024 * 1024).ok_or("bytes")?,
            )?,
        )?;
        assert!(decoded.source_split_admission().is_none());
        Ok(())
    }
}

fn serialize_manifest<S: serde::Serializer>(
    value: &crate::DatasetManifestRef,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    super::EpochManifest::from_manifest(value).serialize(serializer)
}
fn serialize_manifests<S: serde::Serializer>(
    values: &[crate::DatasetManifestRef],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq as _;
    let mut sequence = serializer.serialize_seq(Some(values.len()))?;
    for value in values {
        sequence.serialize_element(&super::EpochManifest::from_manifest(value))?;
    }
    sequence.end()
}

/// Direction is part of the conversion arithmetic contract. Bounds round outwards.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareConversionRounding {
    Central,
    Lower,
    Upper,
}

/// Original-forecast to exact-current-observation share units. Only source-backed history,
/// plans and market selection can mint this receipt. Its serialized record is inert on replay.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ForecastCurrentShareConversion {
    instrument_id: market_squawk_domain::InstrumentId,
    currency: market_squawk_domain::Currency,
    original_basis_identity: [u8; 32],
    input_epoch_identity: [u8; 32],
    original_origin_identity: [u8; 32],
    original_origin_price: Money,
    original_plan_content: [u8; 32],
    original_plan_audit: [u8; 32],
    original_coverage: [u8; 32],
    current_plan_content: [u8; 32],
    current_plan_audit: [u8; 32],
    current_coverage: [u8; 32],
    current_selection_identity: [u8; 32],
    current_observation_identity: [u8; 32],
    current_coordinate_identity: [u8; 32],
    current_definition_identity: [u8; 32],
    original_cutoff: Timestamp,
    original_frame_at: Timestamp,
    quote_at: Timestamp,
    market_cutoff: Timestamp,
    knowledge_cutoff: Timestamp,
    /// Ordered source-applied price ratios and original source record identities.
    splits: Vec<(Timestamp, u32, u32, [u8; 32])>,
    numerator: u128,
    denominator: u128,
    output_scale: u32,
    identity: [u8; 32],
}
impl ForecastCurrentShareConversion {
    pub const fn instrument_id(&self) -> market_squawk_domain::InstrumentId {
        self.instrument_id
    }
    pub const fn currency(&self) -> market_squawk_domain::Currency {
        self.currency
    }
    pub fn identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.identity)
    }
    pub fn original_basis_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.original_basis_identity)
    }
    pub fn input_epoch_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.input_epoch_identity)
    }
    pub fn current_selection_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.current_selection_identity)
    }
    pub fn current_observation_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.current_observation_identity)
    }
    pub const fn quote_at(&self) -> Timestamp {
        self.quote_at
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    pub const fn market_cutoff(&self) -> Timestamp {
        self.market_cutoff
    }
    pub const fn output_scale(&self) -> u32 {
        self.output_scale
    }
    pub const fn price_ratio(&self) -> (u128, u128) {
        (self.numerator, self.denominator)
    }
    pub fn project_money(
        &self,
        value: Money,
        rounding: ShareConversionRounding,
    ) -> Result<Money, DatasetBuildError> {
        self.convert_money(value, self.numerator, self.denominator, rounding)
    }
    /// Backend chart overlay conversion; never changes the original forecast/history artifact.
    pub fn inverse_money(
        &self,
        value: Money,
        rounding: ShareConversionRounding,
    ) -> Result<Money, DatasetBuildError> {
        self.convert_money(value, self.denominator, self.numerator, rounding)
    }
    fn convert_money(
        &self,
        value: Money,
        numerator: u128,
        denominator: u128,
        rounding: ShareConversionRounding,
    ) -> Result<Money, DatasetBuildError> {
        if value.currency() != self.currency || value.amount() <= Decimal::ZERO {
            return Err(invalid());
        }
        let value = exact_scaled_ratio(
            value.amount(),
            numerator,
            denominator,
            self.output_scale,
            rounding,
        )?;
        if value <= Decimal::ZERO {
            return Err(invalid());
        }
        Ok(Money::new(value, self.currency))
    }
}

impl ForecastBasisHistory {
    /// Both plans must be freshly source admitted. The later pool must reproduce the original
    /// share frame before any split after that frame can transform a forecast amount.
    #[allow(clippy::too_many_arguments)]
    pub fn convert_to_current_share_units(
        &self,
        epoch: &FeatureDatasetInputEpoch,
        original_plan: &CorporateActionPlan,
        current_plan: &CorporateActionPlan,
        market: &crate::ProviderMarketEventPointInTimeSelection,
        market_definitions: &crate::MarketDataInstrumentPopulationSelection,
        output_scale: u32,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ForecastCurrentShareConversion, DatasetBuildError> {
        check(deadline, cancellation)?;
        let [source] = market.sources() else {
            return Err(invalid());
        };
        let [candidate] = source.tied_candidates() else {
            return Err(invalid());
        };
        let [definition] = market_definitions.records() else {
            return Err(invalid());
        };
        let coordinate = candidate.coordinate();
        let quote_at = coordinate.source_timestamp().ok_or_else(invalid)?;
        let origin = epoch.market_bar().ok_or_else(invalid)?;
        let origin_at = epoch.target_origin().ok_or_else(invalid)?;
        let original = original_plan.source_split_admission().ok_or_else(invalid)?;
        let current = current_plan.source_split_admission().ok_or_else(invalid)?;
        let epoch_identity = hash(
            b"forecast-history-input-epoch/v1",
            &epoch.canonical_bytes()?,
        )?;
        let epoch_source = match &epoch.source {
            EpochSource::CompletedBarClose(value)
            | EpochSource::NamedSessionCloseForNominalDailyBar(value) => value,
            EpochSource::FinancialPeriod(_) => return Err(invalid()),
        };
        let ComponentAdjustmentEvidence::Applied {
            policy,
            plan_content,
            plan_audit,
            implementation_evidence,
        } = epoch_source.adjustment()
        else {
            return Err(invalid());
        };
        let expected_basis = hash(
            b"forecast-original-split-share-units/v1",
            &(
                epoch_identity,
                plan_content.bytes(),
                plan_audit.bytes(),
                implementation_evidence,
                epoch.current_unit_price()?,
            ),
        )?;
        if market_definitions.disposition()
            != crate::MarketDataInstrumentPopulationDisposition::Complete
            || !market_definitions.exclusions().is_empty()
            || market_definitions.query().instrument_ids() != [self.instrument_id]
            || market_definitions.query().knowledge_at() != market.request().knowledge_cutoff()
            || market_definitions.query().effective_at() != market.request().knowledge_cutoff()
            || definition.definition().instrument_id() != self.instrument_id
            || definition.definition().quote_currency() != self.origin_price.currency()
            || definition.published_at() > market.request().knowledge_cutoff()
            || output_scale > 28
            || epoch_identity != self.input_epoch_identity
            || expected_basis != self.basis_identity
            || self.instrument_id != epoch.instrument_id()
            || self.origin_at != origin_at
            || self.origin_price != epoch.current_unit_price()?
            || original_plan.policy() != *policy
            || original_plan.content_hash() != *plan_content
            || original_plan.audit_hash() != *plan_audit
            || original_plan.knowledge_cutoff() != self.source_cutoff
            || original_plan.valuation_cutoff() != origin_at
            || original.evidence_digest().bytes() != self.action_coverage_identity
            || !current.uses_us_equity_dates()
            || current
                .application_starts_at(self.instrument_id)
                .is_none_or(|start| start > origin_at)
            || !current.instruments().contains(&self.instrument_id)
            || current_plan.valuation_cutoff() != quote_at
            || current_plan.knowledge_cutoff() < self.source_cutoff
            || current_plan.knowledge_cutoff() < market.request().knowledge_cutoff()
            || quote_at < origin_at
            || quote_at > market.request().knowledge_cutoff()
            || market.request().instrument_id() != Some(self.instrument_id)
            || coordinate.instrument_id() != Some(self.instrument_id)
            || !matches!(
                candidate.event(),
                market_squawk_domain::MarketEvent::MarketDataQuote(_)
                    | market_squawk_domain::MarketEvent::MarketDataTrade(_)
                    | market_squawk_domain::MarketEvent::Quote(_)
                    | market_squawk_domain::MarketEvent::Trade(_)
            )
            || current_plan.steps().len() > 1_024
            || original_plan.steps().len() > 1_024
        {
            return Err(invalid());
        }
        let limits = current_plan
            .source_split_projection_limits(
                *policy,
                self.instrument_id,
                current_plan.knowledge_cutoff(),
                origin_at,
            )
            .map_err(|_| invalid())?;
        let reproduced = current_plan
            .try_project_source_split_plan(
                *policy,
                self.instrument_id,
                current_plan.knowledge_cutoff(),
                origin_at,
                limits,
            )
            .map_err(|_| invalid())?;
        let (original_prices, _) = transform(origin, origin_at, self.instrument_id, original_plan)?;
        let (reproduced_prices, _) = transform(origin, origin_at, self.instrument_id, &reproduced)?;
        if original_prices.close != self.origin_price
            || reproduced_prices.close != self.origin_price
        {
            return Err(invalid());
        }
        // Reconcile exact price ratios as well as rounded original amounts. An equal rounded
        // close must not hide a changed source ratio in the original share frame.
        if split_frame(original_plan, self.instrument_id, origin_at)?
            != split_frame(&reproduced, self.instrument_id, origin_at)?
        {
            return Err(invalid());
        }
        let mut splits = Vec::new();
        splits
            .try_reserve_exact(current_plan.steps().len())
            .map_err(|_| DatasetBuildError::LimitExceeded)?;
        let (mut numerator, mut denominator) = (1_u128, 1_u128);
        for step in current_plan.steps() {
            check(deadline, cancellation)?;
            let crate::AdjustmentStep::Split {
                admitted_index,
                price_factor,
                ..
            } = step
            else {
                return Err(invalid());
            };
            let action = current_plan
                .admitted()
                .get(*admitted_index)
                .ok_or_else(invalid)?;
            if action.observation().context().provenance().instrument_id()
                != Some(self.instrument_id)
            {
                continue;
            }
            let effective = action.application_at().ok_or_else(invalid)?;
            // Original close uses the existing exclusive-close convention. The destination is
            // an instantaneous quote: a split at exactly its timestamp is already effective.
            if effective > origin_at && effective <= quote_at {
                let n = price_factor.numerator().get();
                let d = price_factor.denominator().get();
                (numerator, denominator) =
                    multiply_ratio(numerator, denominator, u128::from(n), u128::from(d))?;
                splits.push((effective, n, d, action.evidence_digest().bytes()));
            }
        }
        let mut receipt = ForecastCurrentShareConversion {
            instrument_id: self.instrument_id,
            currency: self.origin_price.currency(),
            original_basis_identity: self.basis_identity,
            input_epoch_identity: epoch_identity,
            original_origin_identity: hash(b"original-raw-history-bar/v1", origin)?,
            original_origin_price: self.origin_price,
            original_plan_content: original_plan.content_hash().bytes(),
            original_plan_audit: original_plan.audit_hash().bytes(),
            original_coverage: original.evidence_digest().bytes(),
            current_plan_content: current_plan.content_hash().bytes(),
            current_plan_audit: current_plan.audit_hash().bytes(),
            current_coverage: current.evidence_digest().bytes(),
            current_selection_identity: market.selection_digest().bytes(),
            current_observation_identity: coordinate.canonical_event_digest().bytes(),
            current_coordinate_identity: coordinate.coordinate_digest().bytes(),
            current_definition_identity: market_definitions.receipt_digest().bytes(),
            original_cutoff: self.source_cutoff,
            original_frame_at: origin_at,
            quote_at,
            market_cutoff: market.request().knowledge_cutoff(),
            knowledge_cutoff: current_plan.knowledge_cutoff(),
            splits,
            numerator,
            denominator,
            output_scale,
            identity: [0; 32],
        };
        receipt.identity = hash(
            b"forecast-current-share-conversion/exact-ratio-nearest-even-outward/v1",
            &receipt,
        )?;
        check(deadline, cancellation)?;
        Ok(receipt)
    }
}

fn split_frame(
    plan: &CorporateActionPlan,
    instrument: market_squawk_domain::InstrumentId,
    origin: Timestamp,
) -> Result<Vec<(Timestamp, u32, u32)>, DatasetBuildError> {
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(plan.steps().len())
        .map_err(|_| DatasetBuildError::LimitExceeded)?;
    for step in plan.steps() {
        let crate::AdjustmentStep::Split {
            admitted_index,
            price_factor,
            ..
        } = step
        else {
            return Err(invalid());
        };
        let action = plan.admitted().get(*admitted_index).ok_or_else(invalid)?;
        if action.observation().context().provenance().instrument_id() == Some(instrument) {
            let at = action.application_at().ok_or_else(invalid)?;
            if origin <= at {
                frame.push((
                    at,
                    price_factor.numerator().get(),
                    price_factor.denominator().get(),
                ));
            }
        }
    }
    frame.sort_unstable();
    Ok(frame)
}
fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
fn multiply_ratio(
    mut n: u128,
    mut d: u128,
    mut other_n: u128,
    mut other_d: u128,
) -> Result<(u128, u128), DatasetBuildError> {
    let left = gcd(n, other_d);
    n /= left;
    other_d /= left;
    let right = gcd(other_n, d);
    other_n /= right;
    d /= right;
    Ok((
        n.checked_mul(other_n).ok_or_else(invalid)?,
        d.checked_mul(other_d).ok_or_else(invalid)?,
    ))
}
fn exact_scaled_ratio(
    value: Decimal,
    numerator: u128,
    denominator: u128,
    scale: u32,
    rounding: ShareConversionRounding,
) -> Result<Decimal, DatasetBuildError> {
    if value <= Decimal::ZERO || numerator == 0 || denominator == 0 || scale > 28 {
        return Err(invalid());
    }
    let (mut n, mut d) = multiply_ratio(
        u128::try_from(value.mantissa()).map_err(|_| invalid())?,
        10_u128.checked_pow(value.scale()).ok_or_else(invalid)?,
        numerator,
        denominator,
    )?;
    (n, d) = multiply_ratio(n, d, 10_u128.checked_pow(scale).ok_or_else(invalid)?, 1)?;
    let q = n / d;
    let rem = n % d;
    let increment = match rounding {
        ShareConversionRounding::Lower => false,
        ShareConversionRounding::Upper => rem != 0,
        ShareConversionRounding::Central => rem > d - rem || (rem == d - rem && q % 2 == 1),
    };
    let q = q.checked_add(u128::from(increment)).ok_or_else(invalid)?;
    Decimal::try_from_i128_with_scale(i128::try_from(q).map_err(|_| invalid())?, scale)
        .map_err(|_| invalid())
}
