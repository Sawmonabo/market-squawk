//! Original economic-date ordinary-action reads and their sole financial admission join.

pub(super) mod source;

use super::*;
use crate::{DatasetManifestRef, MarketDataInstrumentRecord};
use market_squawk_domain::{CalendarDate, EvidenceDigest, Timestamp};

/// Exact reviewed endpoint family. Neither family alone supplies ordinary coverage.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CurrentOrdinaryActionFamily {
    Distributions,
    Splits,
}

/// One original target-instrument row; all other batch rows stay in original source audit.
#[derive(Clone, Debug)]
pub struct CurrentOrdinaryActionRow {
    pub date: CalendarDate,
    pub disposition: CurrentOrdinaryActionDisposition,
}

#[derive(Clone, Debug)]
pub enum CurrentOrdinaryActionDisposition {
    Cancelled {
        evidence: EvidenceDigest,
    },
    Normalized {
        record: CorporateActionRecord,
        payable_date: Option<CalendarDate>,
    },
    MissingUnit {
        evidence: EvidenceDigest,
        /// Original event identity, never inferred from the requested symbol.
        instrument: Option<InstrumentId>,
        /// Native scalar only; this source still supplies no monetary unit.
        distribution: rust_decimal::Decimal,
        payable_date: Option<CalendarDate>,
    },
    Unsupported {
        evidence: EvidenceDigest,
    },
}

/// Issued only by the physical source/canonical/catalog rereader in the private child.
/// A terminal empty response retains its original query; callers cannot construct absence.
#[derive(Debug)]
pub struct CurrentOrdinaryActionSourceRead {
    family: CurrentOrdinaryActionFamily,
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    captured_at: Timestamp,
    knowledge_cutoff: Timestamp,
    instrument: MarketDataInstrumentRecord,
    query_identity: market_squawk_domain::CorporateActionQueryInstrumentIdentity,
    event_identities: Box<
        [(
            CalendarDate,
            market_squawk_domain::CorporateActionEventInstrumentIdentity,
            MarketDataInstrumentRecord,
        )],
    >,
    interval: (CalendarDate, CalendarDate),
    rows: Box<[CurrentOrdinaryActionRow]>,
    source_audit: Box<[u8]>,
}
impl CurrentOrdinaryActionSourceRead {
    pub const fn family(&self) -> CurrentOrdinaryActionFamily {
        self.family
    }
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    pub const fn captured_at(&self) -> Timestamp {
        self.captured_at
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    pub const fn instrument(&self) -> &MarketDataInstrumentRecord {
        &self.instrument
    }
    pub const fn query_identity(
        &self,
    ) -> &market_squawk_domain::CorporateActionQueryInstrumentIdentity {
        &self.query_identity
    }
    pub fn event_identities(
        &self,
    ) -> &[(
        CalendarDate,
        market_squawk_domain::CorporateActionEventInstrumentIdentity,
        MarketDataInstrumentRecord,
    )] {
        &self.event_identities
    }
    pub const fn interval(&self) -> (CalendarDate, CalendarDate) {
        self.interval
    }
    pub fn rows(&self) -> &[CurrentOrdinaryActionRow] {
        &self.rows
    }
    pub fn source_audit(&self) -> &[u8] {
        &self.source_audit
    }
}

use super::source_plan::{check, date_of, instrument_of, ordinary_field, same_ordinary_effect};
use crate::{CorporateActionQueryIdentitySelection, CorporateActionSourceSnapshot};
use market_squawk_domain::{CorporateActionKind, CorporateActionSourceDisposition, InstrumentId};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// Compact completed-history authority. Constructed only from an already verified original
/// history and its native-session replay; no Deserialize or caller-authored fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedOrdinaryHistoryEvidence {
    instrument: InstrumentId,
    cutoff: Timestamp,
    first_date: CalendarDate,
    last_date: CalendarDate,
    first_open: Timestamp,
    native_terminal_close: Timestamp,
    terminal_close: Timestamp,
    manifest: DatasetManifestRef,
    origin_manifest: DatasetManifestRef,
    native_span_digest: crate::Sha256Digest,
    digest: crate::Sha256Digest,
}
impl CompletedOrdinaryHistoryEvidence {
    /// Validates the existing materialized financial input through the same streaming authority.
    pub fn try_from_history(
        history: &crate::CompleteMarketBarHistoryOutput,
    ) -> Result<Self, CorporateActionError> {
        Self::from_originals(
            history.selection(),
            history.read_receipt(),
            history.native_sessions(),
            history.bars().len(),
            history.bars().iter().cloned().map(Ok),
        )
    }
    /// Validates original source observations without constructing a second history vector.
    pub fn try_from_cursor(
        history: &crate::CompleteMarketBarHistoryCursor,
    ) -> Result<Self, CorporateActionError> {
        Self::from_originals(
            history.selection(),
            history.read_receipt(),
            history.native_sessions(),
            history.bar_count(),
            history
                .bars()
                .map(|row| row.map_err(|_| CorporateActionError::InvalidApplication)),
        )
    }
    fn from_originals(
        selection: &crate::CompleteMarketBarHistorySelection,
        read_receipt: &crate::CompleteMarketBarHistoryReadReceipt,
        native: Option<&crate::RetainedHistoryNativeSessions>,
        bar_count: usize,
        bars: impl Iterator<
            Item = Result<market_squawk_domain::MarketBarObservation, CorporateActionError>,
        >,
    ) -> Result<Self, CorporateActionError> {
        use sha2::{Digest as _, Sha256};
        let invalid = || CorporateActionError::InvalidApplication;
        let receipt = selection.receipt();
        let native = native.ok_or_else(invalid)?;
        let first = native
            .sessions()
            .first()
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?;
        let last = native
            .sessions()
            .last()
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?;
        let cutoff = read_receipt.knowledge_cutoff();
        let mut terminal_close = None;
        if !receipt.realized_outcome_eligible()
            || receipt.requested_range().is_none()
            || receipt.source_id().as_str() != "alpaca-basic-iex-market-data"
            || receipt.published_at() > cutoff
            || [
                receipt.knowledge_clocks().0,
                receipt.knowledge_clocks().1,
                receipt.knowledge_clocks().2,
                native.received_at(),
                native.published_at(),
            ]
            .iter()
            .any(|clock| *clock > cutoff)
            || native.received_at() > native.published_at()
            || native.sessions().len() != bar_count
        {
            return Err(invalid());
        }
        let mut span = Sha256::new();
        span.update(b"market-squawk/completed-history-native-span/v1");
        let mut verified_count = 0_usize;
        let mut previous_date = None;
        for (session, bar) in native.sessions().iter().zip(bars) {
            let bar = bar?;
            let session = session.map_err(|_| invalid())?;
            if previous_date.is_some_and(|date| date >= session.native_date()) {
                return Err(invalid());
            }
            previous_date = Some(session.native_date());
            let (start, end) = session.provider_period().ok_or_else(invalid)?;
            if !session.bar_present()
                || session.opens_at() >= session.closes_at_exclusive()
                || session.closes_at_exclusive() > cutoff
                || start > session.opens_at()
                || end < session.closes_at_exclusive()
                || end > cutoff
                || bar.completed_at() != Some(end)
                || bar.time_semantics().period_start() != Some(start)
                || bar.time_semantics().provider_timestamp() != session.provider_timestamp()
                || session.provider_timestamp().is_none()
                || bar.context().provenance().instrument_id() != Some(receipt.instrument_id())
                || bar.adjustment() != market_squawk_domain::MarketBarAdjustment::Raw
            {
                return Err(invalid());
            }
            terminal_close = Some(end);
            verified_count = verified_count.checked_add(1).ok_or_else(invalid)?;
            hash_native_session(
                &mut span,
                session.native_date(),
                session.opens_at(),
                session.closes_at_exclusive(),
            );
        }
        if verified_count != bar_count {
            return Err(invalid());
        };
        let terminal_close = terminal_close.ok_or_else(invalid)?;
        let native_span_digest = crate::Sha256Digest::new(span.finalize().into());
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/completed-ordinary-history/v1");
        hash.update(receipt.instrument_id().as_uuid().as_bytes());
        hash.update(cutoff.unix_nanos().to_be_bytes());
        hash.update(terminal_close.unix_nanos().to_be_bytes());
        for manifest in [
            selection.pinned().manifest(),
            read_receipt.origin_manifest(),
        ] {
            for text in [manifest.dataset_id().as_str(), manifest.schema().name()] {
                hash.update((text.len() as u64).to_be_bytes());
                hash.update(text.as_bytes());
            }
            hash.update(manifest.manifest_version().to_be_bytes());
            hash.update(manifest.schema_version().get().to_be_bytes());
            hash.update(manifest.schema().fingerprint());
            hash.update(manifest.content_hash().bytes());
        }
        for digest in [
            receipt.receipt_digest(),
            read_receipt.publication_receipt_digest(),
            read_receipt.history_content_digest(),
            read_receipt.result_digest(),
            native_span_digest,
        ] {
            hash.update(digest.bytes());
        }
        for digest in [
            Some(native.mapping_digest()),
            Some(native.source_replay_digest()),
            Some(native.capture_receipt_digest()),
            Some(native.calendar_origin_content_digest()),
            Some(native.calendar_capture_binding_digest()),
            native.calendar_component_digest(),
        ] {
            if let Some(digest) = digest {
                hash.update([
                    1,
                    match digest.algorithm() {
                        market_squawk_domain::DigestAlgorithm::Sha256 => 1,
                        market_squawk_domain::DigestAlgorithm::Blake3 => 2,
                    },
                ]);
                hash.update(digest.bytes());
            } else {
                hash.update([0]);
            }
        }
        Ok(Self {
            instrument: receipt.instrument_id(),
            cutoff,
            first_date: first.native_date(),
            last_date: last.native_date(),
            first_open: first.opens_at(),
            native_terminal_close: last.closes_at_exclusive(),
            terminal_close,
            manifest: selection.pinned().manifest().clone(),
            origin_manifest: read_receipt.origin_manifest().clone(),
            native_span_digest,
            digest: crate::Sha256Digest::new(hash.finalize().into()),
        })
    }
    /// Original native trading session close, distinct from provider period completion.
    pub const fn terminal_close(&self) -> Timestamp {
        self.native_terminal_close
    }
    pub const fn terminal_completion(&self) -> Timestamp {
        self.terminal_close
    }
    pub(super) const fn instrument_id(&self) -> InstrumentId {
        self.instrument
    }
    pub(crate) const fn evidence_digest(&self) -> crate::Sha256Digest {
        self.digest
    }
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub(crate) const fn origin_manifest(&self) -> &DatasetManifestRef {
        &self.origin_manifest
    }
    /// Checks the complete original native-session sequence, not only its first and last dates.
    pub(super) fn require_calendar_scope(
        &self,
        calendar: &RetainedCorporateActionCalendar,
        interval: (CalendarDate, CalendarDate),
        cutoff: Timestamp,
        terminal_completion: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentId, CorporateActionError> {
        use sha2::{Digest as _, Sha256};
        let invalid = || CorporateActionError::InvalidApplication;
        check(deadline, cancellation)?;
        let (first_open, last_close) = calendar
            .native_session_bounds(interval)
            .ok_or_else(invalid)?;
        if self.cutoff != cutoff
            || calendar.knowledge_cutoff() != cutoff
            || calendar.available_at() > cutoff
            || self.terminal_close != terminal_completion
            || terminal_completion < last_close
            || terminal_completion > cutoff
            || self.first_open != first_open
            || self.native_terminal_close != last_close
            || calendar.native_dates_in(interval).next() != Some(self.first_date)
            || calendar.native_dates_in(interval).last() != Some(self.last_date)
        {
            return Err(invalid());
        }
        let mut span = Sha256::new();
        span.update(b"market-squawk/completed-history-native-span/v1");
        for date in calendar.native_dates_in(interval) {
            check(deadline, cancellation)?;
            let session = calendar
                .date_session_on(date, cutoff, cutoff)
                .ok_or_else(invalid)?;
            hash_native_session(
                &mut span,
                date,
                session.opens_at,
                session.closes_at_exclusive,
            );
        }
        if crate::Sha256Digest::new(span.finalize().into()) != self.native_span_digest {
            return Err(invalid());
        }
        Ok(self.instrument)
    }
    pub(crate) fn covers_span(
        &self,
        instrument: InstrumentId,
        manifest: &DatasetManifestRef,
        knowledge: Timestamp,
        left: Timestamp,
        right: Timestamp,
    ) -> bool {
        self.instrument == instrument
            && &self.manifest == manifest
            && self.cutoff == knowledge
            && self.first_open <= left
            && left < right
            && right <= self.terminal_close
    }
}

fn hash_native_session(
    hash: &mut sha2::Sha256,
    date: CalendarDate,
    opens: Timestamp,
    closes: Timestamp,
) {
    use sha2::Digest as _;
    hash.update(date.days_since_unix_epoch().to_be_bytes());
    hash.update(opens.unix_nanos().to_be_bytes());
    hash.update(closes.unix_nanos().to_be_bytes());
}

impl CorporateActionPlan {
    /// Joins actual economic-date query receipts with independently retained lifecycle events.
    /// In-progress sessions need their real opening, never a fabricated completed daily bar.
    #[allow(clippy::too_many_arguments)]
    pub fn try_from_current_ordinary_source_reads(
        source: &CorporateActionSourceSnapshot,
        query_identity: &CorporateActionQueryIdentitySelection,
        calendar: &RetainedCorporateActionCalendar,
        reads: &[CurrentOrdinaryActionSourceRead],
        requested_instruments: &BTreeSet<InstrumentId>,
        interval: (CalendarDate, CalendarDate),
        policy: CorporateActionPolicy,
        payment_policy: CorporateActionPaymentPolicy,
        valuation_cutoff: Timestamp,
        evaluated_at: Timestamp,
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, CorporateActionError> {
        Self::try_from_hybrid_ordinary_source_reads(
            source,
            query_identity,
            calendar,
            &[],
            reads,
            requested_instruments,
            interval,
            policy,
            payment_policy,
            valuation_cutoff,
            evaluated_at,
            limits,
            deadline,
            cancellation,
        )
    }

    /// Joins completed original EOD sessions and a bounded economic-date tail. Each required
    /// native date has exactly one authority; missing sessions or fields remain explicit gaps.
    #[allow(clippy::too_many_arguments)]
    pub fn try_from_hybrid_ordinary_source_reads(
        source: &CorporateActionSourceSnapshot,
        query_identity: &CorporateActionQueryIdentitySelection,
        calendar: &RetainedCorporateActionCalendar,
        histories: &[(
            &RetainedTiingoEodActionHistory,
            &RetainedCorporateActionCalendar,
        )],
        reads: &[CurrentOrdinaryActionSourceRead],
        requested_instruments: &BTreeSet<InstrumentId>,
        interval: (CalendarDate, CalendarDate),
        policy: CorporateActionPolicy,
        payment_policy: CorporateActionPaymentPolicy,
        valuation_cutoff: Timestamp,
        evaluated_at: Timestamp,
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, CorporateActionError> {
        check(deadline, cancellation)?;
        if reads.is_empty()
            || reads.len() > limits.max_actions().get()
            || calendar.venue_id().as_str() != "iex"
        {
            return Err(CorporateActionError::InvalidApplication);
        }
        let cutoff = source.knowledge_cutoff();
        let dates: Vec<_> = calendar.native_dates_in(interval).collect();
        let Some((first_open, _)) = calendar.native_session_bounds(interval) else {
            return Err(CorporateActionError::InvalidApplication);
        };
        let native = calendar.native_replay();
        let retained_dates = native.requested_dates();
        if dates.is_empty()
            || valuation_cutoff < first_open
            || retained_dates.0 > interval.0
            || retained_dates.1 != interval.1
            || valuation_cutoff < native.complete_from()
            || valuation_cutoff >= native.complete_until()
        {
            return Err(CorporateActionError::InvalidApplication);
        }
        // Reuse the original all-family financial selector, retaining every source exclusion
        // and unresolved lifecycle event. Only its current-session application gate differs.
        let mut base = Self::try_from_source_reads_inner(
            source,
            query_identity,
            calendar,
            histories,
            requested_instruments,
            interval,
            policy,
            payment_policy,
            valuation_cutoff,
            evaluated_at,
            limits,
            deadline,
            cancellation,
            false,
            !histories.is_empty(),
        )?;
        let coverage = Arc::try_unwrap(
            base.source_coverage
                .take()
                .ok_or(CorporateActionError::InvalidApplication)?,
        )
        .map_err(|_| CorporateActionError::InvalidApplication)?;
        // Listing calendars must agree with the source calendar on every completed prefix
        // date and boundary. A short history cannot erase a gap or relabel another market.
        let mut historical_ends = BTreeMap::new();
        for (history, history_calendar) in histories {
            let instrument = history.history().selection().receipt().instrument_id();
            let native = history
                .history()
                .selection()
                .receipt()
                .date_windows()
                .ok_or(CorporateActionError::InvalidApplication)?
                .requested_dates();
            if !matches!(
                history_calendar.venue_id().as_str(),
                "ARCX" | "XNYS" | "XNAS"
            ) || historical_ends.insert(instrument, native.1).is_some()
            {
                return Err(CorporateActionError::InvalidApplication);
            }
            for date in calendar.native_dates_in((interval.0, native.1)) {
                check(deadline, cancellation)?;
                let source_session = calendar
                    .date_session_on(date, cutoff, evaluated_at)
                    .ok_or(CorporateActionError::InvalidApplication)?;
                let native_session = history_calendar
                    .date_session_on(date, cutoff, evaluated_at)
                    .ok_or(CorporateActionError::InvalidApplication)?;
                if source_session.opens_at != native_session.opens_at
                    || source_session.closes_at_exclusive != native_session.closes_at_exclusive
                    || native_session.closes_at_exclusive > valuation_cutoff
                {
                    return Err(CorporateActionError::InvalidApplication);
                }
            }
        }
        let mut applied = coverage.current_projection_records().to_vec();
        let mut queries = BTreeMap::new();
        let mut source_bytes = 0_usize;
        let mut row_count = histories
            .iter()
            .try_fold(source.actions().len(), |count, (history, _)| {
                count.checked_add(history.history().source_action_count())
            })
            .filter(|count| *count <= limits.max_actions().get())
            .ok_or(CorporateActionError::InvalidApplication)?;
        for read in reads {
            check(deadline, cancellation)?;
            let id = read.instrument.definition().instrument_id();
            source_bytes = source_bytes
                .checked_add(
                    read.retained_bytes()
                        .ok_or(CorporateActionError::RetainedSizeOverflow)?,
                )
                .ok_or(CorporateActionError::RetainedSizeOverflow)?;
            super::retained::require_retained_limit(
                source_bytes,
                limits.max_retained_bytes().get(),
            )?;
            row_count = row_count
                .checked_add(read.rows.len())
                .ok_or(CorporateActionError::InvalidApplication)?;
            if !requested_instruments.contains(&id)
                || read.knowledge_cutoff != cutoff
                || read.captured_at > cutoff
                || read.interval.0 != read.interval.1
                || !dates.contains(&read.interval.0)
                || historical_ends
                    .get(&id)
                    .is_some_and(|end| read.interval.0 <= *end)
                || (!histories.is_empty() && dates.last() != Some(&read.interval.0))
                || read.source_audit.is_empty()
                || row_count > limits.max_actions().get()
                || !read
                    .instrument
                    .definition()
                    .venue_mappings()
                    .iter()
                    .any(|mapping| mapping.venue_id() == calendar.venue_id())
                || queries
                    .insert((id, read.interval.0, read.family), read)
                    .is_some()
                || read.rows.iter().any(|row| row.date != read.interval.0)
            {
                return Err(CorporateActionError::InvalidApplication);
            }
            require_identity_session(
                &read.query_identity,
                &read.instrument,
                read.interval.0,
                calendar,
                cutoff,
                evaluated_at,
            )?;
            for (date, identity, definition) in read.event_identities() {
                if identity.selection.instrument_id != id {
                    return Err(CorporateActionError::InvalidApplication);
                }
                require_identity_session(
                    &identity.selection,
                    definition,
                    *date,
                    calendar,
                    cutoff,
                    evaluated_at,
                )?;
            }
        }
        let mut gaps = Vec::new();
        let mut reconciled = Vec::new();
        for &instrument in requested_instruments {
            for &date in &dates {
                if historical_ends
                    .get(&instrument)
                    .is_some_and(|end| date <= *end)
                {
                    continue;
                }
                let session = calendar
                    .date_session_on(date, cutoff, evaluated_at)
                    .filter(|session| session.opens_at <= valuation_cutoff)
                    .ok_or(CorporateActionError::InvalidApplication)?;
                for (family, field) in [
                    (
                        CurrentOrdinaryActionFamily::Distributions,
                        OrdinaryActionField::Cash,
                    ),
                    (
                        CurrentOrdinaryActionFamily::Splits,
                        OrdinaryActionField::Shares,
                    ),
                ] {
                    check(deadline, cancellation)?;
                    let Some(read) = queries.get(&(instrument, date, family)) else {
                        gaps.push(OrdinaryActionCoverageGap::NativeIntervalNotCovered {
                            instrument,
                        });
                        continue;
                    };
                    let mut normalized = Vec::new();
                    let mut missing_units = Vec::new();
                    let mut unavailable = false;
                    for row in read.rows() {
                        match &row.disposition {
                            CurrentOrdinaryActionDisposition::Cancelled { .. } => {}
                            CurrentOrdinaryActionDisposition::MissingUnit {
                                evidence,
                                instrument: source_instrument,
                                distribution,
                                payable_date,
                            } => {
                                if *source_instrument != Some(instrument) {
                                    unavailable = true;
                                    gaps.push(
                                        OrdinaryActionCoverageGap::EconomicSourceUnavailable {
                                            instrument,
                                            date,
                                            field,
                                            disposition:
                                                CorporateActionSourceDisposition::MissingIdentity,
                                        },
                                    );
                                } else if field != OrdinaryActionField::Cash
                                    || *distribution <= rust_decimal::Decimal::ZERO
                                    || payable_date.is_some_and(|payable| payable < date)
                                {
                                    return Err(CorporateActionError::InvalidApplication);
                                } else {
                                    missing_units.push((*distribution, *payable_date, *evidence));
                                }
                            }
                            CurrentOrdinaryActionDisposition::Unsupported { .. } => {
                                unavailable = true;
                                gaps.push(OrdinaryActionCoverageGap::EconomicSourceUnavailable { instrument, date, field,
                                    disposition: CorporateActionSourceDisposition::UnsupportedCanonicalEconomics });
                            }
                            CurrentOrdinaryActionDisposition::Normalized {
                                record,
                                payable_date,
                            } => {
                                if instrument_of(record)? != instrument
                                    || date_of(record)? != date
                                    || ordinary_field(record.observation().action()) != Some(field)
                                    || record.observation().context().provenance().ingested_at()
                                        > cutoff
                                {
                                    return Err(CorporateActionError::InvalidApplication);
                                }
                                normalized.push((record, *payable_date));
                            }
                        }
                    }
                    if unavailable {
                        continue;
                    }
                    let existing: Vec<_> = applied
                        .iter()
                        .enumerate()
                        .filter_map(|(index, record)| {
                            (instrument_of(record).ok() == Some(instrument)
                                && date_of(record).ok() == Some(date)
                                && ordinary_field(record.observation().action()) == Some(field))
                            .then_some(index)
                        })
                        .collect();
                    if !missing_units.is_empty() {
                        match (
                            normalized.as_slice(),
                            missing_units.as_slice(),
                            existing.as_slice(),
                        ) {
                            ([], [(distribution, payable_date, evidence)], [index])
                                if matches!(
                                    applied[*index].observation().action(),
                                    CorporateActionKind::CashDividend { amount }
                                        | CorporateActionKind::ReturnOfCapital { amount }
                                        if amount.amount() == *distribution
                                ) && applied[*index].application().is_some_and(
                                    |application| {
                                        application.source_snapshot_digest()
                                            == source.receipt_digest()
                                            && application.payable_date() == *payable_date
                                    },
                                ) =>
                            {
                                // Only the independently admitted Alpaca event owns Money and
                                // application timing. Preserve Tiingo's MissingUnit evidence and
                                // the original Alpaca record without minting or changing cash.
                                reconciled.push(ReconciledOrdinaryAction {
                                    instrument,
                                    date,
                                    field,
                                    applied_alpaca_record: applied[*index].evidence_digest(),
                                    corroborating_tiingo_record: *evidence,
                                });
                            }
                            ([], [_], []) => {
                                gaps.push(OrdinaryActionCoverageGap::EconomicSourceUnavailable {
                                    instrument,
                                    date,
                                    field,
                                    disposition: CorporateActionSourceDisposition::MissingCurrency,
                                });
                            }
                            _ => gaps.push(OrdinaryActionCoverageGap::SourceDisagreement {
                                instrument,
                                date,
                                field,
                            }),
                        }
                        continue;
                    }
                    match (normalized.as_slice(), existing.as_slice()) {
                        ([], []) => {} // Original authenticated terminal scope, including cancellations.
                        ([(record, payable_date)], [index])
                            if same_ordinary_effect(
                                applied[*index].observation().action(),
                                record.observation().action(),
                            ) && (field != OrdinaryActionField::Cash
                                || applied[*index].application().is_some_and(|application| {
                                    application.payable_date() == *payable_date
                                })) =>
                        {
                            reconciled.push(ReconciledOrdinaryAction {
                                instrument,
                                date,
                                field,
                                applied_alpaca_record: applied[*index].evidence_digest(),
                                corroborating_tiingo_record: record.evidence_digest(),
                            });
                        }
                        ([(record, payable_date)], []) => {
                            let payable_session = match payment_policy {
                                CorporateActionPaymentPolicy::RetainReceivable => None,
                                CorporateActionPaymentPolicy::EndOfReportedPayableSessionV1 => {
                                    payable_date
                                        .and_then(|date| {
                                            calendar.date_session_on(date, cutoff, evaluated_at)
                                        })
                                        .filter(|session| {
                                            session.closes_at_exclusive <= evaluated_at
                                        })
                                }
                            };
                            let application = CorporateActionApplication::try_from_retained_values(
                                record,
                                read.binding_digest,
                                cutoff,
                                session,
                                *payable_date,
                                payable_session,
                                payment_policy,
                            )
                            .map_err(|_| CorporateActionError::InvalidApplication)?;
                            applied.push(
                                (*record)
                                    .clone()
                                    .with_application(application)
                                    .map_err(|_| CorporateActionError::InvalidApplication)?,
                            );
                        }
                        _ => gaps.push(OrdinaryActionCoverageGap::SourceDisagreement {
                            instrument,
                            date,
                            field,
                        }),
                    }
                }
            }
        }
        let mut boundaries = BTreeMap::<(InstrumentId, CalendarDate), (usize, bool)>::new();
        for record in &applied {
            let entry = boundaries
                .entry((instrument_of(record)?, date_of(record)?))
                .or_default();
            entry.0 += 1;
            entry.1 |= matches!(
                record.observation().action(),
                CorporateActionKind::Split { .. }
                    | CorporateActionKind::Merger { .. }
                    | CorporateActionKind::Spinoff { .. }
            );
        }
        for ((instrument, date), (count, changes_units)) in boundaries {
            if count > 1 && changes_units {
                gaps.push(OrdinaryActionCoverageGap::SameDateEconomicOrdering { instrument, date });
            }
        }
        let projection = applied.clone().into_boxed_slice();
        let mut plan = Self::try_build(policy, cutoff, valuation_cutoff, applied, limits)?;
        let coverage = coverage.try_complete_current(
            reads,
            calendar,
            projection,
            gaps,
            reconciled,
            limits,
            deadline,
            cancellation,
        )?;
        plan.retained_bytes = plan
            .retained_bytes
            .checked_add(coverage.retained_bytes)
            .ok_or(CorporateActionError::RetainedSizeOverflow)?;
        super::retained::require_retained_limit(
            plan.retained_bytes,
            limits.max_retained_bytes().get(),
        )?;
        plan.source_coverage = Some(Arc::new(coverage));
        check(deadline, cancellation)?;
        Ok(plan)
    }
}

fn require_identity_session(
    identity: &market_squawk_domain::CorporateActionQueryInstrumentIdentity,
    definition: &MarketDataInstrumentRecord,
    date: CalendarDate,
    calendar: &RetainedCorporateActionCalendar,
    cutoff: Timestamp,
    evaluated_at: Timestamp,
) -> Result<(), CorporateActionError> {
    let session = calendar
        .date_session_on(date, cutoff, evaluated_at)
        .ok_or(CorporateActionError::InvalidApplication)?;
    let validity = definition.definition().effective_interval();
    if identity.instrument_id != definition.definition().instrument_id()
        || identity.effective_at != session.opens_at
        || identity.knowledge_at > cutoff
        || definition.published_at() > identity.knowledge_at
        || identity.definition_revision_digest != definition.revision_digest()
        || identity.definition_revision_sequence != definition.revision_sequence()
        || validity.starts_at() > session.opens_at
        || validity
            .ends_at()
            .is_some_and(|end| end < session.closes_at_exclusive)
        || identity.provider_identity_validity.starts_at() > session.opens_at
        || identity
            .provider_identity_validity
            .ends_at()
            .is_some_and(|end| end < session.closes_at_exclusive)
    {
        return Err(CorporateActionError::InvalidApplication);
    }
    Ok(())
}
