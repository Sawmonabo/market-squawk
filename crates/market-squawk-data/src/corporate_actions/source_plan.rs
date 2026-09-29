//! Sole source/calendar projection and ordinary overlap selector for application and datasets.
//! A recovery-decoded or caller-built plan cannot acquire the private admission marker.

use super::*;
use crate::{CorporateActionQueryIdentitySelection, CorporateActionSourceSnapshot};
use market_squawk_adapter_tiingo::TiingoEodActionFieldDisposition;
use market_squawk_domain::{
    CalendarDate, CorporateActionKind, CorporateActionSourceDisposition,
    CorporateActionSourcePayload, DigestAlgorithm, EvidenceDigest, InstrumentId, SourceIdentifier,
    Timestamp,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// Why one returned source event still prevents an accounting result in the requested interval.
#[derive(Clone, Debug)]
pub enum ApplicableActionGap {
    SourceDisposition {
        action: SourceIdentifier,
        disposition: CorporateActionSourceDisposition,
    },
    MissingEconomicDate {
        action: SourceIdentifier,
    },
    MissingEffectiveSession {
        action: SourceIdentifier,
        date: CalendarDate,
    },
    InvalidPayableOrder {
        action: SourceIdentifier,
        ex_date: CalendarDate,
        payable_date: CalendarDate,
    },
    /// Two source events at one daily boundary require evidenced ordering/share-basis semantics.
    SameDateEconomicOrdering {
        instrument: InstrumentId,
        date: CalendarDate,
    },
}

/// Precise failure of the requested source-known ordinary-field coverage.
#[derive(Clone, Debug)]
pub enum OrdinaryActionCoverageGap {
    EconomicSourceUnavailable {
        instrument: InstrumentId,
        date: CalendarDate,
        field: OrdinaryActionField,
        disposition: CorporateActionSourceDisposition,
    },
    MissingInstrument {
        instrument: InstrumentId,
    },
    NativeIntervalNotCovered {
        instrument: InstrumentId,
    },
    MissingField {
        instrument: InstrumentId,
        date: CalendarDate,
        field: OrdinaryActionField,
        disposition: TiingoEodActionFieldDisposition,
    },
    MissingSession {
        instrument: InstrumentId,
        date: CalendarDate,
    },
    SourceDisagreement {
        instrument: InstrumentId,
        date: CalendarDate,
        field: OrdinaryActionField,
    },
    SameDateEconomicOrdering {
        instrument: InstrumentId,
        date: CalendarDate,
    },
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OrdinaryActionField {
    Shares,
    Cash,
}

/// Retains both original observations when one exact economic effect is applied only once.
#[derive(Clone, Debug)]
pub struct ReconciledOrdinaryAction {
    pub instrument: InstrumentId,
    pub date: CalendarDate,
    pub field: OrdinaryActionField,
    pub applied_alpaca_record: EvidenceDigest,
    pub corroborating_tiingo_record: EvidenceDigest,
}

impl CorporateActionPlan {
    /// Rebuilds the single financial selector from actual source reads. Empty `reads` retains
    /// finite-query provenance but never produces complete ordinary interval admission.
    #[allow(clippy::too_many_arguments)]
    pub fn try_from_source_reads(
        source: &CorporateActionSourceSnapshot,
        query_identity: &CorporateActionQueryIdentitySelection,
        calendar: &RetainedCorporateActionCalendar,
        reads: &[(
            &RetainedTiingoEodActionHistory,
            &RetainedCorporateActionCalendar,
        )],
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
        Self::try_from_source_reads_inner(source, query_identity, calendar, reads,
            requested_instruments, interval, policy, payment_policy, valuation_cutoff,
            evaluated_at, limits, deadline, cancellation, true)
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_from_source_reads_inner(
        source: &CorporateActionSourceSnapshot,
        query_identity: &CorporateActionQueryIdentitySelection,
        calendar: &RetainedCorporateActionCalendar,
        reads: &[(&RetainedTiingoEodActionHistory, &RetainedCorporateActionCalendar)],
        requested_instruments: &BTreeSet<InstrumentId>,
        interval: (CalendarDate, CalendarDate),
        policy: CorporateActionPolicy,
        payment_policy: CorporateActionPaymentPolicy,
        valuation_cutoff: Timestamp,
        evaluated_at: Timestamp,
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
        require_completed_session: bool,
    ) -> Result<Self, CorporateActionError> {
        check(deadline, cancellation)?;
        let knowledge_cutoff = source.knowledge_cutoff();
        if !query_identity.matches_source(source)
            || requested_instruments.iter().any(|id| {
                !query_identity
                    .retained()
                    .iter()
                    .any(|value| value.instrument_id == *id)
            })
            || interval.0 > interval.1
            || requested_instruments.is_empty()
            || requested_instruments.len() > 32
            || reads.len() > 32
            || evaluated_at < knowledge_cutoff
            || valuation_cutoff > knowledge_cutoff
            || calendar.knowledge_cutoff() != knowledge_cutoff
            || source.source_actions().len() > limits.max_actions().get()
        {
            return Err(CorporateActionError::InvalidApplication);
        }
        let candidate_count = reads
            .iter()
            .try_fold(source.actions().len(), |count, (read, _)| {
                count.checked_add(read.history().source_actions().len())
            })
            .ok_or(CorporateActionError::InvalidApplication)?;
        if candidate_count > limits.max_actions().get() {
            return Err(CorporateActionError::InvalidApplication);
        }
        let mut by_instrument = BTreeMap::new();
        let mut gaps = Vec::new();
        let cutoff = source.knowledge_cutoff();
        for (index, (read, calendar)) in reads.iter().enumerate() {
            check(deadline, cancellation)?;
            let receipt = read.history().selection().receipt();
            let instrument = receipt.instrument_id();
            let graph = receipt
                .date_windows()
                .ok_or(CorporateActionError::InvalidApplication)?;
            if !requested_instruments.contains(&instrument)
                || read.knowledge_cutoff() != cutoff
                || calendar.knowledge_cutoff() != cutoff
                || read.history().read_receipt().knowledge_cutoff() != cutoff
                || !read.matches_calendar(calendar)
                || graph.calendar().capture_binding_digest != calendar.binding_digest()
                || graph.calendar().origin_content_digest.bytes()
                    != calendar.manifest().content_hash().bytes()
                || by_instrument.insert(instrument, index).is_some()
                || read.actions().rows().len() != graph.sessions().len()
                || read.actions().rows().len() > 64_000
                || read
                    .actions()
                    .rows()
                    .iter()
                    .zip(graph.sessions())
                    .any(|(row, session)| row.date != session.date)
            {
                return Err(CorporateActionError::InvalidApplication);
            }
            if read
                .actions()
                .rows()
                .iter()
                .map(|row| row.date)
                .filter(|date| *date >= interval.0 && *date <= interval.1)
                .ne(calendar.native_dates_in(interval))
                || calendar
                    .native_session_bounds(interval)
                    .is_none_or(|(start, end)| {
                        valuation_cutoff < start
                            || end
                                .unix_nanos()
                                .checked_add(1)
                                .is_none_or(|exclusive| valuation_cutoff.unix_nanos() > exclusive)
                    })
            {
                gaps.push(OrdinaryActionCoverageGap::NativeIntervalNotCovered { instrument });
            }
            let native = graph.requested_dates();
            if native.0 > interval.0 || native.1 < interval.1 {
                gaps.push(OrdinaryActionCoverageGap::NativeIntervalNotCovered { instrument });
            }
        }
        for &instrument in requested_instruments.iter().filter(|_| !reads.is_empty()) {
            if !by_instrument.contains_key(&instrument) {
                gaps.push(OrdinaryActionCoverageGap::MissingInstrument { instrument });
            }
        }

        let mut records = BTreeMap::new();
        for record in source.actions() {
            let id = record
                .observation()
                .context()
                .provenance()
                .source_identifier()
                .clone();
            if records.insert(id, record).is_some() {
                return Err(CorporateActionError::InvalidApplication);
            }
        }
        let mut applied = Vec::new();
        let mut outside_window = Vec::new();
        let mut unresolved = Vec::new();
        for descriptor in source.source_actions() {
            check(deadline, cancellation)?;
            let CorporateActionSourcePayload::ReturnedAction {
                action_id,
                dates,
                disposition,
                ..
            } = descriptor.payload()
            else {
                return Err(CorporateActionError::InvalidApplication);
            };
            // An unresolved native identity cannot be silently assigned to a different subject.
            if descriptor
                .context()
                .provenance()
                .instrument_id()
                .is_some_and(|instrument| !requested_instruments.contains(&instrument))
            {
                continue;
            }
            let Some(date) = dates.economic_date() else {
                unresolved.push(ApplicableActionGap::MissingEconomicDate {
                    action: action_id.clone(),
                });
                continue;
            };
            if date < interval.0 || date > interval.1 {
                outside_window.push(action_id.clone());
                continue;
            }
            if *disposition != CorporateActionSourceDisposition::Normalized {
                unresolved.push(ApplicableActionGap::SourceDisposition {
                    action: action_id.clone(),
                    disposition: *disposition,
                });
                continue;
            }
            let record = records
                .get(action_id)
                .ok_or(CorporateActionError::InvalidApplication)?;
            let Some(session) = calendar_for(
                descriptor.context().provenance().instrument_id(),
                calendar,
                reads,
                &by_instrument,
            )
            .and_then(|calendar| calendar.date_session_on(date, knowledge_cutoff, evaluated_at)) else {
                unresolved.push(ApplicableActionGap::MissingEffectiveSession {
                    action: action_id.clone(),
                    date,
                });
                continue;
            };
            if (require_completed_session && session.closes_at_exclusive > evaluated_at)
                || (!require_completed_session && session.opens_at > valuation_cutoff) {
                unresolved.push(ApplicableActionGap::MissingEffectiveSession {
                    action: action_id.clone(),
                    date,
                });
                continue;
            }
            if dates.payable_date.is_some_and(|payable| payable < date) {
                unresolved.push(ApplicableActionGap::InvalidPayableOrder {
                    action: action_id.clone(),
                    ex_date: date,
                    payable_date: dates
                        .payable_date
                        .ok_or(CorporateActionError::InvalidApplication)?,
                });
                continue;
            }
            let payable_session = match payment_policy {
                CorporateActionPaymentPolicy::RetainReceivable => None,
                CorporateActionPaymentPolicy::EndOfReportedPayableSessionV1 => dates
                    .payable_date
                    .and_then(|payable| {
                        calendar_for(
                            descriptor.context().provenance().instrument_id(),
                            calendar,
                            reads,
                            &by_instrument,
                        )
                        .and_then(|calendar| {
                            calendar.date_session_on(payable, knowledge_cutoff, evaluated_at)
                        })
                    })
                    .filter(|session| session.closes_at_exclusive <= evaluated_at),
            };
            let application = CorporateActionApplication::try_from_retained_values(
                record,
                source.receipt_digest(),
                knowledge_cutoff,
                session,
                dates.payable_date,
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
        let mut alpaca =
            BTreeMap::<(InstrumentId, CalendarDate, OrdinaryActionField), Vec<usize>>::new();
        for (index, record) in applied.iter().enumerate() {
            if let Some(field) = ordinary_field(record.observation().action()) {
                alpaca
                    .entry((instrument_of(record)?, date_of(record)?, field))
                    .or_default()
                    .push(index);
            }
        }
        let mut reconciled = Vec::new();
        for (read, calendar) in reads {
            let instrument = read.history().selection().receipt().instrument_id();
            let records = read.records();
            let mut seen = BTreeSet::new();
            for row in read.actions().rows() {
                check(deadline, cancellation)?;
                if row.date < interval.0 || row.date > interval.1 {
                    continue;
                }
                for (field, disposition) in [
                    (OrdinaryActionField::Shares, row.shares),
                    (OrdinaryActionField::Cash, row.cash),
                ] {
                    let key = (instrument, row.date, field);
                    if !seen.insert(key) {
                        return Err(CorporateActionError::InvalidApplication);
                    }
                    let source_matches = alpaca.get(&key).map(Vec::as_slice).unwrap_or(&[]);
                    match disposition {
                        TiingoEodActionFieldDisposition::ExplicitNoEvent => {
                            if !source_matches.is_empty() {
                                gaps.push(OrdinaryActionCoverageGap::SourceDisagreement {
                                    instrument,
                                    date: row.date,
                                    field,
                                });
                            }
                        }
                        TiingoEodActionFieldDisposition::Normalized { observation_index } => {
                            let record = records
                                .get(observation_index)
                                .ok_or(CorporateActionError::InvalidApplication)?;
                            if instrument_of(record)? != instrument
                                || date_of(record)? != row.date
                                || ordinary_field(record.observation().action()) != Some(field)
                            {
                                return Err(CorporateActionError::InvalidApplication);
                            }
                            if let [index] = source_matches {
                                let retained = &applied[*index];
                                if same_ordinary_effect(
                                    retained.observation().action(),
                                    record.observation().action(),
                                ) {
                                    reconciled.push(ReconciledOrdinaryAction {
                                        instrument,
                                        date: row.date,
                                        field,
                                        applied_alpaca_record: retained.evidence_digest(),
                                        corroborating_tiingo_record: record.evidence_digest(),
                                    });
                                } else {
                                    gaps.push(OrdinaryActionCoverageGap::SourceDisagreement {
                                        instrument,
                                        date: row.date,
                                        field,
                                    });
                                }
                            } else if source_matches.is_empty() {
                                let Some(session) = calendar
                                    .date_session_on(row.date, cutoff, evaluated_at)
                                    .filter(|session| session.closes_at_exclusive <= evaluated_at)
                                else {
                                    gaps.push(OrdinaryActionCoverageGap::MissingSession {
                                        instrument,
                                        date: row.date,
                                    });
                                    continue;
                                };
                                // Tiingo EOD reports ex-date, not payment date. This claim remains
                                // unspendable until separately evidenced payment terms exist.
                                let application =
                                    CorporateActionApplication::try_from_retained_values(
                                        record,
                                        digest(
                                            read.history().read_receipt().result_digest().bytes(),
                                        ),
                                        cutoff,
                                        session,
                                        None,
                                        None,
                                        payment_policy,
                                    )
                                    .map_err(|_| CorporateActionError::InvalidApplication)?;
                                applied.push(
                                    record
                                        .clone()
                                        .with_application(application)
                                        .map_err(|_| CorporateActionError::InvalidApplication)?,
                                );
                            } else {
                                // A provider's daily total is not evidence for combining or ordering
                                // multiple independent source events, even when a sum could match.
                                gaps.push(OrdinaryActionCoverageGap::SourceDisagreement {
                                    instrument,
                                    date: row.date,
                                    field,
                                });
                            }
                        }
                        _ => gaps.push(OrdinaryActionCoverageGap::MissingField {
                            instrument,
                            date: row.date,
                            field,
                            disposition,
                        }),
                    }
                }
            }
            for &(other, date, field) in alpaca.keys() {
                if other == instrument && !seen.contains(&(other, date, field)) {
                    gaps.push(OrdinaryActionCoverageGap::SourceDisagreement {
                        instrument,
                        date,
                        field,
                    });
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
                if reads.is_empty() {
                    unresolved
                        .push(ApplicableActionGap::SameDateEconomicOrdering { instrument, date });
                } else {
                    gaps.push(OrdinaryActionCoverageGap::SameDateEconomicOrdering {
                        instrument,
                        date,
                    });
                }
            }
        }
        if applied.len() > limits.max_actions().get() {
            return Err(CorporateActionError::InvalidApplication);
        }
        let projection_records = applied.clone().into_boxed_slice();
        let mut plan =
            Self::try_build(policy, knowledge_cutoff, valuation_cutoff, applied, limits)?;
        let coverage = CorporateActionSourceCoverage::try_retain(
            source,
            query_identity,
            calendar,
            reads,
            requested_instruments,
            interval,
            evaluated_at,
            valuation_cutoff,
            payment_policy,
            projection_records,
            outside_window,
            unresolved,
            gaps,
            reconciled,
            limits,
            deadline,
            cancellation,
        )?;
        let required = plan
            .retained_bytes
            .checked_add(coverage.retained_bytes)
            .ok_or(CorporateActionError::RetainedSizeOverflow)?;
        super::retained::require_retained_limit(required, limits.max_retained_bytes().get())?;
        plan.retained_bytes = required;
        plan.source_coverage = Some(Arc::new(coverage));
        check(deadline, cancellation)?;
        Ok(plan)
    }
}

fn calendar_for<'a>(
    instrument: Option<InstrumentId>,
    fallback: &'a RetainedCorporateActionCalendar,
    reads: &'a [(
        &RetainedTiingoEodActionHistory,
        &RetainedCorporateActionCalendar,
    )],
    by: &BTreeMap<InstrumentId, usize>,
) -> Option<&'a RetainedCorporateActionCalendar> {
    if reads.is_empty() {
        Some(fallback)
    } else {
        instrument
            .and_then(|id| by.get(&id))
            .map(|index| reads[*index].1)
    }
}
pub(super) fn check(deadline: Instant, cancellation: &CancellationToken) -> Result<(), CorporateActionError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(CorporateActionError::SourceReadInterrupted)
    } else {
        Ok(())
    }
}
pub(super) fn instrument_of(record: &CorporateActionRecord) -> Result<InstrumentId, CorporateActionError> {
    record
        .observation()
        .context()
        .provenance()
        .instrument_id()
        .ok_or(CorporateActionError::InvalidApplication)
}
pub(super) fn date_of(record: &CorporateActionRecord) -> Result<CalendarDate, CorporateActionError> {
    record
        .observation()
        .context()
        .time()
        .effective()
        .calendar_date_value()
        .ok_or(CorporateActionError::InvalidApplication)
}
pub(super) fn ordinary_field(action: &CorporateActionKind) -> Option<OrdinaryActionField> {
    match action {
        CorporateActionKind::Split { .. } => Some(OrdinaryActionField::Shares),
        CorporateActionKind::CashDividend { .. } | CorporateActionKind::ReturnOfCapital { .. } => {
            Some(OrdinaryActionField::Cash)
        }
        _ => None,
    }
}
pub(super) fn same_ordinary_effect(a: &CorporateActionKind, b: &CorporateActionKind) -> bool {
    match (a, b) {
        (
            CorporateActionKind::Split {
                numerator: an,
                denominator: ad,
            },
            CorporateActionKind::Split {
                numerator: bn,
                denominator: bd,
            },
        ) => u64::from(an.get()) * u64::from(bd.get()) == u64::from(bn.get()) * u64::from(ad.get()),
        (
            CorporateActionKind::CashDividend { amount: a }
            | CorporateActionKind::ReturnOfCapital { amount: a },
            CorporateActionKind::CashDividend { amount: b },
        ) => a == b,
        _ => false,
    }
}
fn digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}
