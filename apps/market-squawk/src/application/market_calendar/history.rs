//! Current completed-period qualification over an exact immutable history read.

use market_squawk_data::CompleteMarketBarHistoryOutput;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use sha2::{Digest as _, Sha256};

use super::{
    CompletedMarketSessionAuthority, CompletedMarketSessionCurrentnessReceipt,
    CompletedMarketSessionCurrentnessResolution, CompletedMarketSessionDateReceipt,
    CompletedMarketSessionError, CompletedMarketSessionRead, CompletedMarketSessionReference,
    CompletedMarketSessionResolution, CompletedMarketSessionUnavailable, MarketCalendarClock,
    SystemMarketCalendarClock,
};

/// A complete historical window may still omit a more recently completed period.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HistoryCurrentSessionStatus {
    Covered,
    MissingLatestCompletedPeriod,
    CalendarUnavailable(CompletedMarketSessionUnavailable),
}

/// Bounded sealed qualification: no bars, raw bodies, or copied history matrices are retained.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HistoryCurrentSessionQualification {
    status: HistoryCurrentSessionStatus,
    as_of: Timestamp,
    expected_provider_timestamp: Option<Timestamp>,
    currentness: Option<CompletedMarketSessionCurrentnessReceipt>,
    evidence_digest: EvidenceDigest,
    nominal: Option<NominalHistoryCurrentSession>,
}

/// Closed original-calendar coordinate; no copied native calendar or source authority is stored.
#[derive(Clone, Debug, Eq, PartialEq)]
struct NominalHistoryCurrentSession {
    reference: CompletedMarketSessionReference,
    expected: Option<CompletedMarketSessionDateReceipt>,
}

impl HistoryCurrentSessionQualification {
    pub(crate) const fn current_session_covered(&self) -> bool {
        matches!(self.status, HistoryCurrentSessionStatus::Covered)
    }

    pub(crate) const fn status(&self) -> HistoryCurrentSessionStatus {
        self.status
    }
    pub(crate) const fn as_of(&self) -> Timestamp {
        self.as_of
    }
    pub(crate) const fn expected_provider_timestamp(&self) -> Option<Timestamp> {
        self.expected_provider_timestamp
    }
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    pub(crate) fn nominal_calendar_reference(&self) -> Option<&CompletedMarketSessionReference> {
        self.nominal.as_ref().map(|nominal| &nominal.reference)
    }

    /// Qualifies native-date history against its physically reopened original calendar. Both
    /// the actual regular close and the absence of a later completed session are source-owned.
    pub(crate) fn qualify_nominal(
        history: &CompleteMarketBarHistoryOutput,
        calendar: &CompletedMarketSessionRead,
        as_of: Timestamp,
    ) -> Result<Self, CompletedMarketSessionError> {
        let invalid = || CompletedMarketSessionError::InvalidEvidence;
        let publication = history.selection().receipt();
        let graph = publication.date_windows().ok_or_else(invalid)?;
        let original = graph.calendar();
        let native = history.native_sessions().ok_or_else(invalid)?;
        let last = history.bars().last().ok_or_else(invalid)?;
        let date = last
            .time_semantics()
            .nominal_daily_date()
            .ok_or_else(invalid)?
            .date();
        let actual = native
            .sessions()
            .find_date(date)
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?;
        let (available, received, ingested) = publication.knowledge_clocks();
        if history.read_receipt().knowledge_cutoff() != as_of
            || calendar.source_action_calendar().knowledge_cutoff() != as_of
            || calendar.reference().origin_content_digest() != original.origin_content_digest
            || calendar.reference().capture_binding_digest() != original.capture_binding_digest
            || native.calendar_origin_content_digest() != original.origin_content_digest
            || native.calendar_capture_binding_digest() != original.capture_binding_digest
            || native.source_replay_digest() != calendar.source_action_calendar().evidence_digest()
            || !original.relationship.matches(
                calendar.venue_id(),
                publication.venue_id(),
                graph.requested_dates(),
            )
            || last.context().provenance().venue_id() != Some(publication.venue_id())
            || last.interval() != publication.interval()
            || last.context().time().effective().calendar_date_value() != Some(date)
            || !actual.bar_present()
            || actual.provider_timestamp().is_some()
            || actual.provider_period().is_some()
            || actual.opens_at() >= actual.closes_at_exclusive()
            || actual.closes_at_exclusive() > as_of
            || [
                available,
                received,
                ingested,
                publication.published_at(),
                publication.capture_recorded_at(),
                native.published_at(),
                native.received_at(),
                calendar.available_at(),
            ]
            .into_iter()
            .any(|at| at > as_of)
        {
            return Err(invalid());
        }
        let (expected, currentness) =
            calendar.latest_completed_regular_session(as_of, as_of, as_of)?;
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/history-current-nominal-regular-session/v1\0");
        for identity in [
            history.read_receipt().result_digest().bytes(),
            publication.receipt_digest().bytes(),
            native.mapping_digest().bytes(),
            original.origin_content_digest.bytes(),
            original.capture_binding_digest.bytes(),
            original.relationship.relationship_digest().bytes(),
            currentness.evidence().bytes(),
        ] {
            digest.update(identity);
        }
        digest.update(as_of.unix_nanos().to_be_bytes());
        digest.update(date.year().to_be_bytes());
        digest.update([date.month(), date.day()]);
        digest.update(actual.opens_at().unix_nanos().to_be_bytes());
        digest.update(actual.closes_at_exclusive().unix_nanos().to_be_bytes());
        let status = if let Some(expected) = &expected {
            if expected.provider_period().is_some() || expected.reference() != calendar.reference()
            {
                return Err(invalid());
            }
            let covered = expected.date() == date
                && expected.opens_at() == actual.opens_at()
                && expected.closes_at_exclusive() == actual.closes_at_exclusive();
            digest.update([1, u8::from(covered)]);
            digest.update(expected.evidence_digest().bytes());
            digest.update(expected.date().year().to_be_bytes());
            digest.update([expected.date().month(), expected.date().day()]);
            digest.update(expected.opens_at().unix_nanos().to_be_bytes());
            digest.update(expected.closes_at_exclusive().unix_nanos().to_be_bytes());
            if covered {
                HistoryCurrentSessionStatus::Covered
            } else {
                HistoryCurrentSessionStatus::MissingLatestCompletedPeriod
            }
        } else {
            digest.update([0]);
            HistoryCurrentSessionStatus::CalendarUnavailable(
                CompletedMarketSessionUnavailable::NoCompletedPeriod,
            )
        };
        Ok(Self {
            status,
            as_of,
            expected_provider_timestamp: None,
            currentness: Some(currentness),
            evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
            nominal: Some(NominalHistoryCurrentSession {
                reference: calendar.reference().clone(),
                expected,
            }),
        })
    }

    /// The caller physically reopens this qualification's original reference before this check.
    /// A new completion at the actual clock invalidates the old result without renewing sources.
    pub(crate) fn recheck_nominal(
        &self,
        calendar: &CompletedMarketSessionRead,
        as_of: Timestamp,
    ) -> Result<(), CompletedMarketSessionError> {
        let nominal = self
            .nominal
            .as_ref()
            .ok_or(CompletedMarketSessionError::InvalidRequest)?;
        if as_of != self.as_of || calendar.reference() != &nominal.reference {
            return Err(CompletedMarketSessionError::InvalidRequest);
        }
        if !self.current_session_covered() {
            return Err(CompletedMarketSessionError::Unavailable);
        }
        let expected = nominal
            .expected
            .as_ref()
            .ok_or(CompletedMarketSessionError::Unavailable)?;
        let expected_currentness = self
            .currentness
            .as_ref()
            .ok_or(CompletedMarketSessionError::Unavailable)?;
        let (original, currentness) =
            calendar.latest_completed_regular_session(as_of, as_of, as_of)?;
        if original.as_ref() != Some(expected) || &currentness != expected_currentness {
            return Err(CompletedMarketSessionError::Unavailable);
        }
        let now = SystemMarketCalendarClock
            .now()
            .map_err(|_| CompletedMarketSessionError::Unavailable)?;
        let (latest, currentness) = calendar.latest_completed_regular_session(as_of, now, now)?;
        if latest.as_ref() != Some(expected)
            || currentness.identity() != expected_currentness.identity()
        {
            return Err(CompletedMarketSessionError::Unavailable);
        }
        Ok(())
    }

    /// Revalidates the same retained source generation immediately before the financial result
    /// is published, while preserving the original financial cutoff. The evidence owner checks
    /// live revocation independently of that cutoff.
    pub(crate) fn recheck(
        &self,
        authority: &CompletedMarketSessionAuthority,
        as_of: Timestamp,
    ) -> Result<(), CompletedMarketSessionError> {
        if as_of != self.as_of || self.nominal.is_some() {
            return Err(CompletedMarketSessionError::InvalidRequest);
        }
        let Some(expected) = &self.currentness else {
            return Err(CompletedMarketSessionError::Unavailable);
        };
        match authority.recheck_currentness(expected.identity(), as_of) {
            CompletedMarketSessionCurrentnessResolution::Current(actual) if &actual == expected => {
                let now = SystemMarketCalendarClock
                    .now()
                    .map_err(|_| CompletedMarketSessionError::Unavailable)?;
                if now < as_of {
                    return Err(CompletedMarketSessionError::Unavailable);
                }
                let request = authority
                    .request_for(
                        expected.identity().venue_id(),
                        expected.identity().timeframe(),
                        now,
                        now,
                        now,
                    )?
                    .ok_or(CompletedMarketSessionError::Unavailable)?;
                match authority.resolve(request)? {
                    CompletedMarketSessionResolution::Available(latest)
                        if Some(latest.period().provider_timestamp())
                            == self.expected_provider_timestamp
                            && latest.currentness_receipt().identity() == expected.identity() =>
                    {
                        Ok(())
                    }
                    _ => Err(CompletedMarketSessionError::Unavailable),
                }
            }
            _ => Err(CompletedMarketSessionError::Unavailable),
        }
    }
}

/// Compares actual source-defined daily aggregation coordinates, never wall-clock age or the
/// history request's end. An independently captured calendar must cover the exact `as_of`.
pub(crate) fn qualify_history_current_session(
    history: &CompleteMarketBarHistoryOutput,
    completed: &CompletedMarketSessionAuthority,
    as_of: Timestamp,
) -> Result<HistoryCurrentSessionQualification, CompletedMarketSessionError> {
    let publication = history.selection().receipt();
    let (available_at, received_at, ingested_at) = publication.knowledge_clocks();
    if [
        available_at,
        received_at,
        ingested_at,
        publication.published_at(),
        publication.capture_recorded_at(),
    ]
    .into_iter()
    .any(|known_at| known_at > as_of)
    {
        return Err(CompletedMarketSessionError::InvalidRequest);
    }
    let last = history
        .bars()
        .last()
        .ok_or(CompletedMarketSessionError::InvalidEvidence)?;
    let actual = last
        .time_semantics()
        .timestamped_period()
        .ok_or(CompletedMarketSessionError::InvalidEvidence)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/history-current-completed-session/v1\0");
    digest.update(history.read_receipt().history_content_digest().bytes());
    digest.update(publication.receipt_digest().bytes());
    digest.update(as_of.unix_nanos().to_be_bytes());
    digest.update(actual.provider_timestamp().unix_nanos().to_be_bytes());
    digest.update(actual.period_start().unix_nanos().to_be_bytes());
    digest.update(actual.period_end_exclusive().unix_nanos().to_be_bytes());
    let request = completed.request_for(
        publication.venue_id(),
        publication.interval(),
        as_of,
        as_of,
        as_of,
    )?;
    let resolution = match request {
        Some(request) => completed.resolve(request)?,
        None => CompletedMarketSessionResolution::Unavailable(
            CompletedMarketSessionUnavailable::CurrentnessUnproven,
        ),
    };
    let (status, expected_provider_timestamp, currentness) = match resolution {
        CompletedMarketSessionResolution::Available(receipt) => {
            let expected = receipt.period();
            digest.update([1]);
            digest.update(receipt.digest().bytes());
            let covered = actual.provider_timestamp() == expected.provider_timestamp()
                && actual.period_start() == expected.period_start()
                && actual.period_end_exclusive() == expected.period_end_exclusive()
                && actual.timestamp_basis() == expected.timestamp_basis()
                && actual.session().kind() == expected.session().kind()
                && last.context().provenance().venue_id() == Some(publication.venue_id())
                && last.interval() == publication.interval()
                && actual.period_end_exclusive() <= as_of;
            digest.update([u8::from(covered)]);
            (
                if covered {
                    HistoryCurrentSessionStatus::Covered
                } else {
                    HistoryCurrentSessionStatus::MissingLatestCompletedPeriod
                },
                Some(expected.provider_timestamp()),
                Some(receipt.currentness_receipt().clone()),
            )
        }
        CompletedMarketSessionResolution::Unavailable(reason) => {
            digest.update([
                0,
                match reason {
                    CompletedMarketSessionUnavailable::NoCompletedPeriod => 1,
                    CompletedMarketSessionUnavailable::IncompleteRange => 2,
                    CompletedMarketSessionUnavailable::CurrentnessUnproven => 3,
                    CompletedMarketSessionUnavailable::Stale => 4,
                    CompletedMarketSessionUnavailable::Revoked => 5,
                    CompletedMarketSessionUnavailable::Conflict => 6,
                },
            ]);
            (
                HistoryCurrentSessionStatus::CalendarUnavailable(reason),
                None,
                None,
            )
        }
    };
    Ok(HistoryCurrentSessionQualification {
        status,
        as_of,
        expected_provider_timestamp,
        currentness,
        evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
        nominal: None,
    })
}
