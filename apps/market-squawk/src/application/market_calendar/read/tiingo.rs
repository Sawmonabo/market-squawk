//! One immutable Tiingo date request resolved by a genuinely retained listing-market calendar.

use super::tiingo_calendar_relation::relationship_evidence;
use super::{CompletedMarketSessionRead, check, encode_digest};
use crate::application::market_calendar::{
    CompletedMarketSessionError, MarketCalendarClock as _, SystemMarketCalendarClock,
};
use market_squawk_adapter_tiingo::{
    TiingoEodExpectedSessionAuthority, TiingoEodExpectedSessionEvidence,
    TiingoEodExpectedSessionRequest, TiingoEodExpectedSessionValidationReceipt,
    TiingoEodInstrumentAuthority, TiingoEodMapError, TiingoHistoryPlan,
};
use market_squawk_data::{CatalogAuthority, IngestError, IngestPrecommitAuthority};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use sha2::{Digest as _, Sha256};
use std::{fmt, sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

/// An exact operation receipt backed by the original calendar read. No request registry,
/// synthetic session vector, daily aggregation rule or additional runtime is introduced.
pub(crate) struct TiingoCalendarExpectedSessionAuthority {
    calendar: CompletedMarketSessionRead,
    expected: TiingoEodExpectedSessionEvidence,
    knowledge_cutoff: Timestamp,
    original_evaluation: Option<Timestamp>,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl fmt::Debug for TiingoCalendarExpectedSessionAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TiingoCalendarExpectedSessionAuthority")
            .field("reference", self.calendar.reference())
            .field("request", &self.expected.request_identity())
            .field("knowledge_cutoff", &self.knowledge_cutoff)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl CompletedMarketSessionRead {
    /// Resolves one actual Tiingo plan and instrument against the exact selected native market.
    /// A source calendar proves dates and core sessions only, never Tiingo aggregation instants.
    pub(crate) fn tiingo_expected_session_authority(
        &self,
        plan: &TiingoHistoryPlan,
        instrument: &TiingoEodInstrumentAuthority,
        knowledge_cutoff: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Arc<TiingoCalendarExpectedSessionAuthority>, CompletedMarketSessionError> {
        self.tiingo_session_authority_at(
            plan,
            instrument,
            knowledge_cutoff,
            now()?,
            None,
            deadline,
            cancellation,
        )
    }

    /// Reproduces the exact originally retained calendar resolution. Present account authority
    /// remains mandatory at publication; this does not renew old calendar source currentness.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reopen_tiingo_expected_session_authority(
        &self,
        plan: &TiingoHistoryPlan,
        instrument: &TiingoEodInstrumentAuthority,
        original: &market_squawk_data::ProviderCaptureOriginalReceipt,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Arc<TiingoCalendarExpectedSessionAuthority>, CompletedMarketSessionError> {
        let context: crate::provider_activation::tiingo::TiingoHistoryOriginalContext =
            serde_json::from_slice(original.context())
                .map_err(|_| CompletedMarketSessionError::InvalidEvidence)?;
        if original.ordinal() != 0
            || original.published_binding().is_some()
            || context.session != original.session()
            || &context.calendar != self.reference()
        {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        let knowledge_cutoff = context.calendar_cutoff;
        let resolved_at = context.calendar_resolved_at;
        let expected_digest = context.calendar_expected_digest;
        let result = self.tiingo_session_authority_at(
            plan,
            instrument,
            knowledge_cutoff,
            resolved_at,
            Some(resolved_at),
            deadline,
            cancellation,
        )?;
        if result.expected.evidence_identity() != expected_digest {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn tiingo_session_authority_at(
        &self,
        plan: &TiingoHistoryPlan,
        instrument: &TiingoEodInstrumentAuthority,
        knowledge_cutoff: Timestamp,
        resolved_at: Timestamp,
        original_evaluation: Option<Timestamp>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Arc<TiingoCalendarExpectedSessionAuthority>, CompletedMarketSessionError> {
        check(deadline, &cancellation)?;
        let request = TiingoEodExpectedSessionRequest::new(plan, instrument);
        let (start, end) = self.requested_dates();
        let relation = relationship_evidence(
            self.venue_id(),
            request.venue_id(),
            (request.start_date(), request.end_date()),
        )
        .ok_or(CompletedMarketSessionError::Unavailable)?;
        if plan.ticker() != instrument.ticker()
            || request.start_date() < start
            || request.end_date() > end
            || request.start_date() > request.end_date()
        {
            return Err(CompletedMarketSessionError::Unavailable);
        }
        let current = self
            .source
            .require_native_currentness(knowledge_cutoff, resolved_at)?;
        let replay = self.native_session_replay();
        let first = replay
            .sessions()
            .partition_point(|day| day.date() < request.start_date());
        let until = replay
            .sessions()
            .partition_point(|day| day.date() <= request.end_date());
        let selected = replay
            .sessions()
            .get(first..until)
            .ok_or(CompletedMarketSessionError::InvalidEvidence)?;
        for day in selected {
            check(deadline, &cancellation)?;
            // A listing-market calendar cannot silently become an IEX candle-period source.
            if day.provider_timestamp().is_some()
                || day.period_start().is_some()
                || day.period_end_exclusive().is_some()
            {
                return Err(CompletedMarketSessionError::InvalidEvidence);
            }
        }
        let reference = self.reference();
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/tiingo-retained-calendar-resolution/v1\0");
        for digest in [
            request.request_identity(),
            relation.relationship_digest(),
            reference.origin_content_digest(),
            reference.capture_binding_digest(),
            replay.replay_digest(),
            current.identity().calendar_evidence(),
            current.evidence(),
        ] {
            hash.update(digest.bytes());
        }
        for date in [start, end, request.start_date(), request.end_date()] {
            hash_date(&mut hash, date);
        }
        hash.update(resolved_at.unix_nanos().to_be_bytes());
        hash.update(knowledge_cutoff.unix_nanos().to_be_bytes());
        hash.update(
            u32::try_from(selected.len())
                .map_err(|_| CompletedMarketSessionError::ResourceBoundExceeded)?
                .to_be_bytes(),
        );
        for day in selected {
            check(deadline, &cancellation)?;
            hash_date(&mut hash, day.date());
        }
        // The creating immutable manifest content commitment identifies this durable authority
        // generation. It is independent from a process-local provider activation generation.
        let generation_name = encode_digest(reference.origin_content_digest());
        let authority_generation = SourceIdentifier::try_from(generation_name.as_str())
            .map_err(|_| CompletedMarketSessionError::InvalidEvidence)?;
        let expected = TiingoEodExpectedSessionEvidence::try_new_with_sessions(
            &request,
            self.source.calendar_id().clone(),
            self.calendar_revision().clone(),
            authority_generation,
            self.available_at(),
            resolved_at,
            finish(hash),
            selected.len(),
            selected.iter().map(|day| {
                check(deadline, &cancellation).map_err(adapter_error)?;
                Ok(day.date())
            }),
            reference.origin_content_digest(),
            reference.capture_binding_digest(),
            relation,
        )
        .map_err(|_| CompletedMarketSessionError::InvalidEvidence)?;
        check(deadline, &cancellation)?;
        self.source
            .require_native_currentness(knowledge_cutoff, original_evaluation.unwrap_or(now()?))?;
        Ok(Arc::new(TiingoCalendarExpectedSessionAuthority {
            calendar: self.clone(),
            expected,
            knowledge_cutoff,
            original_evaluation,
            deadline,
            cancellation,
        }))
    }
}

impl TiingoCalendarExpectedSessionAuthority {
    /// The same original read is used for data-owned native-session attachment and exact reopen.
    pub(crate) const fn calendar_read(&self) -> &CompletedMarketSessionRead {
        &self.calendar
    }

    pub(crate) const fn expected_evidence(&self) -> &TiingoEodExpectedSessionEvidence {
        &self.expected
    }

    /// Must be acquired after source acquisition, before ingest takes the catalog lock. The
    /// caller retains this exact account/operation authority through the entire durable commit.
    /// Its borrowed hook replaces ordinary calendar validation under that catalog lock.
    pub(crate) async fn acquire_publication_authority(
        self: &Arc<Self>,
        expected: &TiingoEodExpectedSessionEvidence,
    ) -> Result<Arc<dyn IngestPrecommitAuthority>, CompletedMarketSessionError> {
        if expected != &self.expected {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        self.require_current()?;
        let account = self
            .calendar
            .source
            .acquire_publication_authority(self.deadline, &self.cancellation)
            .await?;
        let authority = TiingoCalendarPublicationAuthority {
            calendar: Arc::clone(self),
            account,
        };
        authority.calendar.require_retained_operation()?;
        Ok(Arc::new(authority))
    }

    fn require_current(&self) -> Result<EvidenceDigest, CompletedMarketSessionError> {
        check(self.deadline, &self.cancellation)?;
        let receipt = self.calendar.source.require_native_currentness(
            self.knowledge_cutoff,
            self.original_evaluation.unwrap_or(now()?),
        )?;
        check(self.deadline, &self.cancellation)?;
        Ok(receipt.evidence())
    }

    fn require_retained_operation(&self) -> Result<(), CompletedMarketSessionError> {
        check(self.deadline, &self.cancellation)?;
        self.calendar.source.require_retained_lifecycle(
            self.knowledge_cutoff,
            self.original_evaluation.unwrap_or(now()?),
        )?;
        check(self.deadline, &self.cancellation)
    }
}

impl TiingoEodExpectedSessionAuthority for TiingoCalendarExpectedSessionAuthority {
    fn resolve_expected_sessions(
        &self,
        request: &TiingoEodExpectedSessionRequest,
        emit: &mut dyn FnMut(CalendarDate) -> Result<(), TiingoEodMapError>,
    ) -> Result<TiingoEodExpectedSessionEvidence, TiingoEodMapError> {
        if request.request_identity() != self.expected.request_identity() {
            return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
        }
        self.require_current().map_err(adapter_error)?;
        let replay = self.calendar.native_session_replay();
        let first = replay
            .sessions()
            .partition_point(|day| day.date() < request.start_date());
        let until = replay
            .sessions()
            .partition_point(|day| day.date() <= request.end_date());
        let selected = replay
            .sessions()
            .get(first..until)
            .ok_or(TiingoEodMapError::InvalidExpectedSessionEvidence)?;
        let result = TiingoEodExpectedSessionEvidence::try_new_with_sessions(
            request,
            self.expected.calendar_id().clone(),
            self.expected.calendar_revision().clone(),
            self.expected.authority_generation().clone(),
            self.expected.calendar_available_at(),
            self.expected.resolved_at(),
            self.expected.resolution_receipt(),
            selected.len(),
            selected.iter().map(|day| {
                check(self.deadline, &self.cancellation).map_err(adapter_error)?;
                if day.provider_timestamp().is_some()
                    || day.period_start().is_some()
                    || day.period_end_exclusive().is_some()
                {
                    return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
                }
                emit(day.date())?;
                Ok(day.date())
            }),
            self.expected.origin_content_digest(),
            self.expected.capture_binding_digest(),
            self.expected.relationship().clone(),
        )?;
        if result != self.expected {
            return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
        }
        self.require_current().map_err(adapter_error)?;
        Ok(result)
    }

    fn validate_current(
        &self,
        evidence: &TiingoEodExpectedSessionEvidence,
    ) -> Result<TiingoEodExpectedSessionValidationReceipt, TiingoEodMapError> {
        if evidence != &self.expected {
            return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
        }
        let current = self.require_current().map_err(adapter_error)?;
        let validated_at = now().map_err(adapter_error)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/tiingo-retained-calendar-validation/v1\0");
        for digest in [
            evidence.evidence_identity(),
            evidence.origin_content_digest(),
            evidence.capture_binding_digest(),
            current,
        ] {
            hash.update(digest.bytes());
        }
        hash.update(validated_at.unix_nanos().to_be_bytes());
        let receipt = TiingoEodExpectedSessionValidationReceipt::try_new(
            evidence,
            evidence.authority_generation().clone(),
            validated_at,
            finish(hash),
        )?;
        self.require_current().map_err(adapter_error)?;
        Ok(receipt)
    }
}

struct TiingoCalendarPublicationAuthority {
    calendar: Arc<TiingoCalendarExpectedSessionAuthority>,
    account: Arc<dyn IngestPrecommitAuthority>,
}
impl fmt::Debug for TiingoCalendarPublicationAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TiingoCalendarPublicationAuthority")
            .field("calendar", &self.calendar)
            .finish_non_exhaustive()
    }
}
impl IngestPrecommitAuthority for TiingoCalendarPublicationAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.calendar
            .require_retained_operation()
            .map_err(ingest_error)?;
        self.account.validate_precommit()?;
        self.calendar
            .require_retained_operation()
            .map_err(ingest_error)
    }
    fn validate_catalog_precommit(&self, catalog: &CatalogAuthority) -> Result<(), IngestError> {
        self.calendar
            .require_retained_operation()
            .map_err(ingest_error)?;
        self.account.validate_catalog_precommit(catalog)?;
        self.calendar
            .require_retained_operation()
            .map_err(ingest_error)
    }
}

fn now() -> Result<Timestamp, CompletedMarketSessionError> {
    SystemMarketCalendarClock
        .now()
        .map_err(|_| CompletedMarketSessionError::InvalidEvidence)
}
fn hash_date(hash: &mut Sha256, date: CalendarDate) {
    hash.update(date.year().to_be_bytes());
    hash.update([date.month(), date.day()]);
}
fn finish(hash: Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}
const fn adapter_error(error: CompletedMarketSessionError) -> TiingoEodMapError {
    match error {
        CompletedMarketSessionError::ResourceBoundExceeded => TiingoEodMapError::Allocation,
        _ => TiingoEodMapError::InvalidExpectedSessionEvidence,
    }
}
const fn ingest_error(error: CompletedMarketSessionError) -> IngestError {
    match error {
        CompletedMarketSessionError::Cancelled => IngestError::Cancelled,
        CompletedMarketSessionError::DeadlineExceeded => IngestError::DeadlineExceeded,
        CompletedMarketSessionError::Unavailable => IngestError::PublicationAuthorityRevoked,
        _ => IngestError::ProviderCaptureRequired,
    }
}
