//! Completed daily periods and native trading dates from physically retained calendar responses.

use std::{sync::Arc, time::Instant};

use market_squawk_adapter_alpaca::{
    AlpacaAuthenticatedCalendarRequest, AlpacaCalendarMarket, AlpacaRetainedCalendarSessions,
};
use market_squawk_data::{IngestError, IngestPrecommitAuthority};
use market_squawk_domain::{
    BarTimeSemantics, BarTimestampBasis, CalendarDate, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, MarketBarSessionEvidence, MarketBarSessionKind,
    RevisionBoundPayloadEvidence, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
    SealedResearchJournalSegment,
};
use market_squawk_sources::{ProviderCaptureSetReceipt, SealedProviderCaptureSetReceipt};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{ALPACA_CALENDAR_ID, ALPACA_DAILY_TIMEFRAME};
use crate::application::market_calendar::{
    CompletedMarketSessionAuthority, CompletedMarketSessionCandidate,
    CompletedMarketSessionCandidateSnapshot, CompletedMarketSessionCurrentnessIdentity,
    CompletedMarketSessionCurrentnessReceipt, CompletedMarketSessionCurrentnessResolution,
    CompletedMarketSessionError, CompletedMarketSessionEvidenceAccessError,
    CompletedMarketSessionEvidenceAuthority, CompletedMarketSessionRequest,
};
use crate::application::market_runtime::AlpacaHistoricalRuntimeCapability;

const RULESET: &str = "alpaca-v3-iex-utc-completed-daily-v1";

/// Exact source-native trading date and regular-session endpoints. A nominal action date is
/// never converted to midnight or silently rolled to a nearby trading day.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AlpacaCalendarSessionDateReceipt {
    date: CalendarDate,
    opens_at: Timestamp,
    closes_at_exclusive: Timestamp,
    available_at: Timestamp,
    calendar_evidence: EvidenceDigest,
    provider_period: Option<BarTimeSemantics>,
}

impl AlpacaCalendarSessionDateReceipt {
    pub(crate) const fn date(&self) -> CalendarDate {
        self.date
    }
    pub(crate) const fn opens_at(&self) -> Timestamp {
        self.opens_at
    }
    pub(crate) const fn closes_at_exclusive(&self) -> Timestamp {
        self.closes_at_exclusive
    }
    pub(crate) const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    pub(crate) const fn calendar_evidence(&self) -> EvidenceDigest {
        self.calendar_evidence
    }
    pub(crate) const fn provider_period(&self) -> Option<&BarTimeSemantics> {
        self.provider_period.as_ref()
    }
}

#[derive(Debug)]
struct RetainedDay {
    period: Option<BarTimeSemantics>,
    session: AlpacaCalendarSessionDateReceipt,
}

/// One complete source range, physically rejoined to its exact request/body and guarded by the
/// currently admitted account generation. Raw bytes are dropped after bounded native parsing.
#[derive(Debug)]
pub(crate) struct AlpacaCompletedSessionEvidence {
    runtime: AlpacaHistoricalRuntimeCapability,
    market: AlpacaCalendarMarket,
    independently_published: bool,
    capture: SealedProviderCaptureSetReceipt,
    native_replay: Arc<AlpacaRetainedCalendarSessions>,
    calendar_revision: RevisionBoundPayloadEvidence,
    currentness: CompletedMarketSessionCurrentnessIdentity,
    complete_from: Timestamp,
    complete_until: Timestamp,
    available_at: Timestamp,
    latest_received_at: Timestamp,
    expires_at: Timestamp,
    completeness_evidence: EvidenceDigest,
    days: Box<[RetainedDay]>,
}

impl AlpacaCompletedSessionEvidence {
    /// Reconstructs solely from an original capture and a freshly verified physical segment.
    /// The segment must have been opened through the controlled journal reader (or just sealed)
    /// under the same operation deadline. No caller-authored session list or coverage end enters.
    pub(crate) fn try_from_retained_capture(
        runtime: AlpacaHistoricalRuntimeCapability,
        request: &AlpacaAuthenticatedCalendarRequest,
        capture: ProviderCaptureSetReceipt,
        segment: &SealedResearchJournalSegment,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, CompletedMarketSessionError> {
        Self::from_retained_capture(
            runtime,
            request,
            capture,
            segment,
            None,
            None,
            None,
            deadline,
            cancellation,
        )
    }

    /// Reconstructs an independently published calendar under its original commit clock.
    /// The caller acquires the existing account guard before scheduling this blocking replay;
    /// construction borrows it without retaining it in the returned evidence.
    pub(crate) fn try_from_published_calendar_capture(
        runtime: AlpacaHistoricalRuntimeCapability,
        request: &AlpacaAuthenticatedCalendarRequest,
        capture: ProviderCaptureSetReceipt,
        segment: &SealedResearchJournalSegment,
        published_at: Timestamp,
        retained_metadata: market_squawk_sources::SourceMetadata,
        currentness: &dyn IngestPrecommitAuthority,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, CompletedMarketSessionError> {
        Self::from_retained_capture(
            runtime,
            request,
            capture,
            segment,
            Some(published_at),
            Some(retained_metadata),
            Some(currentness),
            deadline,
            cancellation,
        )
    }

    fn from_retained_capture(
        runtime: AlpacaHistoricalRuntimeCapability,
        request: &AlpacaAuthenticatedCalendarRequest,
        capture: ProviderCaptureSetReceipt,
        segment: &SealedResearchJournalSegment,
        published_at: Option<Timestamp>,
        retained_metadata: Option<market_squawk_sources::SourceMetadata>,
        currentness: Option<&dyn IngestPrecommitAuthority>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, CompletedMarketSessionError> {
        ensure_current(&runtime, currentness, deadline, cancellation)?;
        let active_metadata = if published_at.is_some() {
            runtime.calendar_metadata()
        } else {
            runtime.historical_metadata()
        };
        let metadata = retained_metadata.as_ref().unwrap_or(active_metadata);
        if metadata.source_id() != active_metadata.source_id()
            || metadata.budget_policy().map(|policy| policy.scope())
                != active_metadata.budget_policy().map(|policy| policy.scope())
            || (published_at.is_some()
                && market_squawk_adapter_alpaca::validate_alpaca_calendar_metadata(
                    metadata,
                    runtime.historical_request_bounds(),
                    runtime.trading_api_environment(),
                )
                .is_err())
        {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        if (published_at.is_none() && request.market() != AlpacaCalendarMarket::Iex)
            || capture.source_id() != metadata.source_id()
            || capture.metadata_revision() != metadata.revision()
            || request.origin() != runtime.trading_api_environment().origin()
        {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        let sealed = SealedProviderCaptureSetReceipt::try_bind(capture, segment.receipt().clone())
            .map_err(|_| CompletedMarketSessionError::InvalidEvidence)?;
        let control = CompletedCalendarReplayControl {
            deadline,
            cancellation,
        };
        let replay =
            AlpacaRetainedCalendarSessions::try_replay(request, &sealed, segment, &control)
                .map_err(|error| match error {
                    market_squawk_adapter_alpaca::AlpacaCalendarDecodeError::Control(
                        ResearchObjectControlError::Cancelled,
                    ) => CompletedMarketSessionError::Cancelled,
                    market_squawk_adapter_alpaca::AlpacaCalendarDecodeError::Control(
                        ResearchObjectControlError::DeadlineExceeded,
                    ) => CompletedMarketSessionError::DeadlineExceeded,
                    _ => CompletedMarketSessionError::InvalidEvidence,
                })?;
        if !metadata.is_effective_at(replay.received_at()) {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        let calendar_page = sealed
            .capture()
            .pages()
            .get(usize::from(replay.calendar_page_ordinal()))
            .ok_or(CompletedMarketSessionError::InvalidEvidence)?;
        let calendar_revision = RevisionBoundPayloadEvidence::new(
            metadata.revision().clone(),
            ExactPayloadEvidence::from_content_digest(calendar_page.body_digest()),
        );
        let available_at = published_at.map_or(replay.received_at(), |published| {
            published.max(replay.received_at())
        });
        let latest_received_at = sealed
            .capture()
            .pages()
            .iter()
            .map(|page| page.received_at())
            .max()
            .ok_or(CompletedMarketSessionError::InvalidEvidence)?;
        let complete_from = replay.complete_from();
        let complete_until = replay.complete_until();

        // Source authorization bounds this capability's lifecycle. The five-second quote-age
        // policy is not a calendar refresh rule. Exact captured date coverage is checked on
        // every selection, including the final check at the actual clock.
        let expires_at = [
            active_metadata
                .authorization()
                .effective_interval()
                .ends_at(),
            active_metadata.coverage().effective_interval().ends_at(),
        ]
        .into_iter()
        .flatten()
        .min()
        .filter(|expires_at| *expires_at > latest_received_at)
        .ok_or_else(|| {
            tracing::warn!(
                stage = "calendar-native-replay-expiry",
                failure = "missing-or-not-after-capture",
                "completed market calendar replay unavailable"
            );
            CompletedMarketSessionError::Unavailable
        })?;
        let calendar_evidence = replay.completed_session_evidence_digest(
            metadata
                .revision_evidence()
                .payload_evidence()
                .content_digest(),
            published_at,
        );
        let session_evidence = if request.market() == AlpacaCalendarMarket::Iex {
            Some(
                MarketBarSessionEvidence::try_new(
                    MarketBarSessionKind::ProviderDefined,
                    identifier(RULESET)?,
                    calendar_evidence,
                )
                .map_err(|_| CompletedMarketSessionError::InvalidEvidence)?,
            )
        } else {
            None
        };
        let mut days = Vec::new();
        days.try_reserve_exact(replay.sessions().len())
            .map_err(|_| CompletedMarketSessionError::ResourceBoundExceeded)?;
        for day in replay.sessions() {
            ensure_current(&runtime, currentness, deadline, cancellation)?;
            let period = match (
                day.period_start(),
                day.period_end_exclusive(),
                &session_evidence,
            ) {
                (Some(start), Some(end), Some(session)) => Some(
                    BarTimeSemantics::try_new(
                        start,
                        end,
                        BarTimestampBasis::PeriodStart,
                        session.clone(),
                    )
                    .map_err(|_| CompletedMarketSessionError::InvalidEvidence)?,
                ),
                (None, None, None) => None,
                _ => return Err(CompletedMarketSessionError::InvalidEvidence),
            };
            days.push(RetainedDay {
                period: period.clone(),
                session: AlpacaCalendarSessionDateReceipt {
                    date: day.date(),
                    opens_at: day.opens_at(),
                    closes_at_exclusive: day.closes_at_exclusive(),
                    available_at,
                    calendar_evidence,
                    provider_period: period,
                },
            });
        }
        let currentness_identity = CompletedMarketSessionCurrentnessIdentity::try_new(
            metadata.source_id().clone(),
            metadata.revision().clone(),
            VenueId::try_from(request.market().venue())
                .map_err(|_| CompletedMarketSessionError::InvalidEvidence)?,
            identifier(if request.market() == AlpacaCalendarMarket::Iex {
                ALPACA_DAILY_TIMEFRAME
            } else {
                "native-market-session"
            })?,
            sealed.capture().dataset().clone(),
            identifier(if request.market() == AlpacaCalendarMarket::Iex {
                ALPACA_CALENDAR_ID
            } else {
                "alpaca-v3-listed-market-native-calendar"
            })?,
            identifier(if request.market() == AlpacaCalendarMarket::Iex {
                RULESET
            } else {
                "alpaca-v3-listed-market-native-session-v1"
            })?,
            calendar_evidence,
            runtime.group_generation().digest(),
            runtime.runtime_evidence_digest(),
        )?;
        ensure_current(&runtime, currentness, deadline, cancellation)?;
        Ok(Self {
            runtime,
            market: request.market(),
            independently_published: published_at.is_some(),
            capture: sealed,
            native_replay: Arc::new(replay),
            calendar_revision,
            currentness: currentness_identity,
            complete_from,
            complete_until,
            available_at,
            latest_received_at,
            expires_at,
            completeness_evidence: calendar_evidence,
            days: days.into_boxed_slice(),
        })
    }

    pub(crate) fn venue_id(&self) -> &VenueId {
        self.currentness.venue_id()
    }

    pub(crate) const fn native_session_replay(&self) -> &Arc<AlpacaRetainedCalendarSessions> {
        &self.native_replay
    }

    pub(crate) const fn calendar_revision(&self) -> &RevisionBoundPayloadEvidence {
        &self.calendar_revision
    }

    pub(crate) fn calendar_id(&self) -> &SourceIdentifier {
        self.currentness.calendar_id()
    }

    pub(crate) const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    pub(crate) const fn currentness_expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Ordinary runtime validation; never call while a catalog or publication guard is held.
    pub(crate) fn require_native_currentness(
        &self,
        knowledge_cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Result<CompletedMarketSessionCurrentnessReceipt, CompletedMarketSessionError> {
        self.require_retained_lifecycle(knowledge_cutoff, evaluated_at)?;
        match self.validate_currentness(&self.currentness, evaluated_at) {
            CompletedMarketSessionCurrentnessResolution::Current(receipt) => Ok(receipt),
            CompletedMarketSessionCurrentnessResolution::Conflict => {
                Err(CompletedMarketSessionError::InvalidEvidence)
            }
            _ => Err(CompletedMarketSessionError::Unavailable),
        }
    }

    /// Checks only immutable retained bounds and the runtime's revocation flag. Publication
    /// callers must also hold and validate the existing account publication authority.
    pub(crate) fn require_retained_lifecycle(
        &self,
        knowledge_cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Result<(), CompletedMarketSessionError> {
        if knowledge_cutoff > evaluated_at {
            return Err(CompletedMarketSessionError::InvalidRequest);
        }
        if self.available_at > knowledge_cutoff
            || evaluated_at >= self.expires_at
            || self.runtime.is_revoked()
            || !self.active_metadata().is_effective_at(evaluated_at)
        {
            return Err(CompletedMarketSessionError::Unavailable);
        }
        Ok(())
    }

    /// Acquires the existing account operation/activation guard after source work completes.
    pub(crate) async fn acquire_publication_authority(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Arc<dyn market_squawk_data::IngestPrecommitAuthority>, CompletedMarketSessionError>
    {
        self.runtime
            .acquire_calendar_publication_authority(deadline, cancellation)
            .await
            .map_err(|error| match error {
                crate::application::market_runtime::AlpacaHistoricalCapabilityError::Cancelled => {
                    CompletedMarketSessionError::Cancelled
                }
                crate::application::market_runtime::AlpacaHistoricalCapabilityError::DeadlineExceeded => {
                    CompletedMarketSessionError::DeadlineExceeded
                }
                _ => CompletedMarketSessionError::Unavailable,
            })
    }

    fn active_metadata(&self) -> &market_squawk_sources::SourceMetadata {
        if self.independently_published {
            self.runtime.calendar_metadata()
        } else {
            self.runtime.historical_metadata()
        }
    }

    pub(crate) fn into_authority(self) -> CompletedMarketSessionAuthority {
        CompletedMarketSessionAuthority::new(Arc::new(self))
    }

    /// Proves the latest completed regular session within the original complete source range.
    /// The completion clock is independent of the knowledge cutoff so a final recheck can
    /// detect a session that completed while calculation was in flight. No price is inferred.
    pub(crate) fn latest_completed_regular_session(
        &self,
        knowledge_cutoff: Timestamp,
        completion_cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Result<
        (
            Option<AlpacaCalendarSessionDateReceipt>,
            CompletedMarketSessionCurrentnessReceipt,
        ),
        CompletedMarketSessionError,
    > {
        if completion_cutoff > evaluated_at {
            return Err(CompletedMarketSessionError::InvalidRequest);
        }
        self.require_native_currentness(knowledge_cutoff, evaluated_at)?;
        // The retained request's civil-day boundary is coverage, never a bar timestamp.
        // An exhausted range cannot prove that no later session has completed.
        if completion_cutoff < self.complete_from
            || completion_cutoff >= self.complete_until
            || self.latest_received_at > knowledge_cutoff
        {
            return Err(CompletedMarketSessionError::Unavailable);
        }
        let completed = self
            .days
            .iter()
            .filter(|day| day.session.closes_at_exclusive <= completion_cutoff)
            .max_by_key(|day| day.session.closes_at_exclusive)
            .map(|day| day.session.clone());
        let currentness = self.require_native_currentness(knowledge_cutoff, evaluated_at)?;
        Ok((completed, currentness))
    }

    /// Finds the first reported regular session opening at or after the requested instant.
    /// The instant must itself lie inside complete source coverage; a partial range cannot
    /// establish that an earlier eligible session was absent.
    pub(crate) fn next_session_starting_at_or_after(
        &self,
        eligible_at: Timestamp,
        knowledge_cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Option<AlpacaCalendarSessionDateReceipt> {
        if eligible_at < self.complete_from || eligible_at >= self.complete_until {
            return None;
        }
        let index = self
            .days
            .partition_point(|day| day.session.opens_at < eligible_at);
        let date = self.days.get(index)?.session.date;
        self.session_on(date, knowledge_cutoff, evaluated_at)
    }

    /// Resolves precisely the source date under the same currentness and PIT checks. Weekends,
    /// holidays, absent dates, and dates learned after the cutoff remain unavailable.
    pub(crate) fn session_on(
        &self,
        date: CalendarDate,
        knowledge_cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Option<AlpacaCalendarSessionDateReceipt> {
        if knowledge_cutoff > evaluated_at
            || self.available_at > knowledge_cutoff
            || !matches!(
                self.validate_currentness(&self.currentness, evaluated_at),
                CompletedMarketSessionCurrentnessResolution::Current(_)
            )
        {
            return None;
        }
        let index = self
            .days
            .binary_search_by_key(&date, |day| day.session.date)
            .ok()?;
        let receipt = self.days.get(index)?.session.clone();
        matches!(
            self.validate_currentness(&self.currentness, evaluated_at),
            CompletedMarketSessionCurrentnessResolution::Current(_)
        )
        .then_some(receipt)
    }
}

impl CompletedMarketSessionEvidenceAuthority for AlpacaCompletedSessionEvidence {
    fn evidence_series(
        &self,
        venue: &VenueId,
        timeframe: &SourceIdentifier,
    ) -> Option<SourceIdentifier> {
        (self.market == AlpacaCalendarMarket::Iex
            && venue == self.currentness.venue_id()
            && timeframe == self.currentness.timeframe())
        .then(|| self.currentness.evidence_series().clone())
    }

    fn candidate_snapshot(
        &self,
        request: &CompletedMarketSessionRequest,
    ) -> Result<CompletedMarketSessionCandidateSnapshot, CompletedMarketSessionEvidenceAccessError>
    {
        if self.market != AlpacaCalendarMarket::Iex
            || request.venue_id() != self.currentness.venue_id()
            || request.timeframe() != self.currentness.timeframe()
            || request.evidence_series() != self.currentness.evidence_series()
            || request.completion_cutoff() < self.complete_from
            || request.completion_cutoff() > self.complete_until
        {
            return Err(CompletedMarketSessionEvidenceAccessError::Unavailable);
        }
        let count = self.days.partition_point(|day| {
            day.period
                .as_ref()
                .and_then(BarTimeSemantics::period_end_exclusive)
                .is_some_and(|end| end <= request.completion_cutoff())
        });
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(count)
            .map_err(|_| CompletedMarketSessionEvidenceAccessError::Unavailable)?;
        let effective = EffectiveInterval::new(self.available_at, Some(self.expires_at))
            .map_err(|_| CompletedMarketSessionEvidenceAccessError::Conflict)?;
        for day in &self.days[..count] {
            let period = day
                .period
                .as_ref()
                .ok_or(CompletedMarketSessionEvidenceAccessError::Conflict)?;
            let knowledge = period
                .period_end_exclusive()
                .ok_or(CompletedMarketSessionEvidenceAccessError::Conflict)?
                .max(self.available_at)
                .max(self.latest_received_at);
            // The snapshot must not hide a newer completed period by falling back to an older
            // one whose local availability happened to pass the request's PIT cutoff.
            if knowledge > request.knowledge_cutoff() || knowledge >= self.expires_at {
                return Err(CompletedMarketSessionEvidenceAccessError::Unavailable);
            }
            candidates.push(
                CompletedMarketSessionCandidate::try_new(
                    period.clone(),
                    effective,
                    self.available_at,
                    knowledge,
                    self.expires_at,
                    &self.capture,
                )
                .map_err(|_| CompletedMarketSessionEvidenceAccessError::Conflict)?,
            );
        }
        CompletedMarketSessionCandidateSnapshot::try_new(
            request,
            self.currentness.clone(),
            self.complete_from,
            request.completion_cutoff(),
            self.completeness_evidence,
            self.capture.clone(),
            candidates,
        )
        .map_err(|_| CompletedMarketSessionEvidenceAccessError::Conflict)
    }

    fn validate_currentness(
        &self,
        identity: &CompletedMarketSessionCurrentnessIdentity,
        evaluated_at: Timestamp,
    ) -> CompletedMarketSessionCurrentnessResolution {
        use CompletedMarketSessionCurrentnessResolution as Resolution;
        if identity != &self.currentness {
            return Resolution::Conflict;
        }
        if self.runtime.is_revoked() {
            return Resolution::Revoked;
        }
        if self.runtime.validate_current_now().is_err() {
            return Resolution::Stale;
        }
        if evaluated_at < self.available_at
            || evaluated_at >= self.expires_at
            || !self.active_metadata().is_effective_at(evaluated_at)
        {
            return Resolution::Stale;
        }
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/alpaca-completed-calendar-currentness/v1\0");
        digest.update(self.capture.receipt_digest().bytes());
        digest.update(identity.source_generation().bytes());
        digest.update(identity.revocation_identity().bytes());
        digest.update(
            self.active_metadata()
                .revision_evidence()
                .payload_evidence()
                .content_digest()
                .bytes(),
        );
        digest.update(evaluated_at.unix_nanos().to_be_bytes());
        digest.update(self.expires_at.unix_nanos().to_be_bytes());
        match CompletedMarketSessionCurrentnessReceipt::try_new(
            identity.clone(),
            evaluated_at,
            self.expires_at,
            EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
        ) {
            Ok(receipt) if self.runtime.validate_current_now().is_ok() => {
                Resolution::Current(receipt)
            }
            _ => Resolution::Stale,
        }
    }
}

fn ensure_current(
    runtime: &AlpacaHistoricalRuntimeCapability,
    currentness: Option<&dyn IngestPrecommitAuthority>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CompletedMarketSessionError> {
    if cancellation.is_cancelled() {
        return Err(CompletedMarketSessionError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(CompletedMarketSessionError::DeadlineExceeded);
    }
    if let Some(currentness) = currentness {
        // Reuse the exact held account guard: a queued writer can block a new read while
        // this read remains valid. Waiting here would invert activation-before-worker ordering.
        return currentness.validate_precommit().map_err(|error| {
            let error = match error {
                IngestError::Cancelled => CompletedMarketSessionError::Cancelled,
                IngestError::DeadlineExceeded => CompletedMarketSessionError::DeadlineExceeded,
                _ => CompletedMarketSessionError::Unavailable,
            };
            tracing::warn!(
                ?error,
                stage = "calendar-native-replay-held-currentness",
                "completed market calendar replay unavailable"
            );
            error
        });
    }
    runtime
        .validate_current_now()
        .inspect_err(|error| {
            tracing::warn!(
                ?error,
                stage = "calendar-native-replay-currentness",
                "completed market calendar replay unavailable"
            );
        })
        .map_err(|_| CompletedMarketSessionError::Unavailable)
}

fn identifier(value: &str) -> Result<SourceIdentifier, CompletedMarketSessionError> {
    SourceIdentifier::try_from(value).map_err(|_| CompletedMarketSessionError::InvalidEvidence)
}

// Canonical publication reuses the exact same bounded wire parser as sealed history replay.
pub(super) use market_squawk_adapter_alpaca::calendar_decode::CalendarRangeWire;

struct CompletedCalendarReplayControl<'a> {
    deadline: Instant,
    cancellation: &'a CancellationToken,
}
impl ResearchObjectControl for CompletedCalendarReplayControl<'_> {
    fn checkpoint(&self, _: ResearchObjectControlPoint) -> Result<(), ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            Err(ResearchObjectControlError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ResearchObjectControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}
