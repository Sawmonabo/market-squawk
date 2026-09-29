//! Original creating publication and source metadata joined to opaque physical calendar replay.

use super::CorporateActionSessionValues;
use crate::{
    AnalyticalDataService, DatasetManifestRef, GenerationOwnedProviderCaptureEvidence, IngestError,
};
use market_squawk_adapter_alpaca::{AlpacaCalendarMarket, AlpacaRetainedCalendarSessions};
use market_squawk_domain::{CalendarDate, EvidenceDigest, Timestamp, VenueId};
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

/// Source-admitted calendar dependency. No value constructor or deserializer is exposed.
#[derive(Debug)]
pub struct RetainedCorporateActionCalendar {
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    replay: Arc<AlpacaRetainedCalendarSessions>,
    venue: VenueId,
    knowledge_cutoff: Timestamp,
    available_at: Timestamp,
    evidence_digest: EvidenceDigest,
}
impl RetainedCorporateActionCalendar {
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
    /// Conservative original creating-publication and capture availability.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    /// Original source-native replay, borrowed only inside data for authenticated history joins.
    pub(crate) fn native_replay(&self) -> &AlpacaRetainedCalendarSessions {
        &self.replay
    }
    pub fn native_dates_in(
        &self,
        interval: (CalendarDate, CalendarDate),
    ) -> impl Iterator<Item = CalendarDate> + '_ {
        self.replay
            .sessions()
            .iter()
            .map(|session| session.date())
            .filter(move |date| *date >= interval.0 && *date <= interval.1)
    }
    pub fn native_session_bounds(
        &self,
        interval: (CalendarDate, CalendarDate),
    ) -> Option<(Timestamp, Timestamp)> {
        let mut sessions = self
            .replay
            .sessions()
            .iter()
            .filter(|session| session.date() >= interval.0 && session.date() <= interval.1);
        let first = sessions.next()?;
        let last = sessions.next_back().unwrap_or(first);
        Some((first.opens_at(), last.closes_at_exclusive()))
    }
    /// Exact reported dates only; no holiday rolling or synthetic payment instant.
    pub fn date_session_on(
        &self,
        date: CalendarDate,
        cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Option<CorporateActionSessionValues> {
        if cutoff != self.knowledge_cutoff || evaluated_at < cutoff || self.available_at > cutoff {
            return None;
        }
        let index = self
            .replay
            .sessions()
            .binary_search_by_key(&date, |session| session.date())
            .ok()?;
        let session = &self.replay.sessions()[index];
        Some(CorporateActionSessionValues {
            date,
            opens_at: session.opens_at(),
            closes_at_exclusive: session.closes_at_exclusive(),
            available_at: self.available_at,
            receipt_digest: self.evidence_digest,
        })
    }
}
impl AnalyticalDataService {
    /// Joins genuine data-owned generation evidence to genuine physical replay. Metadata is read
    /// from the exact retained revision; neither native body hash nor session mapping hash can
    /// substitute for that original source contract.
    pub fn rejoin_corporate_action_calendar(
        &self,
        owned: &GenerationOwnedProviderCaptureEvidence,
        replay: Arc<AlpacaRetainedCalendarSessions>,
        knowledge_cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<RetainedCorporateActionCalendar, IngestError> {
        let invalid = || IngestError::ProviderCaptureRequired;
        let [object] = owned.objects() else {
            return Err(invalid());
        };
        let [input] = object.inputs() else {
            return Err(invalid());
        };
        let binding = input.binding();
        let capture = binding.capture();
        if owned.published_at() > knowledge_cutoff
            || owned.source_id() != capture.source_id()
            || binding.native_lineage().implementation() != "alpaca_calendar_v1"
            || binding.sealed_capture_receipt_digest() != replay.capture_receipt_digest()
            || capture.pages().len() != 1
            || replay.calendar_page_ordinal() != 0
            || replay.component().is_some()
            || capture.request_set_identity() != replay.request_identity()
            || capture.pages()[0].received_at() != replay.received_at()
            || replay.received_at() > owned.published_at()
        {
            return Err(invalid());
        }
        let metadata = self
            .retained_source_metadata(
                capture.source_id(),
                capture.metadata_revision(),
                knowledge_cutoff,
                deadline,
                cancellation,
            )?
            .ok_or_else(invalid)?;
        if metadata.authorization().effective_interval().starts_at() > replay.received_at()
            || metadata
                .authorization()
                .effective_interval()
                .ends_at()
                .is_some_and(|end| replay.received_at() >= end)
            || metadata.coverage().effective_interval().starts_at() > replay.received_at()
            || metadata
                .coverage()
                .effective_interval()
                .ends_at()
                .is_some_and(|end| replay.received_at() >= end)
        {
            return Err(invalid());
        }
        let venue = VenueId::try_from(match replay.market() {
            AlpacaCalendarMarket::Iex => "iex",
            AlpacaCalendarMarket::Nyse => "XNYS",
            AlpacaCalendarMarket::Nasdaq => "XNAS",
        })
        .map_err(|_| invalid())?;
        let available_at = owned.published_at().max(replay.received_at());
        let evidence_digest = replay.completed_session_evidence_digest(
            metadata
                .revision_evidence()
                .payload_evidence()
                .content_digest(),
            Some(owned.published_at()),
        );
        Ok(RetainedCorporateActionCalendar {
            manifest: owned.pinned().manifest().clone(),
            binding_digest: binding.binding_digest(),
            replay,
            venue,
            knowledge_cutoff,
            available_at,
            evidence_digest,
        })
    }
}
