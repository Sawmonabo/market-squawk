//! Complete native daily-date requests, with original normalization evidence.
//!
//! Dates are source labels. This graph never creates aggregation instants from a calendar.

use super::*;
use market_squawk_domain::{
    BarTimeSemantics, CalendarDate, Currency, ExactPayloadEvidence, RevisionBoundPayloadEvidence,
};
use std::num::NonZeroU64;

/// Provider-native exchange label, distinct from a canonical venue or identifier grammar.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize)]
#[serde(transparent)]
pub struct ProviderNativeExchangeCode(Box<str>);
impl ProviderNativeExchangeCode {
    pub const MAX_LENGTH: usize = 128;
    pub fn as_str(&self) -> &str { &self.0 }
}
impl TryFrom<&str> for ProviderNativeExchangeCode {
    type Error = ProviderCaptureError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.is_empty() || value.len() > Self::MAX_LENGTH || value.trim() != value
            || value.chars().any(char::is_control)
        { return Err(ProviderCaptureError::InvalidMarketBarHistorySemantics); }
        Ok(Self(value.into()))
    }
}
impl TryFrom<String> for ProviderNativeExchangeCode {
    type Error = ProviderCaptureError;
    fn try_from(value: String) -> Result<Self, Self::Error> { Self::try_from(value.as_str()) }
}
impl<'de> Deserialize<'de> for ProviderNativeExchangeCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = ProviderNativeExchangeCode;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an exact bounded provider exchange label")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                ProviderNativeExchangeCode::try_from(value).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

/// Original provider metadata coverage, independent of requested date-window completeness.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetainedMarketHistoryNativeCoverageV1 {
    Supported { start: CalendarDate, end: CalendarDate },
    Unsupported,
}

/// How the original monetary unit was established. Reviewed interpretation remains distinct
/// from an explicit source assertion after persistence and replay. Neither status grants authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketHistoryCashUnitStatus {
    SourceAttested,
    ReviewedInference,
}

/// Original monetary unit assertion and its explicit provenance classification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMarketHistoryCashUnitV1 {
    pub status: MarketHistoryCashUnitStatus,
    pub currency: Currency,
    pub assertion: RevisionBoundPayloadEvidence,
    pub available_at: Timestamp,
}

/// Original opaque calendar selection and its terminal validation, separate from bar precision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMarketHistoryCalendarV1 {
    pub request_identity: EvidenceDigest,
    pub origin_content_digest: EvidenceDigest,
    pub capture_binding_digest: EvidenceDigest,
    pub relationship: super::ReviewedMarketCalendarRelationship,
    pub calendar_id: SourceIdentifier,
    pub calendar_revision: RevisionBoundPayloadEvidence,
    pub authority_generation: SourceIdentifier,
    pub calendar_available_at: Timestamp,
    pub resolved_at: Timestamp,
    pub resolution_receipt: EvidenceDigest,
    pub evidence_identity: EvidenceDigest,
    pub validated_at: Timestamp,
    pub authority_receipt: EvidenceDigest,
    pub validation_identity: EvidenceDigest,
}

/// Bounded source mapping and decoding coordinates needed to reopen the original raw bytes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMarketHistoryNormalizationV1 {
    pub instrument_definition: RevisionBoundPayloadEvidence,
    pub provider_mapping_evidence: ExactPayloadEvidence,
    pub provider_exchange_code: ProviderNativeExchangeCode,
    pub is_exchange_traded_fund: bool,
    pub resolved_at: Timestamp,
    pub currency: Currency,
    pub source_contract_revision: MetadataRevision,
    pub source_contract_evidence: ExactPayloadEvidence,
    pub native_schema_revision: SourceIdentifier,
    pub native_schema_evidence: ExactPayloadEvidence,
    pub entitlement_generation: SourceIdentifier,
    pub entitlement_generation_number: NonZeroU64,
    pub entitlement_evidence: EvidenceDigest,
    pub adjusted_surface_evidence: ExactPayloadEvidence,
    pub contract_identity: EvidenceDigest,
    pub metadata_decoded_at: Timestamp,
    pub native_coverage: RetainedMarketHistoryNativeCoverageV1,
    pub cash_unit: Option<RetainedMarketHistoryCashUnitV1>,
    pub calendar: RetainedMarketHistoryCalendarV1,
}

/// Exact one-response application window, including empty windows and original local clocks.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteMarketBarDateWindowV1 {
    pub component_ordinal: u16,
    pub request_identity: EvidenceDigest,
    pub start_date: CalendarDate,
    pub end_date: CalendarDate,
    pub first_session_ordinal: u32,
    pub returned_session_count: u32,
    pub decoded_at: Timestamp,
    pub ingested_at: Timestamp,
}

/// One original native row date. Its time semantics retain nominal precision only.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteMarketBarDateSessionV1 {
    pub date: CalendarDate,
    pub time: BarTimeSemantics,
    pub row_digest: EvidenceDigest,
}

/// Constructor input. The bounded deserializer below enforces limits before collecting rows.
#[derive(Debug)]
pub struct CompleteMarketBarDateWindowsInputV1 {
    pub requested_start: CalendarDate,
    pub requested_end: CalendarDate,
    pub instrument_id: InstrumentId,
    pub instrument_revision_digest: EvidenceDigest,
    pub admitted_plan_digest: EvidenceDigest,
    pub provider_instrument_id: ProviderInstrumentId,
    pub venue_id: VenueId,
    pub interval: SourceIdentifier,
    pub graph_purpose: SourceIdentifier,
    pub windows: Vec<CompleteMarketBarDateWindowV1>,
    pub sessions: Vec<CompleteMarketBarDateSessionV1>,
    pub completeness_evidence: EvidenceDigest,
    pub normalization: RetainedMarketHistoryNormalizationV1,
}

/// Exact complete nominal financial-date graph; metadata is component zero, windows follow.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct CompleteMarketBarDateWindowsV1 {
    wire: CompleteMarketBarDateWindowsWire,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CompleteMarketBarDateWindowsWire {
    requested_start: CalendarDate,
    requested_end: CalendarDate,
    instrument_id: InstrumentId,
    instrument_revision_digest: EvidenceDigest,
    admitted_plan_digest: EvidenceDigest,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    interval: SourceIdentifier,
    graph_purpose: SourceIdentifier,
    windows: BoundedVec<CompleteMarketBarDateWindowV1, MAX_PROVIDER_CAPTURE_PAGES>,
    sessions: BoundedVec<CompleteMarketBarDateSessionV1, MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS>,
    completeness_evidence: EvidenceDigest,
    normalization: RetainedMarketHistoryNormalizationV1,
}

impl CompleteMarketBarDateWindowsV1 {
    pub fn try_new(input: CompleteMarketBarDateWindowsInputV1) -> Result<Self, ProviderCaptureError> {
        let invalid = || ProviderCaptureError::InvalidMarketBarHistorySemantics;
        let wire = CompleteMarketBarDateWindowsWire {
            requested_start: input.requested_start,
            requested_end: input.requested_end,
            instrument_id: input.instrument_id,
            instrument_revision_digest: input.instrument_revision_digest,
            admitted_plan_digest: input.admitted_plan_digest,
            provider_instrument_id: input.provider_instrument_id,
            venue_id: input.venue_id,
            interval: input.interval,
            graph_purpose: input.graph_purpose,
            windows: BoundedVec::try_new(input.windows).map_err(|_| invalid())?,
            sessions: BoundedVec::try_new(input.sessions).map_err(|_| invalid())?,
            completeness_evidence: input.completeness_evidence,
            normalization: input.normalization,
        };
        Self::validate(wire)
    }

    fn validate(wire: CompleteMarketBarDateWindowsWire) -> Result<Self, ProviderCaptureError> {
        let invalid = || ProviderCaptureError::InvalidMarketBarHistorySemantics;
        let normalization = &wire.normalization;
        let calendar = &normalization.calendar;
        if wire.requested_start > wire.requested_end
            || wire.windows.is_empty()
            || wire.windows.len() >= MAX_PROVIDER_CAPTURE_PAGES
            || wire.sessions.is_empty()
            || normalization.instrument_definition.payload_evidence().content_digest()
                != wire.instrument_revision_digest
            || calendar.calendar_available_at > calendar.resolved_at
            || calendar.resolved_at > calendar.validated_at
            || calendar.relationship.target_venue() != &wire.venue_id
            || calendar.relationship.requested_dates() != (wire.requested_start, wire.requested_end)
            || matches!(normalization.native_coverage,
                RetainedMarketHistoryNativeCoverageV1::Supported { start, end } if start > end)
        {
            return Err(invalid());
        }
        for digest in [
            wire.instrument_revision_digest, wire.admitted_plan_digest, wire.completeness_evidence,
            normalization.provider_mapping_evidence.content_digest(),
            normalization.source_contract_evidence.content_digest(),
            normalization.native_schema_evidence.content_digest(),
            normalization.entitlement_evidence,
            normalization.adjusted_surface_evidence.content_digest(),
            normalization.contract_identity, calendar.request_identity,
            calendar.calendar_revision.payload_evidence().content_digest(),
            calendar.resolution_receipt, calendar.evidence_identity,
            calendar.authority_receipt, calendar.validation_identity,
            calendar.origin_content_digest, calendar.capture_binding_digest,
            calendar.relationship.relationship_digest(),
        ] {
            require_sha256_identity(digest)?;
        }
        if let Some(unit) = &normalization.cash_unit {
            require_sha256_identity(unit.assertion.payload_evidence().content_digest())?;
        }
        let mut offset = 0_usize;
        let mut previous_date = None;
        let mut previous_end: Option<CalendarDate> = None;
        for (index, window) in wire.windows.as_slice().iter().enumerate() {
            require_sha256_identity(window.request_identity)?;
            if usize::from(window.component_ordinal) != index + 1
                || window.start_date > window.end_date
                || (index == 0 && window.start_date != wire.requested_start)
                || previous_end.is_some_and(|date| date.days_since_unix_epoch().checked_add(1) != Some(window.start_date.days_since_unix_epoch()))
                || usize::try_from(window.first_session_ordinal).ok() != Some(offset)
                || window.decoded_at > window.ingested_at
                || normalization.metadata_decoded_at > window.decoded_at
                || normalization.resolved_at > window.ingested_at
                || normalization.cash_unit.as_ref().is_some_and(|unit| unit.available_at > window.ingested_at)
            {
                return Err(invalid());
            }
            let end = offset.checked_add(window.returned_session_count as usize).ok_or_else(invalid)?;
            for session in wire.sessions.as_slice().get(offset..end).ok_or_else(invalid)? {
                require_sha256_identity(session.row_digest)?;
                let nominal = session.time.nominal_daily_date().ok_or_else(invalid)?;
                if nominal.date() != session.date
                    || nominal.evidence().content_digest() != session.row_digest
                    || session.date < window.start_date || session.date > window.end_date
                    || previous_date.is_some_and(|date| date >= session.date)
                {
                    return Err(invalid());
                }
                previous_date = Some(session.date);
            }
            offset = end;
            previous_end = Some(window.end_date);
        }
        if offset != wire.sessions.len() || previous_end != Some(wire.requested_end) {
            return Err(invalid());
        }
        Ok(Self { wire })
    }

    pub const fn requested_dates(&self) -> (CalendarDate, CalendarDate) { (self.wire.requested_start, self.wire.requested_end) }
    pub const fn instrument_id(&self) -> InstrumentId { self.wire.instrument_id }
    pub const fn instrument_revision_digest(&self) -> EvidenceDigest { self.wire.instrument_revision_digest }
    pub const fn admitted_plan_digest(&self) -> EvidenceDigest { self.wire.admitted_plan_digest }
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId { &self.wire.provider_instrument_id }
    pub const fn venue_id(&self) -> &VenueId { &self.wire.venue_id }
    pub const fn interval(&self) -> &SourceIdentifier { &self.wire.interval }
    pub const fn graph_purpose(&self) -> &SourceIdentifier { &self.wire.graph_purpose }
    pub fn windows(&self) -> &[CompleteMarketBarDateWindowV1] { self.wire.windows.as_slice() }
    pub fn sessions(&self) -> &[CompleteMarketBarDateSessionV1] { self.wire.sessions.as_slice() }
    pub const fn completeness_evidence(&self) -> EvidenceDigest { self.wire.completeness_evidence }
    pub const fn normalization(&self) -> &RetainedMarketHistoryNormalizationV1 { &self.wire.normalization }
    pub const fn calendar(&self) -> &RetainedMarketHistoryCalendarV1 { &self.wire.normalization.calendar }

    pub(super) fn validate_capture_pages(&self, pages: &[ProviderCapturePageReceipt]) -> Result<(), ProviderCaptureError> {
        let invalid = || ProviderCaptureError::InvalidMarketBarHistorySemantics;
        if pages.len() != self.windows().len() + 1 { return Err(invalid()); }
        let mut previous_decoded = self.normalization().metadata_decoded_at;
        if pages[0].received_at() > previous_decoded { return Err(invalid()); }
        for (page, window) in pages[1..].iter().zip(self.windows()) {
            if page.received_at() < previous_decoded || page.received_at() > window.decoded_at
                || page.request_identity() != window.request_identity
            { return Err(invalid()); }
            previous_decoded = window.ingested_at;
        }
        Ok(())
    }

    pub(super) fn hash_into(&self, digest: &mut Sha256) {
        // Serialization contains only closed non-fallible scalar/sequence values. Hash through a
        // writer to avoid a second complete graph allocation. The writer cannot fail.
        struct HashWriter<'a>(&'a mut Sha256);
        impl std::io::Write for HashWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> { self.0.update(bytes); Ok(bytes.len()) }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let result = serde_json::to_writer(HashWriter(digest), &self.wire);
        debug_assert!(result.is_ok(), "closed date-window serialization cannot fail");
    }
}

impl<'de> Deserialize<'de> for CompleteMarketBarDateWindowsV1 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::validate(CompleteMarketBarDateWindowsWire::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}
