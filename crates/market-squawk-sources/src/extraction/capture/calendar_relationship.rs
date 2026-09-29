//! Reviewed relationship between an original source calendar and a listing venue.
//!
//! This is bounded reconstruction evidence, not source, account or publication authority. A data
//! owner must rejoin the actual immutable native calendar and graph before consuming it.

use super::ProviderCaptureError;
use market_squawk_domain::{CalendarDate, DigestAlgorithm, EvidenceDigest, VenueId};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const REVIEWED_ARCA: &[u8] = include_bytes!("calendar_relationship.md");

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "RelationshipWire", into = "RelationshipWire")]
pub struct ReviewedMarketCalendarRelationship {
    native_venue: VenueId,
    target_venue: VenueId,
    start: CalendarDate,
    end: CalendarDate,
    interpretation_digest: EvidenceDigest,
    relationship_digest: EvidenceDigest,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RelationshipWire {
    native_venue: VenueId,
    target_venue: VenueId,
    start: CalendarDate,
    end: CalendarDate,
    interpretation_digest: EvidenceDigest,
    relationship_digest: EvidenceDigest,
}
impl ReviewedMarketCalendarRelationship {
    /// Selects only a reviewed relationship. This cannot establish that a source calendar was
    /// fetched, complete or known by any cutoff; controlled data readers check those separately.
    pub fn try_new(
        native_venue: VenueId,
        target_venue: VenueId,
        start: CalendarDate,
        end: CalendarDate,
    ) -> Result<Self, ProviderCaptureError> {
        let invalid = || ProviderCaptureError::InvalidMarketBarHistorySemantics;
        if start > end { return Err(invalid()); }
        let interpretation_digest = if native_venue == target_venue {
            digest(b"market-squawk/native-calendar-identical-venue/v1\0")
        } else if native_venue.as_str() == "XNYS" && target_venue.as_str() == "ARCX"
            && start.year() >= 2015 && end.year() <= 2027
        {
            digest(REVIEWED_ARCA)
        } else { return Err(invalid()); };
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/reviewed-native-calendar-relationship/v1\0");
        for venue in [&native_venue, &target_venue] {
            hash.update((venue.as_str().len() as u64).to_be_bytes());
            hash.update(venue.as_str().as_bytes());
        }
        for date in [start, end] {
            hash.update(date.year().to_be_bytes());
            hash.update([date.month(), date.day()]);
        }
        hash.update(interpretation_digest.bytes());
        let relationship_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
        Ok(Self { native_venue, target_venue, start, end, interpretation_digest, relationship_digest })
    }
    pub const fn native_venue(&self) -> &VenueId { &self.native_venue }
    pub const fn target_venue(&self) -> &VenueId { &self.target_venue }
    pub const fn requested_dates(&self) -> (CalendarDate, CalendarDate) { (self.start, self.end) }
    pub const fn interpretation_digest(&self) -> EvidenceDigest { self.interpretation_digest }
    pub const fn relationship_digest(&self) -> EvidenceDigest { self.relationship_digest }
    /// Reconstructs the same closed interpretation against actual reader-owned coordinates.
    pub fn matches(&self, native: &VenueId, target: &VenueId, dates: (CalendarDate, CalendarDate)) -> bool {
        Self::try_new(native.clone(), target.clone(), dates.0, dates.1).is_ok_and(|candidate| candidate == *self)
    }
}
impl From<ReviewedMarketCalendarRelationship> for RelationshipWire {
    fn from(value: ReviewedMarketCalendarRelationship) -> Self {
        Self { native_venue: value.native_venue, target_venue: value.target_venue,
            start: value.start, end: value.end, interpretation_digest: value.interpretation_digest,
            relationship_digest: value.relationship_digest }
    }
}
impl TryFrom<RelationshipWire> for ReviewedMarketCalendarRelationship {
    type Error = ProviderCaptureError;
    fn try_from(wire: RelationshipWire) -> Result<Self, Self::Error> {
        let reconstructed = Self::try_new(wire.native_venue, wire.target_venue, wire.start, wire.end)?;
        if reconstructed.interpretation_digest != wire.interpretation_digest
            || reconstructed.relationship_digest != wire.relationship_digest
        { return Err(ProviderCaptureError::InvalidMarketBarHistorySemantics); }
        Ok(reconstructed)
    }
}
fn digest(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}
