//! Governed rationale for non-Level-1 use of producer-derived inputs.

use std::mem::size_of;

use market_squawk_domain::{InstrumentId, Timestamp};

use crate::{
    ActorId, CanonicalHasher, FairValueError, InputInstrumentRelation, InputObservability,
    PriceAdjustment, checked_add,
};

const MAX_RATIONALE_BYTES: usize = 4_096;

digest_id!(
    /// SHA-256 identity of one bounded input-use assessment.
    InputUseAssessmentHash
);

/// Audited accounting judgment for comparable, proxy, adjusted, or unobservable use.
///
/// This assessment never creates quoted-price, active-market, or accessible-market evidence and
/// therefore cannot qualify an input for Level 1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputUseAssessment {
    subject_instrument_id: InstrumentId,
    relationship: InputInstrumentRelation,
    observability: InputObservability,
    adjustment: PriceAdjustment,
    rationale: Box<str>,
    assessed_by: ActorId,
    assessed_at: Timestamp,
    hash: InputUseAssessmentHash,
    retained_bytes: usize,
}

impl InputUseAssessment {
    /// Constructs a bounded, actor-attributed non-Level-1 assessment.
    ///
    /// # Errors
    ///
    /// Rejects quoted-price claims, contradictory observable/unobservable settings, and empty,
    /// excessive, or control-character rationale text.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        subject_instrument_id: InstrumentId,
        relationship: InputInstrumentRelation,
        observability: InputObservability,
        adjustment: PriceAdjustment,
        rationale: &str,
        assessed_by: ActorId,
        assessed_at: Timestamp,
    ) -> Result<Self, FairValueError> {
        let is_non_level_one_use = relationship != InputInstrumentRelation::Identical
            || observability == InputObservability::Unobservable
            || adjustment != PriceAdjustment::None;
        if !is_non_level_one_use
            || observability == InputObservability::QuotedPrice
            || (observability == InputObservability::Observable
                && adjustment == PriceAdjustment::Unobservable)
            || rationale.is_empty()
            || rationale.len() > MAX_RATIONALE_BYTES
            || rationale.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(FairValueError::InvalidInputAssessment);
        }
        let mut hash = CanonicalHasher::new(b"market-squawk/input-use-assessment/v1");
        hash.bytes(subject_instrument_id.as_uuid().as_bytes());
        hash.u8(super::measurement::relation_tag(relationship));
        hash.u8(super::measurement::observability_tag(observability));
        hash.u8(super::measurement::adjustment_tag(adjustment));
        hash.bytes(rationale.as_bytes());
        hash.bytes(assessed_by.as_str().as_bytes());
        hash.i64(assessed_at.unix_nanos());
        let retained_bytes = checked_add(
            size_of::<Self>(),
            checked_add(rationale.len(), assessed_by.retained_bytes())?,
        )?;
        Ok(Self {
            subject_instrument_id,
            relationship,
            observability,
            adjustment,
            rationale: rationale.into(),
            assessed_by,
            assessed_at,
            hash: InputUseAssessmentHash(hash.finish()),
            retained_bytes,
        })
    }

    /// Returns the measured subject selected by the assessment.
    pub const fn subject_instrument_id(&self) -> InstrumentId {
        self.subject_instrument_id
    }

    /// Returns the assessed instrument relationship.
    pub const fn relationship(&self) -> InputInstrumentRelation {
        self.relationship
    }

    /// Returns the assessed accounting observability.
    pub const fn observability(&self) -> InputObservability {
        self.observability
    }

    /// Returns the assessed source-value adjustment.
    pub const fn adjustment(&self) -> PriceAdjustment {
        self.adjustment
    }

    /// Returns the bounded rationale.
    pub fn rationale(&self) -> &str {
        &self.rationale
    }

    /// Returns the responsible assessor.
    pub const fn assessed_by(&self) -> &ActorId {
        &self.assessed_by
    }

    /// Returns the assessment instant.
    pub const fn assessed_at(&self) -> Timestamp {
        self.assessed_at
    }

    /// Returns the immutable assessment identity.
    pub const fn hash(&self) -> InputUseAssessmentHash {
        self.hash
    }

    pub(crate) fn validate_for(
        &self,
        reference_instrument_id: InstrumentId,
        producer_ingested_at: Timestamp,
    ) -> Result<(), FairValueError> {
        let same = self.subject_instrument_id == reference_instrument_id;
        if same != (self.relationship == InputInstrumentRelation::Identical)
            || self.assessed_at < producer_ingested_at
        {
            return Err(FairValueError::InvalidInputAssessment);
        }
        Ok(())
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}
