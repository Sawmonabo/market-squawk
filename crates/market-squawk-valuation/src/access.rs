//! Dual-approved reporting-entity market-access assessments.

use std::mem::size_of;

use market_squawk_domain::{AccountId, InstrumentId, Timestamp, VenueId};

use crate::{ActorId, CanonicalHasher, FairValueError, MarketAccess, checked_add};

const MAX_ACCESS_RATIONALE_BYTES: usize = 4_096;

digest_id!(
    /// SHA-256 identity of one immutable market-access assessment.
    MarketAccessAssessmentId
);

/// Immutable dual-approved reporting-entity access conclusion for one exact market.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovedMarketAccess {
    id: MarketAccessAssessmentId,
    account_id: AccountId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    conclusion: MarketAccess,
    effective_from: Timestamp,
    effective_until: Timestamp,
    rationale: Box<str>,
    prepared_by: ActorId,
    prepared_at: Timestamp,
    approved_by: ActorId,
    approved_at: Timestamp,
    retained_bytes: usize,
}

impl ApprovedMarketAccess {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        account_id: AccountId,
        venue_id: VenueId,
        instrument_id: InstrumentId,
        conclusion: MarketAccess,
        effective_from: Timestamp,
        effective_until: Timestamp,
        rationale: &str,
        prepared_by: ActorId,
        prepared_at: Timestamp,
        approved_by: ActorId,
        approved_at: Timestamp,
    ) -> Result<Self, FairValueError> {
        if conclusion == MarketAccess::NotAssessed
            || effective_until < effective_from
            || approved_at < prepared_at
            || prepared_by == approved_by
            || rationale.is_empty()
            || rationale.len() > MAX_ACCESS_RATIONALE_BYTES
            || rationale.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(if prepared_by == approved_by {
                FairValueError::SeparationOfDuties
            } else {
                FairValueError::InvalidMarketAccessAssessment
            });
        }
        let mut hash = CanonicalHasher::new(b"market-squawk/market-access-assessment/v1");
        hash.bytes(account_id.as_uuid().as_bytes());
        hash.bytes(venue_id.as_str().as_bytes());
        hash.bytes(instrument_id.as_uuid().as_bytes());
        hash.u8(super::measurement::access_tag(conclusion));
        hash.i64(effective_from.unix_nanos());
        hash.i64(effective_until.unix_nanos());
        hash.bytes(rationale.as_bytes());
        hash.bytes(prepared_by.as_str().as_bytes());
        hash.i64(prepared_at.unix_nanos());
        hash.bytes(approved_by.as_str().as_bytes());
        hash.i64(approved_at.unix_nanos());
        let retained_bytes = checked_add(
            size_of::<Self>(),
            checked_add(
                venue_id.retained_bytes(),
                checked_add(
                    rationale.len(),
                    checked_add(prepared_by.retained_bytes(), approved_by.retained_bytes())?,
                )?,
            )?,
        )?;
        Ok(Self {
            id: MarketAccessAssessmentId(hash.finish()),
            account_id,
            venue_id,
            instrument_id,
            conclusion,
            effective_from,
            effective_until,
            rationale: rationale.into(),
            prepared_by,
            prepared_at,
            approved_by,
            approved_at,
            retained_bytes,
        })
    }

    /// Returns the immutable assessment identity.
    pub const fn id(&self) -> MarketAccessAssessmentId {
        self.id
    }

    /// Returns the reporting account whose accessibility was assessed.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the exact venue assessed.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the exact instrument assessed.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the approved access conclusion.
    pub const fn conclusion(&self) -> MarketAccess {
        self.conclusion
    }

    /// Returns the inclusive effective start.
    pub const fn effective_from(&self) -> Timestamp {
        self.effective_from
    }

    /// Returns the inclusive effective end.
    pub const fn effective_until(&self) -> Timestamp {
        self.effective_until
    }

    /// Returns the bounded rationale.
    pub fn rationale(&self) -> &str {
        &self.rationale
    }

    /// Returns the preparer.
    pub const fn prepared_by(&self) -> &ActorId {
        &self.prepared_by
    }

    /// Returns preparation time.
    pub const fn prepared_at(&self) -> Timestamp {
        self.prepared_at
    }

    /// Returns the independent approver.
    pub const fn approved_by(&self) -> &ActorId {
        &self.approved_by
    }

    /// Returns approval time.
    pub const fn approved_at(&self) -> Timestamp {
        self.approved_at
    }

    pub(crate) fn validate_for(
        &self,
        account_id: AccountId,
        venue_id: &VenueId,
        instrument_id: InstrumentId,
        at: Timestamp,
    ) -> Result<(), FairValueError> {
        if self.account_id != account_id
            || &self.venue_id != venue_id
            || self.instrument_id != instrument_id
            || at < self.effective_from
            || at > self.effective_until
            || self.approved_at > at
        {
            return Err(FairValueError::InvalidMarketAccessAssessment);
        }
        Ok(())
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}
