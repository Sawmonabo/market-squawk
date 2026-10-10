//! Date-preserving application values retained by the existing corporate-action plan.
//!
//! These are reconstruction values, not source, calendar, publication, or coverage authority.
//! The application producer obtains their coordinates from opaque reopened source/calendar
//! receipts. Reopening a recipe must rejoin those receipts before serving the rebuilt plan.

use market_squawk_domain::{CalendarDate, DigestAlgorithm, EvidenceDigest, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{CorporateActionError, CorporateActionRecord};

/// The explicit daily simulation assumption governing a source-reported payable date.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CorporateActionPaymentPolicy {
    /// Accrue value but do not infer any spendable cash boundary.
    RetainReceivable,
    /// Assume payment by the close of the exact reported payable session. A holiday, missing
    /// payable date, or absent session remains an unsettled receivable; no date is rolled.
    EndOfReportedPayableSessionV1,
}

/// Exact calendar session values; construction alone grants no calendar authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CorporateActionSessionValues {
    pub date: CalendarDate,
    pub opens_at: Timestamp,
    pub closes_at_exclusive: Timestamp,
    pub available_at: Timestamp,
    pub receipt_digest: EvidenceDigest,
}

impl CorporateActionSessionValues {
    fn valid(self) -> bool {
        self.opens_at < self.closes_at_exclusive && self.receipt_digest.bytes() != [0; 32]
    }
}

/// An application coordinate kept separate from an unchanged source observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CorporateActionApplication {
    source_record_digest: EvidenceDigest,
    source_snapshot_digest: EvidenceDigest,
    source_available_at: Timestamp,
    effective_session: CorporateActionSessionValues,
    payable_date: Option<CalendarDate>,
    payable_session: Option<CorporateActionSessionValues>,
    payment_policy: CorporateActionPaymentPolicy,
}

impl CorporateActionApplication {
    /// Checks reconstruction values against the exact unchanged source record. This constructor
    /// does not certify a source interval or calendar. The serving producer must own both read
    /// receipts; callers cannot turn this value into a complete-source coverage receipt.
    #[allow(clippy::too_many_arguments)]
    pub fn try_from_retained_values(
        record: &CorporateActionRecord,
        source_snapshot_digest: EvidenceDigest,
        source_available_at: Timestamp,
        effective_session: CorporateActionSessionValues,
        payable_date: Option<CalendarDate>,
        payable_session: Option<CorporateActionSessionValues>,
        payment_policy: CorporateActionPaymentPolicy,
    ) -> Result<Self, CorporateActionError> {
        let value = Self {
            source_record_digest: source_record_digest(record)?,
            source_snapshot_digest,
            source_available_at,
            effective_session,
            payable_date,
            payable_session,
            payment_policy,
        };
        value.validate_for(record)?;
        Ok(value)
    }

    pub(super) fn validate_for(
        &self,
        record: &CorporateActionRecord,
    ) -> Result<(), CorporateActionError> {
        if record
            .observation()
            .context()
            .time()
            .effective()
            .calendar_date_value()
            != Some(self.effective_session.date)
            || !self.effective_session.valid()
            || self.source_record_digest != source_record_digest(record)?
            || self.source_snapshot_digest.bytes() == [0; 32]
            || record.observation().context().provenance().ingested_at() > self.source_available_at
            || self
                .payable_date
                .is_some_and(|date| date < self.effective_session.date)
            || self.payable_session.is_some_and(|session| {
                !session.valid()
                    || Some(session.date) != self.payable_date
                    || session.closes_at_exclusive <= self.effective_session.opens_at
            })
            || (self.payment_policy == CorporateActionPaymentPolicy::RetainReceivable
                && self.payable_session.is_some())
        {
            return Err(CorporateActionError::InvalidApplication);
        }
        Ok(())
    }

    /// Returns the native ex/effective date without altering its precision.
    pub const fn source_date(&self) -> CalendarDate {
        self.effective_session.date
    }
    /// Returns the actual session open used by the daily application policy.
    pub const fn application_at(&self) -> Timestamp {
        self.effective_session.opens_at
    }
    /// Returns the exact source-reported payable date, including non-session dates.
    pub const fn payable_date(&self) -> Option<CalendarDate> {
        self.payable_date
    }
    /// Returns the explicitly retained payment assumption.
    pub const fn payment_policy(&self) -> CorporateActionPaymentPolicy {
        self.payment_policy
    }
    /// Returns a simulated cash boundary only for the exact evidenced payable session.
    pub fn simulated_cash_settlement_at(&self) -> Option<Timestamp> {
        match self.payment_policy {
            CorporateActionPaymentPolicy::RetainReceivable => None,
            CorporateActionPaymentPolicy::EndOfReportedPayableSessionV1 => self
                .payable_session
                .map(|session| session.closes_at_exclusive),
        }
    }
    /// Returns actual conservative knowledge time of every retained source/calendar dependency.
    pub fn available_at(&self) -> Timestamp {
        self.source_available_at
            .max(self.effective_session.available_at)
            .max(
                self.payable_session
                    .map_or(self.source_available_at, |session| session.available_at),
            )
    }
    /// Returns the exact sealed source snapshot used for application.
    pub const fn source_snapshot_digest(&self) -> EvidenceDigest {
        self.source_snapshot_digest
    }
    pub(super) fn canonical_bytes(&self) -> Result<Vec<u8>, CorporateActionError> {
        serde_json::to_vec(self).map_err(|_| CorporateActionError::RecoveryCodec)
    }
}

fn source_record_digest(
    record: &CorporateActionRecord,
) -> Result<EvidenceDigest, CorporateActionError> {
    let mut source = record.clone();
    source.application = None;
    let bytes = super::canonical::canonical_record_bytes(&source)?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(bytes).into(),
    ))
}
