//! Cash entitlements retained independently from spendable balances.

use crate::PortfolioError;
use market_squawk_data::CorporateActionPlan;
use market_squawk_domain::{CalendarDate, EvidenceDigest, InstrumentId, Money, Timestamp};

/// Signed amount fixed from inventory at the entitlement boundary. A subsequent sale or split
/// cannot change this amount. Negative amounts retain short-position payment obligations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CashEntitlement {
    pub(crate) admitted_index: usize,
    pub(crate) instrument: InstrumentId,
    pub(crate) action_evidence: EvidenceDigest,
    pub(crate) entitled_at: Timestamp,
    pub(crate) amount: Money,
    pub(crate) payable_date: Option<CalendarDate>,
    pub(crate) simulated_settlement_at: Option<Timestamp>,
    pub(crate) settled: bool,
}
impl CashEntitlement {
    pub(crate) fn from_plan(
        plan: &CorporateActionPlan,
        admitted_index: usize,
        instrument: InstrumentId,
        amount: Money,
    ) -> Result<Self, PortfolioError> {
        let record = plan
            .admitted()
            .get(admitted_index)
            .ok_or(PortfolioError::EvidenceMismatch)?;
        Ok(Self {
            admitted_index,
            instrument,
            amount,
            action_evidence: record.evidence_digest(),
            entitled_at: record
                .application_at()
                .ok_or(PortfolioError::EvidenceMismatch)?,
            payable_date: record
                .application()
                .and_then(|application| application.payable_date()),
            simulated_settlement_at: record
                .application()
                .and_then(|application| application.simulated_cash_settlement_at()),
            settled: false,
        })
    }
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
    pub const fn action_evidence(&self) -> EvidenceDigest {
        self.action_evidence
    }
    pub const fn entitled_at(&self) -> Timestamp {
        self.entitled_at
    }
    pub const fn amount(&self) -> Money {
        self.amount
    }
    pub const fn payable_date(&self) -> Option<CalendarDate> {
        self.payable_date
    }
    /// This is an explicitly retained simulation convention, never a provider payment timestamp.
    pub const fn simulated_settlement_at(&self) -> Option<Timestamp> {
        self.simulated_settlement_at
    }
    pub const fn settled(&self) -> bool {
        self.settled
    }
}
