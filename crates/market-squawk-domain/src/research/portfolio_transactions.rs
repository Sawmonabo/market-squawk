//! Source-neutral checked portfolio transaction evidence.

use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    AccountId, EvidenceDigest, InstrumentId, Money, RevisionNumber, SourceId, SourceIdentifier,
    Timestamp,
};

/// Closed normalized Task 10 transaction classification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizedPortfolioTransactionClass {
    /// Security or digital-asset trade.
    Trade,
    /// External cash deposit or withdrawal.
    CashTransfer,
    /// Dividend, interest, withholding, staking, or other source-classified income.
    Income,
    /// Charged commission or fee.
    Fee,
    /// Source-recorded corporate-action marker.
    CorporateAction,
}

/// Closed source-authored lot method retained by normalized trade evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizedPortfolioLotMethod {
    /// First acquired units are disposed first.
    Fifo,
    /// Last acquired units are disposed first.
    Lifo,
    /// The source identifies exact disposed lots.
    SpecificIdentification,
    /// A source-defined average-cost pool is used.
    AverageCost,
}

/// Complete input for one normalized transaction-evidence contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedPortfolioTransactionEvidenceInput {
    /// Registered source namespace.
    pub source_id: SourceId,
    /// Stable logical raw-record identity across corrections.
    pub logical_record_id: SourceIdentifier,
    /// Exact source revision identity carried by the extraction record.
    pub source_revision: SourceIdentifier,
    /// Exact prior source revision replaced by this correction.
    pub supersedes_source_revision: Option<SourceIdentifier>,
    /// Checked one-based canonical revision number.
    pub revision: RevisionNumber,
    /// Immutable raw-record reference derived by the source adapter.
    pub raw_source_reference: SourceIdentifier,
    /// Digest of the exact raw payload bytes.
    pub raw_payload_digest: EvidenceDigest,
    /// Stable provider transaction identity.
    pub broker_transaction_id: SourceIdentifier,
    /// Canonical account identity.
    pub account_id: AccountId,
    /// Canonical instrument identity when source-scoped.
    pub instrument_id: Option<InstrumentId>,
    /// Closed source transaction class.
    pub classification: NormalizedPortfolioTransactionClass,
    /// Exact signed source amount and currency.
    pub amount: Money,
    /// Exact signed nonzero source quantity when supplied.
    pub quantity: Option<Decimal>,
    /// Exact source occurrence time.
    pub occurred_at: Timestamp,
    /// Exact source-authored lot method when supplied.
    pub lot_method: Option<NormalizedPortfolioLotMethod>,
}

/// Checked normalized transaction evidence retaining raw and correction lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedPortfolioTransactionEvidence {
    source_id: SourceId,
    logical_record_id: SourceIdentifier,
    source_revision: SourceIdentifier,
    supersedes_source_revision: Option<SourceIdentifier>,
    revision: RevisionNumber,
    raw_source_reference: SourceIdentifier,
    raw_payload_digest: EvidenceDigest,
    broker_transaction_id: SourceIdentifier,
    account_id: AccountId,
    instrument_id: Option<InstrumentId>,
    classification: NormalizedPortfolioTransactionClass,
    amount: Money,
    quantity: Option<Decimal>,
    occurred_at: Timestamp,
    lot_method: Option<NormalizedPortfolioLotMethod>,
}

impl NormalizedPortfolioTransactionEvidence {
    /// Validates the closed classification/field matrix and correction lineage.
    ///
    /// # Errors
    ///
    /// Rejects zero quantities/amounts, incomplete trade fields, misplaced lot methods, missing
    /// corporate-action instruments, or inconsistent first/corrected revision lineage.
    pub fn try_new(
        input: NormalizedPortfolioTransactionEvidenceInput,
    ) -> Result<Self, NormalizedPortfolioTransactionError> {
        let revision_is_first = input.revision.get() == 1;
        if revision_is_first == input.supersedes_source_revision.is_some() {
            return Err(NormalizedPortfolioTransactionError::InvalidRevisionLineage);
        }
        if input.quantity.is_some_and(|quantity| quantity.is_zero()) {
            return Err(NormalizedPortfolioTransactionError::ZeroQuantity);
        }
        let invalid_fields = match input.classification {
            NormalizedPortfolioTransactionClass::Trade => {
                input.instrument_id.is_none()
                    || input.quantity.is_none()
                    || input.lot_method.is_none()
                    || input.amount.amount().is_zero()
            }
            NormalizedPortfolioTransactionClass::CashTransfer => {
                input.instrument_id.is_some()
                    || input.quantity.is_some()
                    || input.lot_method.is_some()
                    || input.amount.amount().is_zero()
            }
            NormalizedPortfolioTransactionClass::Income
            | NormalizedPortfolioTransactionClass::Fee => {
                input.quantity.is_some()
                    || input.lot_method.is_some()
                    || input.amount.amount().is_zero()
            }
            NormalizedPortfolioTransactionClass::CorporateAction => {
                input.instrument_id.is_none() || input.lot_method.is_some()
            }
        };
        if invalid_fields {
            return Err(NormalizedPortfolioTransactionError::InvalidFieldCombination);
        }
        Ok(Self {
            source_id: input.source_id,
            logical_record_id: input.logical_record_id,
            source_revision: input.source_revision,
            supersedes_source_revision: input.supersedes_source_revision,
            revision: input.revision,
            raw_source_reference: input.raw_source_reference,
            raw_payload_digest: input.raw_payload_digest,
            broker_transaction_id: input.broker_transaction_id,
            account_id: input.account_id,
            instrument_id: input.instrument_id,
            classification: input.classification,
            amount: Money::new(input.amount.amount().normalize(), input.amount.currency()),
            quantity: input.quantity.map(|quantity| quantity.normalize()),
            occurred_at: input.occurred_at,
            lot_method: input.lot_method,
        })
    }

    /// Returns the registered source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the stable logical raw-record identity.
    pub const fn logical_record_id(&self) -> &SourceIdentifier {
        &self.logical_record_id
    }

    /// Returns the exact active source revision identity.
    pub const fn source_revision(&self) -> &SourceIdentifier {
        &self.source_revision
    }

    /// Returns the exact prior source revision replaced by this correction.
    pub fn supersedes_source_revision(&self) -> Option<&SourceIdentifier> {
        self.supersedes_source_revision.as_ref()
    }

    /// Returns the one-based canonical revision number.
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }

    /// Returns the immutable raw-record reference.
    pub const fn raw_source_reference(&self) -> &SourceIdentifier {
        &self.raw_source_reference
    }

    /// Returns the exact raw payload digest.
    pub const fn raw_payload_digest(&self) -> EvidenceDigest {
        self.raw_payload_digest
    }

    /// Returns the stable provider transaction identity.
    pub const fn broker_transaction_id(&self) -> &SourceIdentifier {
        &self.broker_transaction_id
    }

    /// Returns the canonical account identity.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the canonical instrument identity when source-scoped.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        self.instrument_id
    }

    /// Returns the closed normalized class.
    pub const fn classification(&self) -> NormalizedPortfolioTransactionClass {
        self.classification
    }

    /// Returns the exact signed source amount.
    pub const fn amount(&self) -> Money {
        self.amount
    }

    /// Returns the exact signed source quantity when supplied.
    pub const fn quantity(&self) -> Option<Decimal> {
        self.quantity
    }

    /// Returns the exact source occurrence time.
    pub const fn occurred_at(&self) -> Timestamp {
        self.occurred_at
    }

    /// Returns the source-authored lot method when supplied.
    pub const fn lot_method(&self) -> Option<NormalizedPortfolioLotMethod> {
        self.lot_method
    }
}

/// Normalized portfolio transaction-evidence invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NormalizedPortfolioTransactionError {
    /// First/corrected revision numbering disagrees with supersession evidence.
    InvalidRevisionLineage,
    /// A supplied quantity was exactly zero.
    ZeroQuantity,
    /// Economic fields do not match the closed source classification.
    InvalidFieldCombination,
}

impl fmt::Display for NormalizedPortfolioTransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRevisionLineage => {
                formatter.write_str("normalized portfolio transaction revision lineage is invalid")
            }
            Self::ZeroQuantity => {
                formatter.write_str("normalized portfolio transaction quantity must be nonzero")
            }
            Self::InvalidFieldCombination => formatter.write_str(
                "normalized portfolio transaction fields do not match its classification",
            ),
        }
    }
}

impl std::error::Error for NormalizedPortfolioTransactionError {}
