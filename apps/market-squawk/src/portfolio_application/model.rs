//! Immutable portfolio application read images and durable publication references.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use market_squawk_adapter_portfolio::{
    self as imported, LotMethod, ReconciliationDiscrepancy, TransactionKind,
};
use market_squawk_domain::{
    AccountId, Currency, InstrumentId, LotSize, Money, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_portfolio::{
    PortfolioRevision, PortfolioRevisionToken, PortfolioService, PortfolioServiceLimitInput,
    PortfolioServiceLimits,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{PortfolioApplicationLimits, PortfolioApplicationServiceError};

pub(super) const PUBLICATION_SCHEMA_VERSION: u16 = 2;

/// Canonical source facts shared by admitted imports and reconciled native paper publications.
/// Construction remains private to this portfolio owner; transport JSON cannot mint these rows.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct AccountObservation {
    pub(super) account_id: AccountId,
    pub(super) currency: Currency,
    pub(super) cash_balance: Money,
    pub(super) settlement_available_cash: Option<Money>,
    pub(super) as_of: Timestamp,
    pub(super) source_reference: SourceIdentifier,
}

impl AccountObservation {
    pub(super) const fn account_id(&self) -> AccountId {
        self.account_id
    }
    pub(super) const fn currency(&self) -> Currency {
        self.currency
    }
    pub(super) const fn cash_balance(&self) -> Money {
        self.cash_balance
    }
    pub(super) const fn settlement_available_cash(&self) -> Option<Money> {
        self.settlement_available_cash
    }
    pub(super) const fn as_of(&self) -> Timestamp {
        self.as_of
    }
    pub(super) const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}

impl From<imported::AccountObservation> for AccountObservation {
    fn from(value: imported::AccountObservation) -> Self {
        Self {
            account_id: value.account_id(),
            currency: value.currency(),
            cash_balance: value.cash_balance(),
            settlement_available_cash: value.settlement_available_cash(),
            as_of: value.as_of(),
            source_reference: value.source_reference().clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(super) struct SignedQuantity(pub(super) Decimal);
impl SignedQuantity {
    pub(super) const fn as_decimal(self) -> Decimal {
        self.0
    }
}
impl std::fmt::Display for SignedQuantity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct CostBasisObservation {
    pub(super) account_id: AccountId,
    pub(super) instrument_id: InstrumentId,
    pub(super) amount: Money,
    pub(super) lot_method: LotMethod,
    pub(super) source_reference: SourceIdentifier,
}
impl CostBasisObservation {
    pub(super) const fn amount(&self) -> Money {
        self.amount
    }
    pub(super) const fn lot_method(&self) -> LotMethod {
        self.lot_method
    }
    pub(super) const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum BasisResolution {
    Resolved {
        observation: CostBasisObservation,
    },
    Missing,
    Ambiguous {
        candidates: Vec<Money>,
        lot_method: LotMethod,
    },
}
impl From<&imported::BasisResolution> for BasisResolution {
    fn from(value: &imported::BasisResolution) -> Self {
        match value {
            imported::BasisResolution::Resolved { observation } => Self::Resolved {
                observation: CostBasisObservation {
                    account_id: observation.account_id(),
                    instrument_id: observation.instrument_id(),
                    amount: observation.amount(),
                    lot_method: observation.lot_method(),
                    source_reference: observation.source_reference().clone(),
                },
            },
            imported::BasisResolution::Missing => Self::Missing,
            imported::BasisResolution::Ambiguous {
                candidates,
                lot_method,
            } => Self::Ambiguous {
                candidates: candidates.clone(),
                lot_method: *lot_method,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct HoldingObservation {
    pub(super) account_id: AccountId,
    pub(super) instrument_id: InstrumentId,
    pub(super) currency: Currency,
    pub(super) quantity: SignedQuantity,
    pub(super) lot_size: LotSize,
    pub(super) market_value: Money,
    pub(super) as_of: Timestamp,
    pub(super) basis: BasisResolution,
    pub(super) source_reference: SourceIdentifier,
}
impl HoldingObservation {
    pub(super) const fn account_id(&self) -> AccountId {
        self.account_id
    }
    pub(super) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub(super) const fn currency(&self) -> Currency {
        self.currency
    }
    pub(super) const fn quantity(&self) -> SignedQuantity {
        self.quantity
    }
    pub(super) const fn lot_size(&self) -> LotSize {
        self.lot_size
    }
    pub(super) const fn market_value(&self) -> Money {
        self.market_value
    }
    pub(super) const fn as_of(&self) -> Timestamp {
        self.as_of
    }
    pub(super) const fn basis(&self) -> &BasisResolution {
        &self.basis
    }
    pub(super) const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}
impl From<&imported::HoldingObservation> for HoldingObservation {
    fn from(value: &imported::HoldingObservation) -> Self {
        Self {
            account_id: value.account_id(),
            instrument_id: value.instrument_id(),
            currency: value.currency(),
            quantity: SignedQuantity(value.quantity().as_decimal()),
            lot_size: value.lot_size(),
            market_value: value.market_value(),
            as_of: value.as_of(),
            basis: value.basis().into(),
            source_reference: value.source_reference().clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct PortfolioTransaction {
    pub(super) broker_transaction_id: SourceIdentifier,
    pub(super) account_id: AccountId,
    pub(super) instrument_id: Option<InstrumentId>,
    pub(super) kind: TransactionKind,
    pub(super) amount: Money,
    pub(super) quantity: Option<SignedQuantity>,
    pub(super) occurred_at: Timestamp,
    pub(super) lot_method: Option<LotMethod>,
    pub(super) source_reference: SourceIdentifier,
}
impl PortfolioTransaction {
    pub(super) const fn broker_transaction_id(&self) -> &SourceIdentifier {
        &self.broker_transaction_id
    }
    pub(super) const fn account_id(&self) -> AccountId {
        self.account_id
    }
    pub(super) const fn instrument_id(&self) -> Option<InstrumentId> {
        self.instrument_id
    }
    pub(super) const fn kind(&self) -> TransactionKind {
        self.kind
    }
    pub(super) const fn amount(&self) -> Money {
        self.amount
    }
    pub(super) const fn quantity(&self) -> Option<SignedQuantity> {
        self.quantity
    }
    pub(super) const fn occurred_at(&self) -> Timestamp {
        self.occurred_at
    }
    pub(super) const fn lot_method(&self) -> Option<LotMethod> {
        self.lot_method
    }
    pub(super) const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}
impl From<&imported::PortfolioTransaction> for PortfolioTransaction {
    fn from(value: &imported::PortfolioTransaction) -> Self {
        Self {
            broker_transaction_id: value.broker_transaction_id().clone(),
            account_id: value.account_id(),
            instrument_id: value.instrument_id(),
            kind: value.kind(),
            amount: value.amount(),
            quantity: value.quantity().map(|q| SignedQuantity(q.as_decimal())),
            occurred_at: value.occurred_at(),
            lot_method: value.lot_method(),
            source_reference: value.source_reference().clone(),
        }
    }
}

/// The original publication family determines the only admitted replay path.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum PublicationKind {
    Imported,
    NativePaper,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PublicationEntry {
    pub(super) kind: PublicationKind,
    pub(super) account_id: AccountId,
    pub(super) artifact_reference: String,
    pub(super) artifact_sha256: [u8; 32],
    /// Immutable governed interpretation/authorization receipt for a V1 two-phase import.
    /// Native paper publications retain their original worker evidence instead of import approval.
    pub(super) governance_receipt_reference: Option<String>,
    pub(super) governance_receipt_sha256: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PublicationManifest {
    schema_version: u16,
    pub(super) entries: Vec<PublicationEntry>,
}

impl PublicationManifest {
    pub(super) const fn empty() -> Self {
        Self {
            schema_version: PUBLICATION_SCHEMA_VERSION,
            entries: Vec::new(),
        }
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, PortfolioApplicationServiceError> {
        let manifest: Self = serde_json::from_slice(bytes)
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
        if manifest.schema_version != PUBLICATION_SCHEMA_VERSION {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        Ok(manifest)
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, PortfolioApplicationServiceError> {
        serde_json::to_vec(self).map_err(|_| PortfolioApplicationServiceError::Publication)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct SourceKey {
    pub(super) source_id: SourceId,
    pub(super) metadata_revision: String,
}

#[derive(Clone, Debug)]
pub(super) struct PublishedRevision {
    pub(super) core: PortfolioRevision,
    pub(super) account: AccountObservation,
    pub(super) holdings: Vec<HoldingObservation>,
    pub(super) transactions: Vec<PortfolioTransaction>,
    pub(super) discrepancies: Vec<ReconciliationDiscrepancy>,
    pub(super) source_id: SourceId,
    pub(super) source_coverage: Vec<SourceId>,
    pub(super) effective_at: Timestamp,
    pub(super) available_at: Option<Timestamp>,
    pub(super) artifact_sha256: [u8; 32],
    pub(super) native_paper: Option<NativePaperIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativePaperIdentity {
    pub(super) configuration_digest: [u8; 32],
    pub(super) checkpoint_digest: [u8; 32],
    pub(super) sequence: u64,
    pub(super) origin_sha256: [u8; 32],
    pub(super) accounting_sha256: [u8; 32],
    pub(super) accounting_anchor: bool,
}

impl PublishedRevision {
    pub(super) fn token(&self) -> PortfolioRevisionToken {
        self.core.token()
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct AccountHistory {
    pub(super) revisions: Vec<PublishedRevision>,
}

#[derive(Clone, Debug)]
pub(super) struct PortfolioReadImage {
    pub(super) accounts: BTreeMap<AccountId, AccountHistory>,
    pub(super) revisions: PortfolioService,
}

impl PortfolioReadImage {
    pub(super) fn try_from_accounts(
        accounts: BTreeMap<AccountId, AccountHistory>,
        limits: PortfolioApplicationLimits,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        if accounts.len() > limits.max_accounts {
            return Err(PortfolioApplicationServiceError::ResourceExhausted);
        }
        let mut current = Vec::new();
        let mut revoked = Vec::new();
        current
            .try_reserve_exact(accounts.len())
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
        let mut retained_bytes = std::mem::size_of::<Self>()
            .checked_add(
                accounts
                    .len()
                    .checked_mul(std::mem::size_of::<(AccountId, AccountHistory)>())
                    .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?,
            )
            .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
        for history in accounts.values() {
            if history.revisions.len() > limits.max_history_per_account {
                return Err(PortfolioApplicationServiceError::ResourceExhausted);
            }
            retained_bytes = retained_bytes
                .checked_add(
                    history
                        .revisions
                        .len()
                        .checked_mul(std::mem::size_of::<PublishedRevision>())
                        .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?,
                )
                .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
            for revision in &history.revisions {
                let external = serde_json::to_vec(&(
                    &revision.account,
                    &revision.holdings,
                    &revision.transactions,
                    &revision.discrepancies,
                    revision.source_id.as_str(),
                    &revision.source_coverage,
                    revision.effective_at.unix_nanos(),
                    revision.available_at.map(Timestamp::unix_nanos),
                    revision.artifact_sha256,
                    revision.native_paper,
                ))
                .map_err(|_| PortfolioApplicationServiceError::Publication)?;
                retained_bytes = retained_bytes
                    .checked_add(revision.core.retained_bytes())
                    .and_then(|total| total.checked_add(external.len()))
                    .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
                if retained_bytes > limits.max_retained_bytes {
                    return Err(PortfolioApplicationServiceError::ResourceExhausted);
                }
            }
            let (head, prior) = history
                .revisions
                .split_last()
                .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
            current.push(head.core.clone());
            revoked
                .try_reserve(prior.len())
                .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
            revoked.extend(prior.iter().map(PublishedRevision::token));
        }
        let service_limits = PortfolioServiceLimits::try_new(PortfolioServiceLimitInput {
            max_accounts: nonzero(limits.max_accounts)?,
            max_history_per_account: nonzero(limits.max_history_per_account)?,
            max_results: nonzero(limits.max_result_items)?,
            max_retained_bytes: nonzero(limits.max_retained_bytes)?,
        })
        .map_err(|_| PortfolioApplicationServiceError::InvalidLimits)?;
        let revisions = PortfolioService::try_new(current, revoked, service_limits)
            .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        Ok(Self {
            accounts,
            revisions,
        })
    }
}

fn nonzero(value: usize) -> Result<NonZeroUsize, PortfolioApplicationServiceError> {
    NonZeroUsize::new(value).ok_or(PortfolioApplicationServiceError::InvalidLimits)
}
