//! Versioned raw portfolio wire contracts.

use serde::Deserialize;

use crate::{LotMethod, TransactionKind};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawEnvelopeWire {
    pub(crate) record_id: String,
    #[serde(default)]
    pub(crate) supersedes_revision: Option<String>,
    pub(crate) revision_number: u32,
    pub(crate) received_at_unix_nanos: String,
    pub(crate) ingested_at_unix_nanos: String,
    pub(crate) record: RawRecordWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub(crate) enum RawRecordWire {
    Account {
        account_id: String,
        currency: String,
        cash_balance: String,
        as_of_unix_nanos: String,
    },
    Holding {
        account_id: String,
        instrument_id: String,
        currency: String,
        quantity: String,
        lot_size: String,
        market_value: String,
        as_of_unix_nanos: String,
        cost_basis: BasisWire,
    },
    Transaction {
        broker_transaction_id: String,
        account_id: String,
        instrument_id: Option<String>,
        currency: String,
        transaction_type: TransactionKindWire,
        amount: String,
        quantity: Option<String>,
        occurred_at_unix_nanos: String,
        lot_method: Option<LotMethodWire>,
    },
    SuppliedTotals {
        account_id: String,
        currency: String,
        cash: Option<String>,
        market_value: Option<String>,
        cost_basis: Option<String>,
        absolute_tolerance: String,
        as_of_unix_nanos: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, tag = "status", rename_all = "snake_case")]
pub(crate) enum BasisWire {
    Resolved {
        amount: String,
        lot_method: LotMethodWire,
    },
    Missing,
    Ambiguous {
        candidate_amounts: Vec<String>,
        lot_method: LotMethodWire,
    },
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LotMethodWire {
    Fifo,
    Lifo,
    SpecificIdentification,
    AverageCost,
}

impl From<LotMethodWire> for LotMethod {
    fn from(value: LotMethodWire) -> Self {
        match value {
            LotMethodWire::Fifo => Self::Fifo,
            LotMethodWire::Lifo => Self::Lifo,
            LotMethodWire::SpecificIdentification => Self::SpecificIdentification,
            LotMethodWire::AverageCost => Self::AverageCost,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TransactionKindWire {
    Trade,
    CashTransfer,
    Income,
    Fee,
    CorporateAction,
}

impl From<TransactionKindWire> for TransactionKind {
    fn from(value: TransactionKindWire) -> Self {
        match value {
            TransactionKindWire::Trade => Self::Trade,
            TransactionKindWire::CashTransfer => Self::CashTransfer,
            TransactionKindWire::Income => Self::Income,
            TransactionKindWire::Fee => Self::Fee,
            TransactionKindWire::CorporateAction => Self::CorporateAction,
        }
    }
}
