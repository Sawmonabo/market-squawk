//! Checked raw-record normalization and exact portfolio reconciliation orchestration.

use std::collections::BTreeMap;
use std::str::FromStr;

use market_squawk_domain::{
    AccountId, Currency, DataQuality, InstrumentId, LotSize, Money, NormalizedPortfolioLotMethod,
    NormalizedPortfolioTransactionClass, NormalizedPortfolioTransactionEvidence,
    NormalizedPortfolioTransactionEvidenceInput, PositionObservation, PositionSide, QuantityLots,
    ResearchObservation, RevisionNumber, SourceId, SourceIdentifier, Timestamp,
    TransactionObservation,
};
use market_squawk_sources::ExtractionBatch;
use rust_decimal::Decimal;

use crate::archive::{AccountBinding, ActiveAccountMap, RawPortfolioRecord};
use crate::canonical::{CanonicalObservation, push_scalar, research_context};
use crate::wire::{BasisWire, RawEnvelopeWire, RawRecordWire};
use crate::{
    AccountObservation, BasisResolution, CalculatedTotals, CashFlowKind, CashFlowObservation,
    CostBasisObservation, HoldingObservation, LotMethod, PortfolioImportError,
    PortfolioImportLimits, PortfolioTransaction, ReconciliationDiscrepancy, ReconciliationLimits,
    ReconciliationTolerance, SignedQuantity, SuppliedTotals, TransactionKind, reconcile_totals,
};

const DATASET_ACCOUNTS: &str = "portfolio-accounts";
const DATASET_HOLDINGS: &str = "portfolio-holdings";
const DATASET_TRANSACTIONS: &str = "portfolio-transactions";
const DATASET_TOTALS: &str = "portfolio-supplied-totals";

pub(crate) struct ParsedRecordState {
    pub(crate) record_id: SourceIdentifier,
    pub(crate) revision: SourceIdentifier,
    pub(crate) revision_number: RevisionNumber,
    pub(crate) supersedes_revision: Option<SourceIdentifier>,
    pub(crate) source_reference: SourceIdentifier,
    pub(crate) account_binding: Option<AccountBinding>,
    pub(crate) dependent_account_binding: Option<AccountBinding>,
    pub(crate) broker_account_id: Option<AccountId>,
    pub(crate) broker_transaction_id: Option<SourceIdentifier>,
}

pub(crate) struct NormalizedImport {
    pub(crate) states: Vec<ParsedRecordState>,
    pub(crate) accounts: Vec<AccountObservation>,
    pub(crate) holdings: Vec<HoldingObservation>,
    pub(crate) transactions: Vec<PortfolioTransaction>,
    pub(crate) transaction_evidence: Vec<NormalizedPortfolioTransactionEvidence>,
    pub(crate) cash_flows: Vec<CashFlowObservation>,
    pub(crate) cost_bases: Vec<CostBasisObservation>,
    pub(crate) supplied_totals: Vec<SuppliedTotals>,
    pub(crate) canonical: Vec<CanonicalObservation>,
}

pub(crate) fn normalize_batch(
    batch: &ExtractionBatch,
    batch_raw: &[RawPortfolioRecord],
    source_id: SourceId,
    quality: DataQuality,
    limits: PortfolioImportLimits,
) -> Result<NormalizedImport, PortfolioImportError> {
    if batch.records().len() != batch_raw.len() {
        return Err(PortfolioImportError::CorruptArchive);
    }
    let mut normalized = NormalizedImport {
        states: Vec::new(),
        accounts: Vec::new(),
        holdings: Vec::new(),
        transactions: Vec::new(),
        transaction_evidence: Vec::new(),
        cash_flows: Vec::new(),
        cost_bases: Vec::new(),
        supplied_totals: Vec::new(),
        canonical: Vec::new(),
    };

    for (input_index, (record, raw)) in batch.records().iter().zip(batch_raw.iter()).enumerate() {
        let wire: RawEnvelopeWire = serde_json::from_slice(record.payload())
            .map_err(|_| PortfolioImportError::InvalidRecord)?;
        let record_id = identifier(&wire.record_id)?;
        let supersedes_revision = wire
            .supersedes_revision
            .as_deref()
            .map(identifier)
            .transpose()?;
        let revision_number = RevisionNumber::new(wire.revision_number)
            .map_err(|_| PortfolioImportError::InvalidRecord)?;
        let received_at = timestamp(&wire.received_at_unix_nanos)?;
        let ingested_at = timestamp(&wire.ingested_at_unix_nanos)?;
        let mut state = ParsedRecordState {
            record_id: record_id.clone(),
            revision: record.revision().clone(),
            revision_number,
            supersedes_revision,
            source_reference: raw.source_reference().clone(),
            account_binding: None,
            dependent_account_binding: None,
            broker_account_id: None,
            broker_transaction_id: None,
        };

        match wire.record {
            RawRecordWire::Account {
                account_id,
                currency,
                cash_balance,
                as_of_unix_nanos,
            } => {
                let account_id = account(&account_id)?;
                let currency = parse_currency(&currency)?;
                let cash_balance = money(&cash_balance, currency)?;
                let as_of = timestamp(&as_of_unix_nanos)?;
                state.account_binding = Some(AccountBinding {
                    account_id,
                    currency,
                });
                let context = research_context(
                    record,
                    raw,
                    &record_id,
                    source_id.clone(),
                    quality,
                    None,
                    Some(as_of),
                    received_at,
                    ingested_at,
                    revision_number,
                )?;
                normalized.accounts.push(AccountObservation::new(
                    account_id,
                    currency,
                    cash_balance,
                    as_of,
                    raw.source_reference().clone(),
                ));
                push_scalar(
                    &mut normalized.canonical,
                    input_index,
                    context,
                    DATASET_ACCOUNTS,
                    "cash_balance",
                    cash_balance.amount(),
                    Some(currency.as_str()),
                )?;
            }
            RawRecordWire::Holding {
                account_id,
                instrument_id,
                currency,
                quantity,
                lot_size,
                market_value,
                as_of_unix_nanos,
                cost_basis,
            } => {
                let account_id = account(&account_id)?;
                let instrument_id = instrument(&instrument_id)?;
                let currency = parse_currency(&currency)?;
                state.dependent_account_binding = Some(AccountBinding {
                    account_id,
                    currency,
                });
                let quantity = SignedQuantity::try_new(decimal(&quantity)?)?;
                let lot_size = LotSize::try_from_decimal(decimal(&lot_size)?)
                    .map_err(|_| PortfolioImportError::InvalidLotSize)?;
                let market_value = money(&market_value, currency)?;
                let as_of = timestamp(&as_of_unix_nanos)?;
                let context = research_context(
                    record,
                    raw,
                    &record_id,
                    source_id.clone(),
                    quality,
                    Some(instrument_id),
                    Some(as_of),
                    received_at,
                    ingested_at,
                    revision_number,
                )?;
                let basis = basis_resolution(
                    cost_basis,
                    account_id,
                    instrument_id,
                    currency,
                    raw.source_reference(),
                    limits,
                )?;
                if let BasisResolution::Resolved { observation } = &basis {
                    normalized.cost_bases.push(observation.clone());
                }
                let side = if quantity.as_decimal().is_sign_negative() {
                    PositionSide::Short
                } else {
                    PositionSide::Long
                };
                let absolute_quantity =
                    QuantityLots::try_from_decimal(quantity.absolute(), lot_size)
                        .map_err(|_| PortfolioImportError::InvalidLotSize)?;
                let position = PositionObservation::new(
                    context.clone(),
                    identifier(&account_id.to_string())?,
                    side,
                    absolute_quantity,
                )
                .map_err(|_| PortfolioImportError::ExtractionContract)?;
                normalized.canonical.push(CanonicalObservation {
                    input_index,
                    observation: ResearchObservation::PortfolioPosition(position),
                });
                push_scalar(
                    &mut normalized.canonical,
                    input_index,
                    context.clone(),
                    DATASET_HOLDINGS,
                    "market_value",
                    market_value.amount(),
                    Some(currency.as_str()),
                )?;
                if let BasisResolution::Resolved { observation } = &basis {
                    push_scalar(
                        &mut normalized.canonical,
                        input_index,
                        context,
                        DATASET_HOLDINGS,
                        "cost_basis",
                        observation.amount().amount(),
                        Some(currency.as_str()),
                    )?;
                }
                normalized.holdings.push(HoldingObservation::new(
                    account_id,
                    instrument_id,
                    currency,
                    quantity,
                    lot_size,
                    market_value,
                    as_of,
                    basis,
                    raw.source_reference().clone(),
                ));
            }
            RawRecordWire::Transaction {
                broker_transaction_id,
                account_id,
                instrument_id,
                currency,
                transaction_type,
                amount,
                quantity,
                occurred_at_unix_nanos,
                lot_method,
            } => {
                let broker_transaction_id = identifier(&broker_transaction_id)?;
                let account_id = account(&account_id)?;
                let instrument_id = instrument_id.as_deref().map(instrument).transpose()?;
                let currency = parse_currency(&currency)?;
                state.dependent_account_binding = Some(AccountBinding {
                    account_id,
                    currency,
                });
                let kind = TransactionKind::from(transaction_type);
                let amount = money(&amount, currency)?;
                let quantity = quantity
                    .as_deref()
                    .map(decimal)
                    .transpose()?
                    .map(SignedQuantity::try_new)
                    .transpose()?;
                let occurred_at = timestamp(&occurred_at_unix_nanos)?;
                let lot_method = lot_method.map(LotMethod::from);
                validate_transaction(kind, instrument_id, quantity, lot_method)?;
                let context = research_context(
                    record,
                    raw,
                    &record_id,
                    source_id.clone(),
                    quality,
                    instrument_id,
                    Some(occurred_at),
                    received_at,
                    ingested_at,
                    revision_number,
                )?;
                let canonical_transaction = TransactionObservation::new(
                    context.clone(),
                    identifier(&account_id.to_string())?,
                    transaction_kind_identifier(kind)?,
                    broker_transaction_id.clone(),
                );
                normalized.canonical.push(CanonicalObservation {
                    input_index,
                    observation: ResearchObservation::Transaction(canonical_transaction),
                });
                push_scalar(
                    &mut normalized.canonical,
                    input_index,
                    context.clone(),
                    DATASET_TRANSACTIONS,
                    "amount",
                    amount.amount(),
                    Some(currency.as_str()),
                )?;
                if let Some(quantity) = quantity {
                    push_scalar(
                        &mut normalized.canonical,
                        input_index,
                        context,
                        DATASET_TRANSACTIONS,
                        "quantity",
                        quantity.as_decimal(),
                        None,
                    )?;
                }
                if let Some(flow_kind) = cash_flow_kind(kind) {
                    normalized.cash_flows.push(CashFlowObservation::new(
                        account_id,
                        instrument_id,
                        flow_kind,
                        amount,
                        occurred_at,
                        raw.source_reference().clone(),
                    ));
                }
                normalized.transactions.push(PortfolioTransaction::new(
                    broker_transaction_id.clone(),
                    account_id,
                    instrument_id,
                    kind,
                    amount,
                    quantity,
                    occurred_at,
                    lot_method,
                    raw.source_reference().clone(),
                ));
                normalized.transaction_evidence.push(
                    NormalizedPortfolioTransactionEvidence::try_new(
                        NormalizedPortfolioTransactionEvidenceInput {
                            source_id: source_id.clone(),
                            logical_record_id: record_id.clone(),
                            source_revision: record.revision().clone(),
                            supersedes_source_revision: state.supersedes_revision.clone(),
                            revision: revision_number,
                            raw_source_reference: raw.source_reference().clone(),
                            raw_payload_digest: raw.payload_hash(),
                            broker_transaction_id: broker_transaction_id.clone(),
                            account_id,
                            instrument_id,
                            classification: normalized_transaction_class(kind),
                            amount,
                            quantity: quantity.map(SignedQuantity::as_decimal),
                            occurred_at,
                            lot_method: lot_method.map(normalized_lot_method),
                        },
                    )
                    .map_err(|_| PortfolioImportError::InvalidTransaction)?,
                );
                state.broker_account_id = Some(account_id);
                state.broker_transaction_id = Some(broker_transaction_id);
            }
            RawRecordWire::SuppliedTotals {
                account_id,
                currency,
                cash,
                market_value,
                cost_basis,
                absolute_tolerance,
                as_of_unix_nanos,
            } => {
                let account_id = account(&account_id)?;
                let currency = parse_currency(&currency)?;
                state.dependent_account_binding = Some(AccountBinding {
                    account_id,
                    currency,
                });
                let cash = optional_money(cash.as_deref(), currency)?;
                let market_value = optional_money(market_value.as_deref(), currency)?;
                let cost_basis = optional_money(cost_basis.as_deref(), currency)?;
                let tolerance =
                    ReconciliationTolerance::try_absolute(money(&absolute_tolerance, currency)?)?;
                let as_of = timestamp(&as_of_unix_nanos)?;
                let context = research_context(
                    record,
                    raw,
                    &record_id,
                    source_id.clone(),
                    quality,
                    None,
                    Some(as_of),
                    received_at,
                    ingested_at,
                    revision_number,
                )?;
                let totals = SuppliedTotals::try_new(
                    account_id,
                    currency,
                    cash,
                    market_value,
                    cost_basis,
                    tolerance,
                    raw.source_reference().clone(),
                )?;
                for (field, value) in [
                    ("cash", totals.cash()),
                    ("market_value", totals.market_value()),
                    ("cost_basis", totals.cost_basis()),
                ] {
                    if let Some(value) = value {
                        push_scalar(
                            &mut normalized.canonical,
                            input_index,
                            context.clone(),
                            DATASET_TOTALS,
                            field,
                            value.amount(),
                            Some(currency.as_str()),
                        )?;
                    }
                }
                normalized.supplied_totals.push(totals);
            }
        }
        normalized.states.push(state);
    }
    Ok(normalized)
}

fn basis_resolution(
    wire: BasisWire,
    account_id: AccountId,
    instrument_id: InstrumentId,
    currency: Currency,
    source_reference: &SourceIdentifier,
    limits: PortfolioImportLimits,
) -> Result<BasisResolution, PortfolioImportError> {
    match wire {
        BasisWire::Resolved { amount, lot_method } => {
            let amount = money(&amount, currency)?;
            if amount.amount().is_sign_negative() {
                return Err(PortfolioImportError::InvalidCostBasis);
            }
            Ok(BasisResolution::Resolved {
                observation: CostBasisObservation::new(
                    account_id,
                    instrument_id,
                    amount,
                    lot_method.into(),
                    source_reference.clone(),
                ),
            })
        }
        BasisWire::Missing => Ok(BasisResolution::Missing),
        BasisWire::Ambiguous {
            candidate_amounts,
            lot_method,
        } => {
            if candidate_amounts.len() < 2 {
                return Err(PortfolioImportError::InvalidCostBasis);
            }
            if candidate_amounts.len() > limits.max_basis_candidates {
                return Err(PortfolioImportError::BasisCandidateLimitExceeded {
                    max: limits.max_basis_candidates,
                });
            }
            let candidates = candidate_amounts
                .iter()
                .map(|value| money(value, currency))
                .collect::<Result<Vec<_>, _>>()?;
            if candidates
                .iter()
                .any(|candidate| candidate.amount().is_sign_negative())
            {
                return Err(PortfolioImportError::InvalidCostBasis);
            }
            Ok(BasisResolution::Ambiguous {
                candidates,
                lot_method: lot_method.into(),
            })
        }
    }
}

fn validate_transaction(
    kind: TransactionKind,
    instrument_id: Option<InstrumentId>,
    quantity: Option<SignedQuantity>,
    lot_method: Option<LotMethod>,
) -> Result<(), PortfolioImportError> {
    match kind {
        TransactionKind::Trade => {
            if instrument_id.is_none() {
                return Err(PortfolioImportError::MissingInstrument);
            }
            if quantity.is_none() {
                return Err(PortfolioImportError::MissingQuantity);
            }
            if lot_method.is_none() {
                return Err(PortfolioImportError::MissingLotMethod);
            }
        }
        TransactionKind::CashTransfer | TransactionKind::Income | TransactionKind::Fee => {
            if quantity.is_some() {
                return Err(PortfolioImportError::InvalidTransaction);
            }
            if lot_method.is_some() {
                return Err(PortfolioImportError::UnexpectedLotMethod);
            }
        }
        TransactionKind::CorporateAction => {
            if instrument_id.is_none() {
                return Err(PortfolioImportError::MissingInstrument);
            }
            if lot_method.is_some() {
                return Err(PortfolioImportError::UnexpectedLotMethod);
            }
        }
    }
    Ok(())
}

fn transaction_kind_identifier(
    kind: TransactionKind,
) -> Result<SourceIdentifier, PortfolioImportError> {
    identifier(match kind {
        TransactionKind::Trade => "trade",
        TransactionKind::CashTransfer => "cash_transfer",
        TransactionKind::Income => "income",
        TransactionKind::Fee => "fee",
        TransactionKind::CorporateAction => "corporate_action",
    })
}

const fn normalized_transaction_class(
    kind: TransactionKind,
) -> NormalizedPortfolioTransactionClass {
    match kind {
        TransactionKind::Trade => NormalizedPortfolioTransactionClass::Trade,
        TransactionKind::CashTransfer => NormalizedPortfolioTransactionClass::CashTransfer,
        TransactionKind::Income => NormalizedPortfolioTransactionClass::Income,
        TransactionKind::Fee => NormalizedPortfolioTransactionClass::Fee,
        TransactionKind::CorporateAction => NormalizedPortfolioTransactionClass::CorporateAction,
    }
}

const fn normalized_lot_method(method: LotMethod) -> NormalizedPortfolioLotMethod {
    match method {
        LotMethod::Fifo => NormalizedPortfolioLotMethod::Fifo,
        LotMethod::Lifo => NormalizedPortfolioLotMethod::Lifo,
        LotMethod::SpecificIdentification => NormalizedPortfolioLotMethod::SpecificIdentification,
        LotMethod::AverageCost => NormalizedPortfolioLotMethod::AverageCost,
    }
}

const fn cash_flow_kind(kind: TransactionKind) -> Option<CashFlowKind> {
    match kind {
        TransactionKind::CashTransfer => Some(CashFlowKind::Transfer),
        TransactionKind::Income => Some(CashFlowKind::Income),
        TransactionKind::Fee => Some(CashFlowKind::Fee),
        TransactionKind::Trade | TransactionKind::CorporateAction => None,
    }
}

pub(crate) fn reconcile_import(
    normalized: &NormalizedImport,
    active_accounts: &ActiveAccountMap,
    limits: PortfolioImportLimits,
) -> Result<Vec<ReconciliationDiscrepancy>, PortfolioImportError> {
    struct AccountAggregate {
        currency: Currency,
        cash: Money,
        market_value: Money,
        cost_basis: Money,
        unresolved_basis: bool,
    }

    let mut aggregates = BTreeMap::new();
    for account in &normalized.accounts {
        let authority = active_accounts
            .get(&account.account_id())
            .ok_or(PortfolioImportError::AccountMismatch)?;
        if authority.currency != account.currency() {
            return Err(PortfolioImportError::CurrencyMismatch);
        }
        if aggregates
            .insert(
                account.account_id(),
                AccountAggregate {
                    currency: account.currency(),
                    cash: account.cash_balance(),
                    market_value: Money::new(Decimal::ZERO, account.currency()),
                    cost_basis: Money::new(Decimal::ZERO, account.currency()),
                    unresolved_basis: false,
                },
            )
            .is_some()
        {
            return Err(PortfolioImportError::DuplicateAccountObservation);
        }
    }
    for holding in &normalized.holdings {
        let authority = active_accounts
            .get(&holding.account_id())
            .ok_or(PortfolioImportError::AccountMismatch)?;
        if authority.currency != holding.currency() {
            return Err(PortfolioImportError::CurrencyMismatch);
        }
        let Some(aggregate) = aggregates.get_mut(&holding.account_id()) else {
            continue;
        };
        aggregate.market_value = aggregate
            .market_value
            .checked_add(holding.market_value())
            .map_err(|_| PortfolioImportError::Arithmetic)?;
        match holding.basis() {
            BasisResolution::Resolved { observation } => {
                aggregate.cost_basis = aggregate
                    .cost_basis
                    .checked_add(observation.amount())
                    .map_err(|_| PortfolioImportError::Arithmetic)?;
            }
            BasisResolution::Missing | BasisResolution::Ambiguous { .. } => {
                aggregate.unresolved_basis = true;
            }
        }
    }
    for transaction in &normalized.transactions {
        let authority = active_accounts
            .get(&transaction.account_id())
            .ok_or(PortfolioImportError::AccountMismatch)?;
        if authority.currency != transaction.amount().currency() {
            return Err(PortfolioImportError::CurrencyMismatch);
        }
    }

    let reconciliation_limits = ReconciliationLimits::try_new(limits.max_discrepancies)?;
    let mut discrepancies = Vec::new();
    for supplied in &normalized.supplied_totals {
        let authority = active_accounts
            .get(&supplied.account_id())
            .ok_or(PortfolioImportError::AccountMismatch)?;
        if authority.currency != supplied.currency() {
            return Err(PortfolioImportError::CurrencyMismatch);
        }
        let aggregate = aggregates
            .get(&supplied.account_id())
            .ok_or(PortfolioImportError::AccountMismatch)?;
        let calculated = CalculatedTotals::try_new(
            supplied.account_id(),
            aggregate.currency,
            Some(aggregate.cash),
            Some(aggregate.market_value),
            (!aggregate.unresolved_basis).then_some(aggregate.cost_basis),
        )?;
        let additions = reconcile_totals(supplied, &calculated, reconciliation_limits)?;
        if discrepancies.len().saturating_add(additions.len()) > limits.max_discrepancies {
            return Err(PortfolioImportError::DiscrepancyLimitExceeded {
                max: limits.max_discrepancies,
            });
        }
        discrepancies.extend(additions);
    }
    Ok(discrepancies)
}

fn account(value: &str) -> Result<AccountId, PortfolioImportError> {
    AccountId::from_str(value).map_err(|_| PortfolioImportError::InvalidAccount)
}

fn instrument(value: &str) -> Result<InstrumentId, PortfolioImportError> {
    InstrumentId::from_str(value).map_err(|_| PortfolioImportError::InvalidInstrument)
}

fn identifier(value: &str) -> Result<SourceIdentifier, PortfolioImportError> {
    SourceIdentifier::try_from(value).map_err(|_| PortfolioImportError::InvalidRecord)
}

fn parse_currency(value: &str) -> Result<Currency, PortfolioImportError> {
    Currency::try_from(value).map_err(|_| PortfolioImportError::InvalidCurrency)
}

fn decimal(value: &str) -> Result<Decimal, PortfolioImportError> {
    Decimal::from_str_exact(value).map_err(|_| PortfolioImportError::InvalidDecimal)
}

fn timestamp(value: &str) -> Result<Timestamp, PortfolioImportError> {
    value
        .parse::<i64>()
        .map(Timestamp::from_unix_nanos)
        .map_err(|_| PortfolioImportError::InvalidTimestamp)
}

fn money(value: &str, currency: Currency) -> Result<Money, PortfolioImportError> {
    decimal(value).map(|amount| Money::new(amount, currency))
}

fn optional_money(
    value: Option<&str>,
    currency: Currency,
) -> Result<Option<Money>, PortfolioImportError> {
    value.map(|value| money(value, currency)).transpose()
}
