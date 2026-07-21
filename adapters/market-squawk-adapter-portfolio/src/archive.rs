//! Crash-safe raw archive, active revision authority, and portfolio source lifecycle.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use bytes::Bytes;
use market_squawk_domain::{
    AccountId, Currency, DataQuality, EvidenceDigest, MetadataRevision, RevisionNumber, SourceId,
    SourceIdentifier,
};
use market_squawk_platform::{LocalAuthorityStateStore, SecretReference};
use market_squawk_sources::{ExtractionBatch, ExtractionRecord};
use serde::{Deserialize, Serialize};

use crate::canonical::build_canonical_batch;
use crate::normalize::{NormalizedImport, ParsedRecordState, normalize_batch, reconcile_import};
use crate::raw::{raw_source_reference, validate_raw_capacity, validate_raw_record};
use crate::{
    AccountObservation, CashFlowObservation, CostBasisObservation, HoldingObservation,
    PortfolioImportError, PortfolioImportLimits, PortfolioTransaction, ReconciliationDiscrepancy,
    SuppliedTotals,
};

const ARCHIVE_SCHEMA_VERSION: u16 = 2;

/// One exact source record durably retained before interpretation.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RawPortfolioRecord {
    source_reference: SourceIdentifier,
    record: ExtractionRecord,
}

impl RawPortfolioRecord {
    /// Returns the immutable raw evidence identity.
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }

    /// Returns the exact original payload bytes.
    pub fn bytes(&self) -> &Bytes {
        self.record.payload()
    }

    /// Returns the exact original payload digest.
    pub fn payload_hash(&self) -> EvidenceDigest {
        self.record.evidence().content_digest()
    }

    /// Returns the generic extraction lineage retained with the bytes.
    pub const fn extraction_record(&self) -> &ExtractionRecord {
        &self.record
    }
}

impl fmt::Debug for RawPortfolioRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawPortfolioRecord")
            .field("source_reference", &"[REDACTED]")
            .field("payload", &"[REDACTED]")
            .finish()
    }
}

/// Active logical source-record state retained across process restarts.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActivePortfolioRecord {
    record_id: SourceIdentifier,
    revision: SourceIdentifier,
    revision_number: RevisionNumber,
    source_reference: SourceIdentifier,
    account_binding: Option<AccountBinding>,
    dependent_account_binding: Option<AccountBinding>,
    broker_account_id: Option<AccountId>,
    broker_transaction_id: Option<SourceIdentifier>,
}

impl fmt::Debug for ActivePortfolioRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActivePortfolioRecord")
            .field("record_id", &"[REDACTED]")
            .field("revision", &self.revision)
            .field("source_reference", &"[REDACTED]")
            .field("account_identity", &"[REDACTED]")
            .field("broker_identity", &"[REDACTED]")
            .finish()
    }
}

impl ActivePortfolioRecord {
    /// Returns the stable logical record identity.
    pub const fn record_id(&self) -> &SourceIdentifier {
        &self.record_id
    }

    /// Returns the active source revision.
    pub const fn revision(&self) -> &SourceIdentifier {
        &self.revision
    }

    /// Returns the checked active canonical revision number.
    pub const fn revision_number(&self) -> RevisionNumber {
        self.revision_number
    }

    /// Returns the immutable exact raw record reference.
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}

/// Whether an import changed active logical portfolio state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportDisposition {
    /// At least one new logical record or correction became active.
    Applied,
    /// Every record exactly replayed already-active evidence.
    Replay,
}

/// One fully normalized portfolio import, including exact raw and canonical data-lane output.
pub struct PortfolioImport {
    disposition: ImportDisposition,
    raw_records: Vec<RawPortfolioRecord>,
    accounts: Vec<AccountObservation>,
    holdings: Vec<HoldingObservation>,
    transactions: Vec<PortfolioTransaction>,
    cash_flows: Vec<CashFlowObservation>,
    cost_bases: Vec<CostBasisObservation>,
    supplied_totals: Vec<SuppliedTotals>,
    discrepancies: Vec<ReconciliationDiscrepancy>,
    normalized_batch: ExtractionBatch,
}

impl PortfolioImport {
    /// Returns whether active durable state changed.
    pub const fn disposition(&self) -> ImportDisposition {
        self.disposition
    }
    /// Returns exact input records in source order.
    pub fn raw_records(&self) -> &[RawPortfolioRecord] {
        &self.raw_records
    }
    /// Returns checked account observations in source order.
    pub fn accounts(&self) -> &[AccountObservation] {
        &self.accounts
    }
    /// Returns checked holding observations in source order.
    pub fn holdings(&self) -> &[HoldingObservation] {
        &self.holdings
    }
    /// Returns checked transactions in source order.
    pub fn transactions(&self) -> &[PortfolioTransaction] {
        &self.transactions
    }
    /// Returns cash flows derived exactly from transaction amounts.
    pub fn cash_flows(&self) -> &[CashFlowObservation] {
        &self.cash_flows
    }
    /// Returns only fully resolved cost-basis observations.
    pub fn cost_bases(&self) -> &[CostBasisObservation] {
        &self.cost_bases
    }
    /// Returns exact source-supplied totals.
    pub fn supplied_totals(&self) -> &[SuppliedTotals] {
        &self.supplied_totals
    }
    /// Returns exact source/calculated mismatches outside declared tolerance.
    pub fn discrepancies(&self) -> &[ReconciliationDiscrepancy] {
        &self.discrepancies
    }
    /// Returns canonical research records accepted directly by the data lane.
    pub const fn normalized_batch(&self) -> &ExtractionBatch {
        &self.normalized_batch
    }
}

impl fmt::Debug for PortfolioImport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortfolioImport")
            .field("disposition", &self.disposition)
            .field("raw_records", &self.raw_records.len())
            .field("accounts", &self.accounts.len())
            .field("holdings", &self.holdings.len())
            .field("transactions", &self.transactions.len())
            .field("discrepancies", &self.discrepancies.len())
            .field("payloads", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct AccountBinding {
    pub(crate) account_id: AccountId,
    pub(crate) currency: Currency,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActiveAccountAuthority {
    pub(crate) currency: Currency,
    record_id: SourceIdentifier,
}

pub(crate) type ActiveAccountMap = BTreeMap<AccountId, ActiveAccountAuthority>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SupersessionLink {
    prior: SourceIdentifier,
    successor: SourceIdentifier,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableArchive {
    schema_version: u16,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    raw_records: Vec<RawPortfolioRecord>,
    active_records: Vec<ActivePortfolioRecord>,
    supersessions: Vec<SupersessionLink>,
}

/// Stateful local portfolio extraction source with crash-safe raw authority.
pub struct PortfolioExtractionSource {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    quality: DataQuality,
    archive: LocalAuthorityStateStore,
    _credential: Option<SecretReference>,
    limits: PortfolioImportLimits,
    raw_records: Vec<RawPortfolioRecord>,
    raw_by_reference: BTreeMap<SourceIdentifier, usize>,
    active_records: BTreeMap<SourceIdentifier, ActivePortfolioRecord>,
    active_accounts: ActiveAccountMap,
    active_broker_ids: BTreeMap<(AccountId, SourceIdentifier), SourceIdentifier>,
    superseded_by: BTreeMap<SourceIdentifier, SourceIdentifier>,
    durability_failed: bool,
}

impl fmt::Debug for PortfolioExtractionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortfolioExtractionSource")
            .field("source_id", &self.source_id)
            .field("metadata_revision", &self.metadata_revision)
            .field("quality", &self.quality)
            .field("archive", &"[REDACTED]")
            .field("credential", &"[REDACTED]")
            .field("raw_record_count", &self.raw_records.len())
            .field("active_record_count", &self.active_records.len())
            .finish()
    }
}

impl PortfolioExtractionSource {
    /// Opens a source and validates any existing crash-safe raw and active state.
    ///
    /// # Errors
    ///
    /// Fails closed when durable state cannot be loaded, decoded, or bound to this source.
    pub fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        quality: DataQuality,
        archive: LocalAuthorityStateStore,
        credential: Option<SecretReference>,
        limits: PortfolioImportLimits,
    ) -> Result<Self, PortfolioImportError> {
        let loaded = archive
            .load()
            .map_err(|_| PortfolioImportError::ArchiveUnavailable)?;
        let mut source = Self {
            source_id,
            metadata_revision,
            quality,
            archive,
            _credential: credential,
            limits,
            raw_records: Vec::new(),
            raw_by_reference: BTreeMap::new(),
            active_records: BTreeMap::new(),
            active_accounts: BTreeMap::new(),
            active_broker_ids: BTreeMap::new(),
            superseded_by: BTreeMap::new(),
            durability_failed: false,
        };
        if let Some(payload) = loaded {
            let durable: DurableArchive = serde_json::from_slice(&payload)
                .map_err(|_| PortfolioImportError::CorruptArchive)?;
            source.restore(durable)?;
        }
        Ok(source)
    }

    /// Imports a generic extraction batch after durably committing exact raw evidence.
    ///
    /// # Errors
    ///
    /// Fails closed on lineage, evidence, capacity, normalization, authority, or durability errors.
    pub fn import_batch(
        &mut self,
        batch: &ExtractionBatch,
    ) -> Result<PortfolioImport, PortfolioImportError> {
        if self.durability_failed {
            return Err(PortfolioImportError::ArchiveUnavailable);
        }
        self.validate_batch_binding(batch)?;

        let mut candidate_raw = self.raw_records.clone();
        let mut candidate_index = self.raw_by_reference.clone();
        let mut batch_raw = Vec::new();
        let mut raw_changed = false;
        for record in batch.records() {
            validate_raw_record(record)?;
            let source_reference = raw_source_reference(record)?;
            if let Some(index) = candidate_index.get(&source_reference).copied() {
                batch_raw.push(candidate_raw[index].clone());
                continue;
            }
            if candidate_raw.len() >= self.limits.max_archive_records {
                return Err(PortfolioImportError::ArchiveRecordLimitExceeded {
                    max: self.limits.max_archive_records,
                });
            }
            let raw = RawPortfolioRecord {
                source_reference: source_reference.clone(),
                record: record.clone(),
            };
            let index = candidate_raw.len();
            candidate_raw.push(raw.clone());
            candidate_index.insert(source_reference, index);
            batch_raw.push(raw);
            raw_changed = true;
        }
        validate_raw_capacity(&candidate_raw, self.limits)?;
        if raw_changed {
            if let Err(error) =
                self.persist_state(&candidate_raw, &self.active_records, &self.superseded_by)
            {
                self.durability_failed = true;
                return Err(error);
            }
            self.raw_records = candidate_raw;
            self.raw_by_reference = candidate_index;
        }

        let mut normalized = normalize_batch(
            batch,
            &batch_raw,
            self.source_id.clone(),
            self.quality,
            self.limits,
        )?;
        let (disposition, active, accounts, brokers, supersessions) =
            self.propose_active_state(&normalized.states)?;
        let discrepancies = reconcile_import(&normalized, &accounts, self.limits)?;
        let canonical = std::mem::take(&mut normalized.canonical);
        let canonical_batch = build_canonical_batch(batch, canonical, self.limits)?;

        if disposition == ImportDisposition::Applied {
            if let Err(error) = self.persist_state(&self.raw_records, &active, &supersessions) {
                self.durability_failed = true;
                return Err(error);
            }
            self.active_records = active;
            self.active_accounts = accounts;
            self.active_broker_ids = brokers;
            self.superseded_by = supersessions;
        }

        Ok(portfolio_import(
            disposition,
            batch_raw,
            normalized,
            discrepancies,
            canonical_batch,
        ))
    }

    /// Returns every exact raw record retained by this source authority.
    pub fn raw_records(&self) -> &[RawPortfolioRecord] {
        &self.raw_records
    }

    /// Returns the active revision of one logical record.
    pub fn active_record(&self, record_id: &SourceIdentifier) -> Option<&ActivePortfolioRecord> {
        self.active_records.get(record_id)
    }

    /// Returns whether an immutable raw reference has a durable successor.
    pub fn is_superseded(&self, source_reference: &SourceIdentifier) -> bool {
        self.superseded_by.contains_key(source_reference)
    }

    fn validate_batch_binding(&self, batch: &ExtractionBatch) -> Result<(), PortfolioImportError> {
        let object = batch.request().object();
        if object.source_id() != &self.source_id
            || object.metadata_revision() != &self.metadata_revision
        {
            return Err(PortfolioImportError::SourceBindingMismatch);
        }
        Ok(())
    }

    fn restore(&mut self, durable: DurableArchive) -> Result<(), PortfolioImportError> {
        if durable.schema_version != ARCHIVE_SCHEMA_VERSION {
            return Err(PortfolioImportError::CorruptArchive);
        }
        if durable.source_id != self.source_id
            || durable.metadata_revision != self.metadata_revision
        {
            return Err(PortfolioImportError::SourceBindingMismatch);
        }
        validate_raw_capacity(&durable.raw_records, self.limits)
            .map_err(|_| PortfolioImportError::CorruptArchive)?;

        let mut raw_by_reference = BTreeMap::new();
        for (index, raw) in durable.raw_records.iter().enumerate() {
            validate_raw_record(&raw.record).map_err(|_| PortfolioImportError::CorruptArchive)?;
            if raw.record.source_id() != &self.source_id
                || raw.record.metadata_revision() != &self.metadata_revision
                || raw_source_reference(&raw.record)
                    .map_err(|_| PortfolioImportError::CorruptArchive)?
                    != raw.source_reference
                || raw_by_reference
                    .insert(raw.source_reference.clone(), index)
                    .is_some()
            {
                return Err(PortfolioImportError::CorruptArchive);
            }
        }

        let mut active_records = BTreeMap::new();
        let mut active_accounts = BTreeMap::new();
        let mut active_broker_ids = BTreeMap::new();
        for active in durable.active_records {
            if !raw_by_reference.contains_key(&active.source_reference)
                || active_records
                    .insert(active.record_id.clone(), active.clone())
                    .is_some()
            {
                return Err(PortfolioImportError::CorruptArchive);
            }
            if let Some(binding) = active.account_binding
                && active_accounts
                    .insert(
                        binding.account_id,
                        ActiveAccountAuthority {
                            currency: binding.currency,
                            record_id: active.record_id.clone(),
                        },
                    )
                    .is_some()
            {
                return Err(PortfolioImportError::CorruptArchive);
            }
            if let (Some(account_id), Some(broker_id)) = (
                active.broker_account_id,
                active.broker_transaction_id.clone(),
            ) && active_broker_ids
                .insert((account_id, broker_id), active.record_id.clone())
                .is_some()
            {
                return Err(PortfolioImportError::CorruptArchive);
            }
        }
        validate_active_account_bindings(&active_records, &active_accounts)
            .map_err(|_| PortfolioImportError::CorruptArchive)?;

        let mut superseded_by = BTreeMap::new();
        for link in durable.supersessions {
            if !raw_by_reference.contains_key(&link.prior)
                || !raw_by_reference.contains_key(&link.successor)
                || link.prior == link.successor
                || superseded_by.insert(link.prior, link.successor).is_some()
            {
                return Err(PortfolioImportError::CorruptArchive);
            }
        }
        self.raw_records = durable.raw_records;
        self.raw_by_reference = raw_by_reference;
        self.active_records = active_records;
        self.active_accounts = active_accounts;
        self.active_broker_ids = active_broker_ids;
        self.superseded_by = superseded_by;
        Ok(())
    }

    fn persist_state(
        &self,
        raw_records: &[RawPortfolioRecord],
        active_records: &BTreeMap<SourceIdentifier, ActivePortfolioRecord>,
        superseded_by: &BTreeMap<SourceIdentifier, SourceIdentifier>,
    ) -> Result<(), PortfolioImportError> {
        let durable = DurableArchive {
            schema_version: ARCHIVE_SCHEMA_VERSION,
            source_id: self.source_id.clone(),
            metadata_revision: self.metadata_revision.clone(),
            raw_records: raw_records.to_vec(),
            active_records: active_records.values().cloned().collect(),
            supersessions: superseded_by
                .iter()
                .map(|(prior, successor)| SupersessionLink {
                    prior: prior.clone(),
                    successor: successor.clone(),
                })
                .collect(),
        };
        let payload =
            serde_json::to_vec(&durable).map_err(|_| PortfolioImportError::ArchiveUnavailable)?;
        if payload.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
            return Err(PortfolioImportError::ArchiveByteLimitExceeded {
                max: self.limits.max_archive_bytes,
            });
        }
        self.archive
            .store(&payload)
            .map_err(|_| PortfolioImportError::ArchiveUnavailable)
    }

    fn propose_active_state(
        &self,
        states: &[ParsedRecordState],
    ) -> Result<ActiveProposal, PortfolioImportError> {
        let mut active = self.active_records.clone();
        let mut accounts = self.active_accounts.clone();
        let mut brokers = self.active_broker_ids.clone();
        let mut supersessions = self.superseded_by.clone();
        let mut seen = BTreeSet::new();
        let mut disposition = ImportDisposition::Replay;

        for state in states {
            if !seen.insert(state.record_id.clone()) {
                return Err(PortfolioImportError::DuplicateSourceRecordId);
            }
            if let Some(existing) = active.get(&state.record_id).cloned() {
                if existing.source_reference == state.source_reference {
                    if existing.revision != state.revision
                        || existing.revision_number != state.revision_number
                    {
                        return Err(PortfolioImportError::CorruptArchive);
                    }
                    continue;
                }
                if existing.revision == state.revision {
                    return Err(PortfolioImportError::ReplayConflict);
                }
                let supersedes = state
                    .supersedes_revision
                    .as_ref()
                    .ok_or(PortfolioImportError::SupersessionRequired)?;
                if supersedes != &existing.revision {
                    return Err(PortfolioImportError::SupersessionMismatch);
                }
                if state.revision_number.get() <= existing.revision_number.get() {
                    return Err(PortfolioImportError::NonIncreasingRevision);
                }
                remove_existing_authority(&existing, &mut accounts, &mut brokers)?;
                supersessions.insert(
                    existing.source_reference.clone(),
                    state.source_reference.clone(),
                );
            } else if state.supersedes_revision.is_some() {
                return Err(PortfolioImportError::SupersessionMismatch);
            }

            insert_account_authority(state, &mut accounts)?;
            insert_broker_authority(state, &mut brokers)?;
            active.insert(
                state.record_id.clone(),
                ActivePortfolioRecord {
                    record_id: state.record_id.clone(),
                    revision: state.revision.clone(),
                    revision_number: state.revision_number,
                    source_reference: state.source_reference.clone(),
                    account_binding: state.account_binding,
                    dependent_account_binding: state.dependent_account_binding,
                    broker_account_id: state.broker_account_id,
                    broker_transaction_id: state.broker_transaction_id.clone(),
                },
            );
            disposition = ImportDisposition::Applied;
        }
        validate_active_account_bindings(&active, &accounts)?;
        Ok((disposition, active, accounts, brokers, supersessions))
    }
}

type ActiveProposal = (
    ImportDisposition,
    BTreeMap<SourceIdentifier, ActivePortfolioRecord>,
    ActiveAccountMap,
    BTreeMap<(AccountId, SourceIdentifier), SourceIdentifier>,
    BTreeMap<SourceIdentifier, SourceIdentifier>,
);

fn remove_existing_authority(
    existing: &ActivePortfolioRecord,
    accounts: &mut ActiveAccountMap,
    brokers: &mut BTreeMap<(AccountId, SourceIdentifier), SourceIdentifier>,
) -> Result<(), PortfolioImportError> {
    if let Some(binding) = existing.account_binding {
        if !accounts
            .get(&binding.account_id)
            .is_some_and(|authority| authority.record_id == existing.record_id)
        {
            return Err(PortfolioImportError::CorruptArchive);
        }
        accounts.remove(&binding.account_id);
    }
    if let (Some(account_id), Some(broker_id)) = (
        existing.broker_account_id,
        existing.broker_transaction_id.as_ref(),
    ) {
        brokers.remove(&(account_id, broker_id.clone()));
    }
    Ok(())
}

fn insert_account_authority(
    state: &ParsedRecordState,
    accounts: &mut ActiveAccountMap,
) -> Result<(), PortfolioImportError> {
    let Some(binding) = state.account_binding else {
        return Ok(());
    };
    if accounts
        .get(&binding.account_id)
        .is_some_and(|authority| authority.record_id != state.record_id)
    {
        return Err(PortfolioImportError::DuplicateAccountObservation);
    }
    accounts.insert(
        binding.account_id,
        ActiveAccountAuthority {
            currency: binding.currency,
            record_id: state.record_id.clone(),
        },
    );
    Ok(())
}

fn insert_broker_authority(
    state: &ParsedRecordState,
    brokers: &mut BTreeMap<(AccountId, SourceIdentifier), SourceIdentifier>,
) -> Result<(), PortfolioImportError> {
    if let (Some(account_id), Some(broker_id)) =
        (state.broker_account_id, state.broker_transaction_id.clone())
        && brokers
            .insert((account_id, broker_id), state.record_id.clone())
            .is_some_and(|owner| owner != state.record_id)
    {
        return Err(PortfolioImportError::DuplicateBrokerTransactionId);
    }
    Ok(())
}

fn validate_active_account_bindings(
    active_records: &BTreeMap<SourceIdentifier, ActivePortfolioRecord>,
    active_accounts: &ActiveAccountMap,
) -> Result<(), PortfolioImportError> {
    for active in active_records.values() {
        let Some(dependent) = active.dependent_account_binding else {
            continue;
        };
        let authority = active_accounts
            .get(&dependent.account_id)
            .ok_or(PortfolioImportError::AccountMismatch)?;
        if authority.currency != dependent.currency {
            return Err(PortfolioImportError::CurrencyMismatch);
        }
    }
    Ok(())
}

fn portfolio_import(
    disposition: ImportDisposition,
    raw_records: Vec<RawPortfolioRecord>,
    normalized: NormalizedImport,
    discrepancies: Vec<ReconciliationDiscrepancy>,
    normalized_batch: ExtractionBatch,
) -> PortfolioImport {
    PortfolioImport {
        disposition,
        raw_records,
        accounts: normalized.accounts,
        holdings: normalized.holdings,
        transactions: normalized.transactions,
        cash_flows: normalized.cash_flows,
        cost_bases: normalized.cost_bases,
        supplied_totals: normalized.supplied_totals,
        discrepancies,
        normalized_batch,
    }
}
