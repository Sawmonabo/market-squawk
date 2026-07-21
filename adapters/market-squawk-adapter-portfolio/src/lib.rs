#![forbid(unsafe_code)]
//! Raw-preserving, checked local portfolio import and reconciliation.

mod holdings;
mod reconcile;
mod transactions;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use bytes::Bytes;
use market_squawk_domain::{
    AccountId, AlternativeDataObservation, Currency, DataQuality, DigestAlgorithm, EvidenceDigest,
    ExactPayloadEvidence, InstrumentId, LotSize, MetadataRevision, Money, PayloadHash,
    PayloadReference, PositionObservation, PositionSide, QuantityLots, ResearchContext,
    ResearchObservation, ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber,
    SourceId, SourceIdentifier, Timestamp, TransactionObservation,
};
use market_squawk_platform::{LocalAuthorityStateStore, SecretReference};
use market_squawk_sources::{
    AvailabilityEvidence as SourceAvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA,
    ExtractionBatch, ExtractionBatchAccumulator, ExtractionRecord, MAX_EXTRACTION_RECORDS,
    payload_matches_exact_evidence,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

pub use holdings::{
    AccountObservation, BasisResolution, CostBasisObservation, HoldingObservation, LotMethod,
    SignedQuantity,
};
pub use reconcile::{
    CalculatedTotals, ReconciliationDiscrepancy, ReconciliationField, ReconciliationLimits,
    ReconciliationTolerance, SuppliedTotals, reconcile_totals,
};
pub use transactions::{CashFlowKind, CashFlowObservation, PortfolioTransaction, TransactionKind};

const ARCHIVE_SCHEMA_VERSION: u16 = 1;
const RAW_RECORD_SCHEMA: &str = "market-squawk-portfolio-raw-v1";
const RAW_REFERENCE_PREFIX: &str = "portfolio-raw-";
const DATASET_ACCOUNTS: &str = "portfolio-accounts";
const DATASET_HOLDINGS: &str = "portfolio-holdings";
const DATASET_TRANSACTIONS: &str = "portfolio-transactions";
const DATASET_TOTALS: &str = "portfolio-supplied-totals";

/// Fail-closed portfolio import errors that never include source payloads or credentials.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PortfolioImportError {
    /// A caller-provided capacity bound is zero or exceeds a hard ceiling.
    #[error("portfolio import limits are invalid")]
    InvalidLimits,
    /// The durable archive or input batch belongs to another source authority.
    #[error("portfolio source binding does not match")]
    SourceBindingMismatch,
    /// An input record does not use the versioned portfolio raw schema.
    #[error("portfolio record schema is unsupported")]
    UnsupportedRecordSchema,
    /// Exact source bytes no longer match their retained digest evidence.
    #[error("portfolio raw payload evidence does not match")]
    RawEvidenceMismatch,
    /// The raw archive record-count bound would be exceeded.
    #[error("portfolio raw archive exceeds its record limit of {max}")]
    ArchiveRecordLimitExceeded {
        /// Configured record bound.
        max: usize,
    },
    /// The logical raw-byte bound would be exceeded.
    #[error("portfolio raw archive exceeds its byte limit of {max}")]
    ArchiveByteLimitExceeded {
        /// Configured byte bound.
        max: u64,
    },
    /// Crash-safe raw archive access or publication failed.
    #[error("portfolio raw archive is unavailable")]
    ArchiveUnavailable,
    /// Durable state is malformed, unsupported, or internally inconsistent.
    #[error("portfolio durable archive is corrupt or unsupported")]
    CorruptArchive,
    /// A raw record is not valid versioned portfolio JSON.
    #[error("portfolio record is invalid")]
    InvalidRecord,
    /// A stable account identifier is invalid.
    #[error("portfolio account identifier is invalid")]
    InvalidAccount,
    /// A stable instrument identifier is invalid.
    #[error("portfolio instrument identifier is invalid")]
    InvalidInstrument,
    /// A currency code is invalid.
    #[error("portfolio currency is invalid")]
    InvalidCurrency,
    /// An exact decimal field is invalid.
    #[error("portfolio decimal is invalid")]
    InvalidDecimal,
    /// A timestamp field is outside the supported signed Unix-nanosecond representation.
    #[error("portfolio timestamp is invalid")]
    InvalidTimestamp,
    /// A holding or supplied transaction quantity is zero.
    #[error("portfolio quantity must be nonzero")]
    ZeroQuantity,
    /// A holding lot size is not strictly positive.
    #[error("portfolio lot size is invalid")]
    InvalidLotSize,
    /// Cost basis is negative or otherwise invalid.
    #[error("portfolio cost basis is invalid")]
    InvalidCostBasis,
    /// An ambiguous basis record exceeds its configured candidate bound.
    #[error("portfolio basis candidates exceed the limit of {max}")]
    BasisCandidateLimitExceeded {
        /// Configured candidate bound.
        max: usize,
    },
    /// A transaction's fields do not match its closed classification.
    #[error("portfolio transaction fields are inconsistent")]
    InvalidTransaction,
    /// An instrument-scoped observation omitted its stable instrument identifier.
    #[error("portfolio observation requires an instrument")]
    MissingInstrument,
    /// A trade omitted its nonzero signed quantity.
    #[error("portfolio trade requires a quantity")]
    MissingQuantity,
    /// A trade omitted its explicit lot method.
    #[error("portfolio trade requires a lot method")]
    MissingLotMethod,
    /// A non-trade transaction supplied a lot method.
    #[error("portfolio non-trade transaction cannot supply a lot method")]
    UnexpectedLotMethod,
    /// Two input records claim the same logical source record identity.
    #[error("portfolio batch contains a duplicate source record identifier")]
    DuplicateSourceRecordId,
    /// Two account records claim the same stable account.
    #[error("portfolio batch contains a duplicate account observation")]
    DuplicateAccountObservation,
    /// Two active logical transactions claim one broker transaction identifier.
    #[error("portfolio broker transaction identifier is duplicated")]
    DuplicateBrokerTransactionId,
    /// A new revision did not identify the active revision it replaces.
    #[error("portfolio correction must explicitly supersede the active revision")]
    SupersessionRequired,
    /// A correction identifies a revision other than the active revision.
    #[error("portfolio correction supersession does not match active revision")]
    SupersessionMismatch,
    /// One revision identity was replayed with different source evidence.
    #[error("portfolio revision replay conflicts with archived evidence")]
    ReplayConflict,
    /// A holding, transaction, or total references an absent account or another account binding.
    #[error("portfolio account binding does not match")]
    AccountMismatch,
    /// Money or observations disagree on currency.
    #[error("portfolio currency binding does not match")]
    CurrencyMismatch,
    /// An absolute reconciliation tolerance is negative.
    #[error("portfolio reconciliation tolerance is invalid")]
    InvalidReconciliationTolerance,
    /// A supplied total has no independently calculated counterpart.
    #[error("calculated portfolio total is unavailable for {field:?}")]
    CalculatedTotalUnavailable {
        /// Total whose calculated counterpart is absent.
        field: ReconciliationField,
    },
    /// Exact decimal or bounded integer arithmetic overflowed.
    #[error("portfolio arithmetic overflow")]
    Arithmetic,
    /// Generated discrepancy output exceeds its configured bound.
    #[error("portfolio reconciliation discrepancies exceed the limit of {max}")]
    DiscrepancyLimitExceeded {
        /// Configured output bound.
        max: usize,
    },
    /// Generated canonical output exceeds its configured bound.
    #[error("portfolio normalized records exceed the limit of {max}")]
    NormalizedRecordLimitExceeded {
        /// Configured record bound.
        max: usize,
    },
    /// Canonical research or extraction lineage validation failed.
    #[error("portfolio normalized extraction contract is invalid")]
    ExtractionContract,
}

/// Explicit portfolio importer capacity configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortfolioImportLimitsInput {
    /// Maximum distinct exact raw records retained durably.
    pub max_archive_records: usize,
    /// Maximum sum of exact raw payload bytes retained durably.
    pub max_archive_bytes: u64,
    /// Maximum canonical records generated by one imported batch.
    pub max_normalized_records: usize,
    /// Maximum candidate values in one ambiguous cost-basis record.
    pub max_basis_candidates: usize,
    /// Maximum discrepancies generated by one imported batch.
    pub max_discrepancies: usize,
}

/// Checked portfolio importer capacity bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortfolioImportLimits {
    max_archive_records: usize,
    max_archive_bytes: u64,
    max_normalized_records: usize,
    max_basis_candidates: usize,
    max_discrepancies: usize,
}

impl PortfolioImportLimits {
    /// Returns conservative local-import defaults.
    pub const fn standard() -> Self {
        Self {
            max_archive_records: 4_096,
            max_archive_bytes: 1024 * 1024,
            max_normalized_records: 16_384,
            max_basis_candidates: 16,
            max_discrepancies: 1_024,
        }
    }

    /// Constructs importer limits under process-global and durable-store ceilings.
    ///
    /// # Errors
    ///
    /// Rejects zero values and values beyond their hard ceilings.
    pub fn try_new(input: PortfolioImportLimitsInput) -> Result<Self, PortfolioImportError> {
        let durable_max = u64::try_from(LocalAuthorityStateStore::maximum_payload_bytes())
            .map_err(|_| PortfolioImportError::InvalidLimits)?;
        if input.max_archive_records == 0
            || input.max_archive_records > MAX_EXTRACTION_RECORDS
            || input.max_archive_bytes == 0
            || input.max_archive_bytes > durable_max
            || input.max_normalized_records == 0
            || input.max_normalized_records > MAX_EXTRACTION_RECORDS
            || input.max_basis_candidates == 0
            || input.max_basis_candidates > 64
            || input.max_discrepancies == 0
            || input.max_discrepancies > reconcile::PortfolioImportLimitsCeiling::MAX_DISCREPANCIES
        {
            return Err(PortfolioImportError::InvalidLimits);
        }
        Ok(Self {
            max_archive_records: input.max_archive_records,
            max_archive_bytes: input.max_archive_bytes,
            max_normalized_records: input.max_normalized_records,
            max_basis_candidates: input.max_basis_candidates,
            max_discrepancies: input.max_discrepancies,
        })
    }

    /// Returns the maximum number of distinct durable raw records.
    pub const fn max_archive_records(self) -> usize {
        self.max_archive_records
    }

    /// Returns the maximum sum of durable exact raw payload bytes.
    pub const fn max_archive_bytes(self) -> u64 {
        self.max_archive_bytes
    }

    /// Returns the maximum canonical records generated by one batch.
    pub const fn max_normalized_records(self) -> usize {
        self.max_normalized_records
    }

    /// Returns the maximum ambiguous basis candidates retained per holding.
    pub const fn max_basis_candidates(self) -> usize {
        self.max_basis_candidates
    }

    /// Returns the maximum discrepancy records generated by one batch.
    pub const fn max_discrepancies(self) -> usize {
        self.max_discrepancies
    }
}

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
    source_reference: SourceIdentifier,
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

    /// Imports one generic file/OFX extraction batch using raw-before-normalize durability.
    ///
    /// Exact raw records are durably committed before any payload interpretation. A later
    /// normalization failure therefore retains authoritative evidence for correction or replay.
    ///
    /// # Errors
    ///
    /// Fails closed on lineage, evidence, capacity, parsing, reconciliation, supersession,
    /// durability, or canonical-output contract errors.
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

        // The empty active/supersession proposal is deliberately not used: existing authority
        // remains durable while newly seen exact source evidence is committed first.
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

        let normalized = normalize_batch(
            batch,
            &batch_raw,
            self.source_id.clone(),
            self.quality,
            self.limits,
        )?;
        let (disposition, proposed_active, proposed_brokers, proposed_supersessions) =
            self.propose_active_state(&normalized.states)?;
        let discrepancies = reconcile_import(&normalized, self.limits)?;
        let canonical_batch = build_canonical_batch(batch, normalized.canonical, self.limits)?;

        if disposition == ImportDisposition::Applied {
            if let Err(error) =
                self.persist_state(&self.raw_records, &proposed_active, &proposed_supersessions)
            {
                self.durability_failed = true;
                return Err(error);
            }
            self.active_records = proposed_active;
            self.active_broker_ids = proposed_brokers;
            self.superseded_by = proposed_supersessions;
        }

        Ok(PortfolioImport {
            disposition,
            raw_records: batch_raw,
            accounts: normalized.accounts,
            holdings: normalized.holdings,
            transactions: normalized.transactions,
            cash_flows: normalized.cash_flows,
            cost_bases: normalized.cost_bases,
            supplied_totals: normalized.supplied_totals,
            discrepancies,
            normalized_batch: canonical_batch,
        })
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
        let mut active_broker_ids = BTreeMap::new();
        for active in durable.active_records {
            if !raw_by_reference.contains_key(&active.source_reference)
                || active_records
                    .insert(active.record_id.clone(), active.clone())
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
        let mut brokers = self.active_broker_ids.clone();
        let mut supersessions = self.superseded_by.clone();
        let mut seen = BTreeSet::new();
        let mut disposition = ImportDisposition::Replay;

        for state in states {
            if !seen.insert(state.record_id.clone()) {
                return Err(PortfolioImportError::DuplicateSourceRecordId);
            }
            let existing = active.get(&state.record_id).cloned();
            if let Some(existing) = existing {
                if existing.source_reference == state.source_reference {
                    if existing.revision != state.revision {
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
                if let (Some(account_id), Some(broker_id)) = (
                    existing.broker_account_id,
                    existing.broker_transaction_id.as_ref(),
                ) {
                    brokers.remove(&(account_id, broker_id.clone()));
                }
                supersessions.insert(
                    existing.source_reference.clone(),
                    state.source_reference.clone(),
                );
            } else if state.supersedes_revision.is_some() {
                return Err(PortfolioImportError::SupersessionMismatch);
            }

            if let (Some(account_id), Some(broker_id)) =
                (state.broker_account_id, state.broker_transaction_id.clone())
                && brokers
                    .insert((account_id, broker_id), state.record_id.clone())
                    .is_some_and(|owner| owner != state.record_id)
            {
                return Err(PortfolioImportError::DuplicateBrokerTransactionId);
            }
            active.insert(
                state.record_id.clone(),
                ActivePortfolioRecord {
                    record_id: state.record_id.clone(),
                    revision: state.revision.clone(),
                    source_reference: state.source_reference.clone(),
                    broker_account_id: state.broker_account_id,
                    broker_transaction_id: state.broker_transaction_id.clone(),
                },
            );
            disposition = ImportDisposition::Applied;
        }
        Ok((disposition, active, brokers, supersessions))
    }
}

type ActiveProposal = (
    ImportDisposition,
    BTreeMap<SourceIdentifier, ActivePortfolioRecord>,
    BTreeMap<(AccountId, SourceIdentifier), SourceIdentifier>,
    BTreeMap<SourceIdentifier, SourceIdentifier>,
);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEnvelopeWire {
    record_id: String,
    #[serde(default)]
    supersedes_revision: Option<String>,
    revision_number: u32,
    received_at_unix_nanos: String,
    ingested_at_unix_nanos: String,
    record: RawRecordWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
enum RawRecordWire {
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
enum BasisWire {
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
enum LotMethodWire {
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
enum TransactionKindWire {
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

struct ParsedRecordState {
    record_id: SourceIdentifier,
    revision: SourceIdentifier,
    supersedes_revision: Option<SourceIdentifier>,
    source_reference: SourceIdentifier,
    broker_account_id: Option<AccountId>,
    broker_transaction_id: Option<SourceIdentifier>,
}

struct CanonicalObservation {
    input_index: usize,
    observation: ResearchObservation,
}

struct NormalizedImport {
    states: Vec<ParsedRecordState>,
    accounts: Vec<AccountObservation>,
    holdings: Vec<HoldingObservation>,
    transactions: Vec<PortfolioTransaction>,
    cash_flows: Vec<CashFlowObservation>,
    cost_bases: Vec<CostBasisObservation>,
    supplied_totals: Vec<SuppliedTotals>,
    canonical: Vec<CanonicalObservation>,
}

fn normalize_batch(
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
            supersedes_revision,
            source_reference: raw.source_reference.clone(),
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
                let context = research_context(
                    record,
                    raw,
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
                    raw.source_reference.clone(),
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
                let quantity = SignedQuantity::try_new(decimal(&quantity)?)?;
                let lot_size = LotSize::try_from_decimal(decimal(&lot_size)?)
                    .map_err(|_| PortfolioImportError::InvalidLotSize)?;
                let market_value = money(&market_value, currency)?;
                let as_of = timestamp(&as_of_unix_nanos)?;
                let context = research_context(
                    record,
                    raw,
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
                    &raw.source_reference,
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
                    raw.source_reference.clone(),
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
                        raw.source_reference.clone(),
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
                    raw.source_reference.clone(),
                ));
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
                let cash = optional_money(cash.as_deref(), currency)?;
                let market_value = optional_money(market_value.as_deref(), currency)?;
                let cost_basis = optional_money(cost_basis.as_deref(), currency)?;
                let tolerance =
                    ReconciliationTolerance::try_absolute(money(&absolute_tolerance, currency)?)?;
                let as_of = timestamp(&as_of_unix_nanos)?;
                let context = research_context(
                    record,
                    raw,
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
                    raw.source_reference.clone(),
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

#[allow(
    clippy::too_many_arguments,
    reason = "research provenance must remain explicit"
)]
fn research_context(
    record: &ExtractionRecord,
    raw: &RawPortfolioRecord,
    source_id: SourceId,
    quality: DataQuality,
    instrument_id: Option<InstrumentId>,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    ingested_at: Timestamp,
    revision: RevisionNumber,
) -> Result<ResearchContext, PortfolioImportError> {
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id,
        instrument_id,
        venue_id: None,
        source_identifier: raw.source_reference.clone(),
        source_timestamp,
        received_at,
        ingested_at,
        quality,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            raw.payload_hash().algorithm(),
            raw.payload_hash().bytes(),
        )),
        availability: domain_availability(record.availability()),
    })
    .map_err(|_| PortfolioImportError::ExtractionContract)?;
    let time = ResearchTime::try_new_with_coordinates(
        record.effective_time().clone(),
        record.published_time().cloned(),
        revision,
        record.superseded_time().cloned(),
    )
    .map_err(|_| PortfolioImportError::ExtractionContract)?;
    ResearchContext::new(provenance, time).map_err(|_| PortfolioImportError::ExtractionContract)
}

fn domain_availability(
    availability: &SourceAvailabilityEvidence,
) -> market_squawk_domain::AvailabilityEvidence {
    match availability {
        SourceAvailabilityEvidence::Observed {
            available_at,
            evidence,
        } => market_squawk_domain::AvailabilityEvidence::evidenced(*available_at, evidence.clone()),
        SourceAvailabilityEvidence::LocalFirstObserved { observed_at } => {
            market_squawk_domain::AvailabilityEvidence::local_first_observed(*observed_at)
        }
        SourceAvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => market_squawk_domain::AvailabilityEvidence::inferred(*inferred_at, method.clone()),
        SourceAvailabilityEvidence::Unknown => {
            market_squawk_domain::AvailabilityEvidence::unknown()
        }
    }
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

const fn cash_flow_kind(kind: TransactionKind) -> Option<CashFlowKind> {
    match kind {
        TransactionKind::CashTransfer => Some(CashFlowKind::Transfer),
        TransactionKind::Income => Some(CashFlowKind::Income),
        TransactionKind::Fee => Some(CashFlowKind::Fee),
        TransactionKind::Trade | TransactionKind::CorporateAction => None,
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "canonical scalar fields stay explicit"
)]
fn push_scalar(
    output: &mut Vec<CanonicalObservation>,
    input_index: usize,
    context: ResearchContext,
    dataset: &str,
    field: &str,
    value: Decimal,
    unit: Option<&str>,
) -> Result<(), PortfolioImportError> {
    let observation = AlternativeDataObservation::new(
        context,
        identifier(dataset)?,
        identifier(field)?,
        value,
        unit.map(identifier).transpose()?,
    );
    output.push(CanonicalObservation {
        input_index,
        observation: ResearchObservation::AlternativeData(observation),
    });
    Ok(())
}

fn reconcile_import(
    normalized: &NormalizedImport,
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
        let Some(aggregate) = aggregates.get_mut(&holding.account_id()) else {
            continue;
        };
        if aggregate.currency != holding.currency() {
            return Err(PortfolioImportError::CurrencyMismatch);
        }
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
        if aggregates
            .get(&transaction.account_id())
            .is_some_and(|aggregate| aggregate.currency != transaction.amount().currency())
        {
            return Err(PortfolioImportError::CurrencyMismatch);
        }
    }

    let reconciliation_limits = ReconciliationLimits::try_new(limits.max_discrepancies)?;
    let mut discrepancies = Vec::new();
    for supplied in &normalized.supplied_totals {
        let aggregate = aggregates
            .get(&supplied.account_id())
            .ok_or(PortfolioImportError::AccountMismatch)?;
        if aggregate.currency != supplied.currency() {
            return Err(PortfolioImportError::CurrencyMismatch);
        }
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

fn build_canonical_batch(
    input: &ExtractionBatch,
    canonical: Vec<CanonicalObservation>,
    limits: PortfolioImportLimits,
) -> Result<ExtractionBatch, PortfolioImportError> {
    if canonical.is_empty() {
        return Err(PortfolioImportError::ExtractionContract);
    }
    if canonical.len() > limits.max_normalized_records {
        return Err(PortfolioImportError::NormalizedRecordLimitExceeded {
            max: limits.max_normalized_records,
        });
    }
    let request_max = usize::try_from(input.request().max_records())
        .map_err(|_| PortfolioImportError::ExtractionContract)?;
    if canonical.len() > request_max {
        return Err(PortfolioImportError::NormalizedRecordLimitExceeded { max: request_max });
    }
    let schema = identifier(CURRENT_RESEARCH_RECORD_SCHEMA)?;
    let mut accumulator = ExtractionBatchAccumulator::try_new(input.request())
        .map_err(|_| PortfolioImportError::ExtractionContract)?;
    for item in canonical {
        let source = input
            .records()
            .get(item.input_index)
            .ok_or(PortfolioImportError::ExtractionContract)?;
        let payload = serde_json::to_vec(&item.observation)
            .map_err(|_| PortfolioImportError::ExtractionContract)?;
        let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&payload).into(),
        ));
        let output = ExtractionRecord::try_new_with_time(
            input.request(),
            schema.clone(),
            evidence,
            source.effective_time().clone(),
            source.published_time().cloned(),
            source.availability().clone(),
            source.revision().clone(),
            source.superseded_time().cloned(),
            Bytes::from(payload),
        )
        .map_err(|_| PortfolioImportError::ExtractionContract)?;
        accumulator
            .push(output)
            .map_err(|_| PortfolioImportError::ExtractionContract)?;
    }
    accumulator
        .finish()
        .map_err(|_| PortfolioImportError::ExtractionContract)
}

fn validate_raw_record(record: &ExtractionRecord) -> Result<(), PortfolioImportError> {
    if record.schema().as_str() != RAW_RECORD_SCHEMA {
        return Err(PortfolioImportError::UnsupportedRecordSchema);
    }
    if !payload_matches_exact_evidence(record.payload(), record.evidence()) {
        return Err(PortfolioImportError::RawEvidenceMismatch);
    }
    Ok(())
}

fn validate_raw_capacity(
    records: &[RawPortfolioRecord],
    limits: PortfolioImportLimits,
) -> Result<(), PortfolioImportError> {
    if records.len() > limits.max_archive_records {
        return Err(PortfolioImportError::ArchiveRecordLimitExceeded {
            max: limits.max_archive_records,
        });
    }
    let raw_bytes = records.iter().try_fold(0_u64, |total, record| {
        let bytes = u64::try_from(record.bytes().len()).map_err(|_| {
            PortfolioImportError::ArchiveByteLimitExceeded {
                max: limits.max_archive_bytes,
            }
        })?;
        total
            .checked_add(bytes)
            .ok_or(PortfolioImportError::ArchiveByteLimitExceeded {
                max: limits.max_archive_bytes,
            })
    })?;
    if raw_bytes > limits.max_archive_bytes {
        return Err(PortfolioImportError::ArchiveByteLimitExceeded {
            max: limits.max_archive_bytes,
        });
    }
    Ok(())
}

fn raw_source_reference(
    record: &ExtractionRecord,
) -> Result<SourceIdentifier, PortfolioImportError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-raw-reference/v1");
    hash_text(&mut digest, record.source_id().as_str())?;
    hash_text(
        &mut digest,
        record.metadata_revision().as_source_identifier().as_str(),
    )?;
    hash_text(&mut digest, record.dataset().as_str())?;
    hash_text(&mut digest, record.object_id().as_str())?;
    hash_serialized(&mut digest, record.object_evidence())?;
    hash_text(&mut digest, record.schema().as_str())?;
    hash_serialized(&mut digest, record.evidence())?;
    hash_serialized(&mut digest, record.effective_time())?;
    hash_serialized(&mut digest, &record.published_time())?;
    hash_serialized(&mut digest, record.availability())?;
    hash_text(&mut digest, record.revision().as_str())?;
    hash_serialized(&mut digest, &record.superseded_time())?;
    hash_bytes(&mut digest, record.payload())?;
    let encoded = hex_lower(&digest.finalize());
    identifier(&format!("{RAW_REFERENCE_PREFIX}{encoded}"))
}

fn hash_serialized<T: Serialize>(
    digest: &mut Sha256,
    value: &T,
) -> Result<(), PortfolioImportError> {
    let bytes = serde_json::to_vec(value).map_err(|_| PortfolioImportError::InvalidRecord)?;
    hash_bytes(digest, &bytes)
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), PortfolioImportError> {
    hash_bytes(digest, value.as_bytes())
}

fn hash_bytes(digest: &mut Sha256, bytes: &[u8]) -> Result<(), PortfolioImportError> {
    let length = u64::try_from(bytes.len()).map_err(|_| PortfolioImportError::InvalidRecord)?;
    digest.update(length.to_be_bytes());
    digest.update(bytes);
    Ok(())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
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
