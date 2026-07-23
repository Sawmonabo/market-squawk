#![forbid(unsafe_code)]
//! Raw-preserving, checked local portfolio import and reconciliation.

mod archive;
mod canonical;
mod holdings;
mod normalize;
mod raw;
mod reconcile;
mod source;
mod transactions;
mod wire;

use market_squawk_platform::LocalAuthorityStateStore;
use market_squawk_sources::MAX_EXTRACTION_RECORDS;
use thiserror::Error;

pub use archive::{
    ActivePortfolioRecord, ImportDisposition, PortfolioExtractionSource, PortfolioImport,
    RawPortfolioRecord,
};
pub use holdings::{
    AccountObservation, BasisResolution, CostBasisObservation, HoldingObservation, LotMethod,
    SignedQuantity,
};
pub use reconcile::{
    CalculatedTotals, ReconciliationDiscrepancy, ReconciliationField, ReconciliationLimits,
    ReconciliationTolerance, SuppliedTotals, reconcile_totals,
};
pub use source::{PortfolioManifestExtractionSource, PortfolioManifestSourceError};
pub use transactions::{CashFlowKind, CashFlowObservation, PortfolioTransaction, TransactionKind};

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
    /// A correction's numeric revision does not strictly advance active canonical state.
    #[error("portfolio correction revision must strictly increase")]
    NonIncreasingRevision,
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
    pub(crate) max_archive_records: usize,
    pub(crate) max_archive_bytes: u64,
    pub(crate) max_normalized_records: usize,
    pub(crate) max_basis_candidates: usize,
    pub(crate) max_discrepancies: usize,
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
