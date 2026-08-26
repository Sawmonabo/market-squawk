//! Bounded provider-native evidence aligned exactly to canonical extraction rows.

use std::io::{self, Write};
use std::mem::size_of;

use bytes::Bytes;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::{ExtractionBatch, ExtractionContentIdentity, MAX_EXTRACTION_RECORDS};

const SCHEMA_FINGERPRINT_DOMAIN: &[u8] =
    b"market-squawk/provider-native-lineage/schema-fingerprint/v1";
const BATCH_DIGEST_DOMAIN: &[u8] = b"market-squawk/provider-native-lineage/batch/v1";

/// Current code-owned provider-native lineage schema version.
pub const PROVIDER_NATIVE_LINEAGE_SCHEMA_VERSION: u16 = 1;
/// Maximum exact provider-native semantic bytes retained beside one canonical record.
pub const MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES: usize = 64 * 1024;
/// Maximum checked deep bytes retained by one provider-native lineage batch.
pub const MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Closed adapter encoder implementations admitted by the current native-lineage schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderNativeLineageImplementation {
    /// Alpaca historical-bar response semantics encoder v1.
    AlpacaHistoricalBarV1,
    /// BEA regional/table response semantics encoder v1.
    BeaRegionalV1,
    /// BLS timeseries observation semantics encoder v1.
    BlsTimeseriesV1,
    /// Census tabular observation semantics encoder v1.
    CensusTabularV1,
    /// EIA series observation semantics encoder v1.
    EiaSeriesV1,
    /// Federal Reserve Board H.15 observation semantics encoder v1.
    FederalReserveH15V1,
    /// FRED/ALFRED series and vintage-observation semantics encoder v1.
    FredAlfredSeriesObservationV1,
    /// Treasury daily-rate observation semantics encoder v1.
    TreasuryDailyRateV1,
    /// Treasury Fiscal Data observation semantics encoder v1.
    TreasuryFiscalDataV1,
}

impl ProviderNativeLineageImplementation {
    const fn identifier(self) -> &'static [u8] {
        match self {
            Self::AlpacaHistoricalBarV1 => {
                b"market-squawk/alpaca-historical/provider-native-lineage/v1"
            }
            Self::BeaRegionalV1 => b"market-squawk/bea/provider-native-lineage/v1",
            Self::BlsTimeseriesV1 => b"market-squawk/bls/provider-native-lineage/v1",
            Self::CensusTabularV1 => b"market-squawk/census/provider-native-lineage/v1",
            Self::EiaSeriesV1 => b"market-squawk/eia/provider-native-lineage/v1",
            Self::FederalReserveH15V1 => {
                b"market-squawk/federal-reserve-h15/provider-native-lineage/v1"
            }
            Self::FredAlfredSeriesObservationV1 => {
                b"market-squawk/fred-alfred/provider-native-lineage/v1"
            }
            Self::TreasuryDailyRateV1 => {
                b"market-squawk/treasury-daily-rate/provider-native-lineage/v1"
            }
            Self::TreasuryFiscalDataV1 => {
                b"market-squawk/treasury-fiscal-data/provider-native-lineage/v1"
            }
        }
    }

    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::AlpacaHistoricalBarV1 => 5,
            Self::BeaRegionalV1 => 1,
            Self::BlsTimeseriesV1 => 2,
            Self::CensusTabularV1 => 3,
            Self::EiaSeriesV1 => 4,
            Self::FederalReserveH15V1 => 6,
            Self::FredAlfredSeriesObservationV1 => 9,
            Self::TreasuryDailyRateV1 => 7,
            Self::TreasuryFiscalDataV1 => 8,
        }
    }
}

/// Versioned, code-owned identity of one adapter-native row encoder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderNativeLineageSchema {
    version: u16,
    implementation: ProviderNativeLineageImplementation,
    fingerprint: EvidenceDigest,
}

impl ProviderNativeLineageSchema {
    pub(crate) fn for_implementation(implementation: ProviderNativeLineageImplementation) -> Self {
        let version = PROVIDER_NATIVE_LINEAGE_SCHEMA_VERSION;
        let mut digest = Sha256::new();
        hash_field(&mut digest, SCHEMA_FINGERPRINT_DOMAIN);
        digest.update(version.to_be_bytes());
        hash_field(&mut digest, implementation.identifier());
        Self {
            version,
            implementation,
            fingerprint: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
        }
    }

    /// Returns the code-owned schema version.
    pub const fn version(self) -> u16 {
        self.version
    }

    /// Returns the closed adapter encoder implementation.
    pub const fn implementation(self) -> ProviderNativeLineageImplementation {
        self.implementation
    }

    /// Returns the deterministic schema fingerprint.
    pub const fn fingerprint(self) -> EvidenceDigest {
        self.fingerprint
    }
}

/// One exact provider-native semantic payload aligned to one canonical extraction record.
///
/// Adapter encoders retain provider fields that can change the economic meaning of the row. Local
/// receipt/ingest clocks, raw-page placement, capture digests, canonical copies, row ordinals, and
/// application policy belong to the surrounding batch/capture authorities and must not be encoded
/// again as provider semantics.
#[derive(Debug, Eq, PartialEq)]
pub struct ProviderNativeLineageRow {
    ordinal: u32,
    canonical_record_digest: EvidenceDigest,
    semantic_payload: Bytes,
    semantic_payload_digest: EvidenceDigest,
}

impl ProviderNativeLineageRow {
    /// Returns the zero-based contiguous canonical record ordinal.
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Returns the canonical normalized record payload digest at this ordinal.
    pub const fn canonical_record_digest(&self) -> EvidenceDigest {
        self.canonical_record_digest
    }

    /// Returns the exact adapter-encoded provider-native semantic payload.
    pub fn semantic_payload(&self) -> &Bytes {
        &self.semantic_payload
    }

    /// Returns SHA-256 of the exact provider-native semantic payload bytes.
    pub const fn semantic_payload_digest(&self) -> EvidenceDigest {
        self.semantic_payload_digest
    }
}

/// Checked borrowed restart projection of one exact provider-native lineage row.
///
/// This value carries no live authority and cannot construct a native-lineage batch. It exists
/// only as bounded input to [`verify_provider_native_lineage_batch_evidence`].
#[derive(Debug)]
pub struct ProviderNativeLineageRowEvidenceRef<'a> {
    ordinal: u32,
    canonical_record_digest: EvidenceDigest,
    semantic_payload: &'a [u8],
    semantic_payload_digest: EvidenceDigest,
}

impl<'a> ProviderNativeLineageRowEvidenceRef<'a> {
    /// Validates one borrowed persisted row projection without copying its semantic bytes.
    pub fn try_new(
        ordinal: u32,
        canonical_record_digest: EvidenceDigest,
        semantic_payload: &'a [u8],
        semantic_payload_digest: EvidenceDigest,
    ) -> Result<Self, ProviderNativeLineageError> {
        let ordinal_usize =
            usize::try_from(ordinal).map_err(|_| ProviderNativeLineageError::ByteCountOverflow)?;
        if ordinal_usize >= MAX_EXTRACTION_RECORDS {
            return Err(ProviderNativeLineageError::RecordLimitExceeded {
                max: MAX_EXTRACTION_RECORDS,
            });
        }
        if semantic_payload.is_empty() {
            return Err(ProviderNativeLineageError::EmptySemanticPayload {
                ordinal: ordinal_usize,
            });
        }
        if semantic_payload.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES {
            return Err(ProviderNativeLineageError::RowByteLimitExceeded {
                ordinal: ordinal_usize,
                max: MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES,
            });
        }
        require_sha256_nonzero(canonical_record_digest)?;
        require_sha256_nonzero(semantic_payload_digest)?;
        if semantic_payload_digest != sha256(semantic_payload) {
            return Err(ProviderNativeLineageError::AlignmentMismatch);
        }
        Ok(Self {
            ordinal,
            canonical_record_digest,
            semantic_payload,
            semantic_payload_digest,
        })
    }

    fn digest_row(&self) -> ProviderNativeLineageDigestRow {
        ProviderNativeLineageDigestRow {
            ordinal: self.ordinal,
            canonical_record_digest: self.canonical_record_digest,
            semantic_payload_bytes: self.semantic_payload.len(),
            semantic_payload_digest: self.semantic_payload_digest,
        }
    }
}

/// Non-cloneable, bounded provider-native evidence aligned one-for-one to a canonical batch.
#[derive(Debug, Eq, PartialEq)]
pub struct ProviderNativeLineageBatch {
    schema: ProviderNativeLineageSchema,
    content_identity: ExtractionContentIdentity,
    rows: Box<[ProviderNativeLineageRow]>,
    batch_digest: EvidenceDigest,
}

impl ProviderNativeLineageBatch {
    /// Returns the code-owned native-lineage schema.
    pub const fn schema(&self) -> ProviderNativeLineageSchema {
        self.schema
    }

    /// Returns the exact extraction content identity this lineage was minted against.
    pub const fn content_identity(&self) -> ExtractionContentIdentity {
        self.content_identity
    }

    /// Returns exact contiguous rows in canonical input order.
    pub const fn rows(&self) -> &[ProviderNativeLineageRow] {
        &self.rows
    }

    /// Returns the deterministic digest of schema and every aligned row identity.
    pub const fn batch_digest(&self) -> EvidenceDigest {
        self.batch_digest
    }

    /// Revalidates exact cardinality, ordinal, canonical digest, native payload, and batch digest.
    ///
    /// # Errors
    ///
    /// Rejects any transplant, mutation, or internally inconsistent lineage state.
    pub fn validate(&self, batch: &ExtractionBatch) -> Result<(), ProviderNativeLineageError> {
        let content_identity = ExtractionContentIdentity::try_from_batch(batch)
            .map_err(|_| ProviderNativeLineageError::AlignmentMismatch)?;
        if self.schema
            != ProviderNativeLineageSchema::for_implementation(self.schema.implementation)
            || self.content_identity != content_identity
            || self.rows.len() != batch.records().len()
            || self.rows.len() > MAX_EXTRACTION_RECORDS
        {
            return Err(ProviderNativeLineageError::AlignmentMismatch);
        }
        let mut retained_bytes = size_of::<Self>()
            .checked_add(
                size_of::<ProviderNativeLineageRow>()
                    .checked_mul(self.rows.len())
                    .ok_or(ProviderNativeLineageError::ByteCountOverflow)?,
            )
            .ok_or(ProviderNativeLineageError::ByteCountOverflow)?;
        for (ordinal, (row, record)) in self.rows.iter().zip(batch.records()).enumerate() {
            retained_bytes = retained_bytes
                .checked_add(row.semantic_payload.len())
                .ok_or(ProviderNativeLineageError::ByteCountOverflow)?;
            if row.ordinal != u32::try_from(ordinal).ok().unwrap_or(u32::MAX)
                || row.canonical_record_digest != record.evidence().content_digest()
                || row.semantic_payload.is_empty()
                || row.semantic_payload.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES
                || row.semantic_payload_digest != sha256(&row.semantic_payload)
                || retained_bytes > MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES
            {
                return Err(ProviderNativeLineageError::AlignmentMismatch);
            }
        }
        if self.batch_digest != batch_digest(self.schema, self.content_identity, &self.rows)? {
            return Err(ProviderNativeLineageError::AlignmentMismatch);
        }
        Ok(())
    }
}

/// Verifies persisted provider-native batch evidence without reconstructing a live batch.
///
/// The expected batch digest is caller-supplied restart evidence. Common code independently
/// revalidates the exact code-owned schema, extraction identity, contiguous row alignment,
/// semantic payload digests, and retained-byte bounds before comparing the private canonical hash.
/// No computed digest or reusable verification capability is returned.
#[allow(
    clippy::too_many_arguments,
    reason = "persisted native-lineage evidence remains explicit"
)]
pub fn verify_provider_native_lineage_batch_evidence(
    expected_batch_digest: EvidenceDigest,
    schema_version: u16,
    implementation: ProviderNativeLineageImplementation,
    schema_fingerprint: EvidenceDigest,
    extraction_content_digest: EvidenceDigest,
    extraction_record_count: usize,
    rows: &[ProviderNativeLineageRowEvidenceRef<'_>],
) -> Result<(), ProviderNativeLineageError> {
    require_sha256_nonzero(expected_batch_digest)?;
    require_sha256_nonzero(extraction_content_digest)?;
    let schema = ProviderNativeLineageSchema::for_implementation(implementation);
    if schema.version() != schema_version
        || schema.fingerprint() != schema_fingerprint
        || rows.len() != extraction_record_count
        || rows.len() > MAX_EXTRACTION_RECORDS
    {
        return Err(ProviderNativeLineageError::AlignmentMismatch);
    }
    let mut retained_bytes = size_of::<ProviderNativeLineageBatch>()
        .checked_add(
            size_of::<ProviderNativeLineageRow>()
                .checked_mul(rows.len())
                .ok_or(ProviderNativeLineageError::ByteCountOverflow)?,
        )
        .ok_or(ProviderNativeLineageError::ByteCountOverflow)?;
    for (expected_ordinal, row) in rows.iter().enumerate() {
        retained_bytes = retained_bytes
            .checked_add(row.semantic_payload.len())
            .ok_or(ProviderNativeLineageError::ByteCountOverflow)?;
        if row.ordinal
            != u32::try_from(expected_ordinal)
                .map_err(|_| ProviderNativeLineageError::ByteCountOverflow)?
            || row.semantic_payload.is_empty()
            || row.semantic_payload.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES
            || row.semantic_payload_digest != sha256(row.semantic_payload)
            || retained_bytes > MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES
        {
            return Err(ProviderNativeLineageError::AlignmentMismatch);
        }
        require_sha256_nonzero(row.canonical_record_digest)?;
        require_sha256_nonzero(row.semantic_payload_digest)?;
    }
    let observed = batch_digest_from_rows(
        schema,
        extraction_content_digest,
        extraction_record_count,
        rows.iter()
            .map(ProviderNativeLineageRowEvidenceRef::digest_row),
    )?;
    if observed != expected_batch_digest {
        return Err(ProviderNativeLineageError::AlignmentMismatch);
    }
    Ok(())
}

/// Non-cloneable incremental encoder bound to one exact canonical extraction batch.
///
/// Adapters supply only one serializable row-local semantic value at a time. The builder derives
/// the canonical ordinal and record digest, enforces both row and aggregate retained-byte bounds
/// before retaining that row, and can finish only after every canonical record has one row.
#[derive(Debug)]
pub struct ProviderNativeLineageBatchBuilder<'batch> {
    schema: ProviderNativeLineageSchema,
    batch: &'batch ExtractionBatch,
    content_identity: ExtractionContentIdentity,
    rows: Vec<ProviderNativeLineageRow>,
    retained_bytes: usize,
}

impl<'batch> ProviderNativeLineageBatchBuilder<'batch> {
    /// Starts one bounded incremental adapter encoder bound to an exact extraction batch.
    ///
    /// The builder precharges the final batch header and every row slot before retaining semantic
    /// bytes. Each subsequent row is serialized through the common per-row bound and aligned to
    /// the next canonical record without an intermediate provider-owned payload collection.
    ///
    /// # Errors
    ///
    /// Rejects an oversized canonical batch, checked retained-byte overflow, alignment failure,
    /// or bounded row-slot allocation failure.
    pub fn try_new(
        implementation: ProviderNativeLineageImplementation,
        batch: &'batch ExtractionBatch,
    ) -> Result<Self, ProviderNativeLineageError> {
        let expected = batch.records().len();
        if expected > MAX_EXTRACTION_RECORDS {
            return Err(ProviderNativeLineageError::RecordLimitExceeded {
                max: MAX_EXTRACTION_RECORDS,
            });
        }
        let retained_bytes = size_of::<ProviderNativeLineageBatch>()
            .checked_add(
                size_of::<ProviderNativeLineageRow>()
                    .checked_mul(expected)
                    .ok_or(ProviderNativeLineageError::ByteCountOverflow)?,
            )
            .ok_or(ProviderNativeLineageError::ByteCountOverflow)?;
        if retained_bytes > MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES {
            return Err(ProviderNativeLineageError::BatchByteLimitExceeded {
                max: MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES,
            });
        }
        let content_identity = ExtractionContentIdentity::try_from_batch(batch)
            .map_err(|_| ProviderNativeLineageError::AlignmentMismatch)?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(expected)
            .map_err(|_| ProviderNativeLineageError::AllocationFailure)?;
        Ok(Self {
            schema: ProviderNativeLineageSchema::for_implementation(implementation),
            batch,
            content_identity,
            rows,
            retained_bytes,
        })
    }

    /// Serializes and retains the next provider-native semantic row under the common bounds.
    ///
    /// The zero-based ordinal and canonical digest come only from the exact batch supplied when
    /// this builder was created. Serialization stops at the 64-KiB row boundary, and aggregate
    /// bytes are checked before the right-sized owned payload moves into the retained row.
    pub fn try_push<T>(&mut self, value: &T) -> Result<(), ProviderNativeLineageError>
    where
        T: Serialize + ?Sized,
    {
        let ordinal = self.rows.len();
        let expected = self.batch.records().len();
        let Some(record) = self.batch.records().get(ordinal) else {
            return Err(ProviderNativeLineageError::RowCountMismatch {
                expected,
                observed: ordinal
                    .checked_add(1)
                    .ok_or(ProviderNativeLineageError::ByteCountOverflow)?,
            });
        };
        let semantic_payload = serialize_provider_native_lineage_row(value, ordinal)?;
        let retained_bytes = self
            .retained_bytes
            .checked_add(semantic_payload.len())
            .ok_or(ProviderNativeLineageError::ByteCountOverflow)?;
        if retained_bytes > MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES {
            return Err(ProviderNativeLineageError::BatchByteLimitExceeded {
                max: MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES,
            });
        }
        let semantic_payload = Bytes::from(semantic_payload.into_boxed_slice().into_vec());
        let semantic_payload_digest = sha256(&semantic_payload);
        self.rows.push(ProviderNativeLineageRow {
            ordinal: u32::try_from(ordinal)
                .map_err(|_| ProviderNativeLineageError::ByteCountOverflow)?,
            canonical_record_digest: record.evidence().content_digest(),
            semantic_payload,
            semantic_payload_digest,
        });
        self.retained_bytes = retained_bytes;
        Ok(())
    }

    /// Finishes only when every canonical record has exactly one aligned native row.
    pub fn finish(self) -> Result<ProviderNativeLineageBatch, ProviderNativeLineageError> {
        let expected = self.batch.records().len();
        let observed = self.rows.len();
        if observed != expected {
            return Err(ProviderNativeLineageError::RowCountMismatch { expected, observed });
        }
        let rows = self.rows.into_boxed_slice();
        let batch_digest = batch_digest(self.schema, self.content_identity, &rows)?;
        Ok(ProviderNativeLineageBatch {
            schema: self.schema,
            content_identity: self.content_identity,
            rows,
            batch_digest,
        })
    }
}

/// Failure to construct or revalidate bounded provider-native lineage.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProviderNativeLineageError {
    /// The native payload count did not match the canonical record count.
    #[error("provider-native lineage expected {expected} rows but received {observed}")]
    RowCountMismatch {
        /// Exact canonical record count.
        expected: usize,
        /// Supplied provider-native payload count.
        observed: usize,
    },
    /// A required provider-native semantic payload was empty.
    #[error("provider-native lineage row {ordinal} semantic payload is empty")]
    EmptySemanticPayload {
        /// Zero-based canonical row ordinal.
        ordinal: usize,
    },
    /// A provider-native semantic payload exceeded the per-row byte ceiling.
    #[error("provider-native lineage row {ordinal} exceeds {max} bytes")]
    RowByteLimitExceeded {
        /// Zero-based canonical row ordinal.
        ordinal: usize,
        /// Inclusive byte ceiling.
        max: usize,
    },
    /// The row count exceeded the extraction batch ceiling.
    #[error("provider-native lineage exceeds {max} rows")]
    RecordLimitExceeded {
        /// Inclusive row-count ceiling.
        max: usize,
    },
    /// Checked aggregate retained bytes exceeded the batch ceiling.
    #[error("provider-native lineage exceeds {max} retained bytes")]
    BatchByteLimitExceeded {
        /// Inclusive retained-byte ceiling.
        max: usize,
    },
    /// Checked byte or ordinal arithmetic overflowed.
    #[error("provider-native lineage byte accounting overflowed")]
    ByteCountOverflow,
    /// A bounded allocation could not be reserved.
    #[error("provider-native lineage bounded allocation failed")]
    AllocationFailure,
    /// Rows, schema, digests, or canonical alignment did not revalidate exactly.
    #[error("provider-native lineage does not align to the canonical extraction batch")]
    AlignmentMismatch,
    /// Adapter-native row serialization failed before producing bounded evidence.
    #[error("provider-native lineage semantic serialization failed")]
    SerializationFailure,
}

fn serialize_provider_native_lineage_row<T>(
    value: &T,
    ordinal: usize,
) -> Result<Vec<u8>, ProviderNativeLineageError>
where
    T: Serialize + ?Sized,
{
    let mut writer = BoundedNativeLineageWriter::default();
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(if writer.limit_exceeded {
            ProviderNativeLineageError::RowByteLimitExceeded {
                ordinal,
                max: MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES,
            }
        } else if writer.allocation_failed {
            ProviderNativeLineageError::AllocationFailure
        } else {
            ProviderNativeLineageError::SerializationFailure
        });
    }
    if writer.bytes.is_empty() {
        return Err(ProviderNativeLineageError::EmptySemanticPayload { ordinal });
    }
    Ok(writer.bytes)
}

#[derive(Default)]
struct BoundedNativeLineageWriter {
    bytes: Vec<u8>,
    limit_exceeded: bool,
    allocation_failed: bool,
}

impl Write for BoundedNativeLineageWriter {
    fn write(&mut self, value: &[u8]) -> io::Result<usize> {
        let Some(next_len) = self.bytes.len().checked_add(value.len()) else {
            self.limit_exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "provider-native lineage row length overflow",
            ));
        };
        if next_len > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES {
            self.limit_exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "provider-native lineage row exceeds bound",
            ));
        }
        if self.bytes.try_reserve_exact(value.len()).is_err() {
            self.allocation_failed = true;
            return Err(io::Error::other(
                "provider-native lineage bounded allocation failed",
            ));
        }
        self.bytes.extend_from_slice(value);
        Ok(value.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ProviderNativeLineageDigestRow {
    ordinal: u32,
    canonical_record_digest: EvidenceDigest,
    semantic_payload_bytes: usize,
    semantic_payload_digest: EvidenceDigest,
}

fn batch_digest(
    schema: ProviderNativeLineageSchema,
    content_identity: ExtractionContentIdentity,
    rows: &[ProviderNativeLineageRow],
) -> Result<EvidenceDigest, ProviderNativeLineageError> {
    batch_digest_from_rows(
        schema,
        content_identity.digest(),
        content_identity.record_count(),
        rows.iter().map(|row| ProviderNativeLineageDigestRow {
            ordinal: row.ordinal,
            canonical_record_digest: row.canonical_record_digest,
            semantic_payload_bytes: row.semantic_payload.len(),
            semantic_payload_digest: row.semantic_payload_digest,
        }),
    )
}

fn batch_digest_from_rows(
    schema: ProviderNativeLineageSchema,
    extraction_content_digest: EvidenceDigest,
    extraction_record_count: usize,
    rows: impl ExactSizeIterator<Item = ProviderNativeLineageDigestRow>,
) -> Result<EvidenceDigest, ProviderNativeLineageError> {
    let mut digest = Sha256::new();
    hash_checked_field(&mut digest, BATCH_DIGEST_DOMAIN)?;
    digest.update(schema.version.to_be_bytes());
    hash_checked_field(&mut digest, schema.implementation.identifier())?;
    hash_evidence(&mut digest, schema.fingerprint);
    hash_evidence(&mut digest, extraction_content_digest);
    digest.update(
        u64::try_from(extraction_record_count)
            .map_err(|_| ProviderNativeLineageError::ByteCountOverflow)?
            .to_be_bytes(),
    );
    digest.update(
        u64::try_from(rows.len())
            .map_err(|_| ProviderNativeLineageError::ByteCountOverflow)?
            .to_be_bytes(),
    );
    for row in rows {
        digest.update(row.ordinal.to_be_bytes());
        hash_evidence(&mut digest, row.canonical_record_digest);
        digest.update(
            u64::try_from(row.semantic_payload_bytes)
                .map_err(|_| ProviderNativeLineageError::ByteCountOverflow)?
                .to_be_bytes(),
        );
        hash_evidence(&mut digest, row.semantic_payload_digest);
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn require_sha256_nonzero(evidence: EvidenceDigest) -> Result<(), ProviderNativeLineageError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256
        || evidence.bytes().iter().all(|byte| *byte == 0)
    {
        return Err(ProviderNativeLineageError::AlignmentMismatch);
    }
    Ok(())
}

fn hash_checked_field(digest: &mut Sha256, value: &[u8]) -> Result<(), ProviderNativeLineageError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| ProviderNativeLineageError::ByteCountOverflow)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

fn sha256(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn hash_evidence(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}
