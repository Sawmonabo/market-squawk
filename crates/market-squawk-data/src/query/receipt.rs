//! Non-forgeable receipts for exact manifest-pinned query results.

use arrow::array::{
    Array as _, BinaryArray, Decimal128Array, StringArray, TimestampNanosecondArray, UInt8Array,
    UInt32Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    Currency, DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, SourceId,
    SourceIdentifier, Timestamp, VenueId,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

use super::{QueryError, QueryResult};
use crate::{
    DatasetManifestRef, GenerationKind, GenerationParentRelation, PinnedDataset, Sha256Digest,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PinnedMonetaryColumns {
    value: usize,
    scale: usize,
    currency: usize,
    source_id: usize,
    instrument_id: usize,
    venue_id: usize,
    source_identifier: usize,
    source_timestamp: usize,
    received_at: usize,
    available_at: usize,
    ingested_at: usize,
    effective_at: usize,
    published_at: usize,
    revision: usize,
    quality: usize,
    payload_sha256: usize,
}

pub(super) const RESEARCH_MONETARY_COLUMNS: PinnedMonetaryColumns = PinnedMonetaryColumns {
    value: 0,
    scale: 1,
    currency: 2,
    source_id: 3,
    instrument_id: 4,
    venue_id: 5,
    source_identifier: 6,
    source_timestamp: 7,
    received_at: 8,
    available_at: 9,
    ingested_at: 10,
    effective_at: 11,
    published_at: 12,
    revision: 13,
    quality: 14,
    payload_sha256: 15,
};

/// Exact result of a query executed over a catalog-resolved immutable dataset generation.
///
/// This type has no public constructor or deserializer. [`super::ResearchQueryEngine`] creates it
/// only when its source is an actual [`PinnedDataset`].
#[derive(Debug)]
pub struct PinnedQueryOutput {
    manifest: DatasetManifestRef,
    object_graph_digest: EvidenceDigest,
    query_identity: EvidenceDigest,
    result_digest: EvidenceDigest,
    result: QueryResult,
}

impl PinnedQueryOutput {
    pub(super) const fn new(
        manifest: DatasetManifestRef,
        object_graph_digest: EvidenceDigest,
        query_identity: EvidenceDigest,
        result_digest: EvidenceDigest,
        result: QueryResult,
    ) -> Self {
        Self {
            manifest,
            object_graph_digest,
            query_identity,
            result_digest,
            result,
        }
    }

    /// Returns the exact immutable dataset generation queried.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the digest of the complete catalog-resolved generation and object graph.
    pub const fn object_graph_digest(&self) -> EvidenceDigest {
        self.object_graph_digest
    }

    /// Returns the manifest, SQL, and execution-limit identity.
    pub const fn query_identity(&self) -> EvidenceDigest {
        self.query_identity
    }

    /// Returns the exact Arrow IPC result digest.
    pub const fn result_digest(&self) -> EvidenceDigest {
        self.result_digest
    }

    /// Returns the bounded query result.
    pub const fn result(&self) -> &QueryResult {
        &self.result
    }

    /// Derives an exact monetary value from one inline Decimal128 mantissa, UInt8 scale, and UTF-8
    /// currency cell. All row and column coordinates are retained in the returned receipt.
    ///
    /// # Errors
    ///
    /// Rejects artifact results, out-of-range coordinates, null cells, incompatible Arrow types,
    /// nonzero physical Decimal128 scales, invalid currencies, or values outside `Decimal`'s exact
    /// range.
    pub(super) fn monetary_value(
        &self,
        result_row: usize,
        selected_row: usize,
        columns: PinnedMonetaryColumns,
    ) -> Result<PinnedMonetaryValue, QueryError> {
        if selected_row >= super::MAX_ROWS as usize {
            return Err(QueryError::MonetaryCellOutOfBounds);
        }
        let QueryResult::Inline { batches, .. } = &self.result else {
            return Err(QueryError::MonetaryValueRequiresInlineResult);
        };
        let mut relative_row = result_row;
        let batch = batches
            .iter()
            .find(|batch| {
                if relative_row < batch.num_rows() {
                    true
                } else {
                    relative_row -= batch.num_rows();
                    false
                }
            })
            .ok_or(QueryError::MonetaryCellOutOfBounds)?;
        let value = array::<Decimal128Array>(batch, columns.value)?;
        let scale = array::<UInt8Array>(batch, columns.scale)?;
        let currency = array::<StringArray>(batch, columns.currency)?;
        if value.is_null(relative_row)
            || scale.is_null(relative_row)
            || currency.is_null(relative_row)
        {
            return Err(QueryError::InvalidMonetaryCell);
        }
        let (precision, physical_scale) = match value.data_type() {
            DataType::Decimal128(precision, physical_scale) => (*precision, *physical_scale),
            _ => return Err(QueryError::InvalidMonetaryCell),
        };
        if physical_scale != 0 {
            return Err(QueryError::InvalidMonetaryCell);
        }
        let scale_value = scale.value(relative_row);
        if u32::from(scale_value) > Decimal::MAX_SCALE {
            return Err(QueryError::UnsupportedMonetaryScale);
        }
        let currency_value = Currency::try_from(currency.value(relative_row))
            .map_err(|_| QueryError::InvalidMonetaryCell)?;
        let mantissa = value.value(relative_row);
        Decimal::try_from_i128_with_scale(mantissa, u32::from(scale_value))
            .map_err(|_| QueryError::InvalidMonetaryCell)?;
        let source_id = required_string::<SourceId>(batch, columns.source_id, relative_row)?;
        let instrument_id = optional_instrument(batch, columns.instrument_id, relative_row)?;
        let venue_id = optional_string::<VenueId>(batch, columns.venue_id, relative_row)?;
        let source_identifier =
            required_string::<SourceIdentifier>(batch, columns.source_identifier, relative_row)?;
        let source_timestamp = optional_timestamp(batch, columns.source_timestamp, relative_row)?;
        let received_at = required_timestamp(batch, columns.received_at, relative_row)?;
        let available_at = optional_timestamp(batch, columns.available_at, relative_row)?;
        let ingested_at = required_timestamp(batch, columns.ingested_at, relative_row)?;
        let effective_at = optional_timestamp(batch, columns.effective_at, relative_row)?;
        let published_at = optional_timestamp(batch, columns.published_at, relative_row)?;
        let revision_array = array::<UInt32Array>(batch, columns.revision)?;
        if revision_array.is_null(relative_row) || revision_array.value(relative_row) == 0 {
            return Err(QueryError::InvalidMonetaryCell);
        }
        let revision = revision_array.value(relative_row);
        let quality_array = array::<StringArray>(batch, columns.quality)?;
        if quality_array.is_null(relative_row) {
            return Err(QueryError::InvalidMonetaryCell);
        }
        let data_quality = parse_quality(quality_array.value(relative_row))
            .ok_or(QueryError::InvalidMonetaryCell)?;
        let payload_array = array::<BinaryArray>(batch, columns.payload_sha256)?;
        if payload_array.is_null(relative_row) {
            return Err(QueryError::InvalidMonetaryCell);
        }
        let payload_bytes: [u8; 32] = payload_array
            .value(relative_row)
            .try_into()
            .map_err(|_| QueryError::InvalidMonetaryCell)?;
        Ok(PinnedMonetaryValue {
            manifest: self.manifest.clone(),
            object_graph_digest: self.object_graph_digest,
            query_identity: self.query_identity,
            result_digest: self.result_digest,
            row: selected_row,
            columns,
            mantissa,
            precision,
            scale: scale_value,
            currency: currency_value,
            source_id,
            instrument_id,
            venue_id,
            source_identifier,
            source_timestamp,
            received_at,
            available_at,
            ingested_at,
            effective_at,
            published_at,
            revision,
            data_quality,
            payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, payload_bytes),
        })
    }
}

/// Non-forgeable exact monetary cell derived from a [`PinnedQueryOutput`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinnedMonetaryValue {
    manifest: DatasetManifestRef,
    object_graph_digest: EvidenceDigest,
    query_identity: EvidenceDigest,
    result_digest: EvidenceDigest,
    row: usize,
    columns: PinnedMonetaryColumns,
    mantissa: i128,
    precision: u8,
    scale: u8,
    currency: Currency,
    source_id: SourceId,
    instrument_id: Option<InstrumentId>,
    venue_id: Option<VenueId>,
    source_identifier: SourceIdentifier,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Option<Timestamp>,
    ingested_at: Timestamp,
    effective_at: Option<Timestamp>,
    published_at: Option<Timestamp>,
    revision: u32,
    data_quality: DataQuality,
    payload_digest: EvidenceDigest,
}

impl PinnedMonetaryValue {
    /// Returns the exact queried dataset generation.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the complete pinned object-graph identity.
    pub const fn object_graph_digest(&self) -> EvidenceDigest {
        self.object_graph_digest
    }

    /// Returns the manifest, SQL, and limit identity.
    pub const fn query_identity(&self) -> EvidenceDigest {
        self.query_identity
    }

    /// Returns the exact Arrow IPC result identity.
    pub const fn result_digest(&self) -> EvidenceDigest {
        self.result_digest
    }

    /// Returns the zero-based result row.
    pub const fn row(&self) -> usize {
        self.row
    }

    /// Returns the zero-based Decimal128 column.
    pub const fn value_column(&self) -> usize {
        self.columns.value
    }

    /// Returns the zero-based UInt8 semantic-scale column.
    pub const fn scale_column(&self) -> usize {
        self.columns.scale
    }

    /// Returns the zero-based UTF-8 currency column.
    pub const fn currency_column(&self) -> usize {
        self.columns.currency
    }

    /// Returns the exact Decimal128 mantissa.
    pub const fn mantissa(&self) -> i128 {
        self.mantissa
    }

    /// Returns the admitted Decimal128 precision.
    pub const fn precision(&self) -> u8 {
        self.precision
    }

    /// Returns the exact semantic decimal scale.
    pub const fn scale(&self) -> u8 {
        self.scale
    }

    /// Returns the validated normalized currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the exact row source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the stable row instrument identity when the observation is instrument-scoped.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        self.instrument_id
    }

    /// Returns the row venue identity when one was retained.
    pub const fn venue_id(&self) -> Option<&VenueId> {
        self.venue_id.as_ref()
    }

    /// Returns the source-native row identity.
    pub const fn source_identifier(&self) -> &SourceIdentifier {
        &self.source_identifier
    }

    /// Returns the source-authored timestamp when supplied.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns the local receive time retained by the row.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the conservative point-in-time availability timestamp when established.
    pub const fn available_at(&self) -> Option<Timestamp> {
        self.available_at
    }

    /// Returns the local ingestion timestamp.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }

    /// Returns the effective timestamp when it has timestamp precision.
    pub const fn effective_at(&self) -> Option<Timestamp> {
        self.effective_at
    }

    /// Returns the publication timestamp when supplied.
    pub const fn published_at(&self) -> Option<Timestamp> {
        self.published_at
    }

    /// Returns the source revision retained by the row.
    pub const fn revision(&self) -> u32 {
        self.revision
    }

    /// Returns the row's admitted research data quality.
    pub const fn data_quality(&self) -> DataQuality {
        self.data_quality
    }

    /// Returns the canonical row-payload SHA-256 identity.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Converts the retained mantissa and scale without rounding.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::InvalidMonetaryCell`] if the retained value cannot be represented by
    /// the analytical decimal implementation.
    pub fn decimal(&self) -> Result<Decimal, QueryError> {
        Decimal::try_from_i128_with_scale(self.mantissa, u32::from(self.scale))
            .map_err(|_| QueryError::InvalidMonetaryCell)
    }
}

fn array<T: 'static>(batch: &RecordBatch, column: usize) -> Result<&T, QueryError> {
    batch
        .columns()
        .get(column)
        .and_then(|value| value.as_any().downcast_ref::<T>())
        .ok_or(QueryError::InvalidMonetaryCell)
}

fn required_string<T>(batch: &RecordBatch, column: usize, row: usize) -> Result<T, QueryError>
where
    for<'value> T: TryFrom<&'value str>,
{
    let values = array::<StringArray>(batch, column)?;
    if values.is_null(row) {
        return Err(QueryError::InvalidMonetaryCell);
    }
    T::try_from(values.value(row)).map_err(|_| QueryError::InvalidMonetaryCell)
}

fn optional_string<T>(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<Option<T>, QueryError>
where
    for<'value> T: TryFrom<&'value str>,
{
    let values = array::<StringArray>(batch, column)?;
    if values.is_null(row) {
        Ok(None)
    } else {
        T::try_from(values.value(row))
            .map(Some)
            .map_err(|_| QueryError::InvalidMonetaryCell)
    }
}

fn optional_instrument(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<Option<InstrumentId>, QueryError> {
    let values = array::<StringArray>(batch, column)?;
    if values.is_null(row) {
        Ok(None)
    } else {
        values
            .value(row)
            .parse()
            .map(Some)
            .map_err(|_| QueryError::InvalidMonetaryCell)
    }
}

fn required_timestamp(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<Timestamp, QueryError> {
    optional_timestamp(batch, column, row)?.ok_or(QueryError::InvalidMonetaryCell)
}

fn optional_timestamp(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<Option<Timestamp>, QueryError> {
    let values = array::<TimestampNanosecondArray>(batch, column)?;
    Ok(if values.is_null(row) {
        None
    } else {
        Some(Timestamp::from_unix_nanos(values.value(row)))
    })
}

const fn parse_quality(value: &str) -> Option<DataQuality> {
    match value.as_bytes() {
        b"direct_verified" => Some(DataQuality::DirectVerified),
        b"direct_unverified" => Some(DataQuality::DirectUnverified),
        b"official_delayed" => Some(DataQuality::OfficialDelayed),
        b"aggregated" => Some(DataQuality::Aggregated),
        b"indicative" => Some(DataQuality::Indicative),
        b"modeled" => Some(DataQuality::Modeled),
        b"estimated" => Some(DataQuality::Estimated),
        b"stale" => Some(DataQuality::Stale),
        b"quarantined" => Some(DataQuality::Quarantined),
        _ => None,
    }
}

pub(super) fn pinned_object_graph_digest(dataset: &PinnedDataset) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/pinned-query-object-graph/v1");
    hash_manifest(&mut hash, dataset.manifest());
    hash.update([match dataset.generation_kind() {
        GenerationKind::Ingest => 1,
        GenerationKind::Compaction => 2,
        GenerationKind::Derived => 3,
    }]);
    match dataset.build_spec_digest() {
        Some(value) => {
            hash.update([1]);
            hash.update(value.digest().bytes());
        }
        None => hash.update([0]),
    }
    hash.update(
        u64::try_from(dataset.parents().len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for parent in dataset.parents() {
        hash.update([match parent.relation() {
            GenerationParentRelation::AppendPredecessor => 1,
            GenerationParentRelation::CompactionPredecessor => 2,
            GenerationParentRelation::DerivedInput => 3,
        }]);
        hash_manifest(&mut hash, parent.manifest());
    }
    hash.update(
        u64::try_from(dataset.objects().len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for pinned in dataset.objects() {
        hash.update(pinned.artifact_id().as_bytes());
        hash_bytes(&mut hash, pinned.relative_reference().as_bytes());
        let object = pinned.object();
        hash.update(object.content_hash().bytes());
        hash.update(object.row_count().to_be_bytes());
        hash.update(object.size_bytes().to_be_bytes());
        hash.update(object.lineage_digest().bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn hash_manifest(hash: &mut Sha256, manifest: &DatasetManifestRef) {
    hash_bytes(hash, manifest.dataset_id().as_str().as_bytes());
    hash.update(manifest.manifest_version().to_be_bytes());
    hash_bytes(hash, manifest.schema().name().as_bytes());
    hash.update(manifest.schema_version().get().to_be_bytes());
    hash.update(manifest.schema().fingerprint());
    hash_sha256(hash, manifest.content_hash());
}

fn hash_sha256(hash: &mut Sha256, value: Sha256Digest) {
    hash.update(value.bytes());
}

fn hash_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value);
}
