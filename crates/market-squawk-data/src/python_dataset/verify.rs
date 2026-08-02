use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::Path;
use std::time::{Duration, Instant};

use arrow::array::{
    Array as _, Decimal128Array, FixedSizeBinaryArray, Float64Array, TimestampNanosecondArray,
    UInt8Array, UInt32Array,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use market_squawk_domain::Timestamp;
use market_squawk_platform::LocalPaths;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Encoding;
use rusqlite::limits::Limit;
use rusqlite::{
    Connection, OpenFlags, OptionalExtension as _, Transaction, TransactionBehavior, params,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::descriptor::{Descriptor, Object, digest};
use super::{
    PythonDatasetCatalogError, PythonDatasetRow, PythonDatasetSelection, PythonDatasetValue,
    PythonDatasetVerificationLimits, check_control, finish_selection_hash, new_selection_hasher,
    update_selection_hash,
};
use crate::catalog::{exact_catalog_file_binding, verify_integrity, verify_migration_identities};
use crate::schema::{
    BUILD_DIGEST_KEY, DATASET_KEY, DatasetSchemaRegistry, FEATURE_LABEL_SCHEMA_NAME,
    FEATURE_LABEL_SCHEMA_VERSION, POLICY_DIGEST_KEY, SCHEMA_FINGERPRINT_KEY, SCHEMA_NAME_KEY,
    SCHEMA_VERSION_KEY, UNIVERSE_DIGEST_KEY,
};
use crate::{CatalogEndpointIdentity, DatasetArrowBatch, Sha256Digest};

const CONTROL_EXPANSION: usize = 16;
const CONTROL_OVERHEAD: usize = 64 * 1024;
const DECODED_BYTES_PER_ROW: usize = 1_024;
const DECODED_ROW_GROUP_OVERHEAD: usize = 64 * 1024;
const SELECTED_ROW_RETAINED_BYTES: usize = 4_096;
const MAX_ROW_GROUPS: usize = 4_096;
const HASH_BUFFER_BYTES: usize = 64 * 1024;
const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;

type CatalogGenerationRow = (
    Vec<u8>,
    String,
    i64,
    Vec<u8>,
    String,
    i64,
    Vec<u8>,
    i64,
    i64,
);

pub(super) fn verify(
    local_root: &Path,
    export_sha256: Sha256Digest,
    as_of: Timestamp,
    limits: PythonDatasetVerificationLimits,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PythonDatasetSelection, PythonDatasetCatalogError> {
    check_control(deadline, cancellation)?;
    let paths = LocalPaths::open_existing(local_root)?;
    let location = paths.catalog()?.clone();
    let catalog_file = location.open_catalog_file()?;
    let exact_binding =
        exact_catalog_file_binding(&catalog_file.try_clone_file()?, location.path())?;
    let catalog_identity = CatalogEndpointIdentity::try_from_bytes(exact_binding)
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;

    location.validate_for_open()?;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    let mut connection = Connection::open_with_flags(location.path(), flags)?;
    location.validate_for_open()?;
    catalog_file.validate_identity()?;
    let length_limit = i32::try_from(limits.max_bytes().min(1024 * 1024))
        .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
    connection.set_limit(Limit::SQLITE_LIMIT_LENGTH, length_limit)?;
    connection.busy_timeout(
        deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(1)),
    )?;
    connection.pragma_update(None, "trusted_schema", "OFF")?;
    connection.pragma_update(None, "query_only", "ON")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    let token = cancellation.clone();
    connection.progress_handler(
        SQLITE_PROGRESS_OPERATIONS,
        Some(move || token.is_cancelled() || Instant::now() >= deadline),
    )?;
    verify_migration_identities(&connection)?;
    verify_integrity(&connection)?;
    let journal_mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
    check_control(deadline, cancellation)?;
    let descriptor_bytes = admitted_descriptor(
        &transaction,
        export_sha256,
        catalog_identity,
        limits.max_bytes(),
    )?;
    let control_bytes = descriptor_bytes
        .len()
        .checked_mul(CONTROL_EXPANSION)
        .and_then(|value| value.checked_add(CONTROL_OVERHEAD))
        .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
    if control_bytes >= limits.max_bytes() {
        return Err(PythonDatasetCatalogError::LimitExceeded);
    }
    let descriptor = Descriptor::parse(&descriptor_bytes)?;
    let identity = descriptor.identity()?;
    verify_catalog_generation(&transaction, &descriptor)?;
    check_control(deadline, cancellation)?;
    catalog_file.validate_identity()?;
    location.validate_for_open()?;
    let mut hasher = new_selection_hasher(catalog_identity, export_sha256, as_of);
    let mut selected_rows = 0_usize;
    let mut validator = RowSequenceValidator::new(&descriptor)?;
    let artifacts = paths.artifacts()?.clone();
    for object in &descriptor.objects {
        check_control(deadline, cancellation)?;
        verify_object(
            &artifacts,
            object,
            &descriptor,
            as_of,
            limits,
            control_bytes,
            deadline,
            cancellation,
            &mut selected_rows,
            &mut hasher,
            &mut validator,
        )?;
    }
    validator.finish()?;
    check_control(deadline, cancellation)?;
    catalog_file.validate_identity()?;
    location.validate_for_open()?;
    transaction.commit()?;
    connection.progress_handler::<fn() -> bool>(0, None)?;

    Ok(PythonDatasetSelection {
        local_root: paths.root().to_path_buf(),
        identity,
        catalog_identity,
        export_sha256,
        descriptor: descriptor_bytes.into_boxed_slice(),
        selection_sha256: finish_selection_hash(hasher, selected_rows)?,
        selected_rows,
        as_of,
    })
}

fn admitted_descriptor(
    transaction: &Transaction<'_>,
    export_sha256: Sha256Digest,
    catalog_identity: CatalogEndpointIdentity,
    max_bytes: usize,
) -> Result<Vec<u8>, PythonDatasetCatalogError> {
    let retained: Option<(Vec<u8>, Vec<u8>, i64)> = transaction
        .query_row(
            "SELECT catalog_identity, descriptor_json, selection_digest_version
             FROM python_dataset_admissions WHERE export_sha256=?1",
            params![export_sha256.bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((catalog, descriptor, digest_version)) = retained else {
        return Err(PythonDatasetCatalogError::UnknownAdmission);
    };
    if catalog.as_slice() != catalog_identity.bytes()
        || digest_version != 1
        || descriptor.is_empty()
        || descriptor.len() > max_bytes.min(1024 * 1024)
        || Sha256Digest::new(Sha256::digest(&descriptor).into()) != export_sha256
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    Ok(descriptor)
}

fn verify_catalog_generation(
    transaction: &Transaction<'_>,
    descriptor: &Descriptor,
) -> Result<(), PythonDatasetCatalogError> {
    let dataset = &descriptor.dataset;
    let manifest_version = sql_i64(dataset.manifest_version)?;
    let generation: Option<CatalogGenerationRow> = transaction
        .query_row(
            "SELECT content_hash, schema_name, schema_version, schema_fingerprint,
                        generation_kind, parent_count, build_spec_digest, row_count, total_bytes
                 FROM analytical_generations
                 WHERE dataset_id=?1 AND manifest_version=?2",
            params![dataset.dataset_id, manifest_version],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .optional()?;
    let Some((content, schema_name, schema_version, schema, kind, parents, build, rows, bytes)) =
        generation
    else {
        return Err(PythonDatasetCatalogError::UnknownAdmission);
    };
    let descriptor_rows = descriptor.objects.iter().try_fold(0_u64, |total, object| {
        total
            .checked_add(object.row_count)
            .ok_or(PythonDatasetCatalogError::LimitExceeded)
    })?;
    let descriptor_bytes = descriptor.objects.iter().try_fold(0_u64, |total, object| {
        total
            .checked_add(object.size_bytes)
            .ok_or(PythonDatasetCatalogError::LimitExceeded)
    })?;
    if content.as_slice() != digest(&dataset.manifest_sha256)?
        || schema_name != dataset.schema_name
        || u16::try_from(schema_version).ok() != Some(dataset.schema_version)
        || schema.as_slice() != digest(&dataset.schema_sha256)?
        || kind != "derived"
        || usize::try_from(parents).ok() != Some(descriptor.parents.len())
        || build.as_slice() != digest(&dataset.build_spec_sha256)?
        || u64::try_from(rows).ok() != Some(descriptor_rows)
        || u64::try_from(bytes).ok() != Some(descriptor_bytes)
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    verify_catalog_objects(transaction, descriptor)?;
    verify_catalog_parents(transaction, descriptor)
}

fn verify_catalog_objects(
    transaction: &Transaction<'_>,
    descriptor: &Descriptor,
) -> Result<(), PythonDatasetCatalogError> {
    let manifest_version = sql_i64(descriptor.dataset.manifest_version)?;
    let count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM analytical_generation_objects
         WHERE dataset_id=?1 AND manifest_version=?2",
        params![descriptor.dataset.dataset_id, manifest_version],
        |row| row.get(0),
    )?;
    if usize::try_from(count).ok() != Some(descriptor.objects.len()) {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    for (ordinal, object) in descriptor.objects.iter().enumerate() {
        let matches: bool = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM analytical_generation_objects AS object
                JOIN artifacts AS artifact ON artifact.artifact_id=object.artifact_id
                WHERE object.dataset_id=?1 AND object.manifest_version=?2
                  AND object.ordinal=?3 AND object.artifact_id=?4
                  AND object.content_hash=?5 AND object.row_count=?6
                  AND object.size_bytes=?7 AND object.lineage_hash=?8
                  AND artifact.relative_reference=?9
            )",
            params![
                descriptor.dataset.dataset_id,
                manifest_version,
                i64::try_from(ordinal).map_err(|_| PythonDatasetCatalogError::LimitExceeded)?,
                object.artifact_id,
                digest(&object.sha256)?,
                sql_i64(object.row_count)?,
                sql_i64(object.size_bytes)?,
                digest(&object.lineage_sha256)?,
                object.path,
            ],
            |row| row.get(0),
        )?;
        if !matches {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
    }
    Ok(())
}

fn verify_catalog_parents(
    transaction: &Transaction<'_>,
    descriptor: &Descriptor,
) -> Result<(), PythonDatasetCatalogError> {
    let manifest_version = sql_i64(descriptor.dataset.manifest_version)?;
    let count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM analytical_generation_parents
         WHERE child_dataset_id=?1 AND child_manifest_version=?2",
        params![descriptor.dataset.dataset_id, manifest_version],
        |row| row.get(0),
    )?;
    if usize::try_from(count).ok() != Some(descriptor.parents.len()) {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    for (ordinal, parent) in descriptor.parents.iter().enumerate() {
        let manifest = &parent.manifest;
        let matches: bool = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM analytical_generation_parents
                WHERE child_dataset_id=?1 AND child_manifest_version=?2 AND ordinal=?3
                  AND relation=?4 AND parent_dataset_id=?5 AND parent_manifest_version=?6
                  AND parent_schema_name=?7 AND parent_schema_version=?8
                  AND parent_schema_fingerprint=?9 AND parent_content_hash=?10
            )",
            params![
                descriptor.dataset.dataset_id,
                manifest_version,
                i64::try_from(ordinal).map_err(|_| PythonDatasetCatalogError::LimitExceeded)?,
                parent.relation,
                manifest.dataset_id,
                sql_i64(manifest.manifest_version)?,
                manifest.schema_name,
                manifest.schema_version,
                digest(&manifest.schema_sha256)?,
                digest(&manifest.manifest_sha256)?,
            ],
            |row| row.get(0),
        )?;
        if !matches {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "authority, resources, control, and streaming state remain explicit"
)]
fn verify_object(
    artifacts: &market_squawk_platform::ArtifactRoot,
    object: &Object,
    descriptor: &Descriptor,
    as_of: Timestamp,
    limits: PythonDatasetVerificationLimits,
    control_bytes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
    selected_rows: &mut usize,
    selection_hash: &mut Sha256,
    validator: &mut RowSequenceValidator,
) -> Result<(), PythonDatasetCatalogError> {
    let object_digest = digest(&object.sha256)?;
    let encoded = object.sha256.as_str();
    let expected_reference = format!("objects/sha256/{}/{}.parquet", &encoded[..2], encoded);
    if object.path != expected_reference {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let resolved = artifacts.resolve(&object.path)?;
    let mut file = resolved.open_read()?;
    let metadata = file.metadata()?;
    if metadata.len() != object.size_bytes {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let compressed_bytes =
        usize::try_from(object.size_bytes).map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
    if control_bytes
        .checked_add(compressed_bytes)
        .is_none_or(|bytes| bytes >= limits.max_bytes())
    {
        return Err(PythonDatasetCatalogError::LimitExceeded);
    }
    if hash_file(&mut file, deadline, cancellation)? != object_digest {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    file.seek(SeekFrom::Start(0))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();
    if metadata.file_metadata().num_rows() < 1
        || u64::try_from(metadata.file_metadata().num_rows()).ok() != Some(object.row_count)
        || metadata.num_row_groups() == 0
        || metadata.num_row_groups() > MAX_ROW_GROUPS
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    let file_schema = builder.schema().clone();
    validate_arrow_schema(file_schema.as_ref(), descriptor)?;
    let mut maximum_rows = 0_usize;
    for group in metadata.row_groups() {
        let rows = usize::try_from(group.num_rows())
            .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?;
        if rows == 0
            || group.columns().iter().any(|column| {
                column.encodings().any(|encoding| {
                    matches!(
                        encoding,
                        Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
                    )
                })
            })
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        maximum_rows = maximum_rows.max(rows);
    }
    let decoded_bytes = decoded_bound(maximum_rows)?;
    let selected_bytes = selected_rows
        .checked_mul(SELECTED_ROW_RETAINED_BYTES)
        .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
    if control_bytes
        .checked_add(compressed_bytes)
        .and_then(|value| value.checked_add(decoded_bytes))
        .and_then(|value| value.checked_add(selected_bytes))
        .is_none_or(|bytes| bytes > limits.max_bytes())
    {
        return Err(PythonDatasetCatalogError::LimitExceeded);
    }
    let reader = builder.with_batch_size(maximum_rows).build()?;
    let mut rows_read = 0_u64;
    let mut lineage = Sha256::new();
    lineage.update(b"market-squawk/feature-label-object-lineage/v1");
    lineage.update(digest(&descriptor.dataset.build_spec_sha256)?);
    for batch in reader {
        check_control(deadline, cancellation)?;
        let batch = batch?;
        let batch_schema = batch.schema();
        if batch_schema.fields() != file_schema.fields()
            || (!batch_schema.metadata().is_empty()
                && batch_schema.metadata() != file_schema.metadata())
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        // Parquet preserves file-level Arrow metadata on the builder schema but may omit it from
        // emitted batches. Reattach only the already-validated file schema.
        let batch = batch.with_schema(file_schema.clone())?;
        let admitted = DatasetArrowBatch::try_from_record_batch(batch)?;
        let batch = admitted.record_batch();
        validate_arrow_schema(batch.schema().as_ref(), descriptor)?;
        if batch.get_array_memory_size() > decoded_bound(batch.num_rows())? {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        rows_read = rows_read
            .checked_add(
                u64::try_from(batch.num_rows())
                    .map_err(|_| PythonDatasetCatalogError::LimitExceeded)?,
            )
            .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
        for row_index in 0..batch.num_rows() {
            if row_index % 128 == 0 {
                check_control(deadline, cancellation)?;
            }
            let row = row(batch, row_index)?;
            validator.update(&row)?;
            lineage.update(row.lineage);
            if row.cutoff_at <= as_of {
                *selected_rows = selected_rows
                    .checked_add(1)
                    .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
                if *selected_rows > limits.max_rows() {
                    return Err(PythonDatasetCatalogError::LimitExceeded);
                }
                let selected_bytes = selected_rows
                    .checked_mul(SELECTED_ROW_RETAINED_BYTES)
                    .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
                if control_bytes
                    .checked_add(compressed_bytes)
                    .and_then(|value| value.checked_add(decoded_bytes))
                    .and_then(|value| value.checked_add(selected_bytes))
                    .is_none_or(|bytes| bytes > limits.max_bytes())
                {
                    return Err(PythonDatasetCatalogError::LimitExceeded);
                }
                update_selection_hash(selection_hash, &row);
            }
        }
    }
    let observed_lineage: [u8; 32] = lineage.finalize().into();
    if rows_read != object.row_count || observed_lineage != digest(&object.lineage_sha256)? {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    Ok(())
}

fn hash_file(
    file: &mut std::fs::File,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<[u8; 32], PythonDatasetCatalogError> {
    file.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    loop {
        check_control(deadline, cancellation)?;
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(hash.finalize().into());
        }
        hash.update(&buffer[..read]);
    }
}

fn validate_arrow_schema(
    schema: &Schema,
    descriptor: &Descriptor,
) -> Result<(), PythonDatasetCatalogError> {
    let expected_ref = DatasetSchemaRegistry::local()
        .canonical_feature_labels()
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    let expected = DatasetSchemaRegistry::local()
        .resolve(&expected_ref)
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    let metadata = schema.metadata();
    let expected_metadata = [
        (
            BUILD_DIGEST_KEY,
            descriptor.dataset.build_spec_sha256.as_str(),
        ),
        (DATASET_KEY, descriptor.dataset.dataset_id.as_str()),
        (POLICY_DIGEST_KEY, descriptor.dataset.policy_sha256.as_str()),
        (
            SCHEMA_FINGERPRINT_KEY,
            descriptor.dataset.schema_sha256.as_str(),
        ),
        (SCHEMA_NAME_KEY, FEATURE_LABEL_SCHEMA_NAME),
        (
            UNIVERSE_DIGEST_KEY,
            descriptor.dataset.universe_sha256.as_str(),
        ),
    ];
    if schema.fields() != expected.fields()
        || metadata.len() != expected.metadata().len() + 4
        || expected
            .metadata()
            .iter()
            .any(|(key, value)| metadata.get(key) != Some(value))
        || expected_metadata
            .iter()
            .any(|(key, value)| metadata.get(*key).map(String::as_str) != Some(*value))
        || metadata
            .get(SCHEMA_VERSION_KEY)
            .and_then(|value| value.parse::<u16>().ok())
            != Some(FEATURE_LABEL_SCHEMA_VERSION)
    {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    Ok(())
}

fn decoded_bound(rows: usize) -> Result<usize, PythonDatasetCatalogError> {
    rows.checked_mul(DECODED_BYTES_PER_ROW)
        .and_then(|value| value.checked_add(DECODED_ROW_GROUP_OVERHEAD))
        .ok_or(PythonDatasetCatalogError::LimitExceeded)
}

fn sql_i64(value: u64) -> Result<i64, PythonDatasetCatalogError> {
    i64::try_from(value).map_err(|_| PythonDatasetCatalogError::LimitExceeded)
}

fn row(batch: &RecordBatch, index: usize) -> Result<PythonDatasetRow, PythonDatasetCatalogError> {
    let fixed = |name| {
        batch
            .column_by_name(name)
            .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or(PythonDatasetCatalogError::CorruptAdmission)
    };
    let example = padded_text(fixed("example_id")?, index)?;
    let instrument: [u8; 16] = fixed("instrument_id")?
        .value(index)
        .try_into()
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    let cutoff = batch
        .column_by_name("cutoff_at")
        .and_then(|array| array.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?
        .value(index);
    let split = uint8(batch, "split")?.value(index);
    let kind = uint8(batch, "component_kind")?.value(index);
    let name = padded_text(fixed("component_name")?, index)?;
    let version = batch
        .column_by_name("component_version")
        .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?
        .value(index);
    let floats = batch
        .column_by_name("value_f64")
        .and_then(|array| array.as_any().downcast_ref::<Float64Array>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
    let decimals = batch
        .column_by_name("value_decimal_mantissa")
        .and_then(|array| array.as_any().downcast_ref::<Decimal128Array>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)?;
    let scales = uint8(batch, "value_decimal_scale")?;
    let missing = fixed("missing_reason")?;
    let value = if !floats.is_null(index) {
        PythonDatasetValue::Float(floats.value(index))
    } else if !decimals.is_null(index) {
        if scales.is_null(index) {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        PythonDatasetValue::Decimal {
            mantissa: decimals.value(index),
            scale: scales.value(index),
        }
    } else if !missing.is_null(index) {
        PythonDatasetValue::Missing(padded_text(missing, index)?.into())
    } else {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    };
    let unit = optional_padded(fixed("unit")?, index)?;
    let currency = optional_padded(fixed("currency")?, index)?;
    let lineage: [u8; 32] = fixed("lineage_sha256")?
        .value(index)
        .try_into()
        .map_err(|_| PythonDatasetCatalogError::CorruptAdmission)?;
    PythonDatasetRow::try_new(
        example,
        instrument,
        Timestamp::from_unix_nanos(cutoff),
        split,
        kind,
        name,
        version,
        value,
        unit,
        currency,
        lineage,
    )
}

fn uint8<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a UInt8Array, PythonDatasetCatalogError> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
        .ok_or(PythonDatasetCatalogError::CorruptAdmission)
}

fn optional_padded(
    array: &FixedSizeBinaryArray,
    index: usize,
) -> Result<Option<&str>, PythonDatasetCatalogError> {
    if array.is_null(index) {
        Ok(None)
    } else {
        padded_text(array, index).map(Some)
    }
}

fn padded_text(
    array: &FixedSizeBinaryArray,
    index: usize,
) -> Result<&str, PythonDatasetCatalogError> {
    let bytes = array.value(index);
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    if end == 0 || bytes[end..].iter().any(|byte| *byte != 0) {
        return Err(PythonDatasetCatalogError::CorruptAdmission);
    }
    std::str::from_utf8(&bytes[..end]).map_err(|_| PythonDatasetCatalogError::CorruptAdmission)
}

struct RowSequenceValidator {
    components: Vec<(u8, String, u32)>,
    boundaries: [i64; 3],
    expected_counts: [usize; 3],
    observed_counts: [usize; 3],
    current_key: Option<(i64, [u8; 16], String, u8)>,
    previous_key: Option<(i64, [u8; 16], String)>,
    component_index: usize,
}

impl RowSequenceValidator {
    fn new(descriptor: &Descriptor) -> Result<Self, PythonDatasetCatalogError> {
        let components = descriptor
            .components
            .iter()
            .map(|component| {
                Ok((
                    match component.kind.as_str() {
                        "feature" => 1,
                        "label" => 2,
                        _ => return Err(PythonDatasetCatalogError::CorruptAdmission),
                    },
                    component.name.clone(),
                    component.version,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            components,
            boundaries: [
                descriptor.split_policy.train_end_unix_nanos,
                descriptor.split_policy.validation_end_unix_nanos,
                descriptor.split_policy.test_end_unix_nanos,
            ],
            expected_counts: [
                descriptor.split_counts.train,
                descriptor.split_counts.validation,
                descriptor.split_counts.test,
            ],
            observed_counts: [0; 3],
            current_key: None,
            previous_key: None,
            component_index: 0,
        })
    }

    fn update(&mut self, row: &PythonDatasetRow) -> Result<(), PythonDatasetCatalogError> {
        let split = if row.cutoff_at.unix_nanos() <= self.boundaries[0] {
            1
        } else if row.cutoff_at.unix_nanos() <= self.boundaries[1] {
            2
        } else if row.cutoff_at.unix_nanos() <= self.boundaries[2] {
            3
        } else {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        };
        if row.split != split
            || self
                .components
                .get(self.component_index)
                .is_none_or(|expected| {
                    expected.0 != row.component_kind
                        || expected.1 != row.component_name.as_ref()
                        || expected.2 != row.component_version
                })
        {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        let group = (
            row.cutoff_at.unix_nanos(),
            row.instrument_id,
            row.example_id.to_string(),
            row.split,
        );
        if self.component_index == 0 {
            let ordering = (group.0, group.1, group.2.clone());
            if self
                .previous_key
                .as_ref()
                .is_some_and(|previous| previous >= &ordering)
            {
                return Err(PythonDatasetCatalogError::CorruptAdmission);
            }
            self.previous_key = Some(ordering);
            self.current_key = Some(group);
        } else if self.current_key.as_ref() != Some(&group) {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        self.component_index += 1;
        if self.component_index == self.components.len() {
            self.observed_counts[usize::from(row.split - 1)] = self.observed_counts
                [usize::from(row.split - 1)]
            .checked_add(1)
            .ok_or(PythonDatasetCatalogError::LimitExceeded)?;
            self.component_index = 0;
            self.current_key = None;
        }
        Ok(())
    }

    fn finish(self) -> Result<(), PythonDatasetCatalogError> {
        if self.component_index != 0 || self.observed_counts != self.expected_counts {
            return Err(PythonDatasetCatalogError::CorruptAdmission);
        }
        Ok(())
    }
}
