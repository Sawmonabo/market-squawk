//! Bounded, disk-backed ZIP admission and sequential typed TSV projection.

use std::cell::Cell;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::io::{BufRead, BufReader, ErrorKind, Read, Seek, Write};
use std::path::Path;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::NaiveDate;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use zip::{CompressionMethod, ZipArchive};

use crate::RawEvidenceStore;
use crate::evidence_store::RawEvidenceScratch;

use super::SecBulkError;
use super::model::{
    SecBulkCanonicalProjection, SecBulkCapture, SecBulkColumnContract,
    SecBulkDeclaredTableContract, SecBulkFamily, SecBulkJoinCoordinate, SecBulkJoinDomain,
    SecBulkKeyField, SecBulkLayoutManifest, SecBulkNativeRow, SecBulkNumericAttribute,
    SecBulkTableKind, SecBulkTableReceipt, SecBulkTypedField, SecBulkTypedValue, SecExactNumber,
    SecNcenEtfRow, SecNcenFundRow, SecNcenRegistrantRow, SecNcenSecurityExchangeRow,
    SecNcenSubmissionRow, SecNportFundRow, SecNportHoldingRow, SecNportIdentifierRow,
    SecNportRegistrantRow, SecNportSubmissionRow,
};

const MAX_ARCHIVE_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_ARCHIVE_ENTRIES: usize = 64;
const MAX_METADATA_BYTES: u64 = 8 * 1024 * 1024;
const MAX_README_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TABLE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_EXPANDED_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const MAX_COMPRESSION_RATIO: u64 = 256;
const MAX_COLUMNS: usize = 512;
const MAX_ROWS_PER_TABLE: u64 = 250_000_000;
const MAX_FIELD_BYTES: usize = 1024 * 1024;
const MAX_ROW_BYTES: usize = 4 * 1024 * 1024;
const READ_BUFFER_BYTES: usize = 64 * 1024;
const KEY_DIGEST_BYTES: usize = 32;
const KEY_DIGEST_BYTES_U64: u64 = 32;
const KEY_SORT_CHUNK_KEYS_U64: u64 = 131_072;
const KEY_MERGE_FAN_IN: usize = 16;
const MAX_VALIDATION_SCRATCH_BYTES: u64 = 96 * 1024 * 1024 * 1024;

/// Hard ceilings for streamed SEC quarterly archive admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecBulkParseLimits {
    max_archive_bytes: u64,
    max_archive_entries: usize,
    max_metadata_bytes: u64,
    max_readme_bytes: u64,
    max_table_bytes: u64,
    max_expanded_bytes: u64,
    max_compression_ratio: u64,
    max_columns: usize,
    max_rows_per_table: u64,
    max_field_bytes: usize,
    max_row_bytes: usize,
    max_validation_scratch_bytes: u64,
}

impl SecBulkParseLimits {
    /// Production ceilings sized for the SEC's current N-PORT catalogue while remaining finite.
    pub const fn production_defaults() -> Self {
        Self {
            max_archive_bytes: MAX_ARCHIVE_BYTES,
            max_archive_entries: MAX_ARCHIVE_ENTRIES,
            max_metadata_bytes: MAX_METADATA_BYTES,
            max_readme_bytes: MAX_README_BYTES,
            max_table_bytes: MAX_TABLE_BYTES,
            max_expanded_bytes: MAX_EXPANDED_BYTES,
            max_compression_ratio: MAX_COMPRESSION_RATIO,
            max_columns: MAX_COLUMNS,
            max_rows_per_table: MAX_ROWS_PER_TABLE,
            max_field_bytes: MAX_FIELD_BYTES,
            max_row_bytes: MAX_ROW_BYTES,
            max_validation_scratch_bytes: MAX_VALIDATION_SCRATCH_BYTES,
        }
    }

    /// Returns the maximum compressed response bytes.
    pub const fn max_archive_bytes(self) -> u64 {
        self.max_archive_bytes
    }
}

/// Atomic sequential native-row consumer.
///
/// `stage` must write only to a non-visible generation. The scanner first validates every typed
/// table without touching the sink, then drives exactly `begin -> stage* -> commit`; any failure
/// after `begin` drives `abort`. This makes partial publication impossible for conforming sinks.
pub trait SecBulkRowSink {
    /// Opens a non-visible staging generation for one exact manifest.
    fn begin(&mut self, manifest_evidence: EvidenceDigest) -> Result<(), SecBulkError>;

    /// Stages one bounded provider-native row without making it query-visible.
    fn stage(&mut self, row: SecBulkNativeRow) -> Result<(), SecBulkError>;

    /// Atomically publishes the fully staged generation and its exact scan receipt.
    fn commit(&mut self, report: SecBulkScanReport) -> Result<(), SecBulkError>;

    /// Discards every staged row; must be idempotent.
    fn abort(&mut self);
}

/// Successful sequential scan closure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecBulkScanReport {
    manifest_evidence: EvidenceDigest,
    source_rows: u64,
    emitted_typed_rows: u64,
}

impl SecBulkScanReport {
    /// Returns the exact inspected layout identity.
    pub const fn manifest_evidence(self) -> EvidenceDigest {
        self.manifest_evidence
    }

    /// Returns rows across every exact metadata-declared TSV table.
    pub const fn source_rows(self) -> u64 {
        self.source_rows
    }

    /// Returns rows emitted through currently typed canonical/provider handoffs.
    pub const fn emitted_typed_rows(self) -> u64 {
        self.emitted_typed_rows
    }
}

/// Inspects every member from a disk-backed sealed archive and builds an exact layout manifest.
pub fn inspect_bulk_archive(
    store: &RawEvidenceStore,
    capture: SecBulkCapture,
    official_readme_capture: SecBulkCapture,
    limits: SecBulkParseLimits,
    deadline: market_squawk_domain::Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkLayoutManifest, SecBulkError> {
    check_cancelled(cancellation, deadline)?;
    let verified_readme = store.open_verified_before(
        &official_readme_capture.evidence(),
        official_readme_capture.size_bytes(),
        limits.max_readme_bytes,
        deadline,
        cancellation,
    )?;
    drop(verified_readme);
    let file = store.open_verified_before(
        &capture.evidence(),
        capture.size_bytes(),
        limits.max_archive_bytes,
        deadline,
        cancellation,
    )?;
    inspect_file(
        store,
        file,
        capture,
        official_readme_capture,
        limits,
        deadline,
        cancellation,
    )
}

/// Reopens and re-inspects a sealed archive, accepting recovery only on exact manifest equality.
pub fn recover_bulk_archive(
    store: &RawEvidenceStore,
    expected: &SecBulkLayoutManifest,
    limits: SecBulkParseLimits,
    deadline: market_squawk_domain::Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkLayoutManifest, SecBulkError> {
    let recovered = inspect_bulk_archive(
        store,
        expected.capture().clone(),
        expected.official_readme_capture().clone(),
        limits,
        deadline,
        cancellation,
    )?;
    if recovered != *expected {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(recovered)
}

/// Reopens a sealed archive and emits only closed typed rows, one at a time.
pub fn scan_bulk_archive(
    store: &RawEvidenceStore,
    manifest: &SecBulkLayoutManifest,
    limits: SecBulkParseLimits,
    deadline: market_squawk_domain::Timestamp,
    cancellation: &CancellationToken,
    sink: &mut impl SecBulkRowSink,
) -> Result<SecBulkScanReport, SecBulkError> {
    check_cancelled(cancellation, deadline)?;
    let file = store.open_verified_before(
        &manifest.capture().evidence(),
        manifest.capture().size_bytes(),
        limits.max_archive_bytes,
        deadline,
        cancellation,
    )?;
    let mut archive = ZipArchive::new(file).map_err(|_| SecBulkError::UnsafeArchive)?;
    inspect_zip_structure(
        &mut archive,
        manifest.capture().selection().family(),
        limits,
        deadline,
        cancellation,
    )?;
    // Pass one proves all typed value contracts before a sink can even begin staging.
    let mut validated = 0_u64;
    for receipt in manifest.tables() {
        check_cancelled(cancellation, deadline)?;
        let mut entry = archive
            .by_name(receipt.name().as_str())
            .map_err(|_| SecBulkError::RecoveryMismatch)?;
        let observed = project_typed_table(
            manifest.capture().selection().family(),
            receipt,
            &mut entry,
            limits,
            deadline,
            cancellation,
            &mut |_row| Ok(()),
        )
        .map_err(|error| table_scan_error(receipt, error))?;
        validated = validated
            .checked_add(observed)
            .ok_or(SecBulkError::TsvLimitExceeded)?;
    }
    let source_rows = manifest.tables().iter().try_fold(0_u64, |total, table| {
        total
            .checked_add(table.row_count())
            .ok_or(SecBulkError::TsvLimitExceeded)
    })?;
    let report = SecBulkScanReport {
        manifest_evidence: manifest.evidence(),
        source_rows,
        emitted_typed_rows: validated,
    };

    sink.begin(manifest.evidence())?;
    let staged = (|| {
        let mut emitted = 0_u64;
        for receipt in manifest.tables() {
            check_cancelled(cancellation, deadline)?;
            let mut entry = archive
                .by_name(receipt.name().as_str())
                .map_err(|_| SecBulkError::RecoveryMismatch)?;
            let observed = project_typed_table(
                manifest.capture().selection().family(),
                receipt,
                &mut entry,
                limits,
                deadline,
                cancellation,
                &mut |row| sink.stage(row),
            )
            .map_err(|error| table_scan_error(receipt, error))?;
            emitted = emitted
                .checked_add(observed)
                .ok_or(SecBulkError::TsvLimitExceeded)?;
        }
        if emitted != report.emitted_typed_rows {
            return Err(SecBulkError::RecoveryMismatch);
        }
        sink.commit(report)
    })();
    if let Err(error) = staged {
        sink.abort();
        return Err(error);
    }
    Ok(report)
}

fn table_scan_error(receipt: &SecBulkTableReceipt, error: SecBulkError) -> SecBulkError {
    if matches!(error, SecBulkError::HeaderMismatch) {
        SecBulkError::TableHeaderMismatch(receipt.name().clone())
    } else {
        error
    }
}

fn inspect_file(
    store: &RawEvidenceStore,
    file: std::fs::File,
    capture: SecBulkCapture,
    official_readme_capture: SecBulkCapture,
    limits: SecBulkParseLimits,
    deadline: market_squawk_domain::Timestamp,
    cancellation: &CancellationToken,
) -> Result<SecBulkLayoutManifest, SecBulkError> {
    check_cancelled(cancellation, deadline)?;
    if capture.size_bytes() > limits.max_archive_bytes {
        return Err(SecBulkError::ArchiveTooLarge);
    }
    let family = capture.selection().family();
    let mut archive = ZipArchive::new(file).map_err(|_| SecBulkError::UnsafeArchive)?;
    let structure = inspect_zip_structure(&mut archive, family, limits, deadline, cancellation)?;
    let metadata_name = family.metadata_member();
    let readme_name = family.archive_readme_member();
    let metadata_bytes = read_small_member(
        &mut archive,
        metadata_name,
        limits.max_metadata_bytes,
        deadline,
        cancellation,
    )?;
    let readme_bytes = read_small_member(
        &mut archive,
        readme_name,
        limits.max_readme_bytes,
        deadline,
        cancellation,
    )?;
    let metadata: CsvwMetadata = serde_json::from_slice(&metadata_bytes)?;
    let tables = validate_metadata(&metadata, family, limits)?;
    let declared_table_contracts = tables
        .iter()
        .map(declared_table_contract)
        .collect::<Result<Vec<_>, _>>()?;
    let declared_names = tables
        .iter()
        .map(|table| table.url.as_str())
        .collect::<BTreeSet<_>>();
    if structure.names.iter().any(|name| {
        name != metadata_name && name != readme_name && !declared_names.contains(name.as_str())
    }) {
        return Err(SecBulkError::InvalidLayout);
    }

    let required = required_table_names(family);
    if required.iter().any(|name| !structure.names.contains(*name)) {
        return Err(SecBulkError::MissingRequiredTable);
    }

    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(tables.len())
        .map_err(|_| SecBulkError::AllocationFailed)?;
    let mut absent_declared_tables = Vec::new();
    absent_declared_tables
        .try_reserve_exact(tables.len().saturating_sub(structure.names.len()))
        .map_err(|_| SecBulkError::AllocationFailed)?;
    let mut integrity = ArchiveIntegrityValidator::new(store, family, limits)?;
    for table in &tables {
        if !structure.names.contains(&table.url) {
            absent_declared_tables.push(SourceIdentifier::try_from(table.url.as_str())?);
            continue;
        }
        check_cancelled(cancellation, deadline)?;
        let mut entry = archive
            .by_name(&table.url)
            .map_err(|_| SecBulkError::InvalidLayout)?;
        let (evidence, decoded_bytes, row_count) = inspect_tsv(
            &mut entry,
            table,
            &mut integrity,
            limits,
            deadline,
            cancellation,
        )?;
        let primary_key = table
            .primary_key
            .iter()
            .map(|key| SourceIdentifier::try_from(key.as_str()))
            .collect::<Result<Vec<_>, _>>()?;
        let columns = table
            .columns
            .iter()
            .map(column_contract)
            .collect::<Result<Vec<_>, _>>()?;
        receipts.push(SecBulkTableReceipt::try_new(
            SourceIdentifier::try_from(table.url.as_str())?,
            evidence,
            decoded_bytes,
            row_count,
            primary_key,
            columns,
        )?);
    }
    integrity.finish(deadline, cancellation)?;
    SecBulkLayoutManifest::try_new(
        capture,
        official_readme_capture,
        sha256(&metadata_bytes),
        sha256(&readme_bytes),
        declared_table_contracts,
        receipts,
        absent_declared_tables,
        structure.expanded_bytes,
    )
}

struct ArchiveStructure {
    names: BTreeSet<String>,
    expanded_bytes: u64,
}

fn inspect_zip_structure<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    family: SecBulkFamily,
    limits: SecBulkParseLimits,
    deadline: market_squawk_domain::Timestamp,
    cancellation: &CancellationToken,
) -> Result<ArchiveStructure, SecBulkError> {
    check_cancelled(cancellation, deadline)?;
    if archive.offset() != 0 || archive.len() == 0 || archive.len() > limits.max_archive_entries {
        return Err(SecBulkError::EntryLimitExceeded);
    }
    if archive
        .has_overlapping_files()
        .map_err(|_| SecBulkError::UnsafeArchive)?
    {
        return Err(SecBulkError::UnsafeArchive);
    }
    let mut names = BTreeSet::new();
    let mut portable_names = BTreeSet::new();
    let mut expanded_bytes = 0_u64;
    for index in 0..archive.len() {
        check_cancelled(cancellation, deadline)?;
        let entry = archive
            .by_index(index)
            .map_err(|_| SecBulkError::UnsafeArchive)?;
        validate_member(&entry, family)?;
        let maximum = if entry.name() == family.metadata_member() {
            limits.max_metadata_bytes
        } else if entry.name() == family.archive_readme_member() {
            limits.max_readme_bytes
        } else {
            limits.max_table_bytes
        };
        if entry.size() > maximum {
            return Err(SecBulkError::EntryByteLimitExceeded);
        }
        validate_compression_ratio(
            entry.size(),
            entry.compressed_size(),
            limits.max_compression_ratio,
        )?;
        expanded_bytes = expanded_bytes
            .checked_add(entry.size())
            .ok_or(SecBulkError::ExpandedByteLimitExceeded)?;
        if expanded_bytes > limits.max_expanded_bytes {
            return Err(SecBulkError::ExpandedByteLimitExceeded);
        }
        let name = entry.name().to_owned();
        if !portable_names.insert(name.to_ascii_lowercase()) || !names.insert(name) {
            return Err(SecBulkError::UnsafeArchive);
        }
    }
    Ok(ArchiveStructure {
        names,
        expanded_bytes,
    })
}

fn validate_member<R: Read>(
    entry: &zip::read::ZipFile<'_, R>,
    family: SecBulkFamily,
) -> Result<(), SecBulkError> {
    let name = entry.name();
    let path = entry.enclosed_name().ok_or(SecBulkError::UnsafeArchive)?;
    let admitted_name = name == family.metadata_member()
        || name == family.archive_readme_member()
        || valid_table_member_name(name);
    if path != Path::new(name)
        || name.contains(['/', '\\', ':', '\0'])
        || !name.is_ascii()
        || entry.encrypted()
        || entry.is_symlink()
        || entry.is_dir()
        || !entry.is_file()
        || !matches!(
            entry.compression(),
            CompressionMethod::Stored | CompressionMethod::Deflated
        )
        || !admitted_name
    {
        Err(SecBulkError::UnsafeArchive)
    } else {
        Ok(())
    }
}

fn valid_table_member_name(name: &str) -> bool {
    name.strip_suffix(".tsv").is_some_and(|stem| {
        !stem.is_empty()
            && stem.len() <= 128
            && stem
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    })
}

fn validate_compression_ratio(
    expanded: u64,
    compressed: u64,
    maximum: u64,
) -> Result<(), SecBulkError> {
    if expanded == 0 {
        return Ok(());
    }
    if compressed == 0
        || expanded
            > compressed
                .checked_mul(maximum)
                .ok_or(SecBulkError::CompressionRatioExceeded)?
    {
        Err(SecBulkError::CompressionRatioExceeded)
    } else {
        Ok(())
    }
}

fn read_small_member<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    name: &str,
    maximum: u64,
    deadline: market_squawk_domain::Timestamp,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, SecBulkError> {
    let mut entry = archive
        .by_name(name)
        .map_err(|_| SecBulkError::InvalidLayout)?;
    if entry.size() == 0 || entry.size() > maximum {
        return Err(SecBulkError::EntryByteLimitExceeded);
    }
    let capacity = usize::try_from(entry.size()).map_err(|_| SecBulkError::AllocationFailed)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| SecBulkError::AllocationFailed)?;
    let mut buffer = [0_u8; READ_BUFFER_BYTES];
    while bytes.len() < capacity {
        check_cancelled(cancellation, deadline)?;
        let read = entry
            .read(&mut buffer)
            .map_err(|_| SecBulkError::UnsafeArchive)?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    let mut extra = [0_u8; 1];
    if bytes.len() != capacity
        || entry
            .read(&mut extra)
            .map_err(|_| SecBulkError::UnsafeArchive)?
            != 0
    {
        return Err(SecBulkError::UnsafeArchive);
    }
    Ok(bytes)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CsvwMetadata {
    #[serde(rename = "@context")]
    context: String,
    dialect: CsvwDialect,
    tables: Vec<CsvwTable>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CsvwDialect {
    header: bool,
    #[serde(rename = "headerRowCount")]
    header_row_count: u8,
    delimiter: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CsvwTable {
    url: String,
    #[serde(rename = "tableSchema")]
    table_schema: CsvwTableSchema,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CsvwTableSchema {
    #[serde(rename = "aboutUrl")]
    about_url: String,
    #[serde(rename = "PrimaryKey", default)]
    primary_key: Vec<String>,
    columns: Vec<CsvwColumn>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CsvwColumn {
    name: String,
    titles: String,
    datatype: CsvwDatatype,
    #[serde(rename = "dc:description")]
    description: String,
    required: Option<bool>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CsvwDatatype {
    base: String,
    #[serde(rename = "maxLength")]
    max_length: Option<u64>,
    #[serde(rename = "dataPrecision")]
    data_precision: Option<CsvwNumericAttribute>,
    #[serde(rename = "dataScale")]
    data_scale: Option<CsvwNumericAttribute>,
}

#[derive(Clone, Deserialize)]
#[serde(untagged)]
enum CsvwNumericAttribute {
    Value(u64),
    ProviderNull(String),
}

struct AdmittedTable {
    url: String,
    primary_key: Vec<String>,
    columns: Vec<CsvwColumn>,
}

fn validate_metadata(
    metadata: &CsvwMetadata,
    family: SecBulkFamily,
    limits: SecBulkParseLimits,
) -> Result<Vec<AdmittedTable>, SecBulkError> {
    if metadata.context != "http://www.w3.org/ns/csvw"
        || !metadata.dialect.header
        || metadata.dialect.header_row_count != 1
        || metadata.dialect.delimiter != "\t"
        || metadata.tables.is_empty()
        || metadata.tables.len().saturating_add(2) > limits.max_archive_entries
    {
        return Err(SecBulkError::InvalidMetadata);
    }
    let mut names = BTreeSet::new();
    let mut admitted = Vec::new();
    admitted
        .try_reserve_exact(metadata.tables.len())
        .map_err(|_| SecBulkError::AllocationFailed)?;
    for table in &metadata.tables {
        SecBulkTableKind::from_member(family, &table.url)?;
        if !valid_table_member_name(&table.url)
            || !names.insert(table.url.as_str())
            || table.table_schema.about_url != family.archive_readme_member()
            || table.table_schema.columns.is_empty()
            || table.table_schema.columns.len() > limits.max_columns
        {
            return Err(SecBulkError::InvalidMetadata);
        }
        let mut columns = BTreeSet::new();
        for column in &table.table_schema.columns {
            if !valid_column_name(&column.name)
                || !columns.insert(column.name.as_str())
                || column.titles.is_empty()
                || column.titles.len() > 1024
                || column.description.len() > 16 * 1024
                || column.required == Some(false)
                || column_contract(column).is_err()
            {
                return Err(SecBulkError::InvalidMetadata);
            }
        }
        let mut primary_keys = BTreeSet::new();
        for key in &table.table_schema.primary_key {
            if !columns.contains(key.as_str()) || !primary_keys.insert(key.as_str()) {
                return Err(SecBulkError::InvalidMetadata);
            }
        }
        admitted.push(AdmittedTable {
            url: table.url.clone(),
            primary_key: table.table_schema.primary_key.clone(),
            columns: table.table_schema.columns.clone(),
        });
    }
    if required_table_names(family)
        .iter()
        .any(|name| !names.contains(name))
        || names.len()
            != match family {
                SecBulkFamily::Nport => 30,
                SecBulkFamily::Ncen => 53,
            }
    {
        return Err(SecBulkError::MissingRequiredTable);
    }
    Ok(admitted)
}

fn required_table_names(family: SecBulkFamily) -> &'static [&'static str] {
    match family {
        SecBulkFamily::Nport => &[
            "SUBMISSION.tsv",
            "REGISTRANT.tsv",
            "FUND_REPORTED_INFO.tsv",
            "FUND_REPORTED_HOLDING.tsv",
            "IDENTIFIERS.tsv",
        ],
        SecBulkFamily::Ncen => &["SUBMISSION.tsv", "REGISTRANT.tsv", "FUND_REPORTED_INFO.tsv"],
    }
}

fn column_contract(column: &CsvwColumn) -> Result<SecBulkColumnContract, SecBulkError> {
    SecBulkColumnContract::try_new(
        SourceIdentifier::try_from(column.name.as_str())?,
        column.datatype.base.clone(),
        column.datatype.max_length,
        numeric_attribute(column.datatype.data_precision.as_ref())?,
        numeric_attribute(column.datatype.data_scale.as_ref())?,
        column.required.unwrap_or(false),
    )
}

fn declared_table_contract(
    table: &AdmittedTable,
) -> Result<SecBulkDeclaredTableContract, SecBulkError> {
    let primary_key = table
        .primary_key
        .iter()
        .map(|key| SourceIdentifier::try_from(key.as_str()))
        .collect::<Result<Vec<_>, _>>()?;
    let columns = table
        .columns
        .iter()
        .map(column_contract)
        .collect::<Result<Vec<_>, _>>()?;
    SecBulkDeclaredTableContract::try_new(
        SourceIdentifier::try_from(table.url.as_str())?,
        primary_key,
        columns,
    )
}

fn numeric_attribute(
    value: Option<&CsvwNumericAttribute>,
) -> Result<Option<SecBulkNumericAttribute>, SecBulkError> {
    match value {
        None => Ok(None),
        Some(CsvwNumericAttribute::Value(value)) => {
            Ok(Some(SecBulkNumericAttribute::Value(*value)))
        }
        Some(CsvwNumericAttribute::ProviderNull(value)) if value == "NULL" => {
            Ok(Some(SecBulkNumericAttribute::ProviderNull))
        }
        Some(CsvwNumericAttribute::ProviderNull(_)) => Err(SecBulkError::InvalidMetadata),
    }
}

fn valid_column_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn inspect_tsv<R: Read>(
    reader: R,
    table: &AdmittedTable,
    integrity: &mut ArchiveIntegrityValidator<'_>,
    limits: SecBulkParseLimits,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<(EvidenceDigest, u64, u64), SecBulkError> {
    let hashing = HashingReader::new(reader, limits.max_table_bytes);
    let mut tsv = BoundedTsvReader::new(hashing, limits);
    let headers = tsv
        .next_record(deadline, cancellation)?
        .ok_or(SecBulkError::HeaderMismatch)?;
    validate_headers(&headers, &table.columns)?;
    let primary_indexes = table
        .primary_key
        .iter()
        .map(|key| {
            table
                .columns
                .iter()
                .position(|column| column.name == *key)
                .ok_or(SecBulkError::InvalidMetadata)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut primary_keys = if primary_indexes.is_empty() {
        None
    } else {
        Some(KeySpool::new(
            integrity.store(),
            integrity.scratch_budget(),
        )?)
    };
    let mut rows = 0_u64;
    while let Some(record) = tsv.next_record(deadline, cancellation)? {
        check_cancelled(cancellation, deadline)?;
        if record.len() != table.columns.len() {
            return Err(SecBulkError::InvalidTsv);
        }
        validate_record_contract(&record, &table.columns)?;
        rows = rows.checked_add(1).ok_or(SecBulkError::TsvLimitExceeded)?;
        if rows > limits.max_rows_per_table {
            return Err(SecBulkError::TsvLimitExceeded);
        }
        if let Some(keys) = &mut primary_keys {
            keys.push_record(table.url.as_bytes(), &record, &primary_indexes)?;
        }
        integrity.observe(&table.url, &table.columns, &record)?;
    }
    let hashing = tsv.into_inner();
    let (evidence, bytes) = hashing.finish()?;
    if let Some(keys) = primary_keys {
        drop(keys.seal(integrity.store(), true, deadline, cancellation)?);
    }
    Ok((evidence, bytes, rows))
}

fn validate_record_contract(
    record: &TsvRecord,
    columns: &[CsvwColumn],
) -> Result<(), SecBulkError> {
    for (value, column) in record.iter().zip(columns) {
        validate_cell(
            value,
            &column.datatype.base,
            column.datatype.max_length,
            numeric_attribute(column.datatype.data_precision.as_ref())?,
            numeric_attribute(column.datatype.data_scale.as_ref())?,
            column.required.unwrap_or(false),
        )?;
    }
    Ok(())
}

fn validate_receipt_record_contract(
    record: &TsvRecord,
    columns: &[SecBulkColumnContract],
) -> Result<(), SecBulkError> {
    for (value, column) in record.iter().zip(columns) {
        validate_cell(
            value,
            column.datatype_base(),
            column.max_length(),
            column.data_precision(),
            column.data_scale(),
            column.required(),
        )?;
    }
    Ok(())
}

fn validate_cell(
    value: &str,
    datatype: &str,
    max_length: Option<u64>,
    precision: Option<SecBulkNumericAttribute>,
    scale: Option<SecBulkNumericAttribute>,
    required: bool,
) -> Result<(), SecBulkError> {
    if value.is_empty() {
        return if required {
            Err(SecBulkError::InvalidTsv)
        } else {
            Ok(())
        };
    }
    if max_length.is_some_and(|maximum| {
        u64::try_from(value.chars().count()).map_or(true, |observed| observed > maximum)
    }) {
        return Err(SecBulkError::InvalidTsv);
    }
    match datatype {
        "string" => Ok(()),
        "date (DD-MON-YYYY)" => parse_sec_date(value).map(|_| ()),
        "NUMBER" => validate_fixed_number(value, precision, scale),
        _ => Err(SecBulkError::InvalidMetadata),
    }
}

fn validate_fixed_number(
    value: &str,
    precision: Option<SecBulkNumericAttribute>,
    scale: Option<SecBulkNumericAttribute>,
) -> Result<(), SecBulkError> {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    if unsigned.is_empty() || value.starts_with('+') {
        return Err(SecBulkError::InvalidTsv);
    }
    let mut parts = unsigned.split('.');
    let integer = parts.next().ok_or(SecBulkError::InvalidTsv)?;
    let fraction = parts.next();
    if parts.next().is_some()
        || (integer.is_empty() && fraction.is_none_or(str::is_empty))
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.is_some_and(|digits| {
            digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(SecBulkError::InvalidTsv);
    }
    match (precision, scale) {
        (
            Some(SecBulkNumericAttribute::Value(maximum_digits)),
            Some(SecBulkNumericAttribute::Value(maximum_scale)),
        ) => {
            let fractional_digits = fraction.map_or(0, str::len);
            let total_digits = integer
                .len()
                .checked_add(fractional_digits)
                .ok_or(SecBulkError::InvalidTsv)?;
            if u64::try_from(total_digits).map_or(true, |digits| digits > maximum_digits)
                || u64::try_from(fractional_digits).map_or(true, |digits| digits > maximum_scale)
            {
                return Err(SecBulkError::InvalidTsv);
            }
            Ok(())
        }
        (
            Some(SecBulkNumericAttribute::ProviderNull),
            Some(SecBulkNumericAttribute::ProviderNull),
        ) => Ok(()),
        _ => Err(SecBulkError::InvalidMetadata),
    }
}

fn validate_headers(headers: &TsvRecord, expected: &[CsvwColumn]) -> Result<(), SecBulkError> {
    if headers.len() != expected.len()
        || headers
            .iter()
            .zip(expected)
            .any(|(actual, expected)| actual != expected.name)
    {
        Err(SecBulkError::HeaderMismatch)
    } else {
        Ok(())
    }
}

/// One already-bounded and UTF-8-validated TSV record.
struct TsvRecord(Vec<String>);

impl TsvRecord {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }

    fn get(&self, index: usize) -> Option<&str> {
        self.0.get(index).map(String::as_str)
    }
}

/// Streaming RFC 4180-style tabular reader with ceilings applied before any field or record can
/// allocate beyond its admitted size. The `csv` crate intentionally is not used here because it
/// materializes a complete record before callers can enforce a record ceiling.
struct BoundedTsvReader<R> {
    inner: BufReader<R>,
    limits: SecBulkParseLimits,
}

impl<R: Read> BoundedTsvReader<R> {
    fn new(inner: R, limits: SecBulkParseLimits) -> Self {
        Self {
            inner: BufReader::with_capacity(READ_BUFFER_BYTES, inner),
            limits,
        }
    }

    fn next_record(
        &mut self,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Option<TsvRecord>, SecBulkError> {
        let mut builder = TsvRecordBuilder::new(self.limits)?;
        loop {
            check_cancelled(cancellation, deadline)?;
            let (consumed, complete, eof) = {
                let available = self
                    .inner
                    .fill_buf()
                    .map_err(|_| SecBulkError::InvalidTsv)?;
                if available.is_empty() {
                    (0, false, true)
                } else {
                    let mut consumed = 0_usize;
                    let mut complete = false;
                    for byte in available {
                        consumed = consumed
                            .checked_add(1)
                            .ok_or(SecBulkError::TsvLimitExceeded)?;
                        if builder.accept(*byte)? {
                            complete = true;
                            break;
                        }
                    }
                    (consumed, complete, false)
                }
            };
            self.inner.consume(consumed);
            if complete {
                return builder.finish_record().map(Some);
            }
            if eof {
                return builder.finish_eof();
            }
        }
    }

    fn into_inner(self) -> R {
        self.inner.into_inner()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TsvFieldState {
    Start,
    Unquoted,
    Quoted,
    QuoteClosed,
    CarriageReturn,
}

struct TsvRecordBuilder {
    fields: Vec<String>,
    field: Vec<u8>,
    state: TsvFieldState,
    encoded_bytes: usize,
    saw_input: bool,
    limits: SecBulkParseLimits,
}

impl TsvRecordBuilder {
    fn new(limits: SecBulkParseLimits) -> Result<Self, SecBulkError> {
        let mut fields = Vec::new();
        fields
            .try_reserve(16_usize.min(limits.max_columns))
            .map_err(|_| SecBulkError::AllocationFailed)?;
        let mut field = Vec::new();
        field
            .try_reserve(256_usize.min(limits.max_field_bytes))
            .map_err(|_| SecBulkError::AllocationFailed)?;
        Ok(Self {
            fields,
            field,
            state: TsvFieldState::Start,
            encoded_bytes: 0,
            saw_input: false,
            limits,
        })
    }

    /// Accepts one encoded byte and returns true only after a complete LF or CRLF terminator.
    fn accept(&mut self, byte: u8) -> Result<bool, SecBulkError> {
        self.saw_input = true;
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(1)
            .ok_or(SecBulkError::TsvLimitExceeded)?;
        if self.encoded_bytes > self.limits.max_row_bytes || byte == 0 {
            return Err(SecBulkError::TsvLimitExceeded);
        }

        match self.state {
            TsvFieldState::Start => match byte {
                b'\t' => self.finish_field().map(|()| false),
                b'\n' => Ok(true),
                b'\r' => {
                    self.state = TsvFieldState::CarriageReturn;
                    Ok(false)
                }
                b'"' => {
                    self.state = TsvFieldState::Quoted;
                    Ok(false)
                }
                byte if byte.is_ascii_control() => Err(SecBulkError::InvalidTsv),
                byte => {
                    self.push_field_byte(byte)?;
                    self.state = TsvFieldState::Unquoted;
                    Ok(false)
                }
            },
            TsvFieldState::Unquoted => match byte {
                b'\t' => {
                    self.finish_field()?;
                    self.state = TsvFieldState::Start;
                    Ok(false)
                }
                b'\n' => Ok(true),
                b'\r' => {
                    self.state = TsvFieldState::CarriageReturn;
                    Ok(false)
                }
                b'"' => Err(SecBulkError::InvalidTsv),
                byte if byte.is_ascii_control() => Err(SecBulkError::InvalidTsv),
                byte => {
                    self.push_field_byte(byte)?;
                    Ok(false)
                }
            },
            TsvFieldState::Quoted => match byte {
                b'"' => {
                    self.state = TsvFieldState::QuoteClosed;
                    Ok(false)
                }
                byte if byte == 0 => Err(SecBulkError::InvalidTsv),
                byte => {
                    self.push_field_byte(byte)?;
                    Ok(false)
                }
            },
            TsvFieldState::QuoteClosed => match byte {
                b'"' => {
                    self.push_field_byte(b'"')?;
                    self.state = TsvFieldState::Quoted;
                    Ok(false)
                }
                b'\t' => {
                    self.finish_field()?;
                    self.state = TsvFieldState::Start;
                    Ok(false)
                }
                b'\n' => Ok(true),
                b'\r' => {
                    self.state = TsvFieldState::CarriageReturn;
                    Ok(false)
                }
                _ => Err(SecBulkError::InvalidTsv),
            },
            TsvFieldState::CarriageReturn => {
                if byte == b'\n' {
                    Ok(true)
                } else {
                    Err(SecBulkError::InvalidTsv)
                }
            }
        }
    }

    fn push_field_byte(&mut self, byte: u8) -> Result<(), SecBulkError> {
        if self.field.len() >= self.limits.max_field_bytes {
            return Err(SecBulkError::TsvLimitExceeded);
        }
        self.field
            .try_reserve(1)
            .map_err(|_| SecBulkError::AllocationFailed)?;
        self.field.push(byte);
        Ok(())
    }

    fn finish_field(&mut self) -> Result<(), SecBulkError> {
        if self.fields.len() >= self.limits.max_columns {
            return Err(SecBulkError::TsvLimitExceeded);
        }
        let bytes = std::mem::take(&mut self.field);
        let value = String::from_utf8(bytes).map_err(|_| SecBulkError::InvalidTsv)?;
        self.fields
            .try_reserve(1)
            .map_err(|_| SecBulkError::AllocationFailed)?;
        self.fields.push(value);
        Ok(())
    }

    fn finish_record(mut self) -> Result<TsvRecord, SecBulkError> {
        if self.state == TsvFieldState::Quoted {
            return Err(SecBulkError::InvalidTsv);
        }
        self.finish_field()?;
        Ok(TsvRecord(self.fields))
    }

    fn finish_eof(self) -> Result<Option<TsvRecord>, SecBulkError> {
        if !self.saw_input {
            return Ok(None);
        }
        if matches!(
            self.state,
            TsvFieldState::Quoted | TsvFieldState::CarriageReturn
        ) {
            return Err(SecBulkError::InvalidTsv);
        }
        self.finish_record().map(Some)
    }
}

struct HashingReader<R> {
    inner: R,
    digest: Sha256,
    observed: u64,
    maximum: u64,
    exceeded: bool,
}

impl<R> HashingReader<R> {
    fn new(inner: R, maximum: u64) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            observed: 0,
            maximum,
            exceeded: false,
        }
    }

    fn finish(self) -> Result<(EvidenceDigest, u64), SecBulkError> {
        if self.exceeded || self.observed == 0 {
            return Err(SecBulkError::EntryByteLimitExceeded);
        }
        Ok((
            EvidenceDigest::new(DigestAlgorithm::Sha256, self.digest.finalize().into()),
            self.observed,
        ))
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        let increment = u64::try_from(read).unwrap_or(u64::MAX);
        self.observed = self.observed.saturating_add(increment);
        if self.observed > self.maximum {
            self.exceeded = true;
            return Err(std::io::Error::other("SEC bulk member exceeded its bound"));
        }
        self.digest.update(&buffer[..read]);
        Ok(read)
    }
}

struct ScratchBudget {
    used_bytes: Cell<u64>,
    maximum_bytes: u64,
}

impl ScratchBudget {
    const fn new(maximum_bytes: u64) -> Self {
        Self {
            used_bytes: Cell::new(0),
            maximum_bytes,
        }
    }

    fn reserve(&self, bytes: u64) -> Result<(), SecBulkError> {
        let next = self
            .used_bytes
            .get()
            .checked_add(bytes)
            .ok_or(SecBulkError::ScratchLimitExceeded)?;
        if next > self.maximum_bytes {
            return Err(SecBulkError::ScratchLimitExceeded);
        }
        self.used_bytes.set(next);
        Ok(())
    }

    fn release(&self, bytes: u64) {
        self.used_bytes
            .set(self.used_bytes.get().saturating_sub(bytes));
    }
}

struct TrackedScratch<'a> {
    scratch: RawEvidenceScratch<'a>,
    budget: Rc<ScratchBudget>,
    bytes: u64,
}

impl<'a> TrackedScratch<'a> {
    fn new(store: &'a RawEvidenceStore, budget: Rc<ScratchBudget>) -> Result<Self, SecBulkError> {
        Ok(Self {
            scratch: store.create_scratch()?,
            budget,
            bytes: 0,
        })
    }

    fn write_key(&mut self, key: &[u8; KEY_DIGEST_BYTES]) -> Result<(), SecBulkError> {
        self.budget.reserve(KEY_DIGEST_BYTES_U64)?;
        if let Err(error) = self.scratch.file_mut()?.write_all(key) {
            self.budget.release(KEY_DIGEST_BYTES_U64);
            return Err(error.into());
        }
        self.bytes = self
            .bytes
            .checked_add(KEY_DIGEST_BYTES_U64)
            .ok_or(SecBulkError::ScratchLimitExceeded)?;
        Ok(())
    }

    fn file_mut(&mut self) -> Result<&mut std::fs::File, SecBulkError> {
        self.scratch.file_mut().map_err(Into::into)
    }

    fn rewind(&mut self) -> Result<(), SecBulkError> {
        self.scratch.rewind().map_err(Into::into)
    }

    fn sync_and_rewind(&mut self) -> Result<(), SecBulkError> {
        self.scratch.sync_and_rewind().map_err(Into::into)
    }

    fn sync_and_close(&mut self) -> Result<(), SecBulkError> {
        self.scratch.sync_and_close().map_err(Into::into)
    }
}

impl Drop for TrackedScratch<'_> {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

struct SortedRun<'a> {
    scratch: TrackedScratch<'a>,
    keys: u64,
}

struct KeySpool<'a> {
    scratch: TrackedScratch<'a>,
    keys: u64,
}

impl<'a> KeySpool<'a> {
    fn new(store: &'a RawEvidenceStore, budget: Rc<ScratchBudget>) -> Result<Self, SecBulkError> {
        Ok(Self {
            scratch: TrackedScratch::new(store, budget)?,
            keys: 0,
        })
    }

    fn push(&mut self, domain: &[u8], values: &[&str]) -> Result<(), SecBulkError> {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/sec-bulk-relational-key/v1");
        hash_field(&mut digest, domain);
        for value in values {
            hash_field(&mut digest, value.as_bytes());
        }
        self.push_digest(digest.finalize().into())
    }

    fn push_record(
        &mut self,
        domain: &[u8],
        record: &TsvRecord,
        indexes: &[usize],
    ) -> Result<(), SecBulkError> {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/sec-bulk-relational-key/v1");
        hash_field(&mut digest, domain);
        for index in indexes {
            let value = record.get(*index).ok_or(SecBulkError::InvalidTsv)?;
            hash_field(&mut digest, value.as_bytes());
        }
        self.push_digest(digest.finalize().into())
    }

    fn push_digest(&mut self, digest: [u8; KEY_DIGEST_BYTES]) -> Result<(), SecBulkError> {
        self.keys = self
            .keys
            .checked_add(1)
            .ok_or(SecBulkError::TsvLimitExceeded)?;
        self.scratch.write_key(&digest)
    }

    fn seal(
        mut self,
        store: &'a RawEvidenceStore,
        reject_duplicates: bool,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<SortedKeySet<'a>, SecBulkError> {
        self.scratch.sync_and_rewind()?;
        let mut runs = Vec::new();
        let maximum_runs = usize::try_from(self.keys.div_ceil(KEY_SORT_CHUNK_KEYS_U64))
            .map_err(|_| SecBulkError::AllocationFailed)?;
        runs.try_reserve_exact(maximum_runs)
            .map_err(|_| SecBulkError::AllocationFailed)?;
        let mut remaining = self.keys;
        while remaining != 0 {
            check_cancelled(cancellation, deadline)?;
            let take = remaining.min(KEY_SORT_CHUNK_KEYS_U64);
            let take = usize::try_from(take).map_err(|_| SecBulkError::AllocationFailed)?;
            let mut keys = Vec::new();
            keys.try_reserve_exact(take)
                .map_err(|_| SecBulkError::AllocationFailed)?;
            for index in 0..take {
                if index % READ_BUFFER_BYTES == 0 {
                    check_cancelled(cancellation, deadline)?;
                }
                keys.push(
                    read_key(self.scratch.file_mut()?)?.ok_or(SecBulkError::RelationalIntegrity)?,
                );
            }
            keys.sort_unstable();
            if reject_duplicates && keys.windows(2).any(|window| window[0] == window[1]) {
                return Err(SecBulkError::RelationalIntegrity);
            }
            if !reject_duplicates {
                keys.dedup();
            }
            let mut run = TrackedScratch::new(store, Rc::clone(&self.scratch.budget))?;
            for key in keys {
                run.write_key(&key)?;
            }
            let run_keys = run.bytes / KEY_DIGEST_BYTES_U64;
            run.sync_and_close()?;
            runs.push(SortedRun {
                scratch: run,
                keys: run_keys,
            });
            remaining = remaining
                .checked_sub(u64::try_from(take).map_err(|_| SecBulkError::AllocationFailed)?)
                .ok_or(SecBulkError::RelationalIntegrity)?;
        }
        if read_key(self.scratch.file_mut()?)?.is_some() {
            return Err(SecBulkError::RelationalIntegrity);
        }
        let budget = Rc::clone(&self.scratch.budget);
        drop(self.scratch);

        if runs.is_empty() {
            let mut output = TrackedScratch::new(store, Rc::clone(&budget))?;
            output.sync_and_close()?;
            return Ok(SortedKeySet {
                scratch: output,
                keys: 0,
            });
        }
        while runs.len() > 1 {
            check_cancelled(cancellation, deadline)?;
            let mut source = runs.into_iter();
            let mut next = Vec::new();
            loop {
                let group = source.by_ref().take(KEY_MERGE_FAN_IN).collect::<Vec<_>>();
                if group.is_empty() {
                    break;
                }
                next.push(merge_runs(
                    store,
                    Rc::clone(&budget),
                    group,
                    reject_duplicates,
                    deadline,
                    cancellation,
                )?);
            }
            runs = next;
        }
        let output = runs.pop().ok_or(SecBulkError::RelationalIntegrity)?;
        Ok(SortedKeySet {
            scratch: output.scratch,
            keys: output.keys,
        })
    }
}

fn merge_runs<'a>(
    store: &'a RawEvidenceStore,
    budget: Rc<ScratchBudget>,
    mut runs: Vec<SortedRun<'a>>,
    reject_duplicates: bool,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<SortedRun<'a>, SecBulkError> {
    if runs.is_empty() || runs.len() > KEY_MERGE_FAN_IN {
        return Err(SecBulkError::RelationalIntegrity);
    }
    let mut output = TrackedScratch::new(store, budget)?;
    let mut heap = BinaryHeap::new();
    for (index, run) in runs.iter_mut().enumerate() {
        run.scratch.rewind()?;
        if let Some(key) = read_key(run.scratch.file_mut()?)? {
            heap.push(Reverse((key, index)));
        }
    }
    let mut previous = None;
    let mut unique_keys = 0_u64;
    let checkpoint = u64::try_from(READ_BUFFER_BYTES).unwrap_or(u64::MAX);
    while let Some(Reverse((key, run_index))) = heap.pop() {
        if unique_keys % checkpoint == 0 {
            check_cancelled(cancellation, deadline)?;
        }
        if previous == Some(key) {
            if reject_duplicates {
                return Err(SecBulkError::RelationalIntegrity);
            }
        } else {
            output.write_key(&key)?;
            unique_keys = unique_keys
                .checked_add(1)
                .ok_or(SecBulkError::TsvLimitExceeded)?;
            previous = Some(key);
        }
        if let Some(next) = read_key(runs[run_index].scratch.file_mut()?)? {
            heap.push(Reverse((next, run_index)));
        }
    }
    output.sync_and_close()?;
    Ok(SortedRun {
        scratch: output,
        keys: unique_keys,
    })
}

struct SortedKeySet<'a> {
    scratch: TrackedScratch<'a>,
    keys: u64,
}

impl SortedKeySet<'_> {
    fn require_subset_of(
        &mut self,
        superset: &mut Self,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<(), SecBulkError> {
        self.scratch.rewind()?;
        superset.scratch.rewind()?;
        let mut candidate = read_key(superset.scratch.file_mut()?)?;
        let mut observed = 0_u64;
        while let Some(required) = read_key(self.scratch.file_mut()?)? {
            if observed % u64::try_from(READ_BUFFER_BYTES).unwrap_or(u64::MAX) == 0 {
                check_cancelled(cancellation, deadline)?;
            }
            while candidate.is_some_and(|current| current < required) {
                candidate = read_key(superset.scratch.file_mut()?)?;
            }
            if candidate != Some(required) {
                return Err(SecBulkError::RelationalIntegrity);
            }
            observed = observed
                .checked_add(1)
                .ok_or(SecBulkError::TsvLimitExceeded)?;
        }
        if observed != self.keys {
            return Err(SecBulkError::RelationalIntegrity);
        }
        Ok(())
    }
}

fn read_key(reader: &mut std::fs::File) -> Result<Option<[u8; KEY_DIGEST_BYTES]>, SecBulkError> {
    let mut key = [0_u8; KEY_DIGEST_BYTES];
    match reader.read(&mut key[..1]) {
        Ok(0) => Ok(None),
        Ok(1) => {
            reader.read_exact(&mut key[1..])?;
            Ok(Some(key))
        }
        Ok(_) => Err(SecBulkError::RelationalIntegrity),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
            Err(SecBulkError::RelationalIntegrity)
        }
        Err(error) => Err(error.into()),
    }
}

struct RelationalKeyDomain<'a> {
    producers: KeySpool<'a>,
    consumers: KeySpool<'a>,
}

impl<'a> RelationalKeyDomain<'a> {
    fn new(store: &'a RawEvidenceStore, budget: Rc<ScratchBudget>) -> Result<Self, SecBulkError> {
        Ok(Self {
            producers: KeySpool::new(store, Rc::clone(&budget))?,
            consumers: KeySpool::new(store, budget)?,
        })
    }

    fn finish(
        self,
        store: &'a RawEvidenceStore,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<(), SecBulkError> {
        let mut producers = self.producers.seal(store, true, deadline, cancellation)?;
        let mut consumers = self.consumers.seal(store, false, deadline, cancellation)?;
        consumers.require_subset_of(&mut producers, deadline, cancellation)
    }
}

struct ArchiveIntegrityValidator<'a> {
    store: &'a RawEvidenceStore,
    family: SecBulkFamily,
    scratch_budget: Rc<ScratchBudget>,
    accessions: RelationalKeyDomain<'a>,
    secondary: RelationalKeyDomain<'a>,
    ncen_directors: RelationalKeyDomain<'a>,
    ncen_compliance_officers: RelationalKeyDomain<'a>,
    ncen_valuation_changes: RelationalKeyDomain<'a>,
    ncen_security_lending: RelationalKeyDomain<'a>,
    ncen_lines_of_credit: RelationalKeyDomain<'a>,
}

impl<'a> ArchiveIntegrityValidator<'a> {
    fn new(
        store: &'a RawEvidenceStore,
        family: SecBulkFamily,
        limits: SecBulkParseLimits,
    ) -> Result<Self, SecBulkError> {
        // Pre-admit three complete decoded-generation equivalents before opening any spool: one
        // for accumulated join keys, one for per-table PK/run validation, and one for a bounded
        // merge output. Runtime accounting remains authoritative for key-dense rows and includes
        // every multipass output byte under this same aggregate ceiling.
        let configured_peak = limits
            .max_expanded_bytes
            .checked_mul(3)
            .ok_or(SecBulkError::ScratchLimitExceeded)?;
        if limits.max_validation_scratch_bytes < configured_peak {
            return Err(SecBulkError::ScratchLimitExceeded);
        }
        let budget = Rc::new(ScratchBudget::new(limits.max_validation_scratch_bytes));
        Ok(Self {
            store,
            family,
            scratch_budget: Rc::clone(&budget),
            accessions: RelationalKeyDomain::new(store, Rc::clone(&budget))?,
            secondary: RelationalKeyDomain::new(store, Rc::clone(&budget))?,
            ncen_directors: RelationalKeyDomain::new(store, Rc::clone(&budget))?,
            ncen_compliance_officers: RelationalKeyDomain::new(store, Rc::clone(&budget))?,
            ncen_valuation_changes: RelationalKeyDomain::new(store, Rc::clone(&budget))?,
            ncen_security_lending: RelationalKeyDomain::new(store, Rc::clone(&budget))?,
            ncen_lines_of_credit: RelationalKeyDomain::new(store, budget)?,
        })
    }

    const fn store(&self) -> &'a RawEvidenceStore {
        self.store
    }

    fn scratch_budget(&self) -> Rc<ScratchBudget> {
        Rc::clone(&self.scratch_budget)
    }

    fn observe(
        &mut self,
        table: &str,
        columns: &[CsvwColumn],
        record: &TsvRecord,
    ) -> Result<(), SecBulkError> {
        if table == "SUBMISSION.tsv" {
            let accession = column_value(columns, record, "ACCESSION_NUMBER")?;
            self.accessions.producers.push(b"accession", &[accession])?;
        } else if let Some(accession) = optional_column_value(columns, record, "ACCESSION_NUMBER")?
        {
            self.accessions.consumers.push(b"accession", &[accession])?;
        }

        match self.family {
            SecBulkFamily::Nport if table == "FUND_REPORTED_HOLDING.tsv" => {
                let holding = column_value(columns, record, "HOLDING_ID")?;
                self.secondary.producers.push(b"holding-id", &[holding])?;
            }
            SecBulkFamily::Nport => {
                if let Some(holding) = optional_column_value(columns, record, "HOLDING_ID")? {
                    self.secondary.consumers.push(b"holding-id", &[holding])?;
                }
            }
            SecBulkFamily::Ncen if table == "FUND_REPORTED_INFO.tsv" => {
                let fund = column_value(columns, record, "FUND_ID")?;
                self.secondary.producers.push(b"fund-id", &[fund])?;
            }
            SecBulkFamily::Ncen => {
                if let Some(fund) = optional_column_value(columns, record, "FUND_ID")? {
                    self.secondary.consumers.push(b"fund-id", &[fund])?;
                }
            }
        }
        if self.family == SecBulkFamily::Ncen {
            match table {
                "DIRECTOR.tsv" => self.ncen_directors.producers.push(
                    b"ncen-director",
                    &[
                        column_value(columns, record, "ACCESSION_NUMBER")?,
                        column_value(columns, record, "DIRECTOR_SEQNUM")?,
                    ],
                )?,
                "DIRECTOR_FILE_NUMBER.tsv" => self.ncen_directors.consumers.push(
                    b"ncen-director",
                    &[
                        column_value(columns, record, "ACCESSION_NUMBER")?,
                        column_value(columns, record, "DIRECTOR_SEQNUM")?,
                    ],
                )?,
                "CHIEF_COMPLIANCE_OFFICER.tsv" => self.ncen_compliance_officers.producers.push(
                    b"ncen-compliance-officer",
                    &[
                        column_value(columns, record, "ACCESSION_NUMBER")?,
                        column_value(columns, record, "CCO_SEQNUM")?,
                    ],
                )?,
                "CCO_EMPLOYER.tsv" => self.ncen_compliance_officers.consumers.push(
                    b"ncen-compliance-officer",
                    &[
                        column_value(columns, record, "ACCESSION_NUMBER")?,
                        column_value(columns, record, "CCO_SEQNUM")?,
                    ],
                )?,
                "VALUATION_METHOD_CHANGE.tsv" => self.ncen_valuation_changes.producers.push(
                    b"ncen-valuation-change",
                    &[
                        column_value(columns, record, "ACCESSION_NUMBER")?,
                        column_value(columns, record, "VALUATION_METHOD_CHANGE_SEQNUM")?,
                    ],
                )?,
                "VALUATION_METHOD_CHANGE_SERIES.tsv" => {
                    self.ncen_valuation_changes.consumers.push(
                        b"ncen-valuation-change",
                        &[
                            column_value(columns, record, "ACCESSION_NUMBER")?,
                            column_value(columns, record, "VALUATION_METHOD_CHANGE_SEQNUM")?,
                        ],
                    )?
                }
                "SECURITY_LENDING.tsv" => self.ncen_security_lending.producers.push(
                    b"ncen-security-lending",
                    &[
                        column_value(columns, record, "FUND_ID")?,
                        column_value(columns, record, "SECURITY_LENDING_SEQNUM")?,
                    ],
                )?,
                "SEC_LENDING_IDEMNITY_PROVIDER.tsv" => self.ncen_security_lending.consumers.push(
                    b"ncen-security-lending",
                    &[
                        column_value(columns, record, "FUND_ID")?,
                        column_value(columns, record, "SECURITY_LENDING_SEQNUM")?,
                    ],
                )?,
                "LINE_OF_CREDIT_DETAIL.tsv" => self.ncen_lines_of_credit.producers.push(
                    b"ncen-line-of-credit",
                    &[
                        column_value(columns, record, "FUND_ID")?,
                        column_value(columns, record, "LINE_OF_CREDIT_SEQNUM")?,
                    ],
                )?,
                "LINE_OF_CREDIT_INSTITUTION.tsv" | "CREDIT_USER.tsv" => {
                    self.ncen_lines_of_credit.consumers.push(
                        b"ncen-line-of-credit",
                        &[
                            column_value(columns, record, "FUND_ID")?,
                            column_value(columns, record, "LINE_OF_CREDIT_SEQNUM")?,
                        ],
                    )?
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn finish(
        self,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<(), SecBulkError> {
        self.accessions.finish(self.store, deadline, cancellation)?;
        self.secondary.finish(self.store, deadline, cancellation)?;
        self.ncen_directors
            .finish(self.store, deadline, cancellation)?;
        self.ncen_compliance_officers
            .finish(self.store, deadline, cancellation)?;
        self.ncen_valuation_changes
            .finish(self.store, deadline, cancellation)?;
        self.ncen_security_lending
            .finish(self.store, deadline, cancellation)?;
        self.ncen_lines_of_credit
            .finish(self.store, deadline, cancellation)
    }
}

fn column_value<'a>(
    columns: &[CsvwColumn],
    record: &'a TsvRecord,
    name: &str,
) -> Result<&'a str, SecBulkError> {
    let index = columns
        .iter()
        .position(|column| column.name == name)
        .ok_or(SecBulkError::InvalidMetadata)?;
    let value = record.get(index).ok_or(SecBulkError::InvalidTsv)?;
    if value.is_empty() {
        Err(SecBulkError::RelationalIntegrity)
    } else {
        Ok(value)
    }
}

fn optional_column_value<'a>(
    columns: &[CsvwColumn],
    record: &'a TsvRecord,
    name: &str,
) -> Result<Option<&'a str>, SecBulkError> {
    let Some(index) = columns.iter().position(|column| column.name == name) else {
        return Ok(None);
    };
    let value = record.get(index).ok_or(SecBulkError::InvalidTsv)?;
    Ok((!value.is_empty()).then_some(value))
}

fn has_canonical_projection(family: SecBulkFamily, table: &str) -> bool {
    match family {
        SecBulkFamily::Nport => matches!(
            table,
            "SUBMISSION.tsv"
                | "REGISTRANT.tsv"
                | "FUND_REPORTED_INFO.tsv"
                | "FUND_REPORTED_HOLDING.tsv"
                | "IDENTIFIERS.tsv"
        ),
        SecBulkFamily::Ncen => matches!(
            table,
            "SUBMISSION.tsv"
                | "REGISTRANT.tsv"
                | "FUND_REPORTED_INFO.tsv"
                | "ETF.tsv"
                | "SECURITY_EXCHANGE.tsv"
        ),
    }
}

#[derive(Clone, Copy)]
enum TypedColumnKind {
    String,
    BooleanString,
    Date,
    Number,
    IntegerNumber,
}

fn validate_typed_contract(
    family: SecBulkFamily,
    receipt: &SecBulkTableReceipt,
) -> Result<(), SecBulkError> {
    let expected: &[(&str, TypedColumnKind)] = match (family, receipt.name().as_str()) {
        (SecBulkFamily::Nport, "SUBMISSION.tsv") => &[
            ("ACCESSION_NUMBER", TypedColumnKind::String),
            ("FILING_DATE", TypedColumnKind::Date),
            ("SUB_TYPE", TypedColumnKind::String),
            ("REPORT_ENDING_PERIOD", TypedColumnKind::Date),
            ("REPORT_DATE", TypedColumnKind::Date),
            ("IS_LAST_FILING", TypedColumnKind::BooleanString),
        ],
        (SecBulkFamily::Nport, "REGISTRANT.tsv") => &[
            ("ACCESSION_NUMBER", TypedColumnKind::String),
            ("CIK", TypedColumnKind::String),
            ("REGISTRANT_NAME", TypedColumnKind::String),
            ("LEI", TypedColumnKind::String),
        ],
        (SecBulkFamily::Nport, "FUND_REPORTED_INFO.tsv") => &[
            ("ACCESSION_NUMBER", TypedColumnKind::String),
            ("SERIES_NAME", TypedColumnKind::String),
            ("SERIES_ID", TypedColumnKind::String),
            ("SERIES_LEI", TypedColumnKind::String),
            ("TOTAL_ASSETS", TypedColumnKind::Number),
            ("TOTAL_LIABILITIES", TypedColumnKind::Number),
            ("NET_ASSETS", TypedColumnKind::Number),
        ],
        (SecBulkFamily::Nport, "FUND_REPORTED_HOLDING.tsv") => &[
            ("ACCESSION_NUMBER", TypedColumnKind::String),
            ("HOLDING_ID", TypedColumnKind::IntegerNumber),
            ("ISSUER_NAME", TypedColumnKind::String),
            ("ISSUER_LEI", TypedColumnKind::String),
            ("ISSUER_TITLE", TypedColumnKind::String),
            ("ISSUER_CUSIP", TypedColumnKind::String),
            ("BALANCE", TypedColumnKind::Number),
            ("UNIT", TypedColumnKind::String),
            ("OTHER_UNIT_DESC", TypedColumnKind::String),
            ("CURRENCY_CODE", TypedColumnKind::String),
            ("CURRENCY_VALUE", TypedColumnKind::Number),
            ("EXCHANGE_RATE", TypedColumnKind::Number),
            ("PERCENTAGE", TypedColumnKind::Number),
            ("PAYOFF_PROFILE", TypedColumnKind::String),
            ("ASSET_CAT", TypedColumnKind::String),
            ("OTHER_ASSET", TypedColumnKind::String),
            ("ISSUER_TYPE", TypedColumnKind::String),
            ("OTHER_ISSUER", TypedColumnKind::String),
            ("INVESTMENT_COUNTRY", TypedColumnKind::String),
            ("IS_RESTRICTED_SECURITY", TypedColumnKind::BooleanString),
            ("FAIR_VALUE_LEVEL", TypedColumnKind::String),
            ("DERIVATIVE_CAT", TypedColumnKind::String),
        ],
        (SecBulkFamily::Nport, "IDENTIFIERS.tsv") => &[
            ("HOLDING_ID", TypedColumnKind::IntegerNumber),
            ("IDENTIFIERS_ID", TypedColumnKind::IntegerNumber),
            ("IDENTIFIER_ISIN", TypedColumnKind::String),
            ("IDENTIFIER_TICKER", TypedColumnKind::String),
            ("OTHER_IDENTIFIER", TypedColumnKind::String),
            ("OTHER_IDENTIFIER_DESC", TypedColumnKind::String),
        ],
        (SecBulkFamily::Ncen, "SUBMISSION.tsv") => &[
            ("ACCESSION_NUMBER", TypedColumnKind::String),
            ("SUBMISSION_TYPE", TypedColumnKind::String),
            ("CIK", TypedColumnKind::String),
            ("FILING_DATE", TypedColumnKind::Date),
            ("REPORT_ENDING_PERIOD", TypedColumnKind::Date),
            (
                "IS_REPORT_PERIOD_LT_12MONTH",
                TypedColumnKind::BooleanString,
            ),
        ],
        (SecBulkFamily::Ncen, "REGISTRANT.tsv") => &[
            ("ACCESSION_NUMBER", TypedColumnKind::String),
            ("REGISTRANT_NAME", TypedColumnKind::String),
            ("FILE_NUM", TypedColumnKind::String),
            ("CIK", TypedColumnKind::String),
            ("LEI", TypedColumnKind::String),
            ("INVESTMENT_COMPANY_TYPE", TypedColumnKind::String),
            ("TOTAL_SERIES", TypedColumnKind::IntegerNumber),
        ],
        (SecBulkFamily::Ncen, "FUND_REPORTED_INFO.tsv") => &[
            ("FUND_ID", TypedColumnKind::String),
            ("ACCESSION_NUMBER", TypedColumnKind::String),
            ("FUND_NAME", TypedColumnKind::String),
            ("SERIES_ID", TypedColumnKind::String),
            ("LEI", TypedColumnKind::String),
            ("IS_ETF", TypedColumnKind::BooleanString),
            ("IS_INDEX", TypedColumnKind::BooleanString),
            ("MONTHLY_AVG_NET_ASSETS", TypedColumnKind::Number),
            ("DAILY_AVG_NET_ASSETS", TypedColumnKind::Number),
        ],
        (SecBulkFamily::Ncen, "ETF.tsv") => &[
            ("FUND_ID", TypedColumnKind::String),
            ("FUND_NAME", TypedColumnKind::String),
            ("SERIES_ID", TypedColumnKind::String),
            ("IS_COLLATERAL_REQUIRED", TypedColumnKind::BooleanString),
            ("NUM_SHARES_PER_CREATION_UNIT", TypedColumnKind::Number),
            ("REDEEMED_SHARES_PER_CREATION_UNIT", TypedColumnKind::Number),
            ("IS_FUND_IN_KIND_ETF", TypedColumnKind::BooleanString),
        ],
        (SecBulkFamily::Ncen, "SECURITY_EXCHANGE.tsv") => &[
            ("FUND_ID", TypedColumnKind::String),
            ("FUND_EXCHANGE", TypedColumnKind::String),
            ("FUND_TICKER_SYMBOL", TypedColumnKind::String),
        ],
        _ => return Err(SecBulkError::InvalidLayout),
    };

    for (name, kind) in expected {
        let column = receipt
            .columns()
            .iter()
            .find(|column| column.name().as_str() == *name)
            .ok_or(SecBulkError::HeaderMismatch)?;
        let valid = match kind {
            TypedColumnKind::String => column.datatype_base() == "string",
            TypedColumnKind::BooleanString => {
                column.datatype_base() == "string" && column.max_length() == Some(1)
            }
            TypedColumnKind::Date => column.datatype_base() == "date (DD-MON-YYYY)",
            TypedColumnKind::Number => column.datatype_base() == "NUMBER",
            TypedColumnKind::IntegerNumber => {
                column.datatype_base() == "NUMBER"
                    && matches!(column.data_scale(), Some(SecBulkNumericAttribute::Value(0)))
            }
        };
        if !valid {
            return Err(SecBulkError::InvalidMetadata);
        }
    }
    Ok(())
}

fn project_typed_table<R: Read, F>(
    family: SecBulkFamily,
    receipt: &SecBulkTableReceipt,
    reader: R,
    limits: SecBulkParseLimits,
    deadline: Timestamp,
    cancellation: &CancellationToken,
    emit: &mut F,
) -> Result<u64, SecBulkError>
where
    F: FnMut(SecBulkNativeRow) -> Result<(), SecBulkError>,
{
    let hashing = HashingReader::new(reader, limits.max_table_bytes);
    let mut tsv = BoundedTsvReader::new(hashing, limits);
    let headers = tsv
        .next_record(deadline, cancellation)?
        .ok_or(SecBulkError::HeaderMismatch)?;
    if headers.len() != receipt.columns().len()
        || headers
            .iter()
            .zip(receipt.columns())
            .any(|(actual, expected)| actual != expected.name().as_str())
    {
        return Err(SecBulkError::HeaderMismatch);
    }
    let table = SecBulkTableKind::from_member(family, receipt.name().as_str())?;
    let has_projection = has_canonical_projection(family, receipt.name().as_str());
    if has_projection {
        validate_typed_contract(family, receipt)?;
    }
    let projector = RowProjector::new(&headers)?;
    let mut rows = 0_u64;
    while let Some(record) = tsv.next_record(deadline, cancellation)? {
        check_cancelled(cancellation, deadline)?;
        if record.len() != receipt.columns().len() {
            return Err(SecBulkError::InvalidTsv);
        }
        validate_receipt_record_contract(&record, receipt.columns())?;
        rows = rows.checked_add(1).ok_or(SecBulkError::TsvLimitExceeded)?;
        if rows > receipt.row_count() || rows > limits.max_rows_per_table {
            return Err(SecBulkError::RecoveryMismatch);
        }
        let evidence = row_evidence(receipt.name().as_str(), rows, &record);
        let canonical_projection = if has_projection {
            optional_canonical_projection(project_row(
                family,
                receipt.name().as_str(),
                &projector,
                &record,
                rows,
                evidence,
            ))?
        } else {
            None
        };
        let native = project_metadata_governed_row(
            table,
            receipt,
            &record,
            rows,
            evidence,
            canonical_projection,
        )?;
        emit(native)?;
    }
    let hashing = tsv.into_inner();
    let (evidence, bytes) = hashing.finish()?;
    if evidence != receipt.evidence()
        || bytes != receipt.decoded_bytes()
        || rows != receipt.row_count()
    {
        return Err(SecBulkError::RecoveryMismatch);
    }
    Ok(rows)
}

struct RowProjector {
    columns: BTreeMap<String, usize>,
}

impl RowProjector {
    fn new(headers: &TsvRecord) -> Result<Self, SecBulkError> {
        let mut columns = BTreeMap::new();
        for (index, name) in headers.iter().enumerate() {
            if columns.insert(name.to_owned(), index).is_some() {
                return Err(SecBulkError::HeaderMismatch);
            }
        }
        Ok(Self { columns })
    }

    fn get<'a>(&self, record: &'a TsvRecord, name: &str) -> Result<&'a str, SecBulkError> {
        let index = self
            .columns
            .get(name)
            .copied()
            .ok_or(SecBulkError::HeaderMismatch)?;
        record.get(index).ok_or(SecBulkError::InvalidTsv)
    }
}

fn project_metadata_governed_row(
    table: SecBulkTableKind,
    receipt: &SecBulkTableReceipt,
    record: &TsvRecord,
    row_number: u64,
    row_evidence: EvidenceDigest,
    canonical_projection: Option<SecBulkCanonicalProjection>,
) -> Result<SecBulkNativeRow, SecBulkError> {
    let mut fields = Vec::new();
    fields
        .try_reserve_exact(receipt.columns().len())
        .map_err(|_| SecBulkError::AllocationFailed)?;
    for (index, column) in receipt.columns().iter().enumerate() {
        let lexical = record.get(index).ok_or(SecBulkError::InvalidTsv)?;
        let value = if lexical.is_empty() {
            SecBulkTypedValue::Missing
        } else {
            match column.datatype_base() {
                "string" => SecBulkTypedValue::Text(lexical.to_owned()),
                "date (DD-MON-YYYY)" => SecBulkTypedValue::Date(parse_sec_date(lexical)?),
                "NUMBER" => {
                    SecBulkTypedValue::Number(SecExactNumber::from_validated_lexical(lexical))
                }
                _ => return Err(SecBulkError::InvalidMetadata),
            }
        };
        fields.push(SecBulkTypedField {
            name: column.name().clone(),
            value,
        });
    }

    let mut primary_key = Vec::new();
    primary_key
        .try_reserve_exact(receipt.primary_key().len())
        .map_err(|_| SecBulkError::AllocationFailed)?;
    for key in receipt.primary_key() {
        let index = receipt
            .columns()
            .iter()
            .position(|column| column.name() == key)
            .ok_or(SecBulkError::InvalidMetadata)?;
        let value = record.get(index).ok_or(SecBulkError::InvalidTsv)?;
        primary_key.push(SecBulkKeyField {
            name: key.clone(),
            value: value.to_owned(),
        });
    }

    let mut joins = Vec::new();
    joins
        .try_reserve_exact(11)
        .map_err(|_| SecBulkError::AllocationFailed)?;
    for (column_name, domain) in [
        ("ACCESSION_NUMBER", SecBulkJoinDomain::Accession),
        ("HOLDING_ID", SecBulkJoinDomain::Holding),
        ("FUND_ID", SecBulkJoinDomain::Fund),
        ("SERIES_ID", SecBulkJoinDomain::Series),
        ("CIK", SecBulkJoinDomain::RegistrantCik),
        ("CLASS_ID", SecBulkJoinDomain::ShareClass),
        ("DIRECTOR_SEQNUM", SecBulkJoinDomain::NcenDirectorSequence),
        (
            "CCO_SEQNUM",
            SecBulkJoinDomain::NcenComplianceOfficerSequence,
        ),
        (
            "VALUATION_METHOD_CHANGE_SEQNUM",
            SecBulkJoinDomain::NcenValuationChangeSequence,
        ),
        (
            "SECURITY_LENDING_SEQNUM",
            SecBulkJoinDomain::NcenSecurityLendingSequence,
        ),
        (
            "LINE_OF_CREDIT_SEQNUM",
            SecBulkJoinDomain::NcenLineOfCreditSequence,
        ),
    ] {
        let Some(index) = receipt
            .columns()
            .iter()
            .position(|column| column.name().as_str() == column_name)
        else {
            continue;
        };
        let value = record.get(index).ok_or(SecBulkError::InvalidTsv)?;
        if !value.is_empty() {
            joins.push(SecBulkJoinCoordinate {
                domain,
                column: SourceIdentifier::try_from(column_name)?,
                value: value.to_owned(),
            });
        }
    }
    Ok(SecBulkNativeRow {
        table,
        primary_key,
        joins,
        fields,
        canonical_projection,
        membership: None,
        row_number,
        row_evidence,
    })
}

fn project_row(
    family: SecBulkFamily,
    table: &str,
    columns: &RowProjector,
    row: &TsvRecord,
    row_number: u64,
    row_evidence: EvidenceDigest,
) -> Result<SecBulkCanonicalProjection, SecBulkError> {
    match (family, table) {
        (SecBulkFamily::Nport, "SUBMISSION.tsv") => Ok(
            SecBulkCanonicalProjection::NportSubmission(Box::new(SecNportSubmissionRow {
                accession: accession(columns.get(row, "ACCESSION_NUMBER")?)?,
                filing_date: optional_date(columns.get(row, "FILING_DATE")?)?,
                form: required_identifier(columns.get(row, "SUB_TYPE")?)?,
                report_ending_period: optional_date(columns.get(row, "REPORT_ENDING_PERIOD")?)?,
                report_date: optional_date(columns.get(row, "REPORT_DATE")?)?,
                is_last_filing: optional_bool(columns.get(row, "IS_LAST_FILING")?)?,
                row_number,
                row_evidence,
            })),
        ),
        (SecBulkFamily::Nport, "REGISTRANT.tsv") => Ok(
            SecBulkCanonicalProjection::NportRegistrant(Box::new(SecNportRegistrantRow {
                accession: accession(columns.get(row, "ACCESSION_NUMBER")?)?,
                cik: cik(columns.get(row, "CIK")?)?,
                registrant_name: optional_string(columns.get(row, "REGISTRANT_NAME")?),
                lei: optional_identifier(columns.get(row, "LEI")?)?,
                row_number,
                row_evidence,
            })),
        ),
        (SecBulkFamily::Nport, "FUND_REPORTED_INFO.tsv") => Ok(
            SecBulkCanonicalProjection::NportFund(Box::new(SecNportFundRow {
                accession: accession(columns.get(row, "ACCESSION_NUMBER")?)?,
                series_name: optional_string(columns.get(row, "SERIES_NAME")?),
                series_id: series_id(columns.get(row, "SERIES_ID")?)?,
                series_lei: optional_identifier(columns.get(row, "SERIES_LEI")?)?,
                total_assets: optional_number(columns.get(row, "TOTAL_ASSETS")?),
                total_liabilities: optional_number(columns.get(row, "TOTAL_LIABILITIES")?),
                net_assets: optional_number(columns.get(row, "NET_ASSETS")?),
                row_number,
                row_evidence,
            })),
        ),
        (SecBulkFamily::Nport, "FUND_REPORTED_HOLDING.tsv") => Ok(
            SecBulkCanonicalProjection::NportHolding(Box::new(SecNportHoldingRow {
                accession: accession(columns.get(row, "ACCESSION_NUMBER")?)?,
                holding_id: numeric_identifier(columns.get(row, "HOLDING_ID")?, 38)?,
                issuer_name: optional_string(columns.get(row, "ISSUER_NAME")?),
                issuer_lei: optional_identifier(columns.get(row, "ISSUER_LEI")?)?,
                issuer_title: optional_string(columns.get(row, "ISSUER_TITLE")?),
                cusip: optional_identifier(columns.get(row, "ISSUER_CUSIP")?)?,
                balance: optional_number(columns.get(row, "BALANCE")?),
                unit: optional_identifier(columns.get(row, "UNIT")?)?,
                other_unit_description: optional_string(columns.get(row, "OTHER_UNIT_DESC")?),
                currency: optional_identifier(columns.get(row, "CURRENCY_CODE")?)?,
                value: optional_number(columns.get(row, "CURRENCY_VALUE")?),
                exchange_rate: optional_number(columns.get(row, "EXCHANGE_RATE")?),
                percentage: optional_number(columns.get(row, "PERCENTAGE")?),
                payoff_profile: optional_identifier(columns.get(row, "PAYOFF_PROFILE")?)?,
                asset_category: optional_identifier(columns.get(row, "ASSET_CAT")?)?,
                other_asset: optional_string(columns.get(row, "OTHER_ASSET")?),
                issuer_type: optional_identifier(columns.get(row, "ISSUER_TYPE")?)?,
                other_issuer: optional_string(columns.get(row, "OTHER_ISSUER")?),
                investment_country: optional_identifier(columns.get(row, "INVESTMENT_COUNTRY")?)?,
                restricted_security: optional_bool(columns.get(row, "IS_RESTRICTED_SECURITY")?)?,
                fair_value_level: optional_identifier(columns.get(row, "FAIR_VALUE_LEVEL")?)?,
                derivative_category: optional_identifier(columns.get(row, "DERIVATIVE_CAT")?)?,
                row_number,
                row_evidence,
            })),
        ),
        (SecBulkFamily::Nport, "IDENTIFIERS.tsv") => Ok(
            SecBulkCanonicalProjection::NportIdentifier(Box::new(SecNportIdentifierRow {
                holding_id: numeric_identifier(columns.get(row, "HOLDING_ID")?, 38)?,
                identifiers_id: numeric_identifier(columns.get(row, "IDENTIFIERS_ID")?, 38)?,
                isin: optional_identifier(columns.get(row, "IDENTIFIER_ISIN")?)?,
                ticker: optional_identifier(columns.get(row, "IDENTIFIER_TICKER")?)?,
                other_identifier: optional_string(columns.get(row, "OTHER_IDENTIFIER")?),
                other_identifier_description: optional_string(
                    columns.get(row, "OTHER_IDENTIFIER_DESC")?,
                ),
                row_number,
                row_evidence,
            })),
        ),
        (SecBulkFamily::Ncen, "SUBMISSION.tsv") => Ok(SecBulkCanonicalProjection::NcenSubmission(
            Box::new(SecNcenSubmissionRow {
                accession: accession(columns.get(row, "ACCESSION_NUMBER")?)?,
                form: required_identifier(columns.get(row, "SUBMISSION_TYPE")?)?,
                cik: cik(columns.get(row, "CIK")?)?,
                filing_date: optional_date(columns.get(row, "FILING_DATE")?)?,
                report_ending_period: optional_date(columns.get(row, "REPORT_ENDING_PERIOD")?)?,
                report_period_less_than_twelve_months: optional_bool(
                    columns.get(row, "IS_REPORT_PERIOD_LT_12MONTH")?,
                )?,
                row_number,
                row_evidence,
            }),
        )),
        (SecBulkFamily::Ncen, "REGISTRANT.tsv") => Ok(SecBulkCanonicalProjection::NcenRegistrant(
            Box::new(SecNcenRegistrantRow {
                accession: accession(columns.get(row, "ACCESSION_NUMBER")?)?,
                cik: cik(columns.get(row, "CIK")?)?,
                registrant_name: optional_string(columns.get(row, "REGISTRANT_NAME")?),
                file_number: optional_identifier(columns.get(row, "FILE_NUM")?)?,
                lei: optional_identifier(columns.get(row, "LEI")?)?,
                investment_company_type: optional_identifier(
                    columns.get(row, "INVESTMENT_COMPANY_TYPE")?,
                )?,
                total_series: optional_u64(columns.get(row, "TOTAL_SERIES")?)?,
                row_number,
                row_evidence,
            }),
        )),
        (SecBulkFamily::Ncen, "FUND_REPORTED_INFO.tsv") => Ok(
            SecBulkCanonicalProjection::NcenFund(Box::new(SecNcenFundRow {
                fund_id: compound_fund_id(columns.get(row, "FUND_ID")?)?,
                accession: accession(columns.get(row, "ACCESSION_NUMBER")?)?,
                fund_name: optional_string(columns.get(row, "FUND_NAME")?),
                series_id: optional_identifier(columns.get(row, "SERIES_ID")?)?,
                lei: optional_identifier(columns.get(row, "LEI")?)?,
                is_etf: optional_bool(columns.get(row, "IS_ETF")?)?,
                is_index: optional_bool(columns.get(row, "IS_INDEX")?)?,
                monthly_average_net_assets: optional_number(
                    columns.get(row, "MONTHLY_AVG_NET_ASSETS")?,
                ),
                daily_average_net_assets: optional_number(
                    columns.get(row, "DAILY_AVG_NET_ASSETS")?,
                ),
                row_number,
                row_evidence,
            })),
        ),
        (SecBulkFamily::Ncen, "ETF.tsv") => Ok(SecBulkCanonicalProjection::NcenEtf(Box::new(
            SecNcenEtfRow {
                fund_id: compound_fund_id(columns.get(row, "FUND_ID")?)?,
                fund_name: optional_string(columns.get(row, "FUND_NAME")?),
                series_id: optional_identifier(columns.get(row, "SERIES_ID")?)?,
                collateral_required: optional_bool(columns.get(row, "IS_COLLATERAL_REQUIRED")?)?,
                shares_per_creation_unit: optional_number(
                    columns.get(row, "NUM_SHARES_PER_CREATION_UNIT")?,
                ),
                redeemed_shares_per_creation_unit: optional_number(
                    columns.get(row, "REDEEMED_SHARES_PER_CREATION_UNIT")?,
                ),
                is_in_kind_etf: optional_bool(columns.get(row, "IS_FUND_IN_KIND_ETF")?)?,
                row_number,
                row_evidence,
            },
        ))),
        (SecBulkFamily::Ncen, "SECURITY_EXCHANGE.tsv") => {
            Ok(SecBulkCanonicalProjection::NcenSecurityExchange(Box::new(
                SecNcenSecurityExchangeRow {
                    fund_id: compound_fund_id(columns.get(row, "FUND_ID")?)?,
                    exchange: optional_identifier(columns.get(row, "FUND_EXCHANGE")?)?,
                    ticker: optional_identifier(columns.get(row, "FUND_TICKER_SYMBOL")?)?,
                    row_number,
                    row_evidence,
                },
            )))
        }
        _ => Err(SecBulkError::InvalidLayout),
    }
}

fn optional_canonical_projection(
    projection: Result<SecBulkCanonicalProjection, SecBulkError>,
) -> Result<Option<SecBulkCanonicalProjection>, SecBulkError> {
    match projection {
        Ok(projection) => Ok(Some(projection)),
        // The metadata-governed native row remains valid when a nullable or provider-shaped
        // display field cannot satisfy the narrower canonical identity contract. That row must
        // remain queryable and force canonical abstention instead of invalidating the archive.
        Err(SecBulkError::InvalidTsv | SecBulkError::Identity(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) fn canonical_projection_from_native(
    row: &SecBulkNativeRow,
) -> Result<Option<SecBulkCanonicalProjection>, SecBulkError> {
    if !has_canonical_projection(row.table.family(), row.table.member_name()) {
        return Ok(None);
    }
    let mut columns = BTreeMap::new();
    let mut values = Vec::new();
    values
        .try_reserve_exact(row.fields.len())
        .map_err(|_| SecBulkError::AllocationFailed)?;
    for (index, field) in row.fields.iter().enumerate() {
        if columns
            .insert(field.name.as_str().to_owned(), index)
            .is_some()
        {
            return Err(SecBulkError::RecoveryMismatch);
        }
        values.push(match &field.value {
            SecBulkTypedValue::Missing => String::new(),
            SecBulkTypedValue::Text(value) => value.clone(),
            SecBulkTypedValue::Date(value) => value.format("%d-%b-%Y").to_string().to_uppercase(),
            SecBulkTypedValue::Number(value) => value.as_str().to_owned(),
        });
    }
    optional_canonical_projection(project_row(
        row.table.family(),
        row.table.member_name(),
        &RowProjector { columns },
        &TsvRecord(values),
        row.row_number,
        row.row_evidence,
    ))
}

fn row_evidence(table: &str, row_number: u64, record: &TsvRecord) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/sec-bulk-decoded-row/v1");
    hash_field(&mut digest, table.as_bytes());
    hash_field(&mut digest, &row_number.to_be_bytes());
    for field in record.iter() {
        hash_field(&mut digest, field.as_bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn optional_string(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

fn required_identifier(value: &str) -> Result<SourceIdentifier, SecBulkError> {
    if value.is_empty() {
        return Err(SecBulkError::InvalidTsv);
    }
    SourceIdentifier::try_from(value).map_err(Into::into)
}

fn optional_identifier(value: &str) -> Result<Option<SourceIdentifier>, SecBulkError> {
    if value.is_empty() {
        Ok(None)
    } else {
        required_identifier(value).map(Some)
    }
}

fn numeric_identifier(
    value: &str,
    maximum_digits: usize,
) -> Result<SourceIdentifier, SecBulkError> {
    if value.is_empty()
        || value.len() > maximum_digits
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(SecBulkError::InvalidTsv);
    }
    required_identifier(value)
}

fn accession(value: &str) -> Result<SourceIdentifier, SecBulkError> {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[10] != b'-'
        || bytes[13] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| index != 10 && index != 13 && !byte.is_ascii_digit())
    {
        return Err(SecBulkError::InvalidTsv);
    }
    required_identifier(value)
}

fn cik(value: &str) -> Result<SourceIdentifier, SecBulkError> {
    if value.len() != 10
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || value.bytes().all(|byte| byte == b'0')
    {
        return Err(SecBulkError::InvalidTsv);
    }
    required_identifier(value)
}

fn series_id(value: &str) -> Result<SourceIdentifier, SecBulkError> {
    if value.len() != 10
        || !value.starts_with('S')
        || !value[1..].bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(SecBulkError::InvalidTsv);
    }
    required_identifier(value)
}

fn compound_fund_id(value: &str) -> Result<SourceIdentifier, SecBulkError> {
    let mut parts = value.split('_');
    let accession_value = parts.next().ok_or(SecBulkError::InvalidTsv)?;
    let cik_value = parts.next().ok_or(SecBulkError::InvalidTsv)?;
    let series_value = parts.next().ok_or(SecBulkError::InvalidTsv)?;
    if parts.next().is_some() {
        return Err(SecBulkError::InvalidTsv);
    }
    accession(accession_value)?;
    cik(cik_value)?;
    series_id(series_value)?;
    required_identifier(value)
}

fn optional_number(value: &str) -> Option<SecExactNumber> {
    (!value.is_empty()).then(|| SecExactNumber::from_validated_lexical(value))
}

fn optional_u64(value: &str) -> Result<Option<u64>, SecBulkError> {
    if value.is_empty() {
        Ok(None)
    } else {
        value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| SecBulkError::InvalidTsv)
    }
}

fn optional_bool(value: &str) -> Result<Option<bool>, SecBulkError> {
    match value {
        "" => Ok(None),
        "Y" => Ok(Some(true)),
        "N" => Ok(Some(false)),
        _ => Err(SecBulkError::InvalidTsv),
    }
}

fn optional_date(value: &str) -> Result<Option<NaiveDate>, SecBulkError> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_sec_date(value).map(Some)
}

fn parse_sec_date(value: &str) -> Result<NaiveDate, SecBulkError> {
    let bytes = value.as_bytes();
    const MONTHS: [&[u8; 3]; 12] = [
        b"JAN", b"FEB", b"MAR", b"APR", b"MAY", b"JUN", b"JUL", b"AUG", b"SEP", b"OCT", b"NOV",
        b"DEC",
    ];
    if bytes.len() != 11
        || bytes[2] != b'-'
        || bytes[6] != b'-'
        || !bytes[..2].iter().all(u8::is_ascii_digit)
        || !MONTHS.iter().any(|month| bytes[3..6] == month[..])
        || !bytes[7..].iter().all(u8::is_ascii_digit)
    {
        return Err(SecBulkError::InvalidTsv);
    }
    NaiveDate::parse_from_str(value, "%d-%b-%Y").map_err(|_| SecBulkError::InvalidTsv)
}

fn sha256(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

fn check_cancelled(
    cancellation: &CancellationToken,
    deadline: Timestamp,
) -> Result<(), SecBulkError> {
    if cancellation.is_cancelled() {
        return Err(SecBulkError::Cancelled);
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SecBulkError::DeadlineExceeded)?;
    let seconds = i64::try_from(now.as_secs()).map_err(|_| SecBulkError::DeadlineExceeded)?;
    let nanos = seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i64::from(now.subsec_nanos())))
        .ok_or(SecBulkError::DeadlineExceeded)?;
    if nanos >= deadline.unix_nanos() {
        return Err(SecBulkError::DeadlineExceeded);
    }
    Ok(())
}
