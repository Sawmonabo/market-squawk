use std::mem::size_of;
use std::time::Instant;

use market_squawk_domain::{SourceId, SourceIdentifier, Timestamp, VenueId};
use market_squawk_sources::SourceMetadata;
use rusqlite::{OptionalExtension as _, params};
use tokio_util::sync::CancellationToken;

use super::canonical;
use super::persistence::{exact_evidence, load_generation_receipt};
use super::{
    CatalogAuthority, ListingReferenceError, ListingReferenceExchangeCode,
    ListingReferenceFileEvidence, ListingReferenceFileKind, ListingReferenceFinancialStatus,
    ListingReferenceGenerationReceipt, ListingReferenceMarketCategory, ListingReferenceMatchKind,
    ListingReferenceRecord, ListingReferenceRecordInput, ListingReferenceSearchMatch,
    ListingReferenceSearchPage,
};
use crate::catalog::storage::{ResultBudget, now_timestamp, sha256};

const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;

impl CatalogAuthority {
    pub(super) fn current_listing_reference_generation(
        &self,
        dataset: &SourceIdentifier,
        source_id: &SourceId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ListingReferenceGenerationReceipt>, ListingReferenceError> {
        canonical::check_operation(deadline, cancellation)?;
        let connection = &self.catalog().connection;
        let digest: Option<Vec<u8>> = connection
            .query_row(
                "SELECT generation_digest FROM listing_reference_generations
                 WHERE dataset_id=?1 ORDER BY generation_sequence DESC LIMIT 1",
                [dataset.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(digest) = digest else {
            return Ok(None);
        };
        let digest: [u8; 32] = digest
            .try_into()
            .map_err(|_| ListingReferenceError::CorruptCatalog)?;
        let receipt = load_generation_receipt(connection, digest)?
            .ok_or(ListingReferenceError::CorruptCatalog)?;
        if receipt.dataset() != dataset || receipt.source_id() != source_id {
            return Err(ListingReferenceError::CorruptCatalog);
        }
        require_current_display_authority(connection, &receipt)?;
        canonical::check_operation(deadline, cancellation)?;
        Ok(Some(receipt))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "bounded read coordinates stay explicit"
    )]
    pub(super) fn search_listing_references(
        &self,
        dataset: &SourceIdentifier,
        source_id: &SourceId,
        query: &str,
        maximum_rows: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ListingReferenceSearchPage, ListingReferenceError> {
        let Some(generation) =
            self.current_listing_reference_generation(dataset, source_id, deadline, cancellation)?
        else {
            return Ok(ListingReferenceSearchPage {
                matches: Box::new([]),
                has_more: false,
            });
        };
        let connection = &self.catalog().connection;
        let token = cancellation.clone();
        connection.progress_handler(
            SQLITE_PROGRESS_OPERATIONS,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )?;
        let result = (|| {
            let retrieval_limit = i64::try_from(maximum_rows.saturating_add(1))
                .map_err(|_| ListingReferenceError::InvalidLimit)?;
            let symbol_query = canonical::normalize_symbol(query);
            let name_query = canonical::normalize_name(query);
            let mut statement = connection.prepare(CURRENT_LISTING_SEARCH_SQL)?;
            let rows = statement.query_map(
                params![
                    generation.generation_digest().bytes(),
                    symbol_query,
                    name_query,
                    retrieval_limit,
                ],
                decode_row,
            )?;
            let mut budget = ResultBudget::new(self.catalog().result_bytes);
            let mut matches = Vec::new();
            matches
                .try_reserve_exact(maximum_rows.saturating_add(1))
                .map_err(|_| ListingReferenceError::MemoryLimitExceeded)?;
            for row in rows {
                canonical::check_operation(deadline, cancellation)?;
                let row = row?;
                budget
                    .charge([
                        size_of::<ListingReferenceSearchMatch>(),
                        row.file_kind.len(),
                        row.source_object_id.len(),
                        row.source_reference.len(),
                        row.file_creation_time.len(),
                        row.file_locator_reference.as_ref().map_or(0, String::len),
                        row.file_locator_version.as_ref().map_or(0, String::len),
                        row.provider_symbol.len(),
                        row.security_name.len(),
                        row.listing_venue.len(),
                        row.exchange_code.as_ref().map_or(0, String::len),
                        row.cqs_symbol.as_ref().map_or(0, String::len),
                        row.nasdaq_symbol.as_ref().map_or(0, String::len),
                        row.market_category.as_ref().map_or(0, String::len),
                        row.financial_status.as_ref().map_or(0, String::len),
                        row.directory_presence.len(),
                        row.data_quality.len(),
                        row.authority_class.len(),
                        row.record_revision.len(),
                        row.record_locator_reference.as_ref().map_or(0, String::len),
                        row.record_locator_version.as_ref().map_or(0, String::len),
                        row.match_kind.len(),
                    ])
                    .map_err(|_| ListingReferenceError::MemoryLimitExceeded)?;
                matches.push(rebuild_match(&generation, row)?);
            }
            let has_more = matches.len() > maximum_rows;
            matches.truncate(maximum_rows);
            Ok(ListingReferenceSearchPage {
                matches: matches.into_boxed_slice(),
                has_more,
            })
        })();
        connection.progress_handler::<fn() -> bool>(0, None)?;
        classify_operation(result, deadline, cancellation)
    }
}

fn require_current_display_authority(
    connection: &rusqlite::Connection,
    receipt: &ListingReferenceGenerationReceipt,
) -> Result<(), ListingReferenceError> {
    let now = now_timestamp().map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let durable_clock: i64 = connection.query_row(
        "SELECT last_timestamp_ns FROM catalog_authority_clock WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    if now.unix_nanos() < durable_clock {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    let authorized: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM source_rights
             WHERE rights_id=?1 AND source_id=?2
               AND (operation_mask & 2)=2
               AND admitted_at_ns<=?3
               AND (authorization_expires_at_ns IS NULL OR authorization_expires_at_ns>?3)
         )",
        params![
            receipt.rights_id(),
            receipt.source_id().as_str(),
            now.unix_nanos(),
        ],
        |row| row.get(0),
    )?;
    if !authorized {
        return Err(ListingReferenceError::RightsUnavailable);
    }
    let metadata_json: Option<String> = connection
        .query_row(
            "SELECT metadata_json FROM source_revisions
             WHERE source_id=?1 AND revision_digest=?2",
            params![
                receipt.source_id().as_str(),
                receipt.source_revision_digest().bytes(),
            ],
            |row| row.get(0),
        )
        .optional()?;
    let metadata_json = metadata_json.ok_or(ListingReferenceError::CorruptCatalog)?;
    if sha256(metadata_json.as_bytes()) != receipt.source_revision_digest().bytes() {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    let metadata: SourceMetadata =
        serde_json::from_str(&metadata_json).map_err(|_| ListingReferenceError::CorruptCatalog)?;
    if metadata.source_id() != receipt.source_id()
        || metadata.revision().as_source_identifier() != receipt.source_revision()
        || !metadata.is_effective_at(now)
    {
        return Err(ListingReferenceError::RightsUnavailable);
    }
    Ok(())
}

#[derive(Debug)]
struct StoredListingRow {
    file_kind: String,
    source_object_id: String,
    source_reference: String,
    file_creation_time: String,
    file_algorithm: i64,
    file_payload_digest: Vec<u8>,
    file_locator_reference: Option<String>,
    file_locator_version: Option<String>,
    source_last_modified_at: i64,
    received_at: i64,
    available_at: i64,
    ingested_at: i64,
    file_record_count: i64,
    provider_row_number: i64,
    provider_symbol: String,
    security_name: String,
    listing_venue: String,
    exchange_code: Option<String>,
    cqs_symbol: Option<String>,
    nasdaq_symbol: Option<String>,
    market_category: Option<String>,
    financial_status: Option<String>,
    is_etf: i64,
    is_test_issue: i64,
    round_lot_size: i64,
    is_next_shares: Option<i64>,
    directory_presence: String,
    data_quality: String,
    authority_class: String,
    record_revision: String,
    record_algorithm: i64,
    record_payload_digest: Vec<u8>,
    record_locator_reference: Option<String>,
    record_locator_version: Option<String>,
    value_digest: Vec<u8>,
    record_digest: Vec<u8>,
    match_kind: String,
}

fn decode_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredListingRow> {
    Ok(StoredListingRow {
        file_kind: row.get(0)?,
        source_object_id: row.get(1)?,
        source_reference: row.get(2)?,
        file_creation_time: row.get(3)?,
        file_algorithm: row.get(4)?,
        file_payload_digest: row.get(5)?,
        file_locator_reference: row.get(6)?,
        file_locator_version: row.get(7)?,
        source_last_modified_at: row.get(8)?,
        received_at: row.get(9)?,
        available_at: row.get(10)?,
        ingested_at: row.get(11)?,
        file_record_count: row.get(12)?,
        provider_row_number: row.get(13)?,
        provider_symbol: row.get(14)?,
        security_name: row.get(15)?,
        listing_venue: row.get(16)?,
        exchange_code: row.get(17)?,
        cqs_symbol: row.get(18)?,
        nasdaq_symbol: row.get(19)?,
        market_category: row.get(20)?,
        financial_status: row.get(21)?,
        is_etf: row.get(22)?,
        is_test_issue: row.get(23)?,
        round_lot_size: row.get(24)?,
        is_next_shares: row.get(25)?,
        directory_presence: row.get(26)?,
        data_quality: row.get(27)?,
        authority_class: row.get(28)?,
        record_revision: row.get(29)?,
        record_algorithm: row.get(30)?,
        record_payload_digest: row.get(31)?,
        record_locator_reference: row.get(32)?,
        record_locator_version: row.get(33)?,
        value_digest: row.get(34)?,
        record_digest: row.get(35)?,
        match_kind: row.get(36)?,
    })
}

fn rebuild_match(
    generation: &ListingReferenceGenerationReceipt,
    row: StoredListingRow,
) -> Result<ListingReferenceSearchMatch, ListingReferenceError> {
    let kind = ListingReferenceFileKind::from_database(&row.file_kind)?;
    let file_payload_evidence = exact_evidence(
        row.file_algorithm,
        row.file_payload_digest,
        row.file_locator_reference,
        row.file_locator_version,
    )?;
    let record_payload_evidence = exact_evidence(
        row.record_algorithm,
        row.record_payload_digest,
        row.record_locator_reference,
        row.record_locator_version,
    )?;
    let venue =
        VenueId::try_from(row.listing_venue).map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let row_number = u32::try_from(row.provider_row_number)
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let round_lot_size =
        u32::try_from(row.round_lot_size).map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let revision = SourceIdentifier::try_from(row.record_revision)
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let is_etf = parse_bool(row.is_etf)?;
    let is_test_issue = parse_bool(row.is_test_issue)?;
    let record = match kind {
        ListingReferenceFileKind::NasdaqListed => ListingReferenceRecordInput::try_nasdaq_listed(
            row_number,
            row.provider_symbol.clone(),
            row.security_name.clone(),
            venue.clone(),
            ListingReferenceMarketCategory::from_database(
                row.market_category
                    .as_deref()
                    .ok_or(ListingReferenceError::CorruptCatalog)?,
            )?,
            ListingReferenceFinancialStatus::from_database(
                row.financial_status
                    .as_deref()
                    .ok_or(ListingReferenceError::CorruptCatalog)?,
            )?,
            is_etf,
            is_test_issue,
            round_lot_size,
            parse_optional_bool(row.is_next_shares)?
                .ok_or(ListingReferenceError::CorruptCatalog)?,
            revision.clone(),
            record_payload_evidence.clone(),
            row.file_creation_time.clone(),
            Timestamp::from_unix_nanos(row.source_last_modified_at),
            Timestamp::from_unix_nanos(row.received_at),
            file_payload_evidence.clone(),
        )?,
        ListingReferenceFileKind::OtherListed => ListingReferenceRecordInput::try_other_listed(
            row_number,
            row.provider_symbol.clone(),
            row.security_name.clone(),
            venue.clone(),
            ListingReferenceExchangeCode::from_database(
                row.exchange_code
                    .as_deref()
                    .ok_or(ListingReferenceError::CorruptCatalog)?,
            )?,
            row.cqs_symbol
                .clone()
                .ok_or(ListingReferenceError::CorruptCatalog)?,
            row.nasdaq_symbol
                .clone()
                .ok_or(ListingReferenceError::CorruptCatalog)?,
            is_etf,
            is_test_issue,
            round_lot_size,
            revision.clone(),
            record_payload_evidence.clone(),
            row.file_creation_time.clone(),
            Timestamp::from_unix_nanos(row.source_last_modified_at),
            Timestamp::from_unix_nanos(row.received_at),
            file_payload_evidence.clone(),
        )?,
    };
    let value_digest: [u8; 32] = row
        .value_digest
        .try_into()
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    let record_digest: [u8; 32] = row
        .record_digest
        .try_into()
        .map_err(|_| ListingReferenceError::CorruptCatalog)?;
    if canonical::value_digest(&record) != value_digest
        || canonical::record_digest(kind, &record, value_digest) != record_digest
        || row.directory_presence != "current_directory"
        || row.data_quality != "official_delayed"
        || row.authority_class != "reference_only"
        || row.available_at != row.received_at
        || row.ingested_at < row.received_at
        || row.ingested_at < row.available_at
    {
        return Err(ListingReferenceError::CorruptCatalog);
    }
    let source_file = ListingReferenceFileEvidence {
        kind,
        source_object_id: SourceIdentifier::try_from(row.source_object_id)
            .map_err(|_| ListingReferenceError::CorruptCatalog)?,
        source_reference: SourceIdentifier::try_from(row.source_reference)
            .map_err(|_| ListingReferenceError::CorruptCatalog)?,
        file_creation_time: row.file_creation_time,
        payload_evidence: file_payload_evidence,
        source_last_modified_at: Timestamp::from_unix_nanos(row.source_last_modified_at),
        received_at: Timestamp::from_unix_nanos(row.received_at),
        available_at: Timestamp::from_unix_nanos(row.available_at),
        ingested_at: Timestamp::from_unix_nanos(row.ingested_at),
        record_count: usize::try_from(row.file_record_count)
            .map_err(|_| ListingReferenceError::CorruptCatalog)?,
    };
    let record = ListingReferenceRecord {
        generation: generation.clone(),
        source_file,
        provider_row_number: record.provider_row_number,
        provider_symbol: record.provider_symbol,
        security_name: record.security_name,
        listing_venue: record.listing_venue,
        exchange_code: record.exchange_code,
        cqs_symbol: record.cqs_symbol,
        nasdaq_symbol: record.nasdaq_symbol,
        market_category: record.market_category,
        financial_status: record.financial_status,
        is_etf: record.is_etf,
        is_test_issue: record.is_test_issue,
        round_lot_size: record.round_lot_size,
        is_next_shares: record.is_next_shares,
        record_revision: record.record_revision,
        record_payload_evidence,
    };
    let match_kind = match row.match_kind.as_str() {
        "provider_symbol" => ListingReferenceMatchKind::ProviderSymbol,
        "security_name" => ListingReferenceMatchKind::SecurityName,
        "cqs_symbol" => ListingReferenceMatchKind::CqsSymbol,
        "nasdaq_symbol" => ListingReferenceMatchKind::NasdaqSymbol,
        _ => return Err(ListingReferenceError::CorruptCatalog),
    };
    Ok(ListingReferenceSearchMatch { record, match_kind })
}

fn parse_bool(value: i64) -> Result<bool, ListingReferenceError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ListingReferenceError::CorruptCatalog),
    }
}

fn parse_optional_bool(value: Option<i64>) -> Result<Option<bool>, ListingReferenceError> {
    value.map(parse_bool).transpose()
}

fn classify_operation<T>(
    result: Result<T, ListingReferenceError>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<T, ListingReferenceError> {
    if cancellation.is_cancelled() {
        Err(ListingReferenceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ListingReferenceError::DeadlineExceeded)
    } else {
        result
    }
}

const CURRENT_LISTING_SEARCH_SQL: &str = r#"
SELECT files.file_kind,
       files.source_object_id,
       files.source_reference,
       files.file_creation_time,
       files.payload_algorithm,
       files.payload_digest,
       files.payload_locator_reference,
       files.payload_locator_version,
       files.source_last_modified_at_ns,
       files.received_at_ns,
       files.available_at_ns,
       files.ingested_at_ns,
       files.record_count,
       memberships.provider_row_number,
       values_.provider_symbol,
       values_.security_name,
       values_.listing_venue,
       values_.exchange_code,
       values_.cqs_symbol,
       values_.nasdaq_symbol,
       values_.market_category,
       values_.financial_status,
       values_.is_etf,
       values_.is_test_issue,
       values_.round_lot_size,
       values_.is_next_shares,
       values_.directory_presence,
       values_.data_quality,
       values_.authority_class,
       memberships.record_revision,
       memberships.record_algorithm,
       memberships.record_payload_digest,
       memberships.record_locator_reference,
       memberships.record_locator_version,
       memberships.value_digest,
       memberships.record_digest,
       CASE
           WHEN instr(values_.normalized_provider_symbol, ?2)>0 THEN 'provider_symbol'
           WHEN values_.cqs_symbol IS NOT NULL AND instr(upper(values_.cqs_symbol), ?2)>0
               THEN 'cqs_symbol'
           WHEN values_.nasdaq_symbol IS NOT NULL AND instr(upper(values_.nasdaq_symbol), ?2)>0
               THEN 'nasdaq_symbol'
           ELSE 'security_name'
       END AS match_kind
FROM listing_reference_memberships AS memberships
JOIN listing_reference_values AS values_ ON values_.value_digest=memberships.value_digest
JOIN listing_reference_files AS files
  ON files.generation_digest=memberships.generation_digest
 AND files.file_kind=memberships.file_kind
WHERE memberships.generation_digest=?1
  AND (
      instr(values_.normalized_provider_symbol, ?2)>0
      OR instr(values_.normalized_security_name, ?3)>0
      OR (values_.cqs_symbol IS NOT NULL AND instr(upper(values_.cqs_symbol), ?2)>0)
      OR (values_.nasdaq_symbol IS NOT NULL AND instr(upper(values_.nasdaq_symbol), ?2)>0)
  )
ORDER BY CASE
    WHEN values_.normalized_provider_symbol=?2 THEN 0
    WHEN values_.cqs_symbol IS NOT NULL AND upper(values_.cqs_symbol)=?2 THEN 1
    WHEN values_.nasdaq_symbol IS NOT NULL AND upper(values_.nasdaq_symbol)=?2 THEN 2
    WHEN instr(values_.normalized_provider_symbol, ?2)=1 THEN 3
    WHEN values_.cqs_symbol IS NOT NULL AND instr(upper(values_.cqs_symbol), ?2)=1 THEN 4
    WHEN values_.nasdaq_symbol IS NOT NULL AND instr(upper(values_.nasdaq_symbol), ?2)=1 THEN 5
    ELSE 6
END,
values_.provider_symbol,
values_.listing_venue
LIMIT ?4
"#;
