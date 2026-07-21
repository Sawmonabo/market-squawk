use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::query::{TreasuryFiscalQuery, TreasuryPageRequest, update_component};
use crate::rates::parse_date;
use market_squawk_domain::CalendarDate;

/// Parser admission limits for one Treasury Fiscal Data page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FiscalDataParseLimits {
    max_bytes: usize,
    max_records: usize,
    max_fields: usize,
    max_total_pages: usize,
}

impl FiscalDataParseLimits {
    /// Builds explicit non-zero parser bounds.
    pub fn try_new(
        max_bytes: usize,
        max_records: usize,
        max_fields: usize,
        max_total_pages: usize,
    ) -> Result<Self, TreasuryProtocolError> {
        if max_bytes == 0 || max_records == 0 || max_fields == 0 || max_total_pages == 0 {
            return Err(TreasuryProtocolError::InvalidLimits);
        }
        Ok(Self {
            max_bytes,
            max_records,
            max_fields,
            max_total_pages,
        })
    }

    /// Returns conservative production bounds exceeding the provider's default page size.
    pub const fn production_defaults() -> Self {
        Self {
            max_bytes: 32 * 1024 * 1024,
            max_records: 10_000,
            max_fields: 512,
            max_total_pages: 100_000,
        }
    }

    pub(crate) const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    pub(crate) const fn max_records(self) -> usize {
        self.max_records
    }

    pub(crate) const fn max_fields(self) -> usize {
        self.max_fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FiscalDataSchema {
    data_types: BTreeMap<String, String>,
    data_formats: BTreeMap<String, String>,
    digest: [u8; 32],
}

impl FiscalDataSchema {
    pub(crate) fn data_type(&self, field: &str) -> Option<&str> {
        self.data_types.get(field).map(String::as_str)
    }

    pub(crate) fn data_format(&self, field: &str) -> Option<&str> {
        self.data_formats.get(field).map(String::as_str)
    }
}

/// One Fiscal Data row bound to the exact response schema digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FiscalDataRecord {
    values: BTreeMap<String, String>,
    schema: Arc<FiscalDataSchema>,
    row_identity: [u8; 32],
    sort_key: (CalendarDate, u64),
    source_payload_digest: [u8; 32],
}

impl FiscalDataRecord {
    /// Returns one exact provider string value.
    pub fn get(&self, field: &str) -> Option<&str> {
        self.values.get(field).map(String::as_str)
    }

    /// Returns the canonical digest of labels, logical types, and formats.
    pub fn schema_digest(&self) -> [u8; 32] {
        self.schema.digest
    }

    /// Returns the canonical provider-primary-key identity for this row.
    pub const fn row_identity(&self) -> [u8; 32] {
        self.row_identity
    }

    /// Returns the SHA-256 identity of the exact response payload containing this row.
    pub const fn source_payload_digest(&self) -> [u8; 32] {
        self.source_payload_digest
    }

    pub(crate) fn schema(&self) -> &FiscalDataSchema {
        &self.schema
    }
}

/// One validated Treasury Fiscal Data page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FiscalDataPage {
    page_number: usize,
    total_count: usize,
    total_pages: usize,
    schema: Arc<FiscalDataSchema>,
    records: Vec<FiscalDataRecord>,
    query_digest: [u8; 32],
    request_digest: [u8; 32],
    source_payload_digest: [u8; 32],
}

impl FiscalDataPage {
    /// Parses a page and proves its links match the expected one-based page number.
    pub fn parse(
        bytes: &[u8],
        request: &TreasuryPageRequest,
        limits: FiscalDataParseLimits,
    ) -> Result<Self, TreasuryProtocolError> {
        let expected_page = request.page_number();
        if expected_page == 0 {
            return Err(TreasuryProtocolError::UnexpectedPage {
                expected: 1,
                actual: 0,
            });
        }
        if bytes.len() > limits.max_bytes {
            return Err(TreasuryProtocolError::BodyTooLarge);
        }
        let wire: PageWire = serde_json::from_slice(bytes)?;
        if wire.data.len() > limits.max_records
            || wire.meta.labels.is_empty()
            || wire.meta.labels.len() > limits.max_fields
            || wire.meta.total_pages == 0
            || wire.meta.total_pages > limits.max_total_pages
            || expected_page > wire.meta.total_pages
            || wire.meta.count != wire.data.len()
            || wire.meta.total_count < wire.data.len()
        {
            return Err(TreasuryProtocolError::InvalidMetadata);
        }
        let label_keys = wire.meta.labels.keys().collect::<Vec<_>>();
        if label_keys != wire.meta.data_types.keys().collect::<Vec<_>>()
            || label_keys != wire.meta.data_formats.keys().collect::<Vec<_>>()
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let expected_fields = request.fields().iter().copied().collect::<BTreeSet<_>>();
        if wire
            .meta
            .labels
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            != expected_fields
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        for record in &wire.data {
            if label_keys != record.keys().collect::<Vec<_>>() {
                return Err(TreasuryProtocolError::SchemaDrift);
            }
            if record
                .iter()
                .any(|(key, value)| key.len() > 256 || value.len() > 64 * 1024)
            {
                return Err(TreasuryProtocolError::FieldTooLarge);
            }
        }

        let actual_page = page_from_link(&wire.links.self_link)?;
        if actual_page != expected_page {
            return Err(TreasuryProtocolError::UnexpectedPage {
                expected: expected_page,
                actual: actual_page,
            });
        }
        let expected_page_size = usize::from(request.page_size().get());
        if page_size_from_link(&wire.links.self_link)? != expected_page_size
            || page_from_link(&wire.links.first)? != 1
            || page_size_from_link(&wire.links.first)? != expected_page_size
            || page_from_link(&wire.links.last)? != wire.meta.total_pages
            || page_size_from_link(&wire.links.last)? != expected_page_size
        {
            return Err(TreasuryProtocolError::InvalidLinks);
        }
        validate_optional_link(
            wire.links.prev.as_deref(),
            expected_page.checked_sub(1).filter(|_| expected_page > 1),
            expected_page_size,
        )?;
        validate_optional_link(
            wire.links.next.as_deref(),
            expected_page
                .checked_add(1)
                .filter(|_| expected_page < wire.meta.total_pages),
            expected_page_size,
        )?;

        let digest = schema_digest(
            &wire.meta.labels,
            &wire.meta.data_types,
            &wire.meta.data_formats,
        );
        let schema = Arc::new(FiscalDataSchema {
            data_types: wire.meta.data_types,
            data_formats: wire.meta.data_formats,
            digest,
        });
        let source_payload_digest = Sha256::digest(bytes).into();
        let records = wire
            .data
            .into_iter()
            .map(|values| {
                let row_identity = row_identity(request.primary_key(), &values)?;
                let record_date = values
                    .get("record_date")
                    .ok_or(TreasuryProtocolError::SchemaDrift)
                    .and_then(|value| {
                        parse_date(value).map_err(|_| TreasuryProtocolError::SchemaDrift)
                    })?;
                if !request.contains_record_date(record_date) {
                    return Err(TreasuryProtocolError::QueryBindingMismatch);
                }
                let source_line = values
                    .get("src_line_nbr")
                    .ok_or(TreasuryProtocolError::SchemaDrift)?
                    .parse::<u64>()
                    .map_err(|_| TreasuryProtocolError::SchemaDrift)?;
                if source_line == 0 {
                    return Err(TreasuryProtocolError::SchemaDrift);
                }
                Ok(FiscalDataRecord {
                    values,
                    schema: Arc::clone(&schema),
                    row_identity,
                    sort_key: (record_date, source_line),
                    source_payload_digest,
                })
            })
            .collect::<Result<Vec<_>, TreasuryProtocolError>>()?;
        if records
            .windows(2)
            .any(|records| records[0].sort_key >= records[1].sort_key)
        {
            return Err(TreasuryProtocolError::PageDrift);
        }
        Ok(Self {
            page_number: expected_page,
            total_count: wire.meta.total_count,
            total_pages: wire.meta.total_pages,
            schema,
            records,
            query_digest: request.query_digest(),
            request_digest: request.request_digest(),
            source_payload_digest,
        })
    }

    /// Returns the one-based page number.
    pub const fn page_number(&self) -> usize {
        self.page_number
    }

    /// Returns the provider total row count.
    pub const fn total_count(&self) -> usize {
        self.total_count
    }

    /// Returns the provider total page count.
    pub const fn total_pages(&self) -> usize {
        self.total_pages
    }

    /// Returns validated rows.
    pub fn records(&self) -> &[FiscalDataRecord] {
        &self.records
    }

    /// Returns the canonical response schema digest.
    pub fn schema_digest(&self) -> [u8; 32] {
        self.schema.digest
    }

    /// Returns the canonical query-family digest used to request this page.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }

    /// Returns the canonical request digest including this page number.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    /// Returns the SHA-256 identity of the exact source response.
    pub const fn response_payload_digest(&self) -> [u8; 32] {
        self.source_payload_digest
    }
}

/// Cross-page state that rejects repetition and schema or total drift.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryPaginationTracker {
    query_digest: [u8; 32],
    max_pages: usize,
    max_records: usize,
    expected_page: usize,
    expected_total_pages: Option<usize>,
    expected_total_count: Option<usize>,
    expected_schema: Option<[u8; 32]>,
    accepted_records: usize,
    accepted_row_identities: BTreeSet<[u8; 32]>,
    last_sort_key: Option<(CalendarDate, u64)>,
}

impl TreasuryPaginationTracker {
    /// Builds a one-based tracker with explicit whole-query limits.
    pub fn try_new(
        query: &TreasuryFiscalQuery,
        max_pages: usize,
        max_records: usize,
    ) -> Result<Self, TreasuryProtocolError> {
        if max_pages == 0 || max_records == 0 {
            return Err(TreasuryProtocolError::InvalidLimits);
        }
        Ok(Self {
            query_digest: query.query_digest(),
            max_pages,
            max_records,
            expected_page: 1,
            expected_total_pages: None,
            expected_total_count: None,
            expected_schema: None,
            accepted_records: 0,
            accepted_row_identities: BTreeSet::new(),
            last_sort_key: None,
        })
    }

    /// Accepts exactly the next page and returns `true` only for the terminal page.
    pub fn accept(&mut self, page: &FiscalDataPage) -> Result<bool, TreasuryProtocolError> {
        if page.query_digest != self.query_digest {
            return Err(TreasuryProtocolError::QueryBindingMismatch);
        }
        if page.page_number != self.expected_page {
            return Err(TreasuryProtocolError::UnexpectedPage {
                expected: self.expected_page,
                actual: page.page_number,
            });
        }
        if page.total_pages > self.max_pages || page.total_count > self.max_records {
            return Err(TreasuryProtocolError::PaginationLimitExceeded);
        }
        if let Some(expected) = self.expected_total_pages {
            if expected != page.total_pages
                || self.expected_total_count != Some(page.total_count)
                || self.expected_schema != Some(page.schema_digest())
            {
                return Err(TreasuryProtocolError::PageDrift);
            }
        } else {
            self.expected_total_pages = Some(page.total_pages);
            self.expected_total_count = Some(page.total_count);
            self.expected_schema = Some(page.schema_digest());
        }
        self.accepted_records = self
            .accepted_records
            .checked_add(page.records.len())
            .ok_or(TreasuryProtocolError::PaginationLimitExceeded)?;
        if self.accepted_records > self.max_records {
            return Err(TreasuryProtocolError::PaginationLimitExceeded);
        }
        for record in &page.records {
            if self
                .last_sort_key
                .is_some_and(|last| last >= record.sort_key)
            {
                return Err(TreasuryProtocolError::PageDrift);
            }
            if !self.accepted_row_identities.insert(record.row_identity) {
                return Err(TreasuryProtocolError::DuplicateRecordIdentity);
            }
            self.last_sort_key = Some(record.sort_key);
        }
        let terminal = page.page_number == page.total_pages;
        if terminal {
            if self.accepted_records != page.total_count {
                return Err(TreasuryProtocolError::PageDrift);
            }
        } else {
            self.expected_page = self
                .expected_page
                .checked_add(1)
                .ok_or(TreasuryProtocolError::PaginationLimitExceeded)?;
        }
        Ok(terminal)
    }
}

/// A bounded Fiscal Data protocol failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum TreasuryProtocolError {
    /// Parser or pagination limits are zero.
    #[error("Treasury parser limits must be non-zero")]
    InvalidLimits,
    /// The configured official query is invalid or outside supported bounds.
    #[error("invalid Treasury query")]
    InvalidQuery,
    /// The body exceeds its byte budget.
    #[error("Treasury response exceeds its byte budget")]
    BodyTooLarge,
    /// The JSON shape is invalid.
    #[error("invalid Treasury JSON response: {0}")]
    InvalidJson(String),
    /// The Treasury XML shape is invalid.
    #[error("invalid Treasury XML response: {0}")]
    InvalidXml(String),
    /// Page metadata is inconsistent.
    #[error("invalid Treasury page metadata")]
    InvalidMetadata,
    /// Labels, types, formats, or row fields differ.
    #[error("Treasury response schema drifted")]
    SchemaDrift,
    /// A retained provider field exceeds its size budget.
    #[error("Treasury field exceeds its size budget")]
    FieldTooLarge,
    /// Pagination links are absent or inconsistent.
    #[error("invalid Treasury pagination links")]
    InvalidLinks,
    /// A page arrived outside the required sequence.
    #[error("unexpected Treasury page: expected {expected}, received {actual}")]
    UnexpectedPage {
        /// Required one-based page number.
        expected: usize,
        /// Received one-based page number.
        actual: usize,
    },
    /// Whole-query pagination limits were exceeded.
    #[error("Treasury pagination exceeds configured limits")]
    PaginationLimitExceeded,
    /// Totals or schema changed across a paginated query.
    #[error("Treasury page totals or schema changed during pagination")]
    PageDrift,
    /// A page was produced for a different canonical query.
    #[error("Treasury page does not belong to the tracked canonical query")]
    QueryBindingMismatch,
    /// A provider primary key repeated within the bounded query.
    #[error("Treasury response repeated a provider row identity")]
    DuplicateRecordIdentity,
}

impl From<serde_json::Error> for TreasuryProtocolError {
    fn from(error: serde_json::Error) -> Self {
        Self::InvalidJson(error.to_string())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageWire {
    data: Vec<BTreeMap<String, String>>,
    meta: MetaWire,
    links: LinksWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetaWire {
    count: usize,
    labels: BTreeMap<String, String>,
    #[serde(rename = "dataTypes")]
    data_types: BTreeMap<String, String>,
    #[serde(rename = "dataFormats")]
    data_formats: BTreeMap<String, String>,
    #[serde(rename = "total-count")]
    total_count: usize,
    #[serde(rename = "total-pages")]
    total_pages: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LinksWire {
    #[serde(rename = "self")]
    self_link: String,
    first: String,
    prev: Option<String>,
    next: Option<String>,
    last: String,
}

fn page_from_link(link: &str) -> Result<usize, TreasuryProtocolError> {
    const MARKER: &str = "page%5Bnumber%5D=";
    let start = link
        .find(MARKER)
        .ok_or(TreasuryProtocolError::InvalidLinks)?
        + MARKER.len();
    let tail = &link[start..];
    let end = tail.find('&').unwrap_or(tail.len());
    let page = tail[..end]
        .parse::<usize>()
        .map_err(|_| TreasuryProtocolError::InvalidLinks)?;
    if page == 0 {
        return Err(TreasuryProtocolError::InvalidLinks);
    }
    Ok(page)
}

fn validate_optional_link(
    link: Option<&str>,
    expected: Option<usize>,
    expected_page_size: usize,
) -> Result<(), TreasuryProtocolError> {
    match (link, expected) {
        (None, None) => Ok(()),
        (Some(value), Some(page))
            if page_from_link(value)? == page
                && page_size_from_link(value)? == expected_page_size =>
        {
            Ok(())
        }
        _ => Err(TreasuryProtocolError::InvalidLinks),
    }
}

fn page_size_from_link(link: &str) -> Result<usize, TreasuryProtocolError> {
    const MARKER: &str = "page%5Bsize%5D=";
    let start = link
        .find(MARKER)
        .ok_or(TreasuryProtocolError::InvalidLinks)?
        + MARKER.len();
    let tail = &link[start..];
    let end = tail.find('&').unwrap_or(tail.len());
    let page_size = tail[..end]
        .parse::<usize>()
        .map_err(|_| TreasuryProtocolError::InvalidLinks)?;
    if page_size == 0 {
        return Err(TreasuryProtocolError::InvalidLinks);
    }
    Ok(page_size)
}

fn row_identity(
    primary_key: &[&str],
    values: &BTreeMap<String, String>,
) -> Result<[u8; 32], TreasuryProtocolError> {
    let mut digest = Sha256::new();
    update_component(&mut digest, "treasury-row-identity-v1");
    for field in primary_key {
        let value = values
            .get(*field)
            .ok_or(TreasuryProtocolError::SchemaDrift)?;
        update_component(&mut digest, field);
        update_component(&mut digest, value);
    }
    Ok(digest.finalize().into())
}

fn schema_digest(
    labels: &BTreeMap<String, String>,
    types: &BTreeMap<String, String>,
    formats: &BTreeMap<String, String>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    for (field, label) in labels {
        update_component(&mut digest, field);
        update_component(&mut digest, label);
        if let Some(data_type) = types.get(field) {
            update_component(&mut digest, data_type);
        }
        if let Some(format) = formats.get(field) {
            update_component(&mut digest, format);
        }
    }
    digest.finalize().into()
}
