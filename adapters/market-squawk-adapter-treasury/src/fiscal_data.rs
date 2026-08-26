use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt;
use std::mem::size_of;
use std::num::NonZeroU16;
use std::sync::Arc;

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::query::{
    TreasuryFiscalQuery, TreasuryPageRequest, canonical_page_token, update_component,
};
use crate::rates::parse_date;
use market_squawk_domain::CalendarDate;

const MAX_FISCAL_FIELD_KEY_BYTES: usize = 256;
const MAX_FISCAL_FIELD_VALUE_BYTES: usize = 64 * 1024;
const MAX_FISCAL_DECODED_WORK_BYTES: usize = 128 * 1024 * 1024;
const MIN_FISCAL_VECTOR_CAPACITY: usize = 4;

type FiscalFields = Vec<(String, String)>;

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
    data_types: FiscalFields,
    data_formats: FiscalFields,
    digest: [u8; 32],
}

impl FiscalDataSchema {
    pub(crate) fn data_type(&self, field: &str) -> Option<&str> {
        field_value(&self.data_types, field)
    }

    pub(crate) fn data_format(&self, field: &str) -> Option<&str> {
        field_value(&self.data_formats, field)
    }
}

/// One Fiscal Data row bound to the exact response schema digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FiscalDataRecord {
    values: FiscalFields,
    schema: Arc<FiscalDataSchema>,
    row_identity: [u8; 32],
    sort_key: (CalendarDate, u64),
    source_payload_digest: [u8; 32],
}

impl FiscalDataRecord {
    /// Returns one exact provider string value.
    pub fn get(&self, field: &str) -> Option<&str> {
        field_value(&self.values, field)
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
    next_page_token: Option<String>,
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
        let mut budget = FiscalDecodeBudget::new(limits);
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        let decoded = PageWireSeed {
            budget: &mut budget,
        }
        .deserialize(&mut deserializer);
        let wire = match decoded {
            Ok(wire) => wire,
            Err(error) => return Err(budget.protocol_error(error)),
        };
        deserializer.end()?;
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
        if !same_field_names(&wire.meta.labels, &wire.meta.data_types)
            || !same_field_names(&wire.meta.labels, &wire.meta.data_formats)
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        if wire.meta.labels.len() != request.fields().len()
            || request
                .fields()
                .iter()
                .any(|field| field_value(&wire.meta.labels, field).is_none())
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        for record in &wire.data {
            if !same_field_names(&wire.meta.labels, record) {
                return Err(TreasuryProtocolError::SchemaDrift);
            }
            if record.iter().any(|(key, value)| {
                key.len() > MAX_FISCAL_FIELD_KEY_BYTES || value.len() > MAX_FISCAL_FIELD_VALUE_BYTES
            }) {
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

        let next_page_token = wire
            .links
            .next
            .as_deref()
            .map(|link| -> Result<String, TreasuryProtocolError> {
                Ok(canonical_page_token(
                    page_from_link(link)?,
                    NonZeroU16::new(
                        u16::try_from(page_size_from_link(link)?)
                            .map_err(|_| TreasuryProtocolError::InvalidLinks)?,
                    )
                    .ok_or(TreasuryProtocolError::InvalidLinks)?,
                ))
            })
            .transpose()?;
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
        budget.charge(size_of::<FiscalDataPage>())?;
        let mut records = Vec::new();
        budget.reserve_exact_capacity(&mut records, wire.data.len(), limits.max_records)?;
        for values in wire.data {
            let row_identity = row_identity(request.primary_key(), &values)?;
            let record_date = field_value(&values, "record_date")
                .ok_or(TreasuryProtocolError::SchemaDrift)
                .and_then(|value| {
                    parse_date(value).map_err(|_| TreasuryProtocolError::SchemaDrift)
                })?;
            if !request.contains_record_date(record_date) {
                return Err(TreasuryProtocolError::QueryBindingMismatch);
            }
            let source_line = field_value(&values, "src_line_nbr")
                .ok_or(TreasuryProtocolError::SchemaDrift)?
                .parse::<u64>()
                .map_err(|_| TreasuryProtocolError::SchemaDrift)?;
            if source_line == 0 {
                return Err(TreasuryProtocolError::SchemaDrift);
            }
            records.push(FiscalDataRecord {
                values,
                schema: Arc::clone(&schema),
                row_identity,
                sort_key: (record_date, source_line),
                source_payload_digest,
            });
        }
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
            next_page_token,
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

    /// Returns the exact validated provider next-page token, if this page is nonterminal.
    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
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
    accepted_row_identities: HashSet<[u8; 32]>,
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
            accepted_row_identities: HashSet::new(),
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
            self.accepted_row_identities
                .try_reserve(1)
                .map_err(|_| TreasuryProtocolError::AllocationFailed)?;
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
    /// Fallible retained parser allocation was refused.
    #[error("Treasury parser allocation failed")]
    AllocationFailed,
    /// Decoded retained work exceeds the explicit parser ceiling.
    #[error("Treasury decoded work exceeds its byte budget")]
    DecodedWorkLimitExceeded,
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

struct PageWire {
    data: Vec<FiscalFields>,
    meta: MetaWire,
    links: LinksWire,
}

struct MetaWire {
    count: usize,
    labels: FiscalFields,
    data_types: FiscalFields,
    data_formats: FiscalFields,
    total_count: usize,
    total_pages: usize,
}

struct LinksWire {
    self_link: String,
    first: String,
    prev: Option<String>,
    next: Option<String>,
    last: String,
}

#[derive(Clone, Copy, Debug)]
enum FiscalDecodeFailure {
    InvalidMetadata,
    SchemaDrift,
    FieldTooLarge,
    AllocationFailed,
    DecodedWorkLimitExceeded,
}

impl FiscalDecodeFailure {
    const fn into_protocol(self) -> TreasuryProtocolError {
        match self {
            Self::InvalidMetadata => TreasuryProtocolError::InvalidMetadata,
            Self::SchemaDrift => TreasuryProtocolError::SchemaDrift,
            Self::FieldTooLarge => TreasuryProtocolError::FieldTooLarge,
            Self::AllocationFailed => TreasuryProtocolError::AllocationFailed,
            Self::DecodedWorkLimitExceeded => TreasuryProtocolError::DecodedWorkLimitExceeded,
        }
    }
}

impl fmt::Display for FiscalDecodeFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidMetadata => "Fiscal page metadata exceeded its bound",
            Self::SchemaDrift => "Fiscal page repeated a field",
            Self::FieldTooLarge => "Fiscal page field exceeded its byte bound",
            Self::AllocationFailed => "Fiscal page retained allocation failed",
            Self::DecodedWorkLimitExceeded => "Fiscal page decoded work exceeded its byte bound",
        })
    }
}

impl From<FiscalDecodeFailure> for TreasuryProtocolError {
    fn from(failure: FiscalDecodeFailure) -> Self {
        failure.into_protocol()
    }
}

struct FiscalDecodeBudget {
    limits: FiscalDataParseLimits,
    retained_bytes: usize,
    max_retained_bytes: usize,
    failure: Option<FiscalDecodeFailure>,
}

impl FiscalDecodeBudget {
    fn new(limits: FiscalDataParseLimits) -> Self {
        let max_retained_bytes = limits
            .max_bytes
            .checked_mul(4)
            .unwrap_or(usize::MAX)
            .min(MAX_FISCAL_DECODED_WORK_BYTES);
        Self {
            limits,
            retained_bytes: 0,
            max_retained_bytes,
            failure: None,
        }
    }

    fn record_failure(&mut self, failure: FiscalDecodeFailure) -> FiscalDecodeFailure {
        self.failure.get_or_insert(failure);
        failure
    }

    fn charge(&mut self, bytes: usize) -> Result<(), FiscalDecodeFailure> {
        let Some(retained_bytes) = self
            .retained_bytes
            .checked_add(bytes)
            .filter(|retained| *retained <= self.max_retained_bytes)
        else {
            return Err(self.record_failure(FiscalDecodeFailure::DecodedWorkLimitExceeded));
        };
        self.retained_bytes = retained_bytes;
        Ok(())
    }

    fn reserve_one<T>(
        &mut self,
        values: &mut Vec<T>,
        max_capacity: usize,
    ) -> Result<(), FiscalDecodeFailure> {
        let next_len = values
            .len()
            .checked_add(1)
            .filter(|next_len| *next_len <= max_capacity)
            .ok_or_else(|| self.record_failure(FiscalDecodeFailure::InvalidMetadata))?;
        if next_len <= values.capacity() {
            return Ok(());
        }

        let target_capacity = if values.capacity() == 0 {
            MIN_FISCAL_VECTOR_CAPACITY.min(max_capacity)
        } else {
            values
                .capacity()
                .checked_mul(2)
                .unwrap_or(max_capacity)
                .min(max_capacity)
        }
        .max(next_len);
        self.reserve_exact_capacity(values, target_capacity, max_capacity)
    }

    fn reserve_exact_capacity<T>(
        &mut self,
        values: &mut Vec<T>,
        target_capacity: usize,
        max_capacity: usize,
    ) -> Result<(), FiscalDecodeFailure> {
        if target_capacity <= values.capacity() {
            return Ok(());
        }
        if target_capacity > max_capacity || size_of::<T>() == 0 {
            return Err(self.record_failure(FiscalDecodeFailure::DecodedWorkLimitExceeded));
        }

        let previous_capacity = values.capacity();
        let requested_growth = target_capacity
            .checked_sub(previous_capacity)
            .and_then(|growth| growth.checked_mul(size_of::<T>()))
            .ok_or_else(|| self.record_failure(FiscalDecodeFailure::DecodedWorkLimitExceeded))?;
        self.ensure_chargeable(requested_growth)?;
        let additional = target_capacity
            .checked_sub(values.len())
            .ok_or_else(|| self.record_failure(FiscalDecodeFailure::DecodedWorkLimitExceeded))?;
        values
            .try_reserve_exact(additional)
            .map_err(|_| self.record_failure(FiscalDecodeFailure::AllocationFailed))?;

        let admitted_capacity = values.capacity();
        if admitted_capacity > max_capacity {
            return Err(self.record_failure(FiscalDecodeFailure::DecodedWorkLimitExceeded));
        }
        let admitted_growth = admitted_capacity
            .checked_sub(previous_capacity)
            .and_then(|growth| growth.checked_mul(size_of::<T>()))
            .ok_or_else(|| self.record_failure(FiscalDecodeFailure::DecodedWorkLimitExceeded))?;
        // Charge allocator-reported capacity before the caller can retain another element.
        self.charge(admitted_growth)
    }

    fn ensure_chargeable(&mut self, bytes: usize) -> Result<(), FiscalDecodeFailure> {
        if self
            .retained_bytes
            .checked_add(bytes)
            .is_none_or(|retained| retained > self.max_retained_bytes)
        {
            return Err(self.record_failure(FiscalDecodeFailure::DecodedWorkLimitExceeded));
        }
        Ok(())
    }

    fn owned_string(
        &mut self,
        value: &str,
        max_bytes: usize,
    ) -> Result<String, FiscalDecodeFailure> {
        if value.len() > max_bytes {
            return Err(self.record_failure(FiscalDecodeFailure::FieldTooLarge));
        }
        self.charge(value.len())?;
        let mut owned = String::new();
        owned
            .try_reserve_exact(value.len())
            .map_err(|_| self.record_failure(FiscalDecodeFailure::AllocationFailed))?;
        owned.push_str(value);
        Ok(owned)
    }

    fn retain_string(
        &mut self,
        value: String,
        max_bytes: usize,
    ) -> Result<String, FiscalDecodeFailure> {
        if value.len() > max_bytes {
            return Err(self.record_failure(FiscalDecodeFailure::FieldTooLarge));
        }
        self.charge(value.capacity())?;
        Ok(value)
    }

    fn reject(&mut self, failure: FiscalDecodeFailure) -> FiscalDecodeFailure {
        self.record_failure(failure)
    }

    fn protocol_error(&self, error: serde_json::Error) -> TreasuryProtocolError {
        self.failure.map_or_else(
            || TreasuryProtocolError::InvalidJson(error.to_string()),
            FiscalDecodeFailure::into_protocol,
        )
    }
}

struct BoundedStringSeed<'a> {
    budget: &'a mut FiscalDecodeBudget,
    max_bytes: usize,
}

impl<'de> DeserializeSeed<'de> for BoundedStringSeed<'_> {
    type Value = String;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BoundedStringVisitor {
            budget: self.budget,
            max_bytes: self.max_bytes,
        })
    }
}

struct BoundedStringVisitor<'a> {
    budget: &'a mut FiscalDecodeBudget,
    max_bytes: usize,
}

impl Visitor<'_> for BoundedStringVisitor<'_> {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded Fiscal Data string")
    }

    fn visit_borrowed_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.budget
            .owned_string(value, self.max_bytes)
            .map_err(E::custom)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.budget
            .owned_string(value, self.max_bytes)
            .map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.budget
            .retain_string(value, self.max_bytes)
            .map_err(E::custom)
    }
}

struct OptionalBoundedStringSeed<'a> {
    budget: &'a mut FiscalDecodeBudget,
    max_bytes: usize,
}

impl<'de> DeserializeSeed<'de> for OptionalBoundedStringSeed<'_> {
    type Value = Option<String>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_option(OptionalBoundedStringVisitor {
            budget: self.budget,
            max_bytes: self.max_bytes,
        })
    }
}

struct OptionalBoundedStringVisitor<'a> {
    budget: &'a mut FiscalDecodeBudget,
    max_bytes: usize,
}

impl<'de> Visitor<'de> for OptionalBoundedStringVisitor<'_> {
    type Value = Option<String>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a null or bounded Fiscal Data string")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        BoundedStringSeed {
            budget: self.budget,
            max_bytes: self.max_bytes,
        }
        .deserialize(deserializer)
        .map(Some)
    }
}

struct RejectSeed<'a> {
    budget: &'a mut FiscalDecodeBudget,
    failure: FiscalDecodeFailure,
}

impl<'de> DeserializeSeed<'de> for RejectSeed<'_> {
    type Value = Infallible;

    fn deserialize<D>(self, _deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(de::Error::custom(self.budget.reject(self.failure)))
    }
}

struct FiscalFieldsSeed<'a> {
    budget: &'a mut FiscalDecodeBudget,
    max_fields: usize,
}

impl<'de> DeserializeSeed<'de> for FiscalFieldsSeed<'_> {
    type Value = FiscalFields;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(FiscalFieldsVisitor {
            budget: self.budget,
            max_fields: self.max_fields,
        })
    }
}

struct FiscalFieldsVisitor<'a> {
    budget: &'a mut FiscalDecodeBudget,
    max_fields: usize,
}

impl<'de> Visitor<'de> for FiscalFieldsVisitor<'_> {
    type Value = FiscalFields;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded Fiscal Data field map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut fields = Vec::new();
        loop {
            if fields.len() == self.max_fields {
                match map.next_key_seed(RejectSeed {
                    budget: self.budget,
                    failure: FiscalDecodeFailure::InvalidMetadata,
                })? {
                    None => break,
                    Some(value) => match value {},
                }
            }
            let Some(key) = map.next_key_seed(BoundedStringSeed {
                budget: self.budget,
                max_bytes: MAX_FISCAL_FIELD_KEY_BYTES,
            })?
            else {
                break;
            };
            let value = map.next_value_seed(BoundedStringSeed {
                budget: self.budget,
                max_bytes: MAX_FISCAL_FIELD_VALUE_BYTES,
            })?;
            self.budget
                .reserve_one(&mut fields, self.max_fields)
                .map_err(de::Error::custom)?;
            fields.push((key, value));
        }
        fields.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        if fields
            .windows(2)
            .any(|pair| pair[0].0.as_str() == pair[1].0.as_str())
        {
            return Err(de::Error::custom(
                self.budget.reject(FiscalDecodeFailure::SchemaDrift),
            ));
        }
        Ok(fields)
    }
}

struct FiscalRowsSeed<'a> {
    budget: &'a mut FiscalDecodeBudget,
    max_records: usize,
    max_fields: usize,
}

impl<'de> DeserializeSeed<'de> for FiscalRowsSeed<'_> {
    type Value = Vec<FiscalFields>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(FiscalRowsVisitor {
            budget: self.budget,
            max_records: self.max_records,
            max_fields: self.max_fields,
        })
    }
}

struct FiscalRowsVisitor<'a> {
    budget: &'a mut FiscalDecodeBudget,
    max_records: usize,
    max_fields: usize,
}

impl<'de> Visitor<'de> for FiscalRowsVisitor<'_> {
    type Value = Vec<FiscalFields>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded Fiscal Data row sequence")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut records = Vec::new();
        loop {
            if records.len() == self.max_records {
                match sequence.next_element_seed(RejectSeed {
                    budget: self.budget,
                    failure: FiscalDecodeFailure::InvalidMetadata,
                })? {
                    None => break,
                    Some(value) => match value {},
                }
            }
            let Some(record) = sequence.next_element_seed(FiscalFieldsSeed {
                budget: self.budget,
                max_fields: self.max_fields,
            })?
            else {
                break;
            };
            self.budget
                .reserve_one(&mut records, self.max_records)
                .map_err(de::Error::custom)?;
            records.push(record);
        }
        Ok(records)
    }
}

struct PageWireSeed<'a> {
    budget: &'a mut FiscalDecodeBudget,
}

impl<'de> DeserializeSeed<'de> for PageWireSeed<'_> {
    type Value = PageWire;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.budget
            .charge(size_of::<PageWire>())
            .map_err(de::Error::custom)?;
        deserializer.deserialize_map(PageWireVisitor {
            budget: self.budget,
        })
    }
}

struct PageWireVisitor<'a> {
    budget: &'a mut FiscalDecodeBudget,
}

impl<'de> Visitor<'de> for PageWireVisitor<'_> {
    type Value = PageWire;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded Fiscal Data page")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let limits = self.budget.limits;
        let mut data = None;
        let mut meta = None;
        let mut links = None;
        while let Some(field) = map.next_key_seed(BoundedStringSeed {
            budget: self.budget,
            max_bytes: MAX_FISCAL_FIELD_KEY_BYTES,
        })? {
            match field.as_str() {
                "data" => {
                    if data.is_some() {
                        return Err(de::Error::duplicate_field("data"));
                    }
                    data = Some(map.next_value_seed(FiscalRowsSeed {
                        budget: self.budget,
                        max_records: limits.max_records,
                        max_fields: limits.max_fields,
                    })?);
                }
                "meta" => {
                    if meta.is_some() {
                        return Err(de::Error::duplicate_field("meta"));
                    }
                    meta = Some(map.next_value_seed(MetaWireSeed {
                        budget: self.budget,
                        max_fields: limits.max_fields,
                    })?);
                }
                "links" => {
                    if links.is_some() {
                        return Err(de::Error::duplicate_field("links"));
                    }
                    links = Some(map.next_value_seed(LinksWireSeed {
                        budget: self.budget,
                    })?);
                }
                _ => return Err(de::Error::unknown_field(&field, &["data", "meta", "links"])),
            }
        }
        Ok(PageWire {
            data: data.ok_or_else(|| de::Error::missing_field("data"))?,
            meta: meta.ok_or_else(|| de::Error::missing_field("meta"))?,
            links: links.ok_or_else(|| de::Error::missing_field("links"))?,
        })
    }
}

struct MetaWireSeed<'a> {
    budget: &'a mut FiscalDecodeBudget,
    max_fields: usize,
}

impl<'de> DeserializeSeed<'de> for MetaWireSeed<'_> {
    type Value = MetaWire;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(MetaWireVisitor {
            budget: self.budget,
            max_fields: self.max_fields,
        })
    }
}

struct MetaWireVisitor<'a> {
    budget: &'a mut FiscalDecodeBudget,
    max_fields: usize,
}

impl<'de> Visitor<'de> for MetaWireVisitor<'_> {
    type Value = MetaWire;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded Fiscal Data metadata")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut count = None;
        let mut labels = None;
        let mut data_types = None;
        let mut data_formats = None;
        let mut total_count = None;
        let mut total_pages = None;
        while let Some(field) = map.next_key_seed(BoundedStringSeed {
            budget: self.budget,
            max_bytes: MAX_FISCAL_FIELD_KEY_BYTES,
        })? {
            match field.as_str() {
                "count" => set_once(&mut count, map.next_value()?, "count")?,
                "labels" => {
                    if labels.is_some() {
                        return Err(de::Error::duplicate_field("labels"));
                    }
                    labels = Some(map.next_value_seed(FiscalFieldsSeed {
                        budget: self.budget,
                        max_fields: self.max_fields,
                    })?);
                }
                "dataTypes" => {
                    if data_types.is_some() {
                        return Err(de::Error::duplicate_field("dataTypes"));
                    }
                    data_types = Some(map.next_value_seed(FiscalFieldsSeed {
                        budget: self.budget,
                        max_fields: self.max_fields,
                    })?);
                }
                "dataFormats" => {
                    if data_formats.is_some() {
                        return Err(de::Error::duplicate_field("dataFormats"));
                    }
                    data_formats = Some(map.next_value_seed(FiscalFieldsSeed {
                        budget: self.budget,
                        max_fields: self.max_fields,
                    })?);
                }
                "total-count" => set_once(&mut total_count, map.next_value()?, "total-count")?,
                "total-pages" => set_once(&mut total_pages, map.next_value()?, "total-pages")?,
                _ => {
                    return Err(de::Error::unknown_field(
                        &field,
                        &[
                            "count",
                            "labels",
                            "dataTypes",
                            "dataFormats",
                            "total-count",
                            "total-pages",
                        ],
                    ));
                }
            }
        }
        Ok(MetaWire {
            count: count.ok_or_else(|| de::Error::missing_field("count"))?,
            labels: labels.ok_or_else(|| de::Error::missing_field("labels"))?,
            data_types: data_types.ok_or_else(|| de::Error::missing_field("dataTypes"))?,
            data_formats: data_formats.ok_or_else(|| de::Error::missing_field("dataFormats"))?,
            total_count: total_count.ok_or_else(|| de::Error::missing_field("total-count"))?,
            total_pages: total_pages.ok_or_else(|| de::Error::missing_field("total-pages"))?,
        })
    }
}

struct LinksWireSeed<'a> {
    budget: &'a mut FiscalDecodeBudget,
}

impl<'de> DeserializeSeed<'de> for LinksWireSeed<'_> {
    type Value = LinksWire;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(LinksWireVisitor {
            budget: self.budget,
        })
    }
}

struct LinksWireVisitor<'a> {
    budget: &'a mut FiscalDecodeBudget,
}

impl<'de> Visitor<'de> for LinksWireVisitor<'_> {
    type Value = LinksWire;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded Fiscal Data pagination links")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut self_link = None;
        let mut first = None;
        let mut prev = None;
        let mut next = None;
        let mut last = None;
        while let Some(field) = map.next_key_seed(BoundedStringSeed {
            budget: self.budget,
            max_bytes: MAX_FISCAL_FIELD_KEY_BYTES,
        })? {
            match field.as_str() {
                "self" => bounded_link(&mut self_link, &mut map, self.budget, "self")?,
                "first" => bounded_link(&mut first, &mut map, self.budget, "first")?,
                "prev" => optional_bounded_link(&mut prev, &mut map, self.budget, "prev")?,
                "next" => optional_bounded_link(&mut next, &mut map, self.budget, "next")?,
                "last" => bounded_link(&mut last, &mut map, self.budget, "last")?,
                _ => {
                    return Err(de::Error::unknown_field(
                        &field,
                        &["self", "first", "prev", "next", "last"],
                    ));
                }
            }
        }
        Ok(LinksWire {
            self_link: self_link.ok_or_else(|| de::Error::missing_field("self"))?,
            first: first.ok_or_else(|| de::Error::missing_field("first"))?,
            prev: prev.unwrap_or(None),
            next: next.unwrap_or(None),
            last: last.ok_or_else(|| de::Error::missing_field("last"))?,
        })
    }
}

fn set_once<E>(slot: &mut Option<usize>, value: usize, field: &'static str) -> Result<(), E>
where
    E: de::Error,
{
    if slot.replace(value).is_some() {
        Err(de::Error::duplicate_field(field))
    } else {
        Ok(())
    }
}

fn bounded_link<'de, A>(
    slot: &mut Option<String>,
    map: &mut A,
    budget: &mut FiscalDecodeBudget,
    field: &'static str,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    if slot.is_some() {
        return Err(de::Error::duplicate_field(field));
    }
    *slot = Some(map.next_value_seed(BoundedStringSeed {
        budget,
        max_bytes: MAX_FISCAL_FIELD_VALUE_BYTES,
    })?);
    Ok(())
}

fn optional_bounded_link<'de, A>(
    slot: &mut Option<Option<String>>,
    map: &mut A,
    budget: &mut FiscalDecodeBudget,
    field: &'static str,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    if slot.is_some() {
        return Err(de::Error::duplicate_field(field));
    }
    *slot = Some(map.next_value_seed(OptionalBoundedStringSeed {
        budget,
        max_bytes: MAX_FISCAL_FIELD_VALUE_BYTES,
    })?);
    Ok(())
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
    values: &FiscalFields,
) -> Result<[u8; 32], TreasuryProtocolError> {
    let mut digest = Sha256::new();
    update_component(&mut digest, "treasury-row-identity-v1");
    for field in primary_key {
        let value = field_value(values, field).ok_or(TreasuryProtocolError::SchemaDrift)?;
        update_component(&mut digest, field);
        update_component(&mut digest, value);
    }
    Ok(digest.finalize().into())
}

fn schema_digest(labels: &FiscalFields, types: &FiscalFields, formats: &FiscalFields) -> [u8; 32] {
    let mut digest = Sha256::new();
    for (field, label) in labels {
        update_component(&mut digest, field);
        update_component(&mut digest, label);
        if let Some(data_type) = field_value(types, field) {
            update_component(&mut digest, data_type);
        }
        if let Some(format) = field_value(formats, field) {
            update_component(&mut digest, format);
        }
    }
    digest.finalize().into()
}

fn field_value<'a>(fields: &'a FiscalFields, field: &str) -> Option<&'a str> {
    fields
        .binary_search_by(|(candidate, _)| candidate.as_str().cmp(field))
        .ok()
        .and_then(|index| fields.get(index))
        .map(|(_, value)| value.as_str())
}

fn same_field_names(left: &FiscalFields, right: &FiscalFields) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((left, _), (right, _))| left == right)
}
