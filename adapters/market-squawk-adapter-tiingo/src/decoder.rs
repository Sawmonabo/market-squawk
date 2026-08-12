use std::collections::BTreeSet;

use chrono::{DateTime, Datelike as _, Timelike as _};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};

use crate::error::SchemaResult;
use crate::model::MAX_TICKER_BYTES;
use crate::{
    TiingoAdapterError, TiingoCoverage, TiingoEndpointFamily, TiingoEodReceipt, TiingoEodRow,
    TiingoMetadataReceipt, TiingoPaginationEvidence, TiingoProviderFailure,
    TiingoRequestDisposition, TiingoRequestScope, TiingoRequestSpec, TiingoResponseEvidence,
    TiingoSchemaChange, TiingoSchemaChangeReason, TiingoTicker,
};

const MAX_NAME_BYTES: usize = 512;
const MAX_DESCRIPTION_BYTES: usize = 32 * 1024;
const MAX_EXCHANGE_CODE_BYTES: usize = 64;

/// Durable schema-circuit state for one exact reviewed native contract revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TiingoSchemaCircuitState {
    /// The reviewed contract may decode another response.
    Closed,
    /// A body violated the reviewed contract and all subsequent decoding is denied.
    Open(TiingoSchemaChange),
}

/// Fail-closed Tiingo provider-native schema circuit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoSchemaCircuit {
    contract_revision: SourceIdentifier,
    state: TiingoSchemaCircuitState,
    last_reset_evidence: Option<SourceIdentifier>,
}

impl TiingoSchemaCircuit {
    /// Starts a circuit under one exact reviewed provider-native contract revision.
    pub const fn new(contract_revision: SourceIdentifier) -> Self {
        Self {
            contract_revision,
            state: TiingoSchemaCircuitState::Closed,
            last_reset_evidence: None,
        }
    }

    /// Returns the reviewed native contract revision.
    pub const fn contract_revision(&self) -> &SourceIdentifier {
        &self.contract_revision
    }

    /// Returns current closed/open state and exact opening evidence.
    pub const fn state(&self) -> &TiingoSchemaCircuitState {
        &self.state
    }

    /// Returns evidence for the most recent explicit reviewed reset.
    pub const fn last_reset_evidence(&self) -> Option<&SourceIdentifier> {
        self.last_reset_evidence.as_ref()
    }

    /// Resets an open circuit only while advancing to a different reviewed contract revision.
    ///
    /// # Errors
    ///
    /// Rejects a reset of a closed circuit or reuse of the failed revision.
    pub fn reset_after_review(
        &mut self,
        next_contract_revision: SourceIdentifier,
        review_evidence: SourceIdentifier,
    ) -> Result<(), TiingoAdapterError> {
        if matches!(self.state, TiingoSchemaCircuitState::Closed)
            || self.contract_revision == next_contract_revision
        {
            return Err(TiingoAdapterError::SchemaCircuitOpen);
        }
        self.contract_revision = next_contract_revision;
        self.state = TiingoSchemaCircuitState::Closed;
        self.last_reset_evidence = Some(review_evidence);
        Ok(())
    }

    fn ensure_closed(&self) -> Result<(), TiingoAdapterError> {
        if matches!(self.state, TiingoSchemaCircuitState::Closed) {
            Ok(())
        } else {
            Err(TiingoAdapterError::SchemaCircuitOpen)
        }
    }

    fn trip(&mut self, change: TiingoSchemaChange) {
        if matches!(self.state, TiingoSchemaCircuitState::Closed) {
            self.state = TiingoSchemaCircuitState::Open(change);
        }
    }
}

/// Strict bounded decoder for the reviewed metadata and daily-price native contracts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoDecoder {
    circuit: TiingoSchemaCircuit,
}

impl TiingoDecoder {
    /// Constructs a decoder bound to one reviewed provider-native contract revision.
    pub const fn new(contract_revision: SourceIdentifier) -> Self {
        Self {
            circuit: TiingoSchemaCircuit::new(contract_revision),
        }
    }

    /// Returns the durable fail-closed schema circuit.
    pub const fn circuit(&self) -> &TiingoSchemaCircuit {
        &self.circuit
    }

    /// Returns mutable circuit access for an explicit reviewed reset or persistence handoff.
    pub const fn circuit_mut(&mut self) -> &mut TiingoSchemaCircuit {
        &mut self.circuit
    }

    /// Decodes one exact metadata response and validates the returned ticker.
    pub fn decode_metadata(
        &mut self,
        request: TiingoRequestSpec,
        status: u16,
        body: &[u8],
        received_at: Timestamp,
        decoded_at: Timestamp,
    ) -> Result<TiingoMetadataReceipt, TiingoAdapterError> {
        self.circuit.ensure_closed()?;
        if request.endpoint() != TiingoEndpointFamily::Metadata {
            return Err(TiingoAdapterError::RequestBuild);
        }
        let body_digest = validate_response(&request, status, body, received_at, decoded_at)?;
        let parsed = parse_metadata(body, request.ticker());
        let metadata = match parsed {
            Ok(metadata) => metadata,
            Err(reason) => {
                return Err(self.schema_error(&request, reason, body_digest, decoded_at));
            }
        };
        let response_bytes =
            u64::try_from(body.len()).map_err(|_| TiingoAdapterError::BodyTooLarge)?;
        Ok(TiingoMetadataReceipt {
            metadata,
            evidence: TiingoResponseEvidence::new(
                request,
                status,
                body_digest,
                response_bytes,
                received_at,
                decoded_at,
            ),
            disposition: TiingoRequestDisposition::one_symbol(1, body.len()),
        })
    }

    /// Decodes one exact latest or application-date-window EOD/NAV response.
    pub fn decode_eod(
        &mut self,
        request: TiingoRequestSpec,
        status: u16,
        body: &[u8],
        received_at: Timestamp,
        decoded_at: Timestamp,
    ) -> Result<TiingoEodReceipt, TiingoAdapterError> {
        self.circuit.ensure_closed()?;
        if request.endpoint() == TiingoEndpointFamily::Metadata {
            return Err(TiingoAdapterError::RequestBuild);
        }
        let body_digest = validate_response(&request, status, body, received_at, decoded_at)?;
        let rows = match parse_eod(body, &request) {
            Ok(rows) => rows,
            Err(reason) => {
                return Err(self.schema_error(&request, reason, body_digest, decoded_at));
            }
        };
        let pagination = match request.scope() {
            TiingoRequestScope::History { page, .. } => {
                TiingoPaginationEvidence::ApplicationDateWindow(*page)
            }
            TiingoRequestScope::Latest => TiingoPaginationEvidence::NotApplicable,
            TiingoRequestScope::Metadata => return Err(TiingoAdapterError::RequestBuild),
        };
        let response_bytes =
            u64::try_from(body.len()).map_err(|_| TiingoAdapterError::BodyTooLarge)?;
        let disposition = TiingoRequestDisposition::one_symbol(rows.len(), body.len());
        Ok(TiingoEodReceipt {
            rows: rows.into_boxed_slice(),
            evidence: TiingoResponseEvidence::new(
                request,
                status,
                body_digest,
                response_bytes,
                received_at,
                decoded_at,
            ),
            disposition,
            pagination,
        })
    }

    fn schema_error(
        &mut self,
        request: &TiingoRequestSpec,
        reason: TiingoSchemaChangeReason,
        body_digest: EvidenceDigest,
        observed_at: Timestamp,
    ) -> TiingoAdapterError {
        let change = TiingoSchemaChange::new(request.endpoint(), reason, body_digest, observed_at);
        self.circuit.trip(change.clone());
        TiingoAdapterError::SchemaChanged(change)
    }
}

fn validate_response(
    request: &TiingoRequestSpec,
    status: u16,
    body: &[u8],
    received_at: Timestamp,
    decoded_at: Timestamp,
) -> Result<EvidenceDigest, TiingoAdapterError> {
    if body.len() > request.max_response_bytes() {
        return Err(TiingoAdapterError::BodyTooLarge);
    }
    if received_at > decoded_at {
        return Err(TiingoAdapterError::InvalidChronology);
    }
    if !(200..300).contains(&status) {
        return Err(TiingoAdapterError::Provider(TiingoProviderFailure::new(
            status, body,
        )));
    }
    Ok(digest(body))
}

fn parse_metadata(
    body: &[u8],
    expected_ticker: &TiingoTicker,
) -> SchemaResult<crate::model::TiingoMetadata> {
    let value: Value =
        serde_json::from_slice(body).map_err(|_| TiingoSchemaChangeReason::InvalidTopLevel)?;
    let object = value
        .as_object()
        .ok_or(TiingoSchemaChangeReason::InvalidTopLevel)?;
    ensure_exact_fields(
        object,
        &[
            "ticker",
            "name",
            "exchangeCode",
            "description",
            "startDate",
            "endDate",
        ],
    )?;

    let ticker = TiingoTicker::try_new(required_string(object, "ticker", MAX_TICKER_BYTES)?)
        .map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?;
    if &ticker != expected_ticker {
        return Err(TiingoSchemaChangeReason::SymbolMismatch);
    }
    let name = required_nonempty_string(object, "name", MAX_NAME_BYTES)?.into_boxed_str();
    let exchange_code =
        required_nonempty_string(object, "exchangeCode", MAX_EXCHANGE_CODE_BYTES)?.into_boxed_str();
    let description =
        optional_string(object, "description", MAX_DESCRIPTION_BYTES)?.map(String::into_boxed_str);
    let start = optional_date(object, "startDate")?;
    let end = optional_date(object, "endDate")?;
    let coverage = match (start, end) {
        (Some(start_date), Some(end_date)) if start_date <= end_date => TiingoCoverage::Supported {
            start_date,
            end_date,
        },
        (None, None) => TiingoCoverage::Unsupported,
        _ => return Err(TiingoSchemaChangeReason::InvalidFieldValue),
    };

    Ok(crate::model::TiingoMetadata {
        ticker,
        name,
        exchange_code,
        description,
        coverage,
    })
}

fn parse_eod(body: &[u8], request: &TiingoRequestSpec) -> SchemaResult<Vec<TiingoEodRow>> {
    let value: Value =
        serde_json::from_slice(body).map_err(|_| TiingoSchemaChangeReason::InvalidTopLevel)?;
    let values = value
        .as_array()
        .ok_or(TiingoSchemaChangeReason::InvalidTopLevel)?;
    if values.len() > request.max_rows() {
        return Err(TiingoSchemaChangeReason::RowLimitExceeded);
    }

    let mut rows = Vec::with_capacity(values.len());
    let mut previous_date = None;
    for value in values {
        let object = value
            .as_object()
            .ok_or(TiingoSchemaChangeReason::InvalidFieldType)?;
        ensure_exact_fields(
            object,
            &[
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "adjOpen",
                "adjHigh",
                "adjLow",
                "adjClose",
                "adjVolume",
                "divCash",
                "splitFactor",
            ],
        )?;
        let provider_date = required_nonempty_string(object, "date", 64)?;
        let date = parse_provider_daily_date(&provider_date)?;
        if previous_date.is_some_and(|previous| previous >= date) {
            return Err(TiingoSchemaChangeReason::InvalidRowSequence);
        }
        if let TiingoRequestScope::History {
            start_date,
            end_date,
            ..
        } = request.scope()
            && (date < *start_date || date > *end_date)
        {
            return Err(TiingoSchemaChangeReason::InvalidRowSequence);
        }

        let open = optional_decimal(object, "open")?;
        let high = optional_decimal(object, "high")?;
        let low = optional_decimal(object, "low")?;
        let close = optional_decimal(object, "close")?;
        let volume = optional_decimal(object, "volume")?;
        let adjusted_open = optional_decimal(object, "adjOpen")?;
        let adjusted_high = optional_decimal(object, "adjHigh")?;
        let adjusted_low = optional_decimal(object, "adjLow")?;
        let adjusted_close = optional_decimal(object, "adjClose")?;
        let adjusted_volume = optional_decimal(object, "adjVolume")?;
        let cash_dividend = optional_decimal(object, "divCash")?;
        let split_factor = optional_decimal(object, "splitFactor")?;

        for value in [
            open,
            high,
            low,
            close,
            volume,
            adjusted_open,
            adjusted_high,
            adjusted_low,
            adjusted_close,
            adjusted_volume,
        ]
        .into_iter()
        .flatten()
        {
            if value < Decimal::ZERO {
                return Err(TiingoSchemaChangeReason::InvalidFieldValue);
            }
        }
        if split_factor.is_some_and(|value| value <= Decimal::ZERO) {
            return Err(TiingoSchemaChangeReason::InvalidFieldValue);
        }

        let row_digest = digest_row(
            &provider_date,
            [
                open,
                high,
                low,
                close,
                volume,
                adjusted_open,
                adjusted_high,
                adjusted_low,
                adjusted_close,
                adjusted_volume,
                cash_dividend,
                split_factor,
            ],
        );
        rows.push(TiingoEodRow {
            provider_date: provider_date.into_boxed_str(),
            date,
            open,
            high,
            low,
            close,
            volume,
            adjusted_open,
            adjusted_high,
            adjusted_low,
            adjusted_close,
            adjusted_volume,
            cash_dividend,
            split_factor,
            row_digest,
        });
        previous_date = Some(date);
    }
    Ok(rows)
}

fn ensure_exact_fields(object: &Map<String, Value>, expected: &[&str]) -> SchemaResult<()> {
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    if object.keys().any(|key| !expected.contains(key.as_str())) {
        return Err(TiingoSchemaChangeReason::UnknownField);
    }
    if expected.iter().any(|field| !object.contains_key(*field)) {
        return Err(TiingoSchemaChangeReason::MissingField);
    }
    Ok(())
}

fn required_string(
    object: &Map<String, Value>,
    field: &str,
    max_bytes: usize,
) -> SchemaResult<String> {
    let value = object
        .get(field)
        .ok_or(TiingoSchemaChangeReason::MissingField)?
        .as_str()
        .ok_or(TiingoSchemaChangeReason::InvalidFieldType)?;
    if value.len() > max_bytes {
        return Err(TiingoSchemaChangeReason::InvalidFieldValue);
    }
    Ok(value.to_owned())
}

fn required_nonempty_string(
    object: &Map<String, Value>,
    field: &str,
    max_bytes: usize,
) -> SchemaResult<String> {
    let value = required_string(object, field, max_bytes)?;
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(TiingoSchemaChangeReason::InvalidFieldValue);
    }
    Ok(value)
}

fn optional_string(
    object: &Map<String, Value>,
    field: &str,
    max_bytes: usize,
) -> SchemaResult<Option<String>> {
    let value = object
        .get(field)
        .ok_or(TiingoSchemaChangeReason::MissingField)?;
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_str()
        .ok_or(TiingoSchemaChangeReason::InvalidFieldType)?;
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(TiingoSchemaChangeReason::InvalidFieldValue);
    }
    Ok(Some(value.to_owned()))
}

fn optional_date(object: &Map<String, Value>, field: &str) -> SchemaResult<Option<CalendarDate>> {
    let value = object
        .get(field)
        .ok_or(TiingoSchemaChangeReason::MissingField)?;
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_str()
        .ok_or(TiingoSchemaChangeReason::InvalidFieldType)?;
    parse_calendar_date(value).map(Some)
}

fn optional_decimal(object: &Map<String, Value>, field: &str) -> SchemaResult<Option<Decimal>> {
    let value = object
        .get(field)
        .ok_or(TiingoSchemaChangeReason::MissingField)?;
    if value.is_null() {
        return Ok(None);
    }
    let number = value
        .as_number()
        .ok_or(TiingoSchemaChangeReason::InvalidFieldType)?;
    Decimal::from_str_exact(&number.to_string())
        .map(Some)
        .map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)
}

fn parse_provider_daily_date(value: &str) -> SchemaResult<CalendarDate> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?;
    if timestamp.offset().local_minus_utc() != 0
        || timestamp.hour() != 0
        || timestamp.minute() != 0
        || timestamp.second() != 0
        || timestamp.nanosecond() != 0
    {
        return Err(TiingoSchemaChangeReason::InvalidFieldValue);
    }
    let year =
        u16::try_from(timestamp.year()).map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?;
    let month =
        u8::try_from(timestamp.month()).map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?;
    let day =
        u8::try_from(timestamp.day()).map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?;
    CalendarDate::new(year, month, day).map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)
}

fn parse_calendar_date(value: &str) -> SchemaResult<CalendarDate> {
    let parsed = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?;
    let year =
        u16::try_from(parsed.year()).map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?;
    let month =
        u8::try_from(parsed.month()).map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?;
    let day =
        u8::try_from(parsed.day()).map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?;
    CalendarDate::new(year, month, day).map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)
}

fn digest(value: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

fn digest_row(provider_date: &str, values: [Option<Decimal>; 12]) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    append_field(&mut hasher, b"tiingo.daily-row.v1");
    append_field(&mut hasher, provider_date.as_bytes());
    for value in values {
        match value {
            Some(value) => {
                hasher.update([1]);
                append_field(&mut hasher, value.to_string().as_bytes());
            }
            None => hasher.update([0]),
        }
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

pub(crate) fn append_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}
