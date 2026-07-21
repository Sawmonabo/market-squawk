use std::str::FromStr;

use market_squawk_domain::CalendarDate;
use market_squawk_domain::DataQuality;
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use crate::fiscal_data::FiscalDataRecord;

/// Bound official endpoint and dataset evidence for average Treasury rates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreasuryRateProfile {
    endpoint: &'static str,
    source_url: &'static str,
    api_version: &'static str,
}

impl TreasuryRateProfile {
    /// Returns the official Fiscal Data v2 average-interest-rates profile.
    pub const fn average_interest_rates_v2() -> Self {
        Self {
            endpoint: "/v2/accounting/od/avg_interest_rates",
            source_url: "https://fiscaldata.treasury.gov/datasets/average-interest-rates-treasury-securities/",
            api_version: "v2",
        }
    }

    /// Returns the path relative to the official Fiscal Service API base.
    pub const fn endpoint(self) -> &'static str {
        self.endpoint
    }

    /// Returns the official dataset documentation URL.
    pub const fn source_url(self) -> &'static str {
        self.source_url
    }

    /// Returns the API version bound by the profile.
    pub const fn api_version(self) -> &'static str {
        self.api_version
    }

    /// Returns the official-publication quality of this accounting dataset.
    pub const fn quality(self) -> DataQuality {
        DataQuality::OfficialDelayed
    }
}

/// One exact published average-interest-rate row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AverageInterestRate {
    record_date: CalendarDate,
    security_type_description: String,
    security_description: String,
    rate_percent: Decimal,
    source_line_number: String,
    schema_digest: [u8; 32],
    source_payload_digest: [u8; 32],
}

impl AverageInterestRate {
    /// Converts a schema-bound Fiscal Data row without floating-point arithmetic.
    pub fn try_from_record(
        record: &FiscalDataRecord,
        _profile: &TreasuryRateProfile,
    ) -> Result<Self, TreasuryRateError> {
        require_schema(record, "record_date", "DATE", "YYYY-MM-DD")?;
        require_schema(record, "security_type_desc", "STRING", "String")?;
        require_schema(record, "security_desc", "STRING", "String")?;
        require_schema(record, "avg_interest_rate_amt", "PERCENTAGE", "10.2%")?;
        require_schema(record, "src_line_nbr", "INTEGER", "10")?;
        let rate_percent = Decimal::from_str_exact(required(record, "avg_interest_rate_amt")?)
            .map_err(|_| TreasuryRateError::InvalidRate)?;
        Ok(Self {
            record_date: parse_date(required(record, "record_date")?)?,
            security_type_description: required(record, "security_type_desc")?.to_owned(),
            security_description: required(record, "security_desc")?.to_owned(),
            rate_percent,
            source_line_number: required(record, "src_line_nbr")?.to_owned(),
            schema_digest: record.schema_digest(),
            source_payload_digest: record.source_payload_digest(),
        })
    }

    /// Returns the provider record date without an invented time or zone.
    pub const fn record_date(&self) -> CalendarDate {
        self.record_date
    }

    /// Returns the exact percentage amount.
    pub const fn rate_percent(&self) -> Decimal {
        self.rate_percent
    }

    /// Returns the security type description.
    pub fn security_type_description(&self) -> &str {
        &self.security_type_description
    }

    /// Returns the security description.
    pub fn security_description(&self) -> &str {
        &self.security_description
    }

    /// Returns the provider-supplied source line identity.
    pub fn source_line_number(&self) -> &str {
        &self.source_line_number
    }

    /// Returns the exact response schema digest.
    pub const fn schema_digest(&self) -> [u8; 32] {
        self.schema_digest
    }

    /// Returns the SHA-256 identity of the exact provider response containing this row.
    pub const fn source_payload_digest(&self) -> [u8; 32] {
        self.source_payload_digest
    }
}

/// A rate-specific semantic conversion failure.
#[derive(Debug, Error)]
pub enum TreasuryRateError {
    /// A required field is missing.
    #[error("missing Treasury rate field {0}")]
    MissingField(&'static str),
    /// Required field type or format metadata drifted.
    #[error("Treasury rate schema drifted for field {0}")]
    SchemaDrift(&'static str),
    /// The published percentage is not an exact decimal.
    #[error("invalid Treasury average interest rate")]
    InvalidRate,
    /// The record date is not a valid ISO civil date.
    #[error("invalid Treasury record date")]
    InvalidDate,
}

fn required<'a>(
    record: &'a FiscalDataRecord,
    field: &'static str,
) -> Result<&'a str, TreasuryRateError> {
    record
        .get(field)
        .ok_or(TreasuryRateError::MissingField(field))
}

fn require_schema(
    record: &FiscalDataRecord,
    field: &'static str,
    data_type: &str,
    format: &str,
) -> Result<(), TreasuryRateError> {
    if record.schema().data_type(field) != Some(data_type)
        || record.schema().data_format(field) != Some(format)
    {
        return Err(TreasuryRateError::SchemaDrift(field));
    }
    Ok(())
}

pub(crate) fn parse_date(value: &str) -> Result<CalendarDate, TreasuryRateError> {
    let bytes = value.as_bytes();
    if bytes.len() != 10
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || !bytes[0..4].iter().all(u8::is_ascii_digit)
        || !bytes[5..7].iter().all(u8::is_ascii_digit)
        || !bytes[8..10].iter().all(u8::is_ascii_digit)
    {
        return Err(TreasuryRateError::InvalidDate);
    }
    let year = u16::from_str(&value[0..4]).map_err(|_| TreasuryRateError::InvalidDate)?;
    let month = u8::from_str(&value[5..7]).map_err(|_| TreasuryRateError::InvalidDate)?;
    let day = u8::from_str(&value[8..10]).map_err(|_| TreasuryRateError::InvalidDate)?;
    CalendarDate::new(year, month, day).map_err(|_| TreasuryRateError::InvalidDate)
}
