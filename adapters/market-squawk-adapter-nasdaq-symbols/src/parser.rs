use std::collections::BTreeSet;

use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::model::{
    NasdaqDirectoryKind, NasdaqFileCreationTime, NasdaqFinancialStatus, NasdaqMarketCategory,
    NasdaqModelError, NasdaqOtherExchange, NasdaqProviderFields,
};

/// Maximum exact source-file bytes retained or parsed by this adapter.
pub const MAX_SOURCE_BYTES: usize = 8 * 1024 * 1024;
/// Maximum data rows accepted from either exact directory object.
pub const MAX_DIRECTORY_RECORDS: usize = 32_768;
const MAX_LINE_BYTES: usize = 512;
const NASDAQ_LISTED_HEADER: &str = "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares";
const OTHER_LISTED_HEADER: &str =
    "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol";
const FILE_CREATION_PREFIX: &str = "File Creation Time: ";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedDirectory {
    pub(crate) kind: NasdaqDirectoryKind,
    pub(crate) file_creation_time: NasdaqFileCreationTime,
    pub(crate) rows: Vec<ParsedRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedRow {
    pub(crate) row_number: u32,
    pub(crate) fields: NasdaqProviderFields,
}

pub(crate) fn parse_directory(
    kind: NasdaqDirectoryKind,
    payload: &[u8],
    cancellation: &CancellationToken,
) -> Result<ParsedDirectory, NasdaqParseError> {
    if cancellation.is_cancelled() {
        return Err(NasdaqParseError::Cancelled);
    }
    if payload.is_empty() {
        return Err(NasdaqParseError::Empty);
    }
    if payload.len() > MAX_SOURCE_BYTES {
        return Err(NasdaqParseError::BodyTooLarge {
            max: MAX_SOURCE_BYTES,
        });
    }
    let text = std::str::from_utf8(payload).map_err(|_| NasdaqParseError::InvalidUtf8)?;
    let mut lines = text.lines().enumerate();
    let (_, header) = lines.next().ok_or(NasdaqParseError::Empty)?;
    let expected_header = match kind {
        NasdaqDirectoryKind::NasdaqListed => NASDAQ_LISTED_HEADER,
        NasdaqDirectoryKind::OtherListed => OTHER_LISTED_HEADER,
    };
    if header != expected_header {
        return Err(NasdaqParseError::InvalidHeader);
    }

    let mut rows = Vec::new();
    rows.try_reserve(1_024)
        .map_err(|_| NasdaqParseError::Capacity)?;
    let mut seen_symbols = BTreeSet::new();
    let mut file_creation_time = None;

    for (zero_based, line) in lines {
        if cancellation.is_cancelled() {
            return Err(NasdaqParseError::Cancelled);
        }
        let row_number = u32::try_from(zero_based.saturating_add(1)).map_err(|_| {
            NasdaqParseError::TooManyRecords {
                max: MAX_DIRECTORY_RECORDS,
            }
        })?;
        if line.is_empty() || line.len() > MAX_LINE_BYTES {
            return Err(NasdaqParseError::InvalidLine { row: row_number });
        }
        if let Some(value) = line.strip_prefix(FILE_CREATION_PREFIX) {
            if file_creation_time.is_some() {
                return Err(NasdaqParseError::DuplicateFooter);
            }
            file_creation_time = Some(parse_footer(value, row_number)?);
            continue;
        }
        if file_creation_time.is_some() {
            return Err(NasdaqParseError::DataAfterFooter { row: row_number });
        }
        if rows.len() >= MAX_DIRECTORY_RECORDS {
            return Err(NasdaqParseError::TooManyRecords {
                max: MAX_DIRECTORY_RECORDS,
            });
        }
        let fields = match kind {
            NasdaqDirectoryKind::NasdaqListed => parse_nasdaq_listed(line, row_number)?,
            NasdaqDirectoryKind::OtherListed => parse_other_listed(line, row_number)?,
        };
        if !seen_symbols.insert(fields.primary_symbol().to_owned()) {
            return Err(NasdaqParseError::DuplicateSymbol { row: row_number });
        }
        rows.push(ParsedRow { row_number, fields });
    }

    if cancellation.is_cancelled() {
        return Err(NasdaqParseError::Cancelled);
    }
    if rows.is_empty() {
        return Err(NasdaqParseError::NoRecords);
    }
    let file_creation_time = file_creation_time.ok_or(NasdaqParseError::MissingFooter)?;
    Ok(ParsedDirectory {
        kind,
        file_creation_time,
        rows,
    })
}

fn parse_footer(
    value_and_delimiters: &str,
    row: u32,
) -> Result<NasdaqFileCreationTime, NasdaqParseError> {
    let mut fields = value_and_delimiters.split('|');
    let value = fields
        .next()
        .ok_or(NasdaqParseError::InvalidFooter { row })?;
    let mut field_count = 1_usize;
    for field in fields {
        field_count = field_count
            .checked_add(1)
            .ok_or(NasdaqParseError::InvalidFooter { row })?;
        if !field.is_empty() {
            return Err(NasdaqParseError::InvalidFooter { row });
        }
    }
    if field_count > 8 {
        return Err(NasdaqParseError::InvalidFooter { row });
    }
    NasdaqFileCreationTime::try_from_provider_value(value)
        .map_err(|error| NasdaqParseError::InvalidRecord { row, error })
}

fn parse_nasdaq_listed(line: &str, row: u32) -> Result<NasdaqProviderFields, NasdaqParseError> {
    let fields = split_fields(line, row)?;
    NasdaqProviderFields::try_nasdaq_listed(
        fields[0].to_owned(),
        fields[1].to_owned(),
        parse_market_category(fields[2], row)?,
        parse_boolean("test_issue", fields[3], row)?,
        parse_financial_status(fields[4], row)?,
        parse_round_lot(fields[5], row)?,
        parse_boolean("etf", fields[6], row)?,
        parse_boolean("next_shares", fields[7], row)?,
    )
    .map_err(|error| NasdaqParseError::InvalidRecord { row, error })
}

fn parse_other_listed(line: &str, row: u32) -> Result<NasdaqProviderFields, NasdaqParseError> {
    let fields = split_fields(line, row)?;
    NasdaqProviderFields::try_other_listed(
        fields[0].to_owned(),
        fields[1].to_owned(),
        parse_other_exchange(fields[2], row)?,
        fields[3].to_owned(),
        parse_boolean("etf", fields[4], row)?,
        parse_round_lot(fields[5], row)?,
        parse_boolean("test_issue", fields[6], row)?,
        fields[7].to_owned(),
    )
    .map_err(|error| NasdaqParseError::InvalidRecord { row, error })
}

fn split_fields(line: &str, row: u32) -> Result<Vec<&str>, NasdaqParseError> {
    let fields = line.split('|').collect::<Vec<_>>();
    if fields.len() != 8 {
        Err(NasdaqParseError::InvalidFieldCount { row })
    } else {
        Ok(fields)
    }
}

fn parse_boolean(field: &'static str, value: &str, row: u32) -> Result<bool, NasdaqParseError> {
    match value {
        "Y" => Ok(true),
        "N" => Ok(false),
        _ => Err(NasdaqParseError::InvalidField { row, field }),
    }
}

fn parse_market_category(value: &str, row: u32) -> Result<NasdaqMarketCategory, NasdaqParseError> {
    match value {
        "Q" => Ok(NasdaqMarketCategory::GlobalSelect),
        "G" => Ok(NasdaqMarketCategory::GlobalMarket),
        "S" => Ok(NasdaqMarketCategory::CapitalMarket),
        _ => Err(NasdaqParseError::InvalidField {
            row,
            field: "market_category",
        }),
    }
}

fn parse_financial_status(
    value: &str,
    row: u32,
) -> Result<NasdaqFinancialStatus, NasdaqParseError> {
    match value {
        "N" => Ok(NasdaqFinancialStatus::Normal),
        "D" => Ok(NasdaqFinancialStatus::Deficient),
        "E" => Ok(NasdaqFinancialStatus::Delinquent),
        "Q" => Ok(NasdaqFinancialStatus::Bankrupt),
        "G" => Ok(NasdaqFinancialStatus::DeficientAndBankrupt),
        "H" => Ok(NasdaqFinancialStatus::DeficientAndDelinquent),
        "J" => Ok(NasdaqFinancialStatus::DelinquentAndBankrupt),
        "K" => Ok(NasdaqFinancialStatus::DeficientDelinquentAndBankrupt),
        _ => Err(NasdaqParseError::InvalidField {
            row,
            field: "financial_status",
        }),
    }
}

fn parse_other_exchange(value: &str, row: u32) -> Result<NasdaqOtherExchange, NasdaqParseError> {
    match value {
        "A" => Ok(NasdaqOtherExchange::NyseAmerican),
        "N" => Ok(NasdaqOtherExchange::Nyse),
        "P" => Ok(NasdaqOtherExchange::NyseArca),
        "M" => Ok(NasdaqOtherExchange::NyseTexas),
        "Z" => Ok(NasdaqOtherExchange::CboeBzx),
        "V" => Ok(NasdaqOtherExchange::Iex),
        _ => Err(NasdaqParseError::InvalidField {
            row,
            field: "exchange",
        }),
    }
}

fn parse_round_lot(value: &str, row: u32) -> Result<u32, NasdaqParseError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(NasdaqParseError::InvalidField {
            row,
            field: "round_lot_size",
        });
    }
    value.parse().map_err(|_| NasdaqParseError::InvalidField {
        row,
        field: "round_lot_size",
    })
}

/// Bounded Symbol Directory parse failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NasdaqParseError {
    /// The exact source object was empty.
    #[error("Nasdaq directory is empty")]
    Empty,
    /// The exact source object exceeded the adapter ceiling.
    #[error("Nasdaq directory exceeds maximum size {max}")]
    BodyTooLarge {
        /// Maximum accepted exact bytes.
        max: usize,
    },
    /// The source object was not valid UTF-8.
    #[error("Nasdaq directory is not valid UTF-8")]
    InvalidUtf8,
    /// The source header did not match the selected official file schema.
    #[error("Nasdaq directory header is invalid")]
    InvalidHeader,
    /// A line was empty or exceeded its bounded provider schema.
    #[error("Nasdaq directory line {row} is invalid")]
    InvalidLine {
        /// One-based provider row number.
        row: u32,
    },
    /// A data row did not have exactly eight provider fields.
    #[error("Nasdaq directory row {row} has an invalid field count")]
    InvalidFieldCount {
        /// One-based provider row number.
        row: u32,
    },
    /// One exact provider field used an unsupported value.
    #[error("Nasdaq directory row {row} has an invalid {field} value")]
    InvalidField {
        /// One-based provider row number.
        row: u32,
        /// Stable provider-field name.
        field: &'static str,
    },
    /// A row violated a normalized model invariant.
    #[error("Nasdaq directory row {row} is invalid: {error}")]
    InvalidRecord {
        /// One-based provider row number.
        row: u32,
        /// Exact normalized-model failure.
        error: NasdaqModelError,
    },
    /// A provider symbol appeared more than once in one exact file.
    #[error("Nasdaq directory row {row} duplicates a provider symbol")]
    DuplicateSymbol {
        /// One-based provider row number.
        row: u32,
    },
    /// The exact file contained no data rows.
    #[error("Nasdaq directory contains no records")]
    NoRecords,
    /// The exact file omitted its provider creation footer.
    #[error("Nasdaq directory footer is missing")]
    MissingFooter,
    /// The exact file repeated its provider creation footer.
    #[error("Nasdaq directory footer is duplicated")]
    DuplicateFooter,
    /// The exact file had malformed creation footer fields.
    #[error("Nasdaq directory footer on row {row} is invalid")]
    InvalidFooter {
        /// One-based provider row number.
        row: u32,
    },
    /// Data followed the creation footer.
    #[error("Nasdaq directory contains data after its footer on row {row}")]
    DataAfterFooter {
        /// One-based provider row number.
        row: u32,
    },
    /// The exact file exceeded the maximum accepted data-row count.
    #[error("Nasdaq directory exceeds maximum record count {max}")]
    TooManyRecords {
        /// Maximum accepted rows.
        max: usize,
    },
    /// Bounded parser storage could not be reserved.
    #[error("Nasdaq directory parser capacity is unavailable")]
    Capacity,
    /// The caller cancelled parsing.
    #[error("Nasdaq directory parsing was cancelled")]
    Cancelled,
}

#[cfg(test)]
mod tests {
    use market_squawk_domain::{
        DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, Timestamp,
    };
    use sha2::{Digest, Sha256};
    use tokio_util::sync::CancellationToken;

    use super::{NasdaqDirectoryKind, NasdaqParseError, parse_directory};
    use crate::NasdaqListingRecord;

    #[test]
    fn parses_normalizes_and_rejects_invalid_official_directory_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let nasdaq = b"Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\r\nAACB|Artius II Acquisition Inc. - Class A Ordinary Shares|G|N|D|100|N|N\r\nFile Creation Time: 0807202621:31|||||||\r\n";
        let other = b"ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\nTESTM|NYSE Texas Test Security |M|TESTM|Y|50|Y|TESTM\nFile Creation Time: 0807202621:31||||||\n";
        let parsed_nasdaq =
            parse_directory(NasdaqDirectoryKind::NasdaqListed, nasdaq, &cancellation)?;
        let parsed_other = parse_directory(NasdaqDirectoryKind::OtherListed, other, &cancellation)?;
        assert_eq!(parsed_nasdaq.rows.len(), 1);
        assert_eq!(parsed_nasdaq.file_creation_time.raw(), "0807202621:31");
        assert_eq!(parsed_nasdaq.file_creation_time.date().year(), 2026);
        assert_eq!(parsed_nasdaq.rows[0].fields.primary_symbol(), "AACB");
        assert_eq!(parsed_nasdaq.rows[0].fields.round_lot_size(), 100);
        assert_eq!(
            parsed_other.rows[0].fields.display_name(),
            "NYSE Texas Test Security"
        );
        assert!(parsed_other.rows[0].fields.is_etf());
        assert!(parsed_other.rows[0].fields.is_test_issue());
        assert_eq!(parsed_other.rows[0].fields.cqs_symbol(), Some("TESTM"));

        let digest = Sha256::digest(other);
        let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.into(),
        ));
        let record = NasdaqListingRecord::try_new(
            parsed_other.rows[0].row_number,
            parsed_other.file_creation_time,
            Timestamp::from_unix_nanos(41),
            Timestamp::from_unix_nanos(42),
            evidence,
            parsed_other.rows[0].fields.clone(),
        )?;
        assert_eq!(record.listing_venue().as_str(), "XCHI");
        assert_eq!(record.quality(), DataQuality::OfficialDelayed);
        let payload = serde_json::to_vec(&record)?;
        assert_eq!(NasdaqListingRecord::from_json(&payload)?, record);

        let duplicate = b"Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\nAAPL|Apple Inc.|Q|N|N|100|N|N\nAAPL|Apple Duplicate|Q|N|N|100|N|N\nFile Creation Time: 0807202621:31|||||||\n";
        assert!(matches!(
            parse_directory(NasdaqDirectoryKind::NasdaqListed, duplicate, &cancellation),
            Err(NasdaqParseError::DuplicateSymbol { row: 3 })
        ));
        let malformed_footer = b"ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\nA|Agilent|N|A|N|100|N|A\nFile Creation Time: 0807202621:31|unexpected\n";
        assert!(matches!(
            parse_directory(
                NasdaqDirectoryKind::OtherListed,
                malformed_footer,
                &cancellation
            ),
            Err(NasdaqParseError::InvalidFooter { row: 3 })
        ));
        Ok(())
    }
}
