//! Exact parsed Board data and retained evidence.

use chrono::{Datelike as _, NaiveDate, NaiveDateTime, Weekday};
use market_squawk_domain::{CalendarDate, Timestamp};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::contract::{
    BOARD_NATIVE_CONTRACT_VERSION, BoardArtifactKind, BoardDatasetContract, BoardDatasetFamily,
    BoardFileFormat, BoardFrequency, BoardRelease, BoardSeriesLifecycle,
};
use crate::digest::{finish, update_bytes, update_tag, update_u64};
use crate::{BoardAdapterError, BoardFileRequest};

/// Exact period value without inventing a timestamp for lower-frequency observations.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "precision", rename_all = "snake_case")]
pub enum BoardPeriodValue {
    /// Exact civil observation date.
    CalendarDate { date: CalendarDate },
    /// Source-authored ISO week.
    Week { year: u16, week: u8 },
    /// Source-authored month.
    Month { year: u16, month: u8 },
    /// Source-authored quarter.
    Quarter { year: u16, quarter: u8 },
    /// Source-authored annual period.
    Annual { year: u16 },
}

/// Strictly parsed source period retaining both its exact token and typed value.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BoardPeriod {
    frequency: BoardFrequency,
    raw: Box<str>,
    value: BoardPeriodValue,
}

impl BoardPeriod {
    pub(crate) fn parse(raw: &str, frequency: BoardFrequency) -> Result<Self, BoardAdapterError> {
        if raw.is_empty() || raw.len() > 32 || raw.chars().any(char::is_whitespace) {
            return Err(BoardAdapterError::InvalidPeriod);
        }
        let value = match frequency {
            BoardFrequency::BusinessDaily | BoardFrequency::Daily => {
                let date = NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                    .map_err(|_| BoardAdapterError::InvalidPeriod)?;
                if date.format("%Y-%m-%d").to_string() != raw {
                    return Err(BoardAdapterError::InvalidPeriod);
                }
                let year =
                    u16::try_from(date.year()).map_err(|_| BoardAdapterError::InvalidPeriod)?;
                BoardPeriodValue::CalendarDate {
                    date: CalendarDate::new(year, date.month() as u8, date.day() as u8)
                        .map_err(|_| BoardAdapterError::InvalidPeriod)?,
                }
            }
            BoardFrequency::Weekly => {
                let (year, week) = parse_period_pair(raw, "-W", 53)?;
                if NaiveDate::from_isoywd_opt(i32::from(year), u32::from(week), Weekday::Mon)
                    .is_none()
                {
                    return Err(BoardAdapterError::InvalidPeriod);
                }
                BoardPeriodValue::Week { year, week }
            }
            BoardFrequency::Monthly => {
                let (year, month) = parse_period_pair(raw, "-", 12)?;
                BoardPeriodValue::Month { year, month }
            }
            BoardFrequency::Quarterly => {
                let (year, quarter) = parse_period_pair(raw, "-Q", 4)?;
                BoardPeriodValue::Quarter { year, quarter }
            }
            BoardFrequency::Annual => {
                if raw.len() != 4 || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(BoardAdapterError::InvalidPeriod);
                }
                let year = raw
                    .parse::<u16>()
                    .map_err(|_| BoardAdapterError::InvalidPeriod)?;
                if year == 0 {
                    return Err(BoardAdapterError::InvalidPeriod);
                }
                BoardPeriodValue::Annual { year }
            }
        };
        Ok(Self {
            frequency,
            raw: raw.into(),
            value,
        })
    }

    /// Returns the source frequency.
    pub const fn frequency(&self) -> BoardFrequency {
        self.frequency
    }

    /// Returns the exact provider period token.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Returns the typed period value.
    pub const fn value(&self) -> &BoardPeriodValue {
        &self.value
    }
}

fn parse_period_pair(
    raw: &str,
    separator: &str,
    maximum: u8,
) -> Result<(u16, u8), BoardAdapterError> {
    let (year_text, ordinal_text) = raw
        .split_once(separator)
        .ok_or(BoardAdapterError::InvalidPeriod)?;
    if year_text.len() != 4
        || ordinal_text.len() != 2
        || !year_text.bytes().all(|byte| byte.is_ascii_digit())
        || !ordinal_text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(BoardAdapterError::InvalidPeriod);
    }
    let year = year_text
        .parse::<u16>()
        .map_err(|_| BoardAdapterError::InvalidPeriod)?;
    let ordinal = ordinal_text
        .parse::<u8>()
        .map_err(|_| BoardAdapterError::InvalidPeriod)?;
    if year == 0 || ordinal == 0 || ordinal > maximum {
        return Err(BoardAdapterError::InvalidPeriod);
    }
    Ok((year, ordinal))
}

/// Source-native missing observation and its status code.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardMissingValue {
    marker: Box<str>,
    status: Box<str>,
}

impl BoardMissingValue {
    pub(crate) fn new(marker: &str, status: &str) -> Result<Self, BoardAdapterError> {
        validate_short_text(marker)?;
        validate_short_text(status)?;
        Ok(Self {
            marker: marker.into(),
            status: status.into(),
        })
    }

    /// Returns the exact missing marker, normally `ND`.
    pub fn marker(&self) -> &str {
        &self.marker
    }

    /// Returns the provider observation-status code.
    pub fn status(&self) -> &str {
        &self.status
    }
}

/// Exact decimal or explicit source-native missing value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BoardValue {
    /// Decimal parsed directly from the provider token, without binary floating point.
    Observed {
        raw: Box<str>,
        #[serde(with = "rust_decimal::serde::str")]
        value: Decimal,
        status: Box<str>,
    },
    /// Explicit missing value.
    Missing { missing: BoardMissingValue },
}

impl BoardValue {
    pub(crate) fn parse(raw: Option<&str>, status: &str) -> Result<Self, BoardAdapterError> {
        validate_short_text(status)?;
        let raw = raw.unwrap_or("");
        if raw != raw.trim() {
            return Err(BoardAdapterError::InvalidValue);
        }
        if raw.is_empty() || raw == "ND" || status == "ND" {
            if !raw.is_empty() && raw != "ND" {
                return Err(BoardAdapterError::InvalidValue);
            }
            return Ok(Self::Missing {
                missing: BoardMissingValue::new("ND", status)?,
            });
        }
        let value = Decimal::from_str_exact(raw).map_err(|_| BoardAdapterError::InvalidValue)?;
        Ok(Self::Observed {
            raw: raw.into(),
            value,
            status: status.into(),
        })
    }

    /// Returns whether the provider marked the observation missing.
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing { .. })
    }

    fn update_digest(&self, digest: &mut Sha256) {
        match self {
            Self::Observed { raw, value, status } => {
                update_tag(digest, "observed");
                update_tag(digest, raw);
                update_tag(digest, &value.to_string());
                update_tag(digest, status);
            }
            Self::Missing { missing } => {
                update_tag(digest, "missing");
                update_tag(digest, &missing.marker);
                update_tag(digest, &missing.status);
            }
        }
    }
}

/// One immutable source observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardObservation {
    period: BoardPeriod,
    value: BoardValue,
    dimensions: BTreeMap<Box<str>, Box<str>>,
    row_digest: [u8; 32],
}

impl BoardObservation {
    pub(crate) fn try_new(
        period: BoardPeriod,
        value: BoardValue,
        dimensions: BTreeMap<Box<str>, Box<str>>,
    ) -> Result<Self, BoardAdapterError> {
        validate_dimensions(&dimensions)?;
        let mut digest = Sha256::new();
        update_tag(&mut digest, "market-squawk-federal-reserve-observation-v1");
        update_tag(&mut digest, period.frequency.as_str());
        update_tag(&mut digest, &period.raw);
        value.update_digest(&mut digest);
        update_dimensions(&mut digest, &dimensions);
        Ok(Self {
            period,
            value,
            dimensions,
            row_digest: finish(digest),
        })
    }

    /// Returns the exact period.
    pub const fn period(&self) -> &BoardPeriod {
        &self.period
    }

    /// Returns the exact value or explicit missing marker.
    pub const fn value(&self) -> &BoardValue {
        &self.value
    }

    /// Returns retained bounded SDMX observation dimensions.
    pub const fn dimensions(&self) -> &BTreeMap<Box<str>, Box<str>> {
        &self.dimensions
    }

    /// Returns the deterministic row digest.
    pub const fn row_digest(&self) -> [u8; 32] {
        self.row_digest
    }
}

/// One exact Board series and its observations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardSeries {
    unique_id: Box<str>,
    series_name: Box<str>,
    description: Box<str>,
    unit: Box<str>,
    #[serde(with = "rust_decimal::serde::str")]
    multiplier: Decimal,
    currency: Box<str>,
    frequency: BoardFrequency,
    lifecycle: BoardSeriesLifecycle,
    dimensions: BTreeMap<Box<str>, Box<str>>,
    observations: Vec<BoardObservation>,
    series_digest: [u8; 32],
}

impl BoardSeries {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        unique_id: Box<str>,
        series_name: Box<str>,
        description: Box<str>,
        unit: Box<str>,
        multiplier: Decimal,
        currency: Box<str>,
        frequency: BoardFrequency,
        lifecycle: BoardSeriesLifecycle,
        dimensions: BTreeMap<Box<str>, Box<str>>,
        observations: Vec<BoardObservation>,
    ) -> Result<Self, BoardAdapterError> {
        let multiplier = multiplier.normalize();
        if unique_id.is_empty()
            || series_name.is_empty()
            || description.is_empty()
            || unit.is_empty()
            || currency.is_empty()
            || multiplier <= Decimal::ZERO
            || observations.is_empty()
        {
            return Err(BoardAdapterError::SeriesMismatch);
        }
        validate_dimensions(&dimensions)?;
        let mut periods = BTreeSet::new();
        let mut prior: Option<&BoardPeriod> = None;
        for observation in &observations {
            if observation.period.frequency != frequency
                || !periods.insert(observation.period.raw.as_ref())
                || prior.is_some_and(|value| value >= &observation.period)
            {
                return Err(BoardAdapterError::DuplicateIdentity);
            }
            prior = Some(&observation.period);
        }
        let mut digest = Sha256::new();
        update_tag(&mut digest, "market-squawk-federal-reserve-series-v1");
        update_tag(&mut digest, &unique_id);
        update_tag(&mut digest, &series_name);
        update_tag(&mut digest, &description);
        update_tag(&mut digest, &unit);
        update_tag(&mut digest, &multiplier.to_string());
        update_tag(&mut digest, &currency);
        update_tag(&mut digest, frequency.as_str());
        update_dimensions(&mut digest, &dimensions);
        update_u64(&mut digest, observations.len() as u64);
        for observation in &observations {
            update_bytes(&mut digest, &observation.row_digest);
        }
        Ok(Self {
            unique_id,
            series_name,
            description,
            unit,
            multiplier,
            currency,
            frequency,
            lifecycle,
            dimensions,
            observations,
            series_digest: finish(digest),
        })
    }

    /// Returns the release-qualified provider series identifier.
    pub fn unique_id(&self) -> &str {
        &self.unique_id
    }
    /// Returns the source series name.
    pub fn series_name(&self) -> &str {
        &self.series_name
    }
    /// Returns the source description.
    pub fn description(&self) -> &str {
        &self.description
    }
    /// Returns the source unit.
    pub fn unit(&self) -> &str {
        &self.unit
    }
    /// Returns the exact unit multiplier.
    pub const fn multiplier(&self) -> Decimal {
        self.multiplier
    }
    /// Returns the source currency code or marker.
    pub fn currency(&self) -> &str {
        &self.currency
    }
    /// Returns the source frequency.
    pub const fn frequency(&self) -> BoardFrequency {
        self.frequency
    }
    /// Returns the retained lifecycle.
    pub const fn lifecycle(&self) -> &BoardSeriesLifecycle {
        &self.lifecycle
    }
    /// Returns retained series dimensions.
    pub const fn dimensions(&self) -> &BTreeMap<Box<str>, Box<str>> {
        &self.dimensions
    }
    /// Returns observations in strict source-period order.
    pub fn observations(&self) -> &[BoardObservation] {
        &self.observations
    }
    /// Returns the deterministic series digest.
    pub const fn series_digest(&self) -> [u8; 32] {
        self.series_digest
    }
}

/// Exact SDMX header evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardSdmxHeader {
    id: Box<str>,
    prepared: Box<str>,
    prepared_at: Option<Timestamp>,
    sender_id: Box<str>,
}

impl BoardSdmxHeader {
    pub(crate) fn try_new(
        id: &str,
        prepared: &str,
        sender_id: &str,
    ) -> Result<Self, BoardAdapterError> {
        if id.is_empty()
            || prepared.is_empty()
            || sender_id.is_empty()
            || id.len() > 256
            || prepared.len() > 128
            || sender_id.len() > 256
        {
            return Err(BoardAdapterError::SdmxSchemaDrift);
        }
        let prepared_at = match chrono::DateTime::parse_from_rfc3339(prepared) {
            Ok(value) => Some(
                value
                    .timestamp_nanos_opt()
                    .map(Timestamp::from_unix_nanos)
                    .ok_or(BoardAdapterError::SdmxSchemaDrift)?,
            ),
            Err(_) => {
                NaiveDateTime::parse_from_str(prepared, "%Y-%m-%dT%H:%M:%S%.f")
                    .map_err(|_| BoardAdapterError::SdmxSchemaDrift)?;
                None
            }
        };
        Ok(Self {
            id: id.into(),
            prepared: prepared.into(),
            prepared_at,
            sender_id: sender_id.into(),
        })
    }

    /// Returns the exact header ID.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Returns the exact Prepared token.
    pub fn prepared(&self) -> &str {
        &self.prepared
    }
    /// Returns an exact UTC instant only when the token is RFC 3339 and representable.
    pub const fn prepared_at(&self) -> Option<Timestamp> {
        self.prepared_at
    }
    /// Returns the exact sender identifier.
    pub fn sender_id(&self) -> &str {
        &self.sender_id
    }
}

/// Digest and size receipt for one exact source artifact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardArtifactReceipt {
    name: Box<str>,
    kind: BoardArtifactKind,
    bytes: u64,
    sha256: [u8; 32],
}

impl BoardArtifactReceipt {
    pub(crate) fn new(
        name: impl Into<Box<str>>,
        kind: BoardArtifactKind,
        bytes: usize,
        sha256: [u8; 32],
    ) -> Result<Self, BoardAdapterError> {
        Ok(Self {
            name: name.into(),
            kind,
            bytes: u64::try_from(bytes).map_err(|_| BoardAdapterError::CountOverflow)?,
            sha256,
        })
    }
    /// Returns the artifact name.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Returns the artifact role.
    pub const fn kind(&self) -> BoardArtifactKind {
        self.kind
    }
    /// Returns decoded/raw artifact bytes as applicable.
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
    /// Returns the SHA-256 artifact digest.
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }
}

/// Completely parsed immutable Board dataset ready for canonical conversion and publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ParsedBoardDataset {
    native_contract_version: u16,
    release: BoardRelease,
    family: BoardDatasetFamily,
    format: BoardFileFormat,
    frequency: BoardFrequency,
    route_lifecycle: crate::BoardRouteLifecycle,
    contract_digest: [u8; 32],
    request_digest: [u8; 32],
    source_payload_digest: [u8; 32],
    native_schema_digest: [u8; 32],
    normalized_content_digest: [u8; 32],
    sdmx_header: Option<BoardSdmxHeader>,
    artifacts: Vec<BoardArtifactReceipt>,
    series: Vec<BoardSeries>,
    observation_count: u64,
    missing_observation_count: u64,
}

impl ParsedBoardDataset {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        contract: &BoardDatasetContract,
        request: &BoardFileRequest,
        source_payload_digest: [u8; 32],
        native_schema_digest: [u8; 32],
        sdmx_header: Option<BoardSdmxHeader>,
        artifacts: Vec<BoardArtifactReceipt>,
        series: Vec<BoardSeries>,
    ) -> Result<Self, BoardAdapterError> {
        if request.contract_digest() != contract.contract_digest()
            || series.is_empty()
            || artifacts.is_empty()
        {
            return Err(BoardAdapterError::InvalidContract);
        }
        let mut ids = BTreeSet::new();
        let mut observation_count = 0_u64;
        let mut missing_observation_count = 0_u64;
        let mut digest = Sha256::new();
        update_tag(
            &mut digest,
            "market-squawk-federal-reserve-normalized-dataset-v1",
        );
        update_bytes(&mut digest, &contract.contract_digest());
        update_u64(&mut digest, series.len() as u64);
        for item in &series {
            if !ids.insert(item.unique_id.as_ref()) || item.frequency != contract.frequency() {
                return Err(BoardAdapterError::DuplicateIdentity);
            }
            update_bytes(&mut digest, &item.series_digest);
            observation_count = observation_count
                .checked_add(item.observations.len() as u64)
                .ok_or(BoardAdapterError::CountOverflow)?;
            missing_observation_count = missing_observation_count
                .checked_add(
                    item.observations
                        .iter()
                        .filter(|row| row.value.is_missing())
                        .count() as u64,
                )
                .ok_or(BoardAdapterError::CountOverflow)?;
        }
        Ok(Self {
            native_contract_version: BOARD_NATIVE_CONTRACT_VERSION,
            release: contract.release(),
            family: contract.family(),
            format: contract.format(),
            frequency: contract.frequency(),
            route_lifecycle: contract.route_lifecycle().clone(),
            contract_digest: contract.contract_digest(),
            request_digest: request.request_digest(),
            source_payload_digest,
            native_schema_digest,
            normalized_content_digest: finish(digest),
            sdmx_header,
            artifacts,
            series,
            observation_count,
            missing_observation_count,
        })
    }

    /// Returns the release.
    pub const fn release(&self) -> BoardRelease {
        self.release
    }
    /// Returns the selected family.
    pub const fn family(&self) -> BoardDatasetFamily {
        self.family
    }
    /// Returns the source format.
    pub const fn format(&self) -> BoardFileFormat {
        self.format
    }
    /// Returns the file frequency.
    pub const fn frequency(&self) -> BoardFrequency {
        self.frequency
    }
    /// Returns the source-evidenced lifecycle bound by the dataset contract.
    pub const fn route_lifecycle(&self) -> &crate::BoardRouteLifecycle {
        &self.route_lifecycle
    }
    /// Returns the dataset contract digest.
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
    /// Returns the immutable request digest.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    /// Returns the digest of exact acquired bytes.
    pub const fn source_payload_digest(&self) -> [u8; 32] {
        self.source_payload_digest
    }
    /// Returns the bound native schema/header digest.
    pub const fn native_schema_digest(&self) -> [u8; 32] {
        self.native_schema_digest
    }
    /// Returns the digest of normalized typed content, independent of source packaging.
    pub const fn normalized_content_digest(&self) -> [u8; 32] {
        self.normalized_content_digest
    }
    /// Returns the SDMX header for XML packages.
    pub const fn sdmx_header(&self) -> Option<&BoardSdmxHeader> {
        self.sdmx_header.as_ref()
    }
    /// Returns exact artifact receipts.
    pub fn artifacts(&self) -> &[BoardArtifactReceipt] {
        &self.artifacts
    }
    /// Returns parsed series.
    pub fn series(&self) -> &[BoardSeries] {
        &self.series
    }
    /// Returns total observations.
    pub const fn observation_count(&self) -> u64 {
        self.observation_count
    }
    /// Returns missing observations.
    pub const fn missing_observation_count(&self) -> u64 {
        self.missing_observation_count
    }
}

fn validate_short_text(value: &str) -> Result<(), BoardAdapterError> {
    if value.is_empty()
        || value.len() > 64
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        Err(BoardAdapterError::InvalidValue)
    } else {
        Ok(())
    }
}

fn validate_dimensions(values: &BTreeMap<Box<str>, Box<str>>) -> Result<(), BoardAdapterError> {
    if values.len() > 128
        || values.iter().any(|(key, value)| {
            key.is_empty()
                || key.len() > 128
                || value.len() > 8 * 1024
                || key.chars().any(char::is_control)
                || value.chars().any(char::is_control)
        })
    {
        Err(BoardAdapterError::StructuralLimitExceeded)
    } else {
        Ok(())
    }
}

fn update_dimensions(digest: &mut Sha256, values: &BTreeMap<Box<str>, Box<str>>) {
    update_u64(digest, values.len() as u64);
    for (key, value) in values {
        update_tag(digest, key);
        update_tag(digest, value);
    }
}
